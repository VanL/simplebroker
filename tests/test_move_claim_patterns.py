"""
Test patterns from message claim feature applied to move functionality.

This tests the patterns that make sense for move operations:
1. Concurrent moves don't move the same message twice
2. Move performance with large batches
3. Move with vacuum interaction
4. Schema verification for move operations with claimed column
5. Move operations update claimed status correctly
"""

import concurrent.futures as cf
import threading
from collections import Counter
from pathlib import Path

import pytest

from simplebroker._exceptions import IntegrityError
from simplebroker.db import BrokerDB


def _concurrent_move_worker(
    args: tuple[int, str, str, str, threading.Barrier],
) -> list[tuple[str, int]]:
    """Worker function for concurrent move tests."""
    _worker_id, db_path, source_queue, dest_queue, start_barrier = args
    moved: list[tuple[str, int]] = []

    with BrokerDB(db_path) as db:
        start_barrier.wait(timeout=5)
        # Each worker tries to move 5 messages
        for _ in range(5):
            result = db.move_one(source_queue, dest_queue, with_timestamps=True)
            if result:
                assert isinstance(result, tuple)
                moved.append(result)
            else:
                break  # No more messages

    return moved


def test_concurrent_moves_no_duplicate_move(workdir: Path):
    """Test that concurrent moves don't move the same message twice."""
    db_path = workdir / "test.db"

    # Write 20 messages to source queue
    expected: list[tuple[str, int]] = []
    with BrokerDB(str(db_path)) as db:
        for i in range(20):
            body = f"message{i:02d}"
            expected.append((body, db.write("source_queue", body)))

    # Start 4 concurrent move workers
    start_barrier = threading.Barrier(4)
    with cf.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(4):
            future = executor.submit(
                _concurrent_move_worker,
                (i, str(db_path), "source_queue", "dest_queue", start_barrier),
            )
            futures.append(future)

        # Collect all moved messages
        all_moved = []
        for future in cf.as_completed(futures):
            messages = future.result()
            all_moved.extend(messages)

    assert Counter(all_moved) == Counter(expected)

    # Verify source queue is empty
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.peek_generator("source_queue", with_timestamps=True))
        assert remaining == []

    # Verify dest queue has all messages
    with BrokerDB(str(db_path)) as db:
        dest_messages = list(db.peek_generator("dest_queue", with_timestamps=True))
        assert Counter(dest_messages) == Counter(expected)


def test_move_updates_claimed_status(workdir: Path):
    """A move exposes the exact pending state through public operations."""
    db_path = workdir / "test.db"

    # Write messages
    written: list[tuple[str, int]] = []
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            body = f"message{i}"
            written.append((body, db.write("source", body)))

    # Move first message
    with BrokerDB(str(db_path)) as db:
        result = db.move_one("source", "dest", with_timestamps=False)
        assert result is not None
        assert result == "message0"

    with BrokerDB(str(db_path)) as db:
        assert Counter(
            db.peek_many("source", limit=10, with_timestamps=True)
        ) == Counter(written[1:])
        assert Counter(db.peek_many("dest", limit=10, with_timestamps=True)) == Counter(
            written[:1]
        )
        assert {
            name: (pending, total) for name, pending, total in db.get_queue_stats()
        } == {"dest": (1, 1), "source": (4, 4)}


def test_move_with_vacuum_interaction(workdir: Path):
    """Test that moved messages interact correctly with vacuum."""
    db_path = workdir / "test.db"

    # Create messages in source queue
    written: list[tuple[str, int]] = []
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            body = f"msg{i}"
            written.append((body, db.write("vacuum_source", body)))

    # Move half the messages
    with BrokerDB(str(db_path)) as db:
        for _ in range(5):
            result = db.move_one("vacuum_source", "vacuum_dest", with_timestamps=False)
            assert result is not None

    with BrokerDB(str(db_path)) as db:
        dest_before = db.peek_many("vacuum_dest", limit=10, with_timestamps=True)
        assert {
            name: (pending, total) for name, pending, total in db.get_queue_stats()
        } == {"vacuum_dest": (5, 5), "vacuum_source": (5, 5)}

    # Now claim some messages in source to test vacuum interaction
    with BrokerDB(str(db_path)) as db:
        # Read 2 messages from source to claim them
        for _ in range(2):
            db.claim_one("vacuum_source", with_timestamps=False)
        assert {
            name: (pending, total) for name, pending, total in db.get_queue_stats()
        } == {"vacuum_dest": (5, 5), "vacuum_source": (3, 5)}
        assert Counter(
            db.peek_many(
                "vacuum_source",
                limit=10,
                with_timestamps=True,
                include_claimed=True,
            )
        ) == Counter(written[5:])

    # Run vacuum
    with BrokerDB(str(db_path)) as db:
        db.vacuum()

    with BrokerDB(str(db_path)) as db:
        assert {
            name: (pending, total) for name, pending, total in db.get_queue_stats()
        } == {"vacuum_dest": (5, 5), "vacuum_source": (3, 3)}
        assert Counter(
            db.peek_many("vacuum_source", limit=10, with_timestamps=True)
        ) == Counter(written[7:])
        assert Counter(
            db.peek_many("vacuum_dest", limit=10, with_timestamps=True)
        ) == Counter(dest_before)


def test_move_with_mixed_claimed_unclaimed(workdir: Path):
    """Test move behavior with mix of claimed and unclaimed messages."""
    db_path = workdir / "test.db"

    # Write 10 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("mixed_source", f"message{i}")

    # Read (claim) messages 0, 1, 2, 3, 4
    with BrokerDB(str(db_path)) as db:
        # Read first 5 messages to claim them
        for i in range(5):
            msg = db.claim_one("mixed_source", with_timestamps=False)
            assert msg == f"message{i}"

    # Now move - should only get unclaimed messages in order
    moved = []
    with BrokerDB(str(db_path)) as db:
        for _ in range(5):
            result = db.move_one("mixed_source", "mixed_dest", with_timestamps=False)
            if result:
                moved.append(result)

    # Should have moved the 5 unclaimed messages
    assert len(moved) == 5
    assert moved == ["message5", "message6", "message7", "message8", "message9"]

    # Verify no more messages to move
    with BrokerDB(str(db_path)) as db:
        result = db.move_one("mixed_source", "mixed_dest", with_timestamps=False)
        assert result is None


def test_move_failure_is_atomic(workdir: Path, monkeypatch: pytest.MonkeyPatch):
    """A database failure cannot expose a partial source-to-destination move."""
    db_path = workdir / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("atomic_source", f"message{i}")

    with BrokerDB(str(db_path)) as db:
        source_before = db.peek_many("atomic_source", limit=10, with_timestamps=True)

    with BrokerDB(str(db_path)) as db:
        real_run = db._runner.run

        def fail_after_real_move(
            sql: str,
            params: tuple[object, ...] = (),
            *,
            fetch: bool = False,
        ):
            rows = real_run(sql, params, fetch=fetch)
            if fetch:
                raise IntegrityError("injected post-move failure")
            return rows

        with monkeypatch.context() as transaction_fault:
            transaction_fault.setattr(db._runner, "run", fail_after_real_move)
            with pytest.raises(IntegrityError, match="injected post-move failure"):
                db.move_one("atomic_source", "atomic_dest", with_timestamps=False)

        assert (
            db.peek_many("atomic_source", limit=10, with_timestamps=True)
            == source_before
        )
        assert db.peek_many("atomic_dest", limit=10, with_timestamps=True) == []


def test_move_preserves_message_ordering(workdir: Path):
    """Test that moves preserve strict FIFO ordering."""
    db_path = workdir / "test.db"

    # Write messages with specific content to verify order
    messages = []
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            msg = f"ordered_message_{i:03d}"
            messages.append(msg)
            db.write("order_source", msg)

    # Move all messages
    moved = []
    with BrokerDB(str(db_path)) as db:
        while True:
            result = db.move_one("order_source", "order_dest", with_timestamps=False)
            if result is None:
                break
            moved.append(result)

    # Verify order is preserved
    assert moved == messages

    # Read from destination to verify order is maintained
    with BrokerDB(str(db_path)) as db:
        dest_messages = list(db.peek_generator("order_dest", with_timestamps=False))
        assert dest_messages == messages


def test_move_empty_to_empty_queue(workdir: Path):
    """Test move between non-existent/empty queues."""
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as db:
        # Move from non-existent queue
        result = db.move_one(
            "does_not_exist", "also_does_not_exist", with_timestamps=False
        )
        assert result is None

        # Create empty source queue
        db.write("empty_source", "temp")
        db.claim_one("empty_source", with_timestamps=False)  # Claim the message

        # Move from empty queue
        result = db.move_one("empty_source", "empty_dest", with_timestamps=False)
        assert result is None


def test_multiple_sequential_moves(workdir: Path):
    """Test multiple sequential moves maintain consistency."""
    db_path = workdir / "test.db"

    # Create messages in multiple source queues
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("source1", f"s1_msg{i}")
            db.write("source2", f"s2_msg{i}")

    # Move from alternating sources
    moved = []
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            source = "source1" if i % 2 == 0 else "source2"
            result = db.move_one(source, "combined_dest", with_timestamps=False)
            if result:
                moved.append(result)

    assert len(moved) == 10

    # Verify both sources are empty
    with BrokerDB(str(db_path)) as db:
        assert list(db.peek_generator("source1", with_timestamps=False)) == []
        assert list(db.peek_generator("source2", with_timestamps=False)) == []

        # Verify destination has all messages
        dest_messages = list(db.peek_generator("combined_dest", with_timestamps=False))
        assert len(dest_messages) == 10
