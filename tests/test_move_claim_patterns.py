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


