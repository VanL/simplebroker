"""Test exactly-once delivery guarantee for streaming reads."""

import multiprocessing
from pathlib import Path
from typing import List, Tuple

from simplebroker.db import BrokerDB


def read_messages_with_crash(
    db_path: str, queue: str, crash_after: int
) -> Tuple[List[str], bool]:
    """Read messages and simulate crash after N messages.

    Returns:
        Tuple of (messages_read, did_crash)
    """
    messages = []
    crashed = False

    with BrokerDB(db_path) as db:
        for i, message in enumerate(
            db.stream_read(queue, peek=False, all_messages=True, commit_interval=10)
        ):
            messages.append(message)
            if (
                i == crash_after - 1
            ):  # 0-indexed, so crash_after=2 means crash after 2nd message
                # Simulate crash by abruptly exiting
                crashed = True
                break

    return messages, crashed


def test_single_message_immediate_commit(workdir: Path):
    """Test that single message reads commit immediately."""
    db_path = workdir / "test.db"

    # Write a message
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message1")
        db.write("test_queue", "message2")

    # Read single message
    with BrokerDB(str(db_path)) as db:
        messages = list(db.stream_read("test_queue", peek=False, all_messages=False))
        assert len(messages) == 1
        assert messages[0] == "message1"

    # Verify first message is gone but second remains
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.stream_read("test_queue", peek=True, all_messages=True))
        assert len(remaining) == 1
        assert remaining[0] == "message2"


def test_batch_commit_for_all_messages(workdir: Path):
    """Test that --all provides at-least-once delivery with batch commits."""
    db_path = workdir / "test.db"

    # Write 25 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(25):
            db.write("test_queue", f"message{i}")

    # Simulate reading 12 messages then crashing
    messages, crashed = read_messages_with_crash(str(db_path), "test_queue", 12)
    assert crashed
    assert len(messages) == 12

    # Check how many messages remain
    # With proper at-least-once delivery, commits happen AFTER yielding
    # We read 12 messages (crashed after message 11, 0-indexed), which means:
    # - Batch 1 (messages 0-9): Fetched, yielded completely, committed
    # - Batch 2 (messages 10-19): Fetched, yielded only 2 messages, then crashed
    # Since we crashed before completing the yield of batch 2, it was NOT committed
    # So only messages 0-9 are deleted, leaving messages 10-24
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.stream_read("test_queue", peek=True, all_messages=True))
        assert len(remaining) == 15, f"Expected 15 messages, got {len(remaining)}"
        assert remaining[0] == "message10"  # First uncommitted message
        assert remaining[-1] == "message24"  # Last message


def test_peek_never_commits(workdir: Path):
    """Test that peek operations never delete messages."""
    db_path = workdir / "test.db"

    # Write 5 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")

    # Peek all messages
    with BrokerDB(str(db_path)) as db:
        peeked = list(db.stream_read("test_queue", peek=True, all_messages=True))
        assert len(peeked) == 5

    # All messages should still be there
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.stream_read("test_queue", peek=True, all_messages=True))
        assert len(remaining) == 5


def _read_all_messages_worker(db_path: str, result_list):
    """Read all messages and append to shared list (module-level for pickling)."""
    with BrokerDB(db_path) as db:
        messages = list(
            db.stream_read(
                "test_queue", peek=False, all_messages=True, commit_interval=5
            )
        )
        result_list.extend(messages)


def test_concurrent_readers_safety(workdir: Path):
    """Test that concurrent readers don't cause duplicate delivery."""
    db_path = workdir / "test.db"

    # Write 20 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(20):
            db.write("test_queue", f"message{i}")

    # Use manager to share results between processes
    with multiprocessing.Manager() as manager:
        results1 = manager.list()
        results2 = manager.list()

        # Start two readers concurrently
        p1 = multiprocessing.Process(
            target=_read_all_messages_worker, args=(str(db_path), results1)
        )
        p2 = multiprocessing.Process(
            target=_read_all_messages_worker, args=(str(db_path), results2)
        )

        p1.start()
        p2.start()

        p1.join()
        p2.join()

        # Combine results
        all_messages = list(results1) + list(results2)

        # Should have exactly 20 messages (no duplicates)
        assert len(all_messages) == 20

        # Each message should appear exactly once
        message_set = set(all_messages)
        assert len(message_set) == 20  # No duplicates

        # Verify all messages were read
        expected_messages = {f"message{i}" for i in range(20)}
        assert message_set == expected_messages


def test_commit_interval_parameter_respected(workdir: Path):
    """Test that commit_interval parameter is properly used."""
    db_path = workdir / "test.db"

    # Write 15 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(15):
            db.write("test_queue", f"message{i}")

    # Read with custom commit interval of 3
    messages_read = []
    with BrokerDB(str(db_path)) as db:
        for i, message in enumerate(
            db.stream_read(
                "test_queue", peek=False, all_messages=True, commit_interval=3
            )
        ):
            messages_read.append(message)
            if i == 7:  # Read 8 messages (0-7), then simulate crash
                break

    assert len(messages_read) == 8

    # With proper at-least-once delivery, commits happen AFTER yielding
    # We read 8 messages (0-7), with commit_interval=3:
    # - Batch 1 (messages 0-2): Fetched, yielded completely, committed
    # - Batch 2 (messages 3-5): Fetched, yielded completely, committed
    # - Batch 3 (messages 6-8): Fetched, yielded only 2 messages (6-7), then crashed
    # Since we crashed before completing batch 3, it was NOT committed
    # So only messages 0-5 are deleted, leaving messages 6-14
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.stream_read("test_queue", peek=True, all_messages=True))
        assert len(remaining) == 9, f"Expected 9 messages, got {len(remaining)}"
        assert remaining[0] == "message6"  # First uncommitted message
