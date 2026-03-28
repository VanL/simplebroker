"""Test exactly-once and at-least-once delivery guarantees for streaming reads.

The delivery guarantee depends on the commit_interval parameter:
- commit_interval=1 (default): Exactly-once delivery - messages are committed BEFORE yielding
- commit_interval>1: At-least-once delivery - each batch is committed only after
  the entire batch has been yielded
"""

import multiprocessing
from pathlib import Path

from simplebroker.sbqueue import Queue


def read_messages_with_crash(
    db_path: str, queue: str, crash_after: int, batch_size: int = 100
) -> tuple[list[str], bool]:
    """Read messages and simulate crash after N messages.

    Returns:
        tuple of (messages_read, did_crash)
    """
    messages = []
    crashed = False

    # Use the database layer directly to control batch size
    from simplebroker.db import BrokerDB

    with BrokerDB(db_path) as broker:
        generator = broker.claim_generator(
            queue,
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            batch_size=batch_size,
        )
        try:
            for i, message in enumerate(generator):
                messages.append(message)
                if (
                    i == crash_after - 1
                ):  # 0-indexed, so crash_after=2 means crash after 2nd message
                    # Simulate crash by abruptly exiting
                    crashed = True
                    break
        finally:
            generator.close()

    return messages, crashed


def test_single_message_immediate_commit(workdir: Path):
    """Test that single message reads provide exactly-once delivery."""
    db_path = workdir / "test.db"

    # Write a message
    q = Queue("test_queue", db_path=str(db_path))
    q.write("message1")
    q.write("message2")

    # Read single message
    message = q.read_one()
    assert message == "message1"

    # Verify first message is gone but second remains
    remaining = list(q.peek(all_messages=True))
    assert len(remaining) == 1
    assert remaining[0] == "message2"


def test_batch_commit_for_all_messages(workdir: Path):
    """Test that --all provides at-least-once delivery with batch commits."""
    db_path = workdir / "test.db"

    # Write 25 messages
    q = Queue("test_queue", db_path=str(db_path))
    for i in range(25):
        q.write(f"message{i}")

    # Simulate reading 12 messages then crashing with batch size 10
    messages, crashed = read_messages_with_crash(
        str(db_path), "test_queue", 12, batch_size=10
    )
    assert crashed
    assert len(messages) == 12

    # Check how many messages remain.
    # With at-least-once delivery, a batch is committed only after it is fully yielded.
    # We read 12 messages (crashed after message 11, 0-indexed):
    # - Batch 1 (0-9): fully yielded and committed
    # - Batch 2 (10-19): partially yielded then interrupted, so it rolls back
    # Remaining: messages 10-24 (15 messages)
    q2 = Queue("test_queue", db_path=str(db_path))
    remaining = list(q2.peek(all_messages=True))
    assert len(remaining) == 15, f"Expected 15 messages, got {len(remaining)}"
    assert remaining[0] == "message10"  # First message in rolled-back batch
    assert remaining[-1] == "message24"  # Last message


def test_peek_never_commits(workdir: Path):
    """Test that peek operations never delete messages."""
    db_path = workdir / "test.db"

    # Write 5 messages
    q = Queue("test_queue", db_path=str(db_path))
    for i in range(5):
        q.write(f"message{i}")

    # Peek all messages
    peeked = list(q.peek(all_messages=True))
    assert len(peeked) == 5

    # All messages should still be there
    remaining = list(q.peek(all_messages=True))
    assert len(remaining) == 5


def _read_all_messages_worker(db_path: str, result_list):
    """Read all messages and append to shared list (module-level for pickling)."""
    q = Queue("test_queue", db_path=str(db_path))
    messages = list(q.read_generator(delivery_guarantee="at_least_once"))
    result_list.extend(messages)


def test_exactly_once_delivery_with_default_interval(workdir: Path):
    """Test that default delivery_guarantee='exactly_once' provides exactly-once delivery."""
    db_path = workdir / "test.db"

    # Write 10 messages
    q = Queue("test_queue", db_path=str(db_path))
    for i in range(10):
        q.write(f"message{i}")

    # Read with default delivery_guarantee='exactly_once'
    messages_read = []
    for i, message in enumerate(q.read_generator()):
        messages_read.append(message)
        if i == 4:  # Read 5 messages (0-4), then simulate crash
            break

    assert len(messages_read) == 5

    # With exactly-once delivery, commits happen BEFORE yielding
    # Each message is individually committed before being yielded
    # We read 5 messages (0-4), each was committed before yielding
    # So messages 0-4 are deleted, leaving messages 5-9
    q2 = Queue("test_queue", db_path=str(db_path))
    remaining = list(q2.peek(all_messages=True))
    assert len(remaining) == 5, f"Expected 5 messages, got {len(remaining)}"
    assert remaining[0] == "message5"  # First unread message
    assert remaining[-1] == "message9"  # Last message


def test_exactly_once_with_explicit_interval_1(workdir: Path):
    """Test that explicit delivery_guarantee='exactly_once' provides exactly-once delivery."""
    db_path = workdir / "test.db"

    # Write 15 messages
    q = Queue("test_queue", db_path=str(db_path))
    for i in range(15):
        q.write(f"message{i}")

    # Read with explicit delivery_guarantee='exactly_once'
    messages_read = []
    for i, message in enumerate(q.read_generator(delivery_guarantee="exactly_once")):
        messages_read.append(message)
        if i == 7:  # Read 8 messages (0-7), then simulate crash
            break

    assert len(messages_read) == 8

    # With exactly-once delivery, each message is committed individually
    # Messages 0-7 were each committed before being yielded
    # So messages 0-7 are deleted, leaving messages 8-14
    q2 = Queue("test_queue", db_path=str(db_path))
    remaining = list(q2.peek(all_messages=True))
    assert len(remaining) == 7, f"Expected 7 messages, got {len(remaining)}"
    assert remaining[0] == "message8"  # First unread message
    assert remaining[-1] == "message14"  # Last message


def test_concurrent_readers_safety(workdir: Path):
    """Test that concurrent readers don't cause duplicate delivery."""
    db_path = workdir / "test.db"

    # Write 20 messages
    q = Queue("test_queue", db_path=str(db_path))
    for i in range(20):
        q.write(f"message{i}")

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
    """Test that batch size is properly used with at-least-once delivery."""

    db_path = workdir / "test.db"

    # Write 15 messages
    q = Queue("test_queue", db_path=str(db_path))
    for i in range(15):
        q.write(f"message{i}")

    # Read with batch size of 3 using database layer directly
    from simplebroker.db import BrokerDB

    with BrokerDB(str(db_path)) as broker:
        messages_read = []
        for i, message in enumerate(
            broker.claim_generator(
                "test_queue",
                with_timestamps=False,
                delivery_guarantee="at_least_once",
                batch_size=3,
            )
        ):
            messages_read.append(message)
            if i == 7:  # Read 8 messages (0-7), then simulate crash
                break

    assert len(messages_read) == 8

    # At-least-once now commits only fully consumed batches.
    # With batch_size=3 and reading 8 messages:
    # - Batches [0-2] and [3-5] commit
    # - Batch [6-8] is interrupted after yielding 6 and 7, so it rolls back
    # Remaining: messages 6-14 (9 messages)
    q2 = Queue("test_queue", db_path=str(db_path))
    remaining = list(q2.peek(all_messages=True))
    assert len(remaining) == 9, f"Expected 9 messages, got {len(remaining)}"
    assert remaining[0] == "message6"  # First message in rolled-back batch
