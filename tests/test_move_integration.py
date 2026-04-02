"""Integration tests for enhanced move functionality."""

import pytest

pytestmark = [pytest.mark.shared]


def test_move_with_concurrent_operations(queue_factory):
    """Test move with concurrent reads and writes."""
    source = queue_factory("source")
    dest = queue_factory("dest")

    for i in range(10):
        source.write(f"msg{i}")

    moved_messages = []
    for _ in range(3):
        result = source.move_one("dest", with_timestamps=False)
        assert result is not None
        moved_messages.append(result)

    assert moved_messages == ["msg0", "msg1", "msg2"]

    source_msgs = source.peek_many(limit=10, with_timestamps=False)
    assert len(source_msgs) == 7
    expected_remaining = [f"msg{i}" for i in range(3, 10)]
    assert source_msgs == expected_remaining

    dest_msgs = dest.peek_many(limit=10, with_timestamps=False)
    assert dest_msgs == ["msg0", "msg1", "msg2"]


def test_move_claimed_message_workflow(queue_factory):
    """Test real-world workflow of moving claimed messages."""
    processing = queue_factory("processing")
    error = queue_factory("error")

    processing.write("job1")
    processing.write("job2")
    processing.write("job3")

    job1 = processing.read_one(with_timestamps=False)
    assert job1 == "job1"

    job2 = processing.read_one(with_timestamps=False)
    assert job2 == "job2"

    result = processing.move_one("error", with_timestamps=False)
    assert result is not None
    assert result == "job3"

    error_msgs = error.peek_many(limit=10, with_timestamps=False)
    assert error_msgs == ["job3"]

    result = processing.move_one("error", with_timestamps=False)
    assert result is None


def test_move_maintains_fifo_within_queues(queue_factory):
    """Test that moves maintain FIFO order within each queue."""
    queue1 = queue_factory("queue1")
    queue2 = queue_factory("queue2")

    for i in range(5):
        queue1.write(f"q1-msg{i}")

    for i in range(3):
        queue2.write(f"q2-msg{i}")

    moved1 = queue1.move_one("queue2", with_timestamps=False)
    assert moved1 == "q1-msg0"

    moved2 = queue1.move_one("queue2", with_timestamps=False)
    assert moved2 == "q1-msg1"

    q1_remaining = queue1.peek_many(limit=10, with_timestamps=False)
    assert q1_remaining == ["q1-msg2", "q1-msg3", "q1-msg4"]

    q2_all = queue2.peek_many(limit=10, with_timestamps=False)
    assert len(q2_all) == 5
    assert q2_all == ["q1-msg0", "q1-msg1", "q2-msg0", "q2-msg1", "q2-msg2"]
