"""Integration tests for enhanced move functionality."""

import pytest

pytestmark = [pytest.mark.shared]


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
