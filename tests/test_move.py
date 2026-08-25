"""Test queue move functionality."""

import pytest

pytestmark = [pytest.mark.shared]


def test_move_basic(queue_factory):
    """Test basic message move between queues."""
    source = queue_factory("source")
    source.write("msg1")
    source.write("msg2")
    source.write("msg3")

    # Move first message (request with timestamps to get tuple)
    result = source.move_one("dest", with_timestamps=True)
    assert result is not None
    assert result[0] == "msg1"  # Check message body
    assert isinstance(result[1], int)  # Check timestamp is present

    # Verify source has 2 messages left
    messages = source.peek_many(limit=10, with_timestamps=False)
    assert messages == ["msg2", "msg3"]

    # Verify dest has the moved message
    dest = queue_factory("dest")
    messages = dest.peek_many(limit=10, with_timestamps=False)
    assert messages == ["msg1"]


def test_move_empty_queue(queue_factory):
    """Test move from empty queue returns None."""
    empty_queue = queue_factory("empty")
    result = empty_queue.move_one("dest")
    assert result is None


def test_move_preserves_order(queue_factory):
    """Test that move preserves FIFO order."""
    source = queue_factory("source")
    for i in range(5):
        source.write(f"msg{i}")

    moved = []
    for _ in range(5):
        result = source.move_one("dest", with_timestamps=False)
        assert result is not None
        moved.append(result)

    assert moved == ["msg0", "msg1", "msg2", "msg3", "msg4"]

    result = source.move_one("dest", with_timestamps=False)
    assert result is None


def test_move_only_unclaimed(queue_factory):
    """Test that move only moves unclaimed messages."""
    source = queue_factory("source")
    source.write("msg1")
    source.write("msg2")
    source.write("msg3")

    # Read (claim) first message
    claimed = source.read_one(with_timestamps=False)
    assert claimed == "msg1"

    # Move should get the first unclaimed message
    result = source.move_one("dest", with_timestamps=False)
    assert result is not None
    assert result == "msg2"

    # Verify remaining messages in source (should be msg3, msg1 was claimed)
    messages = source.peek_many(limit=10, with_timestamps=False)
    assert messages == ["msg3"]
    source_stats = source.stats()
    assert (
        source_stats.pending,
        source_stats.claimed,
        source_stats.total,
    ) == (1, 1, 2)
    dest = queue_factory("dest")
    assert dest.peek_many(limit=10, with_timestamps=False) == ["msg2"]


def test_move_invalid_queue_names(queue_factory):
    """Test that move validates queue names."""
    with pytest.raises(ValueError, match="Invalid queue name"):
        queue_factory(".invalid")

    source = queue_factory("source")
    with pytest.raises(ValueError, match="Invalid queue name"):
        source.move_one("-invalid")


def test_move_same_queue(queue_factory):
    """Test move to same queue raises ValueError."""
    queue = queue_factory("queue")
    queue.write("msg1")
    queue.write("msg2")

    with pytest.raises(
        ValueError, match="Source and destination queues cannot be the same"
    ):
        queue.move_one("queue")


@pytest.mark.parametrize("operation", ["move_many", "move_generator"])
def test_bulk_move_interfaces_reject_the_same_source_and_destination(
    queue_factory,
    operation: str,
) -> None:
    """Every bulk move interface must reject a self-move before mutation."""

    queue = queue_factory("queue")
    queue.write("msg1")

    with pytest.raises(
        ValueError, match="Source and destination queues cannot be the same"
    ):
        if operation == "move_many":
            queue.move_many("queue", 1)
        else:
            list(queue.move_generator("queue"))

    assert queue.peek() == "msg1"


def test_move_with_existing_dest_messages(queue_factory):
    """Test move to queue that already has messages."""
    dest = queue_factory("dest")
    dest.write("existing1")
    dest.write("existing2")

    source = queue_factory("source")
    source.write("new1")
    source.write("new2")

    result = source.move_one("dest", with_timestamps=False)
    assert result is not None
    assert result == "new1"

    messages = dest.peek_many(limit=10, with_timestamps=False)
    assert len(messages) == 3
    assert "existing1" in messages
    assert "existing2" in messages
    assert "new1" in messages


def test_move_interleaves_by_original_timestamp_in_destination(queue_factory):
    """Moved messages interleave with destination natives in ID order.

    Folded from the retired test_move_integration.py (audit Task 7.3).
    """
    queue1 = queue_factory("queue1")
    queue2 = queue_factory("queue2")

    for i in range(5):
        queue1.write(f"q1-msg{i}")
    for i in range(3):
        queue2.write(f"q2-msg{i}")

    assert queue1.move_one("queue2", with_timestamps=False) == "q1-msg0"
    assert queue1.move_one("queue2", with_timestamps=False) == "q1-msg1"

    assert queue1.peek_many(limit=10, with_timestamps=False) == [
        "q1-msg2",
        "q1-msg3",
        "q1-msg4",
    ]
    assert queue2.peek_many(limit=10, with_timestamps=False) == [
        "q1-msg0",
        "q1-msg1",
        "q2-msg0",
        "q2-msg1",
        "q2-msg2",
    ]
