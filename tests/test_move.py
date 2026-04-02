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


def test_move_invalid_queue_names(queue_factory):
    """Test that move validates queue names."""
    invalid_source = queue_factory(".invalid")
    with pytest.raises(ValueError, match="Invalid queue name"):
        invalid_source.move_one("dest")

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


def test_move_atomic(queue_factory):
    """Test that move is atomic (all-or-nothing)."""
    source = queue_factory("source")
    source.write("important_message")

    result1 = source.move_one("dest1", with_timestamps=False)
    result2 = source.move_one("dest2", with_timestamps=False)

    # Only one move should succeed
    assert (result1 is None) != (result2 is None)  # XOR

    dest1 = queue_factory("dest1")
    dest2 = queue_factory("dest2")
    dest1_msgs = dest1.peek_many(limit=10, with_timestamps=False)
    dest2_msgs = dest2.peek_many(limit=10, with_timestamps=False)

    if result1:
        assert dest1_msgs == ["important_message"]
        assert dest2_msgs == []
    else:
        assert dest1_msgs == []
        assert dest2_msgs == ["important_message"]


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
