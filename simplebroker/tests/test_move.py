"""Test queue move functionality."""

import os
import tempfile

import pytest

from simplebroker import Queue


def test_move_basic():
    """Test basic message move between queues."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        # Setup: Add messages to source queue
        source = Queue("source", db_path=db_path)
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
        dest = Queue("dest", db_path=db_path)
        messages = dest.peek_many(limit=10, with_timestamps=False)
        assert messages == ["msg1"]


def test_move_empty_queue():
    """Test move from empty queue returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        # Move from non-existent/empty queue
        empty_queue = Queue("empty", db_path=db_path)
        result = empty_queue.move_one("dest")
        assert result is None


def test_move_preserves_order():
    """Test that move preserves FIFO order."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        source = Queue("source", db_path=db_path)
        # Add messages in order
        for i in range(5):
            source.write(f"msg{i}")

        # Move messages one by one (with_timestamps=False for simplicity)
        moved = []
        for _ in range(5):
            result = source.move_one("dest", with_timestamps=False)
            assert result is not None
            moved.append(result)

        # Verify FIFO order was preserved
        assert moved == ["msg0", "msg1", "msg2", "msg3", "msg4"]

        # Source should be empty
        result = source.move_one("dest", with_timestamps=False)
        assert result is None


def test_move_only_unclaimed():
    """Test that move only moves unclaimed messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        source = Queue("source", db_path=db_path)
        # Add messages
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


def test_move_invalid_queue_names():
    """Test that move validates queue names."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        # Invalid source queue
        invalid_source = Queue(".invalid", db_path=db_path)
        with pytest.raises(ValueError, match="Invalid queue name"):
            invalid_source.move_one("dest")

        # Invalid destination queue
        source = Queue("source", db_path=db_path)
        with pytest.raises(ValueError, match="Invalid queue name"):
            source.move_one("-invalid")


def test_move_same_queue():
    """Test move to same queue raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        queue = Queue("queue", db_path=db_path)
        queue.write("msg1")
        queue.write("msg2")

        # Move to same queue should raise ValueError
        with pytest.raises(
            ValueError, match="Source and destination queues cannot be the same"
        ):
            queue.move_one("queue")


def test_move_atomic():
    """Test that move is atomic (all-or-nothing)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        source = Queue("source", db_path=db_path)
        # Add message to source
        source.write("important_message")

        # Concurrent move attempts should be safe
        # (In a real concurrent test, only one would succeed)
        result1 = source.move_one("dest1", with_timestamps=False)
        result2 = source.move_one("dest2", with_timestamps=False)

        # Only one move should succeed
        assert (result1 is None) != (result2 is None)  # XOR

        # Message should be in exactly one destination
        dest1 = Queue("dest1", db_path=db_path)
        dest2 = Queue("dest2", db_path=db_path)
        dest1_msgs = dest1.peek_many(limit=10, with_timestamps=False)
        dest2_msgs = dest2.peek_many(limit=10, with_timestamps=False)

        if result1:
            assert dest1_msgs == ["important_message"]
            assert dest2_msgs == []
        else:
            assert dest1_msgs == []
            assert dest2_msgs == ["important_message"]


def test_move_with_existing_dest_messages():
    """Test move to queue that already has messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        # Setup destination with existing messages
        dest = Queue("dest", db_path=db_path)
        dest.write("existing1")
        dest.write("existing2")

        # Add source messages
        source = Queue("source", db_path=db_path)
        source.write("new1")
        source.write("new2")

        # Move a message (with_timestamps=False for simplicity)
        result = source.move_one("dest", with_timestamps=False)
        assert result is not None
        assert result == "new1"

        # Dest should have all messages in order
        messages = dest.peek_many(limit=10, with_timestamps=False)
        assert len(messages) == 3
        assert "existing1" in messages
        assert "existing2" in messages
        assert "new1" in messages
