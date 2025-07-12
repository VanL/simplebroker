"""Test queue move functionality."""

import os
import tempfile

import pytest

from simplebroker.db import BrokerDB


def test_move_basic():
    """Test basic message move between queues."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Setup: Add messages to source queue
            db.write("source", "msg1")
            db.write("source", "msg2")
            db.write("source", "msg3")

            # Move first message
            result = db.move("source", "dest")
            assert result is not None
            assert result["body"] == "msg1"
            assert "ts" in result

            # Verify source has 2 messages left
            messages = db.read("source", peek=True, all_messages=True)
            assert messages == ["msg2", "msg3"]

            # Verify dest has the moved message
            messages = db.read("dest", peek=True, all_messages=True)
            assert messages == ["msg1"]


def test_move_empty_queue():
    """Test move from empty queue returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Move from non-existent/empty queue
            result = db.move("empty", "dest")
            assert result is None


def test_move_preserves_order():
    """Test that move preserves FIFO order."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages in order
            for i in range(5):
                db.write("source", f"msg{i}")

            # Move messages one by one
            moved = []
            for _ in range(5):
                result = db.move("source", "dest")
                assert result is not None
                moved.append(result["body"])

            # Verify FIFO order was preserved
            assert moved == ["msg0", "msg1", "msg2", "msg3", "msg4"]

            # Source should be empty
            result = db.move("source", "dest")
            assert result is None


def test_move_only_unclaimed():
    """Test that move only moves unclaimed messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages
            db.write("source", "msg1")
            db.write("source", "msg2")
            db.write("source", "msg3")

            # Claim first message
            claimed = db.read("source")
            assert claimed == ["msg1"]

            # Move should get the first unclaimed message
            result = db.move("source", "dest")
            assert result is not None
            assert result["body"] == "msg2"

            # Verify remaining messages
            messages = db.read("source", peek=True, all_messages=True)
            assert messages == ["msg3"]  # msg1 is claimed, msg2 was moved


def test_move_invalid_queue_names():
    """Test that move validates queue names."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Invalid source queue
            with pytest.raises(ValueError, match="Invalid queue name"):
                db.move(".invalid", "dest")

            # Invalid destination queue
            with pytest.raises(ValueError, match="Invalid queue name"):
                db.move("source", "-invalid")


def test_move_same_queue():
    """Test move to same queue (should work but be a no-op effectively)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("queue", "msg1")
            db.write("queue", "msg2")

            # Move to same queue
            result = db.move("queue", "queue")
            assert result is not None
            assert result["body"] == "msg1"

            # Messages should still be in the queue
            messages = db.read("queue", peek=True, all_messages=True)
            assert messages == ["msg1", "msg2"]


def test_move_atomic():
    """Test that move is atomic (all-or-nothing)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add message to source
            db.write("source", "important_message")

            # Concurrent move attempts should be safe
            # (In a real concurrent test, only one would succeed)
            result1 = db.move("source", "dest1")
            result2 = db.move("source", "dest2")

            # Only one move should succeed
            assert (result1 is None) != (result2 is None)  # XOR

            # Message should be in exactly one destination
            dest1_msgs = db.read("dest1", peek=True, all_messages=True)
            dest2_msgs = db.read("dest2", peek=True, all_messages=True)

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

        with BrokerDB(db_path) as db:
            # Setup destination with existing messages
            db.write("dest", "existing1")
            db.write("dest", "existing2")

            # Add source messages
            db.write("source", "new1")
            db.write("source", "new2")

            # Move a message
            result = db.move("source", "dest")
            assert result is not None
            assert result["body"] == "new1"

            # Dest should have all messages in order
            messages = db.read("dest", peek=True, all_messages=True)
            assert len(messages) == 3
            assert "existing1" in messages
            assert "existing2" in messages
            assert "new1" in messages
