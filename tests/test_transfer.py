"""Test queue transfer functionality."""

import os
import tempfile

import pytest

from simplebroker.db import BrokerDB


def test_transfer_basic():
    """Test basic message transfer between queues."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Setup: Add messages to source queue
            db.write("source", "msg1")
            db.write("source", "msg2")
            db.write("source", "msg3")

            # Transfer first message
            result = db.transfer("source", "dest")
            assert result is not None
            assert result["body"] == "msg1"
            assert "ts" in result

            # Verify source has 2 messages left
            messages = db.read("source", peek=True, all_messages=True)
            assert messages == ["msg2", "msg3"]

            # Verify dest has the transferred message
            messages = db.read("dest", peek=True, all_messages=True)
            assert messages == ["msg1"]


def test_transfer_empty_queue():
    """Test transfer from empty queue returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Transfer from non-existent/empty queue
            result = db.transfer("empty", "dest")
            assert result is None


def test_transfer_preserves_order():
    """Test that transfer preserves FIFO order."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages in order
            for i in range(5):
                db.write("source", f"msg{i}")

            # Transfer messages one by one
            transferred = []
            for _ in range(5):
                result = db.transfer("source", "dest")
                assert result is not None
                transferred.append(result["body"])

            # Verify FIFO order was preserved
            assert transferred == ["msg0", "msg1", "msg2", "msg3", "msg4"]

            # Source should be empty
            result = db.transfer("source", "dest")
            assert result is None


def test_transfer_only_unclaimed():
    """Test that transfer only moves unclaimed messages."""
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

            # Transfer should get the first unclaimed message
            result = db.transfer("source", "dest")
            assert result is not None
            assert result["body"] == "msg2"

            # Verify remaining messages
            messages = db.read("source", peek=True, all_messages=True)
            assert messages == ["msg3"]  # msg1 is claimed, msg2 was transferred


def test_transfer_invalid_queue_names():
    """Test that transfer validates queue names."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Invalid source queue
            with pytest.raises(ValueError, match="Invalid queue name"):
                db.transfer(".invalid", "dest")

            # Invalid destination queue
            with pytest.raises(ValueError, match="Invalid queue name"):
                db.transfer("source", "-invalid")


def test_transfer_same_queue():
    """Test transfer to same queue (should work but be a no-op effectively)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("queue", "msg1")
            db.write("queue", "msg2")

            # Transfer to same queue
            result = db.transfer("queue", "queue")
            assert result is not None
            assert result["body"] == "msg1"

            # Messages should still be in the queue
            messages = db.read("queue", peek=True, all_messages=True)
            assert messages == ["msg1", "msg2"]


def test_transfer_atomic():
    """Test that transfer is atomic (all-or-nothing)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add message to source
            db.write("source", "important_message")

            # Concurrent transfer attempts should be safe
            # (In a real concurrent test, only one would succeed)
            result1 = db.transfer("source", "dest1")
            result2 = db.transfer("source", "dest2")

            # Only one transfer should succeed
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


def test_transfer_with_existing_dest_messages():
    """Test transfer to queue that already has messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Setup destination with existing messages
            db.write("dest", "existing1")
            db.write("dest", "existing2")

            # Add source messages
            db.write("source", "new1")
            db.write("source", "new2")

            # Transfer a message
            result = db.transfer("source", "dest")
            assert result is not None
            assert result["body"] == "new1"

            # Dest should have all messages in order
            messages = db.read("dest", peek=True, all_messages=True)
            assert len(messages) == 3
            assert "existing1" in messages
            assert "existing2" in messages
            assert "new1" in messages
