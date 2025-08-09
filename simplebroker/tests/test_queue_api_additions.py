"""Test the new Queue API additions (delete and move methods)."""

import tempfile
from pathlib import Path

import pytest

from simplebroker import Queue


def test_queue_delete_all():
    """Test deleting all messages in a queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("test", db_path=db_path) as q:
            # Write some messages
            q.write("message1")
            q.write("message2")
            q.write("message3")

            # Verify they exist
            assert q.peek() == "message1"

            # Delete all messages
            result = q.delete()
            assert result is True

            # Verify queue is empty
            assert q.peek() is None
            assert q.read() is None


def test_queue_delete_by_id():
    """Test deleting a specific message by ID."""
    # This test shows that delete(message_id=X) works, but we can't easily
    # test it without exposing timestamps in the Queue API.
    # For now, we'll test that the method exists and accepts the parameter.
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("test", db_path=db_path) as q:
            q.write("message1")

            # We can't easily get the timestamp without extending the Queue API
            # Just verify the method exists and returns False for non-existent ID
            result = q.delete(message_id=99999999999999999)
            assert result is False

            # Verify message still exists
            assert q.read() == "message1"


def test_queue_move_all():
    """Test moving all messages between queues."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("source", db_path=db_path) as src:
            # Write messages to source
            src.write("message1")
            src.write("message2")
            src.write("message3")

            # Move all to destination
            moved = list(src.move("destination", all_messages=True))
            assert len(moved) == 3

            # Verify source is empty
            assert src.read() is None

        # Verify destination has all messages
        with Queue("destination", db_path=db_path) as dst:
            messages = list(dst.read(all_messages=True))
            assert messages == ["message1", "message2", "message3"]


def test_queue_move_single_message():
    """Test moving a specific message by ID."""
    # Similar to delete, we can't easily test message_id without exposing timestamps
    # Test that the method exists and handles invalid IDs
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("source", db_path=db_path) as src:
            src.write("message1")
            src.write("message2")

            # Try to move non-existent message
            moved = src.move("destination", message_id=99999999999999999)
            assert moved is None

            # Verify messages still in source
            messages = list(src.read(all_messages=True))
            assert len(messages) == 2


def test_queue_move_since_timestamp():
    """Test moving messages newer than a timestamp."""
    # Test basic functionality - since_timestamp=0 should move all
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("source", db_path=db_path) as src:
            src.write("message1")
            src.write("message2")
            src.write("message3")

            # Move all messages (since timestamp 0) - need to use all_messages=True with since_timestamp
            moved = list(src.move("destination", since_timestamp=0, all_messages=True))
            assert len(moved) == 3

            # Verify source is empty
            assert src.read() is None

        # Verify destination has all messages
        with Queue("destination", db_path=db_path) as dst:
            messages = list(dst.read(all_messages=True))
            assert messages == ["message1", "message2", "message3"]


def test_queue_move_validation():
    """Test move method validation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("test", db_path=db_path) as q:
            q.write("message")

            # Cannot move to same queue
            with pytest.raises(ValueError, match="cannot be the same"):
                q.move("test")

            # Cannot use message_id with all_messages
            with pytest.raises(ValueError, match="cannot be used with"):
                q.move("other", message_id=123, all_messages=True)

            # Cannot use message_id with since_timestamp
            with pytest.raises(ValueError, match="cannot be used with"):
                q.move("other", message_id=123, since_timestamp=456)


def test_queue_move_with_queue_instance():
    """Test moving to a Queue instance instead of string."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("source", db_path=db_path) as src:
            src.write("message1")
            src.write("message2")

            with Queue("destination", db_path=db_path) as dst:
                # Move using Queue instance
                moved = list(src.move(dst, all_messages=True))
                assert len(moved) == 2

                # Verify messages moved
                assert src.read() is None
                messages = list(dst.read(all_messages=True))
                assert messages == ["message1", "message2"]
