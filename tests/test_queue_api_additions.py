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


def test_queue_str_representation():
    """Test Queue.__str__ method returns queue name."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Test with default database
        queue1 = Queue("tasks")
        assert str(queue1) == "tasks"

        # Test with custom database path
        queue2 = Queue("logs", db_path=db_path)
        assert str(queue2) == "logs"

        # Test with persistent mode
        queue3 = Queue("cache", persistent=True)
        assert str(queue3) == "cache"

        # Test natural string usage
        queue_name = "processing"
        queue4 = Queue(queue_name)
        assert f"Processing {queue4}" == f"Processing {queue_name}"
        assert f"Watching {queue4}..." == f"Watching {queue_name}..."


def test_queue_repr_representation():
    """Test Queue.__repr__ method provides eval-friendly representation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Test minimal case - default db_path, non-persistent
        queue1 = Queue("tasks")
        assert repr(queue1) == "Queue('tasks')"

        # Test with custom db_path
        queue2 = Queue("logs", db_path=db_path)
        expected = f"Queue('logs', db_path='{db_path}')"
        assert repr(queue2) == expected

        # Test with persistent=True
        queue3 = Queue("cache", persistent=True)
        assert repr(queue3) == "Queue('cache', persistent=True)"

        # Test with both custom db_path and persistent=True
        queue4 = Queue("data", db_path=db_path, persistent=True)
        expected = f"Queue('data', db_path='{db_path}', persistent=True)"
        assert repr(queue4) == expected

        # Test with special characters in name and path
        special_queue = Queue("test-queue_123", db_path="/tmp/my db.sqlite")
        expected = "Queue('test-queue_123', db_path='/tmp/my db.sqlite')"
        assert repr(special_queue) == expected


def test_queue_string_representations_in_context():
    """Test string representations work correctly in real usage contexts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with Queue("orders", db_path=db_path, persistent=True) as queue:
            # Test __str__ in f-strings
            log_message = f"Processing {queue} queue"
            assert log_message == "Processing orders queue"

            # Test __repr__ shows full configuration
            debug_repr = repr(queue)
            assert "orders" in debug_repr
            assert db_path in debug_repr
            assert "persistent=True" in debug_repr

            # Test they produce different outputs for configured queues
            assert str(queue) != repr(queue)
            assert str(queue) == "orders"
            assert repr(queue).startswith("Queue(")


def test_queue_string_consistency():
    """Test string representations are consistent across operations."""
    queue = Queue("consistency_test")

    # String representation should be consistent
    str1 = str(queue)
    str2 = str(queue)
    assert str1 == str2 == "consistency_test"

    # Repr should be consistent
    repr1 = repr(queue)
    repr2 = repr(queue)
    assert repr1 == repr2 == "Queue('consistency_test')"

    # Should work after queue operations
    with queue:
        queue.write("test message")
        assert str(queue) == "consistency_test"
        assert repr(queue) == "Queue('consistency_test')"

        queue.read()
        assert str(queue) == "consistency_test"
        assert repr(queue) == "Queue('consistency_test')"
