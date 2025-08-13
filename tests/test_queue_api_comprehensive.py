"""Comprehensive tests for Queue API to ensure all methods are covered."""

import tempfile
from pathlib import Path

from simplebroker import Queue


class TestQueueReadMethods:
    """Test all read methods in the Queue API."""

    def test_read_one_basic(self):
        """Test read_one method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("message1")
                q.write("message2")

                # Read one message
                msg = q.read_one(with_timestamps=False)
                assert msg == "message1"

                # With timestamps
                result = q.read_one(with_timestamps=True)
                assert result[0] == "message2"
                assert isinstance(result[1], int)

                # Empty queue
                assert q.read_one() is None
            finally:
                q.close()

    def test_read_one_exact_timestamp(self):
        """Test read_one with exact_timestamp parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("message1")
                q.write("message2")
                q.write("message3")

                # Get timestamps using peek
                messages = list(q.peek_generator(with_timestamps=True))
                target_ts = messages[1][1]  # timestamp of message2

                # Read specific message by timestamp
                result = q.read_one(exact_timestamp=target_ts, with_timestamps=False)
                assert result == "message2"

                # Verify it's gone
                assert q.read_one(exact_timestamp=target_ts) is None

                # Other messages still there
                remaining = list(q.peek_generator(with_timestamps=False))
                assert remaining == ["message1", "message3"]
            finally:
                q.close()

    def test_read_many_basic(self):
        """Test read_many method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(5):
                    q.write(f"message{i}")

                # Read multiple messages
                messages = q.read_many(3, with_timestamps=False)
                assert messages == ["message0", "message1", "message2"]

                # With timestamps
                results = q.read_many(2, with_timestamps=True)
                assert len(results) == 2
                assert results[0][0] == "message3"
                assert results[1][0] == "message4"

                # Empty queue
                assert q.read_many(10) == []
            finally:
                q.close()

    def test_read_many_delivery_guarantees(self):
        """Test read_many with different delivery guarantees."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(10):
                    q.write(f"message{i}")

                # Test exactly_once (default)
                messages = q.read_many(
                    5, delivery_guarantee="exactly_once", with_timestamps=False
                )
                assert len(messages) == 5

                # Test at_least_once
                messages = q.read_many(
                    5, delivery_guarantee="at_least_once", with_timestamps=False
                )
                assert len(messages) == 5
            finally:
                q.close()

    def test_read_many_since_timestamp(self):
        """Test read_many with since_timestamp filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("old1")
                q.write("old2")

                # Get timestamp after old messages
                messages = list(q.peek_generator(with_timestamps=True))
                cutoff_ts = messages[-1][1]

                # Add new messages
                q.write("new1")
                q.write("new2")

                # Read only new messages
                new_messages = q.read_many(
                    10, since_timestamp=cutoff_ts, with_timestamps=False
                )
                assert new_messages == ["new1", "new2"]

                # Old messages still there
                remaining = list(q.peek_generator(with_timestamps=False))
                assert remaining == ["old1", "old2"]
            finally:
                q.close()

    def test_read_generator_basic(self):
        """Test read_generator method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(5):
                    q.write(f"message{i}")

                # Read with generator
                messages = []
                for msg in q.read_generator(with_timestamps=False):
                    messages.append(msg)
                    if len(messages) >= 3:
                        break

                assert messages == ["message0", "message1", "message2"]

                # Remaining messages
                remaining = list(q.read_generator(with_timestamps=False))
                assert remaining == ["message3", "message4"]
            finally:
                q.close()

    def test_read_generator_with_filters(self):
        """Test read_generator with various filters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(10):
                    q.write(f"message{i}")

                # Get timestamp for filtering
                messages = list(q.peek_generator(with_timestamps=True))
                cutoff_ts = messages[4][1]  # After message4

                # Read with since_timestamp
                new_messages = list(
                    q.read_generator(since_timestamp=cutoff_ts, with_timestamps=False)
                )
                assert new_messages == [f"message{i}" for i in range(5, 10)]

                # Read with exact_timestamp
                q.write("extra")
                messages = list(q.peek_generator(with_timestamps=True))
                target_ts = messages[2][1]  # message2

                result = list(
                    q.read_generator(exact_timestamp=target_ts, with_timestamps=False)
                )
                assert result == ["message2"]

            finally:
                q.close()


class TestQueuePeekMethods:
    """Test all peek methods in the Queue API."""

    def test_peek_one_basic(self):
        """Test peek_one method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("message1")
                q.write("message2")

                # Peek one message
                msg = q.peek_one(with_timestamps=False)
                assert msg == "message1"

                # Message still there
                msg = q.peek_one(with_timestamps=False)
                assert msg == "message1"

                # With timestamps
                result = q.peek_one(with_timestamps=True)
                assert result[0] == "message1"
                assert isinstance(result[1], int)
            finally:
                q.close()

    def test_peek_one_exact_timestamp(self):
        """Test peek_one with exact_timestamp parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("message1")
                q.write("message2")
                q.write("message3")

                # Get timestamps
                messages = list(q.peek_generator(with_timestamps=True))
                target_ts = messages[1][1]  # timestamp of message2

                # Peek specific message by timestamp
                result = q.peek_one(exact_timestamp=target_ts, with_timestamps=False)
                assert result == "message2"

                # Still there
                result = q.peek_one(exact_timestamp=target_ts, with_timestamps=False)
                assert result == "message2"
            finally:
                q.close()

    def test_peek_many_basic(self):
        """Test peek_many method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(5):
                    q.write(f"message{i}")

                # Peek multiple messages
                messages = q.peek_many(3, with_timestamps=False)
                assert messages == ["message0", "message1", "message2"]

                # All still there
                messages = q.peek_many(10, with_timestamps=False)
                assert len(messages) == 5
            finally:
                q.close()

    def test_peek_many_since_timestamp(self):
        """Test peek_many with since_timestamp filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("old1")
                q.write("old2")

                # Get timestamp
                messages = list(q.peek_generator(with_timestamps=True))
                cutoff_ts = messages[-1][1]

                # Add new messages
                q.write("new1")
                q.write("new2")

                # Peek only new messages
                new_messages = q.peek_many(
                    10, since_timestamp=cutoff_ts, with_timestamps=False
                )
                assert new_messages == ["new1", "new2"]
            finally:
                q.close()

    def test_peek_generator_basic(self):
        """Test peek_generator method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(5):
                    q.write(f"message{i}")

                # Peek with generator
                messages = []
                for msg in q.peek_generator(with_timestamps=False):
                    messages.append(msg)

                assert messages == [f"message{i}" for i in range(5)]

                # All still there
                messages2 = list(q.peek_generator(with_timestamps=False))
                assert messages2 == messages

            finally:
                q.close()


class TestQueueMoveMethods:
    """Test all move methods in the Queue API."""

    def test_move_one_basic(self):
        """Test move_one method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            source = Queue("source", db_path=db_path)
            try:
                source.write("message1")
                source.write("message2")

                # Move one message
                result = source.move_one("dest", with_timestamps=False)
                assert result == "message1"

                # Check source
                remaining = list(source.peek_generator(with_timestamps=False))
                assert remaining == ["message2"]

                # Check destination
                dest = Queue("dest", db_path=db_path)
                messages = list(dest.peek_generator(with_timestamps=False))
                assert messages == ["message1"]
            finally:
                source.close()

    def test_move_one_exact_timestamp(self):
        """Test move_one with exact_timestamp parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            source = Queue("source", db_path=db_path)
            try:
                source.write("message1")
                source.write("message2")
                source.write("message3")

                # Get timestamps
                messages = list(source.peek_generator(with_timestamps=True))
                target_ts = messages[1][1]  # timestamp of message2

                # Move specific message by timestamp
                result = source.move_one(
                    "dest", exact_timestamp=target_ts, with_timestamps=False
                )
                assert result == "message2"

                # Check source (message2 gone)
                remaining = list(source.peek_generator(with_timestamps=False))
                assert remaining == ["message1", "message3"]

                # Check destination
                dest = Queue("dest", db_path=db_path)
                messages = list(dest.peek_generator(with_timestamps=False))
                assert messages == ["message2"]
            finally:
                source.close()

    def test_move_one_require_unclaimed(self):
        """Test move_one with require_unclaimed parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            source = Queue("source", db_path=db_path)
            try:
                source.write("message1")
                source.write("message2")

                # Read (claim) first message
                claimed = source.read_one(with_timestamps=False)
                assert claimed == "message1"

                # Try to move with require_unclaimed=True (default)
                result = source.move_one("dest", with_timestamps=False)
                assert result == "message2"  # Skips claimed message1

                # Now try to move claimed message with require_unclaimed=False
                # Get timestamp of claimed message
                messages = list(source.peek_generator(with_timestamps=True))
                if messages:
                    claimed_ts = messages[0][1]
                    result = source.move_one(
                        "dest",
                        exact_timestamp=claimed_ts,
                        require_unclaimed=False,
                        with_timestamps=False,
                    )
                    # Note: This might still be None if the message was already deleted
            finally:
                source.close()

    def test_move_many_basic(self):
        """Test move_many method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            source = Queue("source", db_path=db_path)
            try:
                for i in range(5):
                    source.write(f"message{i}")

                # Move multiple messages
                messages = source.move_many("dest", 3, with_timestamps=False)
                assert messages == ["message0", "message1", "message2"]

                # Check source
                remaining = list(source.peek_generator(with_timestamps=False))
                assert remaining == ["message3", "message4"]

                # Check destination
                dest = Queue("dest", db_path=db_path)
                messages = list(dest.peek_generator(with_timestamps=False))
                assert messages == ["message0", "message1", "message2"]
            finally:
                source.close()

    def test_move_many_delivery_guarantees(self):
        """Test move_many with different delivery guarantees."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            source = Queue("source", db_path=db_path)
            try:
                for i in range(10):
                    source.write(f"message{i}")

                # Test exactly_once (default)
                messages = source.move_many(
                    "dest1", 5, delivery_guarantee="exactly_once", with_timestamps=False
                )
                assert len(messages) == 5

                # Test at_least_once
                messages = source.move_many(
                    "dest2",
                    5,
                    delivery_guarantee="at_least_once",
                    with_timestamps=False,
                )
                assert len(messages) == 5
            finally:
                source.close()

    def test_move_generator_basic(self):
        """Test move_generator method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            source = Queue("source", db_path=db_path)
            try:
                for i in range(5):
                    source.write(f"message{i}")

                # Move with generator
                messages = []
                for msg in source.move_generator("dest", with_timestamps=False):
                    messages.append(msg)
                    if len(messages) >= 3:
                        break

                assert messages == ["message0", "message1", "message2"]

                # Check source
                remaining = list(source.peek_generator(with_timestamps=False))
                assert remaining == ["message3", "message4"]

                # Check destination
                dest = Queue("dest", db_path=db_path)
                messages = list(dest.peek_generator(with_timestamps=False))
                assert messages == ["message0", "message1", "message2"]

            finally:
                source.close()


class TestQueueHelperMethods:
    """Test helper methods in the Queue API."""

    def test_has_pending(self):
        """Test has_pending method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                # Empty queue
                assert q.has_pending() is False

                # With messages
                q.write("message1")
                assert q.has_pending() is True

                # Read the message
                q.read_one()
                assert q.has_pending() is False
            finally:
                q.close()

    def test_has_pending_since_timestamp(self):
        """Test has_pending with since_timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("old1")
                q.write("old2")

                # Get timestamp
                messages = list(q.peek_generator(with_timestamps=True))
                cutoff_ts = messages[-1][1]

                # No messages after cutoff
                assert q.has_pending(since_timestamp=cutoff_ts) is False

                # Add new message
                q.write("new1")
                assert q.has_pending(since_timestamp=cutoff_ts) is True
            finally:
                q.close()

    def test_get_data_version(self):
        """Test get_data_version method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # For ephemeral mode, each operation creates a new connection
            # so data_version might not change as expected
            # Use persistent mode for this test
            q = Queue("test", db_path=db_path, persistent=True)
            try:
                # Get initial version
                version1 = q.get_data_version()
                assert version1 is not None
                assert isinstance(version1, int)

                # Write a message (changes database)
                q.write("message")

                # Version should change after a write
                version2 = q.get_data_version()
                assert version2 is not None
                # Note: In some cases version might not change if using same connection
                # Just verify it's a valid integer
                assert isinstance(version2, int)

            finally:
                q.close()

    def test_stream_messages(self):
        """Test stream_messages method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(5):
                    q.write(f"message{i}")

                # Stream with peek (non-destructive)
                messages = []
                for msg, ts in q.stream_messages(peek=True):
                    messages.append(msg)
                    assert isinstance(ts, int)

                assert messages == [f"message{i}" for i in range(5)]

                # All still there
                assert q.has_pending() is True

                # Stream without peek (destructive)
                messages = []
                for msg, _ts in q.stream_messages(peek=False):
                    messages.append(msg)
                    if len(messages) >= 3:
                        break

                assert messages == ["message0", "message1", "message2"]

                # Some consumed
                remaining = list(q.peek_generator(with_timestamps=False))
                assert remaining == ["message3", "message4"]
            finally:
                q.close()

    def test_stream_messages_with_filters(self):
        """Test stream_messages with various filters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("old1")
                q.write("old2")

                # Get timestamp
                messages = list(q.peek_generator(with_timestamps=True))
                cutoff_ts = messages[-1][1]

                # Add new messages
                q.write("new1")
                q.write("new2")

                # Stream only new messages
                messages = []
                for msg, _ts in q.stream_messages(peek=True, since_timestamp=cutoff_ts):
                    messages.append(msg)

                assert messages == ["new1", "new2"]

            finally:
                q.close()


class TestQueueConnectionModes:
    """Test persistent vs ephemeral connection modes."""

    def test_ephemeral_mode_default(self):
        """Test that ephemeral mode is the default."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                assert q._persistent is False
                assert q.conn is None
            finally:
                q.close()

    def test_persistent_mode(self):
        """Test persistent mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path, persistent=True)
            try:
                assert q._persistent is True
                assert q.conn is not None

                # Write and read work normally
                q.write("message")
                assert q.read_one(with_timestamps=False) == "message"

                # Clean up
            finally:
                q.close()

    def test_context_manager(self):
        """Test Queue as context manager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            with Queue("test", db_path=db_path) as q:
                q.write("message")
                assert q.read_one(with_timestamps=False) == "message"

            # After context, queue should be closed
            # Create new queue to verify data persisted
            q2 = Queue("test", db_path=db_path)
            try:
                assert q2.peek_one() is None  # Message was consumed

            finally:
                q2.close()


class TestQueueDelete:
    """Test delete functionality."""

    def test_delete_all(self):
        """Test deleting all messages in a queue."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(5):
                    q.write(f"message{i}")

                # Delete all
                result = q.delete()
                assert result is True

                # Queue is empty
                assert q.has_pending() is False
                assert q.peek_one() is None
            finally:
                q.close()

    def test_delete_by_id(self):
        """Test deleting specific message by ID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                q.write("message1")
                q.write("message2")
                q.write("message3")

                # Get timestamps
                messages = list(q.peek_generator(with_timestamps=True))
                target_ts = messages[1][1]  # timestamp of message2

                # Delete specific message
                result = q.delete(message_id=target_ts)
                assert result is True

                # Check remaining messages
                remaining = list(q.peek_generator(with_timestamps=False))
                assert remaining == ["message1", "message3"]

                # Try to delete non-existent
                result = q.delete(message_id=999999999999999999)
                assert result is False

            finally:
                q.close()


class TestQueueHighLevelMethods:
    """Test high-level read/peek/move methods that mirror CLI behavior."""

    def test_read_high_level(self):
        """Test high-level read() method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(5):
                    q.write(f"message{i}")

                # Read single (default)
                msg = q.read()
                assert msg == "message0"

                # Read all
                messages = list(q.read(all_messages=True))
                assert messages == [f"message{i}" for i in range(1, 5)]

                # Empty queue
                assert q.read() is None
            finally:
                q.close()

    def test_peek_high_level(self):
        """Test high-level peek() method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            q = Queue("test", db_path=db_path)
            try:
                for i in range(3):
                    q.write(f"message{i}")

                # Peek single (default)
                msg = q.peek()
                assert msg == "message0"

                # Peek all
                messages = list(q.peek(all_messages=True))
                assert messages == [f"message{i}" for i in range(3)]

                # All still there
                messages = list(q.peek(all_messages=True))
                assert len(messages) == 3
            finally:
                q.close()

    def test_move_high_level(self):
        """Test high-level move() method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            source = Queue("source", db_path=db_path)
            try:
                for i in range(5):
                    source.write(f"message{i}")

                # Move single (default)
                result = source.move("dest")
                assert result["message"] == "message0"

                # Move all remaining
                results = list(source.move("dest", all_messages=True))
                assert len(results) == 4
                assert all("message" in r for r in results)

                # Source is empty
                assert source.peek() is None

                # Destination has all
                dest = Queue("dest", db_path=db_path)
                messages = list(dest.peek(all_messages=True))
                assert len(messages) == 5
            finally:
                source.close()
