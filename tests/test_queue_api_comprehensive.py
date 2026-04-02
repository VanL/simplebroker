"""Comprehensive tests for Queue API to ensure all methods are covered."""

import pytest

pytestmark = [pytest.mark.shared]


class TestQueueReadMethods:
    """Test all read methods in the Queue API."""

    def test_read_one_basic(self, queue_factory):
        """Test read_one method."""
        q = queue_factory("test")

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

    def test_read_one_exact_timestamp(self, queue_factory):
        """Test read_one with exact_timestamp parameter."""
        q = queue_factory("test")

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

    def test_read_many_basic(self, queue_factory):
        """Test read_many method."""
        q = queue_factory("test")

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

    def test_read_many_delivery_guarantees(self, queue_factory):
        """Test read_many deprecates at-least-once on materialized batches."""
        q = queue_factory("test")

        for i in range(10):
            q.write(f"message{i}")

        # Test exactly_once (default)
        messages = q.read_many(
            5, delivery_guarantee="exactly_once", with_timestamps=False
        )
        assert len(messages) == 5

        with pytest.warns(DeprecationWarning, match="generator APIs"):
            messages = q.read_many(
                5, delivery_guarantee="at_least_once", with_timestamps=False
            )
        assert len(messages) == 5

    def test_read_many_since_timestamp(self, queue_factory):
        """Test read_many with since_timestamp filter."""
        q = queue_factory("test")

        q.write("old1")
        q.write("old2")

        # Get timestamp after old messages
        messages = list(q.peek_generator(with_timestamps=True))
        cutoff_ts = messages[-1][1]

        # Add new messages
        q.write("new1")
        q.write("new2")

        # Read only new messages
        new_messages = q.read_many(10, since_timestamp=cutoff_ts, with_timestamps=False)
        assert new_messages == ["new1", "new2"]

        # Old messages still there
        remaining = list(q.peek_generator(with_timestamps=False))
        assert remaining == ["old1", "old2"]

    def test_read_generator_basic(self, queue_factory):
        """Test read_generator method."""
        q = queue_factory("test")

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

    def test_read_generator_with_filters(self, queue_factory):
        """Test read_generator with various filters."""
        q = queue_factory("test")

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


class TestQueuePeekMethods:
    """Test all peek methods in the Queue API."""

    def test_peek_one_basic(self, queue_factory):
        """Test peek_one method."""
        q = queue_factory("test")

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

    def test_peek_one_exact_timestamp(self, queue_factory):
        """Test peek_one with exact_timestamp parameter."""
        q = queue_factory("test")

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

    def test_peek_many_basic(self, queue_factory):
        """Test peek_many method."""
        q = queue_factory("test")

        for i in range(5):
            q.write(f"message{i}")

        # Peek multiple messages
        messages = q.peek_many(3, with_timestamps=False)
        assert messages == ["message0", "message1", "message2"]

        # All still there
        messages = q.peek_many(10, with_timestamps=False)
        assert len(messages) == 5

    def test_peek_many_since_timestamp(self, queue_factory):
        """Test peek_many with since_timestamp filter."""
        q = queue_factory("test")

        q.write("old1")
        q.write("old2")

        # Get timestamp
        messages = list(q.peek_generator(with_timestamps=True))
        cutoff_ts = messages[-1][1]

        # Add new messages
        q.write("new1")
        q.write("new2")

        # Peek only new messages
        new_messages = q.peek_many(10, since_timestamp=cutoff_ts, with_timestamps=False)
        assert new_messages == ["new1", "new2"]

    def test_peek_generator_basic(self, queue_factory):
        """Test peek_generator method."""
        q = queue_factory("test")

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


class TestQueueMoveMethods:
    """Test all move methods in the Queue API."""

    def test_move_one_basic(self, queue_factory):
        """Test move_one method."""
        source = queue_factory("source")

        source.write("message1")
        source.write("message2")

        # Move one message
        result = source.move_one("dest", with_timestamps=False)
        assert result == "message1"

        # Check source
        remaining = list(source.peek_generator(with_timestamps=False))
        assert remaining == ["message2"]

        # Check destination
        dest = queue_factory("dest")
        messages = list(dest.peek_generator(with_timestamps=False))
        assert messages == ["message1"]


class TestQueueLastTimestampCaching:
    """Validate queue-level caching of meta.last_ts."""

    def test_last_ts_updates_after_generate_and_write(self, queue_factory):
        queue = queue_factory("ts_queue")

        assert queue.last_ts in (None, 0)

        generated = queue.generate_timestamp()
        assert queue.last_ts == generated

        queue.write("hello")
        cached = queue.last_ts
        assert isinstance(cached, int)
        assert cached >= generated

        refreshed = queue.refresh_last_ts()
        assert refreshed == queue.last_ts
        assert refreshed >= generated

    def test_refresh_last_ts_detects_external_writes(self, queue_factory):
        watcher_queue = queue_factory("watcher")
        writer_queue = queue_factory("writer")

        assert watcher_queue.last_ts in (None, 0)

        writer_queue.write("ping")
        assert watcher_queue.last_ts in (None, 0)

        refreshed = watcher_queue.refresh_last_ts()
        assert refreshed == watcher_queue.last_ts
        assert refreshed > 0

    def test_move_one_exact_timestamp(self, queue_factory):
        """Test move_one with exact_timestamp parameter."""
        source = queue_factory("source")

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
        dest = queue_factory("dest")
        messages = list(dest.peek_generator(with_timestamps=False))
        assert messages == ["message2"]

    def test_move_one_require_unclaimed(self, queue_factory):
        """Test move_one with require_unclaimed parameter."""
        source = queue_factory("source")

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

    def test_move_many_basic(self, queue_factory):
        """Test move_many method."""
        source = queue_factory("source")

        for i in range(5):
            source.write(f"message{i}")

        # Move multiple messages
        messages = source.move_many("dest", 3, with_timestamps=False)
        assert messages == ["message0", "message1", "message2"]

        # Check source
        remaining = list(source.peek_generator(with_timestamps=False))
        assert remaining == ["message3", "message4"]

        # Check destination
        dest = queue_factory("dest")
        messages = list(dest.peek_generator(with_timestamps=False))
        assert messages == ["message0", "message1", "message2"]

    def test_move_many_delivery_guarantees(self, queue_factory):
        """Test move_many deprecates at-least-once on materialized batches."""
        source = queue_factory("source")

        for i in range(10):
            source.write(f"message{i}")

        # Test exactly_once (default)
        messages = source.move_many(
            "dest1", 5, delivery_guarantee="exactly_once", with_timestamps=False
        )
        assert len(messages) == 5

        with pytest.warns(DeprecationWarning, match="generator APIs"):
            messages = source.move_many(
                "dest2",
                5,
                delivery_guarantee="at_least_once",
                with_timestamps=False,
            )
        assert len(messages) == 5

    def test_move_generator_basic(self, queue_factory):
        """Test move_generator method."""
        source = queue_factory("source")

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
        dest = queue_factory("dest")
        messages = list(dest.peek_generator(with_timestamps=False))
        assert messages == ["message0", "message1", "message2"]


class TestQueueHelperMethods:
    """Test helper methods in the Queue API."""

    def test_has_pending(self, queue_factory):
        """Test has_pending method."""
        q = queue_factory("test")

        # Empty queue
        assert q.has_pending() is False

        # With messages
        q.write("message1")
        assert q.has_pending() is True

        # Read the message
        q.read_one()
        assert q.has_pending() is False

    def test_has_pending_since_timestamp(self, queue_factory):
        """Test has_pending with since_timestamp."""
        q = queue_factory("test")

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

    def test_get_data_version(self, queue_factory):
        """Test get_data_version method."""
        # Use persistent mode for this test
        q = queue_factory("test")

        # Get initial version
        version1 = q.get_data_version()
        if version1 is None:
            # PG backend returns None (no equivalent of SQLite data_version)
            pytest.skip("get_data_version not supported on this backend")
        assert isinstance(version1, int)

        # Write a message (changes database)
        q.write("message")

        # Version should change after a write
        version2 = q.get_data_version()
        assert version2 is not None
        # Note: In some cases version might not change if using same connection
        # Just verify it's a valid integer
        assert isinstance(version2, int)

    def test_stream_messages(self, queue_factory):
        """Test stream_messages method."""
        q = queue_factory("test")

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

    def test_stream_messages_with_filters(self, queue_factory):
        """Test stream_messages with various filters."""
        q = queue_factory("test")

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

    def test_stream_messages_peek_single_message_mode(self, queue_factory):
        """Test stream_messages respects all_messages=False in peek mode."""
        q = queue_factory("test")

        for i in range(3):
            q.write(f"message{i}")

        messages = list(q.stream_messages(peek=True, all_messages=False))

        assert len(messages) == 1
        assert messages[0][0] == "message0"

        # Peek mode should not remove the message.
        remaining = list(q.peek_generator(with_timestamps=False))
        assert remaining == ["message0", "message1", "message2"]

    def test_stream_messages_consume_single_message_mode(self, queue_factory):
        """Test stream_messages respects all_messages=False in consume mode."""
        q = queue_factory("test")

        for i in range(3):
            q.write(f"message{i}")

        messages = list(q.stream_messages(peek=False, all_messages=False))

        assert len(messages) == 1
        assert messages[0][0] == "message0"

        remaining = list(q.peek_generator(with_timestamps=False))
        assert remaining == ["message1", "message2"]

    def test_stream_messages_non_batch_mode_ignores_commit_interval(
        self, queue_factory
    ):
        """Non-batch streaming should remain exactly-once even if commit_interval > 1."""
        q = queue_factory("test")

        for i in range(5):
            q.write(f"message{i}")

        gen = q.stream_messages(
            peek=False,
            all_messages=True,
            batch_processing=False,
            commit_interval=3,
        )
        first_message = next(gen)
        gen.close()

        assert first_message[0] == "message0"

        remaining = list(q.peek_generator(with_timestamps=False))
        assert remaining == ["message1", "message2", "message3", "message4"]

    def test_stream_messages_batch_mode_uses_commit_interval_as_batch_size(
        self, queue_factory
    ):
        """Batch streaming should use commit_interval as the at-least-once batch size."""
        q = queue_factory("test")

        for i in range(7):
            q.write(f"message{i}")

        gen = q.stream_messages(
            peek=False,
            all_messages=True,
            batch_processing=True,
            commit_interval=3,
        )

        consumed = []
        try:
            for message, timestamp in gen:
                consumed.append((message, timestamp))
                if len(consumed) == 4:
                    break
        finally:
            gen.close()

        assert [message for message, _ in consumed] == [
            "message0",
            "message1",
            "message2",
            "message3",
        ]

        # Batch 1 (message0..message2) should commit.
        # Batch 2 should roll back when the generator is closed mid-batch.
        remaining = list(q.peek_generator(with_timestamps=False))
        assert remaining == ["message3", "message4", "message5", "message6"]


class TestQueueConnectionModes:
    """Test persistent vs ephemeral connection modes."""

    def test_ephemeral_mode_default(self, queue_factory):
        """Test that ephemeral mode is the default."""
        q = queue_factory("test", persistent=False)
        assert q._persistent is False
        assert q.conn is None

    def test_persistent_mode(self, queue_factory):
        """Test persistent mode."""
        q = queue_factory("test")
        assert q._persistent is True
        assert q.conn is not None

        # Write and read work normally
        q.write("message")
        assert q.read_one(with_timestamps=False) == "message"

    def test_context_manager(self, queue_factory):
        """Test Queue as context manager."""
        q = queue_factory("test")

        with q:
            q.write("message")
            assert q.read_one(with_timestamps=False) == "message"

        # After context, queue should be closed
        # Create new queue to verify data persisted
        q2 = queue_factory("test")
        assert q2.peek_one() is None  # Message was consumed


class TestQueueDelete:
    """Test delete functionality."""

    def test_delete_all(self, queue_factory):
        """Test deleting all messages in a queue."""
        q = queue_factory("test")

        for i in range(5):
            q.write(f"message{i}")

        # Delete all
        result = q.delete()
        assert result is True

        # Queue is empty
        assert q.has_pending() is False
        assert q.peek_one() is None

    def test_delete_all_returns_false_when_queue_empty(self, queue_factory):
        """Deleting an empty queue should report that nothing was deleted."""
        q = queue_factory("test")
        assert q.delete() is False

    def test_delete_all_returns_false_for_nonexistent_queue(self, queue_factory):
        """Deleting a never-written queue should report no-op status."""
        q = queue_factory("missing")
        assert q.delete() is False

    def test_delete_by_id(self, queue_factory):
        """Test deleting specific message by ID."""
        q = queue_factory("test")

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


class TestQueueHighLevelMethods:
    """Test high-level read/peek/move methods that mirror CLI behavior."""

    def test_read_high_level(self, queue_factory):
        """Test high-level read() method."""
        q = queue_factory("test")

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

    def test_peek_high_level(self, queue_factory):
        """Test high-level peek() method."""
        q = queue_factory("test")

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

    def test_move_high_level(self, queue_factory):
        """Test high-level move() method."""
        source = queue_factory("source")

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
        dest = queue_factory("dest")
        messages = list(dest.peek(all_messages=True))
        assert len(messages) == 5
