"""Tests for batch operations with configurable delivery semantics."""

import os
import tempfile

import pytest

from simplebroker.db import BrokerDB


class TestBatchOperations:
    """Test batch claim, peek, and move operations."""

    @pytest.fixture
    def broker(self):
        """Create a temporary broker instance."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            db_path = f.name
        try:
            broker = BrokerDB(db_path)
            yield broker
            broker.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_claim_many_exactly_once(self, broker):
        """Test claim_many with exactly-once delivery (default)."""
        # Add test messages
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        # Claim 3 messages with exactly-once (default)
        messages = broker.claim_many("test_queue", limit=3, with_timestamps=False)
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        # Verify they're gone from the queue
        remaining = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(remaining) == 2
        assert remaining == ["message4", "message5"]

    def test_claim_many_at_least_once(self, broker):
        """Test claim_many with at-least-once delivery."""
        # Add test messages
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        # Claim with at-least-once semantics
        messages = broker.claim_many(
            "test_queue",
            limit=3,
            delivery_guarantee="at_least_once",
            with_timestamps=False,
        )
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        # Verify they're still gone (commit happened after return)
        remaining = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(remaining) == 2
        assert remaining == ["message4", "message5"]

    def test_peek_many(self, broker):
        """Test peek_many doesn't remove messages."""
        # Add test messages
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        # Peek at 3 messages
        messages = broker.peek_many("test_queue", limit=3, with_timestamps=False)
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        # Verify all messages still in queue
        all_messages = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(all_messages) == 5
        assert all_messages == [
            "message1",
            "message2",
            "message3",
            "message4",
            "message5",
        ]

    def test_move_many_exactly_once(self, broker):
        """Test move_many with exactly-once delivery."""
        # Add test messages
        for i in range(5):
            broker.write("source_queue", f"message{i + 1}")

        # Move 3 messages with exactly-once (default)
        messages = broker.move_many(
            "source_queue", "dest_queue", limit=3, with_timestamps=False
        )
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        # Verify source has 2 left
        source_remaining = broker.peek_many(
            "source_queue", limit=10, with_timestamps=False
        )
        assert len(source_remaining) == 2
        assert source_remaining == ["message4", "message5"]

        # Verify dest has 3
        dest_messages = broker.peek_many("dest_queue", limit=10, with_timestamps=False)
        assert len(dest_messages) == 3
        assert dest_messages == ["message1", "message2", "message3"]

    def test_move_many_at_least_once(self, broker):
        """Test move_many with at-least-once delivery."""
        # Add test messages
        for i in range(5):
            broker.write("source_queue", f"message{i + 1}")

        # Move with at-least-once semantics
        messages = broker.move_many(
            "source_queue",
            "dest_queue",
            limit=3,
            delivery_guarantee="at_least_once",
            with_timestamps=False,
        )
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        # Verify source has 2 left
        source_remaining = broker.peek_many(
            "source_queue", limit=10, with_timestamps=False
        )
        assert len(source_remaining) == 2

        # Verify dest has 3
        dest_messages = broker.peek_many("dest_queue", limit=10, with_timestamps=False)
        assert len(dest_messages) == 3

    def test_batch_with_since_timestamp(self, broker):
        """Test batch operations with since_timestamp filter."""
        # Add messages and track timestamps
        broker.write("test_queue", "old_message1")
        broker.write("test_queue", "old_message2")

        # Get timestamp of second message using peek_generator
        messages_with_ts = []
        for body, ts in broker.peek_generator("test_queue", with_timestamps=True):
            messages_with_ts.append((body, ts))
            if len(messages_with_ts) >= 2:
                break

        cutoff_ts = messages_with_ts[1][1]  # Timestamp of second message

        # Add more messages
        broker.write("test_queue", "new_message1")
        broker.write("test_queue", "new_message2")

        # Claim only messages after cutoff
        new_messages = broker.claim_many(
            "test_queue", limit=10, since_timestamp=cutoff_ts, with_timestamps=False
        )
        assert len(new_messages) == 2
        assert new_messages == ["new_message1", "new_message2"]

        # Old messages should still be there
        remaining = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(remaining) == 2
        assert remaining == ["old_message1", "old_message2"]

    def test_limit_validation(self, broker):
        """Test that invalid limits are rejected."""
        with pytest.raises(ValueError, match="limit must be at least 1"):
            broker.claim_many("test_queue", limit=0)

        with pytest.raises(ValueError, match="limit must be at least 1"):
            broker.peek_many("test_queue", limit=-1)

        with pytest.raises(ValueError, match="limit must be at least 1"):
            broker.move_many("source", "dest", limit=0)

    def test_empty_queue_behavior(self, broker):
        """Test batch operations on empty queues."""
        # All should return empty lists
        assert broker.claim_many("empty_queue", limit=10, with_timestamps=False) == []
        assert broker.peek_many("empty_queue", limit=10, with_timestamps=False) == []
        assert (
            broker.move_many(
                "empty_source", "empty_dest", limit=10, with_timestamps=False
            )
            == []
        )

    def test_exact_timestamp_with_claim_one(self, broker):
        """Test that exact_timestamp works with claim_one."""
        # Add messages
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")
        broker.write("test_queue", "message3")

        # Get timestamp of second message using peek_generator
        messages_with_ts = []
        for body, ts in broker.peek_generator("test_queue", with_timestamps=True):
            messages_with_ts.append((body, ts))
            if len(messages_with_ts) >= 3:
                break

        target_ts = messages_with_ts[1][1]  # Timestamp of "message2"

        # Claim with exact timestamp should only get the specific message
        message = broker.claim_one(
            "test_queue", exact_timestamp=target_ts, with_timestamps=False
        )
        assert message == "message2"

        # Verify message1 and message3 still there
        remaining = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(remaining) == 2
        assert remaining == ["message1", "message3"]

    def test_batch_with_timestamps_default(self, broker):
        """Test that batch operations return timestamps by default (with_timestamps=True)."""
        # Add test messages
        for i in range(3):
            broker.write("test_queue", f"message{i + 1}")

        # Test claim_many returns (body, timestamp) tuples by default
        claimed = broker.claim_many("test_queue", limit=2)
        assert len(claimed) == 2
        assert isinstance(claimed[0], tuple)
        assert len(claimed[0]) == 2
        assert claimed[0][0] == "message1"  # body
        assert isinstance(claimed[0][1], int)  # timestamp

        # Test peek_many returns (body, timestamp) tuples by default
        peeked = broker.peek_many("test_queue", limit=10)
        assert len(peeked) == 1
        assert isinstance(peeked[0], tuple)
        assert len(peeked[0]) == 2
        assert peeked[0][0] == "message3"  # body
        assert isinstance(peeked[0][1], int)  # timestamp

        # Test move_many returns (body, timestamp) tuples by default
        broker.write("source_queue", "move_message")
        moved = broker.move_many("source_queue", "dest_queue", limit=1)
        assert len(moved) == 1
        assert isinstance(moved[0], tuple)
        assert len(moved[0]) == 2
        assert moved[0][0] == "move_message"  # body
        assert isinstance(moved[0][1], int)  # timestamp
