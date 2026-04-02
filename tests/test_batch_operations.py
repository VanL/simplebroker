"""Tests for batch operations with configurable delivery semantics."""

import pytest

pytestmark = [pytest.mark.shared]


class TestBatchOperations:
    """Test batch claim, peek, and move operations."""

    def test_claim_many_exactly_once(self, broker):
        """Test claim_many with exactly-once delivery (default)."""
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        messages = broker.claim_many("test_queue", limit=3, with_timestamps=False)
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        remaining = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(remaining) == 2
        assert remaining == ["message4", "message5"]

    def test_claim_many_at_least_once(self, broker):
        """Test claim_many deprecates at-least-once on list-returning APIs."""
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        with pytest.warns(DeprecationWarning, match="generator APIs"):
            messages = broker.claim_many(
                "test_queue",
                limit=3,
                delivery_guarantee="at_least_once",
                with_timestamps=False,
            )
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        remaining = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(remaining) == 2
        assert remaining == ["message4", "message5"]

    def test_peek_many(self, broker):
        """Test peek_many doesn't remove messages."""
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        messages = broker.peek_many("test_queue", limit=3, with_timestamps=False)
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

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
        for i in range(5):
            broker.write("source_queue", f"message{i + 1}")

        messages = broker.move_many(
            "source_queue", "dest_queue", limit=3, with_timestamps=False
        )
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        source_remaining = broker.peek_many(
            "source_queue", limit=10, with_timestamps=False
        )
        assert len(source_remaining) == 2
        assert source_remaining == ["message4", "message5"]

        dest_messages = broker.peek_many("dest_queue", limit=10, with_timestamps=False)
        assert len(dest_messages) == 3
        assert dest_messages == ["message1", "message2", "message3"]

    def test_move_many_at_least_once(self, broker):
        """Test move_many deprecates at-least-once on list-returning APIs."""
        for i in range(5):
            broker.write("source_queue", f"message{i + 1}")

        with pytest.warns(DeprecationWarning, match="generator APIs"):
            messages = broker.move_many(
                "source_queue",
                "dest_queue",
                limit=3,
                delivery_guarantee="at_least_once",
                with_timestamps=False,
            )
        assert len(messages) == 3
        assert messages == ["message1", "message2", "message3"]

        source_remaining = broker.peek_many(
            "source_queue", limit=10, with_timestamps=False
        )
        assert len(source_remaining) == 2

        dest_messages = broker.peek_many("dest_queue", limit=10, with_timestamps=False)
        assert len(dest_messages) == 3

    def test_batch_with_since_timestamp(self, broker):
        """Test batch operations with since_timestamp filter."""
        broker.write("test_queue", "old_message1")
        broker.write("test_queue", "old_message2")

        messages_with_ts = []
        for body, ts in broker.peek_generator("test_queue", with_timestamps=True):
            messages_with_ts.append((body, ts))
            if len(messages_with_ts) >= 2:
                break

        cutoff_ts = messages_with_ts[1][1]

        broker.write("test_queue", "new_message1")
        broker.write("test_queue", "new_message2")

        new_messages = broker.claim_many(
            "test_queue", limit=10, since_timestamp=cutoff_ts, with_timestamps=False
        )
        assert len(new_messages) == 2
        assert new_messages == ["new_message1", "new_message2"]

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
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")
        broker.write("test_queue", "message3")

        messages_with_ts = []
        for body, ts in broker.peek_generator("test_queue", with_timestamps=True):
            messages_with_ts.append((body, ts))
            if len(messages_with_ts) >= 3:
                break

        target_ts = messages_with_ts[1][1]

        message = broker.claim_one(
            "test_queue", exact_timestamp=target_ts, with_timestamps=False
        )
        assert message == "message2"

        remaining = broker.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(remaining) == 2
        assert remaining == ["message1", "message3"]

    def test_batch_with_timestamps_default(self, broker):
        """Test that batch operations return timestamps by default (with_timestamps=True)."""
        for i in range(3):
            broker.write("test_queue", f"message{i + 1}")

        claimed = broker.claim_many("test_queue", limit=2)
        assert len(claimed) == 2
        assert isinstance(claimed[0], tuple)
        assert len(claimed[0]) == 2
        assert claimed[0][0] == "message1"
        assert isinstance(claimed[0][1], int)

        peeked = broker.peek_many("test_queue", limit=10)
        assert len(peeked) == 1
        assert isinstance(peeked[0], tuple)
        assert len(peeked[0]) == 2
        assert peeked[0][0] == "message3"
        assert isinstance(peeked[0][1], int)

        broker.write("source_queue", "move_message")
        moved = broker.move_many("source_queue", "dest_queue", limit=1)
        assert len(moved) == 1
        assert isinstance(moved[0], tuple)
        assert len(moved[0]) == 2
        assert moved[0][0] == "move_message"
        assert isinstance(moved[0][1], int)
