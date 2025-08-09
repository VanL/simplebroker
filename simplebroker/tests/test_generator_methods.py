"""Tests for generator methods with configurable delivery semantics."""

import os
import tempfile

import pytest

from simplebroker.db import BrokerDB


class TestGeneratorMethods:
    """Test claim_generator, peek_generator, and move_generator."""

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

    def test_claim_generator_exactly_once(self, broker):
        """Test claim_generator with exactly-once delivery (default)."""
        # Add test messages
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        # Claim with exactly-once (default)
        messages = []
        for msg in broker.claim_generator("test_queue", with_timestamps=False):
            messages.append(msg)
            if len(messages) >= 3:
                break  # Take only first 3

        assert messages == ["message1", "message2", "message3"]

        # Verify they're gone from the queue
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["message4", "message5"]

    def test_claim_generator_at_least_once(self, broker):
        """Test claim_generator with at-least-once delivery."""
        # Add test messages
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        # Claim with at-least-once semantics
        messages = []
        gen = broker.claim_generator(
            "test_queue",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            batch_size=2,
        )
        for msg in gen:
            messages.append(msg)
            if len(messages) >= 3:
                break

        assert messages == ["message1", "message2", "message3"]

        # Complete the generator to ensure commits happen
        for _ in gen:
            pass

        # Verify processed messages are gone
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == []  # All should be claimed now

    def test_peek_generator(self, broker):
        """Test peek_generator doesn't remove messages."""
        # Add test messages
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        # Peek at messages
        messages = []
        for msg in broker.peek_generator("test_queue", with_timestamps=False):
            messages.append(msg)
            if len(messages) >= 3:
                break

        assert messages == ["message1", "message2", "message3"]

        # Verify all messages still in queue
        all_messages = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert all_messages == [
            "message1",
            "message2",
            "message3",
            "message4",
            "message5",
        ]

    def test_move_generator_exactly_once(self, broker):
        """Test move_generator with exactly-once delivery."""
        # Add test messages
        for i in range(5):
            broker.write("source_queue", f"message{i + 1}")

        # Move with exactly-once (default)
        messages = []
        for msg in broker.move_generator(
            "source_queue", "dest_queue", with_timestamps=False
        ):
            messages.append(msg)
            if len(messages) >= 3:
                break

        assert messages == ["message1", "message2", "message3"]

        # Verify source has 2 left
        source_remaining = list(
            broker.peek_generator("source_queue", with_timestamps=False)
        )
        assert source_remaining == ["message4", "message5"]

        # Verify dest has 3
        dest_messages = list(broker.peek_generator("dest_queue", with_timestamps=False))
        assert dest_messages == ["message1", "message2", "message3"]

    def test_generator_with_timestamps(self, broker):
        """Test generators return timestamps when requested."""
        # Add test messages
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")

        # Get with timestamps (default)
        results = []
        for item in broker.peek_generator("test_queue"):
            results.append(item)
            if len(results) >= 2:
                break

        # Should be tuples
        assert len(results) == 2
        assert isinstance(results[0], tuple)
        assert results[0][0] == "message1"
        assert isinstance(results[0][1], int)  # timestamp

        # Without timestamps
        messages = []
        for msg in broker.peek_generator("test_queue", with_timestamps=False):
            messages.append(msg)
            if len(messages) >= 2:
                break

        assert messages == ["message1", "message2"]

    def test_generator_with_since_timestamp(self, broker):
        """Test generators with since_timestamp filter."""
        # Add messages
        broker.write("test_queue", "old_message1")
        broker.write("test_queue", "old_message2")

        # Get timestamp of second message
        timestamps = []
        for _, ts in broker.peek_generator("test_queue"):
            timestamps.append(ts)
            if len(timestamps) >= 2:
                break

        cutoff_ts = timestamps[1]

        # Add more messages
        broker.write("test_queue", "new_message1")
        broker.write("test_queue", "new_message2")

        # Claim only messages after cutoff
        new_messages = list(
            broker.claim_generator(
                "test_queue", with_timestamps=False, since_timestamp=cutoff_ts
            )
        )

        assert new_messages == ["new_message1", "new_message2"]

        # Old messages should still be there
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["old_message1", "old_message2"]

    def test_empty_queue_generator(self, broker):
        """Test generators on empty queues."""
        # All should yield nothing
        assert list(broker.claim_generator("empty_queue", with_timestamps=False)) == []
        assert list(broker.peek_generator("empty_queue", with_timestamps=False)) == []
        assert (
            list(
                broker.move_generator(
                    "empty_source", "empty_dest", with_timestamps=False
                )
            )
            == []
        )

    def test_generator_early_termination(self, broker):
        """Test that generators handle early termination gracefully."""
        # Add many messages
        for i in range(100):
            broker.write("test_queue", f"message{i + 1}")

        # Start claiming but stop early
        gen = broker.claim_generator("test_queue", with_timestamps=False)
        first_five = []
        for msg in gen:
            first_five.append(msg)
            if len(first_five) >= 5:
                break  # Stop early

        assert first_five == [
            "message1",
            "message2",
            "message3",
            "message4",
            "message5",
        ]

        # Close generator without consuming all
        gen.close()

        # Remaining messages should still be there
        remaining_count = len(
            list(broker.peek_generator("test_queue", with_timestamps=False))
        )
        assert remaining_count == 95  # 100 - 5 claimed

    def test_generator_exact_timestamp(self, broker):
        """Test generators with exact_timestamp parameter."""
        # Add messages
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")
        broker.write("test_queue", "message3")

        # Get timestamp of second message
        timestamps = []
        for body, ts in broker.peek_generator("test_queue"):
            timestamps.append((body, ts))
            if len(timestamps) >= 3:
                break

        target_ts = timestamps[1][1]  # Timestamp of message2

        # Claim with exact timestamp
        messages = list(
            broker.claim_generator(
                "test_queue", with_timestamps=False, exact_timestamp=target_ts
            )
        )

        assert messages == ["message2"]

        # Verify message1 and message3 still there
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["message1", "message3"]

    def test_move_generator_queue_validation(self, broker):
        """Test move_generator validates queue names."""
        broker.write("test_queue", "message")

        # Should raise error for same source and target
        with pytest.raises(
            ValueError, match="Source and target queues cannot be the same"
        ):
            list(broker.move_generator("test_queue", "test_queue"))
