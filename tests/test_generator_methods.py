"""Tests for generator methods with configurable delivery semantics."""

import threading

import pytest

pytestmark = [pytest.mark.shared]


class TestGeneratorMethods:
    """Test claim_generator, peek_generator, and move_generator."""

    def test_claim_generator_exactly_once(self, broker):
        """Test claim_generator with exactly-once delivery (default)."""
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        messages = []
        for msg in broker.claim_generator("test_queue", with_timestamps=False):
            messages.append(msg)
            if len(messages) >= 3:
                break

        assert messages == ["message1", "message2", "message3"]

        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["message4", "message5"]

    def test_claim_generator_at_least_once(self, broker):
        """Test claim_generator with at-least-once delivery."""
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

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

        for _ in gen:
            pass

        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == []

    def test_peek_generator(self, broker):
        """Test peek_generator doesn't remove messages."""
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        messages = []
        for msg in broker.peek_generator("test_queue", with_timestamps=False):
            messages.append(msg)
            if len(messages) >= 3:
                break

        assert messages == ["message1", "message2", "message3"]

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
        for i in range(5):
            broker.write("source_queue", f"message{i + 1}")

        messages = []
        for msg in broker.move_generator(
            "source_queue", "dest_queue", with_timestamps=False
        ):
            messages.append(msg)
            if len(messages) >= 3:
                break

        assert messages == ["message1", "message2", "message3"]

        source_remaining = list(
            broker.peek_generator("source_queue", with_timestamps=False)
        )
        assert source_remaining == ["message4", "message5"]

        dest_messages = list(broker.peek_generator("dest_queue", with_timestamps=False))
        assert dest_messages == ["message1", "message2", "message3"]

    def test_generator_with_timestamps(self, broker):
        """Test generators return timestamps when requested."""
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")

        results = []
        for item in broker.peek_generator("test_queue"):
            results.append(item)
            if len(results) >= 2:
                break

        assert len(results) == 2
        assert isinstance(results[0], tuple)
        assert results[0][0] == "message1"
        assert isinstance(results[0][1], int)

        messages = []
        for msg in broker.peek_generator("test_queue", with_timestamps=False):
            messages.append(msg)
            if len(messages) >= 2:
                break

        assert messages == ["message1", "message2"]

    def test_generator_with_since_timestamp(self, broker):
        """Test generators with since_timestamp filter."""
        broker.write("test_queue", "old_message1")
        broker.write("test_queue", "old_message2")

        timestamps = []
        for _, ts in broker.peek_generator("test_queue"):
            timestamps.append(ts)
            if len(timestamps) >= 2:
                break

        cutoff_ts = timestamps[1]

        broker.write("test_queue", "new_message1")
        broker.write("test_queue", "new_message2")

        new_messages = list(
            broker.claim_generator(
                "test_queue", with_timestamps=False, since_timestamp=cutoff_ts
            )
        )

        assert new_messages == ["new_message1", "new_message2"]

        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["old_message1", "old_message2"]

    def test_empty_queue_generator(self, broker):
        """Test generators on empty queues."""
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
        for i in range(100):
            broker.write("test_queue", f"message{i + 1}")

        gen = broker.claim_generator("test_queue", with_timestamps=False)
        first_five = []
        for msg in gen:
            first_five.append(msg)
            if len(first_five) >= 5:
                break

        assert first_five == [
            "message1",
            "message2",
            "message3",
            "message4",
            "message5",
        ]

        gen.close()

        remaining_count = len(
            list(broker.peek_generator("test_queue", with_timestamps=False))
        )
        assert remaining_count == 95

    def test_at_least_once_generator_early_termination_replays_unfinished_batch(
        self, broker
    ):
        """Interrupted at-least-once batches should replay unread batch entries."""
        for i in range(5):
            broker.write("test_queue", f"message{i + 1}")

        gen = broker.claim_generator(
            "test_queue",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            batch_size=2,
        )

        first_three = []
        try:
            for msg in gen:
                first_three.append(msg)
                if len(first_three) >= 3:
                    break
        finally:
            gen.close()

        assert first_three == ["message1", "message2", "message3"]

        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["message3", "message4", "message5"]

    def test_generator_exact_timestamp(self, broker):
        """Test generators with exact_timestamp parameter."""
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")
        broker.write("test_queue", "message3")

        timestamps = []
        for body, ts in broker.peek_generator("test_queue"):
            timestamps.append((body, ts))
            if len(timestamps) >= 3:
                break

        target_ts = timestamps[1][1]

        messages = list(
            broker.claim_generator(
                "test_queue", with_timestamps=False, exact_timestamp=target_ts
            )
        )

        assert messages == ["message2"]

        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["message1", "message3"]

    def test_move_generator_exact_timestamp(self, broker):
        """Test move_generator honors exact_timestamp in all delivery modes."""
        broker.write("source_queue", "message1")
        broker.write("source_queue", "message2")
        broker.write("source_queue", "message3")

        timestamps = list(broker.peek_generator("source_queue"))
        target_ts = timestamps[1][1]

        moved = list(
            broker.move_generator(
                "source_queue",
                "dest_queue",
                with_timestamps=False,
                exact_timestamp=target_ts,
                delivery_guarantee="exactly_once",
            )
        )
        assert moved == ["message2"]

        source_remaining = list(
            broker.peek_generator("source_queue", with_timestamps=False)
        )
        dest_messages = list(broker.peek_generator("dest_queue", with_timestamps=False))
        assert source_remaining == ["message1", "message3"]
        assert dest_messages == ["message2"]

        moved_again = list(
            broker.move_generator(
                "source_queue",
                "dest_queue2",
                with_timestamps=False,
                exact_timestamp=target_ts,
                delivery_guarantee="at_least_once",
                batch_size=10,
            )
        )
        assert moved_again == []

    def test_move_generator_queue_validation(self, broker):
        """Test move_generator validates queue names."""
        broker.write("test_queue", "message")

        with pytest.raises(
            ValueError, match="Source and target queues cannot be the same"
        ):
            list(broker.move_generator("test_queue", "test_queue"))

    def test_at_least_once_generator_reentrant_call_no_deadlock(self, broker):
        """Reentrant calls during generator consumption should not deadlock."""
        for i in range(3):
            broker.write("test_queue", f"message{i + 1}")

        finished = threading.Event()
        errors: list[Exception] = []

        def worker() -> None:
            try:
                for _ in broker.claim_generator(
                    "test_queue",
                    with_timestamps=False,
                    delivery_guarantee="at_least_once",
                    batch_size=2,
                ):
                    broker.peek_one("test_queue", with_timestamps=False)
                    break
            except Exception as e:
                errors.append(e)
            finally:
                finished.set()

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

        assert finished.wait(timeout=2.0), "Generator consumption deadlocked"
        thread.join(timeout=0.2)
        assert not errors

    def test_at_least_once_claim_generator_reentrant_write_rejected(self, broker):
        """Mutating re-entry during claim batches should fail clearly and roll back."""
        for i in range(3):
            broker.write("test_queue", f"message{i + 1}")

        gen = broker.claim_generator(
            "test_queue",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            batch_size=2,
        )

        try:
            assert next(gen) == "message1"

            with pytest.raises(RuntimeError, match="separate BrokerDB/Queue instance"):
                broker.write("other_queue", "nested")
        finally:
            gen.close()

        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        other_messages = list(
            broker.peek_generator("other_queue", with_timestamps=False)
        )

        assert remaining == ["message1", "message2", "message3"]
        assert other_messages == []

    def test_at_least_once_move_generator_reentrant_write_rejected(self, broker):
        """Mutating re-entry during move batches should fail clearly and roll back."""
        for i in range(3):
            broker.write("source_queue", f"message{i + 1}")

        gen = broker.move_generator(
            "source_queue",
            "dest_queue",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            batch_size=2,
        )

        try:
            assert next(gen) == "message1"

            with pytest.raises(RuntimeError, match="separate BrokerDB/Queue instance"):
                broker.write("other_queue", "nested")
        finally:
            gen.close()

        source_remaining = list(
            broker.peek_generator("source_queue", with_timestamps=False)
        )
        dest_messages = list(broker.peek_generator("dest_queue", with_timestamps=False))
        other_messages = list(
            broker.peek_generator("other_queue", with_timestamps=False)
        )

        assert source_remaining == ["message1", "message2", "message3"]
        assert dest_messages == []
        assert other_messages == []
