"""Test suite for QueueWatcher burst mode behavior.

Tests the intelligent burst mode management that only resets on actual activity.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import PollingStrategy, QueueWatcher

from .helpers.timing import wait_for_condition


@pytest.fixture
def no_jitter():
    """Disable jitter for timing-sensitive tests."""
    old_val = os.environ.get("BROKER_JITTER_FACTOR")
    os.environ["BROKER_JITTER_FACTOR"] = "0"
    yield
    if old_val is None:
        os.environ.pop("BROKER_JITTER_FACTOR", None)
    else:
        os.environ["BROKER_JITTER_FACTOR"] = old_val


class InstrumentedPollingStrategy(PollingStrategy):
    """PollingStrategy with instrumentation for testing."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.delay_history: list[float] = []
        self.notify_history: list[float] = []
        self.check_version_calls = 0
        self._lock = threading.Lock()

    def _get_delay(self) -> float:
        """Track delay calculations."""
        delay = super()._get_delay()
        with self._lock:
            self.delay_history.append(delay)
        return delay

    def notify_activity(self) -> None:
        """Track activity notifications."""
        with self._lock:
            self.notify_history.append(time.time())
        super().notify_activity()

    def _check_data_version(self) -> bool:
        """Track version checks."""
        with self._lock:
            self.check_version_calls += 1
        return super()._check_data_version()

    def get_current_check_count(self) -> int:
        """Get current check count for testing."""
        return self._check_count


class InstrumentedQueueWatcher(QueueWatcher):
    """QueueWatcher with instrumented polling strategy."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Replace strategy with instrumented version
        old_strategy = self._strategy
        self._strategy = InstrumentedPollingStrategy(
            old_strategy._stop_event,
            old_strategy._initial_checks,
            old_strategy._max_interval,
            old_strategy._burst_sleep,
        )
        self._has_pending_messages_enabled = True
        self._found_messages_last_drain = False

    def _has_pending_messages(self, db: BrokerDB) -> bool:
        """Fast check if queue has unclaimed messages."""
        if not self._has_pending_messages_enabled:
            return True

        sql = "SELECT EXISTS(SELECT 1 FROM messages WHERE queue = ? AND claimed = 0 LIMIT 1)"
        rows = list(db._runner.run(sql, (self._queue,), fetch=True))
        return bool(rows[0][0]) if rows else False

    def _drain_queue(self) -> None:
        """Override to implement smart burst reset."""
        # Check if we should drain
        if self._has_pending_messages_enabled:
            db = self._get_db()
            if not self._has_pending_messages(db):
                self._found_messages_last_drain = False
                return

        # Track if we found messages before calling parent
        initial_count = getattr(self, "_message_count", 0)
        super()._drain_queue()
        final_count = getattr(self, "_message_count", 0)

        self._found_messages_last_drain = final_count > initial_count

        # Only notify activity if we actually processed messages
        if self._found_messages_last_drain:
            self._strategy.notify_activity()

    def _dispatch(self, message: str, timestamp: int) -> None:
        """Track message dispatches."""
        if not hasattr(self, "_message_count"):
            self._message_count = 0
        self._message_count += 1
        super()._dispatch(message, timestamp)


def test_burst_mode_resets_on_activity(no_jitter) -> None:
    """Verify burst mode resets when messages are found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:
            processed_messages = []

            def handler(msg, ts) -> None:
                processed_messages.append((msg, time.time()))

            watcher = InstrumentedQueueWatcher(db_path, "test_queue", handler)
            strategy = watcher._strategy

            # Start watcher
            watcher.run_in_thread()

            # Wait for backoff - verify delays are increasing
            def has_backed_off():
                if len(strategy.delay_history) < 10:
                    return False
                recent_delays = strategy.delay_history[-5:]
                # Check that we have non-zero delays indicating backoff
                return all(d > 0 for d in recent_delays)

            wait_for_condition(
                has_backed_off,
                timeout=2.0,
                message="Watcher should back off when no messages available",
            )

            # Record polling state before message
            delay_count_before = len(strategy.delay_history)

            # Add a message and record when
            message_time = time.time()
            broker.write("test_queue", "test message")

            # Wait for message processing
            wait_for_condition(
                lambda: len(processed_messages) > 0,
                timeout=1.0,
                message="Message should be processed",
            )

            # Verify rapid polling resumed after message (burst mode)
            # Count zero-delay polls that happened after the message
            def burst_mode_resumed():
                if len(strategy.delay_history) <= delay_count_before:
                    return False
                new_delays = strategy.delay_history[delay_count_before:]
                # In burst mode, we should see multiple zero delays
                zero_count = sum(1 for d in new_delays if d == 0)
                return zero_count >= 5  # At least 5 rapid polls

            wait_for_condition(
                burst_mode_resumed,
                timeout=1.0,
                message="Should resume burst mode after finding message",
            )

            # Verify message was processed quickly after being written
            process_latency = processed_messages[0][1] - message_time
            assert process_latency < 0.1, (
                f"Message should be processed quickly, took {process_latency:.3f}s"
            )

            watcher.stop()
        finally:
            broker.close()


def test_burst_mode_no_reset_on_empty_wake(no_jitter) -> None:
    """Verify burst mode doesn't reset when no messages found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:
            processed_counts = {"active": 0, "idle": 0}

            def make_handler(queue_name):
                def handler(msg, ts) -> None:
                    processed_counts[queue_name] += 1

                return handler

            # Create two watchers - one active, one idle
            active_watcher = InstrumentedQueueWatcher(
                db_path,
                "active_queue",
                make_handler("active"),
            )
            idle_watcher = InstrumentedQueueWatcher(
                db_path,
                "idle_queue",
                make_handler("idle"),
            )

            active_watcher.run_in_thread()
            idle_watcher.run_in_thread()

            # Wait for both to back off
            def both_backed_off():
                active_delays = active_watcher._strategy.delay_history
                idle_delays = idle_watcher._strategy.delay_history
                if len(active_delays) < 20 or len(idle_delays) < 20:
                    return False
                # Check recent delays are non-zero
                return all(d > 0 for d in active_delays[-5:]) and all(
                    d > 0 for d in idle_delays[-5:]
                )

            wait_for_condition(
                both_backed_off,
                timeout=2.0,
                message="Both watchers should back off",
            )

            # Record delay counts before message
            active_delays_before = len(active_watcher._strategy.delay_history)
            idle_delays_before = len(idle_watcher._strategy.delay_history)

            # Write to active queue only
            broker.write("active_queue", "message")

            # Wait for active watcher to process
            wait_for_condition(
                lambda: processed_counts["active"] == 1,
                timeout=1.0,
                message="Active watcher should process message",
            )

            # Give time for more polling cycles
            time.sleep(0.2)

            # Active watcher should show burst mode (zero delays)
            active_new_delays = active_watcher._strategy.delay_history[
                active_delays_before:
            ]
            active_zero_count = sum(1 for d in active_new_delays if d == 0)
            assert active_zero_count >= 3, (
                f"Active watcher should reset to burst, got {active_zero_count} zero delays"
            )

            # Idle watcher should continue with non-zero delays
            idle_new_delays = idle_watcher._strategy.delay_history[idle_delays_before:]
            if len(idle_new_delays) > 0:
                idle_zero_count = sum(1 for d in idle_new_delays if d == 0)
                assert idle_zero_count == 0, "Idle watcher should not have zero delays"

            active_watcher.stop()
            idle_watcher.stop()
        finally:
            broker.close()


def test_burst_mode_gradual_backoff(no_jitter) -> None:
    """Test the gradual backoff behavior."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:

            def handler(msg, ts) -> None:
                pass

            watcher = InstrumentedQueueWatcher(db_path, "test_queue", handler)
            strategy = watcher._strategy

            # Manually test the delay calculation at different check counts
            # Test base delay to avoid jitter issues
            strategy._check_count = 0
            assert strategy._calculate_base_delay() == 0  # Burst mode

            strategy._check_count = 50
            assert strategy._calculate_base_delay() == 0  # Still burst mode

            strategy._check_count = 100
            assert strategy._calculate_base_delay() == 0  # End of burst mode

            strategy._check_count = 150
            base_delay = strategy._calculate_base_delay()
            assert 0 < base_delay < strategy._max_interval  # Gradual increase

            strategy._check_count = 300
            assert (
                strategy._calculate_base_delay() == strategy._max_interval
            )  # Max backoff
        finally:
            broker.close()


def test_burst_mode_with_batch_processing(no_jitter) -> None:
    """Test burst mode with batch message processing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:
            processed = []

            def handler(msg, ts) -> None:
                processed.append(msg)

            watcher = InstrumentedQueueWatcher(
                db_path,
                "test_queue",
                handler,
                batch_processing=True,
            )

            watcher.run_in_thread()
            time.sleep(0.2)

            # Add multiple messages
            for i in range(10):
                broker.write("test_queue", f"message_{i}")

            time.sleep(0.5)

            # Should process all messages
            assert len(processed) == 10
            # With batch processing, counter resets after processing the batch
            # Verify we processed messages efficiently
            assert watcher._message_count == 10

            watcher.stop()
        finally:
            broker.close()


def test_burst_mode_with_errors_single_message(no_jitter) -> None:
    """Test burst mode behavior when handler errors occur (single-message mode)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:
            call_count = 0
            error_count = 0
            successful_messages = []

            def handler(msg, ts) -> None:
                nonlocal call_count, error_count
                call_count += 1
                if call_count % 2 == 1:
                    error_count += 1
                    msg = "Test error"
                    raise ValueError(msg)
                successful_messages.append(msg)

            # Test with default single-message processing
            watcher = InstrumentedQueueWatcher(
                db_path,
                "test_queue",
                handler,
                # batch_processing=False is the default
            )
            watcher.run_in_thread()

            # Add messages
            for i in range(4):
                broker.write("test_queue", f"message_{i}")

            # Wait for all messages to be processed with a generous timeout for CI
            wait_for_condition(
                lambda: call_count >= 4,
                timeout=10.0,  # Very generous timeout for slow CI
                message="Failed to process all 4 messages",
            )

            # Verify all messages were attempted despite errors
            assert call_count == 4
            assert error_count == 2  # Errors on 1st and 3rd messages
            assert len(successful_messages) == 2  # 2nd and 4th messages succeeded

            watcher.stop()
        finally:
            broker.close()


def test_burst_mode_with_errors_batch_processing(no_jitter) -> None:
    """Test burst mode behavior when handler errors occur (batch mode)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:
            call_count = 0
            error_count = 0
            successful_messages = []

            def handler(msg, ts) -> None:
                nonlocal call_count, error_count
                call_count += 1
                if call_count % 2 == 1:
                    error_count += 1
                    msg = "Test error"
                    raise ValueError(msg)
                successful_messages.append(msg)

            # Test with batch processing enabled
            watcher = InstrumentedQueueWatcher(
                db_path,
                "test_queue",
                handler,
                batch_processing=True,  # Process all messages in one polling cycle
            )
            watcher.run_in_thread()

            # Add messages
            for i in range(4):
                broker.write("test_queue", f"message_{i}")

            # Wait for all messages to be processed with a generous timeout for CI
            wait_for_condition(
                lambda: call_count >= 4,
                timeout=10.0,  # Very generous timeout for slow CI
                message="Failed to process all 4 messages",
            )

            # Verify all messages were attempted despite errors
            assert call_count == 4
            assert error_count == 2  # Errors on 1st and 3rd messages
            assert len(successful_messages) == 2  # 2nd and 4th messages succeeded

            watcher.stop()
        finally:
            broker.close()


def test_polling_jitter() -> None:
    """Test that polling includes jitter to prevent synchronization."""
    # Import and directly modify the config
    from simplebroker.watcher import _config

    # Save original value and set new jitter factor
    original_jitter = _config["BROKER_JITTER_FACTOR"]
    _config["BROKER_JITTER_FACTOR"] = 0.2

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            BrokerDB(db_path)

            def handler(msg, ts) -> None:
                pass

            # Create multiple watchers
            watchers = []
            for i in range(5):
                w = InstrumentedQueueWatcher(db_path, f"queue_{i}", handler)
                watchers.append(w)
                w.run_in_thread()

            # Let them run and back off
            time.sleep(1.0)

            # Collect recent delays from each watcher
            all_delays = []
            all_strategies = []
            for w in watchers:
                strategy = w._strategy
                all_strategies.append(strategy)
                if len(strategy.delay_history) > 10:
                    # Only get delays when fully backed off (close to max_interval)
                    # This filters out delays from the ramp-up period
                    backed_off_delays = [
                        d
                        for d in strategy.delay_history[-10:]
                        if d > strategy._max_interval * 0.5
                    ]  # Only delays > 0.05
                    all_delays.extend(backed_off_delays)

            # Delays should vary due to jitter
            if all_delays:
                unique_delays = set(all_delays)
                print(f"DEBUG: All delays: {all_delays}")
                print(f"DEBUG: Unique delays: {unique_delays}")
                assert len(unique_delays) > 1, (
                    f"Delays should vary due to jitter: got {unique_delays}"
                )

                # Calculate the actual base delay for backed-off state
                # When check_count > initial_checks (100), base delay approaches max_interval
                # For a fully backed-off watcher, base_delay = max_interval = 0.1
                actual_base_delay = all_strategies[0]._max_interval  # 0.1

                # Check jitter range against actual base delay
                min_delay = min(all_delays)
                max_delay = max(all_delays)

                # With jitter factor of 0.2 (±20%), delays should be:
                # min: 0.1 * (1 - 0.2) = 0.08
                # max: 0.1 * (1 + 0.2) = 0.12
                assert min_delay >= actual_base_delay * 0.8, (
                    f"Min delay {min_delay} should be >= {actual_base_delay * 0.8}"
                )
                assert max_delay <= actual_base_delay * 1.2, (
                    f"Max delay {max_delay} should be <= {actual_base_delay * 1.2}"
                )

            # Cleanup
            for w in watchers:
                w.stop()

    finally:
        # Restore original config value
        _config["BROKER_JITTER_FACTOR"] = original_jitter


def test_burst_mode_with_peek_mode(no_jitter) -> None:
    """Test burst mode behavior in peek mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:
            peeked_messages = []

            def handler(msg, ts) -> None:
                peeked_messages.append((msg, ts))

            watcher = InstrumentedQueueWatcher(
                db_path, "test_queue", handler, peek=True
            )
            strategy = watcher._strategy

            watcher.run_in_thread()

            # Let it start and potentially back off
            time.sleep(0.2)

            # Record state before message
            delays_before = len(strategy.delay_history)

            # Add first message
            broker.write("test_queue", "message_1")

            # Wait for first peek
            wait_for_condition(
                lambda: len(peeked_messages) == 1,
                timeout=1.0,
                message="Should peek first message",
            )

            # In peek mode with since_timestamp, same message won't be peeked again
            # Add more messages to verify continued burst mode
            for i in range(2, 5):
                broker.write("test_queue", f"message_{i}")
                time.sleep(0.01)

            # Wait for all peeks
            wait_for_condition(
                lambda: len(peeked_messages) == 4,
                timeout=1.0,
                message="Should peek all messages",
            )

            # Verify messages were peeked in order and only once each
            assert [msg for msg, _ in peeked_messages] == [
                "message_1",
                "message_2",
                "message_3",
                "message_4",
            ]

            # Verify burst mode was maintained during message processing
            new_delays = strategy.delay_history[delays_before:]
            zero_count = sum(1 for d in new_delays if d == 0)
            assert zero_count > 5, (
                "Should maintain burst mode while processing messages"
            )

            # Verify messages are still in queue (peek doesn't remove them)
            # Use all_messages=True to get all messages, not just one
            messages = list(broker.read("test_queue", peek=True, all_messages=True))
            assert len(messages) == 4, "All messages should still be in queue"

            watcher.stop()
        finally:
            broker.close()


def test_burst_mode_state_transitions(no_jitter) -> None:
    """Test transitions between burst and backed-off states."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)
        try:
            processed_messages = []

            def handler(msg, ts) -> None:
                processed_messages.append(msg)

            watcher = InstrumentedQueueWatcher(db_path, "test_queue", handler)
            strategy = watcher._strategy

            watcher.run_in_thread()

            # Phase 1: Verify initial burst mode (zero delays)
            def initial_burst_mode():
                if len(strategy.delay_history) < 5:
                    return False
                # Should see zero delays in burst mode
                return all(d == 0 for d in strategy.delay_history[:5])

            wait_for_condition(
                initial_burst_mode,
                timeout=1.0,
                message="Should start in burst mode with zero delays",
            )

            # Phase 2: Wait for backoff (non-zero delays)
            def has_backed_off():
                if len(strategy.delay_history) < 120:  # Need enough history
                    return False
                # Check recent delays are non-zero
                recent = strategy.delay_history[-10:]
                return all(d > 0 for d in recent)

            wait_for_condition(
                has_backed_off,
                timeout=2.0,
                message="Should back off when no messages",
            )

            # Phase 3: Add message and verify burst reset
            delays_before_msg = len(strategy.delay_history)
            broker.write("test_queue", "wake up!")

            # Wait for message processing
            wait_for_condition(
                lambda: len(processed_messages) == 1,
                timeout=1.0,
                message="Message should be processed",
            )

            # Verify return to burst mode
            def returned_to_burst():
                if len(strategy.delay_history) <= delays_before_msg + 3:
                    return False
                new_delays = strategy.delay_history[delays_before_msg:]
                # Should see zero delays after processing message
                zero_count = sum(1 for d in new_delays if d == 0)
                return zero_count >= 3

            wait_for_condition(
                returned_to_burst,
                timeout=1.0,
                message="Should return to burst mode after message",
            )

            # Phase 4: Continuous activity should maintain burst mode
            start_count = len(strategy.delay_history)

            # Add messages continuously
            for i in range(5):
                broker.write("test_queue", f"message_{i}")
                time.sleep(0.02)  # Small delay between messages

            # Wait for all messages
            wait_for_condition(
                lambda: len(processed_messages) == 6,  # 1 + 5 messages
                timeout=2.0,
                message="All messages should be processed",
            )

            # Verify stayed in burst mode during activity
            activity_delays = strategy.delay_history[start_count:]
            zero_count = sum(1 for d in activity_delays if d == 0)
            assert zero_count > len(activity_delays) * 0.8, (
                "Should mostly stay in burst mode with continuous activity"
            )

            watcher.stop()
        finally:
            broker.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
