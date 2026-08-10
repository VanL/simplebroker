"""Test suite for QueueWatcher burst mode behavior.

Tests the intelligent burst mode management that only resets on actual activity.
"""

from __future__ import annotations

import contextlib
import os
import sys
import threading
import time

import pytest

from simplebroker.watcher import PollingStrategy, QueueWatcher

from .helper_scripts.broker_factory import make_broker
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

pytestmark = [pytest.mark.shared]


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

    def get_delay_history(self) -> list[float]:
        """Return a stable copy of recorded delays."""
        with self._lock:
            return self.delay_history.copy()


def make_recorded_watcher(
    queue: str,
    handler,
    *,
    broker_target,
    jitter_factor: float = 0,
    **kwargs,
) -> tuple[QueueWatcher, InstrumentedPollingStrategy]:
    """Build a real watcher with instrumentation limited to its polling seam."""
    stop_event = threading.Event()
    strategy = InstrumentedPollingStrategy(
        stop_event,
        initial_checks=5,
        max_interval=0.01,
        burst_sleep=0.0001,
        jitter_factor=jitter_factor,
    )
    watcher = QueueWatcher(
        queue,
        handler,
        db=broker_target,
        stop_event=stop_event,
        polling_strategy=strategy,
        **kwargs,
    )
    return watcher, strategy


def test_burst_mode_resets_on_activity(no_jitter, broker_target) -> None:
    """Verify burst mode resets when messages are found."""
    broker = make_broker(broker_target)
    watcher = None
    try:
        processed_messages = []

        def handler(msg, ts) -> None:
            processed_messages.append((msg, time.monotonic()))

        watcher, strategy = make_recorded_watcher(
            "test_queue", handler, broker_target=broker_target
        )
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
            interval=0.01,
            message="Watcher should back off when no messages available",
        )

        # Record polling state before message
        delay_count_before = len(strategy.delay_history)

        broker.write("test_queue", "test message")

        # Wait for message processing
        wait_for_condition(
            lambda: len(processed_messages) > 0,
            timeout=1.0,
            interval=0.01,
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
            interval=0.01,
            message="Should resume burst mode after finding message",
        )

        # The watcher should process the message written for this phase. The
        # burst-mode invariant is checked above from polling history; exact
        # wall-clock ordering around broker.write() returning is scheduler-dependent.
        assert processed_messages[0][0] == "test message"
        assert len(strategy.notify_history) == 1

    finally:
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_burst_mode_no_reset_on_empty_wake(no_jitter, broker_target) -> None:
    """Verify burst mode doesn't reset when no messages found."""
    broker = make_broker(broker_target)
    active_watcher = None
    idle_watcher = None
    try:
        processed_counts = {"active": 0, "idle": 0}

        def make_handler(queue_name):
            def handler(msg, ts) -> None:
                processed_counts[queue_name] += 1

            return handler

        # Create two watchers - one active, one idle
        active_watcher, active_strategy = make_recorded_watcher(
            "active_queue",
            make_handler("active"),
            broker_target=broker_target,
        )
        idle_watcher, idle_strategy = make_recorded_watcher(
            "idle_queue",
            make_handler("idle"),
            broker_target=broker_target,
        )

        active_watcher.run_in_thread()
        idle_watcher.run_in_thread()

        # Wait for both to back off
        def both_backed_off():
            active_delays = active_strategy.delay_history
            idle_delays = idle_strategy.delay_history
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
        active_delays_before = len(active_strategy.delay_history)
        idle_delays_before = len(idle_strategy.delay_history)

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
        active_new_delays = active_strategy.delay_history[active_delays_before:]
        active_zero_count = sum(1 for d in active_new_delays if d == 0)
        assert active_zero_count >= 3, (
            f"Active watcher should reset to burst, got {active_zero_count} zero delays"
        )

        # Idle watcher should continue with non-zero delays
        idle_new_delays = idle_strategy.delay_history[idle_delays_before:]
        if len(idle_new_delays) > 0:
            idle_zero_count = sum(1 for d in idle_new_delays if d == 0)
            assert idle_zero_count == 0, "Idle watcher should not have zero delays"

    finally:
        if active_watcher is not None:
            active_watcher.stop()
        if idle_watcher is not None:
            idle_watcher.stop()
        broker.shutdown()


def test_burst_mode_gradual_backoff(no_jitter, broker_target) -> None:
    """Test the gradual backoff behavior."""
    del no_jitter, broker_target
    strategy = PollingStrategy(threading.Event(), jitter_factor=0)

    strategy._check_count = 0
    assert strategy._calculate_base_delay() == 0

    strategy._check_count = 50
    assert strategy._calculate_base_delay() == 0

    strategy._check_count = 100
    assert strategy._calculate_base_delay() == 0

    strategy._check_count = 150
    base_delay = strategy._calculate_base_delay()
    assert 0 < base_delay < 0.1

    strategy._check_count = 300
    assert strategy._calculate_base_delay() == 0.1


def test_burst_mode_with_batch_processing(no_jitter, broker_target) -> None:
    """Test burst mode with batch message processing."""
    broker = make_broker(broker_target)
    watcher = None
    try:
        expected_messages = {f"message_{i}" for i in range(10)}
        processed = []
        processed_seen: set[str] = set()
        processed_lock = threading.Lock()
        all_processed = threading.Event()

        def handler(msg, ts) -> None:
            with processed_lock:
                processed.append(msg)
                processed_seen.add(msg)
                if expected_messages <= processed_seen:
                    all_processed.set()

        def processed_snapshot() -> list[str]:
            with processed_lock:
                return list(processed)

        # Queue the batch before starting the watcher so startup timing does not
        # decide whether CI sees a full batch.
        for i in range(10):
            broker.write("test_queue", f"message_{i}")

        watcher, strategy = make_recorded_watcher(
            "test_queue",
            handler,
            broker_target=broker_target,
            batch_processing=True,
        )

        watcher.run_in_thread()

        if not all_processed.wait(timeout=scale_timeout_for_ci(10.0)):
            snapshot = processed_snapshot()
            raise AssertionError(
                "Timed out waiting for batch watcher to process messages: "
                f"processed={snapshot}, missing={sorted(expected_messages - set(snapshot))}, "
                f"activity_notifications={len(strategy.notify_history)}"
            )

        # Should process all messages
        snapshot = processed_snapshot()

        assert len(snapshot) == 10
        assert set(snapshot) == expected_messages
        assert wait_for_condition(
            lambda: bool(strategy.notify_history),
            timeout=scale_timeout_for_ci(2.0),
            message="real batch drain did not report activity to polling strategy",
        )

    finally:
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_burst_mode_with_errors_single_message(no_jitter, broker_target) -> None:
    """Test burst mode behavior when handler errors occur (single-message mode)."""
    broker = make_broker(broker_target)
    watcher = None
    try:
        attempted_messages: list[str] = []
        failed_messages: list[str] = []
        successful_messages = []

        def handler(msg, ts) -> None:
            del ts
            attempted_messages.append(msg)
            if msg in {"message_0", "message_2"}:
                raise ValueError(f"failed {msg}")
            successful_messages.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            del ts
            assert str(exc) == f"failed {msg}"
            failed_messages.append(msg)
            return True

        # Test with default single-message processing
        watcher, _strategy = make_recorded_watcher(
            "test_queue",
            handler,
            broker_target=broker_target,
            error_handler=error_handler,
            # batch_processing=False is the default
        )
        watcher.run_in_thread()

        # Add messages
        for i in range(4):
            broker.write("test_queue", f"message_{i}")

        # Wait for all messages to be processed with a generous timeout for CI
        wait_for_condition(
            lambda: len(attempted_messages) >= 4,
            timeout=10.0,  # Very generous timeout for slow CI
            message="Failed to process all 4 messages",
        )

        # Verify all messages were attempted despite errors
        assert attempted_messages == [f"message_{i}" for i in range(4)]
        assert failed_messages == ["message_0", "message_2"]
        assert successful_messages == ["message_1", "message_3"]

    finally:
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_burst_mode_with_errors_batch_processing(no_jitter, broker_target) -> None:
    """Test burst mode behavior when handler errors occur (batch mode)."""
    broker = make_broker(broker_target)
    watcher = None
    try:
        attempted_messages: list[str] = []
        failed_messages: list[str] = []
        successful_messages = []

        def handler(msg, ts) -> None:
            del ts
            attempted_messages.append(msg)
            if msg in {"message_0", "message_2"}:
                raise ValueError(f"failed {msg}")
            successful_messages.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            del ts
            assert str(exc) == f"failed {msg}"
            failed_messages.append(msg)
            return True

        # Test with batch processing enabled
        watcher, _strategy = make_recorded_watcher(
            "test_queue",
            handler,
            broker_target=broker_target,
            error_handler=error_handler,
            batch_processing=True,  # Process all messages in one polling cycle
        )
        watcher.run_in_thread()

        # Add messages
        for i in range(4):
            broker.write("test_queue", f"message_{i}")

        # Wait for all messages to be processed with a generous timeout for CI
        wait_for_condition(
            lambda: len(attempted_messages) >= 4,
            timeout=10.0,  # Very generous timeout for slow CI
            message="Failed to process all 4 messages",
        )

        # Verify all messages were attempted despite errors
        assert attempted_messages == [f"message_{i}" for i in range(4)]
        assert failed_messages == ["message_0", "message_2"]
        assert successful_messages == ["message_1", "message_3"]

    finally:
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_polling_jitter(broker_target) -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-032] exception
    """Test that polling includes jitter to prevent synchronization."""
    watchers = []
    strategies: list[InstrumentedPollingStrategy] = []
    broker = make_broker(broker_target)
    try:

        def handler(msg, ts) -> None:
            pass

        # Create multiple watchers
        for i in range(5):
            w, strategy = make_recorded_watcher(
                f"queue_{i}",
                handler,
                broker_target=broker_target,
                jitter_factor=0.2,
            )
            watchers.append(w)
            strategies.append(strategy)
            w.run_in_thread()

        # Wait until we have enough samples to test jitter properly
        # Keep running until we collect sufficient backed-off delay samples
        required_samples = 30  # Need at least 30 samples for meaningful jitter test
        start_time = time.monotonic()
        max_wait = 60.0  # Give up to 60 seconds to collect samples

        while time.monotonic() - start_time < max_wait:
            # Count backed-off samples across all watchers
            all_backed_off_delays = []

            for strategy in strategies:
                if len(strategy.delay_history) > 0:
                    # Collect delays that indicate backed-off state (near max_interval)
                    # Using 0.7 threshold to ensure we're testing at full backoff
                    backed_off = [
                        d
                        for d in strategy.delay_history
                        if d > strategy._max_interval * 0.7
                    ]
                    all_backed_off_delays.extend(backed_off)

            # If we have enough backed-off samples, we can proceed
            if len(all_backed_off_delays) >= required_samples:
                break

            time.sleep(0.1)  # Check every 100ms

        # Ensure we got enough samples before timeout
        elapsed = time.monotonic() - start_time
        assert elapsed < max_wait, (
            f"Timeout: could not collect {required_samples} backed-off samples in {max_wait}s"
        )

        # Collect the same backed-off delays we were waiting for
        all_delays = []
        all_strategies = []
        for strategy in strategies:
            all_strategies.append(strategy)
            # Collect delays that indicate backed-off state (same criteria as wait loop)
            backed_off_delays = [
                d for d in strategy.delay_history if d > strategy._max_interval * 0.7
            ]
            all_delays.extend(backed_off_delays)

        # We should have collected enough samples from the wait loop above
        assert len(all_delays) >= 20, (
            f"Should have at least 20 delay samples for jitter test, got {len(all_delays)}"
        )

        # Delays should vary due to jitter
        unique_delays = set(all_delays)

        # With jitter and multiple watchers, we should see variety
        # But after we're using random.uniform, we might get some repeated values
        # Require at least 2 unique values as absolute minimum
        assert len(unique_delays) >= 2, (
            f"Delays should vary due to jitter: got only {len(unique_delays)} unique values from {len(all_delays)} samples"
        )

        # Calculate the actual base delay for backed-off state
        actual_base_delay = 0.01

        # Check jitter range against actual base delay
        min_delay = min(all_delays)
        max_delay = max(all_delays)

        # With jitter factor of 0.2 (±20%), delays should be:
        # theoretical min: 0.1 * (1 - 0.2) = 0.08
        # theoretical max: 0.1 * (1 + 0.2) = 0.12
        # Allow some tolerance for timing/rounding issues
        assert min_delay >= actual_base_delay * 0.70, (
            f"Min delay {min_delay} should be >= {actual_base_delay * 0.70} (with tolerance)"
        )
        assert max_delay <= actual_base_delay * 1.30, (
            f"Max delay {max_delay} should be <= {actual_base_delay * 1.30} (with tolerance)"
        )

        # Verify we have some spread of delays (not all identical)
        # With many samples, even a small spread shows jitter is working
        delay_spread = max_delay - min_delay
        assert delay_spread > 0, (
            f"Delay spread should be non-zero to show jitter is active, got {delay_spread}"
        )

        # If we have enough unique values, check for reasonable spread
        if len(unique_delays) >= 5:
            assert delay_spread >= actual_base_delay * 0.05, (
                f"With {len(unique_delays)} unique values, spread {delay_spread} should be at least 5% of base delay"
            )

        # Cleanup watchers
        for w in watchers:
            # Ignore errors during cleanup
            with contextlib.suppress(Exception):
                w.stop()
                if sys.platform == "win32":
                    time.sleep(0.5)  # Allow threads to terminate
    finally:
        # Delete the watchers list to clean up references
        for w in watchers:
            del w
        # On Windows, add a small delay to ensure threads fully terminate
        # and file handles are released before closing the database

        if sys.platform == "win32":
            time.sleep(1)

        # Now safe to close the broker
        broker.shutdown()


def test_burst_mode_with_peek_mode(no_jitter, broker_target) -> None:
    """Test burst mode behavior in peek mode."""
    broker = make_broker(broker_target)
    watcher = None
    try:
        peeked_messages = []

        def handler(msg, ts) -> None:
            peeked_messages.append((msg, ts))

        watcher, strategy = make_recorded_watcher(
            "test_queue", handler, broker_target=broker_target, peek=True
        )

        watcher.run_in_thread()

        assert wait_for_condition(
            lambda: (
                len(strategy.delay_history) >= 10
                and all(delay > 0 for delay in strategy.delay_history[-3:])
            ),
            timeout=scale_timeout_for_ci(5.0),
            message="peek watcher did not reach its backed-off live schedule",
        )

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

        # In peek mode with after_timestamp, same message won't be peeked again
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

        # Verify burst mode was maintained after message processing. Native
        # activity backends may reach the assertion immediately after the
        # fourth handler call, before the next burst polls have been recorded.
        wait_for_condition(
            lambda: (
                sum(1 for d in strategy.delay_history[delays_before:] if d == 0) > 5
            ),
            timeout=1.0,
            message="Should maintain burst mode while processing messages",
        )

        # Verify messages are still in queue (peek doesn't remove them)
        # Use peek_many to get all messages, which doesn't remove them
        messages = list(broker.peek_many("test_queue", limit=10, with_timestamps=False))
        assert messages == [
            "message_1",
            "message_2",
            "message_3",
            "message_4",
        ]

    finally:
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_burst_mode_state_transitions(no_jitter, broker_target) -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-032] exception
    """Test transitions between burst and backed-off states."""
    broker = make_broker(broker_target)
    watcher = None
    try:
        processed_messages = []

        def handler(msg, ts) -> None:
            processed_messages.append(msg)

        watcher, strategy = make_recorded_watcher(
            "test_queue", handler, broker_target=broker_target
        )

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

        # Phase 4: Continuous activity should maintain burst mode. First add
        # one message and wait until the watcher has actually woken back into
        # burst mode; native activity backends can legitimately record a few
        # backed-off waits before the first wake arrives.
        first_activity_delay_count = len(strategy.delay_history)
        broker.write("test_queue", "message_0")

        wait_for_condition(
            lambda: len(processed_messages) == 2,
            timeout=2.0,
            message="First continuous activity message should be processed",
        )

        def first_activity_burst_started():
            new_delays = strategy.delay_history[first_activity_delay_count:]
            return sum(1 for d in new_delays if d == 0) >= 3

        wait_for_condition(
            first_activity_burst_started,
            timeout=1.0,
            message="Continuous activity should restart burst mode",
        )

        start_count = len(strategy.get_delay_history())

        # Add remaining messages continuously
        for i in range(1, 5):
            broker.write("test_queue", f"message_{i}")
            time.sleep(0.02)  # Small delay between messages

        # Wait for all messages
        assert wait_for_condition(
            lambda: len(processed_messages) == 6,  # 1 + 5 messages
            timeout=2.0,
            message="All messages should be processed",
        )

        def burst_run_after_continuous_activity() -> bool:
            activity_delays = strategy.get_delay_history()[start_count:]
            consecutive_zeroes = 0
            for delay in activity_delays:
                if delay == 0:
                    consecutive_zeroes += 1
                    if consecutive_zeroes >= 5:
                        return True
                else:
                    consecutive_zeroes = 0
            return False

        assert wait_for_condition(
            burst_run_after_continuous_activity,
            timeout=1.0,
            message="Continuous activity should record a burst run",
        )

        # Verify activity returned the watcher to burst mode. Native activity
        # backends can record a few backed-off waits before notifications wake
        # the watcher, so require a burst run instead of a ratio over the whole
        # window.
        activity_delays = strategy.get_delay_history()[start_count:]
        zero_count = sum(1 for d in activity_delays if d == 0)
        assert zero_count >= 5, (
            "Should return to burst mode with continuous activity, "
            f"got delays={activity_delays}"
        )

    finally:
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
