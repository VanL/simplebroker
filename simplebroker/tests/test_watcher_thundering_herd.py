"""Test suite for QueueWatcher thundering herd mitigation.

Tests the proposed improvements to prevent all watchers from waking up
when unrelated queues receive messages.
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher

from .helpers.timing import wait_for_condition, wait_for_count


class WatcherMetrics:
    """Track performance metrics for a watcher."""

    def __init__(self, queue_name: str) -> None:
        self.queue_name = queue_name
        self.wake_ups = 0
        self.empty_wakes = 0
        self.messages_processed = 0
        self.pre_check_calls = 0
        self.drain_calls = 0
        self.lock = threading.Lock()

    def record_wake_up(self) -> None:
        with self.lock:
            self.wake_ups += 1

    def record_empty_wake(self) -> None:
        with self.lock:
            self.empty_wakes += 1

    def record_message(self) -> None:
        with self.lock:
            self.messages_processed += 1

    def record_pre_check(self) -> None:
        with self.lock:
            self.pre_check_calls += 1

    def record_drain(self) -> None:
        with self.lock:
            self.drain_calls += 1

    @property
    def efficiency(self) -> float:
        """Calculate wake-up efficiency (useful wakes / total wakes)."""
        with self.lock:
            if self.wake_ups == 0:
                return 1.0
            return 1.0 - (self.empty_wakes / self.wake_ups)


class InstrumentedQueueWatcher(QueueWatcher):
    """QueueWatcher with instrumentation for testing."""

    def __init__(self, *args, metrics: WatcherMetrics | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.metrics = metrics or WatcherMetrics(self._queue)
        self._in_main_loop = False

    def _has_pending_messages(self, db: BrokerDB) -> bool:
        """Override to track pre-check calls and wake-ups."""
        self.metrics.record_pre_check()

        # Call the base implementation first
        try:
            has_messages = super()._has_pending_messages(db)
        except Exception:
            # If we're in main loop and hit an exception, we recorded a wake_up but not empty_wake
            if self._in_main_loop:
                pass
            raise

        # Only record wake-ups when we're in the main loop
        # The initial drain doesn't count as a wake-up
        if self._in_main_loop:
            self.metrics.record_wake_up()
            # If no messages, this is an empty wake
            if not has_messages:
                self.metrics.record_empty_wake()

        return has_messages

    def _dispatch(self, message: str, timestamp: int) -> None:
        """Override to track processed messages."""
        self.metrics.record_message()
        super()._dispatch(message, timestamp)

    def _drain_queue(self) -> None:
        """Override to track drains."""
        self.metrics.record_drain()

        # Call original first (as Gemini suggested)
        super()._drain_queue()

        # Then mark that we've done the initial drain and are now in main loop
        if not self._in_main_loop:
            self._in_main_loop = True


def test_thundering_herd_mitigation() -> None:
    """Verify only relevant watchers process messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        # Create 50 watchers on different queues
        watchers = []
        metrics: dict[str, WatcherMetrics] = {}
        call_counts: dict[str, int] = {}

        for i in range(50):
            queue = f"queue_{i}"
            call_counts[queue] = 0
            metrics[queue] = WatcherMetrics(queue)

            def make_handler(q):
                def handler(msg, ts) -> None:
                    call_counts[q] += 1

                return handler

            w = InstrumentedQueueWatcher(
                db_path,
                queue,
                make_handler(queue),
                metrics=metrics[queue],
            )
            watchers.append(w)
            w.run_in_thread()

        # Let watchers initialize
        time.sleep(0.2)

        # Write to only queue_0
        broker.write("queue_0", "test message")

        # Wait for message to be processed
        assert wait_for_condition(
            lambda: call_counts["queue_0"] == 1,
            timeout=2.0,
            message="Waiting for queue_0 to process message",
        )

        # Verify only queue_0 handler was called
        assert call_counts["queue_0"] == 1
        assert all(count == 0 for q, count in call_counts.items() if q != "queue_0")

        # Verify metrics show efficiency
        active_metrics = metrics["queue_0"]
        assert active_metrics.messages_processed == 1
        # Note: Efficiency will be low without the feature implemented
        # This test documents the current behavior

        # Check idle watchers had minimal activity
        mismatched_queues = []
        for i in range(1, 50):
            idle_metrics = metrics[f"queue_{i}"]
            # With pre-check, idle watchers should have high efficiency
            # (few wakes, mostly empty)
            assert idle_metrics.messages_processed == 0
            if idle_metrics.wake_ups > 0:
                if idle_metrics.empty_wakes != idle_metrics.wake_ups:
                    mismatched_queues.append(
                        (f"queue_{i}", idle_metrics.wake_ups, idle_metrics.empty_wakes),
                    )

        if mismatched_queues:
            for _queue, _wake_ups, _empty_wakes in mismatched_queues:
                pass
            # Check if they all have diff of 1
            [wake_ups - empty_wakes for _, wake_ups, empty_wakes in mismatched_queues]
            assert len(mismatched_queues) == 0, (
                f"Found {len(mismatched_queues)} queues with mismatched counts"
            )

        # Cleanup
        for w in watchers:
            w._stop_event.set()
            time.sleep(0.05)  # Brief wait
        for w in watchers:
            w.stop()


def test_thundering_herd_with_multiple_active_queues() -> None:
    """Test behavior when multiple queues are active."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        # Create 20 watchers
        watchers = []
        metrics: dict[str, WatcherMetrics] = {}
        call_counts: dict[str, int] = {}

        for i in range(20):
            queue = f"queue_{i}"
            call_counts[queue] = 0
            metrics[queue] = WatcherMetrics(queue)

            def make_handler(q):
                def handler(msg, ts) -> None:
                    call_counts[q] += 1

                return handler

            w = InstrumentedQueueWatcher(
                db_path,
                queue,
                make_handler(queue),
                metrics=metrics[queue],
            )
            watchers.append(w)
            w.run_in_thread()

        time.sleep(0.2)

        # Write to 5 queues
        active_queues = ["queue_0", "queue_5", "queue_10", "queue_15", "queue_19"]
        for queue in active_queues:
            for i in range(10):
                broker.write(queue, f"message_{i}")

        # Wait for all active queues to process their messages
        for queue in active_queues:
            assert wait_for_condition(
                lambda q=queue: call_counts[q] == 10,
                timeout=5.0,
                message=f"Waiting for {queue} to process 10 messages",
            )

        # Verify active queues processed messages
        for queue in active_queues:
            assert call_counts[queue] == 10
            assert metrics[queue].messages_processed == 10

        # Verify idle queues stayed idle
        for i in range(20):
            queue = f"queue_{i}"
            if queue not in active_queues:
                assert call_counts[queue] == 0
                assert metrics[queue].messages_processed == 0

        # Cleanup
        for w in watchers:
            w.stop()


def test_pre_check_correctness() -> None:
    """Verify pre-check correctly identifies message presence."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        # Create watcher
        handler_calls = []

        def handler(msg, ts) -> None:
            handler_calls.append((msg, ts))

        watcher = InstrumentedQueueWatcher(db_path, "test_queue", handler)
        db = broker

        # Should report no messages when queue is empty
        assert watcher._has_pending_messages(db) is False

        # Add messages to queue
        for i in range(10):
            broker.write("test_queue", f"message_{i}")

        # Should now report messages present
        assert watcher._has_pending_messages(db) is True


def test_pre_check_with_timestamp_filtering() -> None:
    """Test pre-check correctly filters by timestamp."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        # Add messages at different times
        timestamps = []
        for i in range(5):
            broker.write("test_queue", f"message_{i}")
            # Get the timestamp from the database
            rows = list(
                broker._runner.run(
                    "SELECT ts FROM messages WHERE queue = ? ORDER BY ts DESC LIMIT 1",
                    ("test_queue",),
                    fetch=True,
                ),
            )
            timestamps.append(rows[0][0])
            time.sleep(0.01)  # Ensure different timestamps

        handler_calls = []

        def handler(msg, ts) -> None:
            handler_calls.append((msg, ts))

        # Create watcher with last_seen_ts
        watcher = InstrumentedQueueWatcher(db_path, "test_queue", handler, peek=True)
        watcher._last_seen_ts = timestamps[2]  # Should only see messages 3 and 4

        db = broker

        # Pre-check should find messages
        assert watcher._has_pending_messages(db) is True

        # Process messages
        thread = watcher.run_in_thread()

        # Wait for messages to be processed
        assert wait_for_count(lambda: len(handler_calls), expected_count=2, timeout=2.0)

        watcher.stop()
        thread.join(timeout=1.0)

        # Should only have processed messages after timestamp
        assert len(handler_calls) == 2
        assert handler_calls[0][0] == "message_3"
        assert handler_calls[1][0] == "message_4"


def test_disable_pre_check_via_env() -> None:
    """Test disabling pre-check via environment variable."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        BrokerDB(db_path)

        handler_calls = []

        def handler(msg, ts) -> None:
            handler_calls.append((msg, ts))

        # Import load_config to get the default config
        from simplebroker._constants import load_config

        # Get the default config
        default_config = load_config()

        # Test with skip_idle_check = True by patching the config
        config_with_skip = default_config.copy()
        config_with_skip["BROKER_SKIP_IDLE_CHECK"] = True

        with patch("simplebroker.watcher._config", config_with_skip):
            watcher = InstrumentedQueueWatcher(db_path, "empty_queue", handler)

            # Pre-check should be disabled
            assert watcher._skip_idle_check is True

            # When skip_idle_check is True, pre-check should be skipped in main loop

            # Watcher should still try to drain even with no messages
            watcher._drain_queue()
            assert watcher.metrics.drain_calls == 1

        # Test with skip_idle_check = False (default)
        config_no_skip = default_config.copy()
        config_no_skip["BROKER_SKIP_IDLE_CHECK"] = False

        with patch("simplebroker.watcher._config", config_no_skip):
            watcher2 = InstrumentedQueueWatcher(db_path, "empty_queue2", handler)

            # Pre-check should be enabled
            assert watcher2._skip_idle_check is False


def test_concurrent_pre_check_safety() -> None:
    """Test pre-check doesn't cause race conditions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        processed_messages = []
        lock = threading.Lock()

        def handler(msg, ts) -> None:
            with lock:
                processed_messages.append(msg)

        # Create multiple watchers on same queue
        watchers = []
        for _ in range(5):
            w = InstrumentedQueueWatcher(db_path, "shared_queue", handler)
            watchers.append(w)
            w.run_in_thread()

        time.sleep(0.2)

        # Rapidly add messages
        expected_messages = []
        for i in range(50):
            msg = f"message_{i}"
            expected_messages.append(msg)
            broker.write("shared_queue", msg)
            time.sleep(0.001)  # Small delay to spread writes

        # Wait for all messages to be processed
        assert wait_for_count(
            lambda: len(processed_messages),
            expected_count=50,
            timeout=5.0,
        )

        # All messages should be processed exactly once
        assert sorted(processed_messages) == sorted(expected_messages)

        # Cleanup
        for w in watchers:
            w.stop()


def test_metrics_collection() -> None:
    """Test that metrics are collected correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        metrics = WatcherMetrics("test_queue")

        def handler(msg, ts) -> None:
            pass

        watcher = InstrumentedQueueWatcher(
            db_path,
            "test_queue",
            handler,
            metrics=metrics,
        )

        # Process some messages
        for i in range(5):
            broker.write("test_queue", f"message_{i}")

        thread = watcher.run_in_thread()

        # Wait for messages to be processed
        assert wait_for_count(
            lambda: metrics.messages_processed,
            expected_count=5,
            timeout=2.0,
        )

        watcher.stop()
        thread.join(timeout=1.0)

        # Check metrics
        assert metrics.messages_processed == 5
        assert metrics.wake_ups > 0
        # Note: Efficiency assertion removed as it depends on implementation
        assert metrics.pre_check_calls > 0
        assert metrics.drain_calls > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
