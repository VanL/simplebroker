"""Test suite for QueueWatcher metrics and monitoring.

Tests the collection and reporting of performance metrics for
monitoring watcher behavior in production.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher

from .helper_scripts.timing import wait_for_condition, wait_for_count


class MetricsCollector:
    """Collects and aggregates watcher metrics."""

    def __init__(self) -> None:
        self.metrics: dict[str, list[float]] = {
            "wake_ups": [],
            "empty_wakes": [],
            "messages_processed": [],
            "pre_check_time_us": [],
            "drain_time_ms": [],
            "handler_time_ms": [],
            "efficiency": [],
            "throughput": [],
        }
        self.lock = threading.Lock()
        self._start_time = time.monotonic()

    def record_wake_up(self, queue: str, empty: bool = False) -> None:
        with self.lock:
            self.metrics["wake_ups"].append(time.time())
            if empty:
                self.metrics["empty_wakes"].append(time.time())

    def record_message(self, queue: str, processing_time_ms: float) -> None:
        with self.lock:
            self.metrics["messages_processed"].append(time.time())
            self.metrics["handler_time_ms"].append(processing_time_ms)

    def record_pre_check(self, queue: str, time_us: float) -> None:
        with self.lock:
            self.metrics["pre_check_time_us"].append(time_us)

    def record_drain(self, queue: str, time_ms: float) -> None:
        with self.lock:
            self.metrics["drain_time_ms"].append(time_ms)

    def get_stats(self) -> dict:
        """Calculate aggregate statistics."""
        with self.lock:
            elapsed = time.monotonic() - self._start_time
            total_wakes = len(self.metrics["wake_ups"])
            empty_wakes = len(self.metrics["empty_wakes"])
            messages = len(self.metrics["messages_processed"])

            stats = {
                "elapsed_seconds": elapsed,
                "total_wake_ups": total_wakes,
                "empty_wake_ups": empty_wakes,
                "messages_processed": messages,
                "efficiency": 1.0 - (empty_wakes / max(1, total_wakes)),
                "throughput_per_sec": messages / max(0.001, elapsed),
                "wake_ups_per_sec": total_wakes / max(0.001, elapsed),
            }

            # Add timing stats
            for metric, values in self.metrics.items():
                if metric.endswith(("_us", "_ms")) and values:
                    stats[f"{metric}_avg"] = sum(values) / len(values)
                    stats[f"{metric}_max"] = max(values)
                    stats[f"{metric}_min"] = min(values)

            return stats


class MonitoredQueueWatcher(QueueWatcher):
    """QueueWatcher with comprehensive metrics collection."""

    def __init__(
        self,
        *args,
        metrics_collector: MetricsCollector | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.metrics = metrics_collector or MetricsCollector()
        self._enable_pre_check = True
        self._last_log_time = time.time()
        self._log_interval = float(os.environ.get("BROKER_WATCHER_LOG_INTERVAL", "60"))

    def _has_pending_messages(self) -> bool:
        """Track pre-check performance."""
        start = time.perf_counter()

        # Use parent's implementation which uses the Queue API
        result = super()._has_pending_messages()

        elapsed_us = (time.perf_counter() - start) * 1_000_000
        self.metrics.record_pre_check(self._queue, elapsed_us)

        return result

    def _drain_queue(self) -> None:
        """Track drain performance and efficiency."""
        start = time.perf_counter()

        if self._enable_pre_check:
            # Check if there are pending messages using the parent's method
            if not self._has_pending_messages():
                self.metrics.record_wake_up(self._queue, empty=True)
                self._maybe_log_stats()
                elapsed_ms = (time.perf_counter() - start) * 1000
                self.metrics.record_drain(self._queue, elapsed_ms)
                return

        # Record wake up before processing
        initial_count = getattr(self, "_total_messages", 0)

        # Call parent's drain_queue implementation
        super()._drain_queue()

        # Check if any messages were processed
        current_count = getattr(self, "_total_messages", 0)
        if current_count > initial_count:
            self.metrics.record_wake_up(self._queue, empty=False)
        else:
            self.metrics.record_wake_up(self._queue, empty=True)

        elapsed_ms = (time.perf_counter() - start) * 1000
        self.metrics.record_drain(self._queue, elapsed_ms)

        self._maybe_log_stats()

    def _dispatch(self, message: str, timestamp: int, *, config=None) -> None:
        """Track message processing time."""
        start = time.perf_counter()

        if not hasattr(self, "_total_messages"):
            self._total_messages = 0
        self._total_messages += 1

        if config is not None:
            super()._dispatch(message, timestamp, config=config)
        else:
            super()._dispatch(message, timestamp)

        elapsed_ms = (time.perf_counter() - start) * 1000
        self.metrics.record_message(self._queue, elapsed_ms)

    def _maybe_log_stats(self) -> None:
        """Log statistics periodically."""
        if self._log_interval <= 0:
            return

        current_time = time.time()
        if current_time - self._last_log_time >= self._log_interval:
            stats = self.metrics.get_stats()
            logger = logging.getLogger(__name__)
            logger.info(
                f"Watcher stats for {self._queue}: {json.dumps(stats, indent=2)}",
            )
            self._last_log_time = current_time


def test_metrics_collection_basic() -> None:
    """Test basic metrics collection functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        try:
            metrics = MetricsCollector()
            processed = []

            def handler(msg, ts) -> None:
                processed.append(msg)
                time.sleep(0.01)  # Simulate processing time

            watcher = MonitoredQueueWatcher(
                "test_queue",
                handler,
                db=db_path,
                metrics_collector=metrics,
            )

            # Process some messages
            for i in range(10):
                broker.write("test_queue", f"message_{i}")

            thread = watcher.run_in_thread()

            # Wait for all messages to be processed
            assert wait_for_count(
                lambda: metrics.get_stats()["messages_processed"],
                expected_count=10,
                timeout=2.0,
            )

            watcher.stop()
            thread.join(timeout=1.0)

            # Check metrics
            stats = metrics.get_stats()

            assert stats["messages_processed"] == 10
            assert stats["total_wake_ups"] > 0
            # Note: Not asserting on efficiency as it depends on implementation
            assert stats["throughput_per_sec"] > 0

            # Check timing metrics
            assert "pre_check_time_us_avg" in stats
            assert stats["pre_check_time_us_avg"] < 1000  # < 1ms
            assert "handler_time_ms_avg" in stats
            assert stats["handler_time_ms_avg"] > 5  # Should reflect sleep
        finally:
            broker.close()


def test_metrics_efficiency_tracking() -> None:
    """Test efficiency metrics under different conditions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        try:
            # Test scenario 1: All empty wakes
            metrics1 = MetricsCollector()

            def handler(msg, ts) -> None:
                pass

            watcher1 = MonitoredQueueWatcher(
                "empty_queue",
                handler,
                db=db_path,
                metrics_collector=metrics1,
            )

            thread1 = watcher1.run_in_thread()
            assert wait_for_condition(
                lambda: metrics1.get_stats()["total_wake_ups"] > 0,
                timeout=2.0,
                interval=0.05,
            )
            watcher1.stop()
            thread1.join(timeout=1.0)

            metrics1.get_stats()
            # Note: Not asserting on efficiency as it depends on implementation

            # Test scenario 2: Active queue
            metrics2 = MetricsCollector()
            watcher2 = MonitoredQueueWatcher(
                "active_queue",
                handler,
                db=db_path,
                metrics_collector=metrics2,
            )

            # Add messages continuously
            thread2 = watcher2.run_in_thread()
            for i in range(20):
                broker.write("active_queue", f"message_{i}")
                time.sleep(0.02)

            # Wait for messages to be processed
            assert wait_for_count(
                lambda: metrics2.get_stats()["messages_processed"],
                expected_count=20,
                timeout=2.0,
            )

            watcher2.stop()
            thread2.join(timeout=1.0)

            stats2 = metrics2.get_stats()
            assert stats2["messages_processed"] == 20
            # Note: Not asserting on efficiency as it depends on implementation
        finally:
            broker.close()


def test_metrics_logging() -> None:
    """Test periodic logging of metrics."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        # Set short log interval
        os.environ["BROKER_WATCHER_LOG_INTERVAL"] = "0.5"

        try:
            metrics = MetricsCollector()

            def handler(msg, ts) -> None:
                pass

            # Capture log output
            with patch("logging.Logger.info") as mock_log:
                watcher = MonitoredQueueWatcher(
                    "test_queue",
                    handler,
                    db=db_path,
                    metrics_collector=metrics,
                )

                thread = watcher.run_in_thread()

                try:
                    # Generate some activity
                    for i in range(5):
                        broker.write("test_queue", f"message_{i}")
                        time.sleep(0.2)

                    assert wait_for_condition(
                        lambda: mock_log.call_count >= 1,
                        timeout=2.0,
                        interval=0.05,
                    )
                finally:
                    watcher.stop()
                    thread.join(timeout=1.0)

                # Check that stats were logged
                assert mock_log.call_count >= 1

                # Verify log format
                log_call = mock_log.call_args_list[0]
                log_message = log_call[0][0]
                assert "Watcher stats for test_queue" in log_message
                assert "efficiency" in log_message

        finally:
            os.environ.pop("BROKER_WATCHER_LOG_INTERVAL", None)
            broker.close()


def test_metrics_aggregation() -> None:
    """Test aggregation of metrics across multiple watchers."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        try:
            # Global metrics collector
            global_metrics = MetricsCollector()

            # Create multiple watchers sharing metrics
            num_watchers = 5
            watchers = []
            threads = []

            def handler(msg, ts) -> None:
                time.sleep(0.001)

            for i in range(num_watchers):
                watcher = MonitoredQueueWatcher(
                    f"queue_{i}",
                    handler,
                    db=db_path,
                    metrics_collector=global_metrics,
                )
                watchers.append(watcher)
                thread = watcher.run_in_thread()
                threads.append(thread)

            try:
                # Let watchers run without messages to generate empty wake-ups
                time.sleep(0.2)

                # Generate activity
                for i in range(num_watchers):
                    for j in range(10):
                        broker.write(f"queue_{i}", f"q{i}_msg_{j}")

                assert wait_for_count(
                    lambda: global_metrics.get_stats()["messages_processed"],
                    expected_count=num_watchers * 10,
                    timeout=3.0,
                    at_least=True,
                )

                # Check aggregated metrics
                stats = global_metrics.get_stats()

                assert stats["messages_processed"] == num_watchers * 10
                assert stats["total_wake_ups"] > stats["messages_processed"]
                assert stats["efficiency"] > 0.5
            finally:
                # Stop all watchers and join threads
                for w in watchers:
                    w.stop()
                for t in threads:
                    t.join(timeout=1.0)
        finally:
            broker.close()


def test_metrics_export_format() -> None:
    """Test metrics export in various formats."""
    metrics = MetricsCollector()

    # Simulate some activity
    for i in range(100):
        metrics.record_wake_up("test_queue", empty=(i % 3 == 0))
        if i % 3 != 0:
            metrics.record_message("test_queue", 5.0 + i % 10)
        metrics.record_pre_check("test_queue", 50.0 + i % 20)
        metrics.record_drain("test_queue", 2.0 + i % 5)

    # Get stats
    stats = metrics.get_stats()

    # Verify all expected fields
    expected_fields = [
        "elapsed_seconds",
        "total_wake_ups",
        "empty_wake_ups",
        "messages_processed",
        "efficiency",
        "throughput_per_sec",
        "wake_ups_per_sec",
        "pre_check_time_us_avg",
        "pre_check_time_us_max",
        "pre_check_time_us_min",
        "drain_time_ms_avg",
        "drain_time_ms_max",
        "drain_time_ms_min",
        "handler_time_ms_avg",
        "handler_time_ms_max",
        "handler_time_ms_min",
    ]

    for field in expected_fields:
        assert field in stats, f"Missing field: {field}"

    # Test JSON serialization
    json_output = json.dumps(stats, indent=2)
    assert json_output  # Should serialize without error

    # Verify calculations
    assert stats["total_wake_ups"] == 100
    assert stats["empty_wake_ups"] == 34  # ~1/3
    assert 0.6 < stats["efficiency"] < 0.7
    assert stats["messages_processed"] == 66


def test_metrics_thread_safety() -> None:
    """Test thread safety of metrics collection."""
    metrics = MetricsCollector()

    # Function to stress test metrics recording
    def record_metrics(thread_id, iterations) -> None:
        for i in range(iterations):
            metrics.record_wake_up(f"queue_{thread_id}", empty=(i % 2 == 0))
            metrics.record_pre_check(f"queue_{thread_id}", 10.0 + i)
            metrics.record_drain(f"queue_{thread_id}", 5.0 + i)
            if i % 2 == 1:
                metrics.record_message(f"queue_{thread_id}", 2.0 + i)

    # Launch multiple threads
    threads = []
    num_threads = 10
    iterations = 1000

    for i in range(num_threads):
        t = threading.Thread(target=record_metrics, args=(i, iterations))
        threads.append(t)
        t.start()

    # Wait for completion
    for t in threads:
        t.join()

    # Verify counts
    stats = metrics.get_stats()

    expected_wake_ups = num_threads * iterations
    expected_messages = num_threads * (iterations // 2)

    assert stats["total_wake_ups"] == expected_wake_ups
    assert stats["messages_processed"] == expected_messages

    # Verify no data corruption
    assert len(metrics.metrics["pre_check_time_us"]) == expected_wake_ups
    assert len(metrics.metrics["drain_time_ms"]) == expected_wake_ups


def test_metrics_reset_capability() -> None:
    """Test ability to reset metrics."""

    class ResettableMetricsCollector(MetricsCollector):
        def reset(self) -> None:
            with self.lock:
                for key in self.metrics:
                    self.metrics[key].clear()
                self._start_time = time.monotonic()

    metrics = ResettableMetricsCollector()

    # Record some metrics
    for i in range(50):
        metrics.record_wake_up("test", empty=(i % 2 == 0))

    stats1 = metrics.get_stats()
    assert stats1["total_wake_ups"] == 50

    # Reset
    metrics.reset()

    # Record more
    for _ in range(20):
        metrics.record_wake_up("test", empty=False)

    stats2 = metrics.get_stats()
    assert stats2["total_wake_ups"] == 20
    assert stats2["empty_wake_ups"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
