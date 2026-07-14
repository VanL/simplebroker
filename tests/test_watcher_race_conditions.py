"""Test suite for QueueWatcher race conditions and concurrency.

Tests to ensure the pre-check optimization doesn't introduce race conditions
or message loss in concurrent scenarios.
"""

from __future__ import annotations

import concurrent.futures
import math
import os
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass

import pytest

from simplebroker._exceptions import OperationalError
from simplebroker.watcher import QueueWatcher

from .helper_scripts.broker_factory import active_backend, make_broker
from .helper_scripts.timing import (
    get_performance_threshold,
    scale_timeout_for_ci,
    wait_for_condition,
)

pytestmark = [pytest.mark.shared]


class ConcurrencyTestWatcher(QueueWatcher):
    """Watcher with hooks for testing concurrent behavior."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._pre_check_enabled = True
        self._pre_check_delay = 0
        self._drain_delay = 0
        self._dispatch_delay = 0
        self.pre_check_count = 0
        self.pre_check_operational_errors = 0
        self.drain_count = 0
        self.dispatch_count = 0
        self.main_loop_entered = threading.Event()
        self._lock = threading.Lock()

    def _has_pending_messages(self) -> bool:
        """Add instrumentation to pre-check."""
        with self._lock:
            self.pre_check_count += 1

        if self._pre_check_delay > 0:
            time.sleep(self._pre_check_delay)

        # Use the parent's implementation which uses Queue API
        return super()._has_pending_messages()

    def _drain_queue(self) -> None:
        """Add instrumentation and optional pre-check."""
        with self._lock:
            self.drain_count += 1

        if self._drain_delay > 0:
            time.sleep(self._drain_delay)

        # Let parent handle the actual draining
        super()._drain_queue()

    def _process_messages(self) -> None:
        """Signal that startup is complete before entering the main loop."""
        self.main_loop_entered.set()
        super()._process_messages()

    def _dispatch(self, message: str, timestamp: int, *, config=None) -> None:
        """Add instrumentation to dispatch."""
        with self._lock:
            self.dispatch_count += 1

        if self._dispatch_delay > 0:
            time.sleep(self._dispatch_delay)

        if config is not None:
            super()._dispatch(message, timestamp, config=config)
        else:
            super()._dispatch(message, timestamp)

    def _process_with_retry(self, process_func, operation_name, *, config=None):
        """Track pre-check OperationalErrors without changing retry behavior."""

        def wrapped():
            try:
                return process_func()
            except OperationalError:
                if operation_name == "pending_messages_check":
                    with self._lock:
                        self.pre_check_operational_errors += 1
                raise

        if config is not None:
            return super()._process_with_retry(wrapped, operation_name, config=config)
        return super()._process_with_retry(wrapped, operation_name)


def _is_windows_sqlite(broker_target) -> bool:
    return sys.platform == "win32" and broker_target.backend_name == "sqlite"


def _watcher_startup_timeout(broker_target) -> float:
    base_timeout = 10.0 if _is_windows_sqlite(broker_target) else 5.0
    return scale_timeout_for_ci(base_timeout)


def _watcher_processing_timeout(
    broker_target,
    *,
    default: float,
    postgres: float | None = None,
    windows_sqlite: float = 15.0,
) -> float:
    if broker_target.backend_name == "postgres" and postgres is not None:
        return scale_timeout_for_ci(postgres)
    if _is_windows_sqlite(broker_target):
        return scale_timeout_for_ci(windows_sqlite)
    return scale_timeout_for_ci(default)


def _ready_watcher_count(watchers: list[ConcurrencyTestWatcher]) -> int:
    return sum(watcher.main_loop_entered.is_set() for watcher in watchers)


def _watcher_thread_states(
    watchers: list[ConcurrencyTestWatcher],
) -> list[tuple[int, bool]]:
    states: list[tuple[int, bool]] = []
    for index, watcher in enumerate(watchers):
        thread_ref = getattr(watcher, "_thread", None)
        thread = thread_ref() if thread_ref is not None else None
        states.append((index, bool(thread is not None and thread.is_alive())))
    return states


def _wait_for_watchers_ready(
    watchers: list[ConcurrencyTestWatcher],
    broker_target,
) -> bool:
    return wait_for_condition(
        lambda: all(watcher.main_loop_entered.is_set() for watcher in watchers),
        timeout=_watcher_startup_timeout(broker_target),
        interval=0.05,
    )


@dataclass(frozen=True)
class ConcurrentPreCheckResult:
    results: list[bool]
    max_concurrent: int
    total_pre_checks: int
    total_pre_check_errors: int


def _run_synchronized_pre_checks(
    broker_target,
    watcher_type: type[ConcurrencyTestWatcher],
    *,
    num_watchers: int = 20,
    warm_thread_connections: bool = False,
) -> ConcurrentPreCheckResult:
    """Run one simultaneous pre-check per watcher and return observed counters."""
    broker = make_broker(broker_target)
    watchers: list[ConcurrencyTestWatcher] = []
    start_barrier = threading.Barrier(num_watchers)
    entered_barrier = threading.Barrier(num_watchers)
    active_lock = threading.Lock()
    active_pre_checks = 0
    max_concurrent = 0

    def handler(msg, ts) -> None:
        del msg, ts

    def run_pre_check(watcher: ConcurrencyTestWatcher) -> bool:
        nonlocal active_pre_checks, max_concurrent
        barrier_timeout = scale_timeout_for_ci(2.0)
        if warm_thread_connections:
            # Persistent watcher queues use thread-local cores. Warm in this
            # worker thread so the timing assertion measures the pre-check
            # query, not first-use connection setup.
            watcher._queue_obj.has_pending(None)
        start_barrier.wait(timeout=barrier_timeout)
        with active_lock:
            active_pre_checks += 1
            max_concurrent = max(max_concurrent, active_pre_checks)
        try:
            entered_barrier.wait(timeout=barrier_timeout)
            return watcher._has_pending_messages()
        finally:
            with active_lock:
                active_pre_checks -= 1

    try:
        broker.write("queue_0", "trigger")
        for i in range(num_watchers):
            watchers.append(watcher_type(f"queue_{i}", handler, db=broker_target))

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_watchers
        ) as executor:
            futures = [executor.submit(run_pre_check, watcher) for watcher in watchers]
            results = [
                future.result(timeout=scale_timeout_for_ci(5.0)) for future in futures
            ]

        return ConcurrentPreCheckResult(
            results=results,
            max_concurrent=max_concurrent,
            total_pre_checks=sum(w.pre_check_count for w in watchers),
            total_pre_check_errors=sum(
                w.pre_check_operational_errors for w in watchers
            ),
        )
    finally:
        for watcher in watchers:
            watcher.stop()
        broker.shutdown()


def test_pre_check_race_no_message_loss(broker_target) -> None:
    """Verify no messages are lost due to pre-check race conditions."""
    broker = make_broker(broker_target)
    watchers = []
    try:
        # Shared state for tracking
        processed_messages = []
        lock = threading.Lock()

        def handler(msg, ts) -> None:
            with lock:
                processed_messages.append((msg, ts))

        # Create multiple watchers on the same queue
        num_watchers = 5
        for _ in range(num_watchers):
            w = ConcurrencyTestWatcher("shared_queue", handler, db=broker_target)
            watchers.append(w)
            w.run_in_thread()

        assert _wait_for_watchers_ready(watchers, broker_target), (
            f"Only {_ready_watcher_count(watchers)}/{len(watchers)} watchers "
            f"entered the main loop: threads={_watcher_thread_states(watchers)}"
        )

        # Rapidly add messages while watchers are running
        expected_messages = []
        for i in range(100):
            msg = f"message_{i}"
            expected_messages.append(msg)
            broker.write("shared_queue", msg)
            if i % 10 == 0:
                time.sleep(0.01)  # Small breaks to vary timing

        # Wait for all messages to be processed
        def processed_count() -> int:
            with lock:
                return len(processed_messages)

        success = wait_for_condition(
            lambda: processed_count() >= len(expected_messages),
            timeout=_watcher_processing_timeout(broker_target, default=5.0),
            interval=0.05,
        )
        if not success:
            pytest.fail(
                "Timed out waiting for watchers: "
                f"processed {processed_count()} of {len(expected_messages)}; "
                f"ready={_ready_watcher_count(watchers)}/{len(watchers)}; "
                f"threads={_watcher_thread_states(watchers)}"
            )

        # Verify all messages were processed exactly once
        processed_bodies = [msg for msg, ts in processed_messages]
        assert sorted(processed_bodies) == sorted(expected_messages)
        assert len(processed_bodies) == len(expected_messages)

        # Check for duplicates
        msg_counts = Counter(processed_bodies)
        for msg, count in msg_counts.items():
            assert count == 1, f"Message {msg} processed {count} times"
    finally:
        # Stop all watchers before closing broker
        for w in watchers:
            w.stop()
        broker.shutdown()


def test_native_activity_hint_still_checks_empty_queue(broker_target) -> None:
    """A native wake-up hint is not proof that this watcher can claim work."""
    watcher = ConcurrencyTestWatcher(
        "empty_queue",
        lambda msg, ts: None,
        db=broker_target,
    )
    try:
        watcher._strategy._native_activity_pending = True

        assert watcher._has_pending_messages() is False
    finally:
        watcher.stop()


def test_native_activity_waiter_wake_still_checks_empty_queue(broker_target) -> None:
    """A native waiter wake should not skip the live pending-message check."""

    class EmptyWakeWaiter:
        def __init__(self) -> None:
            self.wait_calls = 0

        def wait(self, timeout: float) -> bool:
            del timeout
            self.wait_calls += 1
            return self.wait_calls == 1

        def requires_pending_check(self) -> bool:
            return False

        def close(self) -> None:
            pass

    waiter = EmptyWakeWaiter()
    watcher = ConcurrencyTestWatcher(
        "empty_queue",
        lambda msg, ts: None,
        db=broker_target,
    )
    try:
        watcher._strategy.start(activity_waiter=waiter)
        watcher._strategy.wait_for_activity()

        assert waiter.wait_calls == 1
        assert watcher._strategy._activity_burst_remaining == 0
        assert watcher._has_pending_messages() is False
        assert watcher.pre_check_count == 1
    finally:
        watcher.stop()


def test_concurrent_writers_readers(broker_target) -> None:
    """Test concurrent writing and reading with pre-check."""
    broker = make_broker(broker_target)
    watcher = None

    try:
        processed = []
        processed_lock = threading.Lock()

        def handler(msg, ts) -> None:
            with processed_lock:
                processed.append(msg)

        # Create watcher for test reliability
        watcher = ConcurrencyTestWatcher(
            "test_queue",
            handler,
            db=broker_target,
        )
        watcher.run_in_thread()

        # Function to write messages
        def writer_task(writer_id, count) -> None:
            for i in range(count):
                broker.write("test_queue", f"writer_{writer_id}_msg_{i}")
                time.sleep(0.001)  # Small delay between writes

        # Launch concurrent writers
        num_writers = 5
        messages_per_writer = 20

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_writers) as executor:
            futures = []
            for i in range(num_writers):
                future = executor.submit(writer_task, i, messages_per_writer)
                futures.append(future)

            # Wait for all writers to complete
            concurrent.futures.wait(futures)

        # Wait for processing with timeout and checking
        expected_total = num_writers * messages_per_writer
        max_wait = scale_timeout_for_ci(15.0, ci_factor=2.0)
        deadline = time.monotonic() + max_wait

        while time.monotonic() < deadline:
            with processed_lock:
                if len(processed) >= expected_total:
                    break
            time.sleep(0.1)

        # Verify all messages were processed
        with processed_lock:
            processed_snapshot = list(processed)

        assert len(processed_snapshot) == expected_total, (
            f"Only processed {len(processed_snapshot)} messages out of "
            f"{expected_total} after {max_wait:.1f}s"
        )

        # Verify no duplicates
        assert len(set(processed_snapshot)) == expected_total
    finally:
        # Ensure watcher is stopped before closing broker
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_pre_check_drain_race(broker_target) -> None:
    """Test race between pre-check and actual drain."""
    broker = make_broker(broker_target)
    watcher = None

    try:
        processed = []

        def handler(msg, ts) -> None:
            processed.append(msg)

        # Create watcher with delay to increase race window
        watcher = ConcurrencyTestWatcher("test_queue", handler, db=broker_target)
        watcher._pre_check_delay = 0.01  # 10ms delay after pre-check
        watcher.run_in_thread()

        # Function to consume messages from another connection
        def consume_messages():
            # Use a separate broker instance
            other_broker = make_broker(broker_target)
            try:
                consumed = []
                # Use claim_generator to consume all messages
                for msg in other_broker.claim_generator(
                    "test_queue", with_timestamps=False
                ):
                    consumed.append(msg)
                return consumed
            finally:
                other_broker.shutdown()

        # Add messages
        for i in range(50):
            broker.write("test_queue", f"message_{i}")

        # Start consuming from another thread during watcher operation
        with concurrent.futures.ThreadPoolExecutor() as executor:
            consume_future = executor.submit(consume_messages)

            # Let watcher run
            time.sleep(1.0)

            # Get messages consumed by other thread
            other_consumed = consume_future.result()

            # Total messages processed should equal messages written
            total_processed = len(processed) + len(other_consumed)
            assert total_processed == 50

            # Verify no message was processed twice
            all_messages = processed + other_consumed
            assert len(set(all_messages)) == total_processed
    finally:
        # Ensure watcher is stopped before closing broker
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_multiple_queues_concurrent_activity(broker_target) -> None:
    """Test multiple queues with concurrent activity."""
    broker = make_broker(broker_target)
    watchers = []

    try:
        num_queues = 10
        messages_per_queue = 50

        # Track processed messages per queue
        processed_by_queue: dict[str, list[str]] = {}
        queue_locks: dict[str, threading.Lock] = {}

        for i in range(num_queues):
            queue = f"queue_{i}"
            processed_by_queue[queue] = []
            queue_locks[queue] = threading.Lock()

        # Create watchers
        for i in range(num_queues):
            queue = f"queue_{i}"

            def make_handler(q):
                def handler(msg, ts) -> None:
                    with queue_locks[q]:
                        processed_by_queue[q].append(msg)

                return handler

            w = ConcurrencyTestWatcher(queue, make_handler(queue), db=broker_target)
            watchers.append(w)
            w.run_in_thread()

        wait_for_condition(
            lambda: all(w.drain_count > 0 for w in watchers),
            timeout=scale_timeout_for_ci(5.0),
            interval=0.01,
            message="All watchers should enter their initial drain before writes",
        )

        # Concurrent writer function
        def write_to_queue(queue_name, start_idx) -> None:
            for i in range(messages_per_queue):
                broker.write(queue_name, f"{queue_name}_msg_{start_idx + i}")
                if i % 10 == 0:
                    time.sleep(0.001)

        # Launch concurrent writes to all queues
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_queues) as executor:
            futures = []
            for i in range(num_queues):
                queue = f"queue_{i}"
                future = executor.submit(write_to_queue, queue, i * 1000)
                futures.append(future)

            concurrent.futures.wait(futures)
            for future in futures:
                future.result()

        # Wait for processing - give more time on slower systems
        success = wait_for_condition(
            lambda: all(
                len(processed_by_queue[f"queue_{i}"]) >= messages_per_queue
                for i in range(num_queues)
            ),
            timeout=scale_timeout_for_ci(15.0),
            interval=0.1,
        )
        if not success:
            deficits = {
                queue: len(messages)
                for queue, messages in processed_by_queue.items()
                if len(messages) < messages_per_queue
            }
            pytest.fail(
                "Not all queues drained in time: "
                + ", ".join(
                    f"{queue}={count}/{messages_per_queue}"
                    for queue, count in deficits.items()
                )
            )

        # Verify each queue processed its messages
        for i in range(num_queues):
            queue = f"queue_{i}"
            messages = processed_by_queue[queue]
            assert len(messages) == messages_per_queue, (
                f"Queue {queue} only processed {len(messages)} messages, expected {messages_per_queue}"
            )

            # Verify correct messages for this queue
            for msg in messages:
                assert msg.startswith(f"{queue}_msg_")
    finally:
        # Stop all watchers before closing broker
        for w in watchers:
            w.stop()
        broker.shutdown()


def test_watcher_stop_during_pre_check(broker_target) -> None:
    """Test stopping watcher during pre-check doesn't hang."""
    broker = make_broker(broker_target)
    watcher = None

    try:
        processed = []

        def handler(msg, ts) -> None:
            processed.append(msg)

        # Create watcher with long pre-check delay
        watcher = ConcurrencyTestWatcher("test_queue", handler, db=broker_target)
        watcher._pre_check_delay = 0.5  # 500ms delay

        # Add messages
        for i in range(10):
            broker.write("test_queue", f"message_{i}")

        # Start watcher
        thread = watcher.run_in_thread()

        # Wait a bit then stop during pre-check
        time.sleep(0.1)
        start_stop = time.monotonic()
        stop_timeout = scale_timeout_for_ci(1.0)
        watcher.stop(timeout=stop_timeout)
        stop_duration = time.monotonic() - start_stop

        # stop(timeout=...) should not hang even if the thread needs a short
        # cleanup window after the bounded join returns.
        assert stop_duration < stop_timeout + scale_timeout_for_ci(0.5)
        assert wait_for_condition(
            lambda: not thread.is_alive(),
            timeout=scale_timeout_for_ci(5.0),
            interval=0.05,
            message="Watcher thread should finish after stop during pre-check",
        )
    finally:
        # Watcher already stopped in test, but ensure it's stopped
        if (
            watcher is not None
            and hasattr(watcher, "_stop_event")
            and not watcher._stop_event.is_set()
        ):
            watcher.stop()
        broker.shutdown()


def test_pre_check_with_peek_mode(broker_target) -> None:
    """Test pre-check behavior with peek mode."""
    broker = make_broker(broker_target)
    watcher = None

    try:
        peek_count = 0
        peek_lock = threading.Lock()

        def handler(msg, ts) -> None:
            nonlocal peek_count
            with peek_lock:
                peek_count += 1

        # Create peek watcher
        watcher = ConcurrencyTestWatcher(
            "test_queue",
            handler,
            db=broker_target,
            peek=True,
        )
        watcher.run_in_thread()

        # Add a message
        broker.write("test_queue", "test_message")

        # Wait for the asynchronous watcher thread to observe the message.
        # A fixed sleep is flaky under suite-wide CPU contention.
        assert wait_for_condition(
            lambda: peek_count == 1,
            timeout=scale_timeout_for_ci(2.0),
            interval=0.01,
        )

        # Message should still be in queue (use non-destructive peek)
        check_db = make_broker(broker_target)
        try:
            messages = list(
                check_db.peek_generator("test_queue", with_timestamps=False)
            )
        finally:
            check_db.shutdown()
        assert len(messages) == 1
        assert messages[0] == "test_message"
    finally:
        # Ensure watcher is stopped before closing broker
        if watcher is not None:
            watcher.stop()
        broker.shutdown()


def test_concurrent_pre_checks(broker_target) -> None:
    """Multiple watchers can pre-check concurrently without retry errors."""
    result = _run_synchronized_pre_checks(
        broker_target,
        ConcurrencyTestWatcher,
    )

    assert result.max_concurrent == 20
    assert result.total_pre_checks == 20
    assert result.total_pre_check_errors == 0
    assert result.results.count(True) == 1
    assert result.results.count(False) == 19


@pytest.mark.benchmark
def test_concurrent_pre_check_timing(broker_target) -> None:
    """Track pre-check latency separately from deterministic concurrency semantics."""
    pre_check_times: list[float] = []
    times_lock = threading.Lock()

    class TimingWatcher(ConcurrencyTestWatcher):
        def _has_pending_messages(self) -> bool:
            start = time.perf_counter()
            result = super()._has_pending_messages()
            elapsed = time.perf_counter() - start
            with times_lock:
                pre_check_times.append(elapsed)
            return result

    result = _run_synchronized_pre_checks(
        broker_target,
        TimingWatcher,
        warm_thread_connections=True,
    )

    assert result.total_pre_check_errors == 0
    assert len(pre_check_times) == 20

    sorted_times = sorted(pre_check_times)

    def percentile(values: list[float], p: float) -> float:
        index = min(len(values) - 1, math.ceil(len(values) * p) - 1)
        return values[index]

    avg_time = sum(pre_check_times) / len(pre_check_times)
    p99_time = percentile(sorted_times, 0.99)
    is_pg = active_backend() == "postgres"
    # PostgreSQL pre-checks include 20 simultaneous network round trips through
    # the process-local runner pool. Keep the contract bounded, but do not apply
    # the SQLite micro-query budget to the PG backend.
    avg_default = 0.5 if is_pg else 0.01
    p99_default = 2.0 if is_pg else (0.04 if os.environ.get("CI") else 0.025)
    avg_threshold = get_performance_threshold(
        "WATCHER_PRECHECK_AVG_SECONDS",
        avg_default,
        scale_for_slow_runner=True,
        calibration_name="write_test",
    )
    p99_threshold = get_performance_threshold(
        "WATCHER_PRECHECK_P99_SECONDS",
        p99_default,
        scale_for_slow_runner=True,
        calibration_name="write_test",
    )

    assert avg_time < avg_threshold
    assert p99_time < p99_threshold


def test_pre_check_database_contention(broker_target) -> None:
    """Test pre-check performance under database contention."""
    broker = make_broker(broker_target)
    watchers = []

    try:
        processed_counts = {}
        processed_total = 0
        all_processed = threading.Event()
        writer_errors: list[BaseException] = []

        # Create watchers
        num_watchers = 10
        queue_names = [f"queue_{i}" for i in range(num_watchers)]
        messages_per_queue = 5 if broker_target.backend_name == "postgres" else 10
        expected_messages = num_watchers * messages_per_queue

        processed_lock = threading.Lock()

        for queue in queue_names:
            processed_counts[queue] = 0

            def make_handler(q):
                def handler(msg, ts) -> None:
                    nonlocal processed_total
                    with processed_lock:
                        processed_counts[q] += 1
                        processed_total += 1
                        if processed_total >= expected_messages:
                            all_processed.set()

                return handler

            w = ConcurrencyTestWatcher(queue, make_handler(queue), db=broker_target)
            watchers.append(w)
            w.run_in_thread()

        assert _wait_for_watchers_ready(watchers, broker_target), (
            f"Only {_ready_watcher_count(watchers)}/{len(watchers)} watchers "
            f"entered the main loop: threads={_watcher_thread_states(watchers)}"
        )

        def processing_snapshot() -> tuple[int, dict[str, int]]:
            with processed_lock:
                return processed_total, dict(processed_counts)

        def create_contention() -> None:
            try:
                for round_idx in range(messages_per_queue):
                    for queue in queue_names:
                        broker.write(queue, f"contention_message_{round_idx}_{queue}")
                    time.sleep(0)
            except BaseException as exc:
                writer_errors.append(exc)

        contention_thread = threading.Thread(target=create_contention)
        contention_thread.start()

        writer_timeout = (
            60.0 if broker_target.backend_name in {"postgres", "redis"} else 20.0
        )
        contention_thread.join(timeout=scale_timeout_for_ci(writer_timeout))
        assert not contention_thread.is_alive(), (
            "Writer thread did not complete while competing with watcher reads"
        )
        if writer_errors:
            raise AssertionError("Writer thread failed") from writer_errors[0]

        processing_timeout = _watcher_processing_timeout(
            broker_target,
            default=5.0,
            postgres=30.0,
        )
        assert all_processed.wait(timeout=processing_timeout), (
            "Timed out waiting for watcher processing under contention: "
            f"processed={processing_snapshot()}; "
            f"ready={_ready_watcher_count(watchers)}/{len(watchers)}; "
            f"threads={_watcher_thread_states(watchers)}"
        )

        # Verify all watchers still functioned under contention
        total_processed, final_counts = processing_snapshot()
        assert total_processed == expected_messages, (
            f"All messages should be processed exactly once: counts={final_counts}"
        )

        # Check pre-check efficiency
        total_pre_checks = sum(w.pre_check_count for w in watchers)
        total_drains = sum(w.drain_count for w in watchers)

        # Initial startup drains are intentionally not pre-checked. After
        # startup, every drain should be gated by a pre-check, and pre-checks
        # should keep drain amplification far below "every watcher drains for
        # every write" behavior.
        assert total_pre_checks >= total_drains - num_watchers
        assert total_drains < (total_processed * num_watchers) / 2
    finally:
        # Stop all watchers before closing broker
        for w in watchers:
            w.stop()
        broker.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
