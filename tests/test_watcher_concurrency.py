"""Concurrency tests for the watcher feature."""

import contextlib
import json
import sys
import threading
import time
import traceback
import warnings

import pytest

from simplebroker._exceptions import OperationalError
from simplebroker._targets import BrokerTarget

pytest.importorskip("simplebroker.watcher")
from simplebroker.watcher import QueueWatcher

from .helper_scripts.broker_factory import make_broker
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition
from .helper_scripts.watcher_base import WatcherTestBase

pytestmark = [pytest.mark.shared]


class ConcurrentCollector:
    """Thread-safe collector for concurrent testing."""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.messages: list[tuple[str, int]] = []
        self.lock = threading.Lock()
        self.processing_times: dict[str, float] = {}

    def handler(self, msg: str, ts: int) -> None:
        """Collect messages with timing info."""
        start_time = time.monotonic()
        with self.lock:
            self.messages.append((msg, ts))
            self.processing_times[msg] = start_time

    def get_messages(self) -> list[str]:
        """Get just the message bodies."""
        with self.lock:
            return [msg for msg, _ in self.messages]


def wait_for_queue_drain(
    broker_target: BrokerTarget,
    queue_name: str,
    collectors: list[ConcurrentCollector],
    expected_total: int,
    *,
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> None:
    """Wait until the queue is empty and all expected messages are collected."""
    deadline = time.perf_counter() + timeout

    while time.perf_counter() < deadline:
        total_collected = sum(len(collector.get_messages()) for collector in collectors)

        if total_collected >= expected_total:
            db = make_broker(broker_target)
            try:
                stats = {
                    name: unclaimed for name, unclaimed, _total in db.get_queue_stats()
                }
                if stats.get(queue_name, 0) == 0:
                    return
            finally:
                db.shutdown()

        time.sleep(poll_interval)

    distribution = {
        collector.worker_id: len(collector.get_messages()) for collector in collectors
    }
    db = make_broker(broker_target)
    try:
        stats_snapshot = db.get_queue_stats()
    finally:
        db.shutdown()

    pytest.fail(
        "Timeout waiting for queue to drain: "
        f"expected {expected_total}, got {sum(distribution.values())}; "
        f"distribution={distribution}; stats={stats_snapshot}"
    )


class TestWorkerPool(WatcherTestBase):
    """Test worker pool scenarios."""

    def test_worker_pool_with_slow_handlers(self, broker_target):
        """Test worker pool with varying processing speeds.

        Runs in the regular suite: the handler sleeps are deliberate (a
        second or two total) and the assertions are behavioral, not
        timing-based.
        """
        num_messages = 20

        db = make_broker(broker_target)
        try:
            for i in range(num_messages):
                db.write(
                    "jobs",
                    json.dumps({"id": i, "work_time": 0.05 if i % 3 == 0 else 0.01}),
                )
        finally:
            db.shutdown()

        processed = []
        processed_lock = threading.Lock()
        all_processed = threading.Event()

        def slow_handler(msg: str, ts: int):
            """Handler with variable processing time."""
            data = json.loads(msg)
            time.sleep(data["work_time"])
            with processed_lock:
                processed.append(data["id"])
                if len(processed) >= num_messages:
                    all_processed.set()

        def processed_snapshot() -> list[int]:
            with processed_lock:
                return list(processed)

        # Create 3 workers
        workers = []
        try:
            for _i in range(3):
                watcher = QueueWatcher(
                    "jobs",
                    slow_handler,
                    db=broker_target,
                )
                thread = watcher.run_in_thread()
                workers.append((watcher, thread))

            assert all_processed.wait(timeout=scale_timeout_for_ci(10.0)), (
                "Timed out waiting for worker pool to process messages: "
                f"processed={processed_snapshot()}"
            )

        finally:
            # Ensure all workers are cleaned up
            for watcher, _thread in workers:
                # Ignore stop errors
                with contextlib.suppress(Exception):
                    watcher.stop()

            for _watcher, thread in workers:
                # Ignore join errors during cleanup
                with contextlib.suppress(Exception):
                    thread.join(timeout=5.0)

        # Should have processed all messages
        snapshot = processed_snapshot()
        assert len(snapshot) == num_messages
        assert set(snapshot) == set(range(num_messages))

    def test_worker_joins_late(self, broker_target):
        """Test worker joining after others have started."""
        # Start with 2 workers
        collectors = []
        workers = []

        try:
            for i in range(2):
                collector = ConcurrentCollector(f"early_worker_{i}")
                collectors.append(collector)

                watcher = QueueWatcher(
                    "dynamic_queue",
                    collector.handler,
                    db=broker_target,
                )
                thread = watcher.run_in_thread()
                workers.append((watcher, thread))

            # Add some messages
            db = make_broker(broker_target)
            try:
                for i in range(50):
                    db.write("dynamic_queue", f"early_msg_{i}")
            finally:
                db.shutdown()

            time.sleep(0.5)

            # Add a late worker
            late_collector = ConcurrentCollector("late_worker")
            collectors.append(late_collector)

            late_watcher = QueueWatcher(
                "dynamic_queue",
                late_collector.handler,
                db=broker_target,
            )
            late_thread = late_watcher.run_in_thread()
            workers.append((late_watcher, late_thread))

            assert wait_for_condition(
                late_watcher.is_running,
                timeout=scale_timeout_for_ci(5.0),
                message="late watcher did not reach its public running state",
            )

            # Add more messages
            db = make_broker(broker_target)
            try:
                for i in range(50):
                    db.write("dynamic_queue", f"late_msg_{i}")
            finally:
                db.shutdown()

            wait_for_queue_drain(
                broker_target,
                "dynamic_queue",
                collectors,
                expected_total=100,
                timeout=scale_timeout_for_ci(10.0, ci_factor=3.0),
            )

        finally:
            # Ensure all workers are cleaned up
            for watcher, _thread in workers:
                # Ignore stop errors
                with contextlib.suppress(Exception):
                    watcher.stop()

            for _watcher, thread in workers:
                # Ignore join errors during cleanup
                with contextlib.suppress(Exception):
                    thread.join(timeout=5.0)

        # Verify all messages processed
        all_messages = []
        for collector in collectors:
            all_messages.extend(collector.get_messages())

        expected = {
            *(f"early_msg_{index}" for index in range(50)),
            *(f"late_msg_{index}" for index in range(50)),
        }
        assert len(all_messages) == len(expected)
        assert set(all_messages) == expected


class TestMixedMode(WatcherTestBase):
    """Test mixed peek and read watchers."""

    def test_mixed_peek_read_basic(self, broker_target):
        """Test basic mixed mode operation."""
        peek_messages = []
        read_messages: list[str] = []
        peek_lock = threading.Lock()
        read_lock = threading.Lock()

        def read_count() -> int:
            with read_lock:
                return len(read_messages)

        def peek_handler(msg: str, ts: int):
            with peek_lock:
                peek_messages.append(msg)

        def read_handler(msg: str, ts: int):
            time.sleep(0.01)  # Simulate work
            with read_lock:
                read_messages.append(msg)

        # Watchers to clean up
        peek_watcher = None
        read_watcher = None
        peek_thread = None
        read_thread = None

        try:
            # Start peek watcher
            peek_watcher = QueueWatcher(
                "mixed",
                peek_handler,
                db=broker_target,
                peek=True,
            )
            peek_thread = peek_watcher.run_in_thread()

            # Start read watcher
            read_watcher = QueueWatcher(
                "mixed",
                read_handler,
                db=broker_target,
                peek=False,
            )
            read_thread = read_watcher.run_in_thread()

            assert wait_for_condition(
                lambda: peek_watcher.is_running() and read_watcher.is_running(),
                timeout=scale_timeout_for_ci(5.0),
                message="mixed-mode watchers did not reach running state",
            )

            # Add messages
            db = make_broker(broker_target)
            try:
                for i in range(10):
                    db.write("mixed", f"msg_{i}")
            finally:
                db.shutdown()

            # Wait for the consuming watcher to actually drain the queue.
            assert wait_for_condition(
                lambda: read_count() == 10,
                timeout=scale_timeout_for_ci(3.0),
                interval=0.05,
            ), f"Timed out waiting for 10 consumed messages, got {read_count()}"

        finally:
            # Cleanup all resources
            if peek_watcher:
                with contextlib.suppress(Exception):
                    peek_watcher.stop()
            if read_watcher:
                with contextlib.suppress(Exception):
                    read_watcher.stop()

            if peek_thread:
                with contextlib.suppress(Exception):
                    peek_thread.join(timeout=5.0)
            if read_thread:
                with contextlib.suppress(Exception):
                    read_thread.join(timeout=5.0)

        # All messages should be read (consumed)
        assert len(read_messages) == 10
        assert set(read_messages) == {f"msg_{i}" for i in range(10)}

        db = make_broker(broker_target)
        try:
            stats = {
                name: unclaimed for name, unclaimed, _total in db.get_queue_stats()
            }
            assert stats.get("mixed", 0) == 0
        finally:
            db.shutdown()

        expected = {f"msg_{i}" for i in range(10)}
        assert set(peek_messages) <= expected
        assert len(peek_messages) == len(set(peek_messages))

    def test_multiple_peek_watchers(self, broker_target):  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-033] exception
        """Test multiple peek watchers see same messages."""
        num_peekers = 3
        expected_messages = [f"broadcast_{i}" for i in range(5)]
        collectors = []
        watchers = []

        try:
            # Create multiple peek watchers
            for _i in range(num_peekers):
                messages: list[str] = []
                lock = threading.Lock()

                def make_handler(m, lck):
                    def handler(msg: str, ts: int):
                        with lck:
                            m.append(msg)

                    return handler

                watcher = QueueWatcher(
                    "broadcast",
                    make_handler(messages, lock),
                    db=broker_target,
                    peek=True,
                )
                thread = watcher.run_in_thread()
                collectors.append((messages, lock))
                watchers.append((watcher, thread))

            time.sleep(0.1)

            # Write messages
            db = make_broker(broker_target)
            try:
                for message in expected_messages:
                    db.write("broadcast", message)
                    time.sleep(0.05)  # Small delay to ensure order
            finally:
                db.shutdown()

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                snapshots = []
                for messages, lock in collectors:
                    with lock:
                        snapshots.append(messages.copy())
                if all(snapshot == expected_messages for snapshot in snapshots):
                    break
                time.sleep(0.02)

        finally:
            # Stop all watchers
            for watcher, _thread in watchers:
                with contextlib.suppress(Exception):
                    watcher.stop()

            for _watcher, thread in watchers:
                with contextlib.suppress(Exception):
                    thread.join(timeout=5.0)

        # Every peeker has an independent cursor and must see the whole sequence.
        for messages, lock in collectors:
            with lock:
                assert messages == expected_messages

        # Messages should still be in queue
        db = make_broker(broker_target)
        try:
            stats = db.get_queue_stats()
            broadcast_queue_stats = [stat for stat in stats if stat[0] == "broadcast"]
            if broadcast_queue_stats:
                unclaimed_count = broadcast_queue_stats[0][1]
                assert unclaimed_count == 5, (
                    f"Expected 5 remaining messages, found {unclaimed_count}"
                )
            else:
                raise AssertionError(
                    "Expected broadcast queue to exist with 5 messages"
                )
        finally:
            db.shutdown()

    def test_concurrent_writes_during_watch(self, broker_target):  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-033] exception
        """Test handling concurrent writes while watching."""
        # Filter out the timestamp conflict warning which is expected in this test
        warnings.filterwarnings(
            "ignore", message="Timestamp conflict persisted", category=RuntimeWarning
        )
        test_config = {"BROKER_BUSY_TIMEOUT": 100}

        read_messages = []
        lock = threading.Lock()
        writer_errors: list[tuple[int, str]] = []
        writer_errors_lock = threading.Lock()

        def handler(msg: str, ts: int):
            with lock:
                read_messages.append(msg)

        def writer_stack(thread: threading.Thread) -> str:
            if thread.ident is None:
                return f"{thread.name}: no thread id"
            frame = sys._current_frames().get(thread.ident)
            if frame is None:
                return f"{thread.name}: no Python stack frame"
            return f"{thread.name}:\n{''.join(traceback.format_stack(frame))}"

        # Start watcher
        with self.create_test_watcher(
            broker_target,
            "concurrent",
            handler,
            config=test_config,
        ) as watcher:
            watcher_thread = watcher.run_in_thread()

            # Start concurrent writers
            def writer_func(writer_id: int):
                try:
                    db = make_broker(broker_target, config=test_config)
                    try:
                        for i in range(20):
                            db.write("concurrent", f"w{writer_id}_m{i}")
                            time.sleep(0.01)
                    finally:
                        db.shutdown()
                except Exception:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
                    with writer_errors_lock:
                        writer_errors.append((writer_id, traceback.format_exc()))

            writer_threads = []
            for i in range(3):
                t = threading.Thread(
                    target=writer_func,
                    args=(i,),
                    name=f"concurrent-writer-{i}",
                )
                t.start()
                writer_threads.append(t)

            # Keep SQLite's own wait short in this lock-contention test. The
            # deadline still covers SimpleBroker's retry backoff budget and
            # fails with thread stacks if a writer is genuinely stuck.
            writer_deadline = time.monotonic() + scale_timeout_for_ci(45.0)
            for t in writer_threads:
                t.join(timeout=max(0.0, writer_deadline - time.monotonic()))

            alive_writers = [t for t in writer_threads if t.is_alive()]
            if alive_writers:
                with lock:
                    read_count = len(read_messages)
                stacks = "\n".join(writer_stack(t) for t in alive_writers)
                pytest.fail(
                    "Writer thread didn't complete; "
                    f"read_messages={read_count}; "
                    f"alive={[t.name for t in alive_writers]}\n{stacks}"
                )

            if writer_errors:
                error_text = "\n".join(
                    f"writer {writer_id}:\n{error}"
                    for writer_id, error in writer_errors
                )
                pytest.fail(f"Writer thread failed:\n{error_text}")

            # Wait for the watcher to process all messages
            start_time = time.monotonic()
            while time.monotonic() - start_time < scale_timeout_for_ci(5.0):
                with lock:
                    if len(read_messages) >= 60:
                        break
                time.sleep(0.05)  # Check every 50ms

            # Stop watcher with timeout
            watcher.stop()
            watcher_thread.join(timeout=scale_timeout_for_ci(2.0))
            assert not watcher_thread.is_alive(), "Watcher didn't stop cleanly"

        # Should have all 60 messages
        assert len(read_messages) == 60
        assert len(set(read_messages)) == 60  # No duplicates

        # Verify all messages accounted for
        expected = set()
        for w in range(3):
            for m in range(20):
                expected.add(f"w{w}_m{m}")
        assert set(read_messages) == expected


class TestEdgeCases(WatcherTestBase):
    """Test edge cases and error conditions."""

    def test_empty_queue_behavior(self, broker_target):
        """Test watcher behavior on empty queue."""
        called = threading.Event()

        def handler(msg: str, ts: int):
            called.set()

        watcher = QueueWatcher(
            "empty",
            handler,
            db=broker_target,
        )

        thread = watcher.run_in_thread()
        time.sleep(0.2)  # Let it poll a few times

        # Should not have called handler
        assert not called.is_set()

        # Now add a message
        db = make_broker(broker_target)
        try:
            db.write("empty", "finally!")
        finally:
            db.shutdown()

        # Wait longer on slower systems
        for _ in range(10):  # Up to 1 second total
            if called.is_set():
                break
            time.sleep(0.1)

        # Now should be called
        assert called.is_set()

        watcher.stop()
        thread.join(timeout=5.0)

    def test_rapid_start_stop(self, broker_target):
        """Test rapid start/stop cycles."""
        for i in range(5):
            watcher = QueueWatcher(
                f"queue_{i}",
                lambda m, t: None,
                db=broker_target,
            )

            thread = watcher.run_in_thread()
            time.sleep(0.01)
            watcher.stop(join=False)
            thread.join(timeout=scale_timeout_for_ci(2.0))
            if thread.is_alive():
                assert thread.ident is not None
                frame = sys._current_frames().get(thread.ident)
                stack = (
                    "".join(traceback.format_stack(frame))
                    if frame is not None
                    else "no Python stack frame"
                )
                pytest.fail(f"Watcher did not stop cleanly:\n{stack}")

    def test_stop_during_startup_skips_initial_drain(self, broker_target, monkeypatch):
        """Stop requests during startup should not begin a new initial drain."""
        watcher = QueueWatcher(
            "stop_during_startup",
            lambda m, t: None,
            db=broker_target,
        )
        drain_called = threading.Event()

        def stop_during_strategy_start(*args, **kwargs):
            del args, kwargs
            watcher.stop(join=False)

        def record_drain():
            drain_called.set()

        monkeypatch.setattr(watcher._strategy, "start", stop_during_strategy_start)
        monkeypatch.setattr(watcher, "_drain_queue", record_drain)

        watcher.run_forever()

        assert not drain_called.is_set()

    @pytest.mark.sqlite_only
    def test_stop_during_locked_connection_setup_ends_startup(
        self,
        tmp_path,
        monkeypatch,
    ):
        """A stop during WAL setup must prevent another contention retry."""
        setup_entered = threading.Event()
        release_setup = threading.Event()
        setup_attempts = 0

        def locked_connection_setup(*args, **kwargs):
            del args, kwargs
            nonlocal setup_attempts
            setup_attempts += 1
            setup_entered.set()
            assert release_setup.wait(timeout=scale_timeout_for_ci(2.0))
            raise OperationalError("database is locked")

        from simplebroker._backends import sqlite as sqlite_backend

        monkeypatch.setattr(
            sqlite_backend,
            "setup_connection_phase",
            locked_connection_setup,
        )
        watcher = QueueWatcher(
            "stop_during_locked_setup",
            lambda message, timestamp: None,
            db=str(tmp_path / "broker.db"),
        )
        thread = watcher.run_in_thread()
        try:
            assert setup_entered.wait(timeout=scale_timeout_for_ci(2.0))
            watcher.stop(join=False)
            release_setup.set()
            thread.join(timeout=scale_timeout_for_ci(2.0))

            assert not thread.is_alive()
            assert setup_attempts == 1
            assert not watcher.is_running()
        finally:
            release_setup.set()
            watcher.stop()
            thread.join(timeout=scale_timeout_for_ci(2.0))
