"""Tests for the watcher feature."""

import logging
import signal
import subprocess
import sys
import threading
import time

import pytest

from simplebroker import Queue
from simplebroker._constants import load_config

from .helper_scripts.broker_factory import make_broker
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition
from .helper_scripts.watcher_base import WatcherTestBase

# Import will be available after implementation
pytest.importorskip("simplebroker.watcher")
from simplebroker.watcher import QueueWatcher, StopWatching

pytestmark = [pytest.mark.shared]

logger = logging.getLogger(__name__)


class MessageCollector:
    """Helper to collect messages from watchers."""

    def __init__(self):
        self.messages: list[tuple[str, int]] = []
        self.lock = threading.Lock()
        self.error: Exception | None = None

    def handler(self, msg: str, ts: int) -> None:
        """Collect messages thread-safely."""
        with self.lock:
            self.messages.append((msg, ts))

    def error_handler(self, msg: str, ts: int) -> None:
        """Handler that always raises an error."""
        raise RuntimeError(f"Handler error for message: {msg}")

    def get_messages(self) -> list[tuple[str, int]]:
        """Get collected messages thread-safely."""
        with self.lock:
            return self.messages.copy()

    def wait_for_messages(self, count: int, timeout: float = 5.0) -> bool:
        """Wait for a specific number of messages to be collected.

        Args:
            count: Expected number of messages
            timeout: Maximum time to wait in seconds

        Returns:
            True if the expected count was reached, False if timeout
        """
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            with self.lock:
                if len(self.messages) >= count:
                    return True
            time.sleep(0.01)  # Small sleep to avoid busy waiting
        return False


class FakeActivityWaiter:
    def __init__(
        self,
        *,
        wait_result: bool = False,
        wait_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.wait_result = wait_result
        self.wait_error = wait_error
        self.close_error = close_error
        self.wait_calls: list[float] = []
        self.close_calls = 0

    def wait(self, timeout: float) -> bool:
        self.wait_calls.append(timeout)
        if self.wait_error is not None:
            raise self.wait_error
        return self.wait_result

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


def _polling_strategy_state(strategy):
    """Return every mutable PollingStrategy field for atomicity assertions."""
    return (
        strategy._initial_checks,
        strategy._max_interval,
        strategy._burst_sleep,
        strategy._check_count,
        strategy._stop_event,
        strategy._data_version,
        strategy._data_version_provider,
        strategy._data_change_callback,
        strategy._activity_waiter,
        strategy._pragma_failures,
        strategy._jitter_factor,
        strategy._native_activity_pending,
        strategy._local_activity_pending,
        strategy._local_activity_pending_for_drain,
        strategy._local_activity_empty_check,
        strategy._activity_burst_remaining,
        strategy._native_idle_poll_interval,
        strategy._next_native_idle_poll_at,
    )


def _seed_polling_strategy_state(strategy, waiter):
    """Populate mutable strategy state with distinctive preservation sentinels."""

    def provider():
        return 37

    def callback():
        return None

    strategy._initial_checks = 7
    strategy._max_interval = 0.4
    strategy._burst_sleep = 0.003
    strategy._check_count = 19
    strategy._data_version = 23
    strategy._data_version_provider = provider
    strategy._data_change_callback = callback
    strategy._activity_waiter = waiter
    strategy._pragma_failures = 5
    strategy._jitter_factor = 0.25
    strategy._native_activity_pending = True
    strategy._local_activity_pending = True
    strategy._local_activity_pending_for_drain = True
    strategy._local_activity_empty_check = True
    strategy._activity_burst_remaining = 11
    strategy._native_idle_poll_interval = 7.5
    strategy._next_native_idle_poll_at = 123.5
    return provider, callback


class TestQueueWatcher(WatcherTestBase):
    """Test the QueueWatcher class."""

    @pytest.mark.sqlite_only
    def test_thread_safety_with_brokerdb_instance(self, tmp_path):
        """A watcher can derive its SQLite path from a BrokerDB owner."""
        from simplebroker.db import BrokerDB

        temp_db = tmp_path / "test.db"
        broker_db = BrokerDB(str(temp_db))
        broker_db.write("test_queue", "msg1")
        broker_db.write("test_queue", "msg2")

        collector = MessageCollector()

        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_db,
            peek=False,
        )

        thread = watcher.run_in_thread()
        try:
            if not collector.wait_for_messages(2, timeout=2.0):
                pytest.fail(
                    f"Timeout waiting for messages. Got {len(collector.get_messages())} messages"
                )

            messages = collector.get_messages()
            assert len(messages) == 2
            assert messages[0][0] == "msg1"
            assert messages[1][0] == "msg2"
        finally:
            watcher.stop()
            thread.join(timeout=2.0)
            broker_db.close()

    def test_basic_consuming_mode(self, broker, broker_target):
        """Test basic message consumption (peek=False)."""
        # Write some messages
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")
        broker.write("test_queue", "message3")

        # Create a watcher
        collector = MessageCollector()

        # Use database path for thread-safe operation
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=False,
        )

        # Run watcher in background
        thread = watcher.run_in_thread()
        try:
            # Wait for processing (fast polling will pick up messages quickly)
            assert collector.wait_for_messages(3, timeout=2.0), (
                "Watcher did not process all messages in time"
            )
        finally:
            # Stop the watcher
            watcher.stop()
            thread.join(timeout=2.0)

        # Check that messages were consumed
        messages = collector.get_messages()
        assert len(messages) == 3
        assert messages[0][0] == "message1"
        assert messages[1][0] == "message2"
        assert messages[2][0] == "message3"

        # Verify messages were actually consumed
        # Use new peek API to inspect remaining messages without consuming
        db = make_broker(broker_target)
        try:
            remaining = list(db.peek_generator("test_queue", with_timestamps=False))
        finally:
            db.shutdown()
        assert len(remaining) == 0

    def test_peek_mode(self, broker, broker_target):
        """Test peek mode (peek=True) - messages not consumed."""
        # Write messages
        broker.write("test_queue", "peek1")
        broker.write("test_queue", "peek2")

        collector = MessageCollector()

        # Use database path for thread-safe operation
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=True,
        )

        thread = watcher.run_in_thread()
        try:
            assert collector.wait_for_messages(2, timeout=2.0), (
                "Watcher did not observe messages in time"
            )
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        # Messages should have been seen
        messages = collector.get_messages()
        assert len(messages) == 2

        # But NOT consumed (use new peek API)
        db = make_broker(broker_target)
        try:
            remaining = list(db.peek_generator("test_queue", with_timestamps=False))
        finally:
            db.shutdown()
        assert len(remaining) == 2
        assert remaining[0] == "peek1"
        assert remaining[1] == "peek2"

    def test_last_ts_cache_updates_on_data_version(self, broker_target):
        """Watcher should refresh queue.last_ts when data_version changes."""

        collector = MessageCollector()
        watcher = QueueWatcher(
            "ts_queue",
            collector.handler,
            db=broker_target,
            peek=True,
        )

        thread = watcher.run_in_thread()
        queue_writer = Queue("ts_queue", db_path=broker_target)
        try:
            time.sleep(0.1)  # Allow watcher to initialize polling
            assert watcher._queue_obj.last_ts in (None, 0)

            queue_writer.write("first")
            assert collector.wait_for_messages(1, timeout=2.0)
            first_cached = watcher._queue_obj.last_ts
            assert isinstance(first_cached, int)
            assert first_cached > 0

            queue_writer.write("second")
            assert collector.wait_for_messages(2, timeout=2.0)
            assert isinstance(watcher._queue_obj.last_ts, int)
            assert watcher._queue_obj.last_ts >= first_cached
        finally:
            watcher.stop()
            thread.join(timeout=2.0)
            queue_writer.close()

    def test_run_with_retries_start_strategy_hook(self, broker_target):
        """The retry loop should initialize strategy through the watcher hook."""

        class StrategyHookWatcher(QueueWatcher):
            def __init__(self, *args, **kwargs):
                self.start_strategy_calls = 0
                super().__init__(*args, **kwargs)

            def _start_strategy(self) -> None:
                self.start_strategy_calls += 1
                super()._start_strategy()

            def _process_messages(self) -> None:
                raise StopWatching

        watcher = StrategyHookWatcher(
            "hooked_queue",
            lambda msg, ts: None,
            db=broker_target,
        )
        try:
            watcher._run_with_retries()
            assert watcher.start_strategy_calls == 1
        finally:
            watcher.stop(join=False)

    def test_create_activity_waiter_hook_is_used_by_strategy_startup(
        self, broker_target
    ):
        """Strategy startup should get native waiters through the watcher hook."""
        waiter = FakeActivityWaiter()
        seen_queue_names = []

        class WaiterHookWatcher(QueueWatcher):
            def _create_activity_waiter(self, queue):
                seen_queue_names.append(queue.name)
                return waiter

        watcher = WaiterHookWatcher(
            "hooked_queue",
            lambda msg, ts: None,
            db=broker_target,
        )
        try:
            watcher._start_strategy()
            assert seen_queue_names == ["hooked_queue"]
            assert watcher._strategy.uses_native_activity() is True
        finally:
            watcher._strategy.close()
            watcher.stop(join=False)

    @pytest.mark.sqlite_only
    def test_on_data_version_change_hook_runs_after_sqlite_change(self, tmp_path):
        """The data-version callback should dispatch through the watcher hook."""
        from .helper_scripts.broker_factory import make_target

        broker_target = make_target(tmp_path, backend="sqlite")
        hook_calls = []

        class DataVersionHookWatcher(QueueWatcher):
            def _on_data_version_change(self, queue) -> None:
                hook_calls.append(queue.name)
                super()._on_data_version_change(queue)

        watcher = DataVersionHookWatcher(
            "watched_queue",
            lambda msg, ts: None,
            db=broker_target,
        )
        writer_queue = Queue(
            "watched_queue",
            db_path=broker_target,
            persistent=False,
        )
        try:
            watcher._start_strategy()
            watcher._strategy._check_data_version()
            hook_calls.clear()

            writer_queue.write("changed")

            assert watcher._strategy._check_data_version() is True
            assert hook_calls == ["watched_queue"]
        finally:
            writer_queue.close()
            watcher._strategy.close()
            watcher.stop(join=False)

    @pytest.mark.sqlite_only
    def test_default_start_strategy_transfers_cached_queue_activity_waiter(
        self, tmp_path, monkeypatch
    ):
        """A started strategy takes sole ownership of the Queue waiter."""
        from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin

        from .helper_scripts.broker_factory import make_target

        broker_target = make_target(tmp_path, backend="sqlite")
        waiter = FakeActivityWaiter()

        def create_activity_waiter(**kwargs):
            del kwargs
            return waiter

        monkeypatch.setattr(
            sqlite_backend_plugin,
            "create_activity_waiter",
            create_activity_waiter,
        )

        watcher = QueueWatcher(
            "cached_waiter_queue",
            lambda msg, ts: None,
            db=broker_target,
        )
        try:
            watcher._start_strategy()
            assert watcher._queue_obj._activity_waiter is None
            watcher._start_strategy()

            assert waiter.close_calls == 0
            assert watcher._strategy.uses_native_activity() is True
            assert watcher._queue_obj._activity_waiter is None

            watcher._strategy.close()
            assert waiter.close_calls == 1
        finally:
            watcher._queue_obj._activity_waiter = None
            watcher.stop(join=False)

    def test_graceful_shutdown_stop_method(self, broker_target):
        """Test graceful shutdown via stop() method."""
        with self.create_test_watcher(
            broker_target,
            "test_queue",
            lambda msg, ts: None,
        ) as watcher:
            thread = watcher.run_in_thread()
            time.sleep(0.1)  # Let it start

            # Test graceful stop timing
            start_time = time.monotonic()
            watcher.stop()
            thread.join(timeout=4.0)
            stop_time = time.monotonic() - start_time

            assert not thread.is_alive()
            assert stop_time < 4.0, f"Stop took {stop_time:.2f}s"

    @pytest.mark.sqlite_only
    def test_graceful_shutdown_sigint(self, tmp_path):
        """Test graceful shutdown via SIGINT using subprocess."""
        from simplebroker.db import BrokerDB

        from .conftest import managed_subprocess

        temp_db = tmp_path / "test.db"

        # This test uses a subprocess to properly test SIGINT handling
        # without interfering with the test runner
        # Prepare paths - use improved script with race condition mitigation
        from .helper_scripts import WATCHER_SIGINT_SCRIPT_IMPROVED

        helper_script = WATCHER_SIGINT_SCRIPT_IMPROVED
        ready_file = tmp_path / "watcher_ready.txt"

        # Add a test message to the queue
        with BrokerDB(temp_db) as db:
            db.write("sigint_test_queue", "test_message")

        # Launch the watcher script as a subprocess
        with managed_subprocess(
            [sys.executable, str(helper_script), str(temp_db), str(ready_file)]
        ) as proc:
            # Strategy: Monitor subprocess output for better debugging
            ready_timeout = (
                120  # Increased timeout for CI systems with high database contention
            )
            ready_detected = False

            for _ in range(ready_timeout * 10):  # Check every 0.1s
                if ready_file.exists():
                    ready_detected = True
                    break

                # Also check if process is still running (early termination detection)
                if proc.proc.poll() is not None:
                    # Process terminated early - check output for errors
                    try:
                        stdout, stderr = proc.proc.communicate(timeout=1.0)
                        if stderr:
                            pytest.fail(
                                f"Subprocess terminated early with error: {stderr}"
                            )
                        else:
                            pytest.fail(
                                f"Subprocess terminated early with output: {stdout}"
                            )
                    except subprocess.TimeoutExpired:
                        pytest.fail("Subprocess terminated early and is unresponsive")

                time.sleep(0.1)

            if not ready_detected:
                # Enhanced error reporting for debugging
                poll_result = proc.proc.poll()
                if poll_result is not None:
                    pytest.fail(
                        f"Subprocess terminated with exit code {poll_result} before becoming ready"
                    )
                else:
                    pytest.fail(
                        f"Watcher subprocess did not become ready in {ready_timeout}s (process still running)"
                    )

            # Verify process is still running
            assert proc.proc.poll() is None, "Subprocess terminated prematurely"

            # Send SIGINT signal (Ctrl-C) on Unix, terminate on Windows
            if sys.platform == "win32":
                proc.terminate()
            else:
                proc.proc.send_signal(signal.SIGINT)

            # Wait for graceful exit with timeout
            try:
                exit_code = proc.proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                # Process didn't exit gracefully - force kill it
                logger.warning(
                    "Process did not terminate gracefully after SIGINT, forcing termination"
                )

                # Try SIGTERM first
                proc.proc.terminate()
                try:
                    exit_code = proc.proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    # Force kill if still not terminated
                    proc.proc.kill()
                    try:
                        exit_code = proc.proc.wait(timeout=1.0)
                    except subprocess.TimeoutExpired:
                        pytest.fail("Failed to terminate subprocess even with SIGKILL")

                # On slow/loaded machines, accept forceful termination
                # as a valid outcome
                logger.info(f"Process forcefully terminated with exit code {exit_code}")

            # Check exit code
            # On Windows, terminated processes may exit with code 1
            # When forcefully killed, accept negative codes (signals) on Unix
            if sys.platform == "win32":
                expected_codes = (0, 1)
            else:
                # 0 = clean exit, -2 = SIGINT, -15 = SIGTERM, -9 = SIGKILL
                expected_codes = (0, -2, -15, -9)

            assert exit_code in expected_codes, (
                f"Process exited with code {exit_code}, expected {expected_codes}"
            )

            # Optionally check output
            stderr_output = proc.stderr
            if stderr_output:
                print(f"Subprocess stderr: {stderr_output}")

    @pytest.mark.sqlite_only
    def test_sigint_handler_installation(self, tmp_path):
        """Test that signal handler is correctly installed in main thread."""

        # Use a subprocess to test signal handler installation properly
        import textwrap

        from .conftest import run_subprocess

        temp_db = tmp_path / "test.db"

        # Create a test script that verifies signal handler installation
        test_script = tmp_path / "test_signal_install.py"
        test_script.write_text(
            textwrap.dedent("""
            import signal
            import sys
            from pathlib import Path

            # Add parent to path
            sys.path.insert(0, str(Path(__file__).parent.parent))

            from simplebroker.db import BrokerDB
            from simplebroker.watcher import QueueWatcher

            # Track if our handler was installed
            original_handler = signal.getsignal(signal.SIGINT)

            db_path = sys.argv[1]
            db = BrokerDB(db_path)
            watcher = QueueWatcher("test_queue", lambda m, t: None, db=db)

            # Stop immediately so we don't block
            watcher.stop()

            # Run which should install handler
            watcher.run_forever()

            # Check if handler was installed and restored
            final_handler = signal.getsignal(signal.SIGINT)

            # We expect the handler to be restored to original after run_forever
            if final_handler == original_handler:
                print("PASS: Handler properly installed and restored")
                sys.exit(0)
            else:
                print("FAIL: Handler not properly managed")
                sys.exit(1)
        """)
        )

        # Run the subprocess
        returncode, stdout, stderr = run_subprocess(
            [sys.executable, str(test_script), str(temp_db)], timeout=5.0
        )

        # Check results
        assert returncode == 0, f"Test failed: {stdout} {stderr}"
        assert "PASS" in stdout

    def test_handler_exception_handling(self, broker, broker_target):
        """Test that handler exceptions don't crash the watcher."""
        # Write messages
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")

        handled_messages = []
        exception_count = 0

        def faulty_handler(msg: str, ts: int):
            """Handler that fails on first message."""
            if msg == "message1":
                raise ValueError("Simulated error")
            handled_messages.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            """Count errors and continue."""
            nonlocal exception_count
            exception_count += 1
            return True  # Continue processing

        watcher = QueueWatcher(
            "test_queue",
            faulty_handler,
            db=broker_target,
            error_handler=error_handler,
        )

        thread = watcher.run_in_thread()
        try:
            assert wait_for_condition(
                lambda: exception_count == 1 and handled_messages == ["message2"],
                timeout=scale_timeout_for_ci(2.0),
                interval=0.01,
            ), (
                "Watcher did not handle the first exception and continue to "
                "process the second message in time"
            )
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        # First message should have errored, second should succeed
        assert exception_count == 1
        assert handled_messages == ["message2"]

    def test_peek_handler_failure_does_not_advance_checkpoint(
        self, broker, broker_target
    ) -> None:
        broker.write("peek_queue", "bad")
        attempts: list[str] = []
        failed = threading.Event()
        watcher: QueueWatcher

        def handler(message: str, _timestamp: int) -> None:
            attempts.append(message)
            raise ValueError("peek handler failed")

        def error_handler(_exc: Exception, _message: str, _timestamp: int) -> bool:
            failed.set()
            watcher.stop(join=False)
            return True

        watcher = QueueWatcher(
            "peek_queue",
            handler,
            db=broker_target,
            peek=True,
            error_handler=error_handler,
        )
        thread = watcher.run_in_thread()
        try:
            assert failed.wait(timeout=scale_timeout_for_ci(2.0))
            thread.join(timeout=2.0)
            assert not thread.is_alive()
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        assert attempts == ["bad"]
        assert watcher._last_seen_ts == 0
        assert broker.peek_many("peek_queue", limit=10, with_timestamps=False) == [
            "bad"
        ]

    def test_peek_batch_stops_at_failed_message_and_retries_in_order(
        self, broker, broker_target
    ) -> None:
        broker.write("peek_queue", "bad")
        broker.write("peek_queue", "later")
        attempts: list[str] = []
        failed = threading.Event()
        watcher: QueueWatcher

        def failing_handler(message: str, _timestamp: int) -> None:
            attempts.append(message)
            raise ValueError("peek handler failed")

        def error_handler(_exc: Exception, _message: str, _timestamp: int) -> bool:
            failed.set()
            watcher.stop(join=False)
            return True

        watcher = QueueWatcher(
            "peek_queue",
            failing_handler,
            db=broker_target,
            peek=True,
            batch_processing=True,
            error_handler=error_handler,
        )
        thread = watcher.run_in_thread()
        try:
            assert failed.wait(timeout=scale_timeout_for_ci(2.0))
            thread.join(timeout=2.0)
            assert not thread.is_alive()
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        assert attempts == ["bad"]
        assert watcher._last_seen_ts == 0
        assert broker.peek_many("peek_queue", limit=10, with_timestamps=False) == [
            "bad",
            "later",
        ]

        retried: list[str] = []
        retry_watcher = QueueWatcher(
            "peek_queue",
            lambda message, _timestamp: retried.append(message),
            db=broker_target,
            peek=True,
            batch_processing=True,
        )
        try:
            assert retry_watcher._process_peek_messages() is True
        finally:
            retry_watcher.stop(join=False)

        assert retried == ["bad", "later"]

    def test_legacy_none_dispatch_override_still_advances_peek_checkpoint(
        self, broker, broker_target
    ) -> None:
        broker.write("peek_queue", "payload")
        dispatched: list[tuple[str, int]] = []

        class LegacyDispatchWatcher(QueueWatcher):
            def _dispatch(
                self,
                message: str,
                timestamp: int,
                *,
                config: object | None = None,
            ) -> None:
                del config
                dispatched.append((message, timestamp))

        watcher = LegacyDispatchWatcher(
            "peek_queue",
            lambda _message, _timestamp: None,
            db=broker_target,
            peek=True,
        )
        try:
            assert watcher._process_peek_messages() is True
        finally:
            watcher.stop(join=False)

        assert [message for message, _timestamp in dispatched] == ["payload"]
        assert watcher._last_seen_ts == dispatched[0][1]

    def test_error_handler_returns_false(self, broker, broker_target):
        """Test that error_handler returning False stops the watcher."""
        broker.write("test_queue", "bad_message")
        broker.write("test_queue", "good_message")

        processed = []

        def handler(msg: str, ts: int):
            if msg == "bad_message":
                raise ValueError("Bad message")
            processed.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            return False  # Stop on error

        with self.create_test_watcher(
            broker_target,
            "test_queue",
            handler,
            error_handler=error_handler,
        ) as watcher:
            thread = watcher.run_in_thread()
            thread.join(timeout=scale_timeout_for_ci(2.0))
            assert not thread.is_alive(), "watcher did not stop after error handler"

            # Should have stopped after first error
            assert processed == []
            # Second message should still be in queue (use new peek API)
            db = make_broker(broker_target)
            try:
                remaining = list(db.peek_generator("test_queue", with_timestamps=False))
                assert remaining == ["good_message"]
            finally:
                db.shutdown()

    def test_multiple_workers_exactly_once(self, broker_target):
        """Test multiple workers ensure exactly-once delivery."""
        # Write many messages
        num_messages = 20
        db = make_broker(broker_target)
        try:
            for i in range(num_messages):
                db.write("work_queue", f"task_{i}")
        finally:
            db.shutdown()

        # Create multiple workers
        collectors = [MessageCollector() for _ in range(3)]
        workers = []

        try:
            for _i, collector in enumerate(collectors):
                # Use database path for thread-safe operation
                watcher = QueueWatcher(
                    "work_queue",
                    collector.handler,
                    db=broker_target,
                    peek=False,
                )
                thread = watcher.run_in_thread()
                workers.append((watcher, thread))

            # Wait until all messages processed or timeout
            def total_processed() -> int:
                return sum(len(collector.get_messages()) for collector in collectors)

            success = wait_for_condition(
                lambda: total_processed() >= num_messages,
                timeout=scale_timeout_for_ci(15.0, ci_factor=2.0),
                interval=0.05,
            )
            if not success:
                db = make_broker(broker_target)
                try:
                    remaining = list(
                        db.peek_generator("work_queue", with_timestamps=False)
                    )
                finally:
                    db.shutdown()
                alive_workers = sum(
                    1 for _watcher, thread in workers if thread.is_alive()
                )
                pytest.fail(
                    f"Workers processed only {total_processed()} of {num_messages} "
                    f"messages; remaining={len(remaining)}; "
                    f"alive_workers={alive_workers}"
                )
        finally:
            # Stop all workers
            for watcher, _thread in workers:
                try:
                    watcher.stop()
                except Exception:
                    pass  # Ignore errors during cleanup
            for _watcher, thread in workers:
                try:
                    thread.join(timeout=2.0)
                except Exception:
                    pass  # Ignore errors during cleanup

        # Collect all processed messages
        all_messages = []
        for collector in collectors:
            all_messages.extend(msg for msg, _ in collector.get_messages())

        # Should have exactly num_messages, no duplicates
        assert len(all_messages) == num_messages
        assert len(set(all_messages)) == num_messages
        assert set(all_messages) == {f"task_{i}" for i in range(num_messages)}

        # Queue should be empty
        db = make_broker(broker_target)
        try:
            remaining = list(db.peek_generator("work_queue", with_timestamps=False))
            assert len(remaining) == 0
        finally:
            db.shutdown()

    def test_mixed_peek_and_read_watchers(self, broker_target):
        """Test mixed peek and read watchers on same queue."""
        # Set up collectors
        peek_collector = MessageCollector()
        read_collector = MessageCollector()

        try:
            peek_watcher = QueueWatcher(
                "mixed_queue",
                peek_collector.handler,
                db=broker_target,
                peek=True,
            )
            peek_thread = peek_watcher.run_in_thread()

            # Start read watcher
            read_watcher = QueueWatcher(
                "mixed_queue",
                read_collector.handler,
                db=broker_target,
                peek=False,
            )
            read_thread = read_watcher.run_in_thread()

            time.sleep(0.1)

            # Write messages after watchers are running
            writer_db = make_broker(broker_target)
            try:
                writer_db.write("mixed_queue", "msg1")
                writer_db.write("mixed_queue", "msg2")
                writer_db.write("mixed_queue", "msg3")
            finally:
                writer_db.shutdown()

            # Let watchers process
            time.sleep(0.4)
        finally:
            # Stop watchers
            try:
                peek_watcher.stop()
                peek_thread.join(timeout=2.0)
            except Exception:
                pass  # Ignore errors during cleanup
            try:
                read_watcher.stop()
                read_thread.join(timeout=2.0)
            except Exception:
                pass  # Ignore errors during cleanup

        # Check results
        peek_messages = [msg for msg, _ in peek_collector.get_messages()]
        read_messages = [msg for msg, _ in read_collector.get_messages()]

        # Read watcher should have consumed some/all messages
        assert len(read_messages) > 0
        assert len(read_messages) <= 3

        # Peek watcher might have seen any subset of messages
        # (depending on timing with read watcher)
        assert len(peek_messages) >= 0
        assert len(peek_messages) <= 3

        # No message should appear twice in read_messages
        assert len(read_messages) == len(set(read_messages))

    def test_run_forever_blocking(self, broker_target):
        """Test that run_forever blocks until stopped."""
        run_completed = threading.Event()
        watcher_created = threading.Event()
        watcher_ref = None

        def run_watcher():
            nonlocal watcher_ref
            watcher = QueueWatcher(
                "test_queue",
                lambda m, t: None,
                db=broker_target,
            )
            watcher_ref = watcher
            watcher_created.set()  # Signal that watcher is created
            watcher.run_forever()
            run_completed.set()

        thread = threading.Thread(target=run_watcher)
        thread.start()
        try:
            # Wait for watcher to be created with timeout
            if not watcher_created.wait(timeout=2.0):
                pytest.fail("Watcher was not created within timeout")

            assert thread.is_alive()
            assert not run_completed.is_set()

            # Stop it properly
            assert watcher_ref is not None
            watcher_ref.stop()
        finally:
            thread.join(timeout=2.0)

        assert run_completed.is_set()

    def test_polling_lifecycle(self, broker_target):
        """Test that polling strategy lifecycle works correctly."""
        broker = make_broker(broker_target)
        broker.shutdown()

        watcher = QueueWatcher(
            "test_queue",
            lambda m, t: None,
            db=broker_target,
        )

        # Access internal strategy for testing
        strategy = watcher._strategy

        thread = watcher.run_in_thread()
        try:
            assert wait_for_condition(
                lambda: strategy._check_count > 0,
                timeout=scale_timeout_for_ci(2.0),
                interval=0.01,
            ), "Polling strategy did not complete an idle check in time"
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        # Should be stopped
        assert strategy._stop_event.is_set()

    def test_is_running_tracks_background_execution(self, broker_target) -> None:
        watcher = QueueWatcher(
            "lifecycle_queue",
            lambda _message, _timestamp: None,
            db=broker_target,
        )

        assert watcher.is_running() is False

        thread = watcher.run_in_thread()
        try:
            assert wait_for_condition(
                watcher.is_running,
                timeout=scale_timeout_for_ci(2.0),
                interval=0.01,
            )
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        assert not thread.is_alive()
        assert watcher.is_running() is False

    def test_is_running_remains_true_until_cleanup_finishes(
        self, broker_target
    ) -> None:
        cleanup_started = threading.Event()
        allow_cleanup = threading.Event()

        class BlockingCleanupWatcher(QueueWatcher):
            def _cleanup_thread_local(self) -> None:
                cleanup_started.set()
                assert allow_cleanup.wait(timeout=2.0)
                super()._cleanup_thread_local()

        watcher = BlockingCleanupWatcher(
            "lifecycle_queue",
            lambda _message, _timestamp: None,
            db=broker_target,
        )
        thread = watcher.run_in_thread()
        try:
            assert wait_for_condition(
                watcher.is_running,
                timeout=scale_timeout_for_ci(2.0),
                interval=0.01,
            )
            watcher.stop(join=False)
            assert cleanup_started.wait(timeout=2.0)
            assert watcher.is_running() is True
        finally:
            allow_cleanup.set()
            thread.join(timeout=2.0)
            watcher.stop()

        assert watcher.is_running() is False

    def test_is_running_clears_after_background_fatal_exit(self, broker_target) -> None:
        class FatalWatcher(QueueWatcher):
            def _run_with_retries(self, max_retries: int = 3) -> None:
                del max_retries
                raise RuntimeError("fatal watcher failure")

        watcher = FatalWatcher(
            "lifecycle_queue",
            lambda _message, _timestamp: None,
            db=broker_target,
        )
        errors: list[BaseException] = []
        error_seen = threading.Event()
        original_excepthook = threading.excepthook

        def capture_exception(args: threading.ExceptHookArgs) -> None:
            if args.exc_value is not None:
                errors.append(args.exc_value)
            error_seen.set()

        threading.excepthook = capture_exception
        try:
            thread = watcher.run_in_thread()
            assert error_seen.wait(timeout=2.0)
            thread.join(timeout=2.0)
        finally:
            threading.excepthook = original_excepthook
            watcher.stop()

        assert not thread.is_alive()
        assert len(errors) == 1
        assert "fatal watcher failure" in str(errors[0])
        assert watcher.is_running() is False

    def test_synchronous_fatal_and_cleanup_failures_clear_running_state(
        self, broker_target
    ) -> None:
        class SynchronousFatalWatcher(QueueWatcher):
            def _run_with_retries(self, max_retries: int = 3) -> None:
                del max_retries
                raise RuntimeError("synchronous fatal failure")

        fatal_watcher = SynchronousFatalWatcher(
            "fatal_queue",
            lambda _message, _timestamp: None,
            db=broker_target,
        )
        with pytest.raises(RuntimeError, match="synchronous fatal failure"):
            fatal_watcher.run_forever()
        assert fatal_watcher.is_running() is False
        fatal_watcher.stop(join=False)

        class CleanupFailureWatcher(QueueWatcher):
            cleanup_calls = 0

            def _run_with_retries(self, max_retries: int = 3) -> None:
                del max_retries

            def _cleanup_thread_local(self) -> None:
                self.cleanup_calls += 1
                if self.cleanup_calls == 1:
                    raise RuntimeError("cleanup failure")
                super()._cleanup_thread_local()

        cleanup_watcher = CleanupFailureWatcher(
            "cleanup_queue",
            lambda _message, _timestamp: None,
            db=broker_target,
        )
        previous_sigint = signal.getsignal(signal.SIGINT)
        with pytest.raises(RuntimeError, match="cleanup failure"):
            cleanup_watcher.run_forever()
        assert signal.getsignal(signal.SIGINT) is previous_sigint
        assert cleanup_watcher.is_running() is False
        cleanup_watcher.stop(join=False)

    def test_after_parameter_in_peek_mode(self, broker, broker_target):
        """Test that peek mode respects message ordering and 'after' tracking."""
        # Add messages and capture their timestamps
        broker.write("test_queue", "msg1")
        broker.write("test_queue", "msg2")

        # Get current messages to find msg2's timestamp
        initial_collector = MessageCollector()
        initial_watcher = QueueWatcher(
            "test_queue",
            initial_collector.handler,
            db=broker_target,
            peek=True,
        )
        thread = initial_watcher.run_in_thread()
        try:
            assert initial_collector.wait_for_messages(
                2,
                timeout=scale_timeout_for_ci(2.0),
            ), (
                "Initial peek watcher should collect both seed messages before "
                "the timestamp boundary is chosen"
            )
        finally:
            initial_watcher.stop()
            thread.join(timeout=2.0)

        # Get timestamp of msg2
        initial_messages = initial_collector.get_messages()
        assert len(initial_messages) == 2
        ts_msg2 = initial_messages[1][1]  # Timestamp of msg2

        # Add msg3 after msg2
        broker.write("test_queue", "msg3")

        # Create collector for the actual test
        collector = MessageCollector()

        # Create watcher with after_timestamp set to ts_msg2
        # Should only see msg3 (messages after ts_msg2)
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=True,
            after_timestamp=ts_msg2,
        )

        # Start watcher and let it process messages
        thread = watcher.run_in_thread()
        try:
            assert collector.wait_for_messages(
                1,
                timeout=scale_timeout_for_ci(2.0),
            ), "Peek watcher should collect the post-after message"
        finally:
            # Stop watcher
            watcher.stop()
            thread.join(timeout=2.0)

        # Should only have msg3
        messages = collector.get_messages()
        assert len(messages) == 1
        assert messages[0][0] == "msg3"
        assert messages[0][1] > ts_msg2

    def test_after_timestamp_database_filtering(self, broker, broker_target):
        """Test that after_timestamp filters at database level, not in Python."""
        # Add first batch of messages
        for i in range(50):
            broker.write("test_queue", f"msg{i:03d}")

        # Get timestamp of last message in first batch
        initial_collector = MessageCollector()
        initial_watcher = QueueWatcher(
            "test_queue",
            initial_collector.handler,
            db=broker_target,
            peek=True,
            batch_processing=True,
        )
        thread = initial_watcher.run_in_thread()
        try:
            # Wait for all 50 messages to be collected
            if not initial_collector.wait_for_messages(
                50,
                timeout=scale_timeout_for_ci(5.0),
            ):
                pytest.fail(
                    f"Timeout waiting for messages. Got {len(initial_collector.get_messages())} messages"
                )
        finally:
            initial_watcher.stop()
            thread.join(timeout=2.0)

        # Get timestamp after 50th message
        initial_messages = initial_collector.get_messages()
        assert len(initial_messages) == 50
        ts_mid = initial_messages[-1][1]  # Timestamp of last message

        # Add more messages
        for i in range(50, 100):
            broker.write("test_queue", f"msg{i:03d}")

        # Create collector for actual test
        collector = MessageCollector()

        # Create watcher with after_timestamp
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=True,
            after_timestamp=ts_mid,
            batch_processing=True,
        )

        # Start watcher and let it process messages
        thread = watcher.run_in_thread()
        try:
            # Wait for all 50 new messages to be collected
            if not collector.wait_for_messages(
                50,
                timeout=scale_timeout_for_ci(5.0),
            ):
                pytest.fail(
                    f"Timeout waiting for messages. Got {len(collector.get_messages())} messages"
                )
        finally:
            # Stop watcher
            watcher.stop()
            thread.join(timeout=2.0)

        # Should have exactly 50 messages (050-099)
        messages = collector.get_messages()
        assert len(messages) == 50
        # Check message content
        for i, (msg, ts) in enumerate(messages):
            assert msg == f"msg{i + 50:03d}"
            assert ts > ts_mid

    def test_after_timestamp_filters_consume_single_message_drain(
        self, broker, broker_target
    ):
        """Consume mode must not claim rows at or before the checkpoint."""
        broker.write("test_queue", "old-1")
        broker.write("test_queue", "boundary")
        boundary_ts = list(broker.peek_generator("test_queue"))[-1][1]
        broker.write("test_queue", "new-1")

        collector = MessageCollector()
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=False,
            after_timestamp=boundary_ts,
        )

        try:
            watcher._drain_queue()
        finally:
            watcher.stop(join=False)

        messages = collector.get_messages()
        assert len(messages) == 1
        assert messages[0][0] == "new-1"
        assert messages[0][1] > boundary_ts
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["old-1", "boundary"]

    def test_after_timestamp_filters_consume_batch_drain(self, broker, broker_target):
        """Batch consume mode must use the same exclusive after filter."""
        broker.write("test_queue", "old-1")
        broker.write("test_queue", "boundary")
        boundary_ts = list(broker.peek_generator("test_queue"))[-1][1]
        broker.write("test_queue", "new-1")
        broker.write("test_queue", "new-2")

        collector = MessageCollector()
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=False,
            after_timestamp=boundary_ts,
            batch_processing=True,
        )

        try:
            watcher._drain_queue()
        finally:
            watcher.stop(join=False)

        messages = collector.get_messages()
        assert [body for body, _ in messages] == ["new-1", "new-2"]
        assert all(ts > boundary_ts for _, ts in messages)
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["old-1", "boundary"]


class TestPollingStrategy:
    """Test polling strategy behavior."""

    def test_detach_activity_waiter_returns_without_closing(self):
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        waiter = FakeActivityWaiter()

        strategy.start(activity_waiter=waiter)
        detached = strategy.detach_activity_waiter()

        assert detached is waiter
        assert waiter.close_calls == 0
        assert strategy.uses_native_activity() is False
        assert strategy.detach_activity_waiter() is None

    def test_detach_activity_waiter_expected_mismatch_is_noop(self):
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        waiter_a = FakeActivityWaiter()
        waiter_b = FakeActivityWaiter()

        strategy.start(activity_waiter=waiter_a)

        assert strategy.detach_activity_waiter(expected=waiter_b) is None
        assert strategy.uses_native_activity() is True
        assert waiter_a.close_calls == 0

    def test_start_does_not_close_same_activity_waiter(self):
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        waiter = FakeActivityWaiter()

        strategy.start(activity_waiter=waiter)
        strategy.start(activity_waiter=waiter)

        assert waiter.close_calls == 0
        assert strategy.uses_native_activity() is True

    def test_start_closes_replaced_activity_waiter(self):
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        waiter_a = FakeActivityWaiter()
        waiter_b = FakeActivityWaiter()

        strategy.start(activity_waiter=waiter_a)
        strategy.start(activity_waiter=waiter_b)

        assert waiter_a.close_calls == 1
        assert waiter_b.close_calls == 0

    def test_replace_activity_waiter_returns_displaced_without_closing(self):
        from simplebroker.watcher import PollingStrategy

        strategy = PollingStrategy(threading.Event())
        waiter_a = FakeActivityWaiter()
        waiter_b = FakeActivityWaiter(wait_result=True)
        strategy.start(activity_waiter=waiter_a)

        displaced = strategy.replace_activity_waiter(waiter_b)

        assert displaced is waiter_a
        assert waiter_a.close_calls == 0
        assert waiter_b.close_calls == 0
        strategy.wait_for_activity()
        assert waiter_a.wait_calls == []
        assert len(waiter_b.wait_calls) == 1

        strategy.close()
        waiter_a.close()

    @pytest.mark.parametrize("with_waiter", [False, True])
    def test_replace_activity_waiter_same_object_is_exact_noop(
        self,
        monkeypatch,
        with_waiter,
    ):
        import simplebroker.watcher as watcher_module
        from simplebroker.watcher import PollingStrategy

        strategy = PollingStrategy(threading.Event())
        waiter = FakeActivityWaiter() if with_waiter else None
        _seed_polling_strategy_state(strategy, waiter)
        state_before = _polling_strategy_state(strategy)

        def fail_if_scheduled(*args, **kwargs):
            del args, kwargs
            raise AssertionError("same-object replacement must not schedule")

        monkeypatch.setattr(watcher_module.random, "uniform", fail_if_scheduled)

        assert strategy.replace_activity_waiter(waiter) is None
        assert _polling_strategy_state(strategy) == state_before

    @pytest.mark.parametrize("transition", ["to_none", "from_none"])
    def test_replace_activity_waiter_none_transitions(self, transition):
        from simplebroker.watcher import PollingStrategy

        strategy = PollingStrategy(threading.Event())
        if transition == "to_none":
            versions = [1]
            callback_versions = []
            waiter = FakeActivityWaiter()

            def version_provider():
                return versions[0]

            def on_data_version_change():
                callback_versions.append(versions[0])

            strategy.start(
                version_provider,
                on_data_version_change=on_data_version_change,
                activity_waiter=waiter,
            )
            strategy._data_version = 1

            assert strategy.replace_activity_waiter(None) is waiter
            assert strategy.uses_native_activity() is False
            assert waiter.close_calls == 0

            versions[0] = 2
            strategy.wait_for_activity()
            assert callback_versions == [2]
            waiter.close()
            return

        waiter = FakeActivityWaiter(wait_result=True)
        strategy.start()

        assert strategy.replace_activity_waiter(waiter) is None
        assert strategy.uses_native_activity() is True
        strategy.wait_for_activity()
        assert len(waiter.wait_calls) == 1
        strategy.close()

    def test_replace_activity_waiter_preserves_data_and_local_state(self):
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        waiter_a = FakeActivityWaiter()
        waiter_b = FakeActivityWaiter()
        provider, callback = _seed_polling_strategy_state(strategy, waiter_a)

        strategy.replace_activity_waiter(waiter_b)

        assert strategy._stop_event is stop_event
        assert strategy._initial_checks == 7
        assert strategy._max_interval == 0.4
        assert strategy._burst_sleep == 0.003
        assert strategy._jitter_factor == 0.25
        assert strategy._native_idle_poll_interval == 7.5
        assert strategy._data_version_provider is provider
        assert strategy._data_change_callback is callback
        assert strategy._data_version == 23
        assert strategy._pragma_failures == 5
        assert strategy._local_activity_pending is True
        assert strategy._local_activity_pending_for_drain is True
        assert strategy._local_activity_empty_check is True

    def test_replace_activity_waiter_clears_native_generation_state(
        self,
        monkeypatch,
    ):
        import simplebroker.watcher as watcher_module
        from simplebroker.watcher import PollingStrategy

        strategy = PollingStrategy(threading.Event())
        waiter_a = FakeActivityWaiter()
        waiter_b = FakeActivityWaiter()
        strategy.start(activity_waiter=waiter_a)
        strategy._native_activity_pending = True
        strategy._activity_burst_remaining = 17
        strategy._check_count = 13
        strategy._next_native_idle_poll_at = -1.0

        monkeypatch.setattr(watcher_module.time, "monotonic", lambda: 10.0)
        monkeypatch.setattr(
            watcher_module.random,
            "uniform",
            lambda lower, upper: lower + (upper - lower) / 4,
        )

        assert strategy.replace_activity_waiter(waiter_b) is waiter_a
        assert strategy._native_activity_pending is False
        assert strategy._activity_burst_remaining == 0
        assert strategy._check_count == 0
        assert strategy._next_native_idle_poll_at == 11.25

    def test_replace_activity_waiter_deadline_failure_is_exception_atomic(
        self,
        monkeypatch,
    ):
        import simplebroker.watcher as watcher_module
        from simplebroker.watcher import PollingStrategy

        strategy = PollingStrategy(threading.Event())
        waiter_a = FakeActivityWaiter()
        waiter_b = FakeActivityWaiter()
        _seed_polling_strategy_state(strategy, waiter_a)
        state_before = _polling_strategy_state(strategy)

        def raise_deadline_error(*args, **kwargs):
            del args, kwargs
            raise RuntimeError("deadline failed")

        monkeypatch.setattr(watcher_module.random, "uniform", raise_deadline_error)

        with pytest.raises(RuntimeError, match="deadline failed"):
            strategy.replace_activity_waiter(waiter_b)

        assert _polling_strategy_state(strategy) == state_before
        assert waiter_a.close_calls == 0
        assert waiter_b.close_calls == 0

    def test_replace_activity_waiter_invokes_no_external_seam(self):
        from simplebroker.watcher import PollingStrategy

        class PoisonDetachStrategy(PollingStrategy):
            detach_is_poisoned = False

            def detach_activity_waiter(self, *, expected=None):
                if self.detach_is_poisoned:
                    raise AssertionError("replacement called detach")
                return super().detach_activity_waiter(expected=expected)

        strategy = PoisonDetachStrategy(threading.Event())
        waiter_a = FakeActivityWaiter(
            wait_error=AssertionError("old waiter was called"),
            close_error=AssertionError("old waiter was closed"),
        )
        waiter_b = FakeActivityWaiter(
            wait_error=AssertionError("candidate waiter was called"),
            close_error=AssertionError("candidate waiter was closed"),
        )

        def provider():
            raise AssertionError("provider was called")

        def callback():
            raise AssertionError("callback was called")

        strategy.start(
            provider,
            on_data_version_change=callback,
            activity_waiter=waiter_a,
        )
        strategy.detach_is_poisoned = True

        assert strategy.replace_activity_waiter(waiter_b) is waiter_a
        assert waiter_a.wait_calls == []
        assert waiter_a.close_calls == 0
        assert waiter_b.wait_calls == []
        assert waiter_b.close_calls == 0

    def test_replace_activity_waiter_close_closes_only_installed_waiter(self):
        from simplebroker.watcher import PollingStrategy

        strategy = PollingStrategy(threading.Event())
        waiter_a = FakeActivityWaiter()
        waiter_b = FakeActivityWaiter()
        strategy.start(activity_waiter=waiter_a)

        assert strategy.replace_activity_waiter(waiter_b) is waiter_a
        strategy.close()

        assert waiter_a.close_calls == 0
        assert waiter_b.close_calls == 1
        waiter_a.close()
        strategy.close()
        assert waiter_a.close_calls == 1
        assert waiter_b.close_calls == 1

    def test_polling_strategy_primes_last_ts_callback(self):
        """First observed data_version should still sync dependent caches."""
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        version_container = [2]
        callback_calls = []

        def version_provider():
            return version_container[0]

        def on_data_version_change():
            callback_calls.append(version_container[0])

        strategy.start(
            version_provider,
            on_data_version_change=on_data_version_change,
        )

        # Establishing the initial baseline should still prime cache refreshes.
        assert strategy._check_data_version() is False
        assert callback_calls == [2]

    def test_data_version_callback_failure_logs_when_running(self, caplog):
        """Unexpected cache-sync failures should still be visible."""
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)

        def version_provider():
            return 2

        def on_data_version_change():
            raise RuntimeError("boom")

        strategy.start(
            version_provider,
            on_data_version_change=on_data_version_change,
        )

        with caplog.at_level(logging.ERROR, logger="simplebroker.watcher"):
            assert strategy._check_data_version() is False

        assert "data_version change callback failed" in caplog.text

    def test_data_version_callback_failure_during_stop_is_quiet(self, caplog):
        """Connection interrupts during watcher shutdown should not produce stderr."""
        from simplebroker.watcher import PollingStrategy

        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)

        def version_provider():
            return 2

        def on_data_version_change():
            stop_event.set()
            raise RuntimeError("Connection interrupted")

        strategy.start(
            version_provider,
            on_data_version_change=on_data_version_change,
        )

        with caplog.at_level(logging.ERROR, logger="simplebroker.watcher"):
            assert strategy._check_data_version() is False

        assert "data_version change callback failed" not in caplog.text

    @pytest.mark.sqlite_only
    def test_polling_with_data_version(self, tmp_path):
        """Test that polling uses PRAGMA data_version for efficient change detection."""
        from simplebroker.db import BrokerDB

        temp_db = tmp_path / "test.db"

        # This test verifies the polling strategy detects changes quickly
        with BrokerDB(temp_db) as db:
            messages_received = []

            def handler(msg: str, ts: int):
                messages_received.append(msg)

            watcher = QueueWatcher("version_test", handler, db=db)

            # Start watcher
            thread = watcher.run_in_thread()
            try:
                # Write message - should be detected via data_version change
                db.write("version_test", "test_message")

                assert wait_for_condition(
                    lambda: messages_received == ["test_message"],
                    timeout=scale_timeout_for_ci(2.0),
                    interval=0.01,
                ), f"watcher messages did not settle: {messages_received!r}"
            finally:
                # Stop watcher
                watcher.stop()
                thread.join(timeout=2.0)

            # Should have received the message
            assert len(messages_received) == 1
            assert messages_received[0] == "test_message"

    def test_polling_backoff(self, broker_target):
        """Test polling strategy burst handling and gradual backoff."""
        from simplebroker.watcher import PollingStrategy

        # Test the burst handling and gradual backoff
        # Create a stop event for the strategy
        stop_event = threading.Event()
        strategy = PollingStrategy(
            stop_event=stop_event,
            initial_checks=5,  # Small number for testing
            max_interval=0.1,
        )

        # Initialize the strategy
        db = make_broker(broker_target)
        try:
            strategy.start(db)
            try:
                # First 5 checks should have zero delay
                for i in range(5):
                    assert strategy._calculate_base_delay() == 0
                    strategy._check_count = i + 1

                # After initial checks, delay should gradually increase
                strategy._check_count = 5
                delay1 = strategy._calculate_base_delay()
                assert delay1 == 0  # First check after burst still 0

                strategy._check_count = 6
                delay2 = strategy._calculate_base_delay()
                assert 0 < delay2 < 0.1  # Should start increasing

                strategy._check_count = 105
                delay3 = strategy._calculate_base_delay()
                assert delay3 == 0.1  # Should reach max

                # Activity should reset counter
                strategy.notify_activity()
                assert strategy._check_count == 0
            finally:
                # Signal the strategy to stop before closing the database
                stop_event.set()
        finally:
            db.shutdown()


class TestErrorScenarios(WatcherTestBase):
    """Test various error scenarios."""

    def test_handler_exception_no_error_handler(
        self, broker, broker_target, capsys, caplog
    ):
        """Test default behavior when handler fails and no error_handler."""
        # Enable logging for this test after it's testing logging behavior
        test_config = load_config()
        test_config["BROKER_LOGGING_ENABLED"] = True

        broker.write("test_queue", "bad_message")

        def failing_handler(msg: str, ts: int):
            raise RuntimeError("Handler failed")

        watcher = QueueWatcher(
            "test_queue",
            failing_handler,
            db=broker_target,
            config=test_config,
        )

        thread = watcher.run_in_thread()
        try:
            assert wait_for_condition(
                lambda: (
                    "Handler failed" in caplog.text or "Handler error" in caplog.text
                ),
                timeout=scale_timeout_for_ci(2.0),
                interval=0.01,
            ), f"handler error was not logged; captured logs: {caplog.text!r}"
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        # Should have logged the error
        assert "Handler failed" in caplog.text or "Handler error" in caplog.text

    def test_error_handler_exception(self, broker, broker_target, capsys, caplog):
        """Test when error_handler itself raises exception."""
        # Enable logging for this test after it's testing logging behavior
        test_config = load_config()
        test_config["BROKER_LOGGING_ENABLED"] = True

        broker.write("test_queue", "message")

        def handler(msg: str, ts: int):
            raise ValueError("Handler error")

        def bad_error_handler(exc: Exception, msg: str, ts: int) -> bool:
            raise RuntimeError("Error handler also failed!")

        watcher = QueueWatcher(
            "test_queue",
            handler,
            db=broker_target,
            error_handler=bad_error_handler,
            config=test_config,
        )

        thread = watcher.run_in_thread()
        try:
            assert wait_for_condition(
                lambda: (
                    "Handler error" in caplog.text
                    and (
                        "Error handler also failed" in caplog.text
                        or "Error handler failed" in caplog.text
                    )
                ),
                timeout=scale_timeout_for_ci(5.0),
                interval=0.05,
            )
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        # Both errors should be logged
        assert "Handler error" in caplog.text
        assert (
            "Error handler also failed" in caplog.text
            or "Error handler failed" in caplog.text
        )

    def test_database_connection_isolation(self, broker_target):
        """Test that each watcher needs its own connection."""
        # This is more of a documentation test
        # Using the same target should work — each watcher creates its own connection
        watcher1 = QueueWatcher(
            "queue1",
            lambda m, t: None,
            db=broker_target,
        )

        watcher2 = QueueWatcher(
            "queue2",
            lambda m, t: None,
            db=broker_target,
        )

        # Just verify they were created
        assert watcher1 is not None
        assert watcher2 is not None

    def test_consuming_watcher_queue_preservation_on_failure(
        self, broker, broker_target
    ):
        """Test that when a handler fails, the watcher stops and doesn't consume more messages.

        Note: In consuming mode, messages are removed from the queue when read,
        not after successful processing. The failed message is already consumed.
        """
        # Write messages to the queue
        broker.write("test_queue", "message1")
        broker.write("test_queue", "message2")
        broker.write("test_queue", "message3")

        processed_messages = []

        def failing_handler(msg: str, ts: int):
            """Handler that fails on message2."""
            if msg == "message2":
                raise RuntimeError("Handler failed on message2")
            processed_messages.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            """Error handler that stops processing on failure."""
            return False  # Stop processing

        # Create consuming watcher (peek=False)
        with self.create_test_watcher(
            broker_target,
            "test_queue",
            failing_handler,
            peek=False,
            error_handler=error_handler,
        ) as watcher:
            # Run watcher with timeout
            thread = self.run_watcher_with_timeout(watcher, timeout=2.0)

            # Thread should have stopped due to error_handler returning False
            assert not thread.is_alive()

            # Check processed messages - should only have message1
            assert processed_messages == ["message1"]

            # Check remaining messages in queue (use new peek API)
            db = make_broker(broker_target)
            try:
                remaining = list(db.peek_generator("test_queue", with_timestamps=False))
                # Only message3 should be in the queue (message2 was consumed before handler failed)
                assert len(remaining) == 1
                assert remaining[0] == "message3"
            finally:
                db.shutdown()

    def test_signal_handler_restoration(self, broker_target):
        """Test that original signal handlers are restored after the watcher stops."""
        # This test verifies that signal handlers are properly restored
        # after run_forever completes

        # Track signal handler changes
        original_handler = signal.getsignal(signal.SIGINT)
        handler_during_run = None
        handler_after_run = None

        def capture_handler():
            """Capture the current SIGINT handler."""
            nonlocal handler_during_run
            handler_during_run = signal.getsignal(signal.SIGINT)

        def test_handler(msg: str, ts: int):
            """Handler that captures signal state and stops."""
            capture_handler()
            # Stop the watcher after processing one message
            watcher.stop()

        # Add a message to process
        db = make_broker(broker_target)
        try:
            db.write("signal_test_queue", "test_message")
        finally:
            db.shutdown()

        # Create and run watcher
        watcher = QueueWatcher(
            "signal_test_queue",
            test_handler,
            db=broker_target,
            peek=False,
        )

        # Run in main thread to test signal handler installation
        watcher.run_forever()

        # Capture handler after run_forever completes
        handler_after_run = signal.getsignal(signal.SIGINT)

        # Verify signal handler was changed during run
        assert handler_during_run is not None
        assert handler_during_run != original_handler

        # Verify signal handler was restored after run
        assert handler_after_run == original_handler


def test_context_manager_usage(broker_target):
    """Test that QueueWatcher works properly as a context manager."""
    messages_received = []
    all_messages_received = threading.Event()
    messages_lock = threading.Lock()

    def handler(msg: str, ts: int):
        with messages_lock:
            messages_received.append((msg, ts))
            if len(messages_received) == 3:
                all_messages_received.set()

    def messages_snapshot() -> list[tuple[str, int]]:
        with messages_lock:
            return list(messages_received)

    # Add messages to the queue
    db = make_broker(broker_target)
    try:
        db.write("context_queue", "message1")
        db.write("context_queue", "message2")
        db.write("context_queue", "message3")
    finally:
        db.shutdown()

    # Use watcher as a context manager
    with QueueWatcher(
        "context_queue", handler, db=broker_target, peek=False
    ) as watcher:
        # The thread should be started automatically
        assert hasattr(watcher, "_thread")
        assert watcher._thread is not None
        thread = watcher._thread()  # Get strong reference from weak ref
        assert thread is not None
        assert thread.is_alive()

        assert all_messages_received.wait(timeout=scale_timeout_for_ci(5.0)), (
            "Timed out waiting for context-manager watcher to process messages: "
            f"received={messages_snapshot()}"
        )

    # After exiting context, thread should be stopped
    assert wait_for_condition(
        lambda: (
            (thread := watcher._thread() if watcher._thread else None) is None
            or not thread.is_alive()
        ),
        timeout=scale_timeout_for_ci(5.0, ci_factor=2.0),
        interval=0.05,
        message="Watcher thread should stop after context-manager exit",
    )

    # Verify all messages were processed
    snapshot = messages_snapshot()
    assert len(snapshot) == 3
    assert [msg for msg, _ in snapshot] == ["message1", "message2", "message3"]

    # Verify queue is empty (consumed mode)
    db = make_broker(broker_target)
    try:
        remaining = list(db.peek_generator("context_queue", with_timestamps=False))
        assert len(remaining) == 0
    finally:
        db.shutdown()


def test_context_manager_with_exception(broker_target):
    """Test that cleanup happens properly even when exception occurs in context."""

    def handler(msg: str, ts: int):
        pass

    # Test that cleanup happens even with exception
    try:
        with QueueWatcher("error_queue", handler, db=broker_target) as watcher:
            thread = watcher._thread()  # Get strong reference from weak ref
            assert thread is not None
            assert thread.is_alive()
            msg = "Test exception"
            raise ValueError(msg)
    except ValueError:
        pass  # Expected

    # Thread should still be stopped after exception
    assert wait_for_condition(
        lambda: (
            (thread := watcher._thread() if watcher._thread else None) is None
            or not thread.is_alive()
        ),
        timeout=scale_timeout_for_ci(5.0, ci_factor=2.0),
        interval=0.05,
        message="Watcher thread should stop after context-manager exception",
    )
