"""Test edge cases in watcher.py to increase coverage."""

import contextlib
import inspect
import signal
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, NoReturn, cast
from unittest.mock import Mock, patch

import pytest

import simplebroker.watcher as watcher_module
from simplebroker import Queue
from simplebroker._exceptions import OperationalError
from simplebroker._retry import interruptible_sleep
from simplebroker.watcher import (
    PollingStrategy,
    QueueMoveWatcher,
    QueueWatcher,
    _StopLoop,
)

from .helper_scripts.broker_factory import make_broker
from .helper_scripts.database_errors import DatabaseErrorInjector
from .helper_scripts.timing import scale_timeout_for_ci
from .helper_scripts.watcher_base import WatcherTestBase

pytestmark = [pytest.mark.shared]


class WatcherTestError(Exception):
    """Exercise watcher boundaries with an otherwise unknown exception."""


class TestWatcherEdgeCases(WatcherTestBase):
    """Test edge cases in QueueWatcher."""

    def test_unsupported_message_type_is_not_exported(self) -> None:
        assert "Message" not in watcher_module.__all__
        assert not hasattr(watcher_module, "Message")

    def test_watcher_exit_has_context_manager_protocol_signature(self) -> None:
        parameters = inspect.signature(watcher_module.BaseWatcher.__exit__).parameters
        assert list(parameters) == ["self", "exc_type", "exc_val", "exc_tb"]

    def test_invalid_database_owner_type_is_rejected(self) -> None:
        with pytest.raises(TypeError, match="Watcher db= must be a path"):
            QueueWatcher(
                "queue", lambda message, timestamp: None, db=cast(Any, object())
            )

    def test_invalid_handler_type(self, broker_target) -> None:
        """Test that non-callable handler raises TypeError."""
        with pytest.raises(TypeError, match="handler must be callable"):
            QueueWatcher(
                "queue",
                cast(Callable[[str, int], None], "not_callable"),
                db=broker_target,
            )

    def test_invalid_error_handler_type(self, broker_target) -> None:
        """Test that non-callable error_handler raises TypeError."""

        def handler(msg, ts) -> None:
            pass

        with pytest.raises(TypeError, match="error_handler must be callable"):
            QueueWatcher(
                "queue",
                handler,
                db=broker_target,
                error_handler=cast(
                    Callable[[Exception, str, int], bool | None], "not_callable"
                ),
            )

    def test_live_watcher_enforces_instance_size_limit(self, broker_target) -> None:
        """A live watcher enforces its own limit on a pre-existing large row."""
        writer = Queue(
            "queue",
            db_path=broker_target,
            config={"BROKER_MAX_MESSAGE_SIZE": 100},
        )
        observer = Queue("queue", db_path=broker_target)
        handled: list[tuple[str, int]] = []
        errors: list[tuple[Exception, str, int]] = []
        rejected = threading.Event()

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            errors.append((exc, msg, ts))
            rejected.set()
            return True

        watcher = QueueWatcher(
            "queue",
            lambda msg, ts: handled.append((msg, ts)),
            db=broker_target,
            error_handler=error_handler,
            config={"BROKER_MAX_MESSAGE_SIZE": 3},
        )
        try:
            writer.write("toolong")
            watcher.run_in_thread()
            assert rejected.wait(timeout=scale_timeout_for_ci(5.0))

            assert handled == []
            assert len(errors) == 1
            assert isinstance(errors[0][0], ValueError)
            assert "3 byte limit" in str(errors[0][0])
            assert observer.peek_many(10, with_timestamps=False) == []
            assert observer.peek_many(
                10, with_timestamps=False, include_claimed=True
            ) == ["toolong"]
        finally:
            watcher.stop()
            writer.close()
            observer.close()

    def test_live_watcher_logs_and_discards_oversized_message(
        self, broker_target, caplog
    ) -> None:
        writer = Queue(
            "queue",
            db_path=broker_target,
            config={"BROKER_MAX_MESSAGE_SIZE": 100},
        )
        handled = threading.Event()
        watcher = QueueWatcher(
            "queue",
            lambda _message, _timestamp: handled.set(),
            db=broker_target,
            config={
                "BROKER_LOGGING_ENABLED": True,
                "BROKER_MAX_MESSAGE_SIZE": 1,
            },
        )
        try:
            writer.write("too large")
            with caplog.at_level("ERROR", logger="simplebroker.watcher"):
                watcher.run_in_thread()
                deadline = time.monotonic() + scale_timeout_for_ci(5.0)
                while "exceeds 1 byte limit" not in caplog.text:
                    assert time.monotonic() < deadline
                    time.sleep(0.01)
        finally:
            watcher.stop()
            writer.close()

        assert not handled.is_set()
        assert "exceeds 1 byte limit" in caplog.text
        assert writer.peek_many(10, with_timestamps=False) == []

    def test_legacy_dispatch_supports_handler_and_no_handler_states(
        self, broker_target
    ) -> None:
        handled: list[tuple[str, int]] = []
        watcher = QueueWatcher(
            "queue",
            lambda message, timestamp: handled.append((message, timestamp)),
            db=broker_target,
        )
        watcher._error_handler = None
        try:
            assert watcher._dispatch("payload", 123) is True
            watcher._handler = None
            assert watcher._dispatch("ignored", 456) is False
            assert (
                watcher._safe_call_handler("ignored", 456, lambda *args: True) is False
            )
        finally:
            watcher.stop()

        assert handled == [("payload", 123)]

    def test_operational_retry_logs_attempts_and_exhaustion(
        self, broker_target, caplog
    ) -> None:
        watcher = QueueWatcher(
            "queue",
            lambda message, timestamp: None,
            db=broker_target,
            config={"BROKER_LOGGING_ENABLED": True},
        )
        attempts = 0

        def locked_operation() -> None:
            nonlocal attempts
            attempts += 1
            raise OperationalError("database is locked")

        try:
            with (
                caplog.at_level("DEBUG", logger="simplebroker.watcher"),
                pytest.raises(OperationalError, match="database is locked"),
            ):
                watcher._process_with_retry(locked_operation, "locked-test")
        finally:
            watcher.stop()

        assert attempts == 5
        assert "OperationalError during locked-test" in caplog.text
        assert "Failed after 5 operational errors" in caplog.text

    def test_polling_strategy_handles_missing_versions_and_local_empty_hint(
        self,
    ) -> None:
        strategy = PollingStrategy(threading.Event())

        assert strategy._check_data_version() is False
        strategy.start(lambda: None)
        assert strategy._check_data_version() is False

        strategy.mark_local_activity_as_empty_check()
        assert strategy.consume_local_empty_check_hint() is True
        assert strategy.consume_local_empty_check_hint() is False

    def test_error_handler_returns_false(self, broker_target) -> None:
        """Test that error handler returning False stops the watcher."""

        def handler(msg, ts) -> NoReturn:
            msg = "Handler error"
            raise ValueError(msg)

        def error_handler(exc, msg, ts) -> bool:
            return False  # Request stop

        watcher = QueueWatcher(
            "queue",
            handler,
            db=broker_target,
            error_handler=error_handler,
        )

        # Test dispatch
        with pytest.raises(_StopLoop):
            watcher._dispatch("test", 12345)

        # Verify stop event was set
        assert watcher._stop_event.is_set()

    def test_type_error_inside_error_handler_is_not_retried(
        self,
        broker,
        broker_target,
        caplog,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker.write("queue", "payload")
        calls: list[tuple[Exception, str, int, object]] = []
        called = threading.Event()
        hook_called = threading.Event()
        hook_calls: list[threading.ExceptHookArgs] = []
        handler_failure = ValueError("handler failed")
        callback_failure = TypeError("error handler body failed")
        watcher: QueueWatcher

        def handler(_message: str, _timestamp: int) -> None:
            raise handler_failure

        def error_handler(
            exc: Exception,
            message: str,
            timestamp: int,
            *,
            config: object = None,
        ) -> None:
            calls.append((exc, message, timestamp, config))
            called.set()
            watcher.stop(join=False)
            raise callback_failure

        def capture_thread_failure(args: threading.ExceptHookArgs) -> None:
            hook_calls.append(args)
            hook_called.set()

        monkeypatch.setattr(threading, "excepthook", capture_thread_failure)

        watcher = QueueWatcher(
            "queue",
            handler,
            db=broker_target,
            error_handler=error_handler,
            config={"BROKER_LOGGING_ENABLED": 1},
        )

        with caplog.at_level("ERROR", logger="simplebroker.watcher"):
            thread = watcher.run_in_thread()
            try:
                assert called.wait(timeout=2.0)
                assert hook_called.wait(timeout=2.0)
                thread.join(timeout=2.0)
                assert not thread.is_alive()
            finally:
                watcher.stop()
                thread.join(timeout=2.0)

        assert len(calls) == 1
        assert calls[0][1] == "payload"
        assert len(hook_calls) == 1
        assert hook_calls[0].exc_value is callback_failure
        assert callback_failure.__cause__ is handler_failure
        assert (
            sum("Error handler failed" in record.message for record in caplog.records)
            == 1
        )

    @pytest.mark.parametrize("watcher_kind", ["queue", "move"])
    @pytest.mark.parametrize("logging_enabled", [0, 1])
    def test_default_error_handler_uses_instance_logging_config(
        self, broker_target, caplog, watcher_kind: str, logging_enabled: int
    ) -> None:
        def handler(_message: str, _timestamp: int) -> None:
            raise ValueError("instance-config-handler-error")

        watcher: QueueWatcher | QueueMoveWatcher
        if watcher_kind == "queue":
            watcher = QueueWatcher(
                "queue",
                handler,
                db=broker_target,
                config={"BROKER_LOGGING_ENABLED": logging_enabled},
            )
        else:
            watcher = QueueMoveWatcher(
                "source",
                "destination",
                handler,
                db=broker_target,
                config={"BROKER_LOGGING_ENABLED": logging_enabled},
            )

        try:
            with caplog.at_level("ERROR", logger="simplebroker.watcher"):
                watcher._dispatch("payload", 123)
        finally:
            watcher.stop()

        handler_logs = [
            record
            for record in caplog.records
            if "instance-config-handler-error" in record.message
        ]
        assert len(handler_logs) == logging_enabled

    @pytest.mark.sqlite_only
    def test_polling_strategy_pragma_failures(self) -> None:
        """The polling fallback escalates exactly at its documented threshold."""
        strategy = PollingStrategy(threading.Event())
        calls = 0

        def failing_provider() -> int:
            nonlocal calls
            calls += 1
            raise WatcherTestError("PRAGMA failed")

        strategy.start(failing_provider)
        assert [strategy._check_data_version() for _ in range(9)] == [False] * 9
        assert calls == 9

        with pytest.raises(RuntimeError, match="failed 10 times.*PRAGMA failed"):
            strategy._check_data_version()
        assert calls == 10

    def test_watcher_retry_with_exponential_backoff(
        self, broker_target, monkeypatch
    ) -> None:
        """Test watcher retry logic with exponential backoff."""
        retry_sleeps = []
        drain_count = 0

        def fake_interruptible_sleep(wait_time, stop_event) -> bool:
            retry_sleeps.append(wait_time)
            return not stop_event.is_set()

        monkeypatch.setattr(
            watcher_module,
            "interruptible_sleep",
            fake_interruptible_sleep,
        )

        with self.create_test_watcher(
            broker_target,
            "queue",
            lambda m, t: None,
        ) as watcher:
            # Mock drain_queue to track retry timing
            original_drain = watcher._drain_queue

            def failing_drain() -> None:
                nonlocal drain_count
                drain_count += 1
                if drain_count < 3:
                    msg = "Drain failed"
                    raise WatcherTestError(msg)
                # Stop after successful drain
                watcher.stop()
                original_drain()

            watcher._drain_queue = failing_drain  # type: ignore[method-assign]  # intentional private retry seam

            # Run with timeout
            self.run_watcher_with_timeout(watcher, timeout=10.0)

            # Verify retries happened with exponential backoff
            assert drain_count >= 3
            assert retry_sleeps[:2] == [2, 4]

    def test_stop_during_retry_sleep_exits_cleanly(
        self, broker_target, monkeypatch
    ) -> None:
        """A stop signal during retry backoff should end the retry loop."""
        retry_sleeps = []
        drain_count = 0

        def fake_interruptible_sleep(wait_time, stop_event) -> bool:
            retry_sleeps.append(wait_time)
            stop_event.set()
            return False

        monkeypatch.setattr(
            watcher_module,
            "interruptible_sleep",
            fake_interruptible_sleep,
        )

        with self.create_test_watcher(
            broker_target,
            "queue",
            lambda m, t: None,
        ) as watcher:

            def failing_drain() -> NoReturn:
                nonlocal drain_count
                drain_count += 1
                raise WatcherTestError("retry sleep should be interrupted")

            watcher._drain_queue = failing_drain  # type: ignore[method-assign]  # intentional private retry seam

            watcher.run_forever()

            assert drain_count == 1
            assert retry_sleeps == [2]
            assert watcher._stop_event.is_set()

    def test_watcher_max_retries_exceeded(
        self,
        broker_target,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Terminal retry preserves the original failure and helper contract."""
        with self.create_test_watcher(
            broker_target,
            "queue",
            lambda m, t: None,
        ) as watcher:
            cause = ValueError("underlying failure")
            failure = WatcherTestError("Persistent failure")
            retry_calls: list[tuple[Exception, int, int]] = []
            retry_results: list[bool] = []
            original_handle_retry = watcher._handle_retry

            def failing_drain() -> NoReturn:
                try:
                    raise cause
                except ValueError:
                    raise failure from cause

            def recording_handle_retry(
                error: Exception,
                retry_count: int,
                max_retries: int,
            ) -> bool:
                retry_calls.append((error, retry_count, max_retries))
                result = original_handle_retry(error, retry_count, max_retries)
                retry_results.append(result)
                return result

            watcher._drain_queue = failing_drain  # type: ignore[method-assign]  # intentional private retry seam
            watcher._handle_retry = recording_handle_retry  # type: ignore[method-assign, assignment]  # intentional private retry seam
            monkeypatch.setattr(watcher_module, "interruptible_sleep", lambda *_: True)

            with (
                caplog.at_level("ERROR", logger="simplebroker.watcher"),
                pytest.raises(WatcherTestError, match="Persistent failure") as exc_info,
            ):
                watcher._run_with_retries(max_retries=3)

            raised = exc_info.value
            assert raised is failure
            assert type(raised) is WatcherTestError
            assert str(raised) == "Persistent failure"
            assert raised.__cause__ is cause
            assert raised.__context__ is cause
            assert "failing_drain" in {frame.name for frame in exc_info.traceback}
            assert retry_calls == [
                (failure, 1, 3),
                (failure, 2, 3),
                (failure, 3, 3),
            ]
            assert retry_results == [True, True, False]
            assert (
                "Watcher failed after 3 retries. Last error: Persistent failure"
                in caplog.text
            )

    def test_cleanup_thread_local_delegates_and_propagates(self, broker_target) -> None:
        """Thread-local cleanup delegates once and leaves error policy to callers."""
        watcher = QueueWatcher("queue", lambda m, t: None, db=broker_target)
        try:
            with patch.object(
                watcher._queue_obj,
                "cleanup_connections",
                side_effect=RuntimeError("cleanup failed"),
            ) as cleanup:
                with pytest.raises(RuntimeError, match="cleanup failed"):
                    watcher._cleanup_thread_local()
                cleanup.assert_called_once_with()
        finally:
            watcher.stop()

    def test_handle_retry_suppresses_cleanup_failure(
        self,
        broker_target,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry recovery continues when best-effort cleanup fails."""
        watcher = QueueWatcher("queue", lambda m, t: None, db=broker_target)
        cleanup = Mock(side_effect=RuntimeError("cleanup failed"))
        try:
            monkeypatch.setattr(watcher_module, "interruptible_sleep", lambda *_: True)
            monkeypatch.setattr(watcher, "_cleanup_thread_local", cleanup)

            assert watcher._handle_retry(
                RuntimeError("operation failed"),
                retry_count=1,
                max_retries=3,
            )
            cleanup.assert_called_once_with()
        finally:
            monkeypatch.undo()
            watcher.stop()

    @pytest.mark.parametrize(
        ("logging_enabled", "warning_expected"),
        [(False, False), (True, True)],
    )
    def test_context_manager_stop_warning_uses_instance_config(
        self,
        broker_target,
        logging_enabled: bool,
        warning_expected: bool,
    ) -> None:
        watcher = QueueWatcher(
            "queue",
            lambda m, t: None,
            db=broker_target,
            config={"BROKER_LOGGING_ENABLED": logging_enabled},
        )
        try:
            with (
                patch.object(
                    watcher,
                    "stop",
                    side_effect=WatcherTestError("Stop failed"),
                ) as stop,
                patch.object(watcher_module.logger, "warning") as warning,
            ):
                watcher.__exit__(None, None, None)
                stop.assert_called_once_with()

            if warning_expected:
                warning.assert_called_once_with(
                    "Error during stop in __exit__: Stop failed"
                )
            else:
                warning.assert_not_called()
        finally:
            watcher.stop()

    def test_signal_handler_not_main_thread(self, broker_target) -> None:
        """A non-main-thread run must leave the SIGINT handler untouched.

        Runs in a genuinely non-main thread and asserts the installed
        handler is unchanged afterward — no process-global Mock of
        ``threading.current_thread``, which every other live watcher
        thread in the worker would observe.
        """
        watcher = QueueWatcher("queue", lambda m, t: None, db=broker_target)
        handler_before = signal.getsignal(signal.SIGINT)

        def run_watcher() -> None:
            # Genuinely not the main thread: run_forever must skip signal
            # handler installation entirely.
            watcher.stop()
            watcher.run_forever()

        thread = threading.Thread(target=run_watcher)
        thread.start()
        try:
            thread.join(timeout=5)
            if thread.is_alive():
                with contextlib.suppress(Exception):
                    watcher.stop()
                    thread.join(timeout=1.0)
                pytest.fail("Thread did not complete within timeout")
        finally:
            if thread.is_alive():
                with contextlib.suppress(Exception):
                    watcher.stop()

        assert signal.getsignal(signal.SIGINT) is handler_before

    def test_absolute_timeout_exceeded(self, broker_target) -> None:
        """Test that watcher fails after MAX_TOTAL_RETRY_TIME."""
        with self.create_test_watcher(
            broker_target,
            "queue",
            lambda m, t: None,
        ) as watcher:
            # Mock time.monotonic() to simulate time passing faster
            original_time = time.monotonic
            start_real_time = original_time()

            def mock_time():
                # Make time appear to pass 100x faster
                elapsed = original_time() - start_real_time
                return start_real_time + (elapsed * 100)

            # Mock drain_queue to always fail
            def failing_drain() -> NoReturn:
                # Use real sleep to let the retry loop run
                time.sleep(0.01)
                msg = "Persistent failure"
                raise WatcherTestError(msg)

            watcher._drain_queue = failing_drain  # type: ignore[method-assign]  # intentional private retry seam

            # Patch time.monotonic to make timeout trigger quickly
            with patch("simplebroker.watcher._monotonic", mock_time):
                # Should raise TimeoutError after simulated 300s (3s real time)
                with pytest.raises(TimeoutError) as exc_info:
                    watcher.run_forever()

                assert "retry timeout exceeded" in str(exc_info.value)
                assert "300s" in str(exc_info.value)  # Default timeout

    def test_interruptible_sleep_responsiveness(self, broker, broker_target) -> None:
        """Test that watcher responds quickly to stop signals."""
        # Write a message
        broker.write("slow_queue", "test message")

        process_start = None
        handler_started = threading.Event()

        def slow_handler(msg, ts) -> None:
            nonlocal process_start
            process_start = time.monotonic()
            handler_started.set()  # Signal that handler has started
            # Simulate slow processing with interruptible sleep
            interruptible_sleep(1.0, watcher._stop_event)

        with self.create_test_watcher(
            broker_target,
            "slow_queue",
            slow_handler,
        ) as watcher:
            thread = None
            try:
                # Start watcher
                thread = watcher.run_in_thread()

                # Wait for handler to start processing
                if not handler_started.wait(timeout=2.0):
                    watcher.stop()
                    thread.join(timeout=1.0)
                    pytest.fail("Handler did not start processing within timeout")

                # Stop should interrupt the sleep
                start_stop = time.monotonic()
                watcher.stop()
                thread.join(timeout=0.5)  # Should complete quickly
                stop_time = time.monotonic() - start_stop

                assert stop_time < 0.5, f"Stop took {stop_time:.2f}s, should be < 0.5s"
                assert not thread.is_alive()
            finally:
                # Ensure proper thread cleanup
                if thread and thread.is_alive():
                    with contextlib.suppress(Exception):
                        watcher.stop()
                        thread.join(timeout=1.0)

    def test_concurrent_stop_safety(self, broker, broker_target) -> None:
        """Test stopping watcher from multiple threads."""
        # Add many messages
        for i in range(50):
            broker.write("concurrent_queue", f"msg{i}")

        processing_started = threading.Event()
        process_count = 0
        process_lock = threading.Lock()

        def slow_handler(msg, ts) -> None:
            nonlocal process_count
            with process_lock:
                process_count += 1
                if process_count == 1:
                    processing_started.set()  # Signal first message processed
            time.sleep(0.01)  # Slow processing

        with self.create_test_watcher(
            broker_target,
            "concurrent_queue",
            slow_handler,
        ) as watcher:
            thread = None
            stop_threads = []
            try:
                thread = watcher.run_in_thread()

                # Wait for processing to start
                if not processing_started.wait(timeout=2.0):
                    watcher.stop()
                    thread.join(timeout=1.0)
                    pytest.fail("Handler did not start processing within timeout")

                # Multiple threads try to stop
                for _ in range(5):
                    t = threading.Thread(target=watcher.stop)
                    stop_threads.append(t)
                    t.start()

                # All should complete quickly
                for t in stop_threads:
                    t.join(timeout=0.5)
                    assert not t.is_alive()

                # Main thread should stop
                thread.join(timeout=1.0)
                assert not thread.is_alive()
            finally:
                # Ensure all threads are cleaned up
                for t in stop_threads:
                    if t.is_alive():
                        with contextlib.suppress(Exception):
                            t.join(timeout=0.5)
                if thread and thread.is_alive():
                    with contextlib.suppress(Exception):
                        watcher.stop()
                        thread.join(timeout=1.0)


class TestQueueMoveWatcherEdgeCases(WatcherTestBase):
    """Test edge cases in QueueMoveWatcher."""

    def test_same_queue_error(self, broker_target) -> None:
        """Test that moving to same queue raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Cannot move messages to the same queue",
        ):
            QueueMoveWatcher("queue", "queue", lambda m, t: None, db=broker_target)

    def test_move_with_handler_error(self, broker_target) -> None:
        """Test that handler errors don't affect move (already completed)."""
        broker = make_broker(broker_target)
        try:
            # Add a message to source queue
            broker.write("source", "test message")

            handler_called = []

            def handler(msg, ts) -> NoReturn:
                handler_called.append((msg, ts))
                msg = "Handler failed"
                raise ValueError(msg)

            def error_handler(exc, msg, ts) -> bool:
                return True  # Continue

            watcher = QueueMoveWatcher(
                "source",
                "dest",
                handler,
                db=broker_target,
                error_handler=error_handler,
                max_messages=1,
            )

            # Run move - should handle the error internally
            watcher.run()

            # Verify message was moved despite handler error
            queues = {
                name: pending for name, pending, _total in broker.get_queue_stats()
            }
            assert queues.get("source", 0) == 0
            assert queues.get("dest", 0) == 1

            # Verify handler was called
            assert len(handler_called) == 1
            assert handler_called[0][0] == "test message"

            # Ensure watcher is stopped
            if hasattr(watcher, "stop"):
                watcher.stop()
        finally:
            broker.shutdown()

    @pytest.mark.sqlite_only
    def test_move_unexpected_error(self) -> None:
        """Test handling of unexpected errors during move."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Create database and add message to source queue
            from simplebroker.db import BrokerDB

            db = BrokerDB(str(db_path))
            db.write("source", "test_message")
            db.close()

            # Make the database read-only before creating watcher
            # This will cause the move operation to fail with a real error
            DatabaseErrorInjector.create_readonly_database(str(db_path))

            try:
                # Creating the watcher should work, but drain operations should fail
                # Due to the read-only database, we expect RuntimeError or OperationalError
                with (
                    pytest.raises(
                        (RuntimeError, OperationalError),
                        match="readonly|read-only|attempt to write|Failed to get database connection",
                    ),
                    self.create_test_move_watcher(
                        str(db_path), "source", "dest", lambda m, t: None
                    ) as watcher,
                ):
                    watcher._drain_queue()
            finally:
                # Restore write permissions for cleanup
                DatabaseErrorInjector.restore_writable(str(db_path))

    def test_polling_strategy_activity_detection(self) -> None:
        """Test that polling strategy detects database changes."""
        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)

        # Use a list to allow mutation in the closure
        version_container = [1]

        # Create a data version provider that returns the current version
        def version_provider():
            return version_container[0]

        strategy.start(version_provider)

        # No change detected on first check (establishes baseline)
        assert strategy._check_data_version() is False

        # Now simulate a change
        version_container[0] = 2

        # Should detect change
        assert strategy._check_data_version() is True

        # Check count should reset on activity
        strategy.notify_activity()
        assert strategy._check_count == 0
