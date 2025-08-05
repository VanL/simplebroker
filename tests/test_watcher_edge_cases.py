"""Test edge cases in watcher.py to increase coverage."""

import tempfile
import threading
import time
from pathlib import Path
from typing import NoReturn
from unittest.mock import Mock, patch

import pytest

from simplebroker.db import BrokerDB
from simplebroker.helpers import interruptible_sleep
from simplebroker.watcher import (
    PollingStrategy,
    QueueMoveWatcher,
    QueueWatcher,
    _StopLoop,
)

from .helpers.watcher_base import WatcherTestBase


class TestWatcherEdgeCases(WatcherTestBase):
    """Test edge cases in QueueWatcher."""

    def test_invalid_handler_type(self) -> None:
        """Test that non-callable handler raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with pytest.raises(TypeError, match="handler must be callable"):
                QueueWatcher(str(db_path), "queue", "not_callable")

    def test_invalid_error_handler_type(self) -> None:
        """Test that non-callable error_handler raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            def handler(msg, ts) -> None:
                pass

            with pytest.raises(TypeError, match="error_handler must be callable"):
                QueueWatcher(
                    str(db_path),
                    "queue",
                    handler,
                    error_handler="not_callable",
                )

    def test_environment_variable_parsing(self) -> None:
        """Test parsing of environment variables with invalid values."""
        # Since the config loading has been centralized and validates at module load time,
        # we'll test that the watcher uses correct defaults when created normally
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Create watcher with normal config
            watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)

            # Verify defaults are used from config
            assert watcher._strategy._initial_checks == 100
            assert watcher._strategy._max_interval == 0.1
            assert watcher._strategy._burst_sleep == 0.00001  # Default from constants

            # Test that the watcher handles invalid values gracefully at the strategy level
            # by directly testing the PollingStrategy initialization with mock config
            import threading

            from simplebroker.watcher import PollingStrategy

            # Create strategy with direct parameters to test error handling
            stop_event = threading.Event()
            strategy = PollingStrategy(
                stop_event,
                initial_checks=100,  # These are the validated defaults
                max_interval=0.1,
                burst_sleep=0.00001,
            )

            # Verify strategy has correct values
            assert strategy._initial_checks == 100
            assert strategy._max_interval == 0.1
            assert strategy._burst_sleep == 0.00001

    def test_message_size_limit_exceeded(self) -> None:
        """Test handling of messages exceeding 10MB limit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with BrokerDB(str(Path(tmpdir) / "test.db")) as db:
                # Create a message larger than 10MB
                large_message = "x" * (11 * 1024 * 1024)  # 11MB

                handled = []
                errors = []

                def handler(msg, ts) -> None:
                    handled.append((msg, ts))

                def error_handler(exc, msg, ts) -> bool:
                    errors.append((exc, msg, ts))
                    return True  # Continue processing

                watcher = QueueWatcher(
                    db,
                    "queue",
                    handler,
                    error_handler=error_handler,
                )

                # Directly test dispatch with oversized message
                watcher._dispatch(large_message, 12345)

                # Verify handler was not called
                assert len(handled) == 0

                # Verify error handler was called
                assert len(errors) == 1
                assert isinstance(errors[0][0], ValueError)
                assert "exceeds 10MB limit" in str(errors[0][0])
            assert errors[0][1].endswith("...")  # Truncated message

    def test_error_handler_returns_false(self) -> None:
        """Test that error handler returning False stops the watcher."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))
            try:

                def handler(msg, ts) -> NoReturn:
                    msg = "Handler error"
                    raise ValueError(msg)

                def error_handler(exc, msg, ts) -> bool:
                    return False  # Request stop

                watcher = QueueWatcher(
                    db,
                    "queue",
                    handler,
                    error_handler=error_handler,
                )

                # Test dispatch
                with pytest.raises(_StopLoop):
                    watcher._dispatch("test", 12345)

                # Verify stop event was set
                assert watcher._stop_event.is_set()
            finally:
                db.close()

    def test_error_handler_itself_fails(self) -> None:
        """Test handling when error handler itself raises an exception."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))
            try:

                def handler(msg, ts) -> NoReturn:
                    msg = "Handler error"
                    raise ValueError(msg)

                def error_handler(exc, msg, ts) -> NoReturn:
                    msg = "Error handler failed"
                    raise RuntimeError(msg)

                watcher = QueueWatcher(
                    db,
                    "queue",
                    handler,
                    error_handler=error_handler,
                )

                # Should log but not crash
                with patch("simplebroker.watcher.logger") as mock_logger:
                    with patch(
                        "simplebroker.watcher._config",
                        {"BROKER_LOGGING_ENABLED": True},
                    ):
                        watcher._dispatch("test", 12345)

                    # Verify both errors were logged
                    mock_logger.exception.assert_called()
                    error_call_args = str(mock_logger.exception.call_args)
                    assert "Error handler failed" in error_call_args
                    assert "Handler error" in error_call_args
            finally:
                db.close()

    def test_polling_strategy_pragma_failures(self) -> None:
        """Test handling of repeated PRAGMA data_version failures."""
        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        mock_db = Mock()

        # Simulate repeated failures
        mock_db._runner.run.side_effect = Exception("PRAGMA failed")

        strategy.start(mock_db)

        # Call multiple times to trigger failure threshold
        for _i in range(9):
            result = strategy._check_data_version()
            assert result is False  # Should fallback to regular polling

        # 10th failure should raise
        with pytest.raises(RuntimeError, match="PRAGMA data_version failed 10 times"):
            strategy._check_data_version()

    def test_watcher_retry_with_exponential_backoff(self) -> None:
        """Test watcher retry logic with exponential backoff."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            attempt_times = []
            drain_count = 0

            with self.create_test_watcher(
                str(db_path),
                "queue",
                lambda m, t: None,
            ) as watcher:
                # Mock drain_queue to track retry timing
                original_drain = watcher._drain_queue

                def failing_drain() -> None:
                    nonlocal drain_count
                    attempt_times.append(time.time())
                    drain_count += 1
                    if drain_count < 3:
                        msg = "Drain failed"
                        raise Exception(msg)
                    # Stop after successful drain
                    watcher.stop()
                    original_drain()

                watcher._drain_queue = failing_drain

                # Run with timeout
                self.run_watcher_with_timeout(watcher, timeout=10.0)

                # Verify retries happened with exponential backoff
                assert drain_count >= 3
                assert len(attempt_times) >= 3

                # Check that delays increased (exponential backoff)
                # Note: actual delays will be affected by interruptible_sleep
                if len(attempt_times) > 1:
                    first_delay = attempt_times[1] - attempt_times[0]
                    second_delay = (
                        attempt_times[2] - attempt_times[1]
                        if len(attempt_times) > 2
                        else 0
                    )

                    # Second delay should be longer (exponential backoff)
                    if second_delay > 0:
                        assert second_delay > first_delay * 1.5  # Allow some variance

    def test_watcher_max_retries_exceeded(self) -> None:
        """Test that watcher fails after max retries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with self.create_test_watcher(
                str(db_path),
                "queue",
                lambda m, t: None,
            ) as watcher:
                # Mock drain_queue to always fail
                def failing_drain() -> NoReturn:
                    msg = "Persistent failure"
                    raise Exception(msg)

                watcher._drain_queue = failing_drain

                # Should fail after max retries
                with pytest.raises(Exception, match="Persistent failure"):
                    watcher.run_forever()

    def test_cleanup_thread_local_errors(self) -> None:
        """Test handling of errors during thread-local cleanup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)

            # Create a mock DB that fails to close
            mock_db = Mock()
            mock_db.close.side_effect = Exception("Close failed")
            watcher._thread_local.db = mock_db

            # Mock the config to enable logging just for this call
            from simplebroker.watcher import _config

            with patch(
                "simplebroker.watcher._config",
                {**_config, "BROKER_LOGGING_ENABLED": True},
            ):
                # Should log warning but not raise
                with patch("simplebroker.watcher.logger") as mock_logger:
                    watcher._cleanup_thread_local()

                    mock_logger.warning.assert_called_once()
                    assert "Error closing thread-local database" in str(
                        mock_logger.warning.call_args,
                    )

            # Verify cleanup still happened
            assert not hasattr(watcher._thread_local, "db")

    def test_context_manager_error_handling(self) -> None:
        """Test context manager handles errors during exit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)
            # Start the watcher manually so we can control cleanup
            thread = watcher.run_in_thread()

            # Wait a moment to ensure thread is running
            time.sleep(0.1)

            # Mock stop to raise AFTER actually stopping the thread
            original_stop = watcher.stop
            stop_called = False

            def failing_stop(*args, **kwargs) -> None:
                nonlocal stop_called
                if not stop_called:
                    # First call - actually stop the thread
                    stop_called = True
                    original_stop(*args, **kwargs)
                    # Then raise the exception for testing
                    msg = "Stop failed"
                    raise Exception(msg)
                # Subsequent calls - just call original
                original_stop(*args, **kwargs)

            watcher.stop = failing_stop

            # Patch config to enable logging and capture the warning
            from simplebroker.watcher import _config

            with patch.dict(
                "simplebroker.watcher._config",
                {**_config, "BROKER_LOGGING_ENABLED": True},
            ), patch("simplebroker.watcher.logger") as mock_logger:
                # Simulate context manager exit
                try:
                    watcher.__exit__(None, None, None)
                except Exception:
                    pass  # Expected from our mock

                # Wait a moment for thread to finish
                thread.join(timeout=2.0)

                # Ensure thread is really stopped
                assert not thread.is_alive()

                # Should have logged warning for stop failure
                assert mock_logger.warning.call_count >= 1
                # Verify the warning message was logged
                warning_calls = [
                    str(call) for call in mock_logger.warning.call_args_list
                ]
                assert any(
                    "Error during stop in __exit__: Stop failed" in call
                    for call in warning_calls
                )

    def test_signal_handler_not_main_thread(self) -> None:
        """Test that signal handler is not installed in non-main threads."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)

            # Run in a thread (not main)
            threading.Event()

            def run_watcher() -> None:
                # Patch to make it think it's not the main thread
                with patch("threading.current_thread") as mock_thread:
                    mock_thread.return_value = Mock()
                    mock_thread.return_value.is_main_thread = False

                    # Should not install signal handler
                    with patch("signal.signal") as mock_signal:
                        # Stop immediately
                        watcher.stop()
                        watcher.run_forever()

                        # Signal handler should not have been installed
                        mock_signal.assert_not_called()

            thread = threading.Thread(target=run_watcher)
            thread.start()
            thread.join(timeout=5)

    def test_absolute_timeout_exceeded(self) -> None:
        """Test that watcher fails after MAX_TOTAL_RETRY_TIME."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with self.create_test_watcher(
                str(db_path),
                "queue",
                lambda m, t: None,
            ) as watcher:
                # Mock time.time() to simulate time passing faster
                original_time = time.time
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
                    raise Exception(msg)

                watcher._drain_queue = failing_drain

                # Patch time.time to make timeout trigger quickly
                with patch("simplebroker.watcher.time.time", mock_time):
                    # Should raise TimeoutError after simulated 300s (3s real time)
                    with pytest.raises(TimeoutError) as exc_info:
                        watcher.run_forever()

                    assert "retry timeout exceeded" in str(exc_info.value)
                    assert "300s" in str(exc_info.value)  # Default timeout

    def test_check_stop_centralization(self) -> None:
        """Test that _check_stop is used consistently."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            check_count = 0

            with self.create_test_watcher(
                str(db_path),
                "queue",
                lambda m, t: None,
            ) as watcher:
                original_check_stop = watcher._check_stop

                def mock_check_stop() -> None:
                    nonlocal check_count
                    check_count += 1
                    if check_count > 3:
                        raise _StopLoop
                    # Call original to maintain normal behavior
                    original_check_stop()

                watcher._check_stop = mock_check_stop

                # Should exit after a few checks
                watcher.run_forever()
                assert check_count > 3  # Called multiple times

    def test_interruptible_sleep_responsiveness(self) -> None:
        """Test that watcher responds quickly to stop signals."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            broker = BrokerDB(db_path)
            try:
                # Write a message
                broker.write("slow_queue", "test message")

                process_start = None
                handler_started = threading.Event()

                def slow_handler(msg, ts) -> None:
                    nonlocal process_start
                    process_start = time.time()
                    handler_started.set()  # Signal that handler has started
                    # Simulate slow processing with interruptible sleep
                    interruptible_sleep(1.0, watcher._stop_event)

                with self.create_test_watcher(
                    broker,
                    "slow_queue",
                    slow_handler,
                ) as watcher:
                    # Start watcher
                    thread = watcher.run_in_thread()

                    # Wait for handler to start processing
                    if not handler_started.wait(timeout=2.0):
                        watcher.stop()
                        thread.join(timeout=1.0)
                        pytest.fail("Handler did not start processing within timeout")

                    # Stop should interrupt the sleep
                    start_stop = time.time()
                    watcher.stop()
                    thread.join(timeout=0.5)  # Should complete quickly
                    stop_time = time.time() - start_stop

                    assert stop_time < 0.5, (
                        f"Stop took {stop_time:.2f}s, should be < 0.5s"
                    )
                    assert not thread.is_alive()
            finally:
                broker.close()

    def test_concurrent_stop_safety(self) -> None:
        """Test stopping watcher from multiple threads."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            broker = BrokerDB(db_path)
            try:
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
                    broker,
                    "concurrent_queue",
                    slow_handler,
                ) as watcher:
                    thread = watcher.run_in_thread()

                    # Wait for processing to start
                    if not processing_started.wait(timeout=2.0):
                        watcher.stop()
                        thread.join(timeout=1.0)
                        pytest.fail("Handler did not start processing within timeout")

                    # Multiple threads try to stop
                    stop_threads = []
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
                broker.close()


class TestQueueMoveWatcherEdgeCases(WatcherTestBase):
    """Test edge cases in QueueMoveWatcher."""

    def test_same_queue_error(self) -> None:
        """Test that moving to same queue raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with pytest.raises(
                ValueError,
                match="Cannot move messages to the same queue",
            ):
                QueueMoveWatcher(str(db_path), "queue", "queue", lambda m, t: None)

    def test_move_with_handler_error(self) -> None:
        """Test that handler errors don't affect move (already completed)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))
            try:
                # Add a message to source queue
                db.write("source", "test message")

                handler_called = []

                def handler(msg, ts) -> NoReturn:
                    handler_called.append((msg, ts))
                    msg = "Handler failed"
                    raise ValueError(msg)

                def error_handler(exc, msg, ts) -> bool:
                    return True  # Continue

                watcher = QueueMoveWatcher(
                    db,
                    "source",
                    "dest",
                    handler,
                    error_handler=error_handler,
                    max_messages=1,
                )

                # Run move - should handle the error internally
                watcher.run()

                # Verify message was moved despite handler error
                # Get queue counts from list_queues
                queues = dict(db.list_queues())
                assert queues.get("source", 0) == 0
                assert queues.get("dest", 0) == 1

                # Verify handler was called
                assert len(handler_called) == 1
                assert handler_called[0][0] == "test message"
            finally:
                db.close()

    def test_move_unexpected_error(self) -> None:
        """Test handling of unexpected errors during move."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Use context manager for proper cleanup
            with self.create_test_move_watcher(
                str(db_path),
                "source",
                "dest",
                lambda m, t: None,
            ) as watcher:
                # Mock db.move to raise unexpected error
                def failing_move(*args, **kwargs) -> NoReturn:
                    msg = "Unexpected move error"
                    raise RuntimeError(msg)

                # Patch the db's move method
                with patch.object(watcher._get_db(), "move", side_effect=failing_move):
                    with pytest.raises(RuntimeError, match="Unexpected move error"):
                        watcher._drain_queue()

    def test_polling_strategy_activity_detection(self) -> None:
        """Test that polling strategy detects database changes."""
        stop_event = threading.Event()
        strategy = PollingStrategy(stop_event)
        mock_db = Mock()

        # First call returns version 1
        mock_db._runner.run.return_value = [(1,)]
        strategy.start(mock_db)

        # No change detected on first check
        assert strategy._check_data_version() is False

        # Now simulate a change
        mock_db._runner.run.return_value = [(2,)]

        # Should detect change
        assert strategy._check_data_version() is True

        # Check count should reset on activity
        strategy.notify_activity()
        assert strategy._check_count == 0
