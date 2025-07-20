"""Test edge cases in watcher.py to increase coverage."""

import os
import tempfile
import threading
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import (
    PollingStrategy,
    QueueMoveWatcher,
    QueueWatcher,
    _StopLoop,
)


class TestWatcherEdgeCases:
    """Test edge cases in QueueWatcher."""

    def test_invalid_handler_type(self):
        """Test that non-callable handler raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with pytest.raises(TypeError, match="handler must be callable"):
                QueueWatcher(str(db_path), "queue", "not_callable")

    def test_invalid_error_handler_type(self):
        """Test that non-callable error_handler raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            def handler(msg, ts):
                pass

            with pytest.raises(TypeError, match="error_handler must be callable"):
                QueueWatcher(
                    str(db_path), "queue", handler, error_handler="not_callable"
                )

    def test_environment_variable_parsing(self):
        """Test parsing of environment variables with invalid values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Set invalid environment variables
            os.environ["SIMPLEBROKER_INITIAL_CHECKS"] = "invalid"
            os.environ["SIMPLEBROKER_MAX_INTERVAL"] = "not_a_float"
            os.environ["SIMPLEBROKER_BURST_SLEEP"] = "bad_value"

            try:
                # Should log warnings but not crash
                with patch("simplebroker.watcher.logger") as mock_logger:
                    watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)

                    # Check warnings were logged
                    assert mock_logger.warning.call_count >= 3

                    # Verify defaults were used
                    assert watcher._strategy._initial_checks == 100
                    assert watcher._strategy._max_interval == 0.1
                    assert watcher._strategy._burst_sleep == 0.0002
            finally:
                # Clean up environment
                os.environ.pop("SIMPLEBROKER_INITIAL_CHECKS", None)
                os.environ.pop("SIMPLEBROKER_MAX_INTERVAL", None)
                os.environ.pop("SIMPLEBROKER_BURST_SLEEP", None)

    def test_message_size_limit_exceeded(self):
        """Test handling of messages exceeding 10MB limit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))

            # Create a message larger than 10MB
            large_message = "x" * (11 * 1024 * 1024)  # 11MB

            handled = []
            errors = []

            def handler(msg, ts):
                handled.append((msg, ts))

            def error_handler(exc, msg, ts):
                errors.append((exc, msg, ts))
                return True  # Continue processing

            watcher = QueueWatcher(db, "queue", handler, error_handler=error_handler)

            # Directly test dispatch with oversized message
            watcher._dispatch(large_message, 12345)

            # Verify handler was not called
            assert len(handled) == 0

            # Verify error handler was called
            assert len(errors) == 1
            assert isinstance(errors[0][0], ValueError)
            assert "exceeds 10MB limit" in str(errors[0][0])
            assert errors[0][1].endswith("...")  # Truncated message

    def test_error_handler_returns_false(self):
        """Test that error handler returning False stops the watcher."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))

            def handler(msg, ts):
                raise ValueError("Handler error")

            def error_handler(exc, msg, ts):
                return False  # Request stop

            watcher = QueueWatcher(db, "queue", handler, error_handler=error_handler)

            # Test dispatch
            with pytest.raises(_StopLoop):
                watcher._dispatch("test", 12345)

            # Verify stop event was set
            assert watcher._stop_event.is_set()

    def test_error_handler_itself_fails(self):
        """Test handling when error handler itself raises an exception."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))

            def handler(msg, ts):
                raise ValueError("Handler error")

            def error_handler(exc, msg, ts):
                raise RuntimeError("Error handler failed")

            watcher = QueueWatcher(db, "queue", handler, error_handler=error_handler)

            # Should log but not crash
            with patch("simplebroker.watcher.logger") as mock_logger:
                watcher._dispatch("test", 12345)

                # Verify both errors were logged
                mock_logger.error.assert_called()
                error_call_args = str(mock_logger.error.call_args)
                assert "Error handler failed" in error_call_args
                assert "Handler error" in error_call_args

    def test_polling_strategy_pragma_failures(self):
        """Test handling of repeated PRAGMA data_version failures."""
        strategy = PollingStrategy()
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

    def test_watcher_retry_with_exponential_backoff(self):
        """Test watcher retry logic with exponential backoff."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            call_count = 0

            def handler(msg, ts):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise Exception("Simulated failure")

            watcher = QueueWatcher(str(db_path), "queue", handler)

            # Mock drain_queue to raise exceptions
            original_drain = watcher._drain_queue
            drain_count = 0

            def failing_drain():
                nonlocal drain_count
                drain_count += 1
                if drain_count < 3:
                    raise Exception("Drain failed")
                # Stop after successful drain
                watcher.stop()
                original_drain()

            watcher._drain_queue = failing_drain

            # Run with mocked sleep to avoid actual delays
            with patch("time.sleep") as mock_sleep:
                watcher.run_forever()

                # Verify exponential backoff was used
                assert mock_sleep.call_count >= 2
                sleep_times = [call[0][0] for call in mock_sleep.call_args_list]
                assert sleep_times[0] == 2  # 2^1
                assert sleep_times[1] == 4  # 2^2

    def test_watcher_max_retries_exceeded(self):
        """Test that watcher fails after max retries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)

            # Mock drain_queue to always fail
            def failing_drain():
                raise Exception("Persistent failure")

            watcher._drain_queue = failing_drain

            with patch("time.sleep"):  # Speed up test
                with pytest.raises(Exception, match="Persistent failure"):
                    watcher.run_forever()

    def test_cleanup_thread_local_errors(self):
        """Test handling of errors during thread-local cleanup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)

            # Create a mock DB that fails to close
            mock_db = Mock()
            mock_db.close.side_effect = Exception("Close failed")
            watcher._thread_local.db = mock_db

            # Should log warning but not raise
            with patch("simplebroker.watcher.logger") as mock_logger:
                watcher._cleanup_thread_local()

                mock_logger.warning.assert_called_once()
                assert "Error closing thread-local database" in str(
                    mock_logger.warning.call_args
                )

            # Verify cleanup still happened
            assert not hasattr(watcher._thread_local, "db")

    def test_context_manager_error_handling(self):
        """Test context manager handles errors during exit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with patch("simplebroker.watcher.logger") as mock_logger:
                with QueueWatcher(str(db_path), "queue", lambda m, t: None) as watcher:
                    # Mock stop to raise
                    watcher.stop = Mock(side_effect=Exception("Stop failed"))
                    # Mock cleanup to raise
                    watcher._cleanup_thread_local = Mock(
                        side_effect=Exception("Cleanup failed")
                    )

                # Should have logged both warnings
                assert mock_logger.warning.call_count >= 2

    def test_signal_handler_not_main_thread(self):
        """Test that signal handler is not installed in non-main threads."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            watcher = QueueWatcher(str(db_path), "queue", lambda m, t: None)

            # Run in a thread (not main)
            threading.Event()

            def run_watcher():
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


class TestQueueMoveWatcherEdgeCases:
    """Test edge cases in QueueMoveWatcher."""

    def test_same_queue_error(self):
        """Test that moving to same queue raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with pytest.raises(
                ValueError, match="Cannot move messages to the same queue"
            ):
                QueueMoveWatcher(str(db_path), "queue", "queue", lambda m, t: None)

    def test_move_with_handler_error(self):
        """Test that handler errors don't affect move (already completed)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))

            # Add a message to source queue
            db.write("source", "test message")

            handler_called = []

            def handler(msg, ts):
                handler_called.append((msg, ts))
                raise ValueError("Handler failed")

            def error_handler(exc, msg, ts):
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

    def test_move_unexpected_error(self):
        """Test handling of unexpected errors during move."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = BrokerDB(str(Path(tmpdir) / "test.db"))

            watcher = QueueMoveWatcher(db, "source", "dest", lambda m, t: None)

            # Mock db.move to raise unexpected error

            def failing_move(*args, **kwargs):
                raise RuntimeError("Unexpected move error")

            # Patch the db's move method
            with patch.object(watcher._get_db(), "move", side_effect=failing_move):
                with pytest.raises(RuntimeError, match="Unexpected move error"):
                    watcher._drain_queue()

    def test_polling_strategy_activity_detection(self):
        """Test that polling strategy detects database changes."""
        strategy = PollingStrategy()
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
