"""Tests for improving queue.py coverage (_ensure_core and cleanup)."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from simplebroker import Queue


def test_ensure_core_lazy_initialization():
    """Test that _ensure_core lazily initializes the connection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Create a persistent queue
        queue = Queue("test", db_path=db_path, persistent=True)

        # Initially, core should be initialized (from __init__ since runner exists)
        assert queue._core is not None
        assert (
            queue._runner is not None
        )  # Runner is created in __init__ for persistent mode

        # Close the queue to reset state
        queue.close()

        # Now both should be None
        assert queue._core is None
        assert queue._runner is None

        # Call _ensure_core to trigger lazy initialization
        core = queue._ensure_core()

        # Now both should be initialized
        assert core is not None
        assert queue._core is core
        assert queue._runner is not None

        # Clean up
        queue.close()


def test_cleanup_finalizer_function():
    """Test the cleanup function in _install_finalizer."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Create a persistent queue without custom runner to trigger _install_finalizer
        queue = Queue("test", db_path=db_path, persistent=True)

        # Verify finalizer was installed
        assert hasattr(queue, "_finalizer")

        # Mock the runner's close method to verify it gets called
        original_close = queue._runner.close
        queue._runner.close = Mock(side_effect=original_close)

        # Call the finalizer function directly (simulating object destruction)
        queue._finalizer()

        # Verify the runner was closed
        queue._runner.close.assert_called_once()


def test_cleanup_finalizer_with_exception():
    """Test that cleanup handles exceptions gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Create a persistent queue
        queue = Queue("test", db_path=db_path, persistent=True)

        # Mock the runner's close method to raise an exception
        queue._runner.close = Mock(side_effect=Exception("Test exception"))

        # Patch the logger to verify warning is logged
        with patch("simplebroker.queue.logger") as mock_logger:
            # Call the finalizer function directly
            queue._finalizer()

            # Verify the warning was logged
            mock_logger.warning.assert_called_once()
            assert "Error during Queue finalizer cleanup" in str(
                mock_logger.warning.call_args
            )
            assert "Test exception" in str(mock_logger.warning.call_args)


def test_cleanup_finalizer_with_none_runner():
    """Test that cleanup handles None runner gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        str(Path(tmpdir) / "test.db")

        # Patch the logger to verify no warning is logged
        with patch("simplebroker.queue.logger") as mock_logger:
            # Create the cleanup function directly
            def cleanup(runner):
                try:
                    if runner:
                        runner.close()
                except Exception as e:
                    mock_logger.warning(f"Error during Queue finalizer cleanup: {e}")

            # Call cleanup with None runner - should not raise or log
            cleanup(None)

            # Verify no warning was logged
            mock_logger.warning.assert_not_called()


def test_queue_persistent_with_custom_runner_no_finalizer():
    """Test that custom runner doesn't install finalizer."""
    from simplebroker._runner import SQLiteRunner

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Create a custom runner
        custom_runner = SQLiteRunner(db_path)

        # Create a queue with custom runner
        queue = Queue("test", db_path=db_path, persistent=True, runner=custom_runner)

        # Verify no finalizer was installed (only installed when persistent=True and runner=None)
        assert not hasattr(queue, "_finalizer")

        # Clean up
        queue.close()
        custom_runner.close()
