"""Tests for improving queue.py coverage (_ensure_core and cleanup)."""

import gc
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch

from simplebroker import Queue


def ensure_windows_cleanup() -> None:
    """Ensure proper cleanup on Windows to avoid permission errors."""
    gc.collect()
    if sys.platform == "win32":
        time.sleep(0.1)
        gc.collect()


def test_ensure_core_lazy_initialization() -> None:
    """Test that DBConnection lazily initializes the core."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        try:
            # Create a persistent queue
            queue = Queue("test", db_path=db_path, persistent=True)

            # Initially, connection should exist but core might be lazy
            assert queue.conn is not None

            # Get core to ensure initialization
            assert queue.conn is not None
            core = queue.conn.get_core()
            assert core is not None

            # Second call should return same core
            core2 = queue.conn.get_core()
            assert core is core2

            # Clean up
            queue.close()
        finally:
            # Ensure Windows cleanup
            ensure_windows_cleanup()


def test_cleanup_finalizer_function() -> None:
    """Test the finalizer releases the queue connection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Create a persistent queue to trigger _install_finalizer
        queue = Queue("test", db_path=db_path, persistent=True)

        try:
            # Verify finalizer was installed
            assert hasattr(queue, "_finalizer")

            # Initialize the connection
            assert queue.conn is not None
            core = queue.conn.get_core()
            assert core is not None

            # Mock the close method on the connection
            original_close = queue.conn.close
            # Test-only method replacement verifies the weakref finalizer calls close.
            queue.conn.close = Mock(side_effect=original_close)  # type: ignore[method-assign]

            # Call the finalizer function directly (simulating object destruction)
            queue._finalizer()

            # Verify release was called
            assert isinstance(queue.conn.close, Mock)
            queue.conn.close.assert_called_once()
        finally:
            # Force garbage collection for Windows
            ensure_windows_cleanup()


def test_cleanup_finalizer_with_exception() -> None:
    """Test that finalizer release handles exceptions gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Create a persistent queue with logging enabled config
        queue = Queue(
            "test",
            db_path=db_path,
            persistent=True,
            config={"BROKER_LOGGING_ENABLED": True},
        )

        # Mock the connection close to raise an exception
        assert queue.conn is not None
        # Test-only method replacement drives the finalizer's error path.
        queue.conn.close = Mock(side_effect=Exception("Test exception"))  # type: ignore[method-assign]

        # Patch the logger to verify warning is logged
        with patch("simplebroker.sbqueue.logger") as mock_logger:
            # Call the finalizer function directly
            queue._finalizer()

            # Verify the warning was logged
            mock_logger.warning.assert_called_once()
            assert "Error during Queue finalizer cleanup" in str(
                mock_logger.warning.call_args
            )
            assert "Test exception" in str(mock_logger.warning.call_args)

        ensure_windows_cleanup()


def test_queue_persistent_with_custom_runner_no_finalizer() -> None:
    """Test that custom runner still installs finalizer."""
    from simplebroker._runner import SQLiteRunner

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        try:
            # Create a custom runner
            custom_runner = SQLiteRunner(db_path)

            # Create a queue with custom runner
            queue = Queue(
                "test", db_path=db_path, persistent=True, runner=custom_runner
            )

            # Verify finalizer was installed (now always installed for safety)
            assert hasattr(queue, "_finalizer")

            # Clean up
            queue.close()
            custom_runner.close()
        finally:
            # Ensure Windows cleanup
            ensure_windows_cleanup()
