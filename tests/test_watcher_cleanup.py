"""Test that watchers are properly cleaned up."""

import threading
import time

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher

# Import cleanup helper
from .helper_scripts.cleanup import register_watcher


class TestWatcherCleanup:
    """Test watcher cleanup functionality."""

    def test_watcher_auto_cleanup(self, temp_db):
        """Test that watchers are automatically cleaned up."""
        # Get initial thread count
        initial_threads = len(threading.enumerate())

        # Create and start a watcher
        db = BrokerDB(temp_db)
        watcher = QueueWatcher("test_queue", lambda m, t: None, db=db)
        register_watcher(watcher)  # Register for automatic cleanup
        thread = watcher.run_in_thread()

        # Verify thread is running
        assert thread.is_alive()

        # Thread count should have increased
        assert len(threading.enumerate()) > initial_threads

        # Don't stop explicitly - let cleanup handle it
        # The cleanup fixture should stop this watcher

    def test_multiple_watchers_cleanup(self, temp_db):
        """Test multiple watchers are cleaned up."""
        db = BrokerDB(temp_db)

        # Create multiple watchers
        watchers = []
        threads = []

        for i in range(3):
            watcher = QueueWatcher(f"queue_{i}", lambda m, t: None, db=db)
            register_watcher(watcher)  # Register for automatic cleanup
            thread = watcher.run_in_thread()
            watchers.append(watcher)
            threads.append(thread)

        # All threads should be running
        for thread in threads:
            assert thread.is_alive()

        # Don't stop them - cleanup should handle it

    def test_watcher_stops_quickly(self, temp_db):
        """Test that watchers stop within reasonable time."""
        db = BrokerDB(temp_db)

        # Add a message
        db.write("test_queue", "test message")

        # Create watcher with slow handler
        def slow_handler(msg, ts):
            time.sleep(0.5)  # Simulate slow processing

        watcher = QueueWatcher("test_queue", slow_handler, db=db)
        register_watcher(watcher)  # Register for automatic cleanup
        thread = watcher.run_in_thread()

        try:
            # Let it start processing
            time.sleep(0.1)

            # Stop should be quick even with slow handler
            start_time = time.monotonic()
            watcher.stop()
            thread.join(timeout=2.0)
            stop_time = time.monotonic() - start_time

            assert not thread.is_alive()
            assert stop_time < 2.0
        finally:
            # Ensure cleanup even if test fails
            if thread.is_alive():
                watcher.stop()
                thread.join(timeout=1.0)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    return tmp_path / "test.db"
