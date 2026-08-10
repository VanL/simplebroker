"""Test that watchers are properly cleaned up."""

import threading

import pytest

from simplebroker.watcher import QueueWatcher

# Import cleanup helper
from .helper_scripts.cleanup import WatcherTracker, register_watcher
from .helper_scripts.timing import scale_timeout_for_ci

pytestmark = [pytest.mark.shared]


class TestWatcherCleanup:
    """Test watcher cleanup functionality."""

    def test_tracker_stop_all_stops_registered_watchers(self, broker_target):
        """The cleanup tracker stops every watcher it owns before returning."""
        tracker = WatcherTracker()
        watchers = [
            QueueWatcher(f"queue_{index}", lambda _m, _t: None, db=broker_target)
            for index in range(3)
        ]
        threads = []

        try:
            for watcher in watchers:
                tracker.register(watcher)
                threads.append(watcher.run_in_thread())

            assert all(thread.is_alive() for thread in threads)

            tracker.stop_all(timeout=scale_timeout_for_ci(5.0))
            for thread in threads:
                thread.join(timeout=scale_timeout_for_ci(5.0))

            assert all(not thread.is_alive() for thread in threads)
            assert all(not watcher.is_running() for watcher in watchers)
        finally:
            for watcher in watchers:
                watcher.stop()

    def test_watcher_stops_quickly(self, broker, broker_target):
        """Test that watchers stop within reasonable time."""
        handler_started = threading.Event()
        handler_release = threading.Event()

        broker.write("test_queue", "test message")

        def slow_handler(_msg, _ts):
            handler_started.set()
            assert handler_release.wait(timeout=scale_timeout_for_ci(5.0))

        watcher = QueueWatcher("test_queue", slow_handler, db=broker_target)
        register_watcher(watcher)  # Register for automatic cleanup
        thread = watcher.run_in_thread()

        try:
            assert handler_started.wait(timeout=scale_timeout_for_ci(5.0))
            watcher.stop(join=False)
            assert thread.is_alive()
            handler_release.set()
            thread.join(timeout=scale_timeout_for_ci(5.0))
            assert not thread.is_alive()
            assert not watcher.is_running()
        finally:
            handler_release.set()
            if thread.is_alive():
                watcher.stop()
                thread.join(timeout=scale_timeout_for_ci(5.0))
