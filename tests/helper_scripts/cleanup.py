"""Cleanup utilities for tests to prevent hanging."""

import gc
import logging
import threading
import time
import weakref
from collections.abc import Iterator
from typing import Any

import pytest

logger = logging.getLogger(__name__)


def _watcher_needs_cleanup(watcher: Any) -> bool:
    is_running = getattr(watcher, "is_running", None)
    if callable(is_running) and is_running():
        return True
    finalizer = getattr(watcher, "_finalizer", None)
    return finalizer is None or finalizer.alive


class WatcherTracker:
    """Track all watcher instances created during tests."""

    def __init__(self) -> None:
        self._watchers: set[weakref.ReferenceType[Any]] = set()
        self._lock = threading.Lock()

    def register(self, watcher: Any) -> None:
        """Register a watcher instance."""
        with self._lock:
            self._watchers.add(weakref.ref(watcher))

    def stop_all(self, timeout: float = 5.0) -> None:
        """Stop all registered watchers."""
        with self._lock:
            live_watchers = [
                watcher
                for watcher_ref in self._watchers
                if (watcher := watcher_ref()) is not None
                and _watcher_needs_cleanup(watcher)
            ]

            # Stop all live watchers
            for watcher in live_watchers:
                try:
                    if hasattr(watcher, "stop"):
                        logger.debug(f"Stopping watcher {watcher}")
                        watcher.stop()
                except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
                    logger.warning(f"Error stopping watcher: {e}")

            # Wait for threads to stop
            start_time = time.monotonic()
            for watcher in live_watchers:
                if hasattr(watcher, "_strategy") and hasattr(
                    watcher._strategy, "_stop_event"
                ):
                    remaining = timeout - (time.monotonic() - start_time)
                    if remaining > 0:
                        watcher._strategy._stop_event.wait(timeout=remaining)

            # Clear all references
            self._watchers.clear()


# Global tracker instance
_watcher_tracker = WatcherTracker()


@pytest.fixture(autouse=True)
def cleanup_watchers() -> Iterator[None]:
    """Automatically clean up watchers after each test."""
    yield

    # Stop all watchers
    _watcher_tracker.stop_all()

    # Force garbage collection
    gc.collect()

    # On Windows, add a small delay to ensure file handles are released
    import sys

    if sys.platform == "win32":
        time.sleep(0.1)
        gc.collect()  # Second GC pass on Windows

    # Check for remaining threads
    active_threads = threading.enumerate()
    watcher_threads = [t for t in active_threads if "watcher" in t.name.lower()]

    if watcher_threads:
        logger.warning(
            f"Found {len(watcher_threads)} watcher threads still active after test"
        )
        for thread in watcher_threads:
            logger.warning(f"  - {thread.name} (daemon={thread.daemon})")


def register_watcher(watcher: Any) -> None:
    """Register a watcher for automatic cleanup."""
    _watcher_tracker.register(watcher)


@pytest.fixture(autouse=True, scope="session")
def cleanup_at_exit() -> Iterator[None]:
    """Final cleanup at test session end."""
    yield

    # Final cleanup attempt
    _watcher_tracker.stop_all(timeout=10.0)

    # Log any remaining threads
    active_threads = threading.enumerate()
    if len(active_threads) > 1:  # Main thread is always there
        logger.info(f"Active threads at session end: {len(active_threads)}")
        for thread in active_threads:
            if thread.name != "MainThread":
                logger.info(f"  - {thread.name} (daemon={thread.daemon})")
