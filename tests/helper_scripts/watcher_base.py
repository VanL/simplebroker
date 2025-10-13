"""Base test utilities for watcher tests with timeout safety."""

import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueMoveWatcher, QueueWatcher

from .cleanup import register_watcher


class WatcherTestBase:
    """Base class for watcher tests with timeout safety."""

    DEFAULT_TIMEOUT = 5.0  # 5 seconds default timeout for tests
    STOP_TIMEOUT = 1.0  # 1 second to wait for stop

    def run_watcher_with_timeout(
        self,
        watcher: QueueWatcher | QueueMoveWatcher,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> threading.Thread:
        """Run watcher with guaranteed timeout.

        Args:
            watcher: The watcher to run
            timeout: Maximum time to wait for watcher to complete

        Returns:
            The thread that ran the watcher

        Raises:
            pytest.fail: If watcher doesn't stop within timeout
        """
        thread = watcher.run_in_thread()
        thread.join(timeout=timeout)

        if thread.is_alive():
            # Try graceful stop first
            watcher.stop()
            thread.join(timeout=self.STOP_TIMEOUT)

            if thread.is_alive():
                # Force thread termination (last resort)
                pytest.fail(
                    f"Watcher thread hung after {timeout}s timeout + "
                    f"{self.STOP_TIMEOUT}s stop timeout"
                )

        return thread

    @contextmanager
    def create_test_watcher(
        self,
        broker: BrokerDB | str | Path,
        queue: str,
        handler: Callable[[str, int], None],
        **kwargs,
    ):
        """Context manager for safe watcher testing.

        Ensures watcher is always stopped, even if test fails.

        Args:
            broker: Database path or BrokerDB instance
            queue: Queue name to watch
            handler: Message handler function
            **kwargs: Additional arguments for QueueWatcher

        Yields:
            QueueWatcher instance
        """
        # Adapt to new API: queue_name, handler, then db= keyword (always)
        watcher = QueueWatcher(queue, handler, db=broker, **kwargs)
        register_watcher(watcher)  # Register for automatic cleanup
        try:
            yield watcher
        finally:
            try:
                # Check if a thread was started
                if hasattr(watcher, "_thread") and watcher._thread:
                    # Stop and join the thread
                    watcher.stop(join=True, timeout=2.0)  # Explicitly join with timeout
                else:
                    # No thread started, just stop
                    watcher.stop()
            except Exception:
                pass  # Ignore errors during cleanup

    @contextmanager
    def create_test_move_watcher(
        self,
        broker: BrokerDB | str | Path,
        source_queue: str,
        dest_queue: str,
        handler: Callable[[str, int], None],
        **kwargs,
    ):
        """Context manager for safe move watcher testing.

        Args:
            broker: Database path or BrokerDB instance
            source_queue: Source queue name
            dest_queue: Destination queue name
            handler: Message handler function
            **kwargs: Additional arguments for QueueMoveWatcher

        Yields:
            QueueMoveWatcher instance
        """
        # Adapt to new API: source_queue, dest_queue, handler, then db= keyword (always)
        watcher = QueueMoveWatcher(
            source_queue, dest_queue, handler, db=broker, **kwargs
        )
        register_watcher(watcher)  # Register for automatic cleanup
        try:
            yield watcher
        finally:
            try:
                # Check if a thread was started
                if hasattr(watcher, "_thread") and watcher._thread:
                    # Stop and join the thread
                    watcher.stop(join=True, timeout=2.0)  # Explicitly join with timeout
                else:
                    # No thread started, just stop
                    watcher.stop()
            except Exception:
                pass

    def run_watcher_until_messages(
        self,
        watcher: QueueWatcher | QueueMoveWatcher,
        collector,  # MessageCollector instance
        expected_count: int,
        timeout: float = 5.0,
    ) -> threading.Thread:
        """Run watcher until expected messages are collected.

        Args:
            watcher: The watcher to run
            collector: MessageCollector instance tracking messages
            expected_count: Number of messages to wait for
            timeout: Maximum time to wait

        Returns:
            The watcher thread

        Raises:
            pytest.fail: If timeout is reached before collecting expected messages
        """
        thread = watcher.run_in_thread()

        # Wait for messages to be collected
        if not collector.wait_for_messages(expected_count, timeout=timeout):
            watcher.stop()
            thread.join(timeout=self.STOP_TIMEOUT)
            pytest.fail(
                f"Timeout waiting for {expected_count} messages. "
                f"Got {len(collector.get_messages())} messages"
            )

        return thread

    def assert_watcher_stops_quickly(
        self, watcher: QueueWatcher | QueueMoveWatcher, max_stop_time: float = 0.5
    ):
        """Assert that watcher stops within expected time.

        Args:
            watcher: The watcher to stop
            max_stop_time: Maximum acceptable stop time
        """
        start_time = time.monotonic()()
        watcher.stop()

        # If watcher was running in thread, wait for it
        # (In real test, we'd track the thread)
        stop_time = time.time() - start_time

        assert stop_time < max_stop_time, (
            f"Watcher took {stop_time:.2f}s to stop, expected < {max_stop_time}s"
        )
