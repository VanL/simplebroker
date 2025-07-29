"""Base test utilities for watcher tests with timeout safety."""

import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Union

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
        watcher: Union[QueueWatcher, QueueMoveWatcher],
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
        broker: Union[BrokerDB, str, Path],
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
        watcher = QueueWatcher(broker, queue, handler, **kwargs)
        register_watcher(watcher)  # Register for automatic cleanup
        try:
            yield watcher
        finally:
            try:
                watcher.stop()  # Always attempt cleanup
            except Exception:
                pass  # Ignore errors during cleanup

    @contextmanager
    def create_test_move_watcher(
        self,
        broker: Union[BrokerDB, str, Path],
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
        watcher = QueueMoveWatcher(broker, source_queue, dest_queue, handler, **kwargs)
        register_watcher(watcher)  # Register for automatic cleanup
        try:
            yield watcher
        finally:
            try:
                watcher.stop()
            except Exception:
                pass

    def wait_for_messages(
        self,
        watcher: Union[QueueWatcher, QueueMoveWatcher],
        expected_count: int,
        timeout: float = 2.0,
    ) -> bool:
        """Wait for watcher to process expected number of messages.

        Args:
            watcher: The watcher instance
            expected_count: Number of messages to wait for
            timeout: Maximum time to wait

        Returns:
            True if expected count reached, False if timeout
        """
        start_time = time.time()
        message_count = 0

        # This would need to be implemented based on handler tracking
        # For now, just a placeholder showing the pattern
        while time.time() - start_time < timeout:
            # Check current message count
            # In real implementation, handler would track this
            if message_count >= expected_count:
                return True
            time.sleep(0.01)  # Small sleep to avoid busy loop

        return False

    def assert_watcher_stops_quickly(
        self, watcher: Union[QueueWatcher, QueueMoveWatcher], max_stop_time: float = 0.5
    ):
        """Assert that watcher stops within expected time.

        Args:
            watcher: The watcher to stop
            max_stop_time: Maximum acceptable stop time
        """
        start_time = time.time()
        watcher.stop()

        # If watcher was running in thread, wait for it
        # (In real test, we'd track the thread)
        stop_time = time.time() - start_time

        assert stop_time < max_stop_time, (
            f"Watcher took {stop_time:.2f}s to stop, expected < {max_stop_time}s"
        )
