"""Light-weight queue watcher for SimpleBroker.

This module provides an efficient polling mechanism to consume or monitor
queues with minimal overhead and fast response times.

Typical usage:
    from pathlib import Path
    from simplebroker.watcher import QueueWatcher

    def handle(msg: str, ts: int) -> None:
        print(f"got message @ {ts}: {msg}")

    # Recommended: pass database path for thread-safe operation
    watcher = QueueWatcher(Path("my.db"), "orders", handle)
    watcher.run_forever()  # blocking

    # Or run in background thread:
    thread = watcher.run_async()
    # ... do other work ...
    watcher.stop()
    thread.join()
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any, Callable, NamedTuple, Optional, Type

from .db import BrokerDB

__all__ = ["QueueWatcher", "QueueMoveWatcher", "Message"]


class Message(NamedTuple):
    """Message with metadata from the queue."""

    id: int
    body: str
    timestamp: int
    queue: str


# Create logger for this module
logger = logging.getLogger(__name__)


class _StopLoop(Exception):
    """Internal sentinel for graceful shutdown."""


class SignalHandlerContext:
    """Context manager for proper signal handler restoration."""

    def __init__(self, signum: int, handler: Callable[[int, Any], None]) -> None:
        self.signum = signum
        self.handler = handler
        self.original_handler: Optional[Callable[[int, Any], None] | int] = None

    def __enter__(self) -> SignalHandlerContext:
        self.original_handler = signal.signal(self.signum, self.handler)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        if self.original_handler is not None:
            signal.signal(self.signum, self.original_handler)


class PollingStrategy:
    """High-performance polling strategy with burst handling and PRAGMA data_version."""

    def __init__(
        self,
        initial_checks: int = 100,
        max_interval: float = 0.1,
        burst_sleep: float = 0.0002,
    ):
        self._initial_checks = initial_checks
        self._max_interval = max_interval
        self._burst_sleep = burst_sleep
        self._check_count = 0
        self._stop_event = threading.Event()
        self._data_version: Optional[int] = None
        self._db: Optional[BrokerDB] = None
        self._pragma_failures = 0

    def wait_for_activity(self) -> None:
        """Wait for activity with optimized polling."""
        # Check data version first for immediate activity detection
        if self._db and self._check_data_version():
            self._check_count = 0  # Reset on activity
            return

        # Calculate delay based on check count
        delay = self._get_delay()

        if delay == 0:
            # Micro-sleep to prevent CPU spinning while maintaining responsiveness
            time.sleep(self._burst_sleep)
        else:
            # Wait with timeout
            self._stop_event.wait(timeout=delay)

        self._check_count += 1

    def notify_activity(self) -> None:
        """Reset check count on activity."""
        self._check_count = 0

    def start(self, db: BrokerDB) -> None:
        """Initialize the strategy."""
        self._db = db
        self._check_count = 0
        self._data_version = None
        self._stop_event.clear()

    def stop(self) -> None:
        """Stop the polling loop."""
        self._stop_event.set()

    def _get_delay(self) -> float:
        """Calculate delay based on check count."""
        if self._check_count < self._initial_checks:
            # First 100 checks: no delay (burst handling)
            return 0
        else:
            # Gradual increase to max_interval
            progress = (self._check_count - self._initial_checks) / 100
            return min(progress * self._max_interval, self._max_interval)

    def _check_data_version(self) -> bool:
        """Check PRAGMA data_version for changes."""
        try:
            if self._db is None:
                return False

            cursor = self._db.conn.execute("PRAGMA data_version")
            version = cursor.fetchone()[0]

            if self._data_version is None:
                self._data_version = version
                return False
            elif version != self._data_version:
                self._data_version = version
                return True  # Change detected!

            return False
        except Exception as e:
            # Track PRAGMA failures
            self._pragma_failures += 1
            if self._pragma_failures >= 10:
                raise RuntimeError(
                    f"PRAGMA data_version failed 10 times consecutively. Last error: {e}"
                ) from None
            # Fallback to regular polling if PRAGMA fails
            return False


class QueueWatcher:
    """
    Monitors a queue for new messages and invokes a handler for each one.

    This class provides an efficient polling mechanism with burst handling
    and minimal overhead. It uses PRAGMA data_version for change detection
    when available, falling back to pure polling if needed.

    It is designed to be extensible. Subclasses can override methods like
    _dispatch() or _drain_queue() to add custom behavior such as metrics,
    specialized logging, or message transformation.

    ⚠️ WARNING: Message Loss in Consuming Mode (peek=False)
    -----------------------------------------------
    When running in consuming mode (the default), messages are PERMANENTLY
    REMOVED from the queue immediately upon read, BEFORE your handler processes them.

    The exact sequence is:
    1. Database executes DELETE...RETURNING to remove message from queue
    2. Message is returned to the watcher
    3. Handler is called with the deleted message
    4. If handler fails, the message is already gone forever

    This means:
    - If your handler raises an exception, the message is already gone
    - If your process crashes after reading but before processing, messages are lost
    - There is no built-in retry mechanism for failed messages
    - Messages are removed from the queue immediately, not after successful processing

    For critical applications where message loss is unacceptable, consider:
    1. Using peek mode (peek=True) with manual acknowledgment after successful processing
    2. Implementing an error_handler that saves failed messages to a dead letter queue
    3. Using the checkpoint pattern with timestamps to track processing progress

    See the README for detailed examples of safe message processing patterns.
    """

    def __init__(
        self,
        db: BrokerDB | str | Path,
        queue: str,
        handler: Callable[[str, int], None],
        *,
        peek: bool = False,
        initial_checks: int = 100,
        max_interval: float = 0.1,
        error_handler: Optional[Callable[[Exception, str, int], Optional[bool]]] = None,
        since_timestamp: Optional[int] = None,
    ) -> None:
        """
        Initializes the watcher.

        Parameters
        ----------
        db : Union[BrokerDB, str, Path]
            Either a BrokerDB instance or a path to the database file.
            When using run_async(), it's recommended to pass a path to ensure
            each thread creates its own connection. If a BrokerDB instance is
            provided, its path will be extracted for thread-safe operation.
        queue : str
            Name of the queue to watch.
        handler : Callable[[str, int], None]
            Function to be called for each message. Receives (message, timestamp).
        peek : bool, optional
            If True, monitor messages without consuming them (at-least-once
            notification). If False (default), consume messages with
            exactly-once semantics.

            ⚠️ IMPORTANT: In consuming mode (peek=False), messages are removed
            from the queue BEFORE your handler processes them. If your handler
            fails or crashes, those messages are permanently lost. Use peek=True
            with manual acknowledgment for critical messages.
        initial_checks : int, optional
            Number of checks to perform with zero delay for burst handling.
            Default is 100.
        max_interval : float, optional
            Maximum polling interval in seconds. Default is 0.1 (100ms).
        error_handler : Callable[[Exception, str, int], Optional[bool]], optional
            A function called when the main `handler` raises an exception.
            It receives (exception, message, timestamp).
            - Return True:  Ignore the exception and continue processing.
            - Return False: Stop the watcher gracefully.
            - Return None or don't return: Use the default behavior (log to
              stderr and continue).
        since_timestamp : int, optional
            Only process messages with timestamps greater than this value.
            In peek mode, the watcher maintains an internal checkpoint that is only
            advanced after a message is successfully dispatched to the handler. This
            ensures that if a handler fails, the message will be re-processed on the
            next poll, providing at-least-once notification semantics.
            If None, processes all messages.
        """
        # Extract database path for thread-safe operation
        self._provided_db: Optional[BrokerDB]
        if isinstance(db, BrokerDB):
            self._db_path = db.db_path
            # Keep the original DB for single-threaded compatibility
            self._provided_db = db
        else:
            self._db_path = Path(db)
            self._provided_db = None

        self._queue = queue
        # Validate handler is callable
        if not callable(handler):
            raise TypeError(f"handler must be callable, got {type(handler).__name__}")
        self._handler = handler
        self._peek = peek
        # Validate error_handler is callable if provided
        if error_handler is not None and not callable(error_handler):
            raise TypeError(
                f"error_handler must be callable if provided, got {type(error_handler).__name__}"
            )
        self._error_handler = error_handler
        self._stop_event = threading.Event()
        # Initialize _last_seen_ts with since_timestamp if provided, otherwise 0
        self._last_seen_ts = since_timestamp if since_timestamp is not None else 0

        # Thread-local storage for database connections
        self._thread_local = threading.local()

        # Read environment variables with defaults and error handling
        try:
            env_initial_checks = int(
                os.environ.get("SIMPLEBROKER_INITIAL_CHECKS", str(initial_checks))
            )
        except ValueError:
            logger.warning(
                f"Invalid SIMPLEBROKER_INITIAL_CHECKS value, using default: {initial_checks}"
            )
            env_initial_checks = initial_checks

        # Use max_interval with env var fallback
        try:
            env_max_interval = float(
                os.environ.get("SIMPLEBROKER_MAX_INTERVAL", str(max_interval))
            )
        except ValueError:
            logger.warning(
                f"Invalid SIMPLEBROKER_MAX_INTERVAL value, using default: {max_interval}"
            )
            env_max_interval = max_interval

        try:
            env_burst_sleep = float(
                os.environ.get("SIMPLEBROKER_BURST_SLEEP", "0.0002")
            )
        except ValueError:
            logger.warning(
                "Invalid SIMPLEBROKER_BURST_SLEEP value, using default: 0.0002"
            )
            env_burst_sleep = 0.0002

        # Create polling strategy with optimized settings
        self._strategy = PollingStrategy(
            initial_checks=env_initial_checks,
            max_interval=env_max_interval,
            burst_sleep=env_burst_sleep,
        )

    def _get_db(self) -> BrokerDB:
        """Get a thread-local database connection.

        Creates a new connection if one doesn't exist for the current thread.
        Always creates a thread-local DB for safety, even if a BrokerDB was provided.

        Returns
        -------
        BrokerDB
            A thread-local database connection for the current thread.
        """
        # Check if we have a connection for this thread
        if not hasattr(self._thread_local, "db"):
            # Always create a new thread-local connection for safety
            self._thread_local.db = BrokerDB(str(self._db_path))

        db: BrokerDB = self._thread_local.db
        return db

    def _cleanup_thread_local(self) -> None:
        """Clean up thread-local database connections.

        Closes any active database connection for the current thread and removes
        it from thread-local storage. This method is called during shutdown and
        error recovery to ensure proper resource cleanup.
        """
        if hasattr(self._thread_local, "db"):
            try:
                # Always close thread-local DBs (we no longer reuse provided DBs)
                self._thread_local.db.close()
            except Exception as e:
                logger.warning(f"Error closing thread-local database: {e}")
            finally:
                delattr(self._thread_local, "db")

    def __enter__(self) -> QueueWatcher:
        """Enter the context manager.

        Returns
        -------
        QueueWatcher
            Returns self for use in with statements.
        """
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Exit the context manager.

        Ensures proper cleanup of resources including stopping the watcher
        and closing any thread-local database connections.

        Parameters
        ----------
        exc_type : type | None
            The exception type if an exception occurred.
        exc_val : Exception | None
            The exception instance if an exception occurred.
        exc_tb : TracebackType | None
            The traceback if an exception occurred.
        """
        try:
            self.stop()
        except Exception as e:
            logger.warning(f"Error during stop in __exit__: {e}")

        try:
            self._cleanup_thread_local()
        except Exception as e:
            logger.warning(f"Error during cleanup in __exit__: {e}")

    def run_forever(self) -> None:
        """
        Start watching the queue. This method blocks until stop() is called
        or a SIGINT (Ctrl-C) is received.
        """
        signal_context = None

        try:
            # Only install signal handler if we're in the main thread
            if threading.current_thread() is threading.main_thread():
                signal_context = SignalHandlerContext(
                    signal.SIGINT, self._sigint_handler
                )
                signal_context.__enter__()

            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    # Initialize strategy with thread-local database
                    self._strategy.start(self._get_db())

                    # Initial drain of existing messages
                    self._drain_queue()

                    # Main loop
                    while not self._stop_event.is_set():
                        # Wait for activity
                        self._strategy.wait_for_activity()

                        if self._stop_event.is_set():
                            break

                        # Process available messages
                        self._drain_queue()

                    # If we get here, we exited normally
                    break

                except _StopLoop:
                    # Normal shutdown
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(
                            f"Watcher failed after {max_retries} retries. Last error: {e}"
                        )
                        raise
                    else:
                        wait_time = 2**retry_count  # Exponential backoff
                        logger.warning(
                            f"Watcher error (retry {retry_count}/{max_retries}): {e}. "
                            f"Retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                        # Clean up before retry
                        try:
                            self._strategy.stop()
                            self._cleanup_thread_local()
                        except Exception:
                            pass

        finally:
            # Clean up
            try:
                self._strategy.stop()
            except Exception as e:
                logger.warning(f"Error stopping strategy: {e}")

            # Clean up thread-local connections
            self._cleanup_thread_local()

            # Restore original signal handler
            if signal_context is not None:
                signal_context.__exit__(None, None, None)

    def stop(self) -> None:
        """
        Request a graceful shutdown. This method is thread-safe and can be
        called from another thread or a signal handler. The watcher will stop
        after processing the current message, if any.
        """
        self._stop_event.set()
        self._strategy.notify_activity()  # Wake up wait_for_activity

    def run_async(self) -> threading.Thread:
        """
        Start the watcher in a new background thread.

        Returns
        -------
        threading.Thread
            The thread running the watcher. The thread is configured as
            `daemon=False` to prevent the main application from exiting
            silently while the watcher is active.
        """
        thread = threading.Thread(target=self.run_forever, daemon=False)
        thread.start()
        return thread

    def is_running(self) -> bool:
        """
        Check if the watcher is currently running.

        Returns
        -------
        bool
            True if the watcher's run() method is actively processing messages,
            False if the watcher has been stopped or hasn't started yet.
        """
        return not self._stop_event.is_set()

    def _sigint_handler(self, signum: int, frame: Any) -> None:
        """Convert SIGINT to graceful shutdown."""
        self.stop()

    def _drain_queue(self) -> None:
        """Process all currently available messages with DB error handling.

        IMPORTANT: Message Consumption Timing
        ------------------------------------
        In consuming mode (peek=False), messages are removed from the queue
        by the database's DELETE...RETURNING operation BEFORE the handler is
        called. This means:

        1. Message is deleted from queue (point of no return)
        2. Message is returned to this method
        3. _dispatch() is called with the message
        4. Handler processes the message (may succeed or fail)

        If the handler fails or the process crashes after step 1, the message
        is permanently lost. There is no way to recover it from the queue.

        In peek mode (peek=True), messages are never removed from the queue
        by this watcher. They remain available for other consumers or for
        manual removal after successful processing.
        """
        found_messages = False
        db_retry_count = 0
        max_db_retries = 3

        while db_retry_count < max_db_retries:
            try:
                # Get thread-local database connection
                db = self._get_db()
                break
            except Exception as e:
                db_retry_count += 1
                if db_retry_count >= max_db_retries:
                    logger.error(
                        f"Failed to get database connection after {max_db_retries} retries: {e}"
                    )
                    raise
                wait_time = 2**db_retry_count  # Exponential backoff
                logger.warning(
                    f"Database connection error (retry {db_retry_count}/{max_db_retries}): {e}. "
                    f"Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)

        # Determine if we should process one at a time
        # (when we have an error handler that might stop processing)
        process_one_at_time = self._error_handler is not None and not self._peek

        if self._peek:
            # In peek mode, we can safely process all at once since we're not consuming
            # Pass since_timestamp to filter at database level
            for body, ts in db.stream_read_with_timestamps(
                self._queue,
                all_messages=True,
                peek=True,
                commit_interval=1,
                since_timestamp=self._last_seen_ts,
            ):
                # No need to skip messages - database already filtered them
                try:
                    self._dispatch(body, ts)
                    # Only update _last_seen_ts after successful dispatch
                    self._last_seen_ts = max(self._last_seen_ts, ts)
                    found_messages = True
                except _StopLoop:
                    # Re-raise to exit the loop
                    raise
                except Exception:
                    # Don't update timestamp if dispatch failed
                    # This ensures we'll retry the message next time
                    pass

                if self._stop_event.is_set():
                    raise _StopLoop
        else:
            # Consuming mode
            try:
                while True:
                    # Process one at a time if we might need to stop on error
                    messages_found_this_iteration = False

                    for body, ts in db.stream_read_with_timestamps(
                        self._queue,
                        all_messages=not process_one_at_time,  # Process all at once unless we need fine control
                        peek=False,
                        commit_interval=1,
                    ):
                        try:
                            self._dispatch(body, ts)
                            found_messages = True
                            messages_found_this_iteration = True
                        except _StopLoop:
                            # Re-raise to exit the outer loop
                            raise

                        # If processing one at a time, break after each message
                        # Note: stream_read_with_timestamps already handles commits
                        if process_one_at_time:
                            break

                    if self._stop_event.is_set():
                        raise _StopLoop

                    # If we're processing one at a time and found nothing, we're done
                    if process_one_at_time and not messages_found_this_iteration:
                        break

                    # If we're processing all at once, we're done after one iteration
                    if not process_one_at_time:
                        break
            except _StopLoop:
                # Exit gracefully when stop is requested
                pass

        # Notify strategy that we found messages (helps with polling backoff)
        if found_messages:
            self._strategy.notify_activity()

    def _dispatch(self, message: str, timestamp: int) -> None:
        """Dispatch a message to the handler with error handling and size validation."""
        # Validate message size (10MB limit)
        message_size = len(message.encode("utf-8"))
        if message_size > 10 * 1024 * 1024:  # 10MB
            error_msg = f"Message size ({message_size} bytes) exceeds 10MB limit"
            logger.error(error_msg)
            if self._error_handler:
                try:
                    result = self._error_handler(
                        ValueError(error_msg), message[:1000] + "...", timestamp
                    )
                    if result is False:
                        self._stop_event.set()
                        raise _StopLoop from None
                except Exception as e:
                    logger.error(f"Error handler failed: {e}")
            return

        try:
            self._handler(message, timestamp)
        except Exception as e:
            # Call error handler if provided
            if self._error_handler is not None:
                stop_requested = False
                try:
                    result = self._error_handler(e, message, timestamp)
                    if result is False:
                        # Error handler says stop
                        stop_requested = True
                    # True or None means continue
                except Exception as eh_error:
                    # Error handler itself failed
                    logger.error(
                        f"Error handler failed: {eh_error}\nOriginal error: {e}"
                    )

                # Raise _StopLoop outside the try block to avoid catching it
                if stop_requested:
                    # Set stop event to ensure the watcher stops completely
                    self._stop_event.set()
                    raise _StopLoop from None
            else:
                # Default behavior: log error and continue
                logger.error(f"Handler error: {e}")


class QueueMoveWatcher(QueueWatcher):
    """
    Watches a source queue and atomically moves messages to a destination queue.

    The move happens atomically BEFORE the handler is called, ensuring that
    messages are safely moved even if the handler fails. The handler receives
    the message for observation purposes only.
    """

    def __init__(
        self,
        broker: BrokerDB | str | Path,
        source_queue: str,
        dest_queue: str,
        handler: Callable[[str, int], None],
        *,
        poll_interval: float = 0.1,
        error_handler: Optional[Callable[[Exception, str, int], Optional[bool]]] = None,
        stop_event: Optional[threading.Event] = None,
        max_messages: Optional[int] = None,
    ):
        """
        Initialize a QueueMoveWatcher.

        Args:
            broker: SimpleBroker instance
            source_queue: Name of source queue to move messages from
            dest_queue: Name of destination queue to move messages to
            handler: Function called with (message_body, timestamp) for each moved message
            poll_interval: Seconds between polls when queue is empty
            error_handler: Called when handler raises an exception
            stop_event: Event to signal watcher shutdown
            max_messages: Maximum messages to move before stopping

        Raises:
            ValueError: If source_queue == dest_queue
        """

        if source_queue == dest_queue:
            raise ValueError("Cannot move messages to the same queue")

        # Store move-specific attributes
        self._source_queue = source_queue
        self._dest_queue = dest_queue
        self._move_count = 0
        self._max_messages = max_messages

        # Wrap the user's handler to handle Message tuples
        def wrapped_handler(body: str, ts: int) -> None:
            # This is never called - we override _drain_queue
            pass

        # Initialize parent with peek=True (we handle consumption ourselves)
        super().__init__(
            broker,
            source_queue,  # Watch the source queue
            wrapped_handler,
            peek=True,  # Force peek mode
            max_interval=poll_interval,
            error_handler=None,  # We'll handle errors ourselves
        )

        # Store the original handler and error_handler
        self._move_handler = handler
        self._move_error_handler = error_handler

        # Override stop event if provided
        if stop_event is not None:
            self._stop_event = stop_event

    @property
    def move_count(self) -> int:
        """Total number of successfully moved messages."""
        return self._move_count

    @property
    def source_queue(self) -> str:
        """Source queue name."""
        return self._source_queue

    @property
    def dest_queue(self) -> str:
        """Destination queue name."""
        return self._dest_queue

    def start(self) -> threading.Thread:
        """Start the move watcher in a background thread.

        This is a convenience method that calls run_async().

        Returns:
            The thread running the watcher.
        """
        return self.run_async()

    def run(self) -> None:
        """Run the move watcher synchronously until max_messages or stop_event.

        This is a convenience method that calls run_forever().
        """
        self.run_forever()

    def _drain_queue(self) -> None:
        """Move ALL messages from source to destination queue."""
        found_messages = False
        db_retry_count = 0
        max_db_retries = 3

        while db_retry_count < max_db_retries:
            try:
                # Get thread-local database connection
                db = self._get_db()
                break
            except Exception as e:
                db_retry_count += 1
                if db_retry_count >= max_db_retries:
                    logger.error(
                        f"Failed to get database connection after {max_db_retries} retries: {e}"
                    )
                    raise
                wait_time = 2**db_retry_count  # Exponential backoff
                logger.warning(
                    f"Database connection error (retry {db_retry_count}/{max_db_retries}): {e}. "
                    f"Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)

        # Process messages one at a time until source queue is empty
        while True:
            try:
                # Use db.move() to atomically move oldest unclaimed message
                result = db.move(
                    self._source_queue, self._dest_queue, require_unclaimed=True
                )

                if result is None:
                    # No more unclaimed messages to move
                    break

                found_messages = True
                self._move_count += 1

                # Create Message object with actual ID from move result
                moved_msg = Message(
                    result["id"], result["body"], result["ts"], self._dest_queue
                )

                # Call handler with moved message (body, timestamp)
                try:
                    self._move_handler(moved_msg.body, moved_msg.timestamp)
                except Exception as e:
                    # Handler error doesn't affect move (already done)
                    if self._move_error_handler:
                        try:
                            error_result = self._move_error_handler(
                                e, moved_msg.body, moved_msg.timestamp
                            )
                            if error_result is False:
                                self._stop_event.set()
                                raise _StopLoop from None
                        except Exception as eh_error:
                            logger.error(f"Error handler failed: {eh_error}")
                    else:
                        logger.error(f"Handler error: {e}")

                # Check if we've reached max messages
                if self._max_messages and self._move_count >= self._max_messages:
                    logger.info(f"Reached max_messages limit ({self._max_messages})")
                    self._stop_event.set()
                    raise _StopLoop

            except _StopLoop:
                raise
            except Exception as e:
                logger.error(f"Unexpected error during move: {e}")
                raise

            # Check if we should stop after each message
            if self._stop_event.is_set():
                raise _StopLoop

        # Notify strategy that we found messages
        if found_messages:
            self._strategy.notify_activity()
