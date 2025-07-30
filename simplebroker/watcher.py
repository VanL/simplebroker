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
    thread = watcher.run_in_thread()
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
import weakref
from pathlib import Path
from typing import Any, Callable, NamedTuple, Optional, Type

from ._exceptions import OperationalError
from .db import BrokerDB
from .helpers import interruptible_sleep

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
        stop_event: threading.Event,
        initial_checks: int = 100,
        max_interval: float = 0.1,
        burst_sleep: float = 0.0002,
    ):
        self._initial_checks = initial_checks
        self._max_interval = max_interval
        self._burst_sleep = burst_sleep
        self._check_count = 0
        self._stop_event = stop_event
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
            interruptible_sleep(self._burst_sleep, self._stop_event)
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

            rows = list(self._db._runner.run("PRAGMA data_version", fetch=True))
            if not rows:
                return False
            version = rows[0][0]

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
        batch_processing: bool = False,
    ) -> None:
        """
        Initializes the watcher.

        Parameters
        ----------
        db : Union[BrokerDB, str, Path]
            Either a BrokerDB instance or a path to the database file.
            When using run_in_thread(), it's recommended to pass a path to ensure
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
        batch_processing : bool, optional
            If True, process all available messages in a single batch for better
            performance. If False (default), process messages one at a time for
            maximum safety. When False, the watcher will process exactly one message
            per poll cycle in all modes, ensuring that handler failures don't affect
            other messages. This is especially important in consuming mode where
            messages are permanently removed before processing.
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
        # Store batch processing preference
        self._batch_processing = batch_processing

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
            stop_event=self._stop_event,
            initial_checks=env_initial_checks,
            max_interval=env_max_interval,
            burst_sleep=env_burst_sleep,
        )

        # Automatic cleanup finalizer (important on Windows where open file
        # handles prevent TemporaryDirectory from removing .db files)
        # If user code forgets to call stop() / join the thread, the watcher
        # object will eventually be garbage-collected when the test function
        # returns. The finalizer below makes sure the background thread is
        # stopped and joined and that the thread-local BrokerDB is closed,
        # so every SQLite connection is released before the temp directory
        # is removed.
        def _auto_cleanup(wref: weakref.ReferenceType[QueueWatcher]) -> None:
            obj = wref()
            if obj is None:  # already GC'ed
                return
            try:
                obj.stop()
            except Exception:
                pass

            thr = getattr(obj, "_thread", None)  # set by run_in_thread()
            if isinstance(thr, threading.Thread) and thr.is_alive():
                try:
                    thr.join(timeout=1.0)  # don't hang indefinitely
                except Exception:
                    pass

            # ensure the per-thread BrokerDB is closed
            try:
                obj._cleanup_thread_local()
            except Exception:
                pass

        self._finalizer = weakref.finalize(self, _auto_cleanup, weakref.ref(self))

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

        # Detach the finalizer - manual cleanup succeeded
        if hasattr(self, "_finalizer"):
            self._finalizer.detach()

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
            start_time = time.time()
            MAX_TOTAL_RETRY_TIME = 300  # 5 minutes max

            while retry_count < max_retries:
                # Check absolute timeout
                if time.time() - start_time > MAX_TOTAL_RETRY_TIME:
                    raise TimeoutError(
                        f"Watcher retry timeout exceeded ({MAX_TOTAL_RETRY_TIME}s). "
                        f"Retries: {retry_count}, Time elapsed: {time.time() - start_time:.1f}s"
                    )

                try:
                    # Initialize strategy with thread-local database
                    self._strategy.start(self._get_db())

                    # Initial drain of existing messages
                    self._drain_queue()

                    # Main loop
                    while True:
                        # Wait until something might have happened
                        self._strategy.wait_for_activity()

                        # Always try to drain the queue first; this guarantees
                        # that a stop request does not prevent us from
                        # finishing already-visible work, so connections can
                        # be closed and no messages get lost.
                        self._drain_queue()

                        # Stop afterwards - if requested we break out, but only
                        # after the last drain finished.
                        self._check_stop()

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
                        if not interruptible_sleep(wait_time, self._stop_event):
                            # Sleep was interrupted, exit retry loop
                            logger.info("Watcher retry interrupted by stop signal")
                            break
                        # Clean up before retry
                        try:
                            self._cleanup_thread_local()
                        except Exception:
                            pass

        finally:
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

    def _check_stop(self) -> None:
        """Centralized stop check that can be easily mocked in tests.

        Raises:
            _StopLoop: If stop has been requested
        """
        if self._stop_event.is_set():
            raise _StopLoop

    def run_in_thread(self) -> threading.Thread:
        """
        Start the watcher in a new background thread.

        Returns
        -------
        threading.Thread
            The thread running the watcher. The thread is configured as
            `daemon=True` to prevent hanging test runners or applications
            that forget to call stop(). For production use, always call
            stop() and join() the thread for clean shutdown.
        """
        # Daemon thread so that an accidentally-left watcher cannot block
        # interpreter shutdown (e.g. during test runs).
        thread = threading.Thread(target=self.run_forever, daemon=True)
        thread.start()
        # Store reference for the finalizer
        self._thread = thread
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

    def __del__(self) -> None:
        """Safety net to stop watcher if garbage collected while running."""
        try:
            self.stop()
        except Exception:
            pass

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
                if not interruptible_sleep(wait_time, self._stop_event):
                    # Sleep was interrupted, raise to exit
                    raise _StopLoop from None

        # Determine if we should process one at a time
        # Default to one-at-a-time for safety unless batch processing is explicitly enabled

        if self._peek:
            # In peek mode, process based on batch_processing setting
            # Pass since_timestamp to filter at database level
            operational_error_count = 0
            max_operational_retries = 5

            while operational_error_count < max_operational_retries:
                try:
                    for body, ts in db.stream_read_with_timestamps(
                        self._queue,
                        all_messages=self._batch_processing,  # Respect batch processing preference
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

                        # If not batch processing, break after first message
                        if not self._batch_processing:
                            break

                    # Successfully completed, break out of retry loop
                    break

                except OperationalError as e:
                    operational_error_count += 1
                    if operational_error_count >= max_operational_retries:
                        logger.error(
                            f"Failed after {max_operational_retries} operational errors: {e}"
                        )
                        raise
                    # Exponential backoff with jitter
                    jitter = (time.time() * 1000) % 25 / 1000  # 0-25ms jitter
                    wait_time = 0.05 * (2**operational_error_count) + jitter
                    logger.warning(
                        f"OperationalError during peek (retry {operational_error_count}/{max_operational_retries}): {e}. "
                        f"Retrying in {wait_time:.3f} seconds..."
                    )
                    if not interruptible_sleep(wait_time, self._stop_event):
                        # Sleep was interrupted, raise to exit
                        raise _StopLoop from None
        else:
            # Consuming mode
            operational_error_count = 0
            max_operational_retries = 5

            while operational_error_count < max_operational_retries:
                try:
                    # If batch processing is disabled, process exactly one message per drain call
                    if not self._batch_processing:
                        # Read and process exactly one message
                        for body, ts in db.stream_read_with_timestamps(
                            self._queue,
                            all_messages=False,  # Only get one message
                            peek=False,
                            commit_interval=1,
                        ):
                            try:
                                self._dispatch(body, ts)
                                found_messages = True
                            except _StopLoop:
                                raise

                            # Always break after first message when not batch processing
                            break
                    else:
                        # Batch processing enabled - process all available messages
                        while True:
                            # Check stop before each batch iteration
                            self._check_stop()

                            messages_found_this_iteration = False

                            for body, ts in db.stream_read_with_timestamps(
                                self._queue,
                                all_messages=True,  # Process all messages
                                peek=False,
                                commit_interval=1,
                            ):
                                try:
                                    self._dispatch(body, ts)
                                    found_messages = True
                                    messages_found_this_iteration = True
                                except _StopLoop:
                                    raise

                            # No more messages found, exit the loop
                            if not messages_found_this_iteration:
                                break

                    # Successfully completed, break out of retry loop
                    break

                except OperationalError as e:
                    operational_error_count += 1
                    if operational_error_count >= max_operational_retries:
                        logger.error(
                            f"Failed after {max_operational_retries} operational errors: {e}"
                        )
                        raise
                    # Exponential backoff with jitter
                    jitter = (time.time() * 1000) % 25 / 1000  # 0-25ms jitter
                    wait_time = 0.05 * (2**operational_error_count) + jitter
                    logger.warning(
                        f"OperationalError during consume (retry {operational_error_count}/{max_operational_retries}): {e}. "
                        f"Retrying in {wait_time:.3f} seconds..."
                    )
                    if not interruptible_sleep(wait_time, self._stop_event):
                        # Sleep was interrupted, raise to exit
                        raise _StopLoop from None
                except _StopLoop:
                    # Re-raise to exit the watcher
                    raise

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
        initial_checks: int = 100,
        max_interval: float = 0.1,
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
            initial_checks: Number of checks to perform with zero delay for burst handling
            max_interval: Maximum polling interval in seconds (not a fixed interval)
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
            initial_checks=initial_checks,
            max_interval=max_interval,
            error_handler=None,  # We'll handle errors ourselves
            batch_processing=False,  # Always process one at a time for moves
        )

        # Store the original handler and error_handler
        self._move_handler = handler
        self._move_error_handler = error_handler

        # Override stop event if provided
        if stop_event is not None:
            self._stop_event = stop_event

        # Automatic cleanup finalizer (same as parent class)
        # This ensures proper cleanup even if the move watcher is
        # not explicitly stopped
        def _auto_cleanup_move(wref: weakref.ReferenceType[QueueMoveWatcher]) -> None:
            obj = wref()
            if obj is None:  # already GC'ed
                return
            try:
                obj.stop()
            except Exception:
                pass

            thr = getattr(obj, "_thread", None)  # set by run_in_thread()
            if isinstance(thr, threading.Thread) and thr.is_alive():
                try:
                    thr.join(timeout=1.0)  # don't hang indefinitely
                except Exception:
                    pass

            # ensure the per-thread BrokerDB is closed
            try:
                obj._cleanup_thread_local()
            except Exception:
                pass

        self._finalizer = weakref.finalize(self, _auto_cleanup_move, weakref.ref(self))  # type: ignore[arg-type]

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

        This is a convenience method that calls run_in_thread().

        Returns:
            The thread running the watcher.
        """
        return self.run_in_thread()

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
                if not interruptible_sleep(wait_time, self._stop_event):
                    # Sleep was interrupted, exit
                    return

        # Process messages one at a time until source queue is empty
        operational_error_count = 0
        max_operational_retries = 5

        while True:
            # Check for stop signal before each move for responsiveness
            self._check_stop()

            try:
                # Use db.move() to atomically move oldest unclaimed message
                result = db.move(
                    self._source_queue, self._dest_queue, require_unclaimed=True
                )

                if result is None:
                    # No more unclaimed messages to move
                    break

                # Reset error count on successful operation
                operational_error_count = 0
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
            except OperationalError as e:
                operational_error_count += 1
                if operational_error_count >= max_operational_retries:
                    logger.error(
                        f"Failed after {max_operational_retries} operational errors: {e}"
                    )
                    raise
                # Exponential backoff with jitter
                jitter = (time.time() * 1000) % 25 / 1000  # 0-25ms jitter
                wait_time = 0.05 * (2**operational_error_count) + jitter
                logger.warning(
                    f"OperationalError during move (retry {operational_error_count}/{max_operational_retries}): {e}. "
                    f"Retrying in {wait_time:.3f} seconds..."
                )
                if not interruptible_sleep(wait_time, self._stop_event):
                    # Sleep was interrupted, exit
                    return
                continue  # Retry the loop
            except Exception as e:
                logger.error(f"Unexpected error during move: {e}")
                raise

        # Notify strategy that we found messages
        if found_messages:
            self._strategy.notify_activity()
