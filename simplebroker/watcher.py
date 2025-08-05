"""Light-weight queue watcher for SimpleBroker.

This module provides an efficient polling mechanism to consume or monitor
queues with minimal overhead and fast response times.

IMPORTANT FOR PEOPLE SUBCLASSING/USING API: Proper Resource Cleanup
-------------------------------------------------------------------
Watchers create background threads and database connections that must be
properly cleaned up to avoid resource leaks, especially on Windows where
file locking is strict. Always use one of these patterns:

1. Context Manager (RECOMMENDED - automatic cleanup):
    with QueueWatcher("my.db", "tasks", handler) as watcher:
        # Thread starts automatically in __enter__
        time.sleep(60)  # Do work
    # Thread is stopped and joined automatically in __exit__

2. Manual Management (ensure stop() is called):
    watcher = QueueWatcher("my.db", "tasks", handler)
    thread = watcher.run_in_thread()
    try:
        # Do work
    finally:
        watcher.stop()  # This joins the thread by default, ensuring cleanup

3. Signal Handling (for long-running services):
    import signal
    watcher = QueueWatcher("my.db", "tasks", handler)

    def shutdown(signum, frame):
        watcher.stop()  # Ensures clean shutdown
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    watcher.run_forever()  # Handles SIGINT (Ctrl+C) automatically

WARNING: Not calling stop() can cause:
- Thread leaks (threads continue running after main program exits)
- Database connection leaks (SQLite connections remain open)
- File locking issues on Windows (database files can't be deleted)
- Resource exhaustion in long-running applications

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

import contextlib
import logging
import random
import signal
import threading
import time
import weakref
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, List, NamedTuple, cast

# For Python 3.8 compatibility, we avoid using Self type
# and use string forward references instead
from ._constants import MAX_MESSAGE_SIZE, MAX_TOTAL_RETRY_TIME, load_config
from ._exceptions import OperationalError
from .db import BrokerDB
from .helpers import interruptible_sleep

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["Message", "QueueMoveWatcher", "QueueWatcher"]


class Message(NamedTuple):
    """Message with metadata from the queue."""

    id: int
    body: str
    timestamp: int
    queue: str


# Create logger for this module
logger = logging.getLogger(__name__)

# Load configuration once at module level
_config = load_config()


class _StopLoop(Exception):
    """Internal sentinel for graceful shutdown."""


class BaseWatcher(ABC):
    """Base class for all watchers with common retry and error handling logic.

    This abstract base class provides:
    - Database connection retry logic
    - Operational error retry logic
    - Common error handler patterns
    - Thread and resource management

    Subclasses must implement _drain_queue() to define their specific
    message processing behavior.
    """

    _db_arg: str | Path

    def __init__(
        self,
        db: BrokerDB | str | Path,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Initialize base watcher.

        Args:
            db: Database instance or path
            stop_event: Optional event to signal watcher shutdown

        """
        # Handle different db input types
        if isinstance(db, BrokerDB):
            self._db_arg = db.db_path
        else:
            self._db_arg = db

        # Event to signal the watcher to stop
        self._stop_event = stop_event or threading.Event()

        # Weak reference to the thread running this watcher (for cleanup warnings)
        self._thread: weakref.ref[threading.Thread] | None = None

        # Thread-local storage for database connections
        self._thread_local = threading.local()

        # Track if we have a thread-local db to close
        self._has_thread_db = False

        # Thread-safe lock for stop synchronization
        self._stop_lock = threading.Lock()

    def _get_db(self) -> BrokerDB:
        """Get thread-local database connection.

        Each thread gets its own BrokerDB instance to ensure thread safety.
        The connection is cached in thread-local storage and reused.
        """
        if not hasattr(self._thread_local, "db"):
            self._thread_local.db = BrokerDB(str(self._db_arg))
            self._has_thread_db = True
        return cast(BrokerDB, self._thread_local.db)

    def _get_db_with_retry(self) -> BrokerDB:
        """Get database connection with exponential backoff retry."""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                return self._get_db()
            except Exception as e:
                if attempt >= max_retries - 1:
                    if _config["BROKER_LOGGING_ENABLED"]:
                        logger.exception(
                            f"Failed to get database connection after {max_retries} retries: {e}",
                        )
                    raise

                wait_time = 2 ** (attempt + 1)  # Exponential backoff
                if _config["BROKER_LOGGING_ENABLED"]:
                    logger.debug(
                        f"Database connection error (retry {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {wait_time} seconds...",
                    )

                if not interruptible_sleep(wait_time, self._stop_event):
                    raise _StopLoop from None

        # This should never be reached, but mypy needs it
        raise RuntimeError("Failed to get database connection")

    def _process_with_retry(
        self, process_func: Callable[[], Any], operation_name: str
    ) -> Any:
        """Execute a processing function with operational error retry.

        Args:
            process_func: Function to execute with retry
            operation_name: Name of operation for logging

        Returns:
            Result from process_func

        Raises:
            OperationalError: If all retries exhausted
            _StopLoop: If stop requested during retry

        """
        max_retries = 5

        for attempt in range(max_retries):
            try:
                return process_func()
            except OperationalError as e:
                if attempt >= max_retries - 1:
                    if _config["BROKER_LOGGING_ENABLED"]:
                        logger.exception(
                            f"Failed after {max_retries} operational errors: {e}",
                        )
                    raise

                wait_time = self._calculate_retry_wait_time(attempt)
                if _config["BROKER_LOGGING_ENABLED"]:
                    logger.debug(
                        f"OperationalError during {operation_name} "
                        f"(retry {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {wait_time:.3f} seconds...",
                    )

                if not interruptible_sleep(wait_time, self._stop_event):
                    raise _StopLoop from None
            except _StopLoop:
                raise

        # This should never be reached
        raise RuntimeError("Failed to process with retry")

    def _calculate_retry_wait_time(self, attempt: int) -> float:
        """Calculate retry wait time with exponential backoff and jitter."""
        base_wait: float = 0.05 * (2**attempt)
        jitter: float = (time.time() * 1000) % 25 / 1000  # 0-25ms jitter
        return float(base_wait + jitter)

    def _handle_handler_error(
        self,
        e: Exception,
        message: str,
        timestamp: int,
        error_handler: Callable[[Exception, str, int], bool | None] | None,
    ) -> None:
        """Handle errors from message handler.

        Args:
            e: The exception that was raised
            message: The message being processed
            timestamp: The message timestamp
            error_handler: Optional error handler callback

        Raises:
            _StopLoop: If error handler returns False

        """
        if error_handler is not None:
            stop_requested = False
            try:
                result = error_handler(e, message, timestamp)
                if result is False:
                    # Error handler says stop
                    stop_requested = True
                # True or None means continue
            except Exception as eh_error:
                # Error handler itself failed
                if _config["BROKER_LOGGING_ENABLED"]:
                    logger.exception(
                        f"Error handler failed: {eh_error}\nOriginal error: {e}",
                    )

            # Raise _StopLoop outside the try block to avoid catching it
            if stop_requested:
                # Set stop event to ensure the watcher stops completely
                self._stop_event.set()
                raise _StopLoop from None
        # Default behavior: log error and continue
        elif _config["BROKER_LOGGING_ENABLED"]:
            logger.error(f"Handler error: {e}")

    def _check_stop(self) -> None:
        """Check if stop has been requested and raise _StopLoop if so."""
        if self._stop_event.is_set():
            raise _StopLoop

    def stop(self, *, join: bool = True, timeout: float = 2.0) -> None:
        """Request a graceful shutdown.

        This method is thread-safe and can be called from another thread or
        a signal handler. The watcher will stop after processing the current
        message, if any.

        If join is True (default), this call also waits until the background
        thread finishes (or timeout seconds, whichever comes first). Calling
        stop() multiple times is safe.

        CRITICAL: Always call this method before your program exits!
        ---------------------------------------------------------
        Not calling stop() can cause:
        - Thread leaks (background thread continues running)
        - Database connection leaks (SQLite connections stay open)
        - File locking on Windows (can't delete database files)
        - Resource exhaustion in long-running applications

        The join parameter (default True) is important because it ensures
        the thread has actually terminated before this method returns. This
        prevents race conditions where the main program exits while the
        watcher thread is still cleaning up.

        Thread-safety: This method uses a lock to ensure that multiple
        concurrent calls to stop() are handled correctly. Only the first
        caller will perform the join operation.

        Args:
            join: Whether to wait for the thread to finish. Default is True.
                Set to False only if you will join the thread separately.
            timeout: Maximum time to wait for thread to finish. Default is 2.0 seconds.
                If the thread doesn't finish within this time, the method returns
                anyway (thread might still be running).

        """
        # Use stop_lock if available (from subclasses), otherwise just proceed
        lock = getattr(self, "_stop_lock", None)

        if lock:
            with lock:  # idempotent / thread-safe
                self._perform_stop(join, timeout)
        else:
            self._perform_stop(join, timeout)

    def _perform_stop(self, join: bool, timeout: float) -> None:
        """Internal method to perform the actual stop operations."""
        if self._stop_event.is_set():
            join = False  # someone else already did the join
        else:
            self._stop_event.set()
            # Notify strategy if it exists (from subclasses)
            strategy = getattr(self, "_strategy", None)
            if strategy and hasattr(strategy, "notify_activity"):
                strategy.notify_activity()  # Wake up wait_for_activity

        if join and self._thread is not None:
            thread = self._thread()  # Get strong reference from weak ref
            if (
                thread is not None
                and thread.is_alive()
                and thread != threading.current_thread()
            ):
                thread.join(timeout)

        # After the thread is gone we can close the per-thread DB
        thread_ref = self._thread
        if thread_ref is None:
            should_cleanup = True
        else:
            thread = thread_ref()
            should_cleanup = thread is None or not thread.is_alive()

        if should_cleanup:
            with contextlib.suppress(Exception):
                self._cleanup_thread_local()

        # detach finalizer if it exists - resources are already released
        if hasattr(self, "_finalizer"):
            self._finalizer.detach()

    def _cleanup_thread_local(self) -> None:
        """Clean up thread-local database connections.

        Closes any active database connection for the current thread and removes
        it from thread-local storage. This method is called during shutdown and
        error recovery to ensure proper resource cleanup.
        """
        if hasattr(self._thread_local, "db"):
            try:
                self._thread_local.db.close()
            except Exception as e:
                if _config["BROKER_LOGGING_ENABLED"]:
                    logger.warning(f"Error closing thread-local database: {e}")
            finally:
                delattr(self._thread_local, "db")
                self._has_thread_db = False

    def is_running(self) -> bool:
        """Check if the watcher is currently running."""
        return not self._stop_event.is_set()

    @abstractmethod
    def _drain_queue(self) -> None:
        """Process messages - must be implemented by subclasses."""

    def run_forever(self) -> None:
        """Run the watcher continuously until stopped.

        This method blocks until stop() is called or SIGINT is received.
        """
        msg = "Subclasses must implement run_forever"
        raise NotImplementedError(msg)

    def run_in_thread(self) -> threading.Thread:
        """Start the watcher in a new background thread.

        Returns:
            The thread running the watcher

        """
        msg = "Subclasses must implement run_in_thread"
        raise NotImplementedError(msg)

    def __enter__(self) -> BaseWatcher:
        """Enter context manager - start watcher in background thread."""
        self.run_in_thread()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit context manager - stop and clean up."""
        self.stop()

    def __del__(self) -> None:
        """Destructor warns if watcher wasn't properly stopped."""
        if hasattr(self, "_thread") and self._thread is not None:
            thread = self._thread()
            if thread is not None and thread.is_alive():
                # Resource leak detected
                import warnings

                warnings.warn(
                    f"{self.__class__.__name__} instance was not properly stopped. "
                    "This will leak threads and database connections. "
                    "Always call stop() or use a context manager.",
                    ResourceWarning,
                    stacklevel=2,
                )
                # Attempt cleanup
                with contextlib.suppress(Exception):
                    self.stop(join=False)


class SignalHandlerContext:
    """Context manager for proper signal handler restoration."""

    def __init__(self, signum: int, handler: Callable[[int, Any], None]) -> None:
        self.signum = signum
        self.handler = handler
        self.original_handler: Callable[[int, Any], None] | int | None = None

    def __enter__(self) -> SignalHandlerContext:
        self.original_handler = signal.signal(self.signum, self.handler)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
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
    ) -> None:
        self._initial_checks = initial_checks
        self._max_interval = max_interval
        self._burst_sleep = burst_sleep
        self._check_count = 0
        self._stop_event = stop_event
        self._data_version: int | None = None
        self._db: BrokerDB | None = None
        self._pragma_failures = 0

    def wait_for_activity(self) -> None:
        """Wait for activity with optimized polling."""
        # Check data version first for immediate activity detection
        if self._db and self._check_data_version():
            # Don't reset here - let notify_activity handle it when messages are actually processed
            # Also don't increment check count since we detected activity
            return

        # Calculate delay based on check count
        delay = self._get_delay()

        if delay == 0:
            # Micro-sleep to prevent CPU spinning while maintaining responsiveness
            interruptible_sleep(self._burst_sleep, self._stop_event)
        else:
            # Wait with timeout
            self._stop_event.wait(timeout=delay)

        # Only increment if we actually waited (no activity detected)
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
        base_delay = self._calculate_base_delay()

        if base_delay > 0:
            # Add 10-20% jitter to prevent synchronized polling
            jitter_factor = _config["BROKER_JITTER_FACTOR"]
            jitter = random.uniform(-jitter_factor, jitter_factor) * base_delay
            return max(0, base_delay + jitter)

        return base_delay

    def _calculate_base_delay(self) -> float:
        """Calculate base delay without jitter."""
        if self._check_count < self._initial_checks:
            # First 100 checks: no delay (burst handling)
            return 0
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
            if version != self._data_version:
                self._data_version = version
                return True  # Change detected!

            return False
        except Exception as e:
            # Track PRAGMA failures
            self._pragma_failures += 1
            if self._pragma_failures >= 10:
                msg = f"PRAGMA data_version failed 10 times consecutively. Last error: {e}"
                raise RuntimeError(
                    msg,
                ) from None
            # Fallback to regular polling if PRAGMA fails
            return False


class QueueWatcher(BaseWatcher):
    """Monitors a queue for new messages and invokes a handler for each one.

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
        error_handler: Callable[[Exception, str, int], bool | None] | None = None,
        since_timestamp: int | None = None,
        batch_processing: bool = False,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Initializes the watcher.

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
        # Initialize parent class
        super().__init__(db, stop_event)

        # Keep the original DB for single-threaded compatibility
        self._provided_db = db if isinstance(db, BrokerDB) else None

        self._queue = queue
        # Validate handler is callable
        if not callable(handler):
            msg = f"handler must be callable, got {type(handler).__name__}"
            raise TypeError(msg)
        self._handler = handler
        self._peek = peek
        # Validate error_handler is callable if provided
        if error_handler is not None and not callable(error_handler):
            msg = f"error_handler must be callable if provided, got {type(error_handler).__name__}"
            raise TypeError(
                msg,
            )
        self._error_handler = error_handler
        # Initialize _last_seen_ts with since_timestamp if provided, otherwise 0
        self._last_seen_ts = since_timestamp if since_timestamp is not None else 0
        # Store batch processing preference
        self._batch_processing = batch_processing

        # Two-phase detection configuration
        self._skip_idle_check = _config["BROKER_SKIP_IDLE_CHECK"]

        # Read environment variables with defaults and error handling
        try:
            env_initial_checks = _config["SIMPLEBROKER_INITIAL_CHECKS"]
        except ValueError:
            if _config["BROKER_LOGGING_ENABLED"]:
                logger.warning(
                    f"Invalid SIMPLEBROKER_INITIAL_CHECKS value, using default: {initial_checks}",
                )
            env_initial_checks = initial_checks

        # Use max_interval with env var fallback
        try:
            env_max_interval = _config["SIMPLEBROKER_MAX_INTERVAL"]
        except (ValueError, KeyError):
            if _config["BROKER_LOGGING_ENABLED"]:
                logger.warning(
                    f"Invalid SIMPLEBROKER_MAX_INTERVAL value, using default: {max_interval}",
                )
            env_max_interval = max_interval

        env_burst_sleep = _config["SIMPLEBROKER_BURST_SLEEP"]

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
        #
        # WARNING: This is a safety net, NOT a replacement for proper cleanup!
        # --------------------------------------------------------------------
        # The finalizer runs during garbage collection, which is:
        # - Non-deterministic (might not run immediately)
        # - Not guaranteed (might not run at all in some cases)
        # - Too late (resources held longer than necessary)
        #
        # Always use context managers or call stop() explicitly!
        def _auto_cleanup(wref: weakref.ReferenceType[QueueWatcher]) -> None:
            obj = wref()
            if obj is None:  # already GC'ed
                return
            with contextlib.suppress(Exception):
                obj.stop()

            thr = getattr(obj, "_thread", None)  # set by run_in_thread()
            if isinstance(thr, threading.Thread) and thr.is_alive():
                try:
                    thr.join(timeout=1.0)  # don't hang indefinitely
                except Exception:
                    pass

            # ensure the per-thread BrokerDB is closed
            with contextlib.suppress(Exception):
                obj._cleanup_thread_local()

        self._finalizer = weakref.finalize(self, _auto_cleanup, weakref.ref(self))

    def __enter__(self) -> QueueWatcher:
        """Enter the context manager and start the watcher in a background thread.

        This method is called when entering a `with` statement. It automatically
        starts the watcher in a background thread, making it easy to ensure
        proper cleanup:

        Example:
            with QueueWatcher("my.db", "tasks", handler) as watcher:
                # Watcher thread is now running
                time.sleep(60)  # Process messages for 60 seconds
            # Thread is automatically stopped and joined when exiting the block

        Returns:
        -------
        QueueWatcher
            Returns self for use in with statements.

        """
        thread = self.run_in_thread()
        self._thread = weakref.ref(thread)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit the context manager.

        Ensures proper cleanup of resources including stopping the watcher
        and closing any thread-local database connections. This method is
        called automatically when exiting a `with` block, even if an
        exception occurs.

        IMPORTANT: This method ensures that:
        1. The background thread is stopped (stop event is set)
        2. The thread is joined (waits for it to actually terminate)
        3. Database connections are closed
        4. The finalizer is detached (cleanup is complete)

        This prevents resource leaks and file locking issues, especially
        on Windows where SQLite connections can prevent file deletion.

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
            # stop() now handles thread joining, cleanup, and finalizer detaching
            self.stop()
        except Exception as e:
            if _config["BROKER_LOGGING_ENABLED"]:
                logger.warning(f"Error during stop in __exit__: {e}")

    def run_forever(self) -> None:
        """Start watching the queue. This method blocks until stop() is called
        or a SIGINT (Ctrl-C) is received.
        """
        signal_context = None

        try:
            # Only install signal handler if we're in the main thread
            if threading.current_thread() is threading.main_thread():
                signal_context = SignalHandlerContext(
                    signal.SIGINT,
                    self._sigint_handler,
                )
                signal_context.__enter__()

            retry_count = 0
            max_retries = 3
            start_time = time.time()

            while retry_count < max_retries:
                # Check absolute timeout
                if time.time() - start_time > MAX_TOTAL_RETRY_TIME:
                    msg = (
                        f"Watcher retry timeout exceeded ({MAX_TOTAL_RETRY_TIME}s). "
                        f"Retries: {retry_count}, Time elapsed: {time.time() - start_time:.1f}s"
                    )
                    raise TimeoutError(
                        msg,
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

                        # Check stop before processing
                        self._check_stop()

                        # Two-phase detection: check if we actually have messages
                        if not getattr(self, "_skip_idle_check", False):
                            if not self._has_pending_messages(
                                self._get_db_with_retry(),
                            ):
                                # No messages for this queue, skip drain
                                continue

                        # Always try to drain the queue first; this guarantees
                        # that a stop request does not prevent us from
                        # finishing already-visible work, so connections can
                        # be closed and no messages get lost.
                        self._drain_queue()

                    # If we get here, we exited normally
                    break

                except _StopLoop:
                    # Normal shutdown
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.exception(
                            f"Watcher failed after {max_retries} retries. Last error: {e}",
                        )
                        raise
                    wait_time = 2**retry_count  # Exponential backoff
                    logger.debug(
                        f"Watcher error (retry {retry_count}/{max_retries}): {e}. "
                        f"Retrying in {wait_time} seconds...",
                    )
                    if not interruptible_sleep(wait_time, self._stop_event):
                        # Sleep was interrupted, exit retry loop
                        logger.info("Watcher retry interrupted by stop signal")
                        break
                    # Clean up before retry
                    with contextlib.suppress(Exception):
                        self._cleanup_thread_local()

        finally:
            # Clean up thread-local connections
            self._cleanup_thread_local()

            # Restore original signal handler
            if signal_context is not None:
                signal_context.__exit__(None, None, None)

    def run_in_thread(self) -> threading.Thread:
        """Start the watcher in a new background thread.

        IMPORTANT: You MUST call stop() when done!
        -----------------------------------------
        This method starts a background thread that will continue running
        until explicitly stopped. The thread is marked as daemon=True as
        a safety measure, but you should NOT rely on this for cleanup.

        Proper usage patterns:

        1. With context manager (RECOMMENDED):
            with QueueWatcher(db, queue, handler) as watcher:
                # Thread is started automatically
                # ... do work ...
            # Thread is stopped automatically

        2. Manual management:
            watcher = QueueWatcher(db, queue, handler)
            thread = watcher.run_in_thread()
            try:
                # ... do work ...
            finally:
                watcher.stop()  # CRITICAL: Always call stop()!

        3. Don't do this (resource leak):
            watcher = QueueWatcher(db, queue, handler)
            watcher.run_in_thread()  # Thread starts
            # Program exits without calling stop() - BAD!

        Why daemon=True is not enough:
        - Daemon threads are killed abruptly when the program exits
        - Database connections may not be closed properly
        - File locks may persist (especially on Windows)
        - No guarantee of processing the final message

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
        # Store weak reference for the finalizer
        self._thread = weakref.ref(thread)
        return thread

    def _sigint_handler(self, signum: int, frame: Any) -> None:
        """Convert SIGINT to graceful shutdown."""
        # When handling SIGINT in run_forever(), we're in the main thread
        # and there's no separate thread to join, so set join=False
        self.stop(join=False)

    def __del__(self) -> None:
        """Safety net to stop watcher if garbage collected while running."""
        with contextlib.suppress(Exception):
            self.stop()

    def _has_pending_messages(self, db: BrokerDB) -> bool:
        """Fast check if queue has unclaimed messages with retry on operational errors."""

        def check_func() -> bool:
            sql = "SELECT EXISTS(SELECT 1 FROM messages WHERE queue = ? AND claimed = 0"
            params: List[Any] = [self._queue]

            if self._last_seen_ts:
                sql += " AND ts > ?"
                params.append(self._last_seen_ts)

            sql += " LIMIT 1)"
            rows = list(db._runner.run(sql, tuple(params), fetch=True))
            return bool(rows[0][0]) if rows else False

        return bool(self._process_with_retry(check_func, "pending_messages_check"))

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
        # Get database connection with retry
        db = self._get_db_with_retry()

        # Process messages based on mode
        found_messages = False
        if self._peek:
            found_messages = self._drain_peek_mode(db)
        else:
            found_messages = self._drain_consume_mode(db)

        # Notify strategy if we found messages
        if found_messages:
            self._strategy.notify_activity()

    def _drain_peek_mode(self, db: BrokerDB) -> bool:
        """Process messages in peek mode (doesn't remove from queue)."""
        return bool(
            self._process_with_retry(lambda: self._process_peek_messages(db), "peek")
        )

    def _drain_consume_mode(self, db: BrokerDB) -> bool:
        """Process messages in consume mode (removes from queue)."""
        if not self._batch_processing:
            return self._process_single_message(db)
        return self._process_batch_messages(db)

    def _process_peek_messages(self, db: BrokerDB) -> bool:
        """Process messages without removing them from queue."""
        found_messages = False

        for body, ts in db.stream_read_with_timestamps(
            self._queue,
            all_messages=self._batch_processing,
            peek=True,
            commit_interval=1,
            since_timestamp=self._last_seen_ts,
        ):
            if self._try_dispatch_message(body, ts):
                # Only update timestamp after successful dispatch
                self._last_seen_ts = max(self._last_seen_ts, ts)
                found_messages = True

            # Stop after first message if not batch processing
            if not self._batch_processing:
                break

        return found_messages

    def _process_single_message(self, db: BrokerDB) -> bool:
        """Process exactly one message in consume mode."""
        return bool(
            self._process_with_retry(
                lambda: self._consume_one_message(db),
                "consume",
            )
        )

    def _consume_one_message(self, db: BrokerDB) -> bool:
        """Consume and process a single message."""
        for body, ts in db.stream_read_with_timestamps(
            self._queue,
            all_messages=False,
            peek=False,
            commit_interval=1,
        ):
            self._try_dispatch_message(body, ts)
            return True  # Found and processed one message

        return False  # No messages found

    def _process_batch_messages(self, db: BrokerDB) -> bool:
        """Process all available messages in batch mode."""
        return bool(
            self._process_with_retry(
                lambda: self._consume_all_messages(db),
                "batch consume",
            )
        )

    def _consume_all_messages(self, db: BrokerDB) -> bool:
        """Consume and process all available messages."""
        found_any = False

        while True:
            self._check_stop()
            found_this_iteration = False

            for body, ts in db.stream_read_with_timestamps(
                self._queue,
                all_messages=True,
                peek=False,
                commit_interval=1,
            ):
                self._try_dispatch_message(body, ts)
                found_any = True
                found_this_iteration = True

            # No more messages, exit loop
            if not found_this_iteration:
                break

        return found_any

    def _try_dispatch_message(self, body: str, timestamp: int) -> bool:
        """Try to dispatch a message, return True if successful."""
        try:
            self._dispatch(body, timestamp)
            return True
        except _StopLoop:
            raise  # Re-raise stop signal
        except Exception:
            # Don't update timestamp if dispatch failed in peek mode
            # This ensures we'll retry the message next time
            return False

    def _dispatch(self, message: str, timestamp: int) -> None:
        """Dispatch a message to the handler with error handling and size validation."""
        # Validate message size (10MB limit)
        message_size = len(message.encode("utf-8"))
        if message_size > MAX_MESSAGE_SIZE:
            error_msg = f"Message size ({message_size} bytes) exceeds {MAX_MESSAGE_SIZE // (1024 * 1024)}MB limit"
            if _config["BROKER_LOGGING_ENABLED"]:
                logger.error(error_msg)
            # Use parent's error handler
            self._handle_handler_error(
                ValueError(error_msg),
                message[:1000] + "...",
                timestamp,
                self._error_handler,
            )
            return

        try:
            self._handler(message, timestamp)
        except Exception as e:
            # Use parent's error handler
            self._handle_handler_error(e, message, timestamp, self._error_handler)


class QueueMoveWatcher(BaseWatcher):
    """Watches a source queue and atomically moves messages to a destination queue.

    The move happens atomically BEFORE the handler is called, ensuring that
    messages are safely moved even if the handler fails. The handler receives
    the message for observation purposes only.

    IMPORTANT: Resource Cleanup Requirements
    ---------------------------------------
    This class inherits from QueueWatcher and has the same cleanup requirements:
    - Always call stop() when done
    - Use context managers when possible
    - Ensure threads are joined before program exit

    Example usage:
        # Context manager (recommended)
        with QueueMoveWatcher(db, "inbox", "processed", handler) as watcher:
            # Moves messages for 60 seconds
            time.sleep(60)
        # Thread stopped and resources cleaned up automatically

        # Manual management
        watcher = QueueMoveWatcher(db, "inbox", "processed", handler)
        thread = watcher.run_in_thread()
        try:
            # Process until max_messages reached or stopped
            thread.join()
        finally:
            watcher.stop()  # Ensure cleanup even if join times out

    The same warnings apply as for QueueWatcher - not calling stop() will
    lead to thread leaks, database connection leaks, and file locking issues
    on Windows.
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
        error_handler: Callable[[Exception, str, int], bool | None] | None = None,
        stop_event: threading.Event | None = None,
        max_messages: int | None = None,
    ) -> None:
        """Initialize a QueueMoveWatcher.

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
            msg = "Cannot move messages to the same queue"
            raise ValueError(msg)

        # Initialize parent class
        super().__init__(broker, stop_event)

        # Store move-specific attributes
        self._source_queue = source_queue
        self._dest_queue = dest_queue
        self._move_count = 0
        self._max_messages = max_messages

        # Store handlers
        self._handler = handler
        self._error_handler = error_handler

        # Create polling strategy directly
        self._strategy = PollingStrategy(
            stop_event=self._stop_event,
            initial_checks=initial_checks,
            max_interval=max_interval,
            burst_sleep=_config["SIMPLEBROKER_BURST_SLEEP"],
        )

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

        IMPORTANT: You MUST call stop() when done!
        See the warnings in run_in_thread() and stop() for details.

        Returns:
            The thread running the watcher.

        """
        return self.run_in_thread()

    def run(self) -> None:
        """Run the move watcher synchronously until max_messages or stop_event.

        This is a convenience method that calls run_forever().

        This method blocks until:
        - max_messages have been moved (if specified)
        - stop() is called from another thread
        - SIGINT (Ctrl+C) is received (if in main thread)

        No additional cleanup is needed after this method returns, as it
        runs synchronously in the current thread.
        """
        self.run_forever()

    def run_forever(self) -> None:
        """Run the move watcher continuously in the current thread."""
        # Start the polling strategy
        if hasattr(self._strategy, "start"):
            self._strategy.start(self._get_db())

        # Main polling loop
        try:
            while not self._stop_event.is_set():
                self._strategy.wait_for_activity()
                if self._stop_event.is_set():
                    break

                try:
                    self._drain_queue()
                except _StopLoop:
                    break
        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully
            logger.info("Received interrupt signal, stopping...")
            self.stop(join=False)

    def run_in_thread(self) -> threading.Thread:
        """Start the move watcher in a new background thread."""
        thread = threading.Thread(target=self.run_forever, daemon=True)
        thread.start()
        # Store reference for the finalizer
        self._thread = weakref.ref(thread)
        return thread

    def _drain_queue(self) -> None:
        """Move ALL messages from source to destination queue."""
        # Get database connection with retry
        db = self._get_db_with_retry()

        # Process messages with retry
        found_messages = self._process_with_retry(
            lambda: self._move_all_messages(db),
            "move",
        )

        # Notify strategy if we found messages
        if found_messages:
            self._strategy.notify_activity()

    def _move_all_messages(self, db: BrokerDB) -> bool:
        """Move all available messages atomically."""
        found_any = False

        while True:
            self._check_stop()

            # Atomic move operation
            result = db.move(
                self._source_queue,
                self._dest_queue,
                require_unclaimed=True,
            )

            if result is None:
                break  # No more messages

            found_any = True
            self._move_count += 1

            # Notify handler about the move
            self._notify_handler_safe(result)

            # Check max messages limit
            if self._max_messages and self._move_count >= self._max_messages:
                if _config["BROKER_LOGGING_ENABLED"]:
                    logger.info(f"Reached max_messages limit ({self._max_messages})")
                self._stop_event.set()
                raise _StopLoop

        return found_any

    def _notify_handler_safe(self, move_result: dict) -> None:
        """Safely notify handler about moved message."""
        try:
            self._handler(move_result["body"], move_result["ts"])
        except Exception as e:
            # Move already completed, just handle notification error
            self._handle_handler_error(
                e,
                move_result["body"],
                move_result["ts"],
                self._error_handler,
            )
