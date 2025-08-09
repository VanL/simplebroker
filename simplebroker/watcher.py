"""Light-weight queue watcher for SimpleBroker.

This module provides an efficient polling mechanism to consume or monitor
queues with minimal overhead and fast response times.

IMPORTANT FOR PEOPLE SUBCLASSING/USING API: Proper Resource Cleanup
-------------------------------------------------------------------
Watchers create background threads and database connections that must be
properly cleaned up to avoid resource leaks, especially on Windows where
file locking is strict. Always use one of these patterns:

1. Context Manager (RECOMMENDED - automatic cleanup):
    from simplebroker import Queue
    queue = Queue("tasks", persistent=True)
    with QueueWatcher(queue, handler) as watcher:
        # Thread starts automatically in __enter__
        time.sleep(60)  # Do work
    # Thread is stopped and joined automatically in __exit__

2. Manual Management (ensure stop() is called):
    from simplebroker import Queue
    queue = Queue("tasks", persistent=True)
    watcher = QueueWatcher(queue, handler)
    thread = watcher.run_in_thread()
    try:
        # Do work
    finally:
        watcher.stop()  # This joins the thread by default, ensuring cleanup

3. Signal Handling (for long-running services):
    import signal
    from simplebroker import Queue
    queue = Queue("tasks", persistent=True)
    watcher = QueueWatcher(queue, handler)

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
    from simplebroker import Queue
    from simplebroker.watcher import QueueWatcher

    def handle(msg: str, ts: int) -> None:
        print(f"got message @ {ts}: {msg}")

    # Create a persistent queue (required for watchers)
    queue = Queue("orders", persistent=True)
    watcher = QueueWatcher(queue, handle)
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
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, NamedTuple, Optional

# For Python 3.8 compatibility, we avoid using Self type
# and use string forward references instead
from ._constants import MAX_MESSAGE_SIZE, MAX_TOTAL_RETRY_TIME, load_config
from ._exceptions import OperationalError
from .db import BrokerDB
from .helpers import interruptible_sleep
from .sbqueue import Queue

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

        # Set up automatic cleanup finalizer
        self._setup_finalizer()

    def _get_queue_for_data_version(self) -> Optional[Queue]:
        """Get the Queue object for data version checks.

        Returns the Queue object if available, None otherwise.
        Subclasses with Queue objects should override this.
        """
        if hasattr(self, "_queue_obj"):
            return self._queue_obj  # type: ignore[no-any-return]
        elif hasattr(self, "_source_queue_obj"):
            return self._source_queue_obj  # type: ignore[no-any-return]
        return None

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
            self._cleanup_thread_local()

        # detach finalizer if it exists - resources are already released
        if hasattr(self, "_finalizer"):
            self._finalizer.detach()

    def _cleanup_thread_local(self) -> None:
        """Clean up thread-local database connections.

        Delegates to Queue's cleanup_connections method for proper cleanup.
        This method is called during shutdown and error recovery.
        """
        # Delegate to Queue's cleanup method
        if hasattr(self, "_queue_obj"):
            self._queue_obj.cleanup_connections()
        elif hasattr(self, "_source_queue_obj"):
            self._source_queue_obj.cleanup_connections()

    def is_running(self) -> bool:
        """Check if the watcher is currently running."""
        return not self._stop_event.is_set()

    @abstractmethod
    def _drain_queue(self) -> None:
        """Process messages - must be implemented by subclasses."""

    def _setup_signal_handler(self) -> Optional[SignalHandlerContext]:
        """Set up signal handler for SIGINT if in main thread.

        Returns:
            SignalHandlerContext if handler was set up, None otherwise
        """
        if threading.current_thread() is threading.main_thread():
            signal_context = SignalHandlerContext(
                signal.SIGINT,
                self._sigint_handler,
            )
            signal_context.__enter__()
            return signal_context
        return None

    def _check_retry_timeout(self, start_time: float, retry_count: int) -> None:
        """Check if retry timeout has been exceeded.

        Args:
            start_time: Time when retries started
            retry_count: Current retry attempt number

        Raises:
            TimeoutError: If maximum retry time exceeded
        """
        elapsed = time.monotonic() - start_time
        if elapsed > MAX_TOTAL_RETRY_TIME:
            msg = (
                f"Watcher retry timeout exceeded ({MAX_TOTAL_RETRY_TIME}s). "
                f"Retries: {retry_count}, Time elapsed: {elapsed:.1f}s"
            )
            raise TimeoutError(msg)

    def _process_messages(self) -> None:
        """Process messages from the queue.

        This is the main message processing loop that:
        1. Waits for activity (if strategy exists)
        2. Checks for pending messages (if applicable)
        3. Drains the queue

        Subclasses can override this for custom processing logic.
        """
        # Default implementation for base watchers
        # QueueWatcher will override this with its specific logic
        while True:
            # Check stop before processing
            self._check_stop()

            # Drain the queue
            self._drain_queue()

    def _handle_retry(
        self,
        e: Exception,
        retry_count: int,
        max_retries: int,
    ) -> bool:
        """Handle retry logic when an error occurs.

        Args:
            e: The exception that occurred
            retry_count: Current retry attempt number
            max_retries: Maximum number of retries allowed

        Returns:
            True if should continue retrying, False otherwise

        Raises:
            Exception: Re-raises the exception if max retries exceeded
        """
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
            return False

        # Clean up before retry
        with contextlib.suppress(Exception):
            self._cleanup_thread_local()

        return True

    def _run_with_retries(self, max_retries: int = 3) -> None:
        """Run the watcher with retry logic.

        Args:
            max_retries: Maximum number of retry attempts
        """
        retry_count = 0
        start_time = time.monotonic()

        while retry_count < max_retries:
            # Check absolute timeout
            self._check_retry_timeout(start_time, retry_count)

            try:
                # Initialize strategy with data version getter
                if hasattr(self, "_strategy") and hasattr(self._strategy, "start"):
                    queue = self._get_queue_for_data_version()
                    if queue:
                        # Capture queue in closure to avoid B023 warning
                        self._strategy.start(lambda q=queue: q.get_data_version())
                    else:
                        # Fallback: create temporary BrokerDB for data version
                        db = BrokerDB(str(self._db_arg))
                        # Capture db in closure to avoid B023 warning
                        self._strategy.start(lambda d=db: d.get_data_version())

                # Initial drain of existing messages
                self._drain_queue()

                # Main processing loop
                self._process_messages()

                # If we get here, we exited normally
                break

            except _StopLoop:
                # Normal shutdown
                break
            except Exception as e:
                retry_count += 1
                if not self._handle_retry(e, retry_count, max_retries):
                    break

    def _sigint_handler(self, signum: int, frame: Any) -> None:
        """Convert SIGINT to graceful shutdown.

        Can be overridden by subclasses for custom handling.
        """
        # Default implementation - just stop
        logger.info("Received SIGINT, stopping watcher...")
        self.stop(join=False)

    def _safe_call_handler(
        self,
        message: str,
        timestamp: int,
        error_handler: Callable[[Exception, str, int], bool | None] | None = None,
    ) -> None:
        """Safely call the handler with error handling.

        Args:
            message: Message body to pass to handler
            timestamp: Timestamp to pass to handler
            error_handler: Optional error handler for exceptions
        """
        try:
            if hasattr(self, "_handler"):
                self._handler(message, timestamp)
        except Exception as e:
            self._handle_handler_error(e, message, timestamp, error_handler)

    def run_forever(self) -> None:
        """Run the watcher continuously until stopped.

        This method blocks until stop() is called or SIGINT is received.
        """
        signal_context = None

        try:
            # Set up signal handler if in main thread
            signal_context = self._setup_signal_handler()

            # Run the main loop with retries
            self._run_with_retries()

        finally:
            # Clean up thread-local connections
            self._cleanup_thread_local()

            # Restore original signal handler
            if signal_context is not None:
                signal_context.__exit__(None, None, None)

    def run_in_thread(self) -> threading.Thread:
        """Start the watcher in a new background thread.

        Returns:
            The thread running the watcher
        """
        thread = threading.Thread(target=self.run_forever, daemon=True)
        thread.start()
        # Store weak reference for the finalizer
        self._thread = weakref.ref(thread)
        return thread

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
        try:
            self.stop()
        except Exception as e:
            if _config["BROKER_LOGGING_ENABLED"]:
                logger.warning(f"Error during stop in __exit__: {e}")

    def _setup_finalizer(self) -> None:
        """Set up automatic cleanup finalizer.

        This provides a safety net for resource cleanup if the watcher
        is garbage collected without proper shutdown. This is especially
        important on Windows where open file handles prevent TemporaryDirectory
        from removing .db files.

        WARNING: This is a safety net, NOT a replacement for proper cleanup!
        --------------------------------------------------------------------
        The finalizer runs during garbage collection, which is:
        - Non-deterministic (might not run immediately)
        - Not guaranteed (might not run at all in some cases)
        - Too late (resources held longer than necessary)

        Always use context managers or call stop() explicitly!
        """

        def _auto_cleanup(wref: weakref.ReferenceType[BaseWatcher]) -> None:
            """Automatic cleanup function called by finalizer.

            If user code forgets to call stop() / join the thread, the watcher
            object will eventually be garbage-collected. This finalizer ensures
            the background thread is stopped and joined and that the thread-local
            BrokerDB is closed, so every SQLite connection is released before
            the temp directory is removed.
            """
            obj = wref()
            if obj is None:  # already GC'ed
                return

            # Try to stop the watcher
            with contextlib.suppress(Exception):
                obj.stop()

            # Try to join the thread if it exists
            thr = getattr(obj, "_thread", None)
            if thr is not None:
                thread = thr() if isinstance(thr, weakref.ref) else thr
                if isinstance(thread, threading.Thread) and thread.is_alive():
                    try:
                        thread.join(timeout=1.0)  # don't hang indefinitely
                    except Exception:
                        pass

            # Ensure the per-thread BrokerDB is closed
            with contextlib.suppress(Exception):
                obj._cleanup_thread_local()

        self._finalizer = weakref.finalize(self, _auto_cleanup, weakref.ref(self))

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
        self._data_version_provider: Callable[[], int | None] | None = None
        self._pragma_failures = 0

    def wait_for_activity(self) -> None:
        """Wait for activity with optimized polling."""
        # Check data version first for immediate activity detection
        if self._data_version_provider and self._check_data_version():
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

    def start(
        self, data_version_provider: Callable[[], int | None] | None = None
    ) -> None:
        """Initialize the strategy."""
        self._data_version_provider = data_version_provider
        self._check_count = 0
        self._data_version = None

    def _get_delay(self) -> float:
        """Calculate delay based on check count."""
        base_delay = self._calculate_base_delay()

        if base_delay > 0:
            # Add +/-15% jitter to prevent synchronized polling
            jitter_factor = _config["BROKER_JITTER_FACTOR"]
            jittered_delay = (
                random.uniform(-jitter_factor, jitter_factor) + 1
            ) * base_delay
            return max(0, jittered_delay)

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
            if self._data_version_provider is None:
                return False

            version = self._data_version_provider()
            if version is None:
                return False

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
        queue: Queue | str | Path,  # Can be Queue, queue name, or db path (legacy)
        handler_or_queue: Callable[[str, int], None] | str | None = None,
        handler: Optional[Callable[[str, int], None]] = None,
        *,
        db_path: Optional[str | Path] = None,
        peek: bool = False,
        initial_checks: int = 100,
        max_interval: float = 0.1,
        error_handler: Callable[[Exception, str, int], bool | None] | None = None,
        since_timestamp: int | None = None,
        batch_processing: bool = False,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Initializes the watcher.

        Supports two calling conventions:
        1. New style: QueueWatcher(queue, handler, ...)
           where queue is a Queue object or string queue name
        2. Legacy style: QueueWatcher(db_path, queue_name, handler, ...)
           for backwards compatibility

        Parameters
        ----------
        queue : Union[Queue, str, Path]
            Either a Queue object, a string queue name, or a database path (legacy).
            If a string queue name is provided, a Queue object will be created with persistent=True.
            If a Queue object is provided, persistent mode is recommended for better performance
            but not required.
        handler_or_queue : Union[Callable, str, None], optional
            In new style: the handler function.
            In legacy style: the queue name (string).
        handler : Callable[[str, int], None], optional
            In legacy style: the handler function.
            In new style: not used (pass handler as second argument).
        db_path : Optional[Union[str, Path]], optional
            Path to the SQLite database. Only used when queue is a string queue name.
            If not provided when queue is a string, uses the default database.
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

        Raises
        ------
        TypeError
            If handler or error_handler are not callable.

        Examples
        --------
        >>> from simplebroker import Queue
        >>>
        >>> # Method 1: Pass a Queue object (recommended for control)
        >>> queue = Queue("tasks", persistent=True)
        >>> watcher = QueueWatcher(queue, process_task)
        >>>
        >>> # Method 2: Pass a queue name (automatic persistent mode)
        >>> watcher = QueueWatcher("tasks", process_task)
        >>>
        >>> # Method 3: Pass a queue name with custom database path
        >>> watcher = QueueWatcher("tasks", process_task, db_path="/path/to/db.sqlite")
        >>>
        >>> # Method 4: Legacy style (for backwards compatibility)
        >>> watcher = QueueWatcher("/path/to/db.sqlite", "tasks", process_task)
        >>>
        >>> def process_task(message: str, timestamp: int):
        ...     print(f"Processing: {message} (received at {timestamp})")
        >>> watcher.run_forever()  # Blocks until stopped

        """
        # Detect which calling convention is being used
        if handler_or_queue is not None and handler is not None:
            # Legacy style: QueueWatcher(db_path, queue_name, handler, ...)
            # queue is actually the db_path, handler_or_queue is the queue name
            actual_db_path = queue  # First arg is db path in legacy style
            actual_queue_name = handler_or_queue  # Second arg is queue name
            actual_handler = handler  # Third arg is handler

            # Type checking: ensure handler_or_queue is a string in legacy mode
            if not isinstance(actual_queue_name, str):
                msg = f"queue name must be a string in legacy mode, got {type(actual_queue_name).__name__}"
                raise TypeError(msg)

            # Type checking: ensure actual_db_path is str, Path, or BrokerDB
            if not isinstance(actual_db_path, (str, Path, BrokerDB)):
                msg = f"db_path must be a string, Path, or BrokerDB in legacy mode, got {type(actual_db_path).__name__}"
                raise TypeError(msg)

            # Type checking: ensure handler is callable
            if not callable(actual_handler):
                msg = f"handler must be callable, got {type(actual_handler).__name__}"
                raise TypeError(msg)

            # Create a Queue object with the legacy parameters
            from .sbqueue import Queue as QueueClass

            if isinstance(actual_db_path, BrokerDB):
                # Handle BrokerDB instance for backward compatibility
                self._queue_obj = QueueClass(
                    actual_queue_name,
                    db_path=str(actual_db_path.db_path),
                    persistent=True,
                )
            else:
                self._queue_obj = QueueClass(
                    actual_queue_name, db_path=str(actual_db_path), persistent=True
                )
            # For backward compatibility, store queue name as _queue
            self._queue = actual_queue_name
            self._queue_name = actual_queue_name
            self._handler = actual_handler
        else:
            # New style: QueueWatcher(queue, handler, ...)
            # Validate handler exists and is callable
            if handler_or_queue is None:
                msg = "handler is required in new style calling convention"
                raise TypeError(msg)
            if isinstance(handler_or_queue, str):
                msg = "handler must be callable, got string (possible legacy calling convention?)"
                raise TypeError(msg)
            if not callable(handler_or_queue):
                msg = f"handler must be callable, got {type(handler_or_queue).__name__}"
                raise TypeError(msg)
            # At this point, handler_or_queue is guaranteed to be callable
            self._handler = handler_or_queue

            # Handle queue parameter - either Queue object or string name
            if isinstance(queue, Queue):
                self._queue_obj = queue
                self._queue_name = queue.name
            else:
                # Create a Queue object with persistent=True by default for watchers
                from .sbqueue import Queue as QueueClass

                # Ensure queue is a string for Queue constructor
                if not isinstance(queue, str):
                    # If it's a Path, convert to string
                    queue_name = str(queue)
                else:
                    queue_name = queue
                # Convert db_path to string if it's a Path
                if db_path is not None:
                    self._queue_obj = QueueClass(
                        queue_name, db_path=str(db_path), persistent=True
                    )
                else:
                    # Use default DB path
                    from ._constants import DEFAULT_DB_NAME

                    self._queue_obj = QueueClass(
                        queue_name, db_path=DEFAULT_DB_NAME, persistent=True
                    )
                self._queue_name = queue_name
            # For backward compatibility with tests, store queue name as _queue
            self._queue = self._queue_name

        # Initialize parent class with the queue's db path
        super().__init__(self._queue_obj._db_path, stop_event)
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
        # Finalizer setup is handled by BaseWatcher.__init__()
        # The base class sets up automatic cleanup to ensure:
        # - Background threads are stopped
        # - Thread-local database connections are closed
        # - Resources are released before garbage collection

    # __enter__ and __exit__ are inherited from BaseWatcher

    def _process_messages(self) -> None:
        """Override of base class method with QueueWatcher-specific logic.

        This is the main message processing loop that:
        1. Waits for activity
        2. Checks for pending messages
        3. Drains the queue
        """
        while True:
            # Wait until something might have happened
            self._strategy.wait_for_activity()

            # Check stop before processing
            self._check_stop()

            # Two-phase detection: check if we actually have messages
            if not getattr(self, "_skip_idle_check", False):
                if not self._has_pending_messages(None):
                    # No messages for this queue, skip drain
                    continue

            # Always try to drain the queue first; this guarantees
            # that a stop request does not prevent us from
            # finishing already-visible work, so connections can
            # be closed and no messages get lost.
            self._drain_queue()

    # run_in_thread is inherited from BaseWatcher

    def _sigint_handler(self, signum: int, frame: Any) -> None:
        """Override of base class method with QueueWatcher-specific handling."""
        # When handling SIGINT in run_forever(), we're in the main thread
        # and there's no separate thread to join, so set join=False
        self.stop(join=False)

    # __del__ is inherited from BaseWatcher

    def _has_pending_messages(self, db: BrokerDB | None = None) -> bool:
        """Fast check if queue has unclaimed messages.

        Uses the Queue's has_pending method with retry logic for operational error handling.

        Args:
            db: Optional database connection for backward compatibility.
                Not used in the current implementation.
        """

        def check_func() -> bool:
            return self._queue_obj.has_pending(
                self._last_seen_ts if self._last_seen_ts > 0 else None
            )

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
        # Process messages based on mode using Queue API
        found_messages = False
        if self._peek:
            found_messages = self._drain_peek_mode()
        else:
            found_messages = self._drain_consume_mode()

        # Notify strategy if we found messages
        if found_messages:
            self._strategy.notify_activity()

    def _drain_peek_mode(self) -> bool:
        """Process messages in peek mode (doesn't remove from queue)."""
        return bool(
            self._process_with_retry(lambda: self._process_peek_messages(), "peek")
        )

    def _drain_consume_mode(self) -> bool:
        """Process messages in consume mode (removes from queue)."""
        if not self._batch_processing:
            return self._process_single_message()
        return self._process_batch_messages()

    def _process_peek_messages(self) -> bool:
        """Process messages without removing them from queue."""
        found_messages = False

        for body, ts in self._queue_obj.stream_messages(
            peek=True,
            all_messages=self._batch_processing,
            since_timestamp=self._last_seen_ts,
            commit_interval=1,
        ):
            if self._try_dispatch_message(body, ts):
                # Only update timestamp after successful dispatch
                self._last_seen_ts = max(self._last_seen_ts, ts)
                found_messages = True

            # Stop after first message if not batch processing
            if not self._batch_processing:
                break

        return found_messages

    def _process_single_message(self) -> bool:
        """Process exactly one message in consume mode."""
        return bool(
            self._process_with_retry(
                lambda: self._consume_one_message(),
                "consume",
            )
        )

    def _consume_one_message(self) -> bool:
        """Consume and process a single message."""
        result = self._queue_obj.read_one(with_timestamps=True)
        if result:
            # Type narrowing: with_timestamps=True always returns tuple
            if isinstance(result, tuple):
                body, ts = result
                self._try_dispatch_message(body, ts)
                return True  # Found and processed one message
        return False  # No messages found

    def _process_batch_messages(self) -> bool:
        """Process all available messages in batch mode."""
        return bool(
            self._process_with_retry(
                lambda: self._consume_all_messages(),
                "batch consume",
            )
        )

    def _consume_all_messages(self) -> bool:
        """Consume and process all available messages."""
        found_any = False

        while True:
            self._check_stop()
            found_this_iteration = False

            for body, ts in self._queue_obj.stream_messages(
                peek=False,
                all_messages=True,
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

        # Use base class safe handler method
        self._safe_call_handler(message, timestamp, self._error_handler)


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

        # Create Queue object for source queue (ephemeral mode for watchers)
        self._source_queue_obj = Queue(
            source_queue, db_path=str(self._db_arg), persistent=False
        )

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

    def _process_messages(self) -> None:
        """Override of base class method with QueueMoveWatcher-specific logic.

        Simplified processing loop for move operations.
        """
        while not self._stop_event.is_set():
            self._strategy.wait_for_activity()
            if self._stop_event.is_set():
                break

            try:
                self._drain_queue()
            except _StopLoop:
                break

    # run_forever and run_in_thread are inherited from BaseWatcher

    def _drain_queue(self) -> None:
        """Move ALL messages from source to destination queue."""
        # Process messages with retry
        found_messages = self._process_with_retry(
            lambda: self._move_all_messages(),
            "move",
        )

        # Notify strategy if we found messages
        if found_messages:
            self._strategy.notify_activity()

    def _move_all_messages(self) -> bool:
        """Move all available messages atomically."""
        found_any = False

        while True:
            self._check_stop()

            # Use Queue.move() to move single oldest message
            # Returns dict with 'message' and 'timestamp' or None
            result = self._source_queue_obj.move(self._dest_queue)

            if not result:
                break  # No more messages

            found_any = True
            self._move_count += 1

            # Notify handler about the move
            # Result is a dict containing 'message' and 'timestamp'
            # Type assertion: move() without args returns dict or None
            assert isinstance(result, dict)
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
        # Use base class safe handler method
        # move_result has 'message' and 'timestamp' keys
        self._safe_call_handler(
            move_result["message"],
            move_result["timestamp"],
            self._error_handler,
        )

    def stop(self, *, join: bool = True, timeout: float = 5.0) -> None:
        """Stop the move watcher and clean up resources.

        This extends the base class stop() to also clean up the Queue object.

        Args:
            join: If True, wait for thread to finish
            timeout: Timeout for joining thread
        """
        # Clean up Queue object
        if hasattr(self, "_source_queue_obj"):
            self._source_queue_obj.cleanup_connections()
            self._source_queue_obj.close()

        # Call parent stop() for base cleanup
        super().stop(join=join, timeout=timeout)
