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
import json
import logging
import random
import signal
import threading
import time
import weakref
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

# For Python 3.8 compatibility, we avoid using Self type
# and use string forward references instead
from ._constants import (
    DEFAULT_DB_NAME,
    MAX_MESSAGE_SIZE,
    MAX_TOTAL_RETRY_TIME,
    load_config,
)
from ._exceptions import OperationalError
from .db import BrokerDB
from .helpers import interruptible_sleep
from .sbqueue import Queue

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "Message",
    "QueueMoveWatcher",
    "QueueWatcher",
    "simple_print_handler",
    "json_print_handler",
    "logger_handler",
    "default_error_handler",
]

_config = load_config()


# Default message handlers for common use cases
def simple_print_handler(msg: str, ts: int) -> None:
    """Simple handler that prints messages with timestamps.

    This is the most basic handler that outputs each message with its unique
    timestamp ID. Useful for debugging and simple monitoring scenarios.

    Args:
        msg: Message content
        ts: Message timestamp (unique 64-bit hybrid timestamp ID)

    Example:
        >>> simple_print_handler("Hello World", 1837025672140161024)
        [1837025672140161024] Hello World
    """
    print(f"[{ts}] {msg}")


def json_print_handler(msg: str, ts: int) -> None:
    """Handler that outputs messages in JSON format.

    Outputs each message as a JSON object with 'message' and 'timestamp' fields.
    This format is safe for processing with tools like jq and handles messages
    containing newlines or special characters correctly.

    Args:
        msg: Message content
        ts: Message timestamp (unique 64-bit hybrid timestamp ID)

    Example:
        >>> json_print_handler("Hello\\nWorld", 1837025672140161024)
        {"message": "Hello\\nWorld", "timestamp": 1837025672140161024}
    """
    print(json.dumps({"message": msg, "timestamp": ts}, ensure_ascii=False))


def logger_handler(msg: str, ts: int) -> None:
    """Handler that logs messages using Python's logging system.

    Logs each message at INFO level using the 'simplebroker.watcher' logger.
    This integrates with your application's logging configuration and allows
    for proper log levels, formatting, and output destinations.

    Args:
        msg: Message content
        ts: Message timestamp (unique 64-bit hybrid timestamp ID)

    Example:
        >>> logger_handler("Processing order", 1837025672140161024)
        INFO:simplebroker.watcher:Message 1837025672140161024: Processing order
    """
    logger.info(f"Message {ts}: {msg}")


def default_error_handler(exc: Exception, message: str, timestamp: int) -> bool:
    """Default error handler that logs errors and continues processing.

    This is a clean, predictable error handler suitable for most applications.
    It logs errors at ERROR level and allows processing to continue. This handler
    always logs regardless of SimpleBroker's internal configuration settings,
    making it suitable for building custom error handlers.

    Args:
        exc: The exception raised by the message handler
        message: The message that caused the error
        timestamp: The message timestamp (unique 64-bit hybrid timestamp ID)

    Returns:
        True to continue processing (don't stop the watcher)

    Example:
        >>> # Use directly
        >>> watcher = QueueWatcher("tasks", handler, error_handler=default_error_handler)
        >>>
        >>> # Build custom handler using this as base
        >>> def custom_error_handler(exc, msg, ts):
        ...     print(f"Custom handling: {exc}")
        ...     return default_error_handler(exc, msg, ts)
    """
    logger.error(f"Handler error: {exc}")
    return True


def config_aware_default_error_handler(
    exc: Exception, message: str, timestamp: int, *, config: dict[str, Any] = _config
) -> bool:
    """Internal default error handler that respects BROKER_LOGGING_ENABLED.

    Used internally by SimpleBroker to maintain backward compatibility with
    users who have disabled logging via BROKER_LOGGING_ENABLED=0. For most
    applications, use default_error_handler directly instead.

    This function delegates to default_error_handler only when logging is enabled,
    preserving the existing behavior where users can suppress SimpleBroker's
    internal logging output.

    Args:
        exc: The exception raised by the message handler
        message: The message that caused the error
        timestamp: The message timestamp

    Returns:
        True to continue processing (don't stop the watcher)
    """
    if config["BROKER_LOGGING_ENABLED"]:
        return default_error_handler(exc, message, timestamp)
    return True


class Message(NamedTuple):
    """Message with metadata from the queue."""

    id: int
    body: str
    timestamp: int
    queue: str


# Create logger for this module
logger = logging.getLogger(__name__)

# Load configuration once at module level
config = load_config()


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

    def __init__(
        self,
        queue: str | Queue,
        *,
        db: BrokerDB | str | Path | None = None,
        stop_event: threading.Event | None = None,
        polling_strategy: PollingStrategy | None = None,
        config: dict[str, Any] = _config,
    ) -> None:
        """Initialize base watcher.

        Args:
            queue: Queue object or queue name
            db: Database instance or path (uses default if None)
            stop_event: Optional event to signal watcher shutdown
            polling_strategy: Custom polling strategy (uses default if None)
            config: Configuration dictionary (uses default if None)

        """
        # Handle queue parameter - either Queue object or string name
        if isinstance(queue, Queue):
            self._queue_obj = queue
        else:
            # Create Queue object with persistent=True by default for watchers
            if db is not None:
                if isinstance(db, BrokerDB):
                    db_path = str(db.db_path)
                else:
                    db_path = str(db)
            else:
                # Use default database
                db_path = DEFAULT_DB_NAME

            self._queue_obj = Queue(
                str(queue), db_path=db_path, persistent=True, config=config
            )

        # Event to signal the watcher to stop
        self._stop_event = stop_event or threading.Event()

        # Ensure underlying queue connections are aware of stop event
        if hasattr(self._queue_obj, "set_stop_event"):
            self._queue_obj.set_stop_event(self._stop_event)

        # Store configuration
        self._config = config

        # Weak reference to the thread running this watcher (for cleanup warnings)
        self._thread: weakref.ref[threading.Thread] | None = None

        # Thread-local storage for database connections
        self._thread_local = threading.local()

        # Track if we have a thread-local db to close
        self._has_thread_db = False

        # Thread-safe lock for stop synchronization
        self._stop_lock = threading.Lock()

        # Create or use provided polling strategy
        self._strategy = polling_strategy or self._create_strategy()

        # Initialize handler attributes (will be set by subclasses if not already set)
        if not hasattr(self, "_handler"):
            self._handler: Callable[[str, int], None] | None = None
        if not hasattr(self, "_error_handler"):
            self._error_handler: Callable[[Exception, str, int], bool | None] | None = (
                None
            )

        # Set up automatic cleanup finalizer
        self._setup_finalizer()

    def _get_queue_for_data_version(self) -> Queue:
        """Get the Queue object for data version checks.

        Returns the Queue object (always available in new API).
        """
        return self._queue_obj

    def _create_strategy(self, *, config: dict[str, Any] = _config) -> PollingStrategy:
        """Create the default polling strategy for this watcher.

        This method provides the default PollingStrategy configuration
        using environment-based settings. Subclasses can override this
        method to customize default behavior while preserving the ability
        to inject custom strategies via the polling_strategy parameter.

        Override Examples:
            class HighVolumeWatcher(QueueWatcher):
                def _create_strategy(self, *, config: dict[str, Any] = _config) -> PollingStrategy:
                    # Faster polling for high-volume scenarios
                    return PollingStrategy(
                        stop_event=self._stop_event,
                        initial_checks=1000,
                        max_interval=0.001,
                        burst_sleep=0.00001,
                        jitter_factor=0.15,
                    )

        Returns:
            PollingStrategy: Configured with environment settings from:
                - BROKER_INITIAL_CHECKS (default: 100)
                - BROKER_MAX_INTERVAL (default: 0.1)
                - BROKER_BURST_SLEEP (default: 0.0002)
                - BROKER_JITTER_FACTOR (default: 0.15 - 15%)

        See Also:
            PollingStrategy: For parameter details
            load_config(): For environment variable handling
        """
        return PollingStrategy(
            stop_event=self._stop_event,
            initial_checks=config["BROKER_INITIAL_CHECKS"],
            max_interval=config["BROKER_MAX_INTERVAL"],
            burst_sleep=config["BROKER_BURST_SLEEP"],
            jitter_factor=config["BROKER_JITTER_FACTOR"],
        )

    def _process_with_retry(
        self,
        process_func: Callable[[], Any],
        operation_name: str,
        *,
        config: dict[str, Any] = _config,
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
                    if config["BROKER_LOGGING_ENABLED"]:
                        logger.exception(
                            f"Failed after {max_retries} operational errors: {e}",
                        )
                    raise

                wait_time = self._calculate_retry_wait_time(attempt)
                if config["BROKER_LOGGING_ENABLED"]:
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
        error_handler: Callable[[Exception, str, int], bool | None],
        *,
        config: dict[str, Any] = _config,
    ) -> None:
        """Handle errors from message handler.

        Args:
            e: The exception that was raised
            message: The message being processed
            timestamp: The message timestamp
            error_handler: Error handler callback

        Raises:
            _StopLoop: If error handler returns False

        """
        stop_requested = False
        try:
            # Try calling with config first (for config-aware handlers)
            try:
                result = error_handler(e, message, timestamp, config=config)  # type: ignore[call-arg]
            except TypeError:
                # Fallback for handlers that don't accept config
                result = error_handler(e, message, timestamp)
            if result is False:
                # Error handler says stop
                stop_requested = True
            # True or None means continue
        except Exception as eh_error:
            # Error handler itself failed
            if config["BROKER_LOGGING_ENABLED"]:
                logger.exception(
                    f"Error handler failed: {eh_error}\nOriginal error: {e}",
                )

        # Raise _StopLoop outside the try block to avoid catching it
        if stop_requested:
            # Set stop event to ensure the watcher stops completely
            self._stop_event.set()
            raise _StopLoop from None

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
            # Notify strategy to wake up wait_for_activity
            if hasattr(self._strategy, "notify_activity"):
                self._strategy.notify_activity()  # Wake up wait_for_activity

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
        self._queue_obj.cleanup_connections()

    def is_running(self) -> bool:
        """Check if the watcher is currently running."""
        return not self._stop_event.is_set()

    @abstractmethod
    def _drain_queue(self) -> None:
        """Process messages - must be implemented by subclasses."""

    def _setup_signal_handler(self) -> SignalHandlerContext | None:
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
        """Overridable base class method for message processing.

        This is the main message processing loop that:
        1. Waits for activity
        2. Checks for pending messages (if subclass supports it)
        3. Drains the queue

        Subclasses can override this method to implement custom logic.
        """
        while True:
            # Wait until something might have happened
            self._strategy.wait_for_activity()

            # Check stop before processing
            self._check_stop()

            # Two-phase detection: check if we actually have messages (if subclass supports it)
            if not getattr(self, "_skip_idle_check", False) and hasattr(
                self, "_has_pending_messages"
            ):
                if not self._has_pending_messages():  # Remove the None argument
                    # No messages for this queue, skip drain
                    continue

            # Always try to drain the queue first; this guarantees
            # that a stop request does not prevent us from
            # finishing already-visible work, so connections can
            # be closed and no messages get lost.
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
                if hasattr(self._strategy, "start"):
                    queue = self._get_queue_for_data_version()

                    # Capture queue in closure to avoid B023 warning
                    def data_version_getter(q: Queue = queue) -> int | None:
                        return q.get_data_version()

                    self._strategy.start(data_version_getter)

                # Initial drain of existing messages
                self._drain_queue()

                # Main processing loop
                self._process_messages()

                # If we get here, we exited normally
                break

            except _StopLoop:
                # Normal shutdown
                break
            except KeyboardInterrupt:
                # Propagate KeyboardInterrupt so callers can handle it specifically
                raise
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
        raise KeyboardInterrupt

    def _safe_call_handler(
        self,
        message: str,
        timestamp: int,
        error_handler: Callable[[Exception, str, int], bool | None],
        *,
        config: dict[str, Any] = _config,
    ) -> None:
        """Safely call the handler with error handling.

        Args:
            message: Message body to pass to handler
            timestamp: Timestamp to pass to handler
            error_handler: Optional error handler for exceptions
        """
        try:
            if hasattr(self, "_handler") and self._handler is not None:
                self._handler(message, timestamp)
        except Exception as e:
            self._handle_handler_error(
                e, message, timestamp, error_handler, config=config
            )

    def _try_dispatch_message(self, body: str, timestamp: int) -> bool:
        """Try to dispatch a message, return True if successful.

        This method provides safe message dispatch with exception handling.
        It will re-raise _StopLoop exceptions but catch and handle all other
        exceptions, returning False to indicate dispatch failure.

        Args:
            body: Message content
            timestamp: Message timestamp

        Returns:
            True if message was dispatched successfully, False otherwise

        Raises:
            _StopLoop: If stop was requested during dispatch
        """
        try:
            self._dispatch(body, timestamp, config=self._config)
            return True
        except _StopLoop:
            raise  # Re-raise stop signal
        except Exception:
            # Don't update timestamp if dispatch failed in peek mode
            # This ensures we'll retry the message next time
            return False

    def _dispatch(
        self, message: str, timestamp: int, *, config: dict[str, Any] = _config
    ) -> None:
        """Dispatch a message to the handler with error handling and size validation.

        This method provides standardized message dispatch logic including:
        - Message size validation (10MB limit)
        - Safe handler invocation with error handling
        - Consistent error reporting

        Args:
            message: The message content to dispatch
            timestamp: The message timestamp

        Note:
            This method requires _handler and _error_handler to be set by subclasses.
            If the message exceeds the size limit, it will be truncated for error reporting
            but the original oversized message will be discarded.
        """
        # Validate message size (10MB limit)
        message_size = len(message.encode("utf-8"))
        if message_size > MAX_MESSAGE_SIZE:
            error_msg = f"Message size ({message_size} bytes) exceeds {MAX_MESSAGE_SIZE // (1024 * 1024)}MB limit"
            if config["BROKER_LOGGING_ENABLED"]:
                logger.error(error_msg)
            # Use error handler if available
            if self._error_handler:
                self._handle_handler_error(
                    ValueError(error_msg),
                    message[:1000] + "...",
                    timestamp,
                    self._error_handler,
                    config=config,
                )
            return

        # Dispatch message using safe handler method if error handler is available
        if self._error_handler:
            self._safe_call_handler(
                message, timestamp, self._error_handler, config=config
            )
        elif self._handler:
            # Direct call if no error handler (legacy support)
            self._handler(message, timestamp)

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

    def start(self) -> threading.Thread:
        """Start the watcher in a background thread.

        This is a convenience method that calls run_in_thread().

        IMPORTANT: You MUST call stop() when done!
        See the warnings in run_in_thread() and stop() for details.

        Returns:
            The thread running the watcher.
        """
        return self.run_in_thread()

    def run(self) -> None:
        """Run the watcher synchronously until stopped.

        This is a convenience method that calls run_forever().

        This method blocks until:
        - stop() is called from another thread
        - SIGINT (Ctrl+C) is received (if in main thread)
        - An unrecoverable error occurs

        No additional cleanup is needed after this method returns, as it
        runs synchronously in the current thread.
        """
        self.run_forever()

    def __enter__(self) -> BaseWatcher:
        """Enter context manager - start watcher in background thread."""
        self.run_in_thread()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
        *,
        config: dict[str, Any] = _config,
    ) -> None:
        """Exit context manager - stop and clean up."""
        try:
            self.stop()
        except Exception as e:
            if config["BROKER_LOGGING_ENABLED"]:
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
        jitter_factor: float = 0.15,
    ) -> None:
        self._initial_checks = initial_checks
        self._max_interval = max_interval
        self._burst_sleep = burst_sleep
        self._check_count = 0
        self._stop_event = stop_event
        self._data_version: int | None = None
        self._data_version_provider: Callable[[], int | None] | None = None
        self._pragma_failures = 0
        self._jitter_factor = jitter_factor

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
            # Use shorter timeout chunks for faster SIGINT response
            chunk_timeout = min(delay, 0.05)  # Max 50ms chunks
            remaining = delay
            while remaining > 0 and not self._stop_event.is_set():
                wait_time = min(remaining, chunk_timeout)
                if self._stop_event.wait(timeout=wait_time):
                    break
                remaining -= wait_time

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
            jitter_factor = self._jitter_factor
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
        queue: str | Queue,
        handler: Callable[[str, int], None],
        *,
        db: BrokerDB | str | Path | None = None,
        stop_event: threading.Event | None = None,
        peek: bool = False,
        since_timestamp: int | None = None,
        batch_processing: bool = False,
        polling_strategy: PollingStrategy | None = None,
        error_handler: Callable[
            [Exception, str, int], bool | None
        ] = config_aware_default_error_handler,
        config: dict[str, Any] = _config,
    ) -> None:
        """Initialize the QueueWatcher.

        Args:
            queue: Queue object or queue name
            handler: Message handler function receiving (message, timestamp)
            db: Database instance or path (uses default if None)
            stop_event: Event to signal shutdown
            peek: If True, don't consume messages (default False)
            since_timestamp: Only process messages newer than this timestamp
            batch_processing: Process all messages at once vs one-by-one
            polling_strategy: Custom polling strategy (uses default if None)
            error_handler: Handler for exceptions from main handler

        Examples:
        >>> # Using queue name
        >>> watcher = QueueWatcher("tasks", handler)
        >>>
        >>> # Using Queue object
        >>> queue = Queue("tasks", persistent=True)
        >>> watcher = QueueWatcher(queue, handler)
        >>>
        >>> # With custom database
        >>> watcher = QueueWatcher("tasks", handler, db="/path/to/db")

        """
        # Validate handler is callable
        if not callable(handler):
            msg = f"handler must be callable, got {type(handler).__name__}"
            raise TypeError(msg)

        # Validate error_handler is callable
        if not callable(error_handler):
            msg = f"error_handler must be callable, got {type(error_handler).__name__}"
            raise TypeError(msg)

        # Store handlers before calling super().__init__() to prevent BaseWatcher from overriding
        self._handler = handler
        self._error_handler = error_handler

        # Initialize parent class with queue-first pattern
        super().__init__(
            queue,
            db=db,
            stop_event=stop_event,
            polling_strategy=polling_strategy,
            config=config,
        )

        # Store queue name for backward compatibility
        if isinstance(queue, Queue):
            self._queue_name = queue.name
        else:
            self._queue_name = str(queue)
        self._queue = self._queue_name  # Backward compatibility
        # Store watcher configuration
        self._peek = peek
        self._last_seen_ts = since_timestamp if since_timestamp is not None else 0
        self._batch_processing = batch_processing

        # Two-phase detection configuration
        self._skip_idle_check = config["BROKER_SKIP_IDLE_CHECK"]

    def _has_pending_messages(self) -> bool:
        """Fast check if queue has unclaimed messages.

        Uses the Queue's has_pending method with retry logic for operational error handling.
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
        with QueueMoveWatcher("inbox", "processed", handler) as watcher:
            # Moves messages for 60 seconds
            time.sleep(60)
        # Thread stopped and resources cleaned up automatically

        # Manual management
        watcher = QueueMoveWatcher("inbox", "processed", handler)
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
        source_queue: str | Queue,
        dest_queue: str,
        handler: Callable[[str, int], None],
        *,
        db: BrokerDB | str | Path | None = None,
        stop_event: threading.Event | None = None,
        max_messages: int | None = None,
        polling_strategy: PollingStrategy | None = None,
        error_handler: Callable[
            [Exception, str, int], bool | None
        ] = config_aware_default_error_handler,
        config: dict[str, Any] = _config,
    ) -> None:
        """Initialize a QueueMoveWatcher.

        Args:
            source_queue: Source queue object or name
            dest_queue: Name of destination queue
            handler: Function called with (message_body, timestamp) for each moved message
            db: Database instance or path (uses default if None)
            stop_event: Event to signal watcher shutdown
            max_messages: Maximum messages to move before stopping
            polling_strategy: Custom polling strategy (uses default if None)
            error_handler: Called when handler raises an exception

        Raises:
            ValueError: If source_queue == dest_queue

        """
        # Get source queue name for comparison
        source_name = (
            source_queue.name if isinstance(source_queue, Queue) else str(source_queue)
        )

        if source_name == dest_queue:
            msg = "Cannot move messages to the same queue"
            raise ValueError(msg)

        # Initialize parent class with source queue
        super().__init__(
            source_queue,
            db=db,
            stop_event=stop_event,
            polling_strategy=polling_strategy,
            config=config,
        )

        # Store move-specific attributes
        self._source_queue = source_name
        self._dest_queue = dest_queue
        self._move_count = 0
        self._max_messages = max_messages

        # Store handlers
        self._handler = handler
        self._error_handler = error_handler

        # The main queue is our source queue for moves
        self._source_queue_obj = self._queue_obj

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
            self._dispatch(result["message"], result["timestamp"], config=self._config)

            # Check max messages limit
            if self._max_messages and self._move_count >= self._max_messages:
                if config["BROKER_LOGGING_ENABLED"]:
                    logger.info(f"Reached max_messages limit ({self._max_messages})")
                self._stop_event.set()
                raise _StopLoop

        return found_any


# ~
