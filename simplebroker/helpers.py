"""Helper functions and classes for SimpleBroker."""

import threading
import time
from typing import Callable, Optional, TypeVar

from ._exceptions import OperationalError, StopException

T = TypeVar("T")


def interruptible_sleep(
    seconds: float,
    stop_event: Optional[threading.Event] = None,
    chunk_size: float = 0.1,
) -> bool:
    """Sleep for the specified duration, but can be interrupted by a stop event.

    This function provides a more responsive alternative to time.sleep() that can be
    interrupted by a threading.Event. Even without a stop_event, it sleeps in chunks
    to allow for better thread responsiveness and signal handling.

    Args:
        seconds: Number of seconds to sleep
        stop_event: Optional threading.Event that can interrupt the sleep
        chunk_size: Maximum duration of each sleep chunk (default: 0.1 seconds)

    Returns:
        True if the full sleep duration completed, False if interrupted by stop_event

    Example:
        # In a loop that needs to be stoppable
        stop_event = threading.Event()
        while not stop_event.is_set():
            # Do work...
            if not interruptible_sleep(5.0, stop_event):
                break  # Sleep was interrupted, exit loop
    """
    if seconds <= 0:
        return True

    # Create a dummy event if none provided
    event = stop_event or threading.Event()

    # For short sleeps, do it in one go
    if seconds <= chunk_size:
        return not event.wait(timeout=seconds)

    # For longer sleeps, chunk it up
    start_time = time.perf_counter()
    target_end_time = start_time + seconds

    while time.perf_counter() < target_end_time:
        remaining = target_end_time - time.perf_counter()
        if remaining <= 0:
            break

        if event.wait(timeout=min(chunk_size, remaining)):
            # Only return False if it was the actual stop_event that was set
            return stop_event is None or not stop_event.is_set()

    return True


def _execute_with_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 10,
    retry_delay: float = 0.05,
    stop_event: Optional[threading.Event] = None,
) -> T:
    """Execute a database operation with retry logic for locked database errors.

    Args:
        operation: A callable that performs the database operation
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (exponential backoff applied)
        stop_event: Optional threading.Event that can interrupt the retry loop

    Returns:
        The result of the operation

    Raises:
        The last exception if all retries fail
    """
    locked_markers = (
        "database is locked",
        "database table is locked",
        "database schema is locked",
        "database is busy",
        "database busy",
    )

    for attempt in range(max_retries):
        try:
            return operation()
        except OperationalError as e:
            msg = str(e).lower()
            if any(marker in msg for marker in locked_markers):
                if attempt < max_retries - 1:
                    # exponential back-off + 0-25 ms jitter using time-based pseudo-random
                    jitter = (time.time() * 1000) % 25 / 1000  # 0-25ms jitter
                    wait = retry_delay * (2**attempt) + jitter
                    if not interruptible_sleep(wait, stop_event):
                        # Sleep was interrupted, raise exception to exit retry loop
                        raise StopException("Retry interrupted by stop event") from None
                    continue
            # If not a locked error or last attempt, re-raise
            raise

    # This should never be reached, but satisfies mypy
    raise AssertionError("Unreachable code")
