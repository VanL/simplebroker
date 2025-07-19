"""Helper functions and classes for SimpleBroker."""

import time
from typing import Callable, TypeVar

from ._exceptions import OperationalError

T = TypeVar("T")


def _execute_with_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 10,
    retry_delay: float = 0.05,
) -> T:
    """Execute a database operation with retry logic for locked database errors.

    Args:
        operation: A callable that performs the database operation
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (exponential backoff applied)

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
                    time.sleep(wait)
                    continue
            # If not a locked error or last attempt, re-raise
            raise

    # This should never be reached, but satisfies mypy
    raise AssertionError("Unreachable code")
