"""Timing utilities for robust testing in CI environments."""

from __future__ import annotations

import os
import time
from typing import Callable, TypeVar

T = TypeVar("T")


def wait_for_condition(
    condition_fn: Callable[[], bool],
    timeout: float = 5.0,
    interval: float = 0.1,
    message: str | None = None,
) -> bool:
    """Wait for a condition to become true.

    Args:
        condition_fn: Function that returns True when condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
        message: Optional message for debugging

    Returns:
        True if condition was met, False if timeout occurred

    """
    start_time = time.monotonic()
    while time.monotonic() - start_time < timeout:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


def wait_for_value(
    value_fn: Callable[[], T],
    expected: T,
    timeout: float = 5.0,
    interval: float = 0.1,
    message: str | None = None,
) -> bool:
    """Wait for a function to return an expected value.

    Args:
        value_fn: Function that returns a value
        expected: Expected value to wait for
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
        message: Optional message for debugging

    Returns:
        True if expected value was returned, False if timeout occurred

    """
    return wait_for_condition(
        lambda: value_fn() == expected,
        timeout=timeout,
        interval=interval,
        message=message,
    )


def wait_for_count(
    count_fn: Callable[[], int],
    expected_count: int,
    timeout: float = 5.0,
    interval: float = 0.1,
    at_least: bool = False,
) -> bool:
    """Wait for a count to reach expected value.

    Args:
        count_fn: Function that returns current count
        expected_count: Expected count to wait for
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
        at_least: If True, wait for count >= expected_count

    Returns:
        True if expected count was reached, False if timeout occurred

    """
    if at_least:

        def condition():
            return count_fn() >= expected_count
    else:

        def condition():
            return count_fn() == expected_count

    return wait_for_condition(condition, timeout=timeout, interval=interval)


def retry_on_exception(
    func: Callable[[], T],
    exception_types: type | tuple = Exception,
    max_attempts: int = 3,
    delay: float = 0.5,
    backoff_factor: float = 2.0,
) -> T:
    """Retry a function on exception.

    Args:
        func: Function to retry
        exception_types: Exception types to catch and retry
        max_attempts: Maximum number of attempts
        delay: Initial delay between attempts
        backoff_factor: Multiplier for delay after each attempt

    Returns:
        Result of successful function call

    Raises:
        The last exception if all attempts fail

    """
    current_delay = delay
    last_exception = None

    for attempt in range(max_attempts):
        try:
            return func()
        except exception_types as e:
            last_exception = e
            if attempt < max_attempts - 1:
                time.sleep(current_delay)
                current_delay *= backoff_factor

    raise last_exception


# Performance threshold configuration
def get_performance_threshold(name: str, default: float) -> float:
    """Get performance threshold from environment or default.

    Args:
        name: Name of the threshold (used in env var)
        default: Default value if not set

    Returns:
        Threshold value

    Example:
        max_query_time = get_performance_threshold("MAX_QUERY_TIME_US", 100)
        # Looks for CI_MAX_QUERY_TIME_US environment variable

    """
    env_name = f"CI_{name}"
    env_value = os.environ.get(env_name)
    if env_value:
        try:
            return float(env_value)
        except ValueError:
            pass
    return default


def scale_timeout_for_ci(timeout: float, ci_factor: float = 2.0) -> float:
    """Scale timeout for CI environment.

    Args:
        timeout: Base timeout in seconds
        ci_factor: Multiplication factor for CI

    Returns:
        Scaled timeout

    """
    if os.environ.get("CI"):
        return timeout * ci_factor
    return timeout
