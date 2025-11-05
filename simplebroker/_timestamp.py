"""Hybrid timestamp generation and validation for consistent ordering.

This module provides the canonical timestamp generation and validation logic
that all SimpleBroker extensions must use to ensure consistency.
"""

import os
import random
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING

from ._constants import (
    LOGICAL_COUNTER_MASK,
    MAX_ITERATIONS,
    MAX_LOGICAL_COUNTER,
    SQLITE_MAX_INT64,
    TIMESTAMP_EXACT_NUM_DIGITS,
    UNIX_NATIVE_BOUNDARY,
    WAIT_FOR_NEXT_INCREMENT,
)
from ._exceptions import IntegrityError, OperationalError, TimestampError
from .helpers import _execute_with_retry

if TYPE_CHECKING:
    from ._runner import SQLRunner


class TimestampGenerator:
    """Thread-safe hybrid timestamp generator with validation.

    Generates 64-bit timestamps with:
    - 52 bits: microseconds since epoch
    - 12 bits: monotonic counter for ordering within same microsecond

    This ensures unique, monotonically increasing timestamps even under
    high concurrency.
    """

    def __init__(self, runner: "SQLRunner"):
        self._runner = runner
        self._lock = threading.Lock()
        self._initialized = False
        self._last_ts = 0
        self._counter = 0
        self._pid = os.getpid()

    def _initialize(self) -> None:
        """Initialize state from database."""
        if self._initialized:
            return

        # Load last timestamp from meta table
        result = self._runner.run(
            "SELECT value FROM meta WHERE key = 'last_ts'", fetch=True
        )
        result_list = list(result)
        if result_list:
            self._last_ts = result_list[0][0]
        else:
            self._last_ts = 0

        self._initialized = True

    def _encode_hybrid_timestamp(self, physical_ns: int, logical: int) -> int:
        """Encode physical time and logical counter into a 64-bit hybrid timestamp.

        The timestamp preserves the magnitude of time.time_ns() by clearing the
        bottom bits and using them for the logical counter, rather than shifting.

        Args:
            physical_ns: Physical time in nanoseconds since epoch
            logical: Logical counter (0 to MAX_LOGICAL_COUNTER)

        Returns:
            64-bit hybrid timestamp
        """
        # Clear the bottom LOGICAL_COUNTER_BITS bits
        time_mask = ~LOGICAL_COUNTER_MASK
        time_base = physical_ns & time_mask
        # Add the logical counter in the bottom bits
        return time_base | logical

    def _decode_hybrid_timestamp(self, ts: int) -> tuple[int, int]:
        """Decode a 64-bit hybrid timestamp into physical time and logical counter.

        Args:
            ts: 64-bit hybrid timestamp

        Returns:
            tuple of (physical_ns_base, logical_counter)
        """
        # Extract the time base (top bits)
        time_mask = ~LOGICAL_COUNTER_MASK
        physical_ns_base = ts & time_mask
        # Extract the logical counter (bottom bits)
        logical_counter = ts & LOGICAL_COUNTER_MASK
        return physical_ns_base, logical_counter

    def generate(self) -> int:
        """
        Robust, lock-free (DB-wise) timestamp generator.
        """
        self._ensure_pid()

        # one local fast-path loop, *no* DB locks are held here
        for _ in range(6):  # hard upper bound
            physical_ns, logical = self._next_components()
            new_ts = self._encode_hybrid_timestamp(physical_ns, logical)

            # Ensure it fits in SQLite's signed 64-bit integer
            if new_ts >= SQLITE_MAX_INT64:
                raise TimestampError("Timestamp too far in future")

            # >>> single atomic write – no BEGIN <<< -----------------
            if self._store_if_greater(new_ts):
                self._last_ts = new_ts
                return new_ts
            # ---------------------------------------------------------

            # Someone beat us – read their value and try again
            latest = self._peek_last_ts()
            if latest is None:
                # meta row disappeared – DB is corrupt
                raise TimestampError("meta.last_ts missing")
            self._last_ts = latest

        # Fall back to resilience mechanism
        raise IntegrityError("unable to generate unique timestamp (exhausted retries)")

    # -- internal helpers -------------------------------------

    def _ensure_pid(self) -> None:
        """
        Handle fork() transparently – cheap check, no DB access.
        """
        pid = os.getpid()
        if pid != self._pid:
            self._pid = pid
            self._initialized = False  # force lazy init
            self._last_ts = 0
            self._counter = 0

    # -----------------------------------------------------------------
    # 1. compute next physical/logical pair entirely in memory
    # -----------------------------------------------------------------
    def _next_components(self) -> tuple[int, int]:
        """
        Generate next timestamp components using nanoseconds.
        """
        with self._lock:
            if not self._initialized:
                self._initialize()  # cheap SELECT, autocommit

            now_ns = time.time_ns()
            # Decode the last timestamp to get its base time
            last_phys_ns, last_counter = self._decode_hybrid_timestamp(self._last_ts)

            # Clear bottom bits of current time to get the time base
            time_mask = ~LOGICAL_COUNTER_MASK
            now_ns_base = now_ns & time_mask

            if now_ns_base > last_phys_ns:
                # Time has advanced, reset counter
                self._counter = 0
            else:
                # Same time base, increment counter
                self._counter = last_counter + 1
                if self._counter >= MAX_LOGICAL_COUNTER:
                    # Counter overflow, wait for clock to advance
                    num_iterations = 0
                    while (
                        now_ns_base <= last_phys_ns and num_iterations < MAX_ITERATIONS
                    ):
                        jitter = random.uniform(
                            WAIT_FOR_NEXT_INCREMENT / 2, WAIT_FOR_NEXT_INCREMENT
                        )
                        time.sleep(jitter)
                        now_ns = time.time_ns()
                        now_ns_base = now_ns & time_mask
                        num_iterations += 1
                    self._counter = 0

            return now_ns_base, self._counter

    # -----------------------------------------------------------------
    # 2. try to store the new value if it is higher
    # -----------------------------------------------------------------
    def _store_if_greater(self, new_ts: int) -> bool:
        """
        Try to atomically update meta.last_ts.
        Returns True if we stored the value, False if someone else already
        wrote a higher one.
        """

        def _op() -> bool:
            rows = self._runner.run(
                """
                UPDATE meta
                SET    value = ?
                WHERE  key   = 'last_ts'
                  AND  value < ?
                RETURNING value
                """,
                (new_ts, new_ts),
                fetch=True,
            )
            # rows is non-empty if the UPDATE happened
            return bool(list(rows))

        try:
            return _execute_with_retry(_op, max_retries=15, retry_delay=0.002)
        except OperationalError as e:  # pragma busy_timeout etc.
            raise TimestampError(f"database busy while writing timestamp: {e}") from e

    # -----------------------------------------------------------------
    # 3. lightweight read helper when we lost the race
    # -----------------------------------------------------------------
    def _peek_last_ts(self) -> int | None:
        rows = list(
            self._runner.run("SELECT value FROM meta WHERE key='last_ts'", fetch=True)
        )
        return rows[0][0] if rows else None

    @staticmethod
    def validate(timestamp_str: str, exact: bool = False) -> int:
        """Validate and parse timestamp string into a 64-bit hybrid timestamp.

        This is the canonical validation logic used by the -m flag and other
        timestamp parsing needs. All extensions should use this for consistency.

        Args:
            timestamp_str: String representation of timestamp. Accepts:
                - Native 64-bit hybrid timestamp (e.g., "1837025672140161024", interchangeable with Unix nanoseconds)")
                - ISO 8601 date/datetime (e.g., "2024-01-15", "2024-01-15T14:30:00")
                - Unix timestamp in seconds, milliseconds, or nanoseconds (e.g., "1705329000")
                - Explicit units: "1705329000s" (seconds), "1705329000000ms" (milliseconds),
                  "1705329000000000000ns" (nanoseconds)
            exact: If True, only accept exact 19-digit message IDs (for strict validation)

        Returns:
            Parsed timestamp as 64-bit hybrid integer

        Raises:
            TimestampError: If timestamp is invalid
        """
        # Strip whitespace once at the beginning
        timestamp_str = timestamp_str.strip()
        if not timestamp_str:
            raise TimestampError("Invalid timestamp: empty string")

        # If exact mode, enforce strict 19-digit validation
        if exact:
            return TimestampGenerator._validate_exact_timestamp(timestamp_str)

        # Reject scientific notation early for consistency
        if "e" in timestamp_str.lower():
            raise TimestampError("Invalid timestamp: scientific notation not supported")

        # Check for explicit unit suffixes
        ts = TimestampGenerator._parse_with_unit_suffix(timestamp_str)
        if ts is not None:
            return ts

        # Try formats in order of precedence
        # 1. ISO format (unambiguous)
        ts = TimestampGenerator._parse_iso8601(timestamp_str)
        if ts is not None:
            return ts

        # 2. Native or Unix numeric format
        ts = TimestampGenerator._parse_native_or_unix(timestamp_str)
        if ts is not None:
            return ts

        # 3. Unix float format (e.g., from time.time())
        try:
            ts = TimestampGenerator._parse_numeric_timestamp(timestamp_str)
            if ts is not None:
                return ts
        except ValueError as e:
            if "Invalid timestamp" in str(e):
                raise
            # Fall through to final error
            pass

        raise TimestampError(f"Invalid timestamp: {timestamp_str}")

    @staticmethod
    def _validate_exact_timestamp(timestamp_str: str) -> int:
        """Validate timestamp in exact mode (strict 19-digit validation)."""
        if (
            len(timestamp_str) != TIMESTAMP_EXACT_NUM_DIGITS
            or not timestamp_str.isdigit()
        ):
            raise TimestampError(
                "Invalid timestamp: exact mode requires exactly 19 digits"
            )
        # Convert to int and validate range
        timestamp = int(timestamp_str)
        if timestamp >= SQLITE_MAX_INT64:
            raise TimestampError("Invalid timestamp: exceeds maximum value")
        return timestamp

    @staticmethod
    def _parse_with_unit_suffix(timestamp_str: str) -> int | None:
        """Parse timestamp with explicit unit suffixes (s, ms, ns)."""
        original_str = timestamp_str
        unit = None  # Default to None if no suffix found

        if timestamp_str.endswith("ns"):
            unit = "ns"
            timestamp_str = timestamp_str[:-2]
        elif timestamp_str.endswith("ms"):
            unit = "ms"
            timestamp_str = timestamp_str[:-2]
        elif timestamp_str.endswith("s") and not timestamp_str.endswith("Z"):
            # Check if it's actually part of an ISO format
            if timestamp_str[-2:-1].isdigit():
                unit = "s"
                timestamp_str = timestamp_str[:-1]

        if not unit:
            return None

        try:
            val = float(timestamp_str) if "." in timestamp_str else int(timestamp_str)
            if val < 0:
                raise TimestampError("Invalid timestamp: cannot be negative")

            if unit == "s":
                # Unix seconds to nanoseconds
                ns_since_epoch = int(val * 1_000_000_000)
            elif unit == "ms":
                # Unix milliseconds to nanoseconds
                ns_since_epoch = int(val * 1_000_000)
            elif unit == "ns":
                # Already in nanoseconds
                ns_since_epoch = int(val)

            # Clear bottom bits for counter (hybrid timestamp format)
            time_mask = ~LOGICAL_COUNTER_MASK
            hybrid_ts = ns_since_epoch & time_mask
            if hybrid_ts >= SQLITE_MAX_INT64:
                raise TimestampError("Invalid timestamp: too far in future")
            return hybrid_ts
        except (ValueError, OverflowError) as e:
            if "Invalid timestamp" in str(e):
                raise
            raise TimestampError(f"Invalid timestamp: {original_str}") from None

    @staticmethod
    def _parse_native_or_unix(timestamp_str: str) -> int | None:
        """Parse as native timestamp or Unix timestamp based on heuristic."""
        try:
            # Try integer first
            val = int(timestamp_str)
            if val < 0:
                raise TimestampError("Invalid timestamp: cannot be negative")

            # Use improved heuristic - tighten boundary to avoid edge cases
            # Native timestamps are (ms << LOGICAL_COUNTER_BITS), so for year 2025:
            # ms ≈ 1.7e12, native ≈ 1.8e18
            # Use 2^44 as boundary (≈ 1.76e13 ms ≈ year 2527)
            boundary = UNIX_NATIVE_BOUNDARY  # About 17.6 trillion

            if val < boundary:
                # Treat as Unix timestamp
                ts = TimestampGenerator._parse_numeric_timestamp(timestamp_str)
                if ts is not None:
                    return ts
                raise TimestampError(f"Invalid timestamp: {timestamp_str}")
            else:
                # Treat as native timestamp
                if val >= SQLITE_MAX_INT64:
                    raise TimestampError("Invalid timestamp: exceeds maximum value")
                return val
        except ValueError as e:
            if "Invalid timestamp" in str(e):
                raise
            # Not an integer, continue
            return None

    @staticmethod
    def _parse_iso8601(timestamp_str: str) -> int | None:
        """Try to parse as ISO 8601 date/datetime."""
        # Only try ISO parsing if the string contains date-like characters
        # ISO dates must contain '-' or 'T' or 'Z' or look like YYYYMMDD (exactly 8 digits)
        if not (
            "-" in timestamp_str
            or "T" in timestamp_str.upper()
            or "Z" in timestamp_str.upper()
            or (len(timestamp_str) == 8 and timestamp_str.isdigit())
        ):
            return None

        # Handle both date-only and full datetime formats
        # Replace 'Z' with UTC offset for compatibility
        normalized = timestamp_str.replace("Z", "+00:00")

        # Try to parse as datetime
        dt = None
        try:
            # Try full datetime first
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            # Try date-only format
            try:
                # Parse as date and convert to datetime at midnight UTC
                from datetime import date, time, timezone

                date_obj = date.fromisoformat(normalized)
                dt = datetime.combine(date_obj, time.min, tzinfo=timezone.utc)
            except ValueError:
                return None  # Not a valid date format

        if dt is None:
            return None

        # Convert to UTC if timezone-aware
        if dt.tzinfo is None:
            # Assume UTC for naive datetimes
            from datetime import timezone

            dt = dt.replace(tzinfo=timezone.utc)
        else:
            from datetime import timezone

            dt = dt.astimezone(timezone.utc)

        # Convert to nanoseconds since epoch
        ns_since_epoch = int(dt.timestamp() * 1_000_000_000)
        # Clear bottom bits for counter (hybrid timestamp format)
        time_mask = ~LOGICAL_COUNTER_MASK
        hybrid_ts = ns_since_epoch & time_mask
        # Ensure it fits in SQLite's signed 64-bit integer
        if hybrid_ts >= SQLITE_MAX_INT64:
            raise ValueError("Invalid timestamp: too far in future")
        return hybrid_ts

    @staticmethod
    def _parse_numeric_timestamp(timestamp_str: str) -> int | None:
        """Parse numeric timestamp with unit heuristic."""
        try:
            # Handle decimal numbers
            if "." in timestamp_str:
                # Parse as float
                unix_ts = float(timestamp_str)
                if unix_ts < 0:
                    raise ValueError("Invalid timestamp: cannot be negative")
                int_part = str(int(unix_ts))
                integer_digits = len(int_part)
            else:
                # Pure integer - avoid float conversion to preserve precision
                int_val = int(timestamp_str)
                if int_val < 0:
                    raise ValueError("Invalid timestamp: cannot be negative")

                integer_digits = len(timestamp_str.lstrip("0") or "0")
                unix_ts = int_val

            # Heuristic based on number of digits for the integer part
            # Current time (2025) is ~10 digits in seconds, ~13 digits in ms, ~19 digits in ns

            if integer_digits > 16:  # Likely nanoseconds
                # Already in nanoseconds
                if "." in timestamp_str:
                    ns_since_epoch = int(unix_ts)
                else:
                    ns_since_epoch = int(timestamp_str)
            elif integer_digits > 11:  # Likely milliseconds
                # Convert milliseconds to nanoseconds
                if "." in timestamp_str:
                    ns_since_epoch = int(unix_ts * 1_000_000)
                else:
                    ns_since_epoch = int(timestamp_str) * 1_000_000
            else:  # Likely seconds
                # Convert seconds to nanoseconds
                if "." in timestamp_str:
                    # Preserve fractional seconds
                    ns_since_epoch = int(unix_ts * 1_000_000_000)
                else:
                    # Pure integer - multiply without float conversion
                    ns_since_epoch = int(timestamp_str) * 1_000_000_000

            # Clear bottom bits for counter (hybrid timestamp format)
            time_mask = ~LOGICAL_COUNTER_MASK
            hybrid_ts = ns_since_epoch & time_mask
            # Ensure it fits in signed 64-bit integer
            if hybrid_ts >= SQLITE_MAX_INT64:
                raise ValueError("Invalid timestamp: too far in future")
            return hybrid_ts

        except (ValueError, OverflowError):
            return None


# ~
