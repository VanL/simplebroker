"""Hybrid timestamp generation and validation for consistent ordering.

This module provides the canonical timestamp generation and validation logic
that all SimpleBroker extensions must use to ensure consistency.
"""

import os
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Tuple

from ._exceptions import IntegrityError, TimestampError

if TYPE_CHECKING:
    from ._runner import SQLRunner

# Timestamp constants
TIMESTAMP_EXACT_NUM_DIGITS = 19  # Exact number of digits for message ID timestamps


class TimestampGenerator:
    """Thread-safe hybrid timestamp generator with validation.

    Generates 64-bit timestamps with:
    - 44 bits: milliseconds since epoch
    - 20 bits: monotonic counter for ordering within same millisecond

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

    def _encode_hybrid_timestamp(self, physical_ms: int, logical: int) -> int:
        """Encode physical time and logical counter into a 64-bit hybrid timestamp.

        Args:
            physical_ms: Physical time in milliseconds since epoch
            logical: Logical counter (0 to MAX_LOGICAL_COUNTER)

        Returns:
            64-bit hybrid timestamp
        """
        return (physical_ms << 20) | logical

    def _decode_hybrid_timestamp(self, ts: int) -> Tuple[int, int]:
        """Decode a 64-bit hybrid timestamp into physical time and logical counter.

        Args:
            ts: 64-bit hybrid timestamp

        Returns:
            Tuple of (physical_ms, logical_counter)
        """
        physical_ms = ts >> 20
        logical_counter = ts & ((1 << 20) - 1)
        return physical_ms, logical_counter

    def generate(self) -> int:
        """Generate the next hybrid timestamp.

        Returns:
            64-bit timestamp value

        Raises:
            IntegrityError: If timestamp update conflicts
        """
        # Check for fork safety
        current_pid = os.getpid()
        if current_pid != self._pid:
            # We're in a forked process, reinitialize
            self._pid = current_pid
            self._initialized = False
            self._last_ts = 0
            self._counter = 0

        # Lazy initialization
        self._initialize()

        with self._lock:
            # Current time in milliseconds
            now_ms = int(time.time() * 1000)

            # Extract physical time from last timestamp
            last_physical = self._last_ts >> 20

            if now_ms > last_physical:
                # New millisecond, reset counter
                self._counter = 0
                new_ts = (now_ms << 20) | self._counter
            else:
                # Same millisecond, increment counter
                self._counter += 1
                if self._counter >= (1 << 20):
                    # Counter overflow, wait for next millisecond
                    while now_ms <= last_physical:
                        time.sleep(0.001)
                        now_ms = int(time.time() * 1000)
                    self._counter = 0
                new_ts = (now_ms << 20) | self._counter

            # Ensure it fits in SQLite's signed 64-bit integer
            if new_ts >= 2**63:
                raise TimestampError("Timestamp too far in future")

            # Update database (must be in transaction)
            try:
                self._runner.run(
                    "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ?",
                    (new_ts, self._last_ts),
                )
            except IntegrityError:
                raise IntegrityError("Timestamp update conflict") from None

            self._last_ts = new_ts
            return new_ts

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
            if (
                len(timestamp_str) != TIMESTAMP_EXACT_NUM_DIGITS
                or not timestamp_str.isdigit()
            ):
                raise TimestampError(
                    "Invalid timestamp: exact mode requires exactly 19 digits"
                )
            # Convert to int and validate range
            timestamp = int(timestamp_str)
            if timestamp >= 2**63:
                raise TimestampError("Invalid timestamp: exceeds maximum value")
            return timestamp

        # Reject scientific notation early for consistency
        if "e" in timestamp_str.lower():
            raise TimestampError("Invalid timestamp: scientific notation not supported")

        # Check for explicit unit suffixes
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

        # If explicit unit provided, parse accordingly
        if unit:
            try:
                val = (
                    float(timestamp_str) if "." in timestamp_str else int(timestamp_str)
                )
                if val < 0:
                    raise TimestampError("Invalid timestamp: cannot be negative")

                if unit == "s":
                    # Unix seconds
                    ms_since_epoch = int(val * 1000)
                elif unit == "ms":
                    # Unix milliseconds
                    ms_since_epoch = int(val)
                elif unit == "ns":
                    # Unix nanoseconds
                    ms_since_epoch = int(val / 1_000_000)

                hybrid_ts = ms_since_epoch << 20
                if hybrid_ts >= 2**63:
                    raise TimestampError("Invalid timestamp: too far in future")
                return hybrid_ts
            except (ValueError, OverflowError) as e:
                if "Invalid timestamp" in str(e):
                    raise
                raise TimestampError(f"Invalid timestamp: {original_str}") from None

        # Try formats in order of precedence
        # 1. ISO format (unambiguous)
        ts = TimestampGenerator._parse_iso8601(timestamp_str)
        if ts is not None:
            return ts

        # 2. Native or Unix numeric format
        try:
            # Try integer first
            val = int(timestamp_str)
            if val < 0:
                raise TimestampError("Invalid timestamp: cannot be negative")

            # Use improved heuristic - tighten boundary to avoid edge cases
            # Native timestamps are (ms << 20), so for year 2025:
            # ms ≈ 1.7e12, native ≈ 1.8e18
            # Use 2^44 as boundary (≈ 1.76e13 ms ≈ year 2527)
            boundary = 1 << 44  # About 17.6 trillion

            if val < boundary:
                # Treat as Unix timestamp
                ts = TimestampGenerator._parse_numeric_timestamp(timestamp_str)
                if ts is not None:
                    return ts
                raise TimestampError(f"Invalid timestamp: {timestamp_str}")
            else:
                # Treat as native timestamp
                if val >= 2**63:
                    raise TimestampError("Invalid timestamp: exceeds maximum value")
                return val
        except ValueError as e:
            if "Invalid timestamp" in str(e):
                raise
            # Not an integer, continue
            pass

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
    def _parse_iso8601(timestamp_str: str) -> Optional[int]:
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

        # Convert to milliseconds since epoch (not microseconds!)
        ms_since_epoch = int(dt.timestamp() * 1000)
        # Shift into upper 44 bits (hybrid timestamp format)
        hybrid_ts = ms_since_epoch << 20
        # Ensure it fits in SQLite's signed 64-bit integer
        if hybrid_ts >= 2**63:
            raise ValueError("Invalid timestamp: too far in future")
        return hybrid_ts

    @staticmethod
    def _parse_numeric_timestamp(timestamp_str: str) -> Optional[int]:
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
                # Assume nanoseconds, convert to milliseconds
                if "." in timestamp_str:
                    ms_since_epoch = int(unix_ts / 1_000_000)
                else:
                    # Use integer division to avoid precision loss
                    ms_since_epoch = int(timestamp_str) // 1_000_000
            elif integer_digits > 11:  # Likely milliseconds
                # Assume milliseconds (already in correct unit)
                if "." in timestamp_str:
                    ms_since_epoch = int(unix_ts)
                else:
                    ms_since_epoch = int(timestamp_str)
            else:  # Likely seconds
                # Assume seconds, convert to milliseconds
                if "." in timestamp_str:
                    # Preserve fractional seconds
                    ms_since_epoch = int(unix_ts * 1000)
                else:
                    # Pure integer - multiply without float conversion
                    ms_since_epoch = int(timestamp_str) * 1000

            hybrid_ts = ms_since_epoch << 20
            # Ensure it fits in signed 64-bit integer
            if hybrid_ts >= 2**63:
                raise ValueError("Invalid timestamp: too far in future")
            return hybrid_ts

        except (ValueError, OverflowError):
            return None
