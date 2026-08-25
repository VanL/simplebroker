"""Hybrid timestamp generation and validation for consistent ordering.

This module provides the canonical timestamp generation and validation logic
that all SimpleBroker extensions must use to ensure consistency. Timestamp-bound
string grammar is governed by [SB-CLI-5] and exposed publicly by [SB-API-11].
"""

import os
import random
import re
import threading
import time
import unicodedata
from datetime import UTC, date, datetime
from datetime import time as datetime_time
from typing import TYPE_CHECKING

from ._backend_plugins import BackendPlugin, resolve_runner_backend_plugin
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
from ._retry_policy import _execute_with_retry, _is_locked_operational_error

if TYPE_CHECKING:
    from ._runner import SQLRunner

# Retry budget for the meta.last_ts compare-and-swap under write-lock
# contention: probe at a bounded interval for a bounded wall-clock window.
# A fixed attempt count with uncapped exponential backoff concentrates most
# of its budget in the final two multi-second sleeps, so sustained contention
# gets only a couple of late probes and the write dies spuriously.
TS_RETRY_MAX_ELAPSED = 30.0
TS_RETRY_MAX_DELAY = 0.25
_SCIENTIFIC_NOTATION_RE = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)[eE][+-]?\d+(?:ns|ms|s)?")
# Python's ISO parser accepts fractions on both the wall-clock seconds and an
# offset's seconds. The bound contract permits neither, so reject any decimal
# fraction after the date/time separator before delegating to ``fromisoformat``.
_ISO_FRACTIONAL_SECONDS_RE = re.compile(r"[T ].*[.,]\d+", re.IGNORECASE)
_BOUND_PARSE_GUIDANCE = (
    "timestamp bounds require integral seconds. For finer granularity, use integer "
    "ms, integer ns, or a native hybrid message ID."
)


def decode_hybrid_timestamp(ts: int) -> tuple[int, int]:
    """Decode a hybrid timestamp into its physical base and logical counter."""
    time_mask = ~LOGICAL_COUNTER_MASK
    return ts & time_mask, ts & LOGICAL_COUNTER_MASK


def validate_timestamp_bound(name: str, value: int | None) -> int | None:
    """Validate an integer timestamp filter bound."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int or None")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    if value >= SQLITE_MAX_INT64:
        raise ValueError(f"{name} exceeds maximum timestamp value")
    return value


class TimestampGenerator:
    """Thread-safe hybrid timestamp generator with validation.

    Generates 64-bit timestamps with:
    - high bits: nanoseconds after epoch, aligned to the counter granularity
    - low 12 bits: logical counter for ordering within the same time base

    This ensures unique, monotonically increasing timestamps even under
    high concurrency.
    """

    def __init__(
        self,
        runner: "SQLRunner",
        *,
        backend_plugin: BackendPlugin | None = None,
    ):
        self._runner = runner
        self._backend_plugin = resolve_runner_backend_plugin(runner, backend_plugin)
        self._lock = threading.RLock()
        self._initialized = False
        self._last_ts = 0
        self._counter = 0
        self._pid = os.getpid()

    def _initialize(self) -> None:
        """Initialize state from database."""
        if self._initialized:
            return

        # Load last timestamp from meta table
        self._last_ts = self._backend_plugin.read_last_ts(self._runner)

        self._initialized = True

    def _encode_hybrid_timestamp(self, physical_ns: int, logical: int) -> int:
        """Encode physical time and logical counter into a 64-bit hybrid timestamp.

        The timestamp preserves the magnitude of time.time_ns() by clearing the
        bottom bits and using them for the logical counter, rather than shifting.

        Args:
            physical_ns: Physical time in nanoseconds after epoch
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
        """Decode a hybrid timestamp through the module-level implementation."""
        return decode_hybrid_timestamp(ts)

    def generate(self) -> int:
        """Generate and durably publish the next timestamp.

        One generator lock owns the complete transition: process identity and
        lazy initialization, candidate calculation, durable compare-and-advance,
        conflict refresh, and local cache publication. The backend CAS protects
        durable monotonicity; it cannot by itself prevent an earlier caller from
        publishing stale process-local state after a later caller.
        """
        self._ensure_pid()
        with self._lock:
            # Keep this bounded allocation/CAS loop inside the shared-instance
            # lock. Narrowing the critical section can regress ``_last_ts`` even
            # when every durable compare-and-advance remains monotonic.
            for _ in range(6):  # hard upper bound
                physical_ns, logical = self._next_components()
                new_ts = self._encode_hybrid_timestamp(physical_ns, logical)

                # Ensure it fits in SQLite's signed 64-bit integer
                if new_ts >= SQLITE_MAX_INT64:
                    raise TimestampError("Timestamp too far in future")

                # One atomic compare-and-advance; no explicit transaction.
                if self._store_if_greater(new_ts):
                    self._last_ts = new_ts
                    return new_ts

                # Someone beat us – read their value and try again
                latest = self._peek_last_ts()
                if latest is None:
                    # meta row disappeared – DB is corrupt
                    raise TimestampError("meta.last_ts missing")
                self._last_ts = latest

        # Fall back to resilience mechanism
        raise IntegrityError("unable to generate unique timestamp (exhausted retries)")

    def _reserve_candidates(self, count: int) -> list[int]:
        """Reserve unique local timestamp candidates without persisting them.

        Direct backends use this when target selection and insertion happen in
        one atomic server-side operation. The server-side operation remains
        responsible for advancing persisted ``last_ts`` if it commits at least
        one candidate. An empty target set may therefore leave a harmless gap
        in this process-local cache without changing broker state.
        """

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("count must be an int")
        if count < 0:
            raise ValueError("count must be non-negative")
        if count == 0:
            return []

        self._ensure_pid()
        with self._lock:
            candidates: list[int] = []
            for _ in range(count):
                physical_ns, logical = self._next_components()
                candidate = self._encode_hybrid_timestamp(physical_ns, logical)
                if candidate >= SQLITE_MAX_INT64:
                    raise TimestampError("Timestamp too far in future")
                self._last_ts = candidate
                candidates.append(candidate)
            return candidates

    # -- internal helpers -------------------------------------

    def get_cached_last_ts(self) -> int:
        """Return the most recently observed timestamp without hitting the database."""

        self._ensure_pid()
        with self._lock:
            if not self._initialized:
                self._initialize()
            return self._last_ts

    def refresh_last_ts(self) -> int:
        """Refresh cached timestamp from the database with a lightweight read."""

        self._ensure_pid()
        with self._lock:
            latest = self._peek_last_ts()
            if latest is None:
                self._last_ts = 0
            else:
                self._last_ts = latest
            self._initialized = True
            return self._last_ts

    def advance_to_at_least(self, timestamp: int) -> int:
        """Monotonically install an allocation floor and return the stored value."""

        normalized = validate_timestamp_bound("timestamp", timestamp)
        if normalized is None:
            raise TypeError("timestamp must be an int")
        self._ensure_pid()
        with self._lock:
            # The process-local cache may be ahead of durable state after a
            # direct-backend candidate reservation or a rolled-back write.
            # Always issue the backend's monotone compare-and-advance; the
            # durable state, not this cache, decides whether work is needed.
            self._store_if_greater(normalized)
            try:
                latest = self._peek_last_ts()
            except OperationalError as exc:
                raise TimestampError(
                    "database error after timestamp advance; durable outcome is unknown",
                    outcome_ambiguous=True,
                ) from exc
            if latest is None:
                raise TimestampError("meta.last_ts missing after timestamp advance")
            if latest < normalized:
                raise TimestampError(
                    f"stored timestamp {latest} is below requested floor {normalized}"
                )
            self._last_ts = latest
            self._initialized = True
            return latest

    def _ensure_pid(self) -> None:
        """
        Handle fork() transparently – cheap check, no DB access.
        """
        pid = os.getpid()
        if pid != self._pid:
            self._lock = threading.RLock()
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
                physical_ns = now_ns_base
                self._counter = 0
            else:
                # Clock is unchanged or has regressed. Carry forward the last
                # physical component and advance the logical counter.
                physical_ns = last_phys_ns
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
                    if now_ns_base <= last_phys_ns:
                        raise TimestampError(
                            "Logical counter exhausted while waiting for clock "
                            "to advance"
                        )
                    physical_ns = now_ns_base
                    self._counter = 0

            return physical_ns, self._counter

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
            return self._backend_plugin.advance_last_ts(
                self._runner,
                new_ts=new_ts,
            )

        try:
            return _execute_with_retry(
                _op,
                max_retries=None,
                retry_delay=0.002,
                max_elapsed=TS_RETRY_MAX_ELAPSED,
                max_retry_delay=TS_RETRY_MAX_DELAY,
            )
        except OperationalError as e:  # pragma busy_timeout etc.
            if _is_locked_operational_error(e):
                raise TimestampError(
                    f"database busy while writing timestamp: {e}"
                ) from e
            raise TimestampError(
                "database error while writing timestamp; durable outcome is unknown: "
                f"{e}",
                outcome_ambiguous=True,
            ) from e

    # -----------------------------------------------------------------
    # 3. lightweight read helper when we lost the race
    # -----------------------------------------------------------------
    def _peek_last_ts(self) -> int | None:
        return self._backend_plugin.read_last_ts(self._runner)

    @staticmethod
    def validate(timestamp_str: str, exact: bool = False) -> int:
        """Validate and parse timestamp string into a 64-bit hybrid timestamp.

        This is the canonical validation logic used by the -m flag and other
        timestamp parsing needs. All extensions should use this for consistency.

        Args:
            timestamp_str: String representation of timestamp. Accepts:
                - Native 64-bit hybrid timestamp (e.g., "1837025672140161024", interchangeable with Unix nanoseconds)
                - ISO 8601 date/datetime (e.g., "2024-01-15", "2024-01-15T14:30:00")
                - Unix timestamp in seconds, milliseconds, or nanoseconds (e.g., "1705329000")
                - Explicit units: "1705329000s" (seconds), "1705329000000ms" (milliseconds),
                  "1705329000000000000ns" (nanoseconds)

                Precedence notes: ISO parsing runs before the numeric
                heuristics, so a bare 8-digit number that forms a valid
                calendar date (e.g. "10550401") is read as compact YYYYMMDD,
                not as unix seconds — use an explicit suffix ("10550401s")
                for second counts of that size. Pre-epoch dates clamp to 0
                (the Unix epoch). Digits may be any Unicode decimal digits;
                they are folded to ASCII before parsing, so a value's script
                never changes how it is interpreted.
                Fractional seconds and sign/underscore pseudo-numerics are not
                accepted. Use integer milliseconds, integer nanoseconds, or a
                native hybrid message ID when finer granularity is required.
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

        # Fold non-ASCII decimal digits to ASCII once, before any parsing.
        # int() already accepts Unicode decimal digits but
        # datetime.fromisoformat() does not, so without this the *meaning* of a
        # value would depend on its script: "20240115" read as a compact
        # YYYYMMDD date, "٢٠٢٤٠١١٥" as unix seconds. Folding first makes the
        # digits' script irrelevant to interpretation.
        if not timestamp_str.isascii():
            timestamp_str = "".join(
                str(unicodedata.decimal(char)) if char.isdecimal() else char
                for char in timestamp_str
            )

        # If exact mode, enforce strict 19-digit validation
        if exact:
            return TimestampGenerator._validate_exact_timestamp(timestamp_str)

        # Reject numeric scientific notation early for consistency.
        if _SCIENTIFIC_NOTATION_RE.fullmatch(timestamp_str):
            raise TimestampError(
                "Invalid timestamp: scientific notation not supported; "
                f"{_BOUND_PARSE_GUIDANCE}"
            )

        # Check for explicit unit suffixes
        ts = TimestampGenerator._parse_with_unit_suffix(timestamp_str)
        if ts is not None:
            return ts

        # Try formats in order of precedence
        # 1. ISO format (unambiguous)
        try:
            ts = TimestampGenerator._parse_iso8601(timestamp_str)
        except ValueError as e:
            # _parse_iso8601 raises bare ValueError for out-of-range dates;
            # validate()'s contract is TimestampError for any invalid input.
            raise TimestampError(str(e)) from None
        if ts is not None:
            return ts

        # 2. Native or Unix numeric format
        ts = TimestampGenerator._parse_native_or_unix(timestamp_str)
        if ts is not None:
            return ts

        raise TimestampError(
            f"Invalid timestamp: {timestamp_str}; {_BOUND_PARSE_GUIDANCE}"
        )

    @staticmethod
    def _validate_exact_timestamp(timestamp_str: str) -> int:
        """Validate timestamp in exact mode (strict 19-digit validation).

        Uses ``str.isdecimal()`` rather than ``str.isdigit()``: both accept
        non-ASCII decimal digits, but ``isdigit()`` also accepts characters
        such as superscripts that ``int()`` cannot parse, which would escape
        this validator as a raw ``ValueError`` instead of ``TimestampError``.
        Surrounding whitespace has already been stripped by ``validate()``.
        """
        if (
            len(timestamp_str) != TIMESTAMP_EXACT_NUM_DIGITS
            or not timestamp_str.isdecimal()
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
        suffixed_value = TimestampGenerator._split_unit_suffix(timestamp_str)
        if suffixed_value is None:
            return None
        timestamp_str, multiplier = suffixed_value
        if not timestamp_str.isdecimal():
            raise TimestampError(
                f"Invalid timestamp: {original_str}; {_BOUND_PARSE_GUIDANCE}"
            )

        try:
            val = int(timestamp_str)
            if val < 0:
                raise TimestampError("Invalid timestamp: cannot be negative")

            ns_after_epoch = int(val * multiplier)

            # Clear bottom bits for counter (hybrid timestamp format)
            time_mask = ~LOGICAL_COUNTER_MASK
            hybrid_ts = ns_after_epoch & time_mask
            if hybrid_ts >= SQLITE_MAX_INT64:
                raise TimestampError("Invalid timestamp: too far in future")
            return hybrid_ts
        except (ValueError, OverflowError) as e:
            if "Invalid timestamp" in str(e):
                raise
            raise TimestampError(f"Invalid timestamp: {original_str}") from None

    @staticmethod
    def _split_unit_suffix(timestamp_str: str) -> tuple[str, int] | None:
        """Split a timestamp value from its explicit nanosecond multiplier."""
        if timestamp_str.endswith("ns"):
            return timestamp_str[:-2], 1
        if timestamp_str.endswith("ms"):
            return timestamp_str[:-2], 1_000_000
        if (
            timestamp_str.endswith("s")
            and not timestamp_str.endswith("Z")
            # isdecimal(), not isdigit(): this value goes to int(), which
            # accepts Unicode decimal digits but rejects digit-like
            # characters such as superscripts.
            and timestamp_str[-2:-1].isdecimal()
        ):
            return timestamp_str[:-1], 1_000_000_000
        return None

    @staticmethod
    def _parse_native_or_unix(timestamp_str: str) -> int | None:
        """Parse as native timestamp or Unix timestamp based on heuristic."""
        if not timestamp_str.isdecimal():
            return None

        try:
            # Try integer first
            val = int(timestamp_str)
            if val < 0:
                raise TimestampError("Invalid timestamp: cannot be negative")

            # Native timestamps preserve epoch-nanosecond magnitude, replacing
            # the low LOGICAL_COUNTER_BITS with the logical counter. Around
            # 2025, native values are approximately 1.7e18.
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
            or (len(timestamp_str) == 8 and timestamp_str.isdecimal())
        ):
            return None

        if _ISO_FRACTIONAL_SECONDS_RE.search(timestamp_str):
            raise TimestampError(
                f"Invalid timestamp: {timestamp_str}; {_BOUND_PARSE_GUIDANCE}"
            )

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
                date_obj = date.fromisoformat(normalized)
                dt = datetime.combine(date_obj, datetime_time.min, tzinfo=UTC)
            except ValueError:
                return None  # Not a valid date format

        if dt is None:
            return None

        # Convert to UTC if timezone-aware
        if dt.tzinfo is None:
            # Assume UTC for naive datetimes
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)

        # Convert to nanoseconds after epoch
        ns_after_epoch = int(dt.timestamp() * 1_000_000_000)
        if ns_after_epoch < 0:
            # Pre-epoch dates clamp to the epoch: a bound like
            # "--after 1950-01-01" means "everything", and negative hybrid
            # timestamps are rejected by downstream bound validation.
            return 0
        # Clear bottom bits for counter (hybrid timestamp format)
        time_mask = ~LOGICAL_COUNTER_MASK
        hybrid_ts = ns_after_epoch & time_mask
        # Ensure it fits in SQLite's signed 64-bit integer
        if hybrid_ts >= SQLITE_MAX_INT64:
            raise ValueError("Invalid timestamp: too far in future")
        return hybrid_ts

    @staticmethod
    def _parse_numeric_timestamp(timestamp_str: str) -> int | None:
        """Parse a decimal integer timestamp with the unit heuristic."""
        try:
            int_val = int(timestamp_str)
            integer_digits = len(timestamp_str.lstrip("0") or "0")

            # Heuristic based on number of digits for the integer part
            # Current time (2025) is ~10 digits in seconds, ~13 digits in ms, ~19 digits in ns

            if integer_digits > 16:  # Likely nanoseconds
                ns_after_epoch = int_val
            elif integer_digits > 11:  # Likely milliseconds
                ns_after_epoch = int_val * 1_000_000
            else:  # Likely seconds
                ns_after_epoch = int_val * 1_000_000_000

            # Clear bottom bits for counter (hybrid timestamp format)
            time_mask = ~LOGICAL_COUNTER_MASK
            hybrid_ts = ns_after_epoch & time_mask
            # Ensure it fits in signed 64-bit integer
            if hybrid_ts >= SQLITE_MAX_INT64:
                raise ValueError("Invalid timestamp: too far in future")
            return hybrid_ts

        except (ValueError, OverflowError):
            return None


# ~
