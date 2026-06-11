"""Property-based tests for TimestampGenerator.validate().

validate() is the canonical multi-format timestamp parser used by the CLI
(-m / --since flags) and extensions. Formats: native 19-digit hybrid IDs,
ISO-8601 dates/datetimes, unix seconds/ms/ns (bare, by digit-count heuristic,
or with explicit s/ms/ns suffixes). These properties pin its contract:

1. Totality: any string either parses to an int or raises TimestampError.
2. Exact mode accepts exactly the in-range 19-digit strings.
3. Native IDs round-trip through str().
4. Equivalent representations of the same instant parse equal.

Pure functions only — no broker, no fixtures. The conftest hook will mark
this module sqlite_only, which is intended: nothing here touches a backend.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from simplebroker._constants import (
    LOGICAL_COUNTER_MASK,
    SQLITE_MAX_INT64,
    UNIX_NATIVE_BOUNDARY,
)
from simplebroker._exceptions import TimestampError
from simplebroker._timestamp import TimestampGenerator

NS_PER_S = 1_000_000_000
# Hybrid timestamps zero their bottom 12 bits for a logical counter; parsed
# wall-clock inputs are quantized to this granularity (4096 ns).
QUANTUM = LOGICAL_COUNTER_MASK + 1


@given(st.text(max_size=64))
@example("9999-01-01")  # F1: leaked ValueError before the Task 4 fix
@example("0001-01-01")  # F2: parses to a negative int (pinned below)
@example("١٢٣s")  # F3: non-ASCII digits accepted (pinned below)
@example("  5s  ")  # whitespace is stripped first (documented behavior)
@example("1e9")  # scientific notation is rejected
@example("2024-01-15T14:30:00Z")
def test_validate_is_total(s: str) -> None:
    """Contract: validate() returns an int or raises TimestampError — never
    any other exception type (sbqueue/CLI callers catch TimestampError only).
    """
    try:
        result = TimestampGenerator.validate(s)
    except TimestampError:
        return
    assert isinstance(result, int)


@given(st.integers(min_value=10**18, max_value=SQLITE_MAX_INT64 - 1))
def test_exact_mode_round_trips_in_range_ids(ts: int) -> None:
    """Contract: every real message ID (19 digits, < 2**63) survives
    str() -> validate(exact=True) unchanged. This is the -m flag's path."""
    assert TimestampGenerator.validate(str(ts), exact=True) == ts


@given(st.integers(min_value=SQLITE_MAX_INT64, max_value=10**19 - 1))
def test_exact_mode_rejects_out_of_range_19_digit_ids(ts: int) -> None:
    """19-digit strings at or above 2**63 are not valid IDs."""
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(str(ts), exact=True)


@given(
    st.text(max_size=30).filter(
        # Mirror the implementation gate (len==19 and str.isdigit() after
        # strip) exactly, so this property is the complement of acceptance.
        # str.isdigit (not an ASCII check) is intentional — see finding F3.
        lambda s: not (len(s.strip()) == 19 and s.strip().isdigit())
    )
)
def test_exact_mode_rejects_everything_else(s: str) -> None:
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(s, exact=True)


@given(st.integers(min_value=UNIX_NATIVE_BOUNDARY, max_value=SQLITE_MAX_INT64 - 1))
def test_native_ids_round_trip_in_default_mode(ts: int) -> None:
    """Bare integers at or above 2**44 are treated as native IDs and returned
    verbatim (the digit-count heuristic only applies below that boundary)."""
    assert TimestampGenerator.validate(str(ts)) == ts


@given(st.integers(min_value=0, max_value=9_000_000_000))
def test_unit_suffixes_and_bare_seconds_agree(n: int) -> None:
    """The same instant expressed as Ns / N*1000ms / N*1e9ns / bare N parses
    to one identical hybrid timestamp (integer math end-to-end). Bare numbers
    of <= 11 digits are read as seconds by the documented heuristic; 9e9 stays
    inside both that heuristic and the 2**63 ns range (~year 2255)."""
    expected = (n * NS_PER_S) & ~LOGICAL_COUNTER_MASK
    assert TimestampGenerator.validate(f"{n}s") == expected
    assert TimestampGenerator.validate(f"{n * 1000}ms") == expected
    assert TimestampGenerator.validate(f"{n * NS_PER_S}ns") == expected
    if len(str(n)) != 8:
        # 8-digit bare numbers that form a valid calendar date are hijacked
        # by the YYYYMMDD ISO heuristic before the seconds heuristic —
        # FINDING F7, pinned in the quirk test below.
        assert TimestampGenerator.validate(str(n)) == expected


@given(st.integers(min_value=0, max_value=4_102_444_800))  # 1970 .. 2100-01-01
def test_iso_datetimes_agree_with_unix_seconds(n: int) -> None:
    """An ISO datetime and its unix-seconds form agree to within one logical
    quantum. NOT exact equality: the ISO path multiplies a float (datetime
    .timestamp() * 1e9, _timestamp.py:459) whose rounding can land one 4096ns
    quantum away from the integer-math suffix path. Asserting exact equality
    here WILL flake — do not 'fix' a failure by tightening this."""
    iso = datetime.fromtimestamp(n, tz=UTC).isoformat()
    assert (
        abs(TimestampGenerator.validate(iso) - TimestampGenerator.validate(f"{n}s"))
        <= QUANTUM
    )


@given(st.dates(min_value=date(1970, 1, 1), max_value=date(2200, 12, 31)))
def test_date_only_iso_means_midnight_utc(d: date) -> None:
    """A bare YYYY-MM-DD parses identically to its explicit midnight-UTC
    datetime (both run through the same float path, so equality is exact)."""
    assert TimestampGenerator.validate(d.isoformat()) == TimestampGenerator.validate(
        f"{d.isoformat()}T00:00:00Z"
    )


@given(st.dates(min_value=date(2263, 1, 1), max_value=date(9999, 12, 31)))
def test_far_future_iso_raises_timestamp_error(d: date) -> None:
    """Dates beyond the 2**63-ns horizon (April 2262) are invalid. Guards the
    Task 4 fix; the boundary year 2262 itself is deliberately excluded."""
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(d.isoformat())


def test_known_quirk_pre_epoch_iso_returns_negative_int() -> None:
    """FINDING F2 (pinned, not endorsed): pre-epoch ISO dates parse to
    negative ints, which every downstream bound check then rejects
    (validate_timestamp_bound requires >= 0). If this starts failing, the
    parser's pre-epoch behavior changed — update the findings log."""
    result = TimestampGenerator.validate("0001-01-01")
    assert isinstance(result, int)
    assert result < 0


def test_known_quirk_non_ascii_digits_accepted() -> None:
    """FINDING F3 (pinned, not endorsed): int() accepts any Unicode decimal
    digits, so Eastern Arabic numerals parse like ASCII ones."""
    assert (
        TimestampGenerator.validate("١٢٣s") == (123 * NS_PER_S) & ~LOGICAL_COUNTER_MASK
    )


def test_known_quirk_8_digit_bare_numbers_can_parse_as_yyyymmdd() -> None:
    """FINDING F7 (pinned, not endorsed; discovered by the equivalence
    property at the Task 6 gate): an 8-digit all-digit string that forms a
    valid calendar date is consumed by the ISO YYYYMMDD heuristic
    (_timestamp.py, _parse_iso8601's len==8 check) BEFORE the documented
    unix-seconds heuristic ever sees it. So some legal 1970–1973 unix-seconds
    inputs are silently reinterpreted as ancient or far-future dates."""
    # 10550401 (unix seconds in 1973) parses as 1055-04-01 -> negative (F2).
    assert TimestampGenerator.validate("10550401") < 0
    # 25980801 parses as 2598-08-01 -> beyond the 2262 horizon -> rejected.
    with pytest.raises(TimestampError):
        TimestampGenerator.validate("25980801")
    # 8-digit numbers that do NOT form a valid date still read as seconds.
    assert (
        TimestampGenerator.validate("99999999")
        == (99999999 * NS_PER_S) & ~LOGICAL_COUNTER_MASK
    )
