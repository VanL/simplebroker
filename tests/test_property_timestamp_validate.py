"""Property-based tests for TimestampGenerator.validate().

validate() is the canonical multi-format timestamp parser used by CLI exact
message targeting and the --after/--before bound flags, plus extensions. Formats:
native 19-digit hybrid IDs,
ISO-8601 dates/datetimes, unix seconds/ms/ns (bare, by digit-count heuristic,
or with explicit s/ms/ns suffixes). These properties pin its contract:

1. Totality: any string either parses to an int or raises TimestampError.
2. Exact mode accepts exactly the in-range 19-digit strings.
3. Native IDs round-trip through str().
4. Equivalent representations of the same instant parse equal.

The parser properties are pure except for one SQLite-backed proof that actual
generated message IDs round-trip. The conftest hook marks this module
``sqlite_only``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from simplebroker import Queue
from simplebroker._constants import (
    LOGICAL_COUNTER_MASK,
    SQLITE_MAX_INT64,
)
from simplebroker._exceptions import TimestampError
from simplebroker._timestamp import TimestampGenerator

NS_PER_S = 1_000_000_000


@given(st.text(max_size=64))
@example("9999-01-01")  # F1: leaked ValueError before the Task 4 fix
@example("0001-01-01")  # F2 (resolved): pre-epoch clamps to 0 (pinned below)
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
        # Mirror the implementation gate (len==19 and str.isdecimal() after
        # strip) exactly, so this property is the complement of acceptance.
        # isdecimal() (not an ASCII check) preserves Unicode decimal digits.
        lambda s: not (len(s.strip()) == 19 and s.strip().isdecimal())
    )
)
def test_exact_mode_rejects_everything_else(s: str) -> None:
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(s, exact=True)


def test_native_ids_round_trip_in_default_mode(tmp_path: Path) -> None:
    """Generated IDs and the documented native example round-trip."""
    with Queue("native", db_path=str(tmp_path / "native.db")) as queue:
        generated = [queue.write(f"message-{index}") for index in range(3)]

    documented = 1837025672140161024
    for message_id in [*generated, documented]:
        assert TimestampGenerator.validate(str(message_id)) == message_id


@given(st.integers(min_value=0, max_value=9_000_000_000))
def test_explicit_unit_suffixes_agree(n: int) -> None:
    """Explicit seconds, milliseconds, and nanoseconds for one instant agree."""
    expected = (n * NS_PER_S) & ~LOGICAL_COUNTER_MASK
    assert TimestampGenerator.validate(f"{n}s") == expected
    assert TimestampGenerator.validate(f"{n * 1000}ms") == expected
    assert TimestampGenerator.validate(f"{n * NS_PER_S}ns") == expected


@given(st.integers(min_value=0, max_value=9_000_000_000))
@example(4_638_902_403)  # 2117 float conversion crossed one 4096ns grain
def test_iso_datetimes_agree_with_unix_seconds(n: int) -> None:
    """Equivalent ISO and integral-second bounds select the same grain."""
    iso = datetime.fromtimestamp(n, tz=UTC).isoformat()
    assert TimestampGenerator.validate(iso) == TimestampGenerator.validate(f"{n}s")


@given(st.dates(min_value=date(1970, 1, 1), max_value=date(2200, 12, 31)))
def test_date_only_iso_means_midnight_utc(d: date) -> None:
    """A bare YYYY-MM-DD parses identically to its explicit midnight-UTC
    datetime through the same exact integer epoch conversion."""
    assert TimestampGenerator.validate(d.isoformat()) == TimestampGenerator.validate(
        f"{d.isoformat()}T00:00:00Z"
    )


@given(st.dates(min_value=date(2263, 1, 1), max_value=date(9999, 12, 31)))
def test_far_future_iso_raises_timestamp_error(d: date) -> None:
    """Dates beyond the 2**63-ns horizon (April 2262) are invalid. Guards the
    Task 4 fix; the boundary year 2262 itself is deliberately excluded."""
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(d.isoformat())


@given(st.dates(min_value=date(1, 1, 1), max_value=date(1969, 12, 31)))
def test_pre_epoch_iso_dates_clamp_to_epoch(d: date) -> None:
    """Pre-epoch ISO dates clamp to the Unix epoch (finding F2, resolved):
    a bound like "--after 1950-01-01" means "everything". Previously these
    parsed to negative values that the CLI silently accepted while API bound
    checks rejected them; clamping unifies both on the sensible reading."""
    assert TimestampGenerator.validate(d.isoformat()) == 0


def test_pre_epoch_iso_datetimes_clamp_to_epoch() -> None:
    """The clamp covers datetimes too, including the last pre-epoch second."""
    assert TimestampGenerator.validate("1969-12-31T23:59:59Z") == 0
    assert TimestampGenerator.validate("0001-01-01T00:00:00") == 0


def test_non_ascii_digits_are_script_invariant() -> None:
    """A value's script must not change how it is interpreted.

    int()/float() accept Unicode decimal digits but datetime.fromisoformat()
    does not, so before digits were folded to ASCII an 8-digit value took the
    YYYYMMDD path in ASCII and silently fell through to the unix-seconds path
    in any other script -- the same glyph sequence meaning two instants ~54
    years apart.
    """
    ascii_form = "20240115"
    for other_script in ("٢٠٢٤٠١١٥", "߂߀߂߄߀߁߁߅"):
        assert TimestampGenerator.validate(other_script) == (
            TimestampGenerator.validate(ascii_form)
        )

    # Folding applies to suffixed and dashed forms too, not just bare digits.
    assert TimestampGenerator.validate("١٧٠٥٣٢٩٠٠٠s") == (
        TimestampGenerator.validate("1705329000s")
    )
    assert TimestampGenerator.validate("٢٠٢٤-٠١-١٥") == (
        TimestampGenerator.validate("2024-01-15")
    )


def test_known_quirk_8_digit_bare_numbers_can_parse_as_yyyymmdd() -> None:
    """FINDING F7 (pinned, not endorsed; discovered by the equivalence
    property at the Task 6 gate): an 8-digit all-digit string that forms a
    valid calendar date is consumed by the ISO YYYYMMDD heuristic
    (_timestamp.py, _parse_iso8601's len==8 check) BEFORE the documented
    unix-seconds heuristic ever sees it. So some legal 1970–1973 unix-seconds
    inputs are silently reinterpreted as ancient or far-future dates."""
    # 10550401 (unix seconds in 1973) parses as 1055-04-01, a pre-epoch date
    # that now clamps to the epoch (F2) — still a date, not seconds.
    assert TimestampGenerator.validate("10550401") == 0
    # 25980801 parses as 2598-08-01 -> beyond the 2262 horizon -> rejected.
    with pytest.raises(TimestampError):
        TimestampGenerator.validate("25980801")
    # 8-digit numbers that do NOT form a valid date still read as seconds.
    assert (
        TimestampGenerator.validate("99999999")
        == (99999999 * NS_PER_S) & ~LOGICAL_COUNTER_MASK
    )
