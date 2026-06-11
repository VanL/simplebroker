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

from hypothesis import example, given
from hypothesis import strategies as st

from simplebroker._constants import LOGICAL_COUNTER_MASK
from simplebroker._exceptions import TimestampError
from simplebroker._timestamp import TimestampGenerator

# Imports stay minimal here on purpose: ruff --fix strips unused imports at
# commit time, so each task adds imports only when its code first uses them
# (Task 5 extends this block).

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
