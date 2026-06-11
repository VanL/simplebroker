"""Property-based tests for the dump/load format (SQLite engine).

Three properties, asserted over generated broker states:
  P1 round-trip fixed point: nonheader(dump(load(dump(S)))) == nonheader(dump(S))
  P2 filter algebra: filtered dumps are exact line-subsets of the unfiltered
     dump, selected per the documented include/exclude rule
  P3 parser totality: load_lines on arbitrary text never raises anything but
     the documented ValueError (with a line number) or broker errors

The properties are format/logic-level, so they run on the fast SQLite engine;
the example-based suites cover the backend matrix. Example counts come from
the active Hypothesis profile (tests/conftest.py).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from simplebroker import Queue, dump_lines, load_lines, open_broker
from simplebroker._dump import _selected
from simplebroker.db import QUEUE_NAME_PATTERN

# Queue names: generated from the broker's own validation pattern, the same
# approach as test_property_queue_names.py (its findings F4 apply: from_regex
# can emit trailing newlines, which the validator rejects — filter them).
# Bodies: unicode without NUL or surrogates (findings F6/F8: those are
# rejected/divergent at the broker layer, pinned elsewhere).
_queue_names = st.from_regex(QUEUE_NAME_PATTERN).filter(
    lambda s: 0 < len(s) <= 32 and not s.endswith("\n")
)
_bodies = st.text(
    alphabet=st.characters(exclude_characters="\x00", exclude_categories=("Cs",)),
    max_size=200,
)
_globs = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Nd"), whitelist_characters="*?_-"
    ),
    min_size=1,
    max_size=8,
)


@st.composite
def broker_states(draw: st.DrawFn) -> list[tuple[str, list[str], int]]:
    """(queue, bodies, claim_count) triples describing a broker to build."""
    queues = draw(st.lists(_queue_names, min_size=0, max_size=4, unique=True))
    state = []
    for queue in queues:
        bodies = draw(st.lists(_bodies, min_size=1, max_size=6))
        claimed = draw(st.integers(min_value=0, max_value=len(bodies)))
        state.append((queue, bodies, claimed))
    return state


def _build(tmp_path: Path, state: list[tuple[str, list[str], int]]) -> str:
    db = str(tmp_path / f"{uuid.uuid4().hex}.db")
    for queue, bodies, claimed in state:
        q = Queue(queue, db_path=db)
        for body in bodies:
            q.write(body)
        for _ in range(claimed):
            q.read()
    return db


@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(state=broker_states())
def test_round_trip_fixed_point_property(
    tmp_path: Path, state: list[tuple[str, list[str], int]]
) -> None:
    src = _build(tmp_path, state)
    with open_broker(src) as broker:
        original = list(dump_lines(broker))
    dst = str(tmp_path / f"{uuid.uuid4().hex}.db")
    with open_broker(dst) as broker:
        load_lines(broker, original)
        redump = list(dump_lines(broker))
    assert redump[1:] == original[1:]


@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(
    state=broker_states(),
    include=st.one_of(st.none(), st.lists(_globs, min_size=1, max_size=3)),
    exclude=st.one_of(st.none(), st.lists(_globs, min_size=1, max_size=3)),
)
def test_filter_algebra_property(
    tmp_path: Path,
    state: list[tuple[str, list[str], int]],
    include: list[str] | None,
    exclude: list[str] | None,
) -> None:
    src = _build(tmp_path, state)
    with open_broker(src) as broker:
        full = list(dump_lines(broker))
        filtered = list(dump_lines(broker, include=include, exclude=exclude))

    # P2a: every filtered line exists verbatim in the full dump (no rewriting)
    assert set(filtered[1:]) <= set(full[1:])
    # P2b: exactly the selected queues appear
    full_queues = {
        json.loads(ln)["queue"]
        for ln in full[1:]
        if json.loads(ln)["type"] == "message"
    }
    filtered_queues = {
        json.loads(ln)["queue"]
        for ln in filtered[1:]
        if json.loads(ln)["type"] == "message"
    }
    assert filtered_queues == {q for q in full_queues if _selected(q, include, exclude)}
    # P2c: relative order is preserved
    positions = {ln: i for i, ln in enumerate(full)}
    kept = [positions[ln] for ln in filtered[1:]]
    assert kept == sorted(kept)


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(lines=st.lists(st.text(max_size=80), max_size=8))
def test_parser_totality_property(lines: list[str]) -> None:
    """load_lines on junk: documented ValueError or success, nothing else.

    _NullBroker is a deliberate, narrow exception to the no-test-doubles
    rule, sanctioned for this test only: the property under test is the
    PARSER's exception contract, the broker is irrelevant to it, and running
    hundreds of generated examples against real databases buys nothing but
    runtime. The example-based and P1/P2 tests all use real brokers.
    """
    sink: list[object] = []

    class _NullBroker:
        def insert_messages(self, records: object) -> None:
            sink.append(records)

        def add_alias(self, alias: str, target: str) -> None:
            sink.append((alias, target))

    try:
        load_lines(_NullBroker(), lines)  # type: ignore[arg-type]
    except ValueError as exc:
        assert "line" in str(exc) or "header" in str(exc)
