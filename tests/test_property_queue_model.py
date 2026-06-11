"""Stateful model-based test: real queues vs a plain-Python reference model.

Hypothesis drives random operation sequences against real Queue objects on
the active backend while a dict-of-lists model predicts every return value;
@invariant() re-derives stats/has_pending/exists from the model after every
step. Run it on all three backends (it is marked shared) and any semantic
divergence between SQLite, Postgres, and Redis shows up as a shrunk,
replayable operation script.

Read Part I of docs/plans/2026-06-11-hypothesis-property-testing-plan.md
(domain crash course + design decisions) before editing anything here.
"""

from __future__ import annotations

import itertools
from bisect import insort

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    rule,
    run_state_machine_as_test,
)

from simplebroker._targets import ResolvedTarget

from .helper_scripts.broker_factory import make_queue

pytestmark = pytest.mark.shared

QUEUE_KEYS = ("alpha", "bravo", "charlie")

# Printable-ASCII bodies keep shrunk failure scripts readable; full-Unicode
# body fidelity is already covered by test_property_message_roundtrip.py.
BODIES = st.text(
    alphabet=st.characters(min_codepoint=32, max_codepoint=126), max_size=20
)

# Claimed rows must be deterministic for exact include_claimed/stats
# predictions; see the Stage 3 design notes in the plan.
MACHINE_CONFIG = {"BROKER_AUTO_VACUUM": 0}

_EXECUTIONS = itertools.count()


class QueueModelMachine(RuleBasedStateMachine):
    """One execution = one isolated trio of real queues + a reference model.

    Model representation: pending[key] and claimed[key] are lists of
    (ts, body) kept in ascending-ts order. FIFO == ascending ts, so
    "oldest pending" is always pending[key][0].
    """

    # Injected by the test wrapper below (the active backend's target).
    target: ResolvedTarget

    def __init__(self) -> None:
        super().__init__()
        prefix = f"prop{next(_EXECUTIONS)}"
        self._queues = {
            key: make_queue(f"{prefix}_{key}", self.target, config=MACHINE_CONFIG)
            for key in QUEUE_KEYS
        }
        self.pending: dict[str, list[tuple[int, str]]] = {k: [] for k in QUEUE_KEYS}
        self.claimed: dict[str, list[tuple[int, str]]] = {k: [] for k in QUEUE_KEYS}

    def teardown(self) -> None:
        # Purge (claimed rows included) so executions never see each other's
        # rows on backends whose state outlives a single execution.
        for q in self._queues.values():
            try:
                q.delete()
            finally:
                q.close()

    # ---------- model helpers ----------

    def _entries(self, key: str) -> list[tuple[int, str]]:
        return self.pending[key] + self.claimed[key]

    def _max_known_ts(self) -> int:
        return max((ts for k in QUEUE_KEYS for ts, _ in self._entries(k)), default=0)

    # ---------- rules ----------

    @rule(key=st.sampled_from(QUEUE_KEYS), body=BODIES)
    def write(self, key: str, body: str) -> None:
        q = self._queues[key]
        q.write(body)
        # Single-threaded, so meta.last_ts after our write IS our write's id.
        ts = q.refresh_last_ts()
        assert ts > self._max_known_ts(), (
            "timestamps must be globally strictly increasing"
        )
        self.pending[key].append((ts, body))

    @rule(key=st.sampled_from(QUEUE_KEYS))
    def read_one(self, key: str) -> None:
        got = self._queues[key].read_one(with_timestamps=True)
        if not self.pending[key]:
            assert got is None
        else:
            ts, body = self.pending[key].pop(0)
            assert got == (body, ts)
            insort(self.claimed[key], (ts, body))

    # ---------- invariants ----------

    @invariant()
    def counts_match_the_model(self) -> None:
        for key in QUEUE_KEYS:
            stats = self._queues[key].stats()
            n_pending = len(self.pending[key])
            n_claimed = len(self.claimed[key])
            assert (stats.pending, stats.claimed, stats.total) == (
                n_pending,
                n_claimed,
                n_pending + n_claimed,
            ), f"stats mismatch on {key!r}"
            assert self._queues[key].has_pending() == bool(self.pending[key])
            assert self._queues[key].exists() == bool(n_pending + n_claimed)


def test_queue_semantics_match_reference_model(broker_target) -> None:
    """Run the machine against the active backend (sqlite by default;
    bin/pytest-pg and bin/pytest-redis run the identical machine on
    Postgres and Redis)."""

    class Machine(QueueModelMachine):
        target = broker_target

    # Budget pinned regardless of HYPOTHESIS_PROFILE: executions x steps x
    # per-step invariants is the cost driver here, on three backends. Tune
    # these two numbers if a backend is slow (Task 13 step 3) — never delete
    # rules to save time.
    run_state_machine_as_test(
        Machine,
        settings=settings(
            max_examples=15,
            stateful_step_count=25,
            deadline=None,
            suppress_health_check=[HealthCheck.too_slow],
        ),
    )
