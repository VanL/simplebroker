"""PostgreSQL generated-write allocation under the high-water row lock.

An ordinary generated write locks the singleton ``meta`` row, allocates the
committed ID server-side, inserts the row, notifies, and commits in one
pipelined exchange. These real-PostgreSQL tests pin the guarantees the row lock
provides (uniqueness, durable monotonicity, commit order equal to ID order for
checkpoint readers, atomic allocation and row) and the lock order that keeps
writers from deadlocking with broadcast.

Spec: docs/specs/13-message-identity.md [SB-ID-1] through [SB-ID-4];
docs/specs/14-timestamp-selection.md [SB-SELECT-5]. Plan:
docs/plans/2026-11-08-pg-write-lock-hold-plan.md.
"""

from __future__ import annotations

import threading
import time
import warnings
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

import pytest
from simplebroker_pg import PostgresRunner
from simplebroker_pg import _sql as pg_sql
from simplebroker_pg import runner as runner_module

from simplebroker import _timestamp as timestamp_module
from simplebroker._backend_plugins import BackendPlugin
from simplebroker._constants import (
    LOGICAL_COUNTER_MASK,
    MAX_LOGICAL_COUNTER,
    NS_PER_SECOND,
)
from simplebroker._exceptions import TimestampError
from simplebroker.db import BrokerCore
from simplebroker.ext import IntegrityError

pytestmark = [pytest.mark.pg_only]


def _classify(sql: str) -> str:
    normalized = " ".join(sql.split())
    if normalized.startswith("SELECT last_ts FROM meta") and "FOR UPDATE" in normalized:
        return "lock_high_water"
    if normalized.startswith("WITH allocated AS"):
        return "allocate_insert"
    if normalized.startswith("UPDATE meta") and "last_ts" in normalized:
        return "advance"
    if "FROM meta" in normalized and "last_ts" in normalized:
        return "read_meta"
    if "INSERT INTO messages" in normalized:
        return "insert"
    return "other"


class _ExchangeRecorder:
    """Pass-through spy that records each client exchange with PostgreSQL.

    Every call is delegated to the real runner; nothing is faked. A
    ``run_transaction`` call is one pipelined exchange and is recorded with the
    classified statements it carries.
    """

    def __init__(self, inner: PostgresRunner) -> None:
        self._inner = inner
        self.exchanges: list[tuple[str, tuple[str, ...]]] = []

    def run(
        self, sql: str, params: tuple[Any, ...] = (), *, fetch: bool = False
    ) -> Any:
        self.exchanges.append(("run", (_classify(sql),)))
        return self._inner.run(sql, params, fetch=fetch)

    def run_transaction(
        self, statements: tuple[tuple[str, tuple[Any, ...]], ...]
    ) -> Any:
        self.exchanges.append(
            ("run_transaction", tuple(_classify(sql) for sql, _ in statements))
        )
        return self._inner.run_transaction(statements)

    def begin_immediate(self) -> None:
        self.exchanges.append(("begin", ()))
        self._inner.begin_immediate()

    def commit(self) -> None:
        self.exchanges.append(("commit", ()))
        self._inner.commit()

    def rollback(self) -> None:
        self.exchanges.append(("rollback", ()))
        self._inner.rollback()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


@pytest.fixture
def second_core(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
) -> Iterator[Callable[[], BrokerCore]]:
    """Create extra cores with their own runners (separate connections)."""
    created: list[tuple[BrokerCore, PostgresRunner]] = []

    def make() -> BrokerCore:
        runner = PostgresRunner(pg_dsn, schema=pg_schema)
        core = BrokerCore(runner, backend_plugin=pg_plugin)
        created.append((core, runner))
        return core

    yield make
    for core, runner in created:
        core.close()
        runner.shutdown()


def _future_high_water(logical: int) -> int:
    physical = (time.time_ns() + 5 * NS_PER_SECOND) & ~LOGICAL_COUNTER_MASK
    return physical | logical


def test_stale_candidate_write_is_one_exchange_allocating_above_high_water(
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
    second_core: Callable[[], BrokerCore],
) -> None:
    """A writer whose candidate is behind durable high-water neither retries nor
    holds the row lock across client round trips.

    Before this change the write was BEGIN, a failed compare-and-advance, a
    ``meta`` re-read, a second compare-and-advance, INSERT, and COMMIT: six
    exchanges, three of them while holding the high-water row lock.
    """
    recorder = _ExchangeRecorder(pg_runner)
    writer = BrokerCore(recorder, backend_plugin=pg_plugin)
    try:
        writer.write("jobs", "warm")
        high_water = _future_high_water(logical=7)
        assert second_core().advance_last_timestamp(high_water) == high_water

        recorder.exchanges.clear()
        written = writer.write("jobs", "stale candidate")

        assert recorder.exchanges == [
            ("run_transaction", ("lock_high_water", "allocate_insert"))
        ]
        # Same physical component, next logical value: what the generator
        # itself computes when the clock has not passed the stored high-water.
        assert written == high_water + 1
        assert pg_plugin.read_last_ts(pg_runner) == written
        assert writer.get_cached_last_timestamp() == written
        assert writer.peek_one("jobs", exact_timestamp=written) == (
            "stale candidate",
            written,
        )
        assert writer.get_conflict_metrics() == {
            "ts_conflict_count": 0,
            "ts_resync_count": 0,
        }
    finally:
        writer.close()


def test_exhausted_high_water_counter_refuses_without_mutation(
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
    second_core: Callable[[], BrokerCore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The server never rolls the logical counter into the next physical grain.

    [SB-ID-4]: after a far-future exact insert, later allocations consume the
    remaining logical values and then fail until the clock catches up. The
    writer's cache is stale, so its candidate reaches the server, which must
    allocate nothing; the client then refreshes and its generator refuses.
    """
    recorder = _ExchangeRecorder(pg_runner)
    writer = BrokerCore(recorder, backend_plugin=pg_plugin)
    try:
        writer.write("jobs", "warm")
        future_base = _future_high_water(logical=0)
        exact_id = future_base | (MAX_LOGICAL_COUNTER - 2)
        second_core().insert_messages([("jobs", "future exact ID", exact_id)])
        exhausted = future_base | LOGICAL_COUNTER_MASK
        assert pg_plugin.read_last_ts(pg_runner) == exhausted

        monkeypatch.setattr(timestamp_module, "MAX_ITERATIONS", 0)
        monkeypatch.setattr(timestamp_module, "_time_ns", lambda: future_base - 1)
        recorder.exchanges.clear()

        with pytest.raises(TimestampError, match="Logical counter exhausted"):
            writer.write("jobs", "refused")

        # Positive control: the stale candidate did reach the server, which had
        # the opportunity to roll the counter over and declined.
        assert recorder.exchanges[0] == (
            "run_transaction",
            ("lock_high_water", "allocate_insert"),
        )
        assert pg_plugin.read_last_ts(pg_runner) == exhausted
        assert writer.peek_many("jobs", with_timestamps=False) == [
            "warm",
            "future exact ID",
        ]
    finally:
        writer.close()


def test_queued_writer_does_not_deadlock_broadcast_lock_order(
    pg_core: BrokerCore,
    pg_plugin: BackendPlugin,
    pg_dsn: str,
    pg_schema: str,
    pg_wait_for_blocked: Callable[[list[int], int, threading.Event, float], None],
) -> None:
    """A writer queued on the high-water row holds no ``messages`` lock.

    Broadcast takes the row lock and then ``SHARE ROW EXCLUSIVE`` on
    ``messages``. A single-statement writer would already hold ``ROW EXCLUSIVE``
    on ``messages`` (taken at statement analysis) while waiting for the row,
    and PostgreSQL would abort one side with ``deadlock detected``. The hook is
    called directly: ``write()``'s lock-contention retry would otherwise absorb
    the deadlock and hide the inverted order.
    """
    pg_core.write("alpha", "seed")
    broadcast_runner = PostgresRunner(pg_dsn, schema=pg_schema)
    writer_runner = PostgresRunner(pg_dsn, schema=pg_schema)
    write_generated = pg_plugin.write_generated_message  # type: ignore[attr-defined]
    candidate = time.time_ns() & ~LOGICAL_COUNTER_MASK
    finished = threading.Event()
    contender_pids: list[int] = []

    def write_queued() -> int | None:
        try:
            contender_pids.append(writer_runner._get_thread_conn().info.backend_pid)
            return cast(
                "int | None",
                write_generated(
                    writer_runner, queue="alpha", message="queued", candidate=candidate
                ),
            )
        finally:
            finished.set()

    executor = ThreadPoolExecutor(max_workers=1)
    try:
        broadcast_runner.begin_immediate()
        broadcast_runner.run(pg_sql.LOCK_LAST_TS_ROW, fetch=True)
        queued = executor.submit(write_queued)
        pg_wait_for_blocked(
            contender_pids,
            broadcast_runner._get_thread_conn().info.backend_pid,
            finished,
            5.0,
        )

        broadcast_runner.run(pg_sql.LOCK_BROADCAST_SCOPE)
        assert not finished.is_set()
        broadcast_runner.commit()

        # Re-raises the writer's error, e.g. "deadlock detected".
        assert queued.result(timeout=5.0) == candidate
        assert pg_core.peek_many("alpha", with_timestamps=False) == ["seed", "queued"]
    finally:
        if broadcast_runner._in_transaction():
            broadcast_runner.rollback()
        executor.shutdown(wait=True)
        broadcast_runner.shutdown()
        writer_runner.shutdown()


def test_generated_write_conflict_rolls_back_allocation(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
) -> None:
    """Allocation and row commit together or not at all ([SB-ID-2]).

    The high-water is corrupted below a stored row so the server-chosen ID
    collides; the failed insert must also undo the high-water advance, and the
    connection must stay usable.
    """
    # Mid-counter occupant so high-water one below it still has logical room;
    # a stale candidate then makes the server choose last_ts + 1 == occupant.
    occupant = (time.time_ns() & ~LOGICAL_COUNTER_MASK) | 5
    pg_core.insert_messages([("jobs", "occupant", occupant)])
    pg_plugin.write_last_ts(pg_runner, occupant - 1)
    write_generated = pg_plugin.write_generated_message  # type: ignore[attr-defined]

    with pytest.raises(IntegrityError):
        write_generated(pg_runner, queue="jobs", message="collides", candidate=1)

    assert pg_plugin.read_last_ts(pg_runner) == occupant - 1
    assert pg_core.peek_many("jobs", with_timestamps=False) == ["occupant"]

    after = write_generated(
        pg_runner, queue="jobs", message="after", candidate=occupant + 1
    )
    assert after == occupant + 1
    assert pg_plugin.read_last_ts(pg_runner) == after


def test_persistently_conflicting_candidate_self_heals_after_resync(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replacement proof for the shared "unresolvable conflict" case.

    With a corrupted high-water and a candidate that always names a stored
    row, the write's existing ladder (backoff, then guarded resync) suffices:
    after resync the server allocates above the repaired high-water, so the
    conflict cannot persist on PostgreSQL.
    """
    occupant = pg_core.write("jobs", "occupant")
    pg_plugin.write_last_ts(pg_runner, occupant - 1)
    monkeypatch.setattr(
        pg_core._timestamp_gen, "_reserve_candidates", lambda count: [occupant]
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        written = pg_core.write("jobs", "healed")

    assert written == occupant + 1
    assert pg_core.get_conflict_metrics() == {
        "ts_conflict_count": 2,
        "ts_resync_count": 1,
    }
    assert pg_core.peek_many("jobs") == [("occupant", occupant), ("healed", written)]


class _CheckpointReader:
    """Poll with a monotone ``after_timestamp`` checkpoint, as peek watchers do."""

    def __init__(self, core: BrokerCore, queue: str) -> None:
        self._core = core
        self._queue = queue
        self.checkpoint = 0
        self.seen: dict[str, int] = {}

    def drain(self) -> None:
        rows = cast(
            "list[tuple[str, int]]",
            self._core.peek_many(
                self._queue,
                limit=1000,
                with_timestamps=True,
                after_timestamp=self.checkpoint,
            ),
        )
        for body, ts in rows:
            self.seen[body] = ts
            self.checkpoint = max(self.checkpoint, ts)


def _write_concurrently(
    writers: list[BrokerCore],
    per_writer: int,
    poll: Callable[[], None],
) -> list[list[int]]:
    """Start writers together, call ``poll`` until all finish, return their IDs."""
    barrier = threading.Barrier(len(writers) + 1)

    def write_all(writer: BrokerCore, index: int) -> list[int]:
        barrier.wait(timeout=10)
        return [writer.write("race", f"w{index}-{n}") for n in range(per_writer)]

    with ThreadPoolExecutor(max_workers=len(writers)) as executor:
        futures = [
            executor.submit(write_all, writer, index)
            for index, writer in enumerate(writers)
        ]
        barrier.wait(timeout=10)
        deadline = time.monotonic() + 60
        while not all(future.done() for future in futures):
            assert time.monotonic() < deadline, "writers did not finish"
            poll()
        poll()
        # Re-raises any writer's exception.
        return [future.result() for future in futures]


def test_concurrent_writers_commit_in_id_order_for_checkpoint_reader(
    pg_core: BrokerCore,
    second_core: Callable[[], BrokerCore],
) -> None:
    """Concurrent generated writes are unique, monotonic per writer, and become
    visible in ID order, so an ``after_timestamp`` checkpoint reader polling
    during the writes skips nothing.

    This is the end-to-end, probabilistic guard: committing the allocation
    and the insert in separate transactions made it report skipped messages
    in 5 of 10 runs. The deterministic proofs of the same mechanism are the
    one-exchange and conflict-rollback tests above.
    """
    writer_count = 8
    per_writer = 100
    writers = [second_core() for _ in range(writer_count)]
    reader = _CheckpointReader(second_core(), "race")

    returned = _write_concurrently(writers, per_writer, reader.drain)

    expected = {f"w{i}-{n}" for i in range(writer_count) for n in range(per_writer)}
    missing = expected - set(reader.seen)
    assert not missing, f"checkpoint reader skipped {sorted(missing)[:5]}"
    all_ids = [ts for ids in returned for ts in ids]
    assert len(set(all_ids)) == writer_count * per_writer
    for ids in returned:
        assert ids == sorted(ids)
    assert pg_core.refresh_last_timestamp() == max(all_ids)


def test_sequential_transport_preserves_generated_write_semantics(
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
    second_core: Callable[[], BrokerCore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without libpq pipeline mode the same statements run in one explicit
    transaction; allocation, ordering, and atomicity are unchanged."""
    monkeypatch.setattr(runner_module, "_pipeline_supported", lambda: False)
    writer = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    try:
        first = writer.write("jobs", "first")
        high_water = _future_high_water(logical=3)
        second_core().advance_last_timestamp(high_water)

        second = writer.write("jobs", "stale candidate")

        assert first < high_water < second == high_water + 1
        assert pg_plugin.read_last_ts(pg_runner) == second
        assert writer.peek_many("jobs") == [
            ("first", first),
            ("stale candidate", second),
        ]
        assert not pg_runner._in_transaction()
    finally:
        writer.close()
