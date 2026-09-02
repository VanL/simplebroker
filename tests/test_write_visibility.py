"""Checkpoint-visibility invariant for write().

The meta.last_ts advance and the message row must become visible in the
same commit.  If the timestamp is allocated in its own autocommit
statement (the old behavior), a concurrent writer can commit a higher
timestamp while this writer waits for the write lock, and a checkpoint
reader (peek --after / peek-mode QueueWatcher) advances past the
in-flight message and permanently skips it.

Two tests pin the fix from opposite directions:
- a multi-process stress test that reproduces the user-visible failure
  (probabilistic red pre-fix, deterministic green post-fix), and
- a deterministic statement-ordering test using a pass-through spy over
  each SQL backend's real runner (no behavior is faked; only ordering is
  recorded). Redis owns a separate real-Valkey Lua visibility proof.
"""

import multiprocessing
import threading
import time
from collections.abc import Callable, Sequence
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any, Protocol, cast

import pytest

from simplebroker import Queue
from simplebroker._backend_plugins import BackendPlugin
from simplebroker.db import BrokerCore

from .helper_scripts.broker_factory import active_backend
from .helper_scripts.timing import scale_timeout_for_ci

NUM_WRITERS = 16
MESSAGES_PER_WRITER = 40


def _writer_proc(
    db_path: str,
    writer_id: int,
    barrier: Any,
    barrier_timeout: float,
) -> None:
    """Module-level so it pickles under the spawn start method."""
    with Queue("race", db_path=db_path) as q:
        barrier.wait(timeout=barrier_timeout)
        for i in range(MESSAGES_PER_WRITER):
            q.write(f"w{writer_id}-{i}")


def _run_visibility_writers(
    processes: Sequence[BaseProcess],
    *,
    barrier: Any,
    barrier_timeout: float,
    drain: Callable[[], None],
) -> tuple[list[str], list[int | None]]:
    """Run the synchronized writer phase and return timeout/exit diagnostics."""

    for process in processes:
        process.start()

    try:
        barrier.wait(timeout=barrier_timeout)
    except threading.BrokenBarrierError as exc:
        states = {process.name: process.exitcode for process in processes}
        raise AssertionError(
            f"writers did not reach the start barrier: {states}"
        ) from exc

    active_at_deadline: list[str] = []
    deadline = time.monotonic() + scale_timeout_for_ci(60.0)
    while active := [process for process in processes if process.is_alive()]:
        if time.monotonic() >= deadline:
            active_at_deadline = [process.name for process in active]
            break
        drain()
        time.sleep(0.001)

    if not active_at_deadline:
        for _ in range(5):
            drain()

    for process in processes:
        process.join(timeout=scale_timeout_for_ci(1.0))
    return active_at_deadline, [process.exitcode for process in processes]


def _stop_visibility_writers(processes: Sequence[BaseProcess]) -> list[str]:
    """Bound cleanup and return any child that survived terminate and kill."""

    for process in processes:
        if process.is_alive():
            process.terminate()
    for process in processes:
        process.join(timeout=scale_timeout_for_ci(2.0))
    for process in processes:
        if process.is_alive():
            process.kill()
            process.join(timeout=scale_timeout_for_ci(2.0))
    return [process.name for process in processes if process.is_alive()]


@pytest.mark.xdist_group(name="sqlite_process_stress")
@pytest.mark.sqlite_only
def test_checkpoint_reader_sees_every_message(tmp_path: Path) -> None:
    """A checkpoint reader polling during concurrent writes misses nothing."""
    db_path = str(tmp_path / "race.db")
    ctx = multiprocessing.get_context("spawn")
    barrier_timeout = scale_timeout_for_ci(20.0)
    barrier = ctx.Barrier(NUM_WRITERS + 1)
    procs = [
        ctx.Process(
            target=_writer_proc,
            args=(db_path, wid, barrier, barrier_timeout),
            name=f"visibility-writer-{wid}",
        )
        for wid in range(NUM_WRITERS)
    ]
    reader = Queue("race", db_path=db_path)
    seen: set[str] = set()
    checkpoint = 0
    cleanup_survivors: list[str] = []

    def drain() -> None:
        nonlocal checkpoint
        rows = reader.peek(
            all_messages=True, with_timestamps=True, after_timestamp=checkpoint
        )
        for body, ts in rows:
            seen.add(body)
            checkpoint = max(checkpoint, ts)

    try:
        # Poll aggressively WHILE writers run: pre-fix, the skip only happens
        # when the reader observes a higher committed ts during another
        # writer's allocate->insert window. Yield briefly so this polling
        # process cannot starve writers on a small CI runner.
        writers_active_at_deadline, writer_exitcodes = _run_visibility_writers(
            procs,
            barrier=barrier,
            barrier_timeout=barrier_timeout,
            drain=drain,
        )
    finally:
        cleanup_survivors = _stop_visibility_writers(procs)
        reader.close()

    assert not cleanup_survivors, f"writer cleanup failed: {cleanup_survivors}"
    assert not writers_active_at_deadline, (
        f"writers exceeded the bounded runtime: {writers_active_at_deadline}"
    )
    assert writer_exitcodes == [0] * NUM_WRITERS, (
        f"writers exited unsuccessfully: {writer_exitcodes}"
    )

    expected = {
        f"w{wid}-{i}" for wid in range(NUM_WRITERS) for i in range(MESSAGES_PER_WRITER)
    }
    missing = expected - seen
    assert not missing, (
        f"checkpoint reader permanently skipped {len(missing)} message(s), "
        f"e.g. {sorted(missing)[:5]}"
    )


class _RecordingRunner:
    """Pass-through spy over one SQL backend's real runner.

    Delegates every call to the real runner (nothing is faked) and records
    the order of transaction boundaries, the last_ts CAS, and the message
    insert.  Ordering IS the invariant under test: the CAS must execute
    between BEGIN IMMEDIATE and the COMMIT that publishes the insert.
    """

    def __init__(self, inner: object) -> None:
        self._inner = inner
        self.events: list[str] = []

    def run(self, sql, params=(), *, fetch=False):
        normalized = " ".join(sql.split())
        if normalized.startswith("UPDATE meta") and "last_ts" in normalized:
            self.events.append("advance_last_ts")
        elif "INSERT INTO messages" in normalized:
            self.events.append("insert_message")
        elif normalized.startswith("UPDATE messages SET claimed ="):
            self.events.append("claim_older_pending")
        return self._inner.run(sql, params, fetch=fetch)  # type: ignore[attr-defined]

    def begin_immediate(self):
        self.events.append("begin")
        return self._inner.begin_immediate()  # type: ignore[attr-defined]

    def commit(self):
        self.events.append("commit")
        return self._inner.commit()  # type: ignore[attr-defined]

    def rollback(self):
        self.events.append("rollback")
        return self._inner.rollback()  # type: ignore[attr-defined]

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _SQLBrokerFixture(Protocol):
    """Structural view of the shared SQL broker fixture used by this test."""

    _runner: object
    _backend_plugin: BackendPlugin


@pytest.mark.shared
def test_write_allocates_timestamp_inside_the_insert_transaction(
    broker: object,
) -> None:
    if active_backend() == "redis":
        pytest.skip(
            "test_write_allocates_timestamp_inside_the_insert_transaction "
            "is a SQL transaction-ordering proof; Redis uses the real-Valkey "
            "ordinary-write visibility tests"
        )

    fixture = cast(_SQLBrokerFixture, broker)
    runner = _RecordingRunner(fixture._runner)
    core = BrokerCore(
        runner,
        backend_plugin=fixture._backend_plugin,
    )
    runner.events.clear()  # discard schema-setup noise

    core.write("q", "hello")

    events = runner.events
    assert "advance_last_ts" in events and "insert_message" in events
    begin = events.index("begin")
    cas = events.index("advance_last_ts")
    insert = events.index("insert_message")
    commit = events.index("commit")
    # events.index() returns FIRST occurrences, so begin < cas < insert <
    # commit also proves nothing committed before the insert transaction.
    assert begin < cas < insert < commit, (
        "last_ts CAS must happen inside the BEGIN IMMEDIATE .. COMMIT that "
        f"publishes the insert; got {events}"
    )
    assert "rollback" not in events, f"write rolled back unexpectedly: {events}"
    core.close()


@pytest.mark.shared
def test_write_keep_claims_between_insert_and_commit(broker: object) -> None:
    if active_backend() == "redis":
        pytest.skip("Redis owns the equivalent ordering inside WRITE_MESSAGE Lua")

    fixture = cast(_SQLBrokerFixture, broker)
    runner = _RecordingRunner(fixture._runner)
    core = BrokerCore(
        runner,
        backend_plugin=fixture._backend_plugin,
    )
    core.write("q", "older")
    runner.events.clear()

    core.write("q", "newest", keep_newest=1)

    events = runner.events
    assert events.index("begin") < events.index("advance_last_ts")
    assert events.index("advance_last_ts") < events.index("insert_message")
    assert events.index("insert_message") < events.index("claim_older_pending")
    assert events.index("claim_older_pending") < events.index("commit")
    assert "rollback" not in events
    core.close()
