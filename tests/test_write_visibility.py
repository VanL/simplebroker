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
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

from simplebroker import Queue
from simplebroker.db import BrokerCore

from .helper_scripts.broker_factory import active_backend

NUM_WRITERS = 16
MESSAGES_PER_WRITER = 40


def _writer_proc(db_path: str, writer_id: int, barrier) -> None:
    """Module-level so it pickles under the spawn start method."""
    q = Queue("race", db_path=db_path)
    barrier.wait()
    for i in range(MESSAGES_PER_WRITER):
        q.write(f"w{writer_id}-{i}")
    q.close()


@pytest.mark.xdist_group(name="write_visibility")
@pytest.mark.sqlite_only
def test_checkpoint_reader_sees_every_message(tmp_path: Path) -> None:
    """A checkpoint reader polling during concurrent writes misses nothing."""
    db_path = str(tmp_path / "race.db")
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(NUM_WRITERS)
    procs = [
        ctx.Process(target=_writer_proc, args=(db_path, wid, barrier))
        for wid in range(NUM_WRITERS)
    ]
    for p in procs:
        p.start()

    reader = Queue("race", db_path=db_path)
    seen: set[str] = set()
    checkpoint = 0

    def drain() -> None:
        nonlocal checkpoint
        rows = cast(
            Iterator[tuple[str, int]],
            reader.peek(
                all_messages=True, with_timestamps=True, after_timestamp=checkpoint
            ),
        )
        for body, ts in rows:
            seen.add(body)
            checkpoint = max(checkpoint, ts)

    # Poll aggressively WHILE writers run: pre-fix, the skip only happens
    # when the reader observes a higher committed ts during another
    # writer's allocate->insert window.
    while any(p.is_alive() for p in procs):
        drain()
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0, f"writer crashed with exit code {p.exitcode}"

    # Final settled drains, still via the checkpoint pattern: a message
    # skipped by the checkpoint stays invisible forever, which is exactly
    # the bug.
    for _ in range(5):
        drain()

    expected = {
        f"w{wid}-{i}" for wid in range(NUM_WRITERS) for i in range(MESSAGES_PER_WRITER)
    }
    missing = expected - seen
    reader.close()
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

    runner = _RecordingRunner(broker._runner)  # type: ignore[attr-defined]
    core = BrokerCore(
        runner,
        backend_plugin=broker._backend_plugin,  # type: ignore[attr-defined]
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
