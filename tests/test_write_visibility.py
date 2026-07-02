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
  a real SQLiteRunner (no behavior is faked; only ordering is recorded).
"""

import multiprocessing
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore

pytestmark = pytest.mark.sqlite_only

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
        for body, ts in reader.peek(
            all_messages=True, with_timestamps=True, after_timestamp=checkpoint
        ):
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
    """Pass-through spy over a real SQLiteRunner.

    Delegates every call to the real runner (nothing is faked) and records
    the order of transaction boundaries, the last_ts CAS, and the message
    insert.  Ordering IS the invariant under test: the CAS must execute
    between BEGIN IMMEDIATE and the COMMIT that publishes the insert.
    """

    def __init__(self, inner: SQLiteRunner) -> None:
        self._inner = inner
        self.events: list[str] = []

    def run(self, sql, params=(), *, fetch=False):
        normalized = " ".join(sql.split())
        if normalized.startswith("UPDATE meta SET value"):
            self.events.append("advance_last_ts")
        elif normalized.startswith("INSERT INTO messages"):
            self.events.append("insert_message")
        return self._inner.run(sql, params, fetch=fetch)

    def begin_immediate(self):
        self.events.append("begin")
        return self._inner.begin_immediate()

    def commit(self):
        self.events.append("commit")
        return self._inner.commit()

    def rollback(self):
        self.events.append("rollback")
        return self._inner.rollback()

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_write_allocates_timestamp_inside_the_insert_transaction(
    tmp_path: Path,
) -> None:
    runner = _RecordingRunner(SQLiteRunner(str(tmp_path / "spy.db")))
    core = BrokerCore(runner)
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
