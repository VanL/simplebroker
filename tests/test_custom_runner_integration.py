"""Integration tests for injected SQLRunner behavior."""

from __future__ import annotations

import gc
import threading
import weakref
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker._runner import SetupPhase, SQLiteRunner
from simplebroker.db import BrokerCore, BrokerDB, DBConnection

pytestmark = [pytest.mark.sqlite_only]


class RecordingRunner:
    """Thin SQLiteRunner wrapper that records calls through the public protocol."""

    def __init__(self, db_path: str):
        self._inner = SQLiteRunner(db_path)
        self.close_calls = 0
        self.transaction_events: list[str] = []
        self.delete_failure: Exception | None = None

    def run(
        self, sql: str, params: tuple[Any, ...] = (), *, fetch: bool = False
    ) -> list[tuple[Any, ...]]:
        deleting_messages = sql.strip().startswith("DELETE FROM messages")
        if deleting_messages:
            self.transaction_events.append("delete")
        rows = list(self._inner.run(sql, params, fetch=fetch))
        if deleting_messages and self.delete_failure is not None:
            raise self.delete_failure
        return rows

    def begin_immediate(self) -> None:
        self.transaction_events.append("begin")
        self._inner.begin_immediate()

    def commit(self) -> None:
        self.transaction_events.append("commit")
        self._inner.commit()

    def rollback(self) -> None:
        self.transaction_events.append("rollback")
        self._inner.rollback()

    def close(self) -> None:
        self.close_calls += 1
        self._inner.close()

    def setup(self, phase: SetupPhase) -> None:
        self._inner.setup(phase)

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return self._inner.is_setup_complete(phase)


class ShutdownRecordingSQLiteRunner(SQLiteRunner):
    """Real SQLite runner that records its destructive lifecycle hooks."""

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.close_calls = 0
        self.shutdown_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        super().close()

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        super().close()


@pytest.mark.parametrize("persistent", [False, True])
def test_injected_runner_target_wins_over_decoy_queue_target_in_both_modes(
    tmp_path: Path, persistent: bool
) -> None:
    """Every supported operation stays on the caller's runner target."""
    decoy_path = tmp_path / f"decoy-{persistent}.db"
    runner_path = tmp_path / f"runner-{persistent}.db"
    runner = RecordingRunner(str(runner_path))
    queue = Queue(
        "tasks", db_path=str(decoy_path), runner=runner, persistent=persistent
    )
    try:
        queue.write("runner-only")
        assert queue.peek_one(with_timestamps=False) == "runner-only"
        with Queue("tasks", db_path=str(runner_path)) as runner_observer:
            assert runner_observer.peek_one(with_timestamps=False) == "runner-only"
        assert queue.read_one(with_timestamps=False) == "runner-only"
        assert queue.peek_one(with_timestamps=False) is None

        with Queue("tasks", db_path=str(runner_path)) as runner_observer:
            assert runner_observer.peek_one(with_timestamps=False) is None
        assert decoy_path.exists() is False
    finally:
        queue.close()
        runner.close()


def test_injected_runner_is_caller_owned_across_close_and_finalizer(
    tmp_path: Path,
) -> None:
    """Explicit close and real GC leave a supplied runner usable."""
    db_path = tmp_path / "runner.db"
    runner = RecordingRunner(str(db_path))

    queue = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    queue.write("after-close")
    queue.close()
    assert runner.close_calls == 0

    queue = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    queue_ref = weakref.ref(queue)
    del queue
    gc.collect()
    assert queue_ref() is None
    assert runner.close_calls == 0

    observer = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    try:
        assert observer.read_one(with_timestamps=False) == "after-close"
    finally:
        observer.close()
        runner.close()
    assert runner.close_calls == 1


def test_sql_borrowed_runner_masks_destructive_verbs_across_teardown(
    tmp_path: Path,
) -> None:
    """Core, manager, and queue teardown leave an injected runner usable."""

    runner_path = tmp_path / "borrowed-runner.db"
    decoy_path = tmp_path / "decoy.db"
    runner = ShutdownRecordingSQLiteRunner(str(runner_path))
    try:
        connection = DBConnection(str(decoy_path), runner=runner)
        try:
            core = connection.get_connection()
            core.write("tasks", "before-shutdown")
            core.shutdown()
        finally:
            connection.close()

        queue = Queue("tasks", db_path=str(decoy_path), runner=runner)
        try:
            queue.write("after-shutdown")
        finally:
            queue.close()

        observer = Queue("tasks", db_path=str(decoy_path), runner=runner)
        try:
            assert observer.peek_many(with_timestamps=False) == [
                "before-shutdown",
                "after-shutdown",
            ]
        finally:
            observer.close()

        assert runner.shutdown_calls == 0
        assert runner.close_calls == 0
    finally:
        runner.shutdown()


def test_queue_delete_owns_an_explicit_transaction_and_commits_once(
    tmp_path: Path,
) -> None:
    """Successful queue deletion begins before mutation and commits once."""

    runner_path = tmp_path / "delete-success.db"
    runner = RecordingRunner(str(runner_path))
    queue = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    try:
        queue.write("durable")
        runner.transaction_events.clear()

        assert queue.delete() is True
        assert runner.transaction_events == ["begin", "delete", "commit"]

        with Queue("tasks", db_path=str(runner_path)) as observer:
            assert observer.peek_one() is None
    finally:
        queue.close()
        runner.close()


def test_delete_all_owns_the_same_explicit_transaction(
    tmp_path: Path,
) -> None:
    """The all-queues form uses the same transaction boundary."""

    runner_path = tmp_path / "delete-all.db"
    runner = RecordingRunner(str(runner_path))
    core = BrokerCore(runner)
    try:
        core.write("first", "one")
        core.write("second", "two")
        runner.transaction_events.clear()

        assert core.delete() == 2
        assert runner.transaction_events == ["begin", "delete", "commit"]

        with Queue("first", db_path=str(runner_path)) as first:
            assert first.peek_one() is None
        with Queue("second", db_path=str(runner_path)) as second:
            assert second.peek_one() is None
    finally:
        core.close()


def test_queue_delete_rolls_back_a_mutation_failure_and_preserves_the_error(
    tmp_path: Path,
) -> None:
    """A post-statement failure rolls back durable state without masking it."""

    runner_path = tmp_path / "delete-failure.db"
    runner = RecordingRunner(str(runner_path))
    queue = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    failure = RuntimeError("injected delete failure")
    try:
        queue.write("still-present")
        runner.transaction_events.clear()
        runner.delete_failure = failure

        with pytest.raises(RuntimeError) as exc_info:
            queue.delete()

        assert exc_info.value is failure
        assert runner.transaction_events == ["begin", "delete", "rollback"]
        with Queue("tasks", db_path=str(runner_path)) as observer:
            assert observer.peek_one() == "still-present"
    finally:
        queue.close()
        runner.close()


def test_broker_db_private_connection_view_follows_runner_replacement(
    tmp_path: Path,
) -> None:
    """Compatibility access never retains a connection closed by the runner."""

    db = BrokerDB(str(tmp_path / "live-connection.db"))
    try:
        first_connection = db._conn
        db._runner.close()

        replacement = db._conn

        assert replacement is not first_connection
        assert replacement.execute("SELECT 1").fetchone() == (1,)
    finally:
        db.close()


def test_broker_db_private_connection_view_follows_thread_local_access(
    tmp_path: Path,
) -> None:
    """Compatibility access delegates to each caller's runner generation."""

    db = BrokerDB(str(tmp_path / "thread-local-connection.db"))
    worker_connections: list[Any] = []
    try:
        main_connection = db._conn
        worker = threading.Thread(target=lambda: worker_connections.append(db._conn))
        worker.start()
        worker.join()

        assert len(worker_connections) == 1
        assert worker_connections[0] is not main_connection
        assert db._conn is main_connection
    finally:
        db.close()


def test_broker_core_teardown_does_not_force_global_gc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Broker handle teardown should not scale with process-wide object graph size."""
    collect_calls = 0

    def collect() -> int:
        nonlocal collect_calls
        collect_calls += 1
        return 0

    monkeypatch.setattr(gc, "collect", collect)

    core = BrokerDB(str(tmp_path / "close.db"))
    core.close()
    assert collect_calls == 0

    owned_core = BrokerDB(str(tmp_path / "shutdown.db"))
    owned_core.shutdown()
    assert collect_calls == 0

    finalizer_core = BrokerDB(str(tmp_path / "finalizer.db"))
    finalizer_core.__del__()
    assert collect_calls == 0
