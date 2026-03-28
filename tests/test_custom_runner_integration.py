"""Integration tests for injected SQLRunner behavior."""

from __future__ import annotations

import gc
import tempfile
from pathlib import Path
from typing import Any

from simplebroker import Queue
from simplebroker._runner import SetupPhase, SQLiteRunner
from simplebroker.db import BrokerCore, BrokerDB


class RecordingRunner:
    """Thin SQLiteRunner wrapper that records calls through the public protocol."""

    def __init__(self, db_path: str):
        self._inner = SQLiteRunner(db_path)
        self.sql_log: list[str] = []
        self.close_calls = 0

    def run(
        self, sql: str, params: tuple[Any, ...] = (), *, fetch: bool = False
    ) -> list[tuple[Any, ...]]:
        self.sql_log.append(sql)
        return list(self._inner.run(sql, params, fetch=fetch))

    def begin_immediate(self) -> None:
        self._inner.begin_immediate()

    def commit(self) -> None:
        self._inner.commit()

    def rollback(self) -> None:
        self._inner.rollback()

    def close(self) -> None:
        self.close_calls += 1
        self._inner.close()

    def setup(self, phase: SetupPhase) -> None:
        self._inner.setup(phase)

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return self._inner.is_setup_complete(phase)

    def clear_log(self) -> None:
        self.sql_log.clear()


def _has_sql_marker(sql_log: list[str], marker: str) -> bool:
    """Return True when any recorded statement contains the marker."""

    return any(marker in sql for sql in sql_log)


def test_injected_runner_handles_actual_queue_writes_in_both_modes():
    """Injected runner should execute message inserts for both queue modes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        for persistent in (False, True):
            runner = RecordingRunner(db_path)
            queue = Queue(
                "tasks", db_path=db_path, runner=runner, persistent=persistent
            )
            try:
                runner.clear_log()
                queue.write("hello")
                assert _has_sql_marker(runner.sql_log, "INSERT INTO messages"), (
                    "Injected runner should execute the message insert statement"
                )
            finally:
                queue.close()
                runner.close()


def test_injected_runner_handles_actual_queue_reads_in_both_modes():
    """Injected runner should execute the claim/delete path for queue reads."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        for persistent in (False, True):
            runner = RecordingRunner(db_path)
            queue = Queue(
                "tasks", db_path=db_path, runner=runner, persistent=persistent
            )
            try:
                queue.write("hello")
                runner.clear_log()

                assert queue.read_one(with_timestamps=False) == "hello"
                assert (
                    _has_sql_marker(runner.sql_log, "DELETE FROM messages")
                    or _has_sql_marker(runner.sql_log, "RETURNING")
                ), "Injected runner should execute the claim/delete SQL"
                assert queue.peek_one(with_timestamps=False) is None
            finally:
                queue.close()
                runner.close()


def test_ephemeral_queue_with_injected_runner_reuses_runner_backed_core():
    """Injected runners should not create hidden BrokerDB instances per operation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        runner = RecordingRunner(db_path)
        queue = Queue("tasks", db_path=db_path, runner=runner, persistent=False)
        try:
            with queue.get_connection() as conn1:
                with queue.get_connection() as conn2:
                    assert conn1 is conn2
                    assert isinstance(conn1, BrokerCore)
                    assert not isinstance(conn1, BrokerDB)
        finally:
            queue.close()
            runner.close()


def test_injected_runner_is_caller_owned_across_close_and_finalizer():
    """Queue cleanup paths must not close a supplied runner."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        runner = RecordingRunner(db_path)

        queue = Queue("tasks", db_path=db_path, runner=runner, persistent=True)
        queue.close()
        assert runner.close_calls == 0

        queue = Queue("tasks", db_path=db_path, runner=runner, persistent=True)
        queue._finalizer()
        del queue
        gc.collect()
        assert runner.close_calls == 0

        runner.close()
        assert runner.close_calls == 1
