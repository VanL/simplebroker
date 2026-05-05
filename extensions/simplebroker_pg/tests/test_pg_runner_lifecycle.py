"""Postgres runner lifecycle unit tests."""

from __future__ import annotations

import threading
from typing import Any, cast

from simplebroker_pg import PostgresRunner

from simplebroker._runner import SetupPhase


class FakePool:
    def __init__(self) -> None:
        self.putconn_calls = 0
        self.close_calls = 0

    def putconn(self, conn: object) -> None:
        self.putconn_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def _runner_with_thread_connection() -> tuple[PostgresRunner, FakePool]:
    runner = cast(Any, object.__new__(PostgresRunner))
    pool = FakePool()
    runner._pool = pool
    runner._thread_local = threading.local()
    runner._thread_local.conn = object()
    return cast(PostgresRunner, runner), pool


def test_release_thread_connection_returns_connection_without_closing_pool() -> None:
    runner, pool = _runner_with_thread_connection()

    runner.release_thread_connection()

    assert pool.putconn_calls == 1
    assert pool.close_calls == 0
    assert not hasattr(runner._thread_local, "conn")


def test_close_returns_thread_connection_and_closes_pool() -> None:
    runner, pool = _runner_with_thread_connection()

    runner.close()

    assert pool.putconn_calls == 1
    assert pool.close_calls == 1
    assert not hasattr(runner._thread_local, "conn")


def test_run_exclusive_setup_runs_operation_once_per_phase() -> None:
    runner = object.__new__(PostgresRunner)
    runner._setup_lock = threading.Lock()
    runner._completed_phases = set()
    calls = 0

    def operation() -> None:
        nonlocal calls
        calls += 1

    assert runner.run_exclusive_setup(SetupPhase.SCHEMA, operation) is True
    assert runner.run_exclusive_setup(SetupPhase.SCHEMA, operation) is False
    assert calls == 1
