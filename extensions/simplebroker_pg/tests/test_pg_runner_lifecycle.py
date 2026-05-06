"""Postgres runner lifecycle unit tests."""

from __future__ import annotations

import threading
from typing import Any, cast

import pytest
import simplebroker_pg.runner as pg_runner_module
from psycopg import OperationalError as PsycopgOperationalError
from psycopg_pool import PoolClosed
from simplebroker_pg import PostgresRunner

from simplebroker._exceptions import OperationalError
from simplebroker._runner import SetupPhase
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


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
    runner._setup_lock = threading.RLock()
    runner._completed_phases = set()
    calls = 0

    def operation() -> None:
        nonlocal calls
        calls += 1

    assert runner.run_exclusive_setup(SetupPhase.SCHEMA, operation) is True
    assert runner.run_exclusive_setup(SetupPhase.SCHEMA, operation) is False
    assert calls == 1


def test_invalidate_bootstrap_state_clears_schema_setup_phase() -> None:
    runner = object.__new__(PostgresRunner)
    runner._setup_lock = threading.RLock()
    runner._completed_phases = {SetupPhase.CONNECTION, SetupPhase.SCHEMA}
    runner._meta_cache_lock = threading.Lock()
    runner._meta_cache = None
    runner._schema_bootstrapped = True

    runner.invalidate_bootstrap_state()

    assert runner.is_setup_complete(SetupPhase.CONNECTION)
    assert not runner.is_setup_complete(SetupPhase.SCHEMA)
    assert not runner.is_schema_bootstrapped()


def test_release_thread_connection_returns_real_pool_connection(
    pg_runner: PostgresRunner,
) -> None:
    conn = pg_runner._get_thread_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
    assert hasattr(pg_runner._thread_local, "conn")

    pg_runner.release_thread_connection()

    assert not hasattr(pg_runner._thread_local, "conn")
    assert list(pg_runner.run("SELECT 1", fetch=True)) == [(1,)]


def test_close_closes_real_pool(pg_runner: PostgresRunner) -> None:
    assert list(pg_runner.run("SELECT 1", fetch=True)) == [(1,)]

    pg_runner.close()

    with pytest.raises(PoolClosed):
        list(pg_runner.run("SELECT 1", fetch=True))


def test_same_runner_rebootstraps_after_schema_drop_error(
    pg_runner: PostgresRunner,
    pg_plugin: Any,
    pg_dsn: str,
    pg_schema: str,
) -> None:
    core = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    core.close()

    pg_plugin.cleanup_target(pg_dsn, backend_options={"schema": pg_schema})
    with pytest.raises(OperationalError):
        list(pg_runner.run("TRUNCATE messages RESTART IDENTITY CASCADE"))

    core = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    core.close()

    pg_plugin.validate_target(
        pg_dsn,
        backend_options={"schema": pg_schema},
        verify_initialized=True,
    )


def test_shared_activity_registry_is_pid_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeListener:
        instances: list[FakeListener] = []

        def __init__(self, dsn: str, *, schema: str) -> None:
            self.dsn = dsn
            self.schema = schema
            self.close_calls = 0
            self.instances.append(self)

        def close(self) -> None:
            self.close_calls += 1

    current_pid = 1001

    monkeypatch.setattr(pg_runner_module, "_SharedActivityListener", FakeListener)
    monkeypatch.setattr(pg_runner_module.os, "getpid", lambda: current_pid)

    registry = pg_runner_module._SharedActivityRegistry()

    parent_first = cast(FakeListener, registry.acquire("dsn", schema="schema"))
    parent_second = cast(FakeListener, registry.acquire("dsn", schema="schema"))
    assert parent_first is parent_second
    assert len(FakeListener.instances) == 1

    current_pid = 1002
    child_listener = cast(FakeListener, registry.acquire("dsn", schema="schema"))
    assert child_listener is not parent_first
    assert len(FakeListener.instances) == 2

    registry.release("dsn", schema="schema")
    assert child_listener.close_calls == 1
    assert parent_first.close_calls == 0

    current_pid = 1001
    registry.release("dsn", schema="schema")
    assert parent_first.close_calls == 0
    registry.release("dsn", schema="schema")
    assert parent_first.close_calls == 1


def test_activity_listener_startup_error_raises_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_connect(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise PsycopgOperationalError("bad dsn")

    monkeypatch.setattr(pg_runner_module.psycopg, "connect", fail_connect)

    with pytest.raises(OperationalError, match="bad dsn"):
        pg_runner_module._SharedActivityListener("bad", schema="schema")


def test_activity_listener_startup_timeout_raises_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def never_ready(self: Any) -> None:
        del self

    monkeypatch.setattr(pg_runner_module._SharedActivityListener, "_run", never_ready)

    with pytest.raises(OperationalError, match="did not start"):
        pg_runner_module._SharedActivityListener(
            "dsn",
            schema="schema",
            startup_timeout=0.01,
        )
