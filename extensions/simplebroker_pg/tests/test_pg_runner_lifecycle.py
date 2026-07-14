"""Postgres runner lifecycle unit tests."""

from __future__ import annotations

import os
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


class FakeCursor:
    def __init__(
        self,
        rows: list[tuple[Any, ...]],
        execute_error: Exception | None = None,
    ) -> None:
        self._rows = rows
        self._execute_error = execute_error

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        prepare: bool = False,
    ) -> None:
        del sql, params, prepare
        if self._execute_error is not None:
            raise self._execute_error

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or [(1,)]
        self.commit_calls = 0
        self.rollback_calls = 0
        self.commit_error: PsycopgOperationalError | None = None
        self.rollback_error: PsycopgOperationalError | None = None
        self.execute_error: Exception | None = None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._rows, self.execute_error)

    def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error


class FakePool:
    def __init__(self) -> None:
        self.conn = FakeConnection()
        self.getconn_calls = 0
        self.putconn_calls = 0
        self.close_calls = 0

    def getconn(self) -> FakeConnection:
        self.getconn_calls += 1
        return self.conn

    def putconn(self, conn: object) -> None:
        assert conn is self.conn
        self.putconn_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def _runner_with_thread_connection() -> tuple[PostgresRunner, FakePool]:
    runner = cast(Any, object.__new__(PostgresRunner))
    pool = FakePool()
    runner._pool = pool
    runner._thread_local = threading.local()
    runner._thread_local.conn = pool.conn
    runner._pid = os.getpid()
    runner._lease_lock = threading.RLock()
    runner._leased_operation_lock = threading.RLock()
    runner._leased_conn = None
    runner._lease_depth = 0
    runner._meta_cache_lock = threading.Lock()
    runner._meta_cache = None
    return cast(PostgresRunner, runner), pool


def _runner_with_fake_pool() -> tuple[PostgresRunner, FakePool]:
    runner = cast(Any, object.__new__(PostgresRunner))
    pool = FakePool()
    runner._pool = pool
    runner._thread_local = threading.local()
    runner._pid = os.getpid()
    runner._lease_lock = threading.RLock()
    runner._leased_operation_lock = threading.RLock()
    runner._leased_conn = None
    runner._lease_depth = 0
    runner._meta_cache_lock = threading.Lock()
    runner._meta_cache = None
    return cast(PostgresRunner, runner), pool


def test_release_thread_connection_returns_connection_without_closing_pool() -> None:
    runner, pool = _runner_with_thread_connection()

    runner.release_thread_connection()

    assert pool.putconn_calls == 1
    assert pool.close_calls == 0
    assert not hasattr(runner._thread_local, "conn")


def test_lease_thread_connection_keeps_operation_checkout_until_release() -> None:
    runner, pool = _runner_with_fake_pool()

    runner.lease_thread_connection()
    assert pool.getconn_calls == 1

    assert list(runner.run("SELECT 1", fetch=True)) == [(1,)]

    assert pool.getconn_calls == 1
    assert pool.putconn_calls == 0
    assert runner._leased_conn is pool.conn

    runner.release_thread_connection()

    assert pool.putconn_calls == 1
    assert runner._leased_conn is None


def test_multiple_leases_share_one_checkout_until_last_release() -> None:
    runner, pool = _runner_with_fake_pool()

    runner.lease_thread_connection()
    runner.lease_thread_connection()

    assert pool.getconn_calls == 1
    assert runner._leased_conn is pool.conn

    runner.release_thread_connection()
    assert pool.putconn_calls == 0
    assert runner._leased_conn is pool.conn

    runner.release_thread_connection()
    assert pool.putconn_calls == 1
    assert runner._leased_conn is None


def test_leased_checkout_is_shared_across_threads() -> None:
    runner, pool = _runner_with_fake_pool()
    runner.lease_thread_connection()
    main_conn = runner._get_thread_conn()
    worker_conns: list[object] = []

    def touch_runner() -> None:
        runner.lease_thread_connection()
        try:
            worker_conns.append(runner._get_thread_conn())
        finally:
            runner.release_thread_connection()

    thread = threading.Thread(target=touch_runner)
    thread.start()
    thread.join()

    assert worker_conns == [main_conn]
    assert pool.getconn_calls == 1
    assert pool.putconn_calls == 0

    runner.release_thread_connection()
    assert pool.putconn_calls == 1


def test_leased_commit_failure_releases_failed_checkout() -> None:
    runner, pool = _runner_with_fake_pool()
    runner.lease_thread_connection()
    pool.conn.commit_error = PsycopgOperationalError("boom")

    runner.begin_immediate()

    with pytest.raises(OperationalError):
        runner.commit()

    assert runner._lease_depth == 1
    assert runner._leased_conn is None
    assert pool.putconn_calls == 1

    pool.conn.commit_error = None
    assert list(runner.run("SELECT 1", fetch=True)) == [(1,)]
    assert pool.getconn_calls == 2

    runner.release_thread_connection()


def test_close_returns_thread_connection_and_closes_pool() -> None:
    runner, pool = _runner_with_thread_connection()

    runner.close()

    assert pool.putconn_calls == 1
    assert pool.close_calls == 1
    assert not hasattr(runner._thread_local, "conn")


def test_fork_check_rebuilds_all_process_owned_runner_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, old_pool = _runner_with_fake_pool()
    runner._thread_local.conn = old_pool.conn
    runner._setup_lock = threading.RLock()
    runner._completed_phases = {SetupPhase.CONNECTION, SetupPhase.SCHEMA}
    runner._meta_cache_lock = threading.Lock()
    runner._meta_cache = pg_runner_module.RunnerMetaState("magic", 5, 10, 3)
    runner._schema_bootstrapped = True
    runner._pid = 1001
    new_pool = FakePool()

    monkeypatch.setattr(pg_runner_module.os, "getpid", lambda: 1002)
    monkeypatch.setattr(runner, "_create_pool", lambda: new_pool)

    runner._check_fork()

    assert runner._pid == 1002
    assert runner._pool is new_pool
    assert not hasattr(runner._thread_local, "conn")
    assert runner._lease_depth == 0
    assert runner._leased_conn is None
    assert runner._completed_phases == set()
    assert runner._meta_cache is None
    assert runner._schema_bootstrapped is False


def test_lease_adopts_thread_checkout_and_clears_transaction_markers() -> None:
    runner, pool = _runner_with_thread_connection()
    runner._thread_local.in_transaction = True
    runner._thread_local.transaction_uses_leased_conn = False

    runner.lease_thread_connection()

    assert runner._leased_conn is pool.conn
    assert runner._lease_depth == 1
    assert not hasattr(runner._thread_local, "conn")
    assert not hasattr(runner._thread_local, "in_transaction")
    assert not hasattr(runner._thread_local, "transaction_uses_leased_conn")
    assert pool.getconn_calls == 0


@pytest.mark.parametrize("leased", [False, True])
@pytest.mark.parametrize(
    ("execute_error", "expected_error"),
    [
        (PsycopgOperationalError("database unavailable"), OperationalError),
        (RuntimeError("cursor failed"), RuntimeError),
    ],
)
def test_begin_failure_returns_checkout_and_releases_operation_lock(
    leased: bool,
    execute_error: Exception,
    expected_error: type[Exception],
) -> None:
    runner, pool = _runner_with_fake_pool()
    pool.conn.execute_error = execute_error
    if leased:
        runner.lease_thread_connection()

    with pytest.raises(expected_error):
        runner.begin_immediate()

    assert pool.putconn_calls == 1
    assert not hasattr(runner._thread_local, "in_transaction")
    assert runner._leased_operation_lock.acquire(blocking=False)
    runner._leased_operation_lock.release()


def test_nonleased_commit_failure_returns_checkout() -> None:
    runner, pool = _runner_with_fake_pool()
    pool.conn.commit_error = PsycopgOperationalError("commit failed")

    with pytest.raises(OperationalError, match="commit failed"):
        runner.commit()

    assert pool.putconn_calls == 1
    assert not hasattr(runner._thread_local, "conn")


@pytest.mark.parametrize("leased", [False, True])
def test_rollback_failure_returns_checkout_and_releases_operation_lock(
    leased: bool,
) -> None:
    runner, pool = _runner_with_fake_pool()
    pool.conn.rollback_error = PsycopgOperationalError("rollback failed")
    if leased:
        runner.lease_thread_connection()
        runner._leased_operation_lock.acquire()
    runner._thread_local.in_transaction = True
    runner._thread_local.transaction_uses_leased_conn = leased

    with pytest.raises(OperationalError, match="rollback failed"):
        runner.rollback()

    assert pool.putconn_calls == 1
    assert not hasattr(runner._thread_local, "in_transaction") or not leased
    assert runner._leased_operation_lock.acquire(blocking=False)
    runner._leased_operation_lock.release()


def test_shutdown_swallows_pool_failures_after_releasing_local_state() -> None:
    class BrokenPool(FakePool):
        def putconn(self, conn: object) -> None:
            raise RuntimeError("pool already closed")

        def close(self) -> None:
            raise RuntimeError("pool already closed")

    runner, _ = _runner_with_fake_pool()
    pool = BrokenPool()
    runner_state = cast(Any, runner)
    runner_state._pool = pool
    runner_state._leased_conn = pool.conn
    runner_state._lease_depth = 1

    runner.shutdown()

    assert runner._leased_conn is None
    assert runner._lease_depth == 0


def test_update_meta_cache_is_noop_before_metadata_is_loaded() -> None:
    runner = object.__new__(PostgresRunner)
    runner._meta_cache_lock = threading.Lock()
    runner._meta_cache = None

    runner.update_meta_cache(last_ts=10)

    assert runner._meta_cache is None


def test_listener_handles_unknown_registrations_and_surfaces_errors() -> None:
    listener = object.__new__(pg_runner_module._SharedActivityListener)
    listener._lock = threading.RLock()
    listener._conditions = {}
    listener._versions = {}
    listener._queue_refcounts = {}
    listener._fan_in_entries = {}
    listener._wildcard_version = 7
    listener._stop_event = threading.Event()
    listener._error = None

    listener.unregister_queue("missing")
    listener.unregister_queue_set(999)
    assert listener.wait(
        queue_name="missing",
        stop_event=threading.Event(),
        timeout=0,
        last_queue_version=3,
        last_wildcard_version=4,
    ) == (False, 3, 7)
    assert listener.wait_any(
        fan_in_id=999,
        stop_event=threading.Event(),
        timeout=0,
        last_queue_versions={"jobs": 2},
        last_wildcard_version=4,
    ) == (False, {"jobs": 2}, 7)

    listener.register_queue("jobs")
    listener._error = RuntimeError("listener failed")
    with pytest.raises(RuntimeError, match="listener failed"):
        listener.wait(
            queue_name="jobs",
            stop_event=threading.Event(),
            timeout=1,
            last_queue_version=0,
            last_wildcard_version=7,
        )


def test_activity_registry_ignores_unknown_release() -> None:
    registry = pg_runner_module._SharedActivityRegistry()

    registry.release("unknown-dsn", schema="unknown_schema")

    assert registry._listeners == {}


def test_activity_waiters_clamp_versions_and_translate_driver_errors() -> None:
    class SingleListener(pg_runner_module._SharedActivityListener):
        def __init__(self) -> None:
            self.error: Exception | None = None

        def wait(self, **kwargs: object) -> tuple[bool, int, int]:
            if self.error is not None:
                raise self.error
            return True, 4, 8

    single = object.__new__(pg_runner_module.PostgresActivityWaiter)
    single_listener = SingleListener()
    single._listener = single_listener
    single._queue_name = "jobs"
    single._stop_event = threading.Event()
    single._last_queue_version = 4
    single._last_wildcard_version = 5

    assert single.wait(1.0) is True
    assert single._last_queue_version == 4
    assert single._last_wildcard_version == 6

    single_listener.error = PsycopgOperationalError("listener connection failed")
    with pytest.raises(OperationalError, match="listener connection failed"):
        single.wait(1.0)

    class MultiListener(pg_runner_module._SharedActivityListener):
        def __init__(self) -> None:
            return None

        def wait_any(self, **kwargs: object) -> tuple[bool, dict[str, int], int]:
            return True, {"jobs": 3, "other": 1}, 9

    multi = object.__new__(pg_runner_module.PostgresMultiQueueActivityWaiter)
    multi._listener = MultiListener()
    multi._fan_in_id = 1
    multi._queue_names = ("jobs", "other")
    multi._stop_event = threading.Event()
    multi._last_queue_versions = {"jobs": 1, "other": 1}
    multi._last_wildcard_version = 5

    assert multi.wait(1.0) is True
    assert multi._last_queue_versions == {"jobs": 2, "other": 1}
    assert multi._last_wildcard_version == 6

    with pytest.raises(ValueError, match="queue_names cannot be empty"):
        pg_runner_module.PostgresMultiQueueActivityWaiter(
            "postgresql://example/test",
            schema="broker_data",
            queue_names=(),
            stop_event=threading.Event(),
        )


def test_connection_return_helpers_handle_active_and_mismatched_leases() -> None:
    runner, pool = _runner_with_thread_connection()
    runner_state = cast(Any, runner)
    runner_state._leased_conn = pool.conn
    runner_state._lease_depth = 1

    runner._return_thread_conn()
    runner._return_leased_conn(object())  # type: ignore[arg-type]
    runner._return_thread_conn_after_operation()

    assert runner._thread_local.conn is pool.conn
    assert runner._leased_conn is pool.conn
    assert pool.putconn_calls == 0


def test_begin_handles_lease_ending_between_precheck_and_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, pool = _runner_with_fake_pool()
    monkeypatch.setattr(runner, "_has_leased_connection", lambda: True)
    monkeypatch.setattr(runner, "_uses_leased_connection", lambda conn: False)

    runner.begin_immediate()

    assert runner._thread_local.in_transaction is True
    assert runner._thread_local.transaction_uses_leased_conn is False
    assert runner._leased_operation_lock.acquire(blocking=False)
    runner._leased_operation_lock.release()
    runner.rollback()
    assert pool.putconn_calls == 1


def test_release_empty_lease_and_mark_bootstrap_are_idempotent() -> None:
    runner, pool = _runner_with_fake_pool()
    runner._lease_depth = 1
    runner._leased_conn = None

    runner.release_thread_connection()

    assert runner._lease_depth == 0
    assert pool.putconn_calls == 0

    runner._meta_cache_lock = threading.Lock()
    runner._schema_bootstrapped = False
    runner.mark_schema_bootstrapped()
    assert runner._schema_bootstrapped is True


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
