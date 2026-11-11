"""PostgresRunner never returns a checkout whose statement is still in flight.

psycopg holds ``Connection.lock`` for a whole statement, and its execute
generators keep a raw libpq pointer across yields. A connection handed to the
pool while its owner is mid-statement can be closed (``PQfinish``) or checked
out by another thread under that statement. Each test pins a statement in
flight by hooking ``Connection.wait``, which psycopg calls inside that lock, and
spies on ``ConnectionPool.putconn``.

Plan: 2026-11-10-backend-thread-ownership-plan.md at 4cea0cb (F4, F5; review R1).
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg
import psycopg_pool
import pytest
import simplebroker_pg.runner as pg_runner_module
from simplebroker_pg import PostgresRunner

from simplebroker._backend_plugins import BackendPlugin

pytestmark = [pytest.mark.pg_only]

_PutconnRecord = tuple[threading.Thread, int, bool]


@pytest.fixture
def statement_hook(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Run one armed callable inside the next statement's connection lock."""
    armed: dict[str, Any] = {}
    real_wait = psycopg.Connection.wait

    def wait(self: psycopg.Connection[Any], *args: Any, **kwargs: Any) -> Any:
        if armed.get("thread") is threading.current_thread():
            hook = armed.pop("hook", None)
            if hook is not None:
                assert self.lock.locked()
                hook()
        return real_wait(self, *args, **kwargs)

    monkeypatch.setattr(psycopg.Connection, "wait", wait)
    return armed


@pytest.fixture
def execute_hook(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Run one armed callable before the Nth cursor execute (no lock held)."""
    armed: dict[str, Any] = {}
    real_execute = psycopg.Cursor.execute

    def execute(self: psycopg.Cursor[Any], *args: Any, **kwargs: Any) -> Any:
        if "hook" in armed and armed.get("thread") is threading.current_thread():
            armed["countdown"] -= 1
            if armed["countdown"] == 0:
                armed.pop("hook")()
        return real_execute(self, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    return armed


@pytest.fixture
def putconn_records(monkeypatch: pytest.MonkeyPatch) -> list[_PutconnRecord]:
    """Record each pool return: thread, connection id, statement in flight."""
    records: list[_PutconnRecord] = []
    real_putconn = psycopg_pool.ConnectionPool.putconn

    def putconn(
        self: psycopg_pool.ConnectionPool, conn: psycopg.Connection[Any]
    ) -> None:
        records.append((threading.current_thread(), id(conn), conn.lock.locked()))
        real_putconn(self, conn)

    monkeypatch.setattr(psycopg_pool.ConnectionPool, "putconn", putconn)
    return records


@pytest.fixture
def close_records(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[threading.Thread, int]]:
    """Record each psycopg connection close: thread and connection id."""
    records: list[tuple[threading.Thread, int]] = []
    real_close = psycopg.Connection.close

    def close(self: psycopg.Connection[Any]) -> None:
        if not self.closed:
            records.append((threading.current_thread(), id(self)))
        real_close(self)

    monkeypatch.setattr(psycopg.Connection, "close", close)
    return records


@pytest.fixture
def pg_target(
    pg_dsn: str, pg_schema: str, pg_plugin: BackendPlugin
) -> Iterator[tuple[str, str]]:
    try:
        yield pg_dsn, pg_schema
    finally:
        pg_plugin.cleanup_target(pg_dsn, backend_options={"schema": pg_schema})


@pytest.mark.parametrize(
    "window",
    [
        "before-execute",
        "in-statement",
        "between-transaction-statements",
        "leased",
        "two-leases",
    ],
)
def test_reentrant_release_leaves_the_operation_its_checkout(
    window: str,
    pg_target: tuple[str, str],
    statement_hook: dict[str, Any],
    execute_hook: dict[str, Any],
    putconn_records: list[_PutconnRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A release requested on a thread mid-operation waits for that operation.

    ``BrokerCore.close()`` on a caller-supplied runner calls
    ``release_thread_connection()``; when that runs re-entrantly (a finalizer
    or callback on the thread that is inside a runner operation), the runner
    still owns the checkout until the operation's own cleanup.
    """
    dsn, schema = pg_target
    runner = PostgresRunner(dsn, schema=schema)
    in_hook: list[_PutconnRecord] = []

    def reentrant_release() -> None:
        before = len(putconn_records)
        runner.release_thread_connection()
        if window == "two-leases":
            runner.release_thread_connection()
        in_hook.extend(putconn_records[before:])

    try:
        # Warm up: the pool's configure step runs statements on a worker thread.
        assert list(runner.run("SELECT 1", fetch=True)) == [(1,)]
        me = threading.current_thread()
        putconn_records.clear()
        if window in {"leased", "two-leases"}:
            runner.lease_thread_connection()
        if window == "two-leases":
            runner.lease_thread_connection()
        if window == "before-execute":
            execute_hook.update(thread=me, countdown=1, hook=reentrant_release)
            assert list(runner.run("SELECT 41 + 1", fetch=True)) == [(42,)]
        elif window in {"in-statement", "leased", "two-leases"}:
            statement_hook.update(thread=me, hook=reentrant_release)
            assert list(runner.run("SELECT 41 + 1", fetch=True)) == [(42,)]
        else:
            monkeypatch.setattr(pg_runner_module, "_pipeline_supported", lambda: False)
            execute_hook.update(thread=me, countdown=2, hook=reentrant_release)
            results = runner.run_transaction(
                (("SELECT txid_current()", ()), ("SELECT txid_current()", ()))
            )
            # Both statements ran in the one transaction the operation began.
            assert results[0] == results[1]
        assert "hook" not in statement_hook
        assert "hook" not in execute_hook
        assert in_hook == []
        if window in {"leased", "two-leases"}:
            # Every deferred release was kept and surrendered after the
            # operation, returning the leased checkout exactly once.
            assert runner._lease_depth == 0
            assert runner._leased_conn is None
            assert len([r for r in putconn_records if r[0] is me]) == 1
    finally:
        runner.shutdown()


def test_shutdown_leaves_a_live_lease_owner_its_checkout(
    pg_target: tuple[str, str],
    statement_hook: dict[str, Any],
    putconn_records: list[_PutconnRecord],
    close_records: list[tuple[threading.Thread, int]],
) -> None:
    """Shutdown while a lease owner on another thread is mid-statement."""
    dsn, schema = pg_target
    runner = PostgresRunner(dsn, schema=schema)
    in_statement = threading.Event()
    release_statement = threading.Event()
    owner_threads: list[threading.Thread] = []

    def hold() -> None:
        in_statement.set()
        if not release_statement.wait(10.0):
            # Fail closed: never enter libpq while shutdown may still run.
            raise AssertionError("statement gate was never released")

    def lease_owner() -> list[tuple[Any, ...]]:
        owner_threads.append(threading.current_thread())
        runner.lease_thread_connection()
        try:
            statement_hook.update(thread=threading.current_thread(), hook=hold)
            return list(runner.run("SELECT 1", fetch=True))
        finally:
            runner.release_thread_connection()

    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lease-owner")
    try:
        result = pool.submit(lease_owner)
        assert in_statement.wait(10.0)
        runner.shutdown()
        # Shutdown returned while the owner's statement is still held.
        assert not result.done()
    finally:
        release_statement.set()
        pool.shutdown(wait=True)
    # The owner's statement completed on its own checkout.
    assert result.result() == [(1,)]
    [owner] = owner_threads
    assert [r for r in putconn_records if r[2]] == []
    assert [r[0] for r in putconn_records] == [owner]
    # The closed pool closed the returned checkout on its owner thread.
    owner_conn_id = putconn_records[0][1]
    assert (owner, owner_conn_id) in close_records
    assert all(thread is owner for thread, cid in close_records if cid == owner_conn_id)


def test_reentrant_shutdown_waits_for_the_operation(
    pg_target: tuple[str, str],
    statement_hook: dict[str, Any],
    putconn_records: list[_PutconnRecord],
    close_records: list[tuple[threading.Thread, int]],
) -> None:
    """Shutdown run by a callback on the thread that is mid-statement."""
    dsn, schema = pg_target
    runner = PostgresRunner(dsn, schema=schema)
    assert list(runner.run("SELECT 1", fetch=True)) == [(1,)]
    me = threading.current_thread()
    runner.lease_thread_connection()
    in_hook: list[_PutconnRecord] = []

    def reentrant_shutdown() -> None:
        before = len(putconn_records)
        runner.shutdown()
        in_hook.extend(putconn_records[before:])

    putconn_records.clear()
    statement_hook.update(thread=me, hook=reentrant_shutdown)
    assert list(runner.run("SELECT 41 + 1", fetch=True)) == [(42,)]
    assert in_hook == []
    # After the operation, its leased checkout went back into the closed pool
    # on this thread and was closed here.
    assert [r[0] for r in putconn_records] == [me]
    assert (me, putconn_records[0][1]) in close_records
    assert runner._lease_depth == 0


def test_first_transaction_after_fork_recovery_runs_in_a_fresh_scope(
    pg_target: tuple[str, str],
) -> None:
    """Fork recovery replaces thread-local state before an operation's scope.

    A child's first runner call recovers process-owned state; the operation
    scope must start after that recovery, or its exit acts on state the
    recovery discarded (and would raise after the transaction committed).
    """
    dsn, schema = pg_target
    runner = PostgresRunner(dsn, schema=schema)
    inherited_pool = runner._pool
    try:
        assert list(runner.run("SELECT 1", fetch=True)) == [(1,)]
        runner._pid = -1  # the next call recovers as a forked child would
        assert runner.run_transaction((("SELECT 41 + 1", ()),)) == [[(42,)]]
        assert not hasattr(runner._thread_local, "operation_depth")
    finally:
        runner.shutdown()
        if inherited_pool in pg_runner_module._ABANDONED_FORK_POSTGRES_POOLS:
            pg_runner_module._ABANDONED_FORK_POSTGRES_POOLS.remove(inherited_pool)
        inherited_pool.close()
