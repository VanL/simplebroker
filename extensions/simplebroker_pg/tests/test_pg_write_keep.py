"""PostgreSQL locking behavior for write-time pending windows."""

from __future__ import annotations

import concurrent.futures
import threading
from typing import Any

import psycopg
import pytest
from psycopg import sql
from simplebroker_pg import PostgresRunner
from simplebroker_pg import _sql as pg_sql

from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


class _PausingKeepLockRunner:
    """Delegate to a real runner and pause only after the keep table lock."""

    def __init__(self, inner: PostgresRunner) -> None:
        self._inner = inner
        self.locked = threading.Event()
        self.release = threading.Event()

    def run(
        self,
        statement: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Any:
        rows = self._inner.run(statement, params, fetch=fetch)
        if statement == pg_sql.LOCK_WRITE_KEEP_SCOPE:
            self.locked.set()
            if not self.release.wait(timeout=5):
                raise AssertionError("keep-write table lock was not released")
        return rows

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _MutationAttemptRunner:
    """Signal at the actual runner call boundary after setup is complete."""

    def __init__(self, inner: PostgresRunner) -> None:
        self._inner = inner
        self.attempted = threading.Event()
        self._armed = False

    def arm(self) -> None:
        self.attempted.clear()
        self._armed = True

    def run(
        self,
        statement: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Any:
        if self._armed:
            self.attempted.set()
        return self._inner.run(statement, params, fetch=fetch)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _run_competing_operation(core: BrokerCore, operation: str) -> object:
    if operation == "move":
        return core.move_one("source", "moved", with_timestamps=False)
    if operation == "claim":
        return core.claim_one("source", with_timestamps=False)
    if operation == "insert":
        return core.insert_messages([("unrelated", "exact", 1)])
    if operation == "broadcast":
        return core.broadcast("broadcast", queue_names=["unrelated"])
    if operation == "write":
        return core.write("unrelated", "ordinary")
    if operation == "delete":
        return core.delete("source")
    if operation == "rename":
        return core.rename_queue("source", "renamed")
    raise AssertionError(f"unknown operation: {operation}")


def _assert_competing_operation_result(
    core: BrokerCore,
    operation: str,
    result: Any,
) -> None:
    if operation == "move":
        assert result == "movable"
        assert core.peek_one("source") is None
        assert core.peek_one("moved", with_timestamps=False) == "movable"
    elif operation == "claim":
        assert result == "movable"
        assert core.peek_one("source") is None
        assert core.peek_many(
            "source",
            limit=10,
            with_timestamps=False,
            include_claimed=True,
        ) == ["movable"]
    elif operation == "insert":
        assert result is None
        assert core.peek_one("unrelated", with_timestamps=False) == "exact"
    elif operation == "broadcast":
        assert result == 1
        assert (
            core.peek_many("unrelated", limit=10, with_timestamps=False)[-1]
            == "broadcast"
        )
    elif operation == "write":
        assert isinstance(result, int)
        assert (
            core.peek_many("unrelated", limit=10, with_timestamps=False)[-1]
            == "ordinary"
        )
    elif operation == "delete":
        assert result == 1
        assert core.peek_one("source") is None
    else:
        assert result.messages_renamed == 1
        assert core.peek_one("source") is None
        assert core.peek_one("renamed", with_timestamps=False) == "movable"


def test_keep_write_blocks_unrelated_queue_row_mutation_until_commit(
    pg_dsn: str,
    pg_schema: str,
    pg_core: BrokerCore,
) -> None:
    pg_core.write("snapshots", "old")
    inner = PostgresRunner(pg_dsn, schema=pg_schema)
    runner = _PausingKeepLockRunner(inner)
    writer = BrokerCore(runner, backend_plugin=pg_core._backend_plugin)
    insert_started = threading.Event()
    insert_finished = threading.Event()

    def insert_unrelated() -> None:
        insert_started.set()
        with (
            psycopg.connect(pg_dsn) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(pg_schema))
            )
            cursor.execute(
                "INSERT INTO messages (queue, body, ts, claimed) "
                "VALUES (%s, %s, %s, FALSE)",
                ("unrelated", "manual", 1),
            )
        insert_finished.set()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            keep_future = executor.submit(
                writer.write,
                "snapshots",
                "new",
                keep_newest=1,
            )
            assert runner.locked.wait(timeout=5)
            insert_future = executor.submit(insert_unrelated)
            assert insert_started.wait(timeout=5)
            assert not insert_finished.wait(timeout=0.2)

            runner.release.set()
            assert keep_future.result(timeout=5) > 1
            insert_future.result(timeout=5)

        assert pg_core.peek_many("snapshots", limit=10, with_timestamps=False) == [
            "new"
        ]
        assert pg_core.peek_one("unrelated", with_timestamps=False) == "manual"
    finally:
        runner.release.set()
        writer.close()
        inner.shutdown()


@pytest.mark.parametrize(
    "operation",
    ["move", "claim", "insert", "broadcast", "write", "delete", "rename"],
)
def test_keep_write_serializes_every_pending_row_producer_and_queue_mutator(
    pg_dsn: str,
    pg_schema: str,
    pg_core: BrokerCore,
    operation: str,
) -> None:
    """Every competing mutation waits, then completes in keep-before-op order."""
    pg_core.write("snapshots", "old")
    pg_core.write("source", "movable")
    pg_core.write("unrelated", "existing")
    keep_inner = PostgresRunner(pg_dsn, schema=pg_schema)
    keep_runner = _PausingKeepLockRunner(keep_inner)
    keep_writer = BrokerCore(
        keep_runner,
        backend_plugin=pg_core._backend_plugin,
    )
    contender_inner = PostgresRunner(pg_dsn, schema=pg_schema)
    contender_runner = _MutationAttemptRunner(contender_inner)
    contender = BrokerCore(
        contender_runner,
        backend_plugin=pg_core._backend_plugin,
    )
    operation_finished = threading.Event()
    contender_runner.arm()

    def run_operation() -> object:
        try:
            return _run_competing_operation(contender, operation)
        finally:
            operation_finished.set()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            keep_future = executor.submit(
                keep_writer.write,
                "snapshots",
                "new",
                keep_newest=1,
            )
            assert keep_runner.locked.wait(timeout=5)
            operation_future = executor.submit(run_operation)
            assert contender_runner.attempted.wait(timeout=5)
            assert not operation_finished.wait(timeout=0.2)

            keep_runner.release.set()
            assert keep_future.result(timeout=5) > 1
            result = operation_future.result(timeout=5)

        assert pg_core.peek_many("snapshots", limit=10, with_timestamps=False) == [
            "new"
        ]
        _assert_competing_operation_result(pg_core, operation, result)
    finally:
        keep_runner.release.set()
        contender.close()
        contender_inner.shutdown()
        keep_writer.close()
        keep_inner.shutdown()
