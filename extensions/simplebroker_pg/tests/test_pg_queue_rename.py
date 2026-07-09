"""Postgres-specific queue rename behavior."""

from __future__ import annotations

import contextlib
import threading
import time
from typing import cast

import psycopg
import pytest
from simplebroker_pg import PostgresRunner
from simplebroker_pg import _sql as pg_sql
from simplebroker_pg.validation import quote_ident

from simplebroker import Queue
from simplebroker._backend_plugins import BackendPlugin
from simplebroker._runner import SQLRunner
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


class _RecordingRunner:
    schema = "test_schema"

    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    def run(
        self,
        sql: str,
        params: tuple[object, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[object, ...]]:
        del params
        self.calls.append((sql, fetch))
        return []


def _set_search_path(conn: psycopg.Connection[object], pg_schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {quote_ident(pg_schema)}, public")


def _lock_messages_nowait(pg_dsn: str, pg_schema: str) -> bool:
    with psycopg.connect(pg_dsn, autocommit=False) as conn:
        _set_search_path(conn, pg_schema)
        with conn.cursor() as cur:
            try:
                cur.execute("LOCK TABLE messages IN ROW EXCLUSIVE MODE NOWAIT")
            except psycopg.errors.LockNotAvailable:
                conn.rollback()
                return False
        conn.rollback()
        return True


def test_postgres_rename_notifies_old_and_new_waiters(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
) -> None:
    old_queue = Queue("old", runner=pg_runner, persistent=True)
    new_queue = Queue("new", runner=pg_runner, persistent=True)
    other_queue = Queue("other", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    old_waiter = None
    new_waiter = None
    other_waiter = None

    try:
        old_waiter = old_queue.create_activity_waiter(stop_event=stop_event)
        new_waiter = new_queue.create_activity_waiter(stop_event=stop_event)
        other_waiter = other_queue.create_activity_waiter(stop_event=stop_event)
        assert old_waiter is not None
        assert new_waiter is not None
        assert other_waiter is not None

        pg_core.write("old", "payload")
        assert old_waiter.wait(1.5) is True
        assert new_waiter.wait(0.2) is False

        pg_core.rename_queue("old", "new")

        assert old_waiter.wait(1.5) is True
        assert new_waiter.wait(1.5) is True
        assert other_waiter.wait(0.2) is False
    finally:
        if old_waiter is not None:
            old_waiter.close()
        if new_waiter is not None:
            new_waiter.close()
        if other_waiter is not None:
            other_waiter.close()
        old_queue.close()
        new_queue.close()
        other_queue.close()


def test_postgres_prepare_rename_locks_meta_before_messages(
    pg_plugin: BackendPlugin,
) -> None:
    runner = _RecordingRunner()

    pg_plugin.prepare_queue_operation(
        cast(SQLRunner, runner),
        operation="rename",
        queue="old",
    )

    assert runner.calls == [
        (pg_sql.LOCK_LAST_TS_ROW, True),
        (pg_sql.LOCK_RENAME_SCOPE, False),
    ]


def test_postgres_prepare_rename_waits_for_meta_before_messages_lock(
    pg_core: BrokerCore,
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
) -> None:
    pg_core.write("old", "payload")
    rename_runner = PostgresRunner(pg_dsn, schema=pg_schema)
    prepare_started = threading.Event()
    prepare_returned = threading.Event()
    release_prepare = threading.Event()
    errors: list[BaseException] = []

    def run_prepare() -> None:
        try:
            rename_runner.begin_immediate()
            prepare_started.set()
            pg_plugin.prepare_queue_operation(
                rename_runner,
                operation="rename",
                queue="old",
            )
            prepare_returned.set()
            release_prepare.wait(timeout=3.0)
            rename_runner.rollback()
        except BaseException as exc:
            errors.append(exc)
            with contextlib.suppress(Exception):
                rename_runner.rollback()

    try:
        with psycopg.connect(pg_dsn, autocommit=False) as conn:
            _set_search_path(conn, pg_schema)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_ts FROM meta WHERE singleton = TRUE FOR UPDATE"
                )

            thread = threading.Thread(target=run_prepare, daemon=True)
            thread.start()
            assert prepare_started.wait(timeout=3.0)

            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                assert not prepare_returned.is_set()
                assert _lock_messages_nowait(pg_dsn, pg_schema)
                time.sleep(0.01)

            conn.rollback()
            assert prepare_returned.wait(timeout=3.0)
            release_prepare.set()
            thread.join(timeout=3.0)
            assert not thread.is_alive()

        assert errors == []
    finally:
        release_prepare.set()
        rename_runner.shutdown()


def test_postgres_rename_waits_for_write_like_table_lock(
    pg_core: BrokerCore,
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
) -> None:
    pg_core.write("old", "payload")
    rename_runner = PostgresRunner(pg_dsn, schema=pg_schema)
    rename_core = BrokerCore(rename_runner, backend_plugin=pg_plugin)
    finished = threading.Event()
    results: list[object] = []
    errors: list[BaseException] = []

    def run_rename() -> None:
        try:
            results.append(rename_core.rename_queue("old", "new"))
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    try:
        with psycopg.connect(pg_dsn, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quote_ident(pg_schema)}, public")
                cur.execute("LOCK TABLE messages IN ROW EXCLUSIVE MODE")

            thread = threading.Thread(target=run_rename, daemon=True)
            thread.start()
            assert finished.wait(0.2) is False
            conn.commit()

            assert finished.wait(3.0) is True
            thread.join(timeout=1.0)

        assert errors == []
        assert len(results) == 1
        assert pg_core.peek_many("old", limit=10, with_timestamps=False) == []
        assert pg_core.peek_many("new", limit=10, with_timestamps=False) == ["payload"]
    finally:
        rename_core.close()
        rename_runner.shutdown()
