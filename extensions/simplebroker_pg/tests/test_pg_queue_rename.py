"""Postgres-specific queue rename behavior."""

from __future__ import annotations

import threading

import psycopg
import pytest
from simplebroker_pg import PostgresRunner
from simplebroker_pg.validation import quote_ident

from simplebroker import Queue
from simplebroker._backend_plugins import BackendPlugin
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


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
        assert pg_core.peek_many("new", limit=10, with_timestamps=False) == [
            "payload"
        ]
    finally:
        rename_core.close()
        rename_runner.shutdown()
