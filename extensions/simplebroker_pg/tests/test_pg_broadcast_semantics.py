"""Broadcast consistency tests for the Postgres backend."""

from __future__ import annotations

import threading

import pytest
from simplebroker_pg import PostgresRunner

from simplebroker._backend_plugins import BackendPlugin
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


def test_prepare_broadcast_excludes_concurrent_new_queue(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    pg_schema: str,
) -> None:
    """A queue created during broadcast should not join the in-flight fan-out set."""
    runner_broadcast = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_writer = PostgresRunner(pg_dsn, schema=pg_schema)
    broadcast_core = BrokerCore(runner_broadcast, backend_plugin=pg_plugin)
    writer_core = BrokerCore(runner_writer, backend_plugin=pg_plugin)

    try:
        broadcast_core.write("alpha", "seed-alpha")

        runner_broadcast.begin_immediate()
        pg_plugin.prepare_broadcast(runner_broadcast)

        assert pg_plugin.sql is not None
        queues = [
            row[0]
            for row in runner_broadcast.run(
                pg_plugin.sql.GET_DISTINCT_QUEUES, fetch=True
            )
        ]
        assert queues == ["alpha"]

        writer_done = threading.Event()

        def create_new_queue() -> None:
            writer_core.write("late", "seed-late")
            writer_done.set()

        thread = threading.Thread(target=create_new_queue, daemon=True)
        thread.start()

        assert writer_done.wait(0.2) is False

        timestamp = broadcast_core.generate_timestamp()
        runner_broadcast.run(
            pg_plugin.sql.INSERT_MESSAGE,
            ("alpha", "announcement", timestamp),
        )
        runner_broadcast.commit()

        assert writer_done.wait(2.0) is True
        thread.join(timeout=2.0)

        assert broadcast_core.peek_many("alpha", with_timestamps=False) == [
            "seed-alpha",
            "announcement",
        ]
        assert writer_core.peek_many("late", with_timestamps=False) == ["seed-late"]
    finally:
        broadcast_core.close()
        writer_core.close()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


def test_exact_broadcast_does_not_resurrect_queue_deleted_before_selection(
    pg_core: BrokerCore,
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    pg_schema: str,
) -> None:
    """Broadcast waits for an in-flight delete, then observes the committed absence."""
    pg_core.write("victim", "seed")
    delete_runner = PostgresRunner(pg_dsn, schema=pg_schema)
    broadcast_runner = PostgresRunner(pg_dsn, schema=pg_schema)
    broadcast_core = BrokerCore(broadcast_runner, backend_plugin=pg_plugin)
    finished = threading.Event()
    results: list[int] = []
    errors: list[BaseException] = []

    def run_broadcast() -> None:
        try:
            results.append(
                broadcast_core.broadcast(
                    "notice",
                    queue_names=("victim",),
                )
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    try:
        delete_runner.begin_immediate()
        assert (
            pg_plugin.delete_from_queues(
                delete_runner,
                queue_names=("victim",),
            )
            == 1
        )

        thread = threading.Thread(target=run_broadcast, daemon=True)
        thread.start()
        assert finished.wait(0.2) is False

        delete_runner.commit()
        assert finished.wait(3.0) is True
        thread.join(timeout=1.0)

        assert errors == []
        assert results == [0]
        assert pg_core.peek_many("victim", limit=10, with_timestamps=False) == []
    finally:
        if not finished.is_set():
            try:
                delete_runner.rollback()
            except Exception:
                pass
        broadcast_core.close()
        delete_runner.shutdown()
