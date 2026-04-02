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
