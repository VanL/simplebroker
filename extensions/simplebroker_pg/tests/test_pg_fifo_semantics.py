"""FIFO and same-queue serialization tests for the Postgres backend."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import cast

import pytest
from simplebroker_pg import PostgresRunner

from simplebroker._backend_plugins import BackendPlugin
from simplebroker._sql import RetrieveQuerySpec
from simplebroker.db import BrokerCore
from simplebroker.ext import DatabaseError

pytestmark = [pytest.mark.pg_only]


def test_same_queue_claim_waits_instead_of_skipping(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    pg_schema: str,
    pg_wait_for_blocked: Callable[[list[int], int, threading.Event, float], None],
) -> None:
    """A second consumer must wait for the queue head, not overtake it."""
    runner_a = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_b = PostgresRunner(pg_dsn, schema=pg_schema)
    core_a = BrokerCore(runner_a, backend_plugin=pg_plugin)
    core_b = BrokerCore(runner_b, backend_plugin=pg_plugin)
    thread: threading.Thread | None = None
    errors: list[DatabaseError] = []

    try:
        core_a.write("jobs", "first")
        core_a.write("jobs", "second")

        assert pg_plugin.sql is not None
        query, params = pg_plugin.sql.build_retrieve_query(
            "claim",
            RetrieveQuerySpec(
                queue="jobs",
                limit=1,
                offset=0,
                exact_timestamp=None,
                after_timestamp=None,
                require_unclaimed=True,
                target_queue=None,
            ),
        )

        runner_a.begin_immediate()
        pg_plugin.prepare_queue_operation(
            runner_a,
            operation="claim",
            queue="jobs",
        )
        locked_rows = list(runner_a.run(query, params, fetch=True))
        assert [row[0] for row in locked_rows] == ["first"]

        result: dict[str, tuple[str, int] | None] = {}
        finished = threading.Event()
        contender_pids: list[int] = []

        def consume_second() -> None:
            try:
                contender_pids.append(runner_b._get_thread_conn().info.backend_pid)
                result["value"] = cast(
                    tuple[str, int] | None,
                    core_b.claim_one("jobs", with_timestamps=True),
                )
            except DatabaseError as exc:
                errors.append(exc)
            finally:
                finished.set()

        thread = threading.Thread(target=consume_second, daemon=True)
        thread.start()

        pg_wait_for_blocked(
            contender_pids, runner_a._get_thread_conn().info.backend_pid, finished, 2.0
        )

        runner_a.commit()

        assert finished.wait(2.0) is True
        assert errors == []
        assert result["value"] is not None
        assert result["value"][0] == "second"
        thread.join(timeout=2.0)
    finally:
        if runner_a._in_transaction():
            runner_a.rollback()
        if thread is not None and thread.ident is not None:
            thread.join(timeout=2.0)
        core_a.close()
        core_b.close()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )
