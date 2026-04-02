"""FIFO and same-queue serialization tests for the Postgres backend."""

from __future__ import annotations

import threading
from typing import cast

import pytest
from simplebroker_pg import PostgresRunner

from simplebroker._backend_plugins import BackendPlugin
from simplebroker._sql import RetrieveQuerySpec
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


def test_same_queue_claim_waits_instead_of_skipping(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    pg_schema: str,
) -> None:
    """A second consumer must wait for the queue head, not overtake it."""
    runner_a = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_b = PostgresRunner(pg_dsn, schema=pg_schema)
    core_a = BrokerCore(runner_a, backend_plugin=pg_plugin)
    core_b = BrokerCore(runner_b, backend_plugin=pg_plugin)

    try:
        core_a.write("jobs", "first")
        core_a.write("jobs", "second")

        query, params = pg_plugin.sql.build_retrieve_query(
            "claim",
            RetrieveQuerySpec(
                queue="jobs",
                limit=1,
                offset=0,
                exact_timestamp=None,
                since_timestamp=None,
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

        def consume_second() -> None:
            result["value"] = cast(
                tuple[str, int] | None,
                core_b.claim_one("jobs", with_timestamps=True),
            )
            finished.set()

        thread = threading.Thread(target=consume_second, daemon=True)
        thread.start()

        assert finished.wait(0.2) is False

        runner_a.commit()

        assert finished.wait(2.0) is True
        assert result["value"] is not None
        assert result["value"][0] == "second"
        thread.join(timeout=2.0)
    finally:
        core_a.close()
        core_b.close()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )
