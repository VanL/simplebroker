"""Postgres tests for targeted queue metadata APIs."""

from __future__ import annotations

import os
import uuid

import pytest
from simplebroker_pg import PostgresRunner, get_backend_plugin

from simplebroker.db import BrokerCore

TEST_DSN = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
pytestmark = [
    pytest.mark.pg_only,
    pytest.mark.skipif(
        not TEST_DSN,
        reason="Set SIMPLEBROKER_PG_TEST_DSN to run Postgres extension tests",
    ),
]


def _schema_name() -> str:
    return f"sbtest_metadata_{uuid.uuid4().hex[:12]}"


def _require_test_dsn() -> str:
    if TEST_DSN is None:
        raise RuntimeError("SIMPLEBROKER_PG_TEST_DSN is required for pg tests")
    return TEST_DSN


def test_postgres_get_queue_stat_counts_claimed_rows() -> None:
    schema = _schema_name()
    dsn = _require_test_dsn()
    plugin = get_backend_plugin()
    runner = PostgresRunner(dsn, schema=schema)
    core: BrokerCore | None = None

    try:
        core = BrokerCore(runner, backend_plugin=plugin)
        for message in ("one", "two", "three"):
            core.write("jobs", message)

        assert core.claim_one("jobs", with_timestamps=False) == "one"

        assert core.queue_exists("jobs") is True
        stats = core.get_queue_stat("jobs")
        assert (stats.queue, stats.pending, stats.claimed, stats.total) == (
            "jobs",
            2,
            1,
            3,
        )
        missing = core.get_queue_stat("missing")
        assert (missing.queue, missing.pending, missing.claimed, missing.total) == (
            "missing",
            0,
            0,
            0,
        )
    finally:
        if core is not None:
            core.close()
        runner.close()
        plugin.cleanup_target(dsn, backend_options={"schema": schema})


def test_postgres_list_queue_stats_filters_by_prefix() -> None:
    schema = _schema_name()
    dsn = _require_test_dsn()
    plugin = get_backend_plugin()
    runner = PostgresRunner(dsn, schema=schema)
    core: BrokerCore | None = None

    try:
        core = BrokerCore(runner, backend_plugin=plugin)
        for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
            core.write(queue, f"message for {queue}")

        assert core.claim_one("weft.jobs.b", with_timestamps=False) == (
            "message for weft.jobs.b"
        )

        stats = core.list_queue_stats(prefix="weft.jobs.")
        assert [
            (item.queue, item.pending, item.claimed, item.total) for item in stats
        ] == [
            ("weft.jobs.a", 1, 0, 1),
            ("weft.jobs.b", 0, 1, 1),
        ]
    finally:
        if core is not None:
            core.close()
        runner.close()
        plugin.cleanup_target(dsn, backend_options={"schema": schema})
