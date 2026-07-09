"""Postgres tests for latest pending timestamp."""

from __future__ import annotations

import pytest
from simplebroker_pg import PostgresRunner
from simplebroker_pg._constants import POSTGRES_SCHEMA_VERSION

from simplebroker._backend_plugins import BackendPlugin
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


def _queue_ts_order_unclaimed_index_exists(pg_runner: PostgresRunner) -> bool:
    rows = list(
        pg_runner.run(
            """
            SELECT EXISTS(
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname = 'idx_messages_queue_ts_order_unclaimed'
            )
            """,
            fetch=True,
        )
    )
    return bool(rows and rows[0][0])


def test_postgres_latest_pending_timestamp_ignores_claimed_newest(
    pg_core: BrokerCore,
) -> None:
    pg_core.insert_messages(
        [
            ("jobs", "old", 10),
            ("jobs", "middle", 20),
            ("jobs", "newest", 30),
        ]
    )

    assert pg_core.latest_pending_timestamp("jobs") == 30

    assert pg_core.claim_one("jobs", exact_timestamp=30, with_timestamps=True) == (
        "newest",
        30,
    )
    assert pg_core.latest_pending_timestamp("jobs") == 20

    assert pg_core.claim_many("jobs", 2, with_timestamps=True) == [
        ("old", 10),
        ("middle", 20),
    ]
    assert pg_core.latest_pending_timestamp("jobs") is None


def test_postgres_current_schema_recreates_missing_pending_timestamp_index(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
) -> None:
    del pg_core
    versions: list[int] = []

    assert _queue_ts_order_unclaimed_index_exists(pg_runner) is True

    pg_runner.run("DROP INDEX IF EXISTS idx_messages_queue_ts_order_unclaimed")
    assert _queue_ts_order_unclaimed_index_exists(pg_runner) is False

    pg_plugin.migrate_schema(
        pg_runner,
        current_version=POSTGRES_SCHEMA_VERSION,
        write_schema_version=versions.append,
    )

    assert versions == []
    assert _queue_ts_order_unclaimed_index_exists(pg_runner) is True
