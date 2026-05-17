"""Maintenance and delete-count behavior for the Postgres backend."""

from __future__ import annotations

import pytest
from simplebroker_pg import PostgresRunner

from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


def test_delete_returns_exact_server_counts(pg_core: BrokerCore) -> None:
    """Bulk delete paths should return row counts without materializing rows."""
    pg_core.write("jobs", "one")
    pg_core.write("jobs", "two")
    pg_core.write("other", "three")

    assert pg_core.delete("jobs") == 2
    assert pg_core.delete() == 1
    assert pg_core.delete() == 0


def test_vacuum_removes_claimed_rows(pg_core: BrokerCore) -> None:
    """Backend vacuum should reclaim claimed rows on Postgres."""
    pg_core.write("jobs", "one")
    pg_core.write("jobs", "two")
    pg_core.write("jobs", "three")

    assert pg_core.claim_one("jobs", with_timestamps=True) is not None
    assert pg_core.claim_one("jobs", with_timestamps=True) is not None

    assert pg_core.get_overall_stats() == (2, 3)

    pg_core.vacuum()

    assert pg_core.count_claimed_messages() == 0
    assert pg_core.get_overall_stats() == (0, 1)


def test_delete_message_ids_physically_removes_claimed_and_pending_rows(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
) -> None:
    """Exact batch delete should remove physical Postgres rows."""
    pg_core.write("jobs", "one")
    pg_core.write("jobs", "two")
    pg_core.write("jobs", "three")
    timestamps = dict(pg_core.peek_many("jobs", limit=10))

    assert (
        pg_core.claim_one(
            "jobs", exact_timestamp=timestamps["two"], with_timestamps=False
        )
        == "two"
    )
    before_total = list(pg_runner.run("SELECT COUNT(*) FROM messages", fetch=True))
    before_claimed = list(
        pg_runner.run("SELECT COUNT(*) FROM messages WHERE claimed = TRUE", fetch=True)
    )
    assert before_total[0][0] == 3
    assert before_claimed[0][0] == 1

    deleted = pg_core.delete_message_ids(
        "jobs", [timestamps["two"], timestamps["three"]]
    )

    after_total = list(pg_runner.run("SELECT COUNT(*) FROM messages", fetch=True))
    after_claimed = list(
        pg_runner.run("SELECT COUNT(*) FROM messages WHERE claimed = TRUE", fetch=True)
    )
    assert deleted == 2
    assert after_total[0][0] == 1
    assert after_claimed[0][0] == 0
