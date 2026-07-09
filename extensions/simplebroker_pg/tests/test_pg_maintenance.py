"""Maintenance and delete-count behavior for the Postgres backend."""

from __future__ import annotations

from typing import cast

import pytest
from simplebroker_pg import PostgresRunner
from simplebroker_pg.plugin import PostgresBackendPlugin

from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


def _counts_by_queue(pg_runner: PostgresRunner) -> dict[str, int]:
    rows = list(
        pg_runner.run(
            "SELECT queue, COUNT(*) FROM messages GROUP BY queue ORDER BY queue",
            fetch=True,
        )
    )
    return {str(queue): int(count) for queue, count in rows}


def _timestamp_map(rows: list[tuple[str, int]] | list[str]) -> dict[str, int]:
    return dict(cast(list[tuple[str, int]], rows))


class SessionTrackingVacuumRunner:
    """Fake runner that changes backend sessions unless explicitly leased."""

    schema = "test_schema"

    def __init__(self, *, forced_unlock_result: bool | None = None) -> None:
        self.lease_calls = 0
        self.release_calls = 0
        self.lock_session: int | None = None
        self.unlock_session: int | None = None
        self.unlock_fetch = False
        self.forced_unlock_result = forced_unlock_result
        self._leased = False
        self._leased_session: int | None = None
        self._transaction_session: int | None = None
        self._next_session_id = 1

    def lease_thread_connection(self) -> None:
        self.lease_calls += 1
        self._leased = True
        if self._leased_session is None:
            self._leased_session = self._next_session()

    def release_thread_connection(self) -> None:
        self.release_calls += 1
        self._leased = False

    def _next_session(self) -> int:
        session_id = self._next_session_id
        self._next_session_id += 1
        return session_id

    def _operation_session(self) -> int:
        if self._transaction_session is not None:
            return self._transaction_session
        if self._leased:
            assert self._leased_session is not None
            return self._leased_session
        return self._next_session()

    def run(
        self,
        sql: str,
        params: tuple[object, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[object, ...]]:
        del params
        session_id = self._operation_session()
        normalized = " ".join(sql.split())
        if "pg_try_advisory_lock" in normalized:
            self.lock_session = session_id
            return [(True,)] if fetch else []
        if "pg_advisory_unlock" in normalized:
            self.unlock_session = session_id
            self.unlock_fetch = fetch
            unlocked = (
                self.forced_unlock_result
                if self.forced_unlock_result is not None
                else session_id == self.lock_session
            )
            return [(unlocked,)] if fetch else []
        if "SELECT COUNT(*) FROM deleted" in normalized:
            return [(0,)] if fetch else []
        return []

    def begin_immediate(self) -> None:
        self._transaction_session = self._operation_session()

    def commit(self) -> None:
        self._transaction_session = None

    def rollback(self) -> None:
        self._transaction_session = None

    def close(self) -> None:
        return None


def test_vacuum_leases_connection_for_advisory_lock_lifetime() -> None:
    """Advisory lock acquire and release must run on one Postgres session."""
    runner = SessionTrackingVacuumRunner()

    PostgresBackendPlugin().vacuum(
        runner,  # type: ignore[arg-type]
        compact=False,
        config={"BROKER_VACUUM_BATCH_SIZE": 1000},
    )

    assert runner.lease_calls == 1
    assert runner.release_calls == 1
    assert runner.lock_session == runner.unlock_session
    assert runner.unlock_fetch is True


def test_vacuum_warns_when_advisory_unlock_fails() -> None:
    """A failed session-level unlock should not be silently ignored."""
    runner = SessionTrackingVacuumRunner(forced_unlock_result=False)

    with pytest.warns(RuntimeWarning, match="advisory lock release failed"):
        PostgresBackendPlugin().vacuum(
            runner,  # type: ignore[arg-type]
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert runner.release_calls == 1


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
    timestamps = _timestamp_map(pg_core.peek_many("jobs", limit=10))

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


def test_delete_from_queues_removes_selected_postgres_rows(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
) -> None:
    """Multi-queue delete should remove selected pending and claimed rows."""
    pg_core.write("alpha", "alpha1")
    pg_core.write("alpha", "alpha2")
    pg_core.write("beta", "beta1")
    pg_core.write("gamma", "gamma1")
    timestamps = _timestamp_map(pg_core.peek_many("alpha", limit=10))

    assert (
        pg_core.claim_one(
            "alpha",
            exact_timestamp=timestamps["alpha1"],
            with_timestamps=False,
        )
        == "alpha1"
    )
    assert _counts_by_queue(pg_runner) == {"alpha": 2, "beta": 1, "gamma": 1}

    deleted = pg_core.delete_from_queues(["alpha", "beta"])

    assert deleted == 3
    assert _counts_by_queue(pg_runner) == {"gamma": 1}


def test_delete_from_queues_postgres_before_timestamp_is_strict(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
) -> None:
    """The Postgres before filter should use ts < before_timestamp."""
    pg_core.write("alpha", "old-alpha")
    pg_core.write("beta", "old-beta")
    pg_core.write("gamma", "old-gamma")
    pg_core.write("alpha", "boundary-alpha")
    boundary_ts = _timestamp_map(pg_core.peek_many("alpha", limit=10))["boundary-alpha"]
    pg_core.write("alpha", "new-alpha")
    pg_core.write("beta", "new-beta")

    deleted = pg_core.delete_from_queues(
        ["alpha", "beta"],
        before_timestamp=boundary_ts,
    )

    rows = list(
        pg_runner.run(
            "SELECT queue, body FROM messages ORDER BY order_id",
            fetch=True,
        )
    )
    assert deleted == 2
    assert rows == [
        ("gamma", "old-gamma"),
        ("alpha", "boundary-alpha"),
        ("alpha", "new-alpha"),
        ("beta", "new-beta"),
    ]
