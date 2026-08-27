"""Maintenance and delete-count behavior for the Postgres backend."""

from __future__ import annotations

import warnings
from typing import cast

import pytest
from simplebroker_pg import PostgresRunner
from simplebroker_pg._failure_order import (
    capture_ordinary_pg_cleanup,
    capture_pg_step,
)
from simplebroker_pg.plugin import PostgresBackendPlugin

from simplebroker._runner import SetupPhase
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

    def setup(self, phase: SetupPhase) -> None:
        del phase

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        del phase
        return True


def test_pg_step_capture_preserves_unformattable_base_exception() -> None:
    class UnformattableAbort(BaseException):
        def __str__(self) -> str:
            raise RuntimeError("stringified unexpectedly")

    failure = UnformattableAbort()

    def abort() -> None:
        raise failure

    result = capture_pg_step("body", abort)

    assert result.failure is failure


def test_pg_cleanup_note_does_not_format_secondary_failure() -> None:
    class UnformattableCleanupFailure(RuntimeError):
        def __str__(self) -> str:
            raise RuntimeError("cleanup failure was stringified")

    primary = RuntimeError("primary")
    cleanup_failure = UnformattableCleanupFailure("cleanup detail")

    def fail_cleanup() -> None:
        raise cleanup_failure

    capture_ordinary_pg_cleanup(
        primary=primary,
        phase="connection discard",
        action=fail_cleanup,
    )

    assert primary.__notes__ == [
        (
            "connection discard failure: "
            f"{type(cleanup_failure).__qualname__}: cleanup detail"
        )
    ]


class FailureOrderingVacuumRunner(SessionTrackingVacuumRunner):
    """Inject failures at vacuum boundaries without simulating lock release."""

    def __init__(
        self,
        *,
        body_failure: BaseException | None = None,
        unlock_failure: BaseException | None = None,
        discard_failure: BaseException | None = None,
        release_failure: BaseException | None = None,
        forced_unlock_result: bool | None = None,
    ) -> None:
        super().__init__(forced_unlock_result=forced_unlock_result)
        self.body_failure = body_failure
        self.unlock_failure = unlock_failure
        self.discard_failure = discard_failure
        self.release_failure = release_failure

    def run(
        self,
        sql: str,
        params: tuple[object, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[object, ...]]:
        normalized = " ".join(sql.split())
        if (
            "SELECT COUNT(*) FROM deleted" in normalized
            and self.body_failure is not None
        ):
            raise self.body_failure
        if "pg_advisory_unlock" in normalized and self.unlock_failure is not None:
            raise self.unlock_failure
        return super().run(sql, params, fetch=fetch)

    def _discard_thread_connection(self) -> None:
        if self.discard_failure is not None:
            raise self.discard_failure

    def release_thread_connection(self) -> None:
        super().release_thread_connection()
        if self.release_failure is not None:
            raise self.release_failure


def test_vacuum_leases_connection_for_advisory_lock_lifetime() -> None:
    """Advisory lock acquire and release must run on one Postgres session."""
    runner = SessionTrackingVacuumRunner()

    PostgresBackendPlugin().vacuum(
        runner,
        compact=False,
        config={"BROKER_VACUUM_BATCH_SIZE": 1000},
    )

    assert runner.lease_calls == 1
    assert runner.release_calls == 1
    assert runner.lock_session == runner.unlock_session
    assert runner.unlock_fetch is True


def test_vacuum_unlock_false_releases_without_warning() -> None:
    """Unlock false proves this session does not own the advisory lock."""
    runner = SessionTrackingVacuumRunner(forced_unlock_result=False)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert runner.release_calls == 1


def test_vacuum_discards_checkout_when_unlock_completion_is_unknown() -> None:
    class UnlockTransportFailureRunner(SessionTrackingVacuumRunner):
        def __init__(self) -> None:
            super().__init__()
            self.discard_calls = 0

        def run(
            self,
            sql: str,
            params: tuple[object, ...] = (),
            *,
            fetch: bool = False,
        ) -> list[tuple[object, ...]]:
            if "pg_advisory_unlock" in " ".join(sql.split()):
                raise RuntimeError("unlock transport failed")
            return super().run(sql, params, fetch=fetch)

        def _discard_thread_connection(self) -> None:
            self.discard_calls += 1

    runner = UnlockTransportFailureRunner()

    with pytest.raises(RuntimeError, match="unlock transport failed"):
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert runner.discard_calls == 1
    assert runner.release_calls == 1


def test_vacuum_body_base_exception_survives_ordinary_cleanup_failures() -> None:
    class BodyAbort(BaseException):
        pass

    class CleanupFailureRunner(SessionTrackingVacuumRunner):
        def run(
            self,
            sql: str,
            params: tuple[object, ...] = (),
            *,
            fetch: bool = False,
        ) -> list[tuple[object, ...]]:
            normalized = " ".join(sql.split())
            if "SELECT COUNT(*) FROM deleted" in normalized:
                raise BodyAbort("body aborted")
            if "pg_advisory_unlock" in normalized:
                raise RuntimeError("unlock failed")
            return super().run(sql, params, fetch=fetch)

        def _discard_thread_connection(self) -> None:
            raise RuntimeError("discard failed")

        def release_thread_connection(self) -> None:
            super().release_thread_connection()
            raise RuntimeError("release failed")

    runner = CleanupFailureRunner()

    with pytest.raises(BodyAbort, match="body aborted") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    notes = cast(tuple[str, ...], getattr(caught.value, "__notes__", ()))
    assert len(notes) == 3
    assert "unlock failed" in notes[0]
    assert "discard failed" in notes[1]
    assert "release failed" in notes[2]


def test_vacuum_unlock_failure_is_primary_over_ordinary_body_failure() -> None:
    body_failure = RuntimeError("body failed")
    unlock_failure = RuntimeError("unlock failed")
    discard_failure = RuntimeError("discard failed")
    runner = FailureOrderingVacuumRunner(
        body_failure=body_failure,
        unlock_failure=unlock_failure,
        discard_failure=discard_failure,
    )

    with pytest.raises(RuntimeError, match="unlock failed") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert caught.value is unlock_failure
    assert caught.value.__context__ is body_failure
    assert any("discard failed" in note for note in caught.value.__notes__)


def test_vacuum_discard_base_exception_is_primary() -> None:
    class DiscardAbort(BaseException):
        pass

    body_failure = RuntimeError("body failed")
    unlock_failure = RuntimeError("unlock failed")
    discard_failure = DiscardAbort("discard aborted")
    runner = FailureOrderingVacuumRunner(
        body_failure=body_failure,
        unlock_failure=unlock_failure,
        discard_failure=discard_failure,
    )

    with pytest.raises(DiscardAbort, match="discard aborted") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert caught.value is discard_failure
    assert caught.value.__context__ is unlock_failure
    assert unlock_failure.__context__ is body_failure


def test_vacuum_unlock_base_exception_survives_ordinary_later_cleanup() -> None:
    class UnlockAbort(BaseException):
        pass

    body_failure = RuntimeError("body failed")
    unlock_failure = UnlockAbort("unlock aborted")
    runner = FailureOrderingVacuumRunner(
        body_failure=body_failure,
        unlock_failure=unlock_failure,
        discard_failure=RuntimeError("discard failed"),
        release_failure=RuntimeError("release failed"),
    )

    with pytest.raises(UnlockAbort, match="unlock aborted") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert caught.value is unlock_failure
    assert caught.value.__context__ is body_failure
    notes = cast(tuple[str, ...], getattr(caught.value, "__notes__", ()))
    assert "discard failed" in notes[0]
    assert "release failed" in notes[1]


def test_vacuum_release_base_exception_is_primary() -> None:
    class ReleaseAbort(BaseException):
        pass

    body_failure = RuntimeError("body failed")
    release_failure = ReleaseAbort("release aborted")
    runner = FailureOrderingVacuumRunner(
        body_failure=body_failure,
        release_failure=release_failure,
    )

    with pytest.raises(ReleaseAbort, match="release aborted") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert caught.value is release_failure
    assert caught.value.__context__ is body_failure


def test_vacuum_ordinary_release_failure_preserves_combined_context_chain() -> None:
    body_failure = RuntimeError("body failed")
    unlock_failure = RuntimeError("unlock failed")
    release_failure = RuntimeError("release failed")
    runner = FailureOrderingVacuumRunner(
        body_failure=body_failure,
        unlock_failure=unlock_failure,
        release_failure=release_failure,
    )

    with pytest.raises(RuntimeError, match="release failed") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert caught.value is release_failure
    assert caught.value.__context__ is unlock_failure
    assert unlock_failure.__context__ is body_failure


@pytest.mark.parametrize("unlock_result", [True, False])
def test_vacuum_body_base_exception_survives_definite_unlock_result(
    unlock_result: bool,
) -> None:
    class BodyAbort(BaseException):
        pass

    body_failure = BodyAbort("body aborted")
    runner = FailureOrderingVacuumRunner(
        body_failure=body_failure,
        forced_unlock_result=unlock_result,
    )

    with pytest.raises(BodyAbort, match="body aborted") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert caught.value is body_failure
    assert getattr(caught.value, "__notes__", ()) == ()


def test_vacuum_body_base_exception_survives_ordinary_rollback_failure() -> None:
    class BodyAbort(BaseException):
        pass

    class RollbackFailureRunner(FailureOrderingVacuumRunner):
        def rollback(self) -> None:
            super().rollback()
            raise RuntimeError("rollback failed")

    body_failure = BodyAbort("body aborted")
    runner = RollbackFailureRunner(body_failure=body_failure)

    with pytest.raises(BodyAbort, match="body aborted") as caught:
        PostgresBackendPlugin().vacuum(
            runner,
            compact=False,
            config={"BROKER_VACUUM_BATCH_SIZE": 1000},
        )

    assert caught.value is body_failure
    assert any("rollback failed" in note for note in caught.value.__notes__)
    assert runner.unlock_session == runner.lock_session
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
            "SELECT queue, body FROM messages ORDER BY ts",
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
