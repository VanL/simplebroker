"""Executable transition contracts for Postgres extension state machines."""

from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Self

import psycopg
import pytest
import simplebroker_pg.runner as pg_runner_module
from simplebroker_pg._identifiers import stable_lock_key
from simplebroker_pg.plugin import PostgresBackendPlugin
from simplebroker_pg.runner import PostgresRunner, _SharedActivityListener

from simplebroker.db import BrokerCore
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)

pytestmark = [pytest.mark.pg_only]

_CLOSED = object()


class _Cursor:
    def __init__(self, connection: _ScriptedPGConnection) -> None:
        self._connection = connection

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: str) -> None:
        self._connection.statements.append(statement)


class _ScriptedPGConnection:
    """Thread-safe fake for only the nondeterministic psycopg transport."""

    def __init__(self) -> None:
        self.events: queue.Queue[object] = queue.Queue()
        self.statements: list[str] = []
        self.closed = False

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def notifies(self, *, timeout: float, stop_after: int) -> Iterator[object]:
        del stop_after
        try:
            event = self.events.get(timeout=timeout)
        except queue.Empty:
            return
        if event is _CLOSED:
            return
        if isinstance(event, BaseException):
            raise event
        yield event

    def notify(self, payload: str) -> None:
        self.events.put(SimpleNamespace(payload=payload))

    def fail(self, error: BaseException) -> None:
        self.events.put(error)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.events.put(_CLOSED)


@contextmanager
def _started_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[_SharedActivityListener, _ScriptedPGConnection]]:
    connection = _ScriptedPGConnection()
    monkeypatch.setattr(
        pg_runner_module.psycopg,
        "connect",
        lambda *args, **kwargs: connection,
    )
    listener = _SharedActivityListener(
        "postgresql://transport.invalid/db",
        schema="transition_contract",
        startup_timeout=1.0,
    )
    try:
        yield listener, connection
    finally:
        listener.close()


def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("listener transition did not become observable")
        time.sleep(0.005)


def _listener_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        assert listener._ready.is_set()
        assert listener._error is None
        assert len(connection.statements) == 1
        assert connection.statements[0].startswith('LISTEN "simplebroker_')


def _listener_start_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pg_runner_module.psycopg,
        "connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("connect failed")),
    )
    with pytest.raises(
        pg_runner_module.OperationalError,
        match="connect failed",
    ):
        _SharedActivityListener(
            "postgresql://transport.invalid/db",
            schema="transition_contract",
            startup_timeout=1.0,
        )


def _listener_start_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _ScriptedPGConnection()

    def slow_connect(*args: object, **kwargs: object) -> _ScriptedPGConnection:
        del args, kwargs
        time.sleep(0.05)
        return connection

    monkeypatch.setattr(pg_runner_module.psycopg, "connect", slow_connect)
    with pytest.raises(
        pg_runner_module.OperationalError,
        match="did not start",
    ):
        _SharedActivityListener(
            "postgresql://transport.invalid/db",
            schema="transition_contract",
            startup_timeout=0.01,
        )
    assert connection.closed


def _listener_registers_and_refcounts(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _connection):
        assert listener.register_queue("jobs") == (0, 0)
        assert listener.register_queue("jobs") == (0, 0)
        assert listener._queue_refcounts == {"jobs": 2}
        assert set(listener._conditions) == {"jobs"}


def _listener_routes_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        queue_version, wildcard_version = listener.register_queue("jobs")
        connection.notify("jobs")
        assert listener.wait(
            queue_name="jobs",
            stop_event=threading.Event(),
            timeout=1.0,
            last_queue_version=queue_version,
            last_wildcard_version=wildcard_version,
        ) == (True, 1, 0)


def _listener_routes_wildcard(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        queue_version, wildcard_version = listener.register_queue("jobs")
        connection.notify("*")
        assert listener.wait(
            queue_name="jobs",
            stop_event=threading.Event(),
            timeout=1.0,
            last_queue_version=queue_version,
            last_wildcard_version=wildcard_version,
        ) == (True, 0, 1)


def _listener_ignores_unknown_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        queue_version, wildcard_version = listener.register_queue("jobs")
        connection.notify("unknown")
        assert listener.wait(
            queue_name="jobs",
            stop_event=threading.Event(),
            timeout=0.05,
            last_queue_version=queue_version,
            last_wildcard_version=wildcard_version,
        ) == (False, 0, 0)


def _listener_publishes_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        listener.register_queue("jobs")
        connection.fail(RuntimeError("notification read failed"))
        _wait_until(lambda: listener._error is not None)
        with pytest.raises(RuntimeError, match="notification read failed"):
            listener.wait(
                queue_name="jobs",
                stop_event=threading.Event(),
                timeout=0.1,
                last_queue_version=0,
                last_wildcard_version=0,
            )


def _listener_fan_in_routes_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        fan_in_id, versions, wildcard_version = listener.register_queue_set(
            ("alpha", "beta")
        )
        assert listener._queue_refcounts == {"alpha": 1, "beta": 1}
        connection.notify("beta")
        assert listener.wait_any(
            fan_in_id=fan_in_id,
            stop_event=threading.Event(),
            timeout=1.0,
            last_queue_versions=versions,
            last_wildcard_version=wildcard_version,
        ) == (True, {"alpha": 0, "beta": 1}, 0)


def _listener_fan_in_routes_wildcard(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        fan_in_id, versions, wildcard_version = listener.register_queue_set(
            ("alpha", "beta")
        )
        connection.notify("*")
        assert listener.wait_any(
            fan_in_id=fan_in_id,
            stop_event=threading.Event(),
            timeout=1.0,
            last_queue_versions=versions,
            last_wildcard_version=wildcard_version,
        ) == (True, {"alpha": 0, "beta": 0}, 1)


def _listener_fan_in_publishes_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        fan_in_id, versions, wildcard_version = listener.register_queue_set(
            ("alpha", "beta")
        )
        connection.fail(RuntimeError("fan-in notification read failed"))
        _wait_until(lambda: listener._error is not None)
        with pytest.raises(RuntimeError, match="fan-in notification read failed"):
            listener.wait_any(
                fan_in_id=fan_in_id,
                stop_event=threading.Event(),
                timeout=0.1,
                last_queue_versions=versions,
                last_wildcard_version=wildcard_version,
            )


def _listener_unregisters_fan_in(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _connection):
        listener.register_queue("alpha")
        fan_in_id, _versions, _wildcard_version = listener.register_queue_set(
            ("alpha", "beta")
        )
        listener.unregister_queue_set(fan_in_id)
        listener.unregister_queue_set(fan_in_id)
        assert listener._fan_in_entries == {}
        assert listener._queue_refcounts == {"alpha": 1}
        assert set(listener._conditions) == {"alpha"}


def _listener_unregisters_one_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _connection):
        listener.register_queue("jobs")
        listener.register_queue("jobs")
        listener.unregister_queue("jobs")
        assert listener._queue_refcounts == {"jobs": 1}
        assert set(listener._conditions) == {"jobs"}


def _listener_unregisters_last_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _connection):
        listener.register_queue("jobs")
        listener.unregister_queue("jobs")
        listener.unregister_queue("jobs")
        assert listener._queue_refcounts == {}
        assert listener._conditions == {}
        assert listener._versions == {}


def _listener_close_stops_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        queue_version, wildcard_version = listener.register_queue("jobs")
        result: list[tuple[bool, int, int]] = []
        thread = threading.Thread(
            target=lambda: result.append(
                listener.wait(
                    queue_name="jobs",
                    stop_event=threading.Event(),
                    timeout=5.0,
                    last_queue_version=queue_version,
                    last_wildcard_version=wildcard_version,
                )
            )
        )
        thread.start()
        listener.close()
        thread.join(timeout=1.0)
        assert not thread.is_alive()
        assert result == [(False, 0, 0)]
        assert connection.closed
        assert not listener._thread.is_alive()


def _listener_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, connection):
        listener.close()
        listener.close()
        assert listener._stop_event.is_set()
        assert connection.closed
        assert not listener._thread.is_alive()


PG_LISTENER_TRANSITIONS = (
    TransitionCase(
        transition_id="START-READY",
        start_state="starting",
        event="LISTEN subscription succeeds",
        guard="external connection accepts the schema activity channel",
        next_state="ready",
        effects="records the connection and publishes readiness",
        expected_result="listener accepts registrations",
        payload=_listener_starts,
    ),
    TransitionCase(
        transition_id="START-FAILURE",
        start_state="starting",
        event="connection setup fails",
        guard="stop was not requested",
        next_state="failed",
        effects="stores the first error, publishes readiness, and closes",
        expected_result="OperationalError reaches the creator",
        payload=_listener_start_fails,
    ),
    TransitionCase(
        transition_id="START-TIMEOUT",
        start_state="starting",
        event="LISTEN setup does not publish readiness before the deadline",
        guard="the startup timeout expires while connect is still in progress",
        next_state="failed-and-closed",
        effects="requests stop, joins the worker, and closes the late connection",
        expected_result="OperationalError identifies startup timeout",
        payload=_listener_start_times_out,
    ),
    TransitionCase(
        transition_id="REGISTER-REFCOUNT",
        start_state="ready without queue registrations",
        event="the same queue is registered twice",
        guard="listener is ready",
        next_state="ready with two queue references",
        effects="creates one condition/version owner and increments its refcount",
        expected_result="both registrations observe the current versions",
        payload=_listener_registers_and_refcounts,
    ),
    TransitionCase(
        transition_id="NOTIFY-QUEUE",
        start_state="ready with jobs registered",
        event="jobs notification arrives",
        guard="payload names an active queue",
        next_state="ready with jobs version advanced",
        effects="increments the queue version and wakes matching waiters",
        expected_result="wait reports the queue transition",
        payload=_listener_routes_queue,
    ),
    TransitionCase(
        transition_id="NOTIFY-WILDCARD",
        start_state="ready with jobs registered",
        event="wildcard notification arrives",
        guard="payload is the wildcard marker",
        next_state="ready with wildcard version advanced",
        effects="increments the wildcard version and wakes every waiter",
        expected_result="wait reports the wildcard transition",
        payload=_listener_routes_wildcard,
    ),
    TransitionCase(
        transition_id="NOTIFY-UNKNOWN",
        start_state="ready with jobs registered",
        event="an unknown queue notification arrives",
        guard="payload is neither wildcard nor registered",
        next_state="ready with versions unchanged",
        effects="does not create state or wake the jobs waiter",
        expected_result="wait times out without activity",
        payload=_listener_ignores_unknown_queue,
    ),
    TransitionCase(
        transition_id="READ-FAILURE",
        start_state="ready with jobs registered",
        event="notification transport raises",
        guard="stop was not requested",
        next_state="failed",
        effects="stores the failure and wakes registered waiters",
        expected_result="the stored failure reaches the waiter",
        payload=_listener_publishes_failure,
    ),
    TransitionCase(
        transition_id="FAN-IN-NOTIFY-QUEUE",
        start_state="ready with alpha/beta fan-in registered",
        event="beta notification arrives",
        guard="beta belongs to the fan-in queue set",
        next_state="ready with beta version advanced",
        effects="wakes the shared fan-in condition and preserves alpha version",
        expected_result="wait_any reports beta activity",
        payload=_listener_fan_in_routes_queue,
    ),
    TransitionCase(
        transition_id="FAN-IN-NOTIFY-WILDCARD",
        start_state="ready with alpha/beta fan-in registered",
        event="wildcard notification arrives",
        guard="wildcard activity applies to every fan-in waiter",
        next_state="ready with wildcard version advanced",
        effects="wakes the fan-in condition without changing queue versions",
        expected_result="wait_any reports wildcard activity",
        payload=_listener_fan_in_routes_wildcard,
    ),
    TransitionCase(
        transition_id="FAN-IN-READ-FAILURE",
        start_state="ready with alpha/beta fan-in registered",
        event="notification transport raises",
        guard="the fan-in waiter is blocked",
        next_state="failed",
        effects="stores the failure and wakes the fan-in condition",
        expected_result="the stored failure reaches wait_any",
        payload=_listener_fan_in_publishes_failure,
    ),
    TransitionCase(
        transition_id="FAN-IN-UNREGISTER",
        start_state="ready with one single alpha and one alpha/beta fan-in reference",
        event="fan-in registration closes twice",
        guard="the single alpha registration remains",
        next_state="ready with only alpha registered",
        effects="removes the fan-in owner and decrements each queue exactly once",
        expected_result="duplicate fan-in close is a no-op",
        payload=_listener_unregisters_fan_in,
    ),
    TransitionCase(
        transition_id="UNREGISTER-DECREMENT",
        start_state="ready with two jobs references",
        event="one jobs registration closes",
        guard="another jobs registration remains",
        next_state="ready with one jobs reference",
        effects="decrements the refcount and preserves queue state",
        expected_result="remaining waiter stays registered",
        payload=_listener_unregisters_one_reference,
    ),
    TransitionCase(
        transition_id="UNREGISTER-LAST",
        start_state="ready with one jobs reference",
        event="last jobs registration closes",
        guard="no other jobs registration remains",
        next_state="ready without jobs state",
        effects="removes condition, version, and refcount; duplicate close is a no-op",
        expected_result="no jobs registration remains",
        payload=_listener_unregisters_last_reference,
    ),
    TransitionCase(
        transition_id="STOP-WAIT",
        start_state="ready with a blocked waiter",
        event="listener closes",
        guard="wait is blocked without activity",
        next_state="stopped",
        effects="sets stop, closes the transport, wakes waiters, and joins the worker",
        expected_result="blocked wait returns false and the thread exits",
        payload=_listener_close_stops_wait,
    ),
    TransitionCase(
        transition_id="CLOSE-IDEMPOTENT",
        start_state="stopped",
        event="close repeats",
        guard="transport and worker are already closed",
        next_state="stopped",
        effects="preserves stopped state without restarting or failing",
        expected_result="repeated close succeeds",
        payload=_listener_close_is_idempotent,
    ),
)


@fires_transition_table("SM-PG-LISTENER", PG_LISTENER_TRANSITIONS)
def test_pg_listener_fires_transition_table(
    transition_case: TransitionCase[Callable[[pytest.MonkeyPatch], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition_case.payload(monkeypatch)


@dataclass(frozen=True, slots=True)
class _VacuumScenario:
    claimed_messages: int = 0
    hold_lock: bool = False
    compact: bool = False
    fail_at: str | tuple[str, ...] | None = None
    refuse_unlock: bool = False
    expected_events: tuple[str, ...] = ()
    expected_claimed: int = 0
    expected_error: str | None = None
    expected_contexts: tuple[str, ...] = ()
    expected_warning: bool = False


def _vacuum_fails_at(scenario: _VacuumScenario, point: str) -> bool:
    fail_at = scenario.fail_at
    return fail_at == point or isinstance(fail_at, tuple) and point in fail_at


def _vacuum_sql_point(sql: str) -> str | None:
    normalized = " ".join(sql.split())
    if "pg_try_advisory_lock" in normalized:
        return "lock"
    if "SELECT COUNT(*) FROM deleted" in normalized:
        return "delete"
    if "pg_advisory_unlock" in normalized:
        return "unlock"
    if normalized.startswith("VACUUM"):
        return "compact"
    if normalized.startswith("ANALYZE"):
        return "analyze"
    return None


class _RealVacuumHarness:
    """Observe the real runner and inject only explicit external failures."""

    def __init__(
        self,
        scenario: _VacuumScenario,
        pg_runner: PostgresRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self.scenario = scenario
        self.runner = pg_runner
        self.monkeypatch = monkeypatch
        self.events: list[str] = []
        self._run = pg_runner.run
        self._lease = pg_runner.lease_thread_connection
        self._release = pg_runner.release_thread_connection
        self._begin = pg_runner.begin_immediate
        self._commit = pg_runner.commit
        self._rollback = pg_runner.rollback

    def install(self) -> None:
        self.monkeypatch.setattr(self.runner, "run", self.run)
        self.monkeypatch.setattr(
            self.runner,
            "lease_thread_connection",
            self.lease,
        )
        self.monkeypatch.setattr(
            self.runner,
            "release_thread_connection",
            self.release,
        )
        self.monkeypatch.setattr(self.runner, "begin_immediate", self.begin)
        self.monkeypatch.setattr(self.runner, "commit", self.commit)
        self.monkeypatch.setattr(self.runner, "rollback", self.rollback)

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        point = _vacuum_sql_point(sql)
        if point is not None:
            self.events.append(point)
        if point == "unlock":
            rows = list(self._run(sql, params, fetch=fetch))
            self._raise_at(point)
            return [(False,)] if self.scenario.refuse_unlock else rows
        self._raise_at(point)
        return self._run(sql, params, fetch=fetch)

    def lease(self) -> None:
        self.events.append("lease")
        self._lease()

    def release(self) -> None:
        self.events.append("release")
        self._release()
        self._raise_at("release")

    def begin(self) -> None:
        self.events.append("begin")
        self._begin()

    def commit(self) -> None:
        self.events.append("commit")
        self._raise_at("commit")
        self._commit()

    def rollback(self) -> None:
        self.events.append("rollback")
        self._rollback()
        self._raise_at("rollback")

    def _raise_at(self, point: str | None) -> None:
        if point is not None and _vacuum_fails_at(self.scenario, point):
            raise RuntimeError(f"{point} failed")

    def execute(self) -> BaseException | None:
        lock_connection: psycopg.Connection[Any] | None = None
        if self.scenario.hold_lock:
            lock_connection = psycopg.connect(self.runner.dsn, autocommit=True)
            with lock_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_lock(%s)",
                    (stable_lock_key("vacuum", self.runner.schema),),
                )

        caught: Exception | None = None
        try:
            PostgresBackendPlugin().vacuum(
                self.runner,
                compact=self.scenario.compact,
                config={"BROKER_VACUUM_BATCH_SIZE": 100},
            )
        except RuntimeError as exc:
            caught = exc
        finally:
            if lock_connection is not None:
                lock_connection.close()
        return caught


def _run_real_vacuum_transition(
    scenario: _VacuumScenario,
    *,
    pg_runner: PostgresRunner,
    pg_core: BrokerCore,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[str], BaseException | None]:
    for index in range(scenario.claimed_messages):
        pg_core.write("jobs", f"claimed-{index}")
        assert pg_core.claim_one("jobs", with_timestamps=False) == f"claimed-{index}"

    harness = _RealVacuumHarness(scenario, pg_runner, monkeypatch)
    harness.install()
    caught = harness.execute()

    assert pg_core.count_claimed_messages() == scenario.expected_claimed
    return harness.events, caught


PG_VACUUM_TRANSITIONS = (
    TransitionCase(
        transition_id="LOCK-REFUSED",
        start_state="idle",
        event="maintenance requested",
        guard="session advisory lock is held elsewhere",
        next_state="idle",
        effects="releases the leased connection without deleting or maintaining",
        expected_result="no-op success",
        payload=_VacuumScenario(
            claimed_messages=1,
            hold_lock=True,
            expected_events=("lease", "lock", "release"),
            expected_claimed=1,
        ),
    ),
    TransitionCase(
        transition_id="EMPTY-ROLLBACK",
        start_state="lock-held",
        event="delete batch returns zero",
        guard="no claimed messages exist and compact is false",
        next_state="unlocking",
        effects="rolls back the empty batch and skips ANALYZE",
        expected_result="unlock and lease release succeed",
        payload=_VacuumScenario(
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="DELETE-COMMIT-ANALYZE",
        start_state="lock-held",
        event="one nonempty batch then an empty batch",
        guard="compact is false",
        next_state="unlocking",
        effects="commits deletion, rolls back empty batch, and analyzes three tables",
        expected_result="unlock and lease release succeed",
        payload=_VacuumScenario(
            claimed_messages=2,
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "commit",
                "begin",
                "delete",
                "rollback",
                "analyze",
                "analyze",
                "analyze",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="COMPACT",
        start_state="lock-held",
        event="empty delete scan completes",
        guard="compact is true",
        next_state="unlocking",
        effects="rolls back the empty batch and compacts three tables",
        expected_result="unlock and lease release succeed",
        payload=_VacuumScenario(
            compact=True,
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "compact",
                "compact",
                "compact",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="DELETE-FAILURE",
        start_state="delete-batch-open",
        event="delete query fails",
        guard="transaction is active",
        next_state="failed",
        effects="rolls back, unlocks, and releases the lease",
        expected_result="delete failure propagates",
        payload=_VacuumScenario(
            claimed_messages=1,
            fail_at="delete",
            expected_error="delete failed",
            expected_claimed=1,
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="COMMIT-FAILURE",
        start_state="delete-batch-open",
        event="delete commit fails",
        guard="a nonempty delete batch was executed",
        next_state="failed",
        effects="attempts rollback, unlocks, and releases the lease",
        expected_result="commit failure propagates",
        payload=_VacuumScenario(
            claimed_messages=1,
            fail_at="commit",
            expected_error="commit failed",
            expected_claimed=1,
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "commit",
                "rollback",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="ROLLBACK-FAILURE",
        start_state="delete-batch-open",
        event="empty-batch rollback fails",
        guard="advisory lock is still held",
        next_state="failed",
        effects="still unlocks and releases the connection lease",
        expected_result="rollback failure propagates",
        payload=_VacuumScenario(
            fail_at="rollback",
            expected_error="rollback failed",
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "rollback",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="MAINTENANCE-FAILURE",
        start_state="delete-scan-complete",
        event="first compact statement fails",
        guard="compact is true",
        next_state="failed",
        effects="unlocks and releases the connection lease",
        expected_result="maintenance failure propagates",
        payload=_VacuumScenario(
            compact=True,
            fail_at="compact",
            expected_error="compact failed",
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "compact",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="ANALYZE-FAILURE",
        start_state="delete-scan-complete",
        event="first ANALYZE statement fails",
        guard="claimed messages were deleted and compact is false",
        next_state="failed",
        effects="unlocks and releases the connection lease",
        expected_result="analyze failure propagates",
        payload=_VacuumScenario(
            claimed_messages=1,
            fail_at="analyze",
            expected_error="analyze failed",
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "commit",
                "begin",
                "delete",
                "rollback",
                "analyze",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="COMBINED-FAILURE-PRECEDENCE",
        start_state="delete-batch-open",
        event="delete, rollback, unlock, and lease release all fail",
        guard="each later cleanup runs while an earlier failure is active",
        next_state="failed-after-cleanup",
        effects="attempts every cleanup in nesting order",
        expected_result="outer lease-release failure wins with the full context chain",
        payload=_VacuumScenario(
            claimed_messages=1,
            fail_at=("delete", "rollback", "unlock", "release"),
            expected_error="release failed",
            expected_claimed=1,
            expected_contexts=(
                "unlock failed",
                "rollback failed",
                "delete failed",
            ),
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="UNLOCK-REFUSED",
        start_state="unlocking",
        event="server reports advisory unlock false",
        guard="maintenance body completed",
        next_state="idle-with-unlock-warning",
        effects="warns that the session lock may remain and releases the lease",
        expected_result="warning does not replace successful maintenance",
        payload=_VacuumScenario(
            refuse_unlock=True,
            expected_warning=True,
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "unlock",
                "release",
            ),
        ),
    ),
    TransitionCase(
        transition_id="UNLOCK-FAILURE",
        start_state="unlocking",
        event="advisory unlock query fails",
        guard="maintenance body completed",
        next_state="failed",
        effects="releases the connection lease in the outer finalizer",
        expected_result="unlock failure propagates",
        payload=_VacuumScenario(
            fail_at="unlock",
            expected_error="unlock failed",
            expected_events=(
                "lease",
                "lock",
                "begin",
                "delete",
                "rollback",
                "unlock",
                "release",
            ),
        ),
    ),
)


@fires_transition_table("SM-PG-VACUUM", PG_VACUUM_TRANSITIONS)
def test_pg_vacuum_fires_transition_table(
    transition_case: TransitionCase[_VacuumScenario],
    pg_runner: PostgresRunner,
    pg_core: BrokerCore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = transition_case.payload
    expectation = (
        pytest.warns(RuntimeWarning, match="advisory lock release failed")
        if scenario.expected_warning
        else nullcontext()
    )
    with expectation:
        events, caught = _run_real_vacuum_transition(
            scenario,
            pg_runner=pg_runner,
            pg_core=pg_core,
            monkeypatch=monkeypatch,
        )

    if scenario.expected_error is not None:
        assert isinstance(caught, RuntimeError)
        assert scenario.expected_error in str(caught)
        context = caught.__context__
        for expected_context in scenario.expected_contexts:
            assert context is not None
            assert str(context) == expected_context
            context = context.__context__
    else:
        assert caught is None
    assert tuple(events) == scenario.expected_events
