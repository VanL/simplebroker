"""DBConnection lifecycle and fallback contracts."""

from __future__ import annotations

import gc
import sqlite3
import threading
import weakref
from pathlib import Path
from types import SimpleNamespace

import pytest

from simplebroker import Queue, resolve_config
from simplebroker._constants import Config
from simplebroker._exceptions import OperationalError, StopException
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore, DBConnection

pytestmark = [pytest.mark.sqlite_only]


class ShutdownResource:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.shutdown_calls = 0

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        if self.fail:
            raise RuntimeError("shutdown failed")


class CloseResource:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self.fail:
            raise RuntimeError("close failed")


def test_get_connection_rejects_pre_set_stop_event(tmp_path: Path) -> None:
    connection = DBConnection(str(tmp_path / "broker.db"))
    stop_event = threading.Event()
    stop_event.set()
    connection.set_stop_event(stop_event)

    with pytest.raises(StopException, match="Connection interrupted"):
        connection.get_connection()


def test_set_stop_event_tolerates_legacy_cached_connection(tmp_path: Path) -> None:
    connection = DBConnection(str(tmp_path / "broker.db"))
    legacy = CloseResource()
    connection._thread_local.db = legacy

    stop_event = threading.Event()
    connection.set_stop_event(stop_event)
    connection.cleanup()

    assert connection._stop_event is stop_event
    assert legacy.close_calls == 1


def test_connection_failure_logs_retry_and_terminal_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog,
) -> None:
    connection = DBConnection(
        str(tmp_path / "broker.db"),
        config=resolve_config(override={"BROKER_LOGGING_ENABLED": True}),
    )
    failure = OperationalError("database is locked")

    def fail_retry(operation, *, before_sleep, **kwargs):
        del operation, kwargs
        before_sleep(SimpleNamespace(tries=1), failure, 0.01)
        raise failure

    monkeypatch.setattr("simplebroker.db._execute_connection_retry", fail_retry)

    with (
        caplog.at_level("DEBUG", logger="simplebroker.db"),
        pytest.raises(RuntimeError, match="Failed to get database connection"),
    ):
        connection.get_connection()

    assert "Database connection error (retry 1/3)" in caplog.text
    assert "Failed to get database connection after 3 retries" in caplog.text


def test_get_core_lazily_creates_and_reuses_sqlite_core(tmp_path: Path) -> None:
    with DBConnection(str(tmp_path / "broker.db")) as connection:
        core = connection.get_core()

        assert connection.get_core() is core
        core.write("jobs", "usable")
        assert list(core.peek_generator("jobs", with_timestamps=False)) == ["usable"]


def test_queue_default_is_ephemeral_and_operation_scoped(tmp_path: Path) -> None:
    """The public default yields distinct usable operation leases."""
    queue = Queue("jobs", db_path=str(tmp_path / "broker.db"))
    try:
        with queue.get_connection() as first:
            assert isinstance(first, BrokerCore)
            first.write("jobs", "one")
            assert isinstance(first._runner, SQLiteRunner)
            first_sqlite = first._runner.get_connection()
            assert first_sqlite.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            first_sqlite.execute("SELECT 1")

        with queue.get_connection() as second:
            assert isinstance(second, BrokerCore)
            second.write("jobs", "two")
            assert isinstance(second._runner, SQLiteRunner)
            second_sqlite = second._runner.get_connection()
            assert second_sqlite.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            second_sqlite.execute("SELECT 1")

        with queue.get_connection() as third:
            assert isinstance(third, BrokerCore)
            assert list(third.peek_generator("jobs", with_timestamps=False)) == [
                "one",
                "two",
            ]
            assert isinstance(third._runner, SQLiteRunner)
            third_sqlite = third._runner.get_connection()
            assert third_sqlite.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            third_sqlite.execute("SELECT 1")

        assert first is not second
        assert second is not third
        assert first is not third
    finally:
        queue.close()


def test_queue_gc_finalizer_closes_owned_connection_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real collection closes the queue-owned connection exactly once."""
    close = DBConnection.close
    effective_close_calls: list[int] = []

    def recording_close(connection: DBConnection) -> None:
        if not connection._shared_released:
            effective_close_calls.append(id(connection))
        close(connection)

    monkeypatch.setattr(DBConnection, "close", recording_close)
    queue = Queue("jobs", db_path=str(tmp_path / "broker.db"), persistent=True)
    assert queue.peek_one(with_timestamps=False) is None
    assert queue.conn is not None
    owned_connection_id = id(queue.conn)
    queue_ref = weakref.ref(queue)

    del queue
    gc.collect()

    assert queue_ref() is None
    assert effective_close_calls.count(owned_connection_id) == 1


def test_queue_gc_finalizer_logs_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Cleanup failures are logged by the actual GC path and never escape it."""
    close = DBConnection.close
    failing_connection_id: int | None = None

    def failing_close(connection: DBConnection) -> None:
        if id(connection) == failing_connection_id:
            raise RuntimeError("semantic cleanup failure")
        close(connection)

    monkeypatch.setattr(DBConnection, "close", failing_close)
    queue = Queue(
        "jobs",
        db_path=str(tmp_path / "broker.db"),
        persistent=True,
        config=resolve_config(override={"BROKER_LOGGING_ENABLED": True}),
    )
    assert queue.peek_one(with_timestamps=False) is None
    assert queue.conn is not None
    failing_connection_id = id(queue.conn)
    queue_ref = weakref.ref(queue)

    with caplog.at_level("WARNING", logger="simplebroker.sbqueue"):
        del queue
        gc.collect()

    assert queue_ref() is None
    assert "Error during Queue finalizer cleanup" in caplog.text
    assert "semantic cleanup failure" in caplog.text


@pytest.mark.parametrize(
    "scenario", ["terminal-first", "mixed-attempt-limit", "mixed-deadline"]
)
def test_capacity_failure_diagnostics_follow_invocation_not_last_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    scenario: str,
) -> None:
    """Real managed retries keep extended logs accurate even with mixed errors."""
    config = resolve_config(override={"BROKER_LOGGING_ENABLED": True})
    connection = DBConnection(str(tmp_path / "broker.db"), config=config)
    monkeypatch.setattr(connection, "_backend_plugin", SimpleNamespace(name="postgres"))
    now = 0.0
    attempts = 0
    failure = OperationalError("capacity exhausted")
    failure._connection_capacity = True
    ordinary = OperationalError("authentication failed")

    def sleep(wait: float, stop_event=None) -> bool:
        nonlocal now
        now += wait
        return True

    def open_connection():
        nonlocal attempts, now
        attempts += 1
        if scenario == "terminal-first":
            now = 31
            raise failure
        if attempts == 1:
            if scenario == "mixed-deadline":
                now = 27
            raise failure
        if scenario == "mixed-attempt-limit" and attempts == 2:
            raise failure
        raise ordinary

    monkeypatch.setattr("simplebroker._retry._monotonic", lambda: now)
    monkeypatch.setattr("simplebroker._retry._uniform", lambda floor, upper: upper)
    monkeypatch.setattr("simplebroker._retry_policy.interruptible_sleep", sleep)
    with (
        connection,
        caplog.at_level("DEBUG", logger="simplebroker.db"),
        pytest.raises(
            RuntimeError, match="Failed to get database connection"
        ) as caught,
    ):
        connection._open_connection_with_retry(open_connection, config=config)

    assert (
        attempts
        == {"terminal-first": 1, "mixed-attempt-limit": 3, "mixed-deadline": 2}[
            scenario
        ]
    )
    assert caught.value.__cause__ is (
        failure if scenario == "terminal-first" else ordinary
    )
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "simplebroker.db"
    ]
    assert any(
        message.startswith("Failed to get database connection") for message in messages
    )
    assert all(
        "/3" not in message and "after 3 retries" not in message for message in messages
    )
    assert all(
        "deadline" not in message.lower() and "attempt limit" not in message.lower()
        for message in messages
    )
    if scenario != "terminal-first":
        assert any("Retrying in" in message for message in messages)


@pytest.mark.parametrize(
    ("plugin_name", "budget", "expected_attempts"),
    [
        ("postgres", 30, 5),
        ("postgres", 0, 3),
        ("sqlite", 30, 3),
        ("redis", 30, 3),
        ("third-party", 30, 3),
        ("postgres", None, 5),
    ],
)
def test_managed_capacity_wait_is_enabled_only_for_resolved_postgres_plugin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    plugin_name: str,
    budget: int | None,
    expected_attempts: int,
) -> None:
    """Backend identity gates the real policy; custom Config omission uses 30."""
    config = resolve_config()
    values = dict(config)
    if budget is None:
        values.pop("POSTGRES_CAPACITY_WAIT_SECONDS", None)
        config = Config(
            values,
            defaults={
                key: field
                for key, field in config._defaults.items()
                if key != "POSTGRES_CAPACITY_WAIT_SECONDS"
            },
        )
    else:
        values["POSTGRES_CAPACITY_WAIT_SECONDS"] = budget
        config = Config(values)
    connection = DBConnection(str(tmp_path / "broker.db"), config=config)
    monkeypatch.setattr(
        connection, "_backend_plugin", SimpleNamespace(name=plugin_name)
    )
    now = 0.0
    attempts = 0
    failure = OperationalError("capacity exhausted")
    failure._connection_capacity = True
    resource = CloseResource()

    def sleep(wait: float, stop_event=None) -> bool:
        nonlocal now
        now += wait
        return True

    def open_connection():
        nonlocal attempts
        attempts += 1
        if attempts < 5:
            raise failure
        return resource

    monkeypatch.setattr("simplebroker._retry._monotonic", lambda: now)
    monkeypatch.setattr("simplebroker._retry._uniform", lambda floor, upper: upper)
    monkeypatch.setattr("simplebroker._retry_policy.interruptible_sleep", sleep)
    with connection:
        if expected_attempts == 5:
            assert (
                connection._open_connection_with_retry(open_connection, config=config)
                is resource
            )
        else:
            with pytest.raises(RuntimeError) as caught:
                connection._open_connection_with_retry(open_connection, config=config)
            assert caught.value.__cause__ is failure
    assert attempts == expected_attempts
    assert now < 30
