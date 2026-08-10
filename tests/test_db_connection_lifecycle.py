"""DBConnection lifecycle and fallback contracts."""

from __future__ import annotations

import gc
import sqlite3
import threading
import weakref
from pathlib import Path
from types import SimpleNamespace

import pytest

from simplebroker import Queue
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


def test_get_connection_caches_and_cleans_up_managed_resource(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = DBConnection(str(tmp_path / "broker.db"))
    resource = ShutdownResource()
    monkeypatch.setattr(connection, "_create_managed_connection", lambda: resource)

    assert connection.get_connection() is resource
    assert connection.get_connection() is resource
    connection.cleanup()

    assert resource.shutdown_calls == 1
    assert not hasattr(connection._thread_local, "db")


def test_get_connection_rejects_pre_set_stop_event(tmp_path: Path) -> None:
    connection = DBConnection(str(tmp_path / "broker.db"))
    stop_event = threading.Event()
    stop_event.set()
    connection.set_stop_event(stop_event)

    with pytest.raises(StopException, match="Connection interrupted"):
        connection.get_connection()


def test_cleanup_handles_registered_and_thread_only_resources(tmp_path: Path) -> None:
    connection = DBConnection(str(tmp_path / "broker.db"))
    registered = CloseResource()
    thread_only = ShutdownResource()
    connection._connection_registry.add(registered)
    connection._thread_local.db = thread_only

    connection.cleanup()

    assert registered.close_calls == 1
    assert thread_only.shutdown_calls == 1


def test_cleanup_does_not_close_an_owned_core_twice(tmp_path: Path) -> None:
    connection = DBConnection(str(tmp_path / "broker.db"))
    core = ShutdownResource()
    connection._core = core  # type: ignore[assignment]
    connection._connection_registry.add(core)

    connection.cleanup()

    assert core.shutdown_calls == 1


def test_cleanup_logs_registered_core_and_runner_failures(
    tmp_path: Path, caplog
) -> None:
    connection = DBConnection(
        str(tmp_path / "broker.db"), config={"BROKER_LOGGING_ENABLED": True}
    )
    registered = ShutdownResource(fail=True)
    owned_core = ShutdownResource(fail=True)
    connection._connection_registry.add(registered)
    connection._core = owned_core  # type: ignore[assignment]

    runner_connection = DBConnection(
        str(tmp_path / "runner.db"), config={"BROKER_LOGGING_ENABLED": True}
    )
    owned_runner = CloseResource(fail=True)
    runner_connection._runner = owned_runner  # type: ignore[assignment]

    with caplog.at_level("WARNING", logger="simplebroker.db"):
        connection.cleanup()
        runner_connection.cleanup()

    assert registered.shutdown_calls == 1
    assert owned_core.shutdown_calls == 1
    assert owned_runner.close_calls == 1
    assert "Error closing registered connection: shutdown failed" in caplog.text
    assert "Error closing owned core: shutdown failed" in caplog.text
    assert "Error closing runner: close failed" in caplog.text


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
        str(tmp_path / "broker.db"), config={"BROKER_LOGGING_ENABLED": True}
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
        config={"BROKER_LOGGING_ENABLED": True},
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
