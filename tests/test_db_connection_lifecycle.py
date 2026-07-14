"""DBConnection lifecycle and fallback contracts."""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from simplebroker._exceptions import OperationalError, StopException
from simplebroker.db import DBConnection

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

    with caplog.at_level("DEBUG", logger="simplebroker.db"):
        with pytest.raises(RuntimeError, match="Failed to get database connection"):
            connection.get_connection()

    assert "Database connection error (retry 1/3)" in caplog.text
    assert "Failed to get database connection after 3 retries" in caplog.text


def test_get_core_lazily_creates_and_reuses_sqlite_core(tmp_path: Path) -> None:
    with DBConnection(str(tmp_path / "broker.db")) as connection:
        core = connection.get_core()

        assert connection.get_core() is core
