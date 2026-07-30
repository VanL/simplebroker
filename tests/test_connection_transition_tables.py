"""Executable transition tables for connection and delivery ownership."""

from __future__ import annotations

import threading
import warnings
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker._broker_session import (
    _ProcessBrokerSession,
    close_process_broker_sessions,
)
from simplebroker._exceptions import OperationalError, StopException
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerDB, DBConnection
from tests.helpers.state_machine_contracts import TransitionCase, fires_transition_table

pytestmark = pytest.mark.sqlite_only


def _case(
    transition_id: str,
    start: str,
    event: str,
    next_state: str,
    effects: str,
    result: str,
) -> TransitionCase[str]:
    return TransitionCase(
        transition_id=transition_id,
        start_state=start,
        event=event,
        guard=f"resource starts in {start!r}; event {event!r} identifies its caller",
        next_state=next_state,
        effects=effects,
        expected_result=result,
        payload=transition_id,
    )


CONNECTION_TRANSITIONS = (
    _case(
        "LAZY_CREATE",
        "empty",
        "get connection",
        "open",
        "create and cache one managed core",
        "subsequent get reuses it",
    ),
    _case(
        "CLEANUP",
        "open",
        "cleanup",
        "empty",
        "shut down the owned core and clear thread state",
        "next get creates a distinct core",
    ),
    _case(
        "REPEATED_CLEANUP",
        "empty",
        "cleanup",
        "empty",
        "perform no duplicate close",
        "no error",
    ),
    _case(
        "STOP_REJECTS_CREATE",
        "empty and stopped",
        "get connection",
        "empty and stopped",
        "perform no allocation",
        "StopException",
    ),
    _case(
        "BORROWED_RUNNER_CLEANUP",
        "borrowed runner open",
        "manager cleanup",
        "manager detached and runner open",
        "close borrowed core without closing caller-owned runner",
        "runner remains usable",
    ),
    _case(
        "CREATE_FAILURE",
        "empty",
        "managed core creation fails",
        "empty",
        "publish no cached resource",
        "failure is wrapped with connection context",
    ),
    _case(
        "REGISTERED_CLOSE_FAILURE",
        "registered managed resource open",
        "cleanup and resource shutdown fails",
        "registry cleared",
        "log failure and continue cleanup",
        "shutdown called once without raising",
    ),
    _case(
        "OWNED_CLOSE_FAILURE",
        "owned core open",
        "cleanup and core shutdown fails",
        "owned core cleared",
        "log failure and continue cleanup",
        "shutdown called once without raising",
    ),
    _case(
        "RUNNER_CLOSE_FAILURE",
        "owned runner open",
        "cleanup and runner close fails",
        "owned runner cleared",
        "log failure and continue cleanup",
        "close called once without raising",
    ),
)


class _FailingShutdown:
    def __init__(self) -> None:
        self.calls = 0

    def shutdown(self) -> None:
        self.calls += 1
        raise RuntimeError("shutdown failed")


class _FailingClose:
    def __init__(self) -> None:
        self.calls = 0

    def close(self) -> None:
        self.calls += 1
        raise RuntimeError("close failed")


def _assert_cleanup_failure(
    payload: str,
    path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = DBConnection(path, config={"BROKER_LOGGING_ENABLED": True})
    if payload == "REGISTERED_CLOSE_FAILURE":
        resource: Any = _FailingShutdown()
        manager._connection_registry.add(resource)
        expected_log = "Error closing registered connection: shutdown failed"
    elif payload == "OWNED_CLOSE_FAILURE":
        resource = _FailingShutdown()
        manager._core = resource
        expected_log = "Error closing owned core: shutdown failed"
    else:
        resource = _FailingClose()
        manager._runner = resource
        expected_log = "Error closing runner: close failed"

    with caplog.at_level("WARNING", logger="simplebroker.db"):
        manager.cleanup()
    assert resource.calls == 1
    assert expected_log in caplog.text
    assert manager._core is None
    assert manager._runner is None
    assert list(manager._connection_registry) == []


@fires_transition_table("SM-CONNECTION", CONNECTION_TRANSITIONS)
def test_connection_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = str(tmp_path / f"{transition_case.payload}.db")
    if transition_case.payload.endswith("CLOSE_FAILURE"):
        _assert_cleanup_failure(transition_case.payload, path, caplog)
        return
    if transition_case.payload == "BORROWED_RUNNER_CLEANUP":
        runner = SQLiteRunner(path)
        manager = DBConnection(path, runner=runner)
        manager.get_connection()
        manager.cleanup()
        assert list(runner.run("SELECT 1", fetch=True)) == [(1,)]
        runner.close()
        return

    manager = DBConnection(path)
    if transition_case.payload == "STOP_REJECTS_CREATE":
        stopped = threading.Event()
        stopped.set()
        manager.set_stop_event(stopped)
        with pytest.raises(StopException):
            manager.get_connection()
        return
    if transition_case.payload == "CREATE_FAILURE":
        monkeypatch.setattr(
            manager,
            "_create_managed_connection",
            lambda: (_ for _ in ()).throw(OperationalError("open failed")),
        )
        with pytest.raises(RuntimeError, match="Failed to get database connection"):
            manager.get_connection()
        assert not hasattr(manager._thread_local, "db")
        return

    first = manager.get_connection()
    if transition_case.payload == "LAZY_CREATE":
        assert manager.get_connection() is first
    else:
        manager.cleanup()
        if transition_case.payload == "CLEANUP":
            assert manager.get_connection() is not first
        else:
            manager.cleanup()
    manager.close()


PROCESS_SESSION_TRANSITIONS = (
    _case(
        "ACQUIRE_REUSE",
        "absent",
        "two same-target leases",
        "shared",
        "publish one process session",
        "both managers see the same thread core",
    ),
    _case(
        "RETAIN_WHILE_REFERENCED",
        "shared with two leases",
        "release one lease",
        "shared with one lease",
        "retain the process session",
        "remaining manager stays usable",
    ),
    _case(
        "CLOSE_LAST",
        "shared with one lease",
        "release final lease",
        "closed",
        "close all owned cores and the factory",
        "session records terminal close",
    ),
    _case(
        "REPEATED_CLOSE",
        "closed",
        "close again",
        "closed",
        "perform no duplicate teardown",
        "no error",
    ),
    _case(
        "ACTIVE_OPERATION_CLOSE",
        "one active operation lease",
        "final session close begins",
        "closing until operation ends",
        "wait for the active lease before closing resources",
        "close completes after release",
    ),
    _case(
        "CORE_CREATION_FAILURE",
        "open with no thread core",
        "factory create fails",
        "open with no thread core",
        "release creation and operation counters without publishing a core",
        "original creation error propagates",
    ),
)


class _FailingSessionFactory:
    def create(self, stop_event: threading.Event | None) -> Any:
        del stop_event
        raise RuntimeError("core creation failed")

    def close_core(self, core: Any) -> None:
        raise AssertionError(f"unpublished core must not be closed: {core!r}")

    def close(self) -> None:
        return


@fires_transition_table("SM-PROCESS-SESSION", PROCESS_SESSION_TRANSITIONS)
def test_process_session_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
) -> None:
    close_process_broker_sessions()
    if transition_case.payload == "CORE_CREATION_FAILURE":
        failing_session = _ProcessBrokerSession(_FailingSessionFactory())
        with pytest.raises(RuntimeError, match="core creation failed"):
            failing_session.get_connection(None)
        assert failing_session._active_core_creations == 0
        assert failing_session._active_operations == 0
        assert failing_session._cores == set()
        failing_session.close_all()
        return

    path = str(tmp_path / f"{transition_case.payload}.db")
    first = DBConnection(path, share_in_process=True)
    second = DBConnection(path, share_in_process=True)
    session = first._shared_session
    assert session is not None
    assert session is second._shared_session
    first_core = first.get_core()
    assert second.get_core() is first_core

    if transition_case.payload == "ACQUIRE_REUSE":
        pass
    elif transition_case.payload == "RETAIN_WHILE_REFERENCED":
        first.close()
        assert second.get_core() is first_core
    elif transition_case.payload == "ACTIVE_OPERATION_CLOSE":
        # get_connection leases one active operation through the real manager.
        leased = first.get_connection()
        assert leased is first_core
        close_done = threading.Event()
        close_waiting = threading.Event()
        original_wait = (
            session._operation_condition.wait if session is not None else None
        )
        assert original_wait is not None

        def observe_close_wait(timeout: float | None = None) -> bool:
            close_waiting.set()
            return original_wait(timeout)

        session._operation_condition.wait = observe_close_wait  # type: ignore[method-assign]

        def close_manager() -> None:
            first.close()
            close_done.set()

        second.close()
        closer = threading.Thread(target=close_manager)
        closer.start()
        assert close_waiting.wait(1)
        assert not close_done.is_set()
        first.release_connection_after_use()
        closer.join(2)
        assert close_done.is_set()
        assert session is not None and session._closed
    else:
        first.close()
        second.close()
        assert session is not None and session._closed
        if transition_case.payload == "REPEATED_CLOSE":
            second.close()
    first.close()
    second.close()
    close_process_broker_sessions()
    first_core.shutdown()


DELIVERY_POISON_TRANSITIONS = (
    _case(
        "OWNER_CLOSE_ROLLBACK_OK",
        "owner transaction suspended",
        "owner closes generator",
        "rolled back and usable",
        "rollback the claim and release the lock",
        "messages remain pending",
    ),
    _case(
        "OWNER_CLOSE_ROLLBACK_ERROR",
        "owner transaction suspended",
        "owner closes generator and rollback raises Exception",
        "original close semantics preserved",
        "suppress ordinary rollback failure and release owner lock",
        "generator closes without replacing GeneratorExit",
    ),
    _case(
        "OWNER_CLOSE_ROLLBACK_BASE_ERROR",
        "owner transaction suspended",
        "owner closes and rollback raises BaseException",
        "cleanup failed",
        "replace GeneratorExit with the rollback BaseException",
        "cleanup failure propagates",
    ),
    _case(
        "OWNER_THROW_ROLLBACK_OK",
        "owner transaction suspended",
        "owner throws into generator",
        "rolled back and usable",
        "rollback the claim and release the lock",
        "thrown error propagates and messages remain pending",
    ),
    _case(
        "OWNER_THROW_ROLLBACK_ERROR",
        "owner transaction suspended",
        "owner throws and rollback raises Exception",
        "usable with original error",
        "suppress ordinary rollback failure",
        "thrown error remains primary",
    ),
    _case(
        "OWNER_THROW_ROLLBACK_BASE_ERROR",
        "owner transaction suspended",
        "owner throws and rollback raises BaseException",
        "cleanup failed",
        "replace the thrown Exception with the rollback BaseException",
        "cleanup failure propagates",
    ),
    _case(
        "OWNER_NEXT_TO_EXHAUSTION",
        "owner transaction suspended",
        "owner exhausts generator",
        "committed and usable",
        "commit the claim and release the lock",
        "messages are claimed",
    ),
    _case(
        "OWNER_NEXT_COMMIT_ERROR",
        "owner transaction suspended",
        "owner exhausts generator and commit fails",
        "rolled back and usable",
        "rollback the uncommitted claim and release the lock",
        "commit error propagates and messages remain pending",
    ),
    _case(
        "OWNER_NEXT_COMMIT_ERROR_ROLLBACK_ERROR",
        "owner transaction suspended",
        "owner exhausts, commit fails, then rollback raises Exception",
        "usable with commit error primary",
        "suppress ordinary rollback failure in favor of commit failure",
        "commit error propagates",
    ),
    _case(
        "OWNER_NEXT_COMMIT_ERROR_ROLLBACK_BASE_ERROR",
        "owner transaction suspended",
        "owner exhausts, commit fails, then rollback raises BaseException",
        "cleanup failed",
        "replace commit failure with rollback BaseException",
        "cleanup failure propagates",
    ),
    _case(
        "FOREIGN_CLOSE",
        "owner transaction suspended",
        "foreign thread closes generator",
        "poisoned",
        "publish poison without foreign rollback or lock release",
        "later owner operation raises permanent OperationalError",
    ),
    _case(
        "FOREIGN_THROW",
        "owner transaction suspended",
        "foreign thread throws into generator",
        "poisoned",
        "publish poison without foreign rollback or lock release",
        "thrown error returns to foreign caller and owner sees poison",
    ),
    _case(
        "FOREIGN_NEXT",
        "owner transaction suspended",
        "foreign thread resumes next",
        "poisoned",
        "publish poison after the foreign yield without database cleanup",
        "foreign caller gets one value and owner sees poison",
    ),
)


def _foreign_call(
    call: Any,
) -> tuple[BaseException | None, list[warnings.WarningMessage]]:
    error: list[BaseException] = []
    caught: list[warnings.WarningMessage] = []

    def run() -> None:
        nonlocal caught
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            try:
                call()
            except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] exception
                error.append(exc)
            caught = list(recorded)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(2.0)
    assert not thread.is_alive()
    return (error[0] if error else None), caught


def _install_rollback_failure(
    core: BrokerDB,
    *,
    base_error: bool = False,
) -> Any:
    original_rollback = core._runner.rollback

    def rollback_failure() -> None:
        original_rollback()
        if base_error:
            raise KeyboardInterrupt("rollback interrupted")
        raise RuntimeError("rollback failed")

    core._runner.rollback = rollback_failure  # type: ignore[method-assign]
    return original_rollback


def _assert_owner_close(
    core: BrokerDB,
    generator: Generator[str | tuple[str, int], Any, Any],
    *,
    rollback_mode: str,
) -> None:
    original_rollback = core._runner.rollback
    if rollback_mode != "ok":
        original_rollback = _install_rollback_failure(
            core,
            base_error=rollback_mode == "base",
        )
    if rollback_mode == "base":
        with pytest.raises(KeyboardInterrupt, match="rollback interrupted"):
            generator.close()
    else:
        generator.close()
    core._runner.rollback = original_rollback  # type: ignore[method-assign]
    assert list(core.peek_generator("jobs", with_timestamps=False)) == ["one", "two"]
    core.close()


def _assert_owner_throw(
    core: BrokerDB,
    generator: Generator[str | tuple[str, int], Any, Any],
    *,
    rollback_mode: str,
) -> None:
    original_rollback = core._runner.rollback
    if rollback_mode != "ok":
        original_rollback = _install_rollback_failure(
            core,
            base_error=rollback_mode == "base",
        )
    expected_error: type[BaseException] = (
        KeyboardInterrupt if rollback_mode == "base" else ValueError
    )
    with pytest.raises(expected_error):
        generator.throw(ValueError("owner throw"))
    core._runner.rollback = original_rollback  # type: ignore[method-assign]
    assert list(core.peek_generator("jobs", with_timestamps=False)) == ["one", "two"]
    core.close()


def _assert_foreign_finalization(
    payload: str,
    core: BrokerDB,
    generator: Generator[str | tuple[str, int], Any, Any],
) -> None:
    transaction_calls: list[tuple[str, int]] = []
    for method_name in ("commit", "rollback"):
        original = getattr(core._runner, method_name)

        def record(
            *,
            _method_name: str = method_name,
            _original: Any = original,
        ) -> Any:
            transaction_calls.append((_method_name, threading.get_ident()))
            return _original()

        setattr(core._runner, method_name, record)

    def foreign_throw() -> Any:
        return generator.throw(ValueError("foreign throw"))

    def foreign_next() -> list[str | tuple[str, int]]:
        return list(generator)

    operation = {
        "FOREIGN_CLOSE": generator.close,
        "FOREIGN_THROW": foreign_throw,
        "FOREIGN_NEXT": foreign_next,
    }[payload]
    error, caught = _foreign_call(operation)
    if payload == "FOREIGN_THROW":
        assert isinstance(error, ValueError)
    else:
        assert error is None
    assert any("foreign thread" in str(item.message) for item in caught)
    assert transaction_calls == []
    with pytest.raises(OperationalError, match="cross-thread finalization"):
        core.peek_one("jobs")
    # The owner connection still owns the suspended transaction. Close only
    # the raw runner to avoid pretending the poison is recoverable.
    core._runner.close()


def _assert_owner_commit_failure(
    payload: str,
    core: BrokerDB,
    generator: Generator[str | tuple[str, int], Any, Any],
) -> None:
    original_commit = core._runner.commit
    original_rollback = core._runner.rollback

    def commit_failure() -> None:
        raise RuntimeError("commit failed")

    core._runner.commit = commit_failure  # type: ignore[method-assign]
    if payload.endswith("ROLLBACK_ERROR"):
        _install_rollback_failure(core)
    elif payload.endswith("ROLLBACK_BASE_ERROR"):
        _install_rollback_failure(core, base_error=True)

    expected_error: type[BaseException] = (
        KeyboardInterrupt if payload.endswith("BASE_ERROR") else RuntimeError
    )
    with pytest.raises(expected_error):
        list(generator)
    core._runner.commit = original_commit  # type: ignore[method-assign]
    core._runner.rollback = original_rollback  # type: ignore[method-assign]
    assert list(core.peek_generator("jobs", with_timestamps=False)) == ["one", "two"]
    core.close()


@fires_transition_table("SM-DELIVERY-POISON", DELIVERY_POISON_TRANSITIONS)
def test_delivery_poison_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
) -> None:
    core = BrokerDB(str(tmp_path / f"{transition_case.payload}.db"))
    core.write("jobs", "one")
    core.write("jobs", "two")
    generator = cast(
        Generator[str | tuple[str, int], Any, Any],
        core.claim_generator(
            "jobs",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            batch_size=2,
        ),
    )
    assert next(generator) == "one"

    payload = transition_case.payload
    if payload.startswith("OWNER_CLOSE"):
        rollback_mode = (
            "base"
            if payload.endswith("BASE_ERROR")
            else ("error" if payload.endswith("ERROR") else "ok")
        )
        _assert_owner_close(core, generator, rollback_mode=rollback_mode)
    elif payload.startswith("OWNER_THROW"):
        rollback_mode = (
            "base"
            if payload.endswith("BASE_ERROR")
            else ("error" if payload.endswith("ERROR") else "ok")
        )
        _assert_owner_throw(core, generator, rollback_mode=rollback_mode)
    elif payload == "OWNER_NEXT_TO_EXHAUSTION":
        assert list(generator) == ["two"]
        assert list(core.peek_generator("jobs", with_timestamps=False)) == []
        core.close()
    elif payload.startswith("OWNER_NEXT_COMMIT_ERROR"):
        _assert_owner_commit_failure(payload, core, generator)
    else:
        _assert_foreign_finalization(payload, core, generator)
