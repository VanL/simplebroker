"""Deterministic SQLite proofs for cross-thread finalization poisoning.

These tests deliberately violate the thread-affinity contract to prove that
the broker publishes a permanent poison state instead of silently wedging.
They use real SQLite runners and real threads. Test doubles are limited to
direct concurrent publication of distinct first-cause metadata.
"""

from __future__ import annotations

import gc
import multiprocessing as mp
import os
import threading
import time
import warnings
from collections.abc import Callable, Generator, Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker import Queue, _broker_session
from simplebroker._exceptions import OperationalError
from simplebroker._runner import SQLiteRunner
from simplebroker.db import (
    _LOCK_PROBE_QUANTUM,
    BrokerCore,
    BrokerDB,
    _PoisonAwareRLock,
)
from simplebroker.helpers import _execute_with_retry

from .helper_scripts.timing import scale_timeout_for_ci

pytestmark = pytest.mark.sqlite_only

_DIAGNOSTIC_PREFIX = "cross-thread finalization:"
_TEST_CORES: list[BrokerCore] = []


class _ForeignThrow(Exception):
    """Distinct exception used to exercise foreign ``throw()`` paths."""


def _make_core(tmp_path: Path, name: str) -> BrokerDB:
    core = BrokerDB(str(tmp_path / f"{name}.db"))
    _TEST_CORES.append(core)
    return core


@pytest.fixture(autouse=True)
def _dispose_test_runners() -> Iterator[None]:
    """Dispose owner-thread SQLite resources after each poison proof.

    Every core and transaction in this module is created and primed by the
    pytest thread. Foreign workers are joined before teardown. The same owner
    thread then closes the raw runner only to prevent unclosed-resource
    warnings; this bypass is not an asserted recovery path and never makes the
    poisoned core usable again.
    """

    first_new_core = len(_TEST_CORES)
    yield
    new_cores = _TEST_CORES[first_new_core:]
    for core in reversed(new_cores):
        core._runner.close()
    del _TEST_CORES[first_new_core:]


def _prime_claim(core: BrokerCore) -> Generator[str | tuple[str, int], Any, Any]:
    core.write("jobs", "one")
    core.write("jobs", "two")
    return _open_claim(core)


def _open_claim(core: BrokerCore) -> Generator[str | tuple[str, int], Any, Any]:
    generator = core.claim_generator(
        "jobs",
        with_timestamps=False,
        delivery_guarantee="at_least_once",
        batch_size=2,
    )
    assert next(generator) == "one"
    return cast(Generator[str | tuple[str, int], Any, Any], generator)


def _call_in_thread(
    call: Callable[[], Any],
    *,
    name: str,
) -> tuple[Any, BaseException | None, list[warnings.WarningMessage]]:
    result: dict[str, Any] = {}
    caught_warnings: list[warnings.WarningMessage] = []

    def run() -> None:
        nonlocal caught_warnings
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            try:
                result["value"] = call()
            except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
                result["error"] = exc
            caught_warnings = list(recorded)

    thread = threading.Thread(target=run, name=name, daemon=True)
    thread.start()
    thread.join(scale_timeout_for_ci(2.0))
    assert not thread.is_alive(), f"{name} did not finish"
    return result.get("value"), result.get("error"), caught_warnings


def _assert_foreign_warning(
    caught: list[warnings.WarningMessage],
    *,
    surface: str,
) -> None:
    matching = [
        warning
        for warning in caught
        if issubclass(warning.category, RuntimeWarning)
        and f"{surface} finalized on a foreign thread" in str(warning.message)
    ]
    assert len(matching) == 1


def _assert_poison_diagnostic(exc: BaseException) -> OperationalError:
    assert isinstance(exc, OperationalError)
    assert str(exc).startswith(_DIAGNOSTIC_PREFIX)
    assert exc.retryable is False
    return exc


def _record_runner_calls(
    core: BrokerDB,
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []
    for method_name in ("begin_immediate", "run", "commit", "rollback"):
        original = getattr(core._runner, method_name)

        def record(
            *args: Any,
            _method_name: str = method_name,
            _original: Callable[..., Any] = original,
            **kwargs: Any,
        ) -> Any:
            calls.append((_method_name, threading.current_thread().name))
            return _original(*args, **kwargs)

        monkeypatch.setattr(core._runner, method_name, record)
    return calls


def _queue_close_mode_child(
    send_connection: Any,
    db_path: str,
    mode: str,
) -> None:
    try:
        if mode == "private_persistent":
            from simplebroker._runner import SQLiteRunner

            runner = SQLiteRunner(db_path)
            queue = Queue(
                "jobs",
                db_path=db_path,
                persistent=True,
                runner=runner,
            )
        else:
            queue = Queue("jobs", db_path=db_path)
        queue.write("one")
        queue.write("two")
        generator = cast(
            Generator[Any, Any, Any],
            queue.read_generator(
                with_timestamps=False,
                delivery_guarantee="at_least_once",
            ),
        )
        assert next(generator) == "one"
        _, foreign_error, caught = _call_in_thread(
            generator.close,
            name=f"foreign-{mode}-close",
        )
        try:
            queue.close()
            queue_close_error = None
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            queue_close_error = f"{type(exc).__name__}: {exc}"
        result = {
            "foreign_error": (
                None
                if foreign_error is None
                else f"{type(foreign_error).__name__}: {foreign_error}"
            ),
            "warning_count": len(caught),
            "queue_close_error": queue_close_error,
        }
    except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
        result = {"probe_error": f"{type(exc).__name__}: {exc}"}
    try:
        send_connection.send(result)
        send_connection.close()
    finally:
        os._exit(0)


def _run_queue_close_mode_probe(db_path: str, mode: str) -> dict[str, Any]:
    context = mp.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_queue_close_mode_child,
        args=(send_connection, db_path, mode),
    )
    process.start()
    send_connection.close()
    try:
        assert receive_connection.poll(scale_timeout_for_ci(5.0))
        result = cast(dict[str, Any], receive_connection.recv())
    finally:
        receive_connection.close()
    process.join(scale_timeout_for_ci(2.0))
    if process.is_alive():
        process.terminate()
        process.join(scale_timeout_for_ci(2.0))
    assert not process.is_alive()
    assert process.exitcode == 0
    return result


@pytest.mark.parametrize("action", ["close", "throw"])
def test_foreign_generator_finalization_publishes_poison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    core = _make_core(tmp_path, f"generator-{action}")
    generator = _prime_claim(core)
    calls = _record_runner_calls(core, monkeypatch)
    thrown = _ForeignThrow("foreign generator throw")

    def finalize() -> None:
        if action == "close":
            generator.close()
        else:
            generator.throw(thrown)

    _, error, caught = _call_in_thread(
        finalize,
        name=f"foreign-generator-{action}",
    )

    if action == "close":
        assert error is None
    else:
        assert error is thrown
    assert core._poisoned is True
    assert core._poison_cause
    _assert_foreign_warning(caught, surface="at_least_once generator")
    assert calls == []


def test_foreign_generator_normal_resumption_terminates_and_poisons(
    tmp_path: Path,
) -> None:
    core = _make_core(tmp_path, "generator-next")
    generator = _prime_claim(core)

    _, error, caught = _call_in_thread(
        lambda: next(generator),
        name="foreign-generator-next",
    )

    assert isinstance(error, StopIteration)
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="at_least_once generator")


@pytest.mark.parametrize(
    "surface",
    ["read_generator", "move_generator", "sidecar", "stream_messages"],
)
def test_foreign_persistent_wrapper_finalization_drains_then_diagnoses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
) -> None:
    monkeypatch.setattr(_broker_session, "_CLOSE_ACTIVE_OPERATION_TIMEOUT", 0.05)
    queue = Queue(
        "jobs",
        db_path=str(tmp_path / f"{surface}.db"),
        persistent=True,
    )
    queue.write("one")
    queue.write("two")
    finalize: Callable[[], Any]
    resource: Any

    if surface == "read_generator":
        resource = cast(
            Generator[Any, Any, Any],
            queue.read_generator(
                with_timestamps=False,
                delivery_guarantee="at_least_once",
            ),
        )
        assert next(resource) == "one"
        finalize = resource.close
    elif surface == "move_generator":
        resource = cast(
            Generator[Any, Any, Any],
            queue.move_generator(
                "done",
                with_timestamps=False,
                delivery_guarantee="at_least_once",
            ),
        )
        assert next(resource) == "one"
        finalize = resource.close
    elif surface == "sidecar":
        resource = queue.sidecar()
        resource.__enter__()

        def finalize_sidecar() -> Any:
            return resource.__exit__(None, None, None)

        finalize = finalize_sidecar
    else:
        resource = cast(
            Generator[tuple[str, int], Any, Any],
            queue.stream_messages(
                batch_processing=True,
                commit_interval=2,
            ),
        )
        assert next(resource)[0] == "one"
        finalize = resource.close

    assert queue.conn is not None
    core = cast(BrokerDB, queue.conn.get_core())
    _TEST_CORES.append(core)

    _, close_error, caught = _call_in_thread(
        finalize,
        name=f"foreign-{surface}-close",
    )

    assert close_error is None
    assert core._poisoned is True
    warning_surface = (
        "sidecar session" if surface == "sidecar" else "at_least_once generator"
    )
    _assert_foreign_warning(caught, surface=warning_surface)
    shared_session = queue.conn._shared_session
    assert shared_session is not None
    assert shared_session._active_operations == 1
    close_started = time.monotonic()
    with pytest.raises(OperationalError) as close_diagnostic:
        queue.close()
    close_elapsed = time.monotonic() - close_started
    _assert_poison_diagnostic(close_diagnostic.value)
    assert close_elapsed >= 0.04
    assert close_elapsed <= scale_timeout_for_ci(1.0)
    queue.close()  # the shared-session registry entry is already gone


def test_foreign_persistent_wrapper_non_last_lease_close_returns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_broker_session, "_CLOSE_ACTIVE_OPERATION_TIMEOUT", 0.05)
    db_path = str(tmp_path / "shared-non-last.db")
    queue = Queue("jobs", db_path=db_path, persistent=True)
    sibling = Queue("other", db_path=db_path, persistent=True)
    queue.write("one")
    queue.write("two")
    generator = cast(
        Generator[Any, Any, Any],
        queue.read_generator(
            with_timestamps=False,
            delivery_guarantee="at_least_once",
        ),
    )
    assert next(generator) == "one"
    assert queue.conn is not None
    core = cast(BrokerDB, queue.conn.get_core())
    _TEST_CORES.append(core)

    _, close_error, caught = _call_in_thread(
        generator.close,
        name="foreign-shared-read-close",
    )
    assert close_error is None
    _assert_foreign_warning(caught, surface="at_least_once generator")

    with pytest.raises(OperationalError) as sibling_rejected:
        sibling.write("rejected")
    _assert_poison_diagnostic(sibling_rejected.value)

    queue.close()
    with pytest.raises(OperationalError) as last_lease:
        sibling.close()
    _assert_poison_diagnostic(last_lease.value)
    sibling.close()


@pytest.mark.parametrize("mode", ["private_persistent", "ephemeral"])
def test_foreign_wrapper_queue_close_return_modes_are_process_isolated(
    tmp_path: Path,
    mode: str,
) -> None:
    result = _run_queue_close_mode_probe(str(tmp_path / f"{mode}.db"), mode)
    assert "probe_error" not in result
    assert result["foreign_error"] is None
    assert result["warning_count"] == 1
    assert result["queue_close_error"] is None


def test_poison_diagnoses_owner_and_post_publication_waiter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _make_core(tmp_path, "owner-waiter")
    generator = _prime_claim(core)
    calls = _record_runner_calls(core, monkeypatch)

    _, foreign_close_error, caught = _call_in_thread(
        generator.close,
        name="foreign-generator-close",
    )
    assert foreign_close_error is None
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="at_least_once generator")

    with pytest.raises(OperationalError) as write_error:
        core.write("jobs", "owner-write")
    _assert_poison_diagnostic(write_error.value)

    with pytest.raises(OperationalError) as read_error:
        core.peek_one("jobs", with_timestamps=False)
    _assert_poison_diagnostic(read_error.value)

    _, waiter_error, _ = _call_in_thread(
        lambda: core.peek_one("jobs", with_timestamps=False),
        name="post-publication-waiter",
    )
    assert waiter_error is not None
    _assert_poison_diagnostic(waiter_error)

    with pytest.raises(OperationalError) as core_close_error:
        core.close()
    _assert_poison_diagnostic(core_close_error.value)
    with pytest.raises(OperationalError) as shutdown_error:
        core.shutdown()
    _assert_poison_diagnostic(shutdown_error.value)
    assert calls == []


def test_preblocked_waiter_observes_poison_without_hanging(tmp_path: Path) -> None:
    core = _make_core(tmp_path, "preblocked-waiter")
    generator = _prime_claim(core)
    waiter_started = threading.Event()
    waiter_finished = threading.Event()
    waiter_error: list[BaseException] = []

    def wait_on_core() -> None:
        waiter_started.set()
        try:
            core.peek_one("jobs", with_timestamps=False)
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            waiter_error.append(exc)
        finally:
            waiter_finished.set()

    waiter = threading.Thread(
        target=wait_on_core,
        name="preblocked-waiter",
        daemon=True,
    )
    waiter.start()
    assert waiter_started.wait(scale_timeout_for_ci(1.0))
    assert not waiter_finished.wait(0.05), "waiter did not block on the held lock"

    published_at = time.monotonic()
    _, close_error, caught = _call_in_thread(
        generator.close,
        name="foreign-generator-close",
    )
    assert close_error is None
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="at_least_once generator")

    deadline = scale_timeout_for_ci((2 * _LOCK_PROBE_QUANTUM) + 0.5)
    assert waiter_finished.wait(deadline), "waiter did not diagnose poison"
    waiter.join(scale_timeout_for_ci(1.0))
    assert not waiter.is_alive()
    assert time.monotonic() - published_at <= deadline
    assert len(waiter_error) == 1
    _assert_poison_diagnostic(waiter_error[0])


def test_shared_runner_sibling_times_out_after_foreign_generator_poison(
    tmp_path: Path,
) -> None:
    config = {"BROKER_BUSY_TIMEOUT": 50}
    runner = SQLiteRunner(str(tmp_path / "shared-runner-poison.db"), config=config)
    core = BrokerCore(runner, config=config)
    _TEST_CORES.append(core)
    generator = _prime_claim(core)

    _, close_error, caught = _call_in_thread(
        generator.close,
        name="foreign-generator-close",
    )
    assert close_error is None
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="at_least_once generator")

    started_at = time.monotonic()
    _, sibling_error, _ = _call_in_thread(
        lambda: runner.run("SELECT 1", fetch=True),
        name="shared-runner-sibling",
    )
    elapsed = time.monotonic() - started_at

    assert isinstance(sibling_error, OperationalError)
    assert sibling_error.retryable is True
    assert "timed out waiting for transaction admission" in str(sibling_error)
    assert elapsed <= scale_timeout_for_ci(1.0)


@pytest.mark.parametrize("transaction", [False, True])
@pytest.mark.parametrize("action", ["clean_exit", "throw"])
def test_foreign_sidecar_exit_closes_retained_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction: bool,
    action: str,
) -> None:
    core = _make_core(tmp_path, f"sidecar-{transaction}-{action}")
    manager = core.sidecar(transaction=transaction)
    session = manager.__enter__()
    calls = _record_runner_calls(core, monkeypatch)
    thrown = _ForeignThrow("foreign sidecar throw")

    def finalize() -> Any:
        if action == "clean_exit":
            return manager.__exit__(None, None, None)
        return manager.gen.throw(thrown)

    value, error, caught = _call_in_thread(
        finalize,
        name=f"foreign-sidecar-{action}",
    )

    if action == "clean_exit":
        assert value is False
        assert error is None
    else:
        assert error is thrown
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="sidecar session")
    with pytest.raises(RuntimeError, match="sidecar session is closed"):
        session.run("SELECT 1", fetch=True)
    assert calls == []


@pytest.mark.parametrize("resumption", ["next", "throw", "close"])
def test_owner_generator_resumption_after_foreign_outer_sidecar_exit(
    tmp_path: Path,
    resumption: str,
) -> None:
    core = _make_core(tmp_path, f"nested-sidecar-outer-{resumption}")
    core.write("jobs", "one")
    core.write("jobs", "two")
    sidecar_manager = core.sidecar()
    session = sidecar_manager.__enter__()
    generator = _open_claim(core)

    _, exit_error, caught = _call_in_thread(
        lambda: sidecar_manager.__exit__(None, None, None),
        name="foreign-outer-sidecar-exit",
    )
    assert exit_error is None
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="sidecar session")
    with pytest.raises(RuntimeError, match="sidecar session is closed"):
        session.run("SELECT 1", fetch=True)

    thrown = _ForeignThrow("owner resumption throw")
    with pytest.raises(OperationalError) as resumed:
        if resumption == "next":
            next(generator)
        elif resumption == "throw":
            generator.throw(thrown)
        else:
            generator.close()
    diagnostic = _assert_poison_diagnostic(resumed.value)
    if resumption == "throw":
        assert diagnostic.__cause__ is thrown
    elif resumption == "close":
        assert isinstance(diagnostic.__cause__, GeneratorExit)
    else:
        assert diagnostic.__cause__ is None

    observer = BrokerDB(str(core.db_path))
    _TEST_CORES.append(observer)
    assert observer.peek_many("jobs", 10, with_timestamps=False) == ["one", "two"]
    observer.shutdown()


def test_poison_published_during_query_blocks_first_yield(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _make_core(tmp_path, "first-yield-race")
    core.write("jobs", "one")
    core.write("jobs", "two")
    sidecar_manager = core.sidecar()
    sidecar_manager.__enter__()
    generator = core.claim_generator(
        "jobs",
        with_timestamps=False,
        delivery_guarantee="at_least_once",
        batch_size=2,
    )
    query_ready = threading.Event()
    allow_query_return = threading.Event()
    original_run = core._runner.run

    def pause_after_query(*args: Any, **kwargs: Any) -> Any:
        result = original_run(*args, **kwargs)
        if kwargs.get("fetch") and not query_ready.is_set():
            query_ready.set()
            assert allow_query_return.wait(scale_timeout_for_ci(2.0))
        return result

    monkeypatch.setattr(core._runner, "run", pause_after_query)
    result: dict[str, Any] = {}

    def poison_outer_sidecar() -> None:
        assert query_ready.wait(scale_timeout_for_ci(2.0))
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                result["exit"] = sidecar_manager.__exit__(None, None, None)
            except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
                result["error"] = exc
            result["warnings"] = list(caught)
        allow_query_return.set()

    poisoner = threading.Thread(
        target=poison_outer_sidecar,
        name="foreign-sidecar-query-race",
        daemon=True,
    )
    poisoner.start()
    with pytest.raises(OperationalError) as rejected:
        next(generator)
    poisoner.join(scale_timeout_for_ci(2.0))

    assert not poisoner.is_alive()
    assert result.get("exit") is False
    assert "error" not in result
    _assert_foreign_warning(
        cast(list[warnings.WarningMessage], result["warnings"]),
        surface="sidecar session",
    )
    _assert_poison_diagnostic(rejected.value)


def test_owner_sidecar_resumption_after_foreign_inner_generator_exit(
    tmp_path: Path,
) -> None:
    core = _make_core(tmp_path, "nested-generator-inner")
    core.write("jobs", "one")
    core.write("jobs", "two")
    sidecar_manager = core.sidecar()
    session = sidecar_manager.__enter__()
    generator = _open_claim(core)

    _, close_error, caught = _call_in_thread(
        generator.close,
        name="foreign-inner-generator-close",
    )
    assert close_error is None
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="at_least_once generator")

    with pytest.raises(OperationalError) as resumed:
        sidecar_manager.__exit__(None, None, None)
    _assert_poison_diagnostic(resumed.value)
    with pytest.raises(RuntimeError, match="sidecar session is closed"):
        session.run("SELECT 1", fetch=True)


def test_adversarial_nested_sidecar_poison_is_monotonic(tmp_path: Path) -> None:
    core = _make_core(tmp_path, "nested-sidecars")
    outer_manager = core.sidecar()
    outer_session = outer_manager.__enter__()
    inner_manager = core.sidecar()
    inner_session = inner_manager.__enter__()

    _, inner_error, caught = _call_in_thread(
        lambda: inner_manager.__exit__(None, None, None),
        name="foreign-inner-sidecar-close",
    )
    assert inner_error is None
    _assert_foreign_warning(caught, surface="sidecar session")
    first_cause = core._poison_cause

    with pytest.raises(OperationalError) as outer_rejected:
        outer_manager.__exit__(None, None, None)
    _assert_poison_diagnostic(outer_rejected.value)
    assert core._poison_cause == first_cause
    with pytest.raises(RuntimeError, match="sidecar session is closed"):
        inner_session.run("SELECT 1", fetch=True)
    with pytest.raises(RuntimeError, match="sidecar session is closed"):
        outer_session.run("SELECT 1", fetch=True)


def test_poison_diagnostic_wins_when_owner_rollback_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _make_core(tmp_path, "rollback-failure")
    core.write("jobs", "one")
    core.write("jobs", "two")
    sidecar_manager = core.sidecar()
    sidecar_manager.__enter__()
    generator = _open_claim(core)

    def finalize_sidecar() -> Any:
        return sidecar_manager.__exit__(None, None, None)

    _, exit_error, caught = _call_in_thread(
        finalize_sidecar,
        name="foreign-sidecar-poison",
    )
    assert exit_error is None
    _assert_foreign_warning(caught, surface="sidecar session")

    def fail_rollback() -> None:
        raise RuntimeError("injected rollback failure")

    monkeypatch.setattr(core._runner, "rollback", fail_rollback)
    with pytest.raises(OperationalError) as rejected:
        generator.close()

    diagnostic = _assert_poison_diagnostic(rejected.value)
    assert isinstance(diagnostic.__cause__, GeneratorExit)
    assert diagnostic.__notes__ == [
        "cleanup failure: RuntimeError: injected rollback failure"
    ]


@pytest.mark.parametrize("resumption", ["throw", "close"])
@pytest.mark.parametrize("rollback_fails", [False, True])
def test_owner_transactional_sidecar_exception_resumption_after_poison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resumption: str,
    rollback_fails: bool,
) -> None:
    core = _make_core(
        tmp_path,
        f"transactional-sidecar-{resumption}-{rollback_fails}",
    )
    outer = core.sidecar(transaction=True)
    outer.__enter__()
    poisoner = core.sidecar(transaction=False)
    poisoner.__enter__()

    _, poison_error, caught = _call_in_thread(
        lambda: poisoner.__exit__(None, None, None),
        name="foreign-inner-sidecar-exit",
    )
    assert poison_error is None
    _assert_foreign_warning(caught, surface="sidecar session")

    if rollback_fails:

        def fail_rollback() -> None:
            raise RuntimeError("injected sidecar rollback failure")

        monkeypatch.setattr(core._runner, "rollback", fail_rollback)

    thrown = _ForeignThrow("owner sidecar throw")
    with pytest.raises(OperationalError) as rejected:
        if resumption == "throw":
            outer.gen.throw(thrown)
        else:
            outer.gen.close()

    diagnostic = _assert_poison_diagnostic(rejected.value)
    if resumption == "throw":
        assert diagnostic.__cause__ is thrown
    else:
        assert isinstance(diagnostic.__cause__, GeneratorExit)
    expected_notes = (
        ["cleanup failure: RuntimeError: injected sidecar rollback failure"]
        if rollback_fails
        else None
    )
    assert getattr(diagnostic, "__notes__", None) == expected_notes


def test_warning_failure_never_replaces_generator_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _make_core(tmp_path, "warning-failure")
    generator = _prime_claim(core)

    def fail_warning(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise RuntimeWarning("warning promoted to error")

    monkeypatch.setattr(warnings, "warn", fail_warning)
    _, close_error, caught = _call_in_thread(
        generator.close,
        name="foreign-generator-warning-failure",
    )

    assert close_error is None
    assert caught == []
    assert core._poisoned is True
    with pytest.raises(OperationalError) as rejected:
        core.write("jobs", "rejected")
    _assert_poison_diagnostic(rejected.value)


def test_foreign_gc_publishes_poison_and_warns(tmp_path: Path) -> None:
    core = _make_core(tmp_path, "foreign-gc")
    generator_box = [_prime_claim(core)]

    def drop_last_reference() -> None:
        generator_box.clear()
        gc.collect()

    _, gc_error, caught = _call_in_thread(
        drop_last_reference,
        name="foreign-generator-gc",
    )

    assert gc_error is None
    assert generator_box == []
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="at_least_once generator")


def test_poison_aware_lock_preserves_explicit_rlock_compatibility(
    tmp_path: Path,
) -> None:
    core = _make_core(tmp_path, "lock-compatibility")
    lock = core._lock
    assert isinstance(lock, _PoisonAwareRLock)

    assert lock.acquire(blocking=False)
    assert lock._is_owned()
    lock.release()
    assert not lock._is_owned()

    lock.acquire()
    entered = threading.Event()
    finished = threading.Event()
    errors: list[BaseException] = []

    def contend() -> None:
        try:
            with lock:
                entered.set()
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)
        finally:
            finished.set()

    waiter = threading.Thread(target=contend, name="healthy-lock-waiter", daemon=True)
    waiter.start()
    assert not entered.wait(0.05)
    lock.release()
    assert finished.wait(scale_timeout_for_ci(1.0))
    waiter.join(scale_timeout_for_ci(1.0))
    assert not waiter.is_alive()
    assert entered.is_set()
    assert errors == []
    assert core._poisoned is False
    core.shutdown()


def test_healthy_lock_contention_stress_never_publishes_poison(
    tmp_path: Path,
) -> None:
    core = _make_core(tmp_path, "healthy-contention-stress")
    start = threading.Barrier(9)
    counter = 0
    errors: list[BaseException] = []

    def contend() -> None:
        nonlocal counter
        try:
            start.wait()
            for _ in range(100):
                with core._lock:
                    counter += 1
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    threads = [
        threading.Thread(target=contend, name=f"healthy-contender-{index}")
        for index in range(8)
    ]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(scale_timeout_for_ci(2.0))
        assert not thread.is_alive()

    assert errors == []
    assert counter == 800
    assert core._poisoned is False
    core.get_meta()


def test_lock_wrapper_construction_tolerance_and_post_success_release() -> None:
    uninitialized = object.__new__(BrokerDB)
    uninitialized._raise_if_poisoned()

    class FalseSuccessLock:
        def __init__(self) -> None:
            self.release_calls = 0

        def acquire(self, *args: Any, **kwargs: Any) -> bool:
            del args, kwargs
            return True

        def release(self) -> None:
            self.release_calls += 1

    lock = FalseSuccessLock()
    probe_calls = 0

    def poison_after_acquire() -> None:
        nonlocal probe_calls
        probe_calls += 1
        if probe_calls == 2:
            raise OperationalError("post-success poison")

    wrapper = _PoisonAwareRLock(lock, poison_after_acquire)
    with pytest.raises(OperationalError, match="post-success poison"):
        wrapper.acquire_held()
    assert probe_calls == 2
    assert lock.release_calls == 1


def test_concurrent_publication_preserves_exactly_one_first_cause(
    tmp_path: Path,
) -> None:
    core = _make_core(tmp_path, "first-cause")
    owner_thread = threading.current_thread()
    barrier = threading.Barrier(3)
    errors: list[BaseException] = []

    def publish(kind: str, operation: str) -> None:
        try:
            barrier.wait(timeout=scale_timeout_for_ci(1.0))
            core._publish_orphan_poison(
                kind=kind,
                operation=operation,
                owner_thread=owner_thread,
            )
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    publishers = [
        threading.Thread(
            target=publish,
            args=("at_least_once generator", "claim"),
            name="publish-generator",
            daemon=True,
        ),
        threading.Thread(
            target=publish,
            args=("sidecar session", "sidecar"),
            name="publish-sidecar",
            daemon=True,
        ),
    ]
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        for publisher in publishers:
            publisher.start()
        barrier.wait(timeout=scale_timeout_for_ci(1.0))
        for publisher in publishers:
            publisher.join(scale_timeout_for_ci(1.0))
            assert not publisher.is_alive()

    assert errors == []
    assert len(recorded) == 2
    assert all(issubclass(warning.category, RuntimeWarning) for warning in recorded)
    assert core._poisoned is True
    first_cause = core._poison_cause
    assert first_cause
    generator_won = first_cause.startswith("at_least_once generator:claim:") and (
        "finalizer=publish-generator/" in first_cause
    )
    sidecar_won = first_cause.startswith("sidecar session:sidecar:") and (
        "finalizer=publish-sidecar/" in first_cause
    )
    assert generator_won ^ sidecar_won

    with pytest.raises(OperationalError):
        core._raise_if_poisoned()
    assert core._poison_cause == first_cause
    with pytest.warns(RuntimeWarning, match="later violation finalized"):
        core._publish_orphan_poison(
            kind="later violation",
            operation="later",
            owner_thread=owner_thread,
        )
    assert core._poison_cause == first_cause


def test_poison_diagnostic_is_non_retryable_and_attempted_once(
    tmp_path: Path,
) -> None:
    core = _make_core(tmp_path, "retryability")
    generator = _prime_claim(core)
    _, close_error, caught = _call_in_thread(
        generator.close,
        name="foreign-generator-close",
    )
    assert close_error is None
    assert core._poisoned is True
    _assert_foreign_warning(caught, surface="at_least_once generator")

    with pytest.raises(OperationalError) as rejected:
        core.write("jobs", "rejected")
    diagnostic = _assert_poison_diagnostic(rejected.value)

    attempts = 0

    def reject() -> None:
        nonlocal attempts
        attempts += 1
        raise diagnostic

    with pytest.raises(OperationalError) as reraised:
        _execute_with_retry(reject, max_retries=5, retry_delay=0)
    assert reraised.value is diagnostic
    assert attempts == 1
