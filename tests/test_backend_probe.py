"""Tests for the private first-party SQL backend probe seam."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Protocol, cast

import pytest

from simplebroker import Queue
from simplebroker._exceptions import OperationalError

from .helper_scripts.timing import scale_timeout_for_ci

pytestmark = [pytest.mark.sqlite_only]


class _BackendProbe(Protocol):
    def _run_backend_probe(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]: ...


def test_backend_probe_returns_materialized_rows(tmp_path: Path) -> None:
    queue = Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)
    try:
        with queue.get_connection() as connection:
            probe = cast(_BackendProbe, connection)

            assert probe._run_backend_probe("SELECT ?", (42,)) == [(42,)]
    finally:
        queue.close()


def test_backend_probe_retries_failures_raised_during_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)
    attempts = 0
    try:
        with queue.get_connection() as connection:
            core = cast(Any, connection)
            original_run = core._runner.run

            def lazy_run(
                sql: str,
                params: tuple[Any, ...] = (),
                *,
                fetch: bool = False,
            ) -> Any:
                nonlocal attempts
                if sql != "SELECT 42":
                    return original_run(sql, params, fetch=fetch)
                attempts += 1

                def rows() -> Any:
                    assert core._lock._is_owned()
                    if attempts == 1:
                        raise OperationalError("database is locked")
                    yield (42,)

                return rows()

            monkeypatch.setattr(core._runner, "run", lazy_run)
            probe = cast(_BackendProbe, connection)

            assert probe._run_backend_probe("SELECT 42") == [(42,)]
    finally:
        queue.close()

    assert attempts == 2


def test_backend_probe_serializes_same_core_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)
    first_entered = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    second_entered = threading.Event()
    call_lock = threading.Lock()
    call_count = 0
    results: list[list[tuple[int]]] = []
    try:
        with queue.get_connection() as connection:
            core = cast(Any, connection)

            def controlled_run(
                sql: str,
                params: tuple[Any, ...] = (),
                *,
                fetch: bool = False,
            ) -> list[tuple[int]]:
                nonlocal call_count
                del sql, params
                assert fetch is True
                with call_lock:
                    call_count += 1
                    call_number = call_count
                if call_number == 1:
                    first_entered.set()
                    assert release_first.wait(scale_timeout_for_ci(2.0))
                else:
                    second_entered.set()
                return [(call_number,)]

            monkeypatch.setattr(core._runner, "run", controlled_run)
            probe = cast(_BackendProbe, connection)

            def call_probe(*, mark_second_started: bool = False) -> list[tuple[int]]:
                if mark_second_started:
                    second_started.set()
                return probe._run_backend_probe("SELECT 42")

            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(call_probe)
                assert first_entered.wait(scale_timeout_for_ci(2.0))
                second = executor.submit(call_probe, mark_second_started=True)
                assert second_started.wait(scale_timeout_for_ci(2.0))
                assert not second_entered.wait(scale_timeout_for_ci(0.1))
                release_first.set()
                results.extend([first.result(), second.result()])
    finally:
        release_first.set()
        queue.close()

    assert sorted(results) == [[(1,)], [(2,)]]


def test_backend_probe_refuses_fork_mismatch_before_runner_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)
    calls = 0
    try:
        with queue.get_connection() as connection:
            core = cast(Any, connection)
            original_pid = core._pid

            def unexpected_run(*args: Any, **kwargs: Any) -> list[tuple[Any, ...]]:
                nonlocal calls
                del args, kwargs
                calls += 1
                return []

            monkeypatch.setattr(core._runner, "run", unexpected_run)
            core._pid = -1
            try:
                with pytest.raises(RuntimeError, match="forked process"):
                    cast(_BackendProbe, connection)._run_backend_probe("SELECT 42")
            finally:
                core._pid = original_pid
    finally:
        queue.close()

    assert calls == 0


def test_backend_probe_refuses_open_at_least_once_batch(tmp_path: Path) -> None:
    queue = Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)
    try:
        queue.write("one")
        queue.write("two")
        with queue.get_connection() as connection:
            messages = connection.claim_generator(
                "tasks",
                with_timestamps=False,
                delivery_guarantee="at_least_once",
            )
            assert next(messages) == "one"
            probe = cast(_BackendProbe, connection)

            with pytest.raises(RuntimeError, match="at_least_once"):
                probe._run_backend_probe("SELECT 42")

            cast(Any, messages).close()
    finally:
        queue.close()


def test_backend_probe_preserves_terminal_database_error_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)
    error = OperationalError("terminal probe failure")
    error.retryable = False
    try:
        with queue.get_connection() as connection:
            core = cast(Any, connection)

            def failed_run(
                sql: str,
                params: tuple[Any, ...] = (),
                *,
                fetch: bool = False,
            ) -> Any:
                del sql, params, fetch

                def rows() -> Any:
                    yield from ()
                    raise error

                return rows()

            monkeypatch.setattr(core._runner, "run", failed_run)
            probe = cast(_BackendProbe, connection)

            with pytest.raises(OperationalError) as exc_info:
                probe._run_backend_probe("SELECT 42")
    finally:
        queue.close()

    assert exc_info.value is error


def test_backend_probe_does_not_return_success_after_poison_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)
    core: Any | None = None
    try:
        with queue.get_connection() as connection:
            core = cast(Any, connection)

            def poisoning_run(
                sql: str,
                params: tuple[Any, ...] = (),
                *,
                fetch: bool = False,
            ) -> list[tuple[int]]:
                del sql, params, fetch
                core._publish_orphan_poison(
                    "backend probe test",
                    "probe",
                    threading.Thread(name="probe-owner"),
                )
                return [(42,)]

            monkeypatch.setattr(core._runner, "run", poisoning_run)
            probe = cast(_BackendProbe, connection)

            with (
                pytest.warns(RuntimeWarning, match="finalized on a foreign thread"),
                pytest.raises(OperationalError),
            ):
                probe._run_backend_probe("SELECT 42")
    finally:
        try:
            queue.close()
        except OperationalError:
            # A poisoned core intentionally refuses normal close. The process
            # session lease has already been released; close the test-owned
            # runner only to avoid leaking the SQLite handle.
            assert core is not None
            core._runner.close()
