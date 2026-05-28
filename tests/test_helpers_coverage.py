"""Focused coverage for helper edge cases."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from simplebroker import helpers
from simplebroker._exceptions import OperationalError, StopException
from simplebroker.helpers import (
    SetupProgressBudget,
    _create_compound_db_directories,
    _execute_with_retry,
    _find_project_database,
    _is_compound_db_name,
    _is_filesystem_root,
    _resolve_symlinks_safely,
    _validate_database_parent_directory,
    _validate_path_containment,
    _validate_working_directory,
    ensure_compound_db_path,
    execute_setup_with_retry,
    interruptible_sleep,
    is_ancestor,
)


def test_interruptible_sleep_handles_zero_and_interrupts() -> None:
    assert interruptible_sleep(0) is True

    class StopEvent:
        def __init__(self) -> None:
            self.waits = 0

        def wait(self, timeout: float) -> bool:
            self.waits += 1
            return True

        def is_set(self) -> bool:
            return True

    stop_event = StopEvent()
    assert interruptible_sleep(1.0, stop_event, chunk_size=0.01) is False
    assert stop_event.waits == 1


def test_interruptible_sleep_short_sleep_uses_single_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Event:
        def __init__(self) -> None:
            self.timeouts: list[float] = []

        def wait(self, timeout: float) -> bool:
            self.timeouts.append(timeout)
            return False

    event = Event()
    monkeypatch.setattr(helpers.threading, "Event", lambda: event)

    assert interruptible_sleep(0.05, chunk_size=0.1) is True
    assert event.timeouts == [0.05]


def test_execute_with_retry_retries_locked_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    attempts = 0

    def fake_sleep(wait: float, stop_event=None) -> bool:
        sleeps.append(wait)
        return True

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("database is locked")
        return "ok"

    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)

    assert _execute_with_retry(operation, max_retries=3, retry_delay=0) == "ok"
    assert attempts == 2
    assert len(sleeps) == 1


def test_execute_with_retry_stops_when_sleep_is_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_sleep(wait: float, stop_event=None) -> bool:
        return False

    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)

    with pytest.raises(StopException, match="Retry interrupted"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is busy")),
            max_retries=2,
            retry_delay=0,
        )


def test_execute_with_retry_does_not_retry_unrelated_operational_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        helpers,
        "interruptible_sleep",
        lambda wait, stop_event=None: pytest.fail("unexpected retry"),
    )

    with pytest.raises(OperationalError, match="syntax error"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("syntax error"))
        )


def test_execute_with_retry_uses_elapsed_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    sleeps: list[float] = []

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        sleeps.append(wait)
        monotonic_time += wait
        return True

    monkeypatch.setattr(helpers.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)

    with pytest.raises(OperationalError, match="database is locked"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is locked")),
            max_retries=None,
            retry_delay=0.1,
            max_retry_delay=0.1,
            max_elapsed=0.15,
        )

    assert sleeps
    assert all(wait <= 0.1 for wait in sleeps)
    assert monotonic_time <= 0.15


def test_execute_with_retry_elapsed_budget_still_honors_stop_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()

    def fake_sleep(wait: float, stop_event=None) -> bool:
        return False

    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)

    with pytest.raises(StopException, match="Retry interrupted"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is busy")),
            max_retries=None,
            retry_delay=0.01,
            max_elapsed=1.0,
            stop_event=stop_event,
        )


def test_execute_setup_with_retry_refreshes_progress_budget_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    first_attempts = 0
    second_ran = False

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    monkeypatch.setattr(helpers.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(helpers.time, "time", lambda: 0.0)
    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(helpers, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    def first_operation() -> str:
        nonlocal first_attempts
        first_attempts += 1
        if monotonic_time < 0.14:
            raise OperationalError("database is locked")
        return "first"

    def second_operation() -> str:
        nonlocal second_ran
        second_ran = True
        return "second"

    budget = SetupProgressBudget()

    assert (
        execute_setup_with_retry(
            first_operation,
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )
        == "first"
    )
    assert first_attempts > 1
    assert (
        execute_setup_with_retry(
            second_operation,
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )
        == "second"
    )
    assert second_ran


def test_execute_setup_with_retry_fails_when_no_operation_makes_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    monkeypatch.setattr(helpers.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(helpers.time, "time", lambda: 0.0)
    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(helpers, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    budget = SetupProgressBudget()

    with pytest.raises(OperationalError) as exc_info:
        execute_setup_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is locked")),
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    message = str(exc_info.value)
    assert "made no progress" in message
    assert "database is locked" in message


def test_execute_setup_with_retry_does_not_refresh_budget_on_failed_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    second_ran = False

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    def second_operation() -> str:
        nonlocal second_ran
        second_ran = True
        return "second"

    monkeypatch.setattr(helpers.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(helpers.time, "time", lambda: 0.0)
    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(helpers, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    budget = SetupProgressBudget()

    with pytest.raises(OperationalError, match="made no progress"):
        execute_setup_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is locked")),
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    with pytest.raises(OperationalError, match="setup idle timeout expired"):
        execute_setup_with_retry(
            second_operation,
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    assert not second_ran


def test_execute_setup_with_retry_reports_immediate_setup_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helpers, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    budget = SetupProgressBudget()

    with pytest.raises(OperationalError) as exc_info:
        execute_setup_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("syntax error")),
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    message = str(exc_info.value)
    assert "failed" in message
    assert "syntax error" in message
    assert "made no progress" not in message


def test_validate_working_directory_reports_missing_and_file_paths(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="Directory not found"):
        _validate_working_directory(tmp_path / "missing")

    file_path = tmp_path / "not-a-dir"
    file_path.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="Path is a file"):
        _validate_working_directory(file_path)


def test_filesystem_root_and_ancestor_helpers(tmp_path: Path) -> None:
    assert _is_filesystem_root(Path("/")) is True
    child = tmp_path / "parent" / "child"
    child.mkdir(parents=True)

    assert is_ancestor(tmp_path / "parent", child) is True
    assert is_ancestor(child, tmp_path / "parent") is False


def test_find_project_database_rejects_missing_start_and_obeys_depth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="Starting directory does not exist"):
        _find_project_database(".broker.db", tmp_path / "missing")

    root = tmp_path / "root"
    nested = root / "a" / "b"
    nested.mkdir(parents=True)
    candidate = root / ".broker.db"
    candidate.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        helpers,
        "_is_valid_sqlite_db",
        lambda path, verify_magic=True: Path(path) == candidate,
    )

    assert _find_project_database(".broker.db", nested, max_depth=1) is None
    assert _find_project_database(".broker.db", nested, max_depth=5) == candidate


def test_compound_database_path_creation_and_validation(tmp_path: Path) -> None:
    assert _is_compound_db_name("simple.db") == (False, [])
    assert ensure_compound_db_path(tmp_path, "simple.db") == tmp_path / "simple.db"

    compound = ensure_compound_db_path(tmp_path, "state/broker.db")
    assert compound == tmp_path / "state" / "broker.db"
    assert compound.parent.is_dir()

    with pytest.raises(ValueError, match="nested directories"):
        ensure_compound_db_path(tmp_path, "too/deep/broker.db")


def test_create_compound_db_directories_noops_for_simple_names(
    tmp_path: Path,
) -> None:
    _create_compound_db_directories(tmp_path, "broker.db")

    assert list(tmp_path.iterdir()) == []


def test_create_compound_db_directories_creates_parent_directory(
    tmp_path: Path,
) -> None:
    _create_compound_db_directories(tmp_path, "state/broker.db")

    assert (tmp_path / "state").is_dir()


def test_create_compound_db_directories_wraps_mkdir_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def raise_permission_error(self: Path, *args, **kwargs) -> None:
        raise PermissionError("blocked")

    monkeypatch.setattr(Path, "mkdir", raise_permission_error)

    with pytest.raises(ValueError, match="Cannot create intermediate directories"):
        _create_compound_db_directories(tmp_path, "state/broker.db")


def test_validate_database_parent_directory_rejects_missing_parent(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="Parent directory not found"):
        _validate_database_parent_directory(tmp_path / "missing" / "broker.db")


def test_validate_database_parent_directory_rejects_inaccessible_parent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "broker.db"

    monkeypatch.setattr(helpers.os, "access", lambda path, mode: False)
    with pytest.raises(ValueError, match="not accessible"):
        _validate_database_parent_directory(db_path)

    def writable_but_not_executable(path: Path, mode: int) -> bool:
        return mode != helpers.os.W_OK

    monkeypatch.setattr(helpers.os, "access", writable_but_not_executable)
    with pytest.raises(ValueError, match="not writable"):
        _validate_database_parent_directory(db_path)


def test_validate_path_containment_rejects_outside_db_and_bad_project_scope(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    outside = tmp_path / "outside.db"

    with pytest.raises(ValueError, match="within the working directory"):
        _validate_path_containment(outside, workdir, used_project_scope=False)

    sibling_db = tmp_path / "sibling" / "broker.db"
    sibling_db.parent.mkdir()
    with pytest.raises(ValueError, match="parent directory chain"):
        _validate_path_containment(sibling_db, workdir, used_project_scope=True)


def test_resolve_symlinks_safely_wraps_resolution_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_os_error(self: Path) -> Path:
        raise OSError("boom")

    monkeypatch.setattr(Path, "resolve", raise_os_error)

    with pytest.raises(RuntimeError, match="Failed to resolve symlinks"):
        _resolve_symlinks_safely(Path("broker.db"))


def test_resolve_symlinks_safely_resolves_absolute_unfinished_symlink(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.db"

    class FakeSymlink:
        parent = tmp_path

        def resolve(self):
            return self

        def is_symlink(self) -> bool:
            return True

        def readlink(self) -> Path:
            return target

    resolved = _resolve_symlinks_safely(FakeSymlink())  # type: ignore[arg-type]

    assert resolved == target.resolve()


def test_resolve_symlinks_safely_resolves_relative_unfinished_symlink(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.db"

    class FakeParent:
        def __truediv__(self, child: Path) -> Path:
            assert child == Path("target.db")
            return target

    class FakeSymlink:
        parent = FakeParent()

        def resolve(self):
            return self

        def is_symlink(self) -> bool:
            return True

        def readlink(self) -> Path:
            return Path("target.db")

    resolved = _resolve_symlinks_safely(FakeSymlink())  # type: ignore[arg-type]

    assert resolved == target.resolve()


def test_resolve_symlinks_safely_returns_partial_path_on_inner_read_error() -> None:
    class BrokenSymlink:
        parent = Path(".")

        def resolve(self):
            return self

        def is_symlink(self) -> bool:
            return True

        def readlink(self) -> Path:
            raise OSError("cannot read link")

    link = BrokenSymlink()

    assert _resolve_symlinks_safely(link) is link  # type: ignore[arg-type]
