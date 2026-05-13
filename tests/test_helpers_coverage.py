"""Focused coverage for helper edge cases."""

from __future__ import annotations

from pathlib import Path

import pytest

from simplebroker import helpers
from simplebroker._exceptions import OperationalError, StopException
from simplebroker.helpers import (
    _execute_with_retry,
    _find_project_database,
    _is_compound_db_name,
    _is_filesystem_root,
    _resolve_symlinks_safely,
    _validate_database_parent_directory,
    _validate_path_containment,
    _validate_working_directory,
    ensure_compound_db_path,
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
