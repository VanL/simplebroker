"""Focused coverage for path-security and project-discovery edge cases."""
# mypy: disable-error-code=no-untyped-def

from __future__ import annotations

from pathlib import Path

import pytest

from simplebroker import _paths
from simplebroker._exceptions import _ArgumentValidationError
from simplebroker._paths import (
    _create_compound_db_directories,
    _find_project_database,
    _is_compound_db_name,
    _is_filesystem_root,
    _resolve_symlinks_safely,
    _validate_database_parent_directory,
    _validate_path_containment,
    _validate_working_directory,
    ensure_compound_db_path,
    is_ancestor,
)


def test_validate_working_directory_reports_missing_and_file_paths(
    tmp_path: Path,
) -> None:
    with pytest.raises(_ArgumentValidationError, match="Directory not found"):
        _validate_working_directory(tmp_path / "missing")

    file_path = tmp_path / "not-a-dir"
    file_path.write_text("", encoding="utf-8")
    with pytest.raises(_ArgumentValidationError, match="Path is a file"):
        _validate_working_directory(file_path)


def test_filesystem_root_and_ancestor_helpers(tmp_path: Path) -> None:
    assert _is_filesystem_root(Path("/")) is True
    child = tmp_path / "parent" / "child"
    child.mkdir(parents=True)

    assert is_ancestor(tmp_path / "parent", child) is True
    assert is_ancestor(child, tmp_path / "parent") is False


def test_find_project_database_rejects_missing_start_and_obeys_depth(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="Starting directory does not exist"):
        _find_project_database(".broker.db", tmp_path / "missing")

    root = tmp_path / "root"
    nested = root / "a" / "b"
    nested.mkdir(parents=True)
    candidate = root / ".broker.db"
    # A real broker database, so real validity checking runs (no mock).
    from simplebroker.db import BrokerDB

    with BrokerDB(str(candidate)):
        pass

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
    with pytest.raises(_ArgumentValidationError, match="Parent directory not found"):
        _validate_database_parent_directory(tmp_path / "missing" / "broker.db")


def test_validate_database_parent_directory_rejects_inaccessible_parent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "broker.db"

    monkeypatch.setattr(_paths.os, "access", lambda path, mode: False)
    with pytest.raises(ValueError, match="not accessible") as inaccessible:
        _validate_database_parent_directory(db_path)
    assert not isinstance(inaccessible.value, _ArgumentValidationError)

    def writable_but_not_executable(path: Path, mode: int) -> bool:
        return mode != _paths.os.W_OK

    monkeypatch.setattr(_paths.os, "access", writable_but_not_executable)
    with pytest.raises(ValueError, match="not writable") as unwritable:
        _validate_database_parent_directory(db_path)
    assert not isinstance(unwritable.value, _ArgumentValidationError)


def test_validate_path_containment_rejects_outside_db_and_bad_project_scope(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    outside = tmp_path / "outside.db"

    with pytest.raises(_ArgumentValidationError, match="within the working directory"):
        _validate_path_containment(outside, workdir, used_project_scope=False)

    sibling_db = tmp_path / "sibling" / "broker.db"
    sibling_db.parent.mkdir()
    with pytest.raises(_ArgumentValidationError, match="parent directory chain"):
        _validate_path_containment(sibling_db, workdir, used_project_scope=True)


def test_resolve_symlinks_safely_wraps_resolution_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_os_error(self: Path, *, strict: bool = False) -> Path:
        assert strict is False
        raise OSError("boom")

    monkeypatch.setattr(Path, "resolve", raise_os_error)

    with pytest.raises(RuntimeError, match="Failed to resolve symlinks"):
        _resolve_symlinks_safely(Path("broker.db"))


def test_resolve_symlinks_safely_resolves_absolute_unfinished_symlink(
    tmp_path: Path,
) -> None:
    """Models the Windows-only branch where Path.resolve(strict=False)
    leaves a symlink unresolved; POSIX resolve never takes it, so a fake
    duck-type is the only cross-platform driver (plan Task 5.3 retains
    exactly this pair with the branch named)."""
    target = tmp_path / "target.db"

    class FakeSymlink:
        parent = tmp_path

        def resolve(self, *, strict: bool = False):
            assert strict is False
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
    """Relative variant of the Windows-only unresolved-symlink branch."""
    target = tmp_path / "target.db"

    class FakeParent:
        def __truediv__(self, child: Path) -> Path:
            assert child == Path("target.db")
            return target

    class FakeSymlink:
        parent = FakeParent()

        def resolve(self, *, strict: bool = False):
            assert strict is False
            return self

        def is_symlink(self) -> bool:
            return True

        def readlink(self) -> Path:
            return Path("target.db")

    resolved = _resolve_symlinks_safely(FakeSymlink())  # type: ignore[arg-type]

    assert resolved == target.resolve()


def test_resolve_symlinks_safely_rejects_inner_read_error() -> None:
    class BrokenSymlink:
        parent = Path(".")

        def resolve(self, *, strict: bool = False):
            assert strict is False
            return self

        def is_symlink(self) -> bool:
            return True

        def readlink(self) -> Path:
            raise OSError("cannot read link")

    link = BrokenSymlink()

    with pytest.raises(RuntimeError, match="Failed to resolve symlinks"):
        _resolve_symlinks_safely(link)  # type: ignore[arg-type]

# Folded from the retired test_compound_db_names.py (audit Task 7.3);
# end-to-end compound behavior lives in test_queue_config_defaults.
@pytest.mark.parametrize(
    ("name", "expected_compound", "expected_parts"),
    [
        ("broker.db", False, []),
        ("some/name.db", True, ["some", "name.db"]),
        (".hidden/db.sqlite", True, [".hidden", "db.sqlite"]),
        ("some\\name.db", True, ["some", "name.db"]),
    ],
)
def test_is_compound_db_name_classification(
    name: str, expected_compound: bool, expected_parts: list[str]
) -> None:
    is_compound, parts = _is_compound_db_name(name)
    assert is_compound is expected_compound
    assert parts == expected_parts


@pytest.mark.parametrize(
    "name", [".weft/project/broker.db", ".config/app/queues/main.db"]
)
def test_is_compound_db_name_rejects_nested_directories(name: str) -> None:
    with pytest.raises(
        ValueError, match="Database name must not contain nested directories"
    ):
        _is_compound_db_name(name)

