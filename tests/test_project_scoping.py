"""Tests for SimpleBroker project scoping functionality.

This module tests the project scoping features including:
- Environment variable parsing and defaults
- Project scope search algorithm
- Security boundary validation
- CLI integration and precedence
- Error handling and edge cases
- Cross-platform compatibility

Important: All broker databases are properly cleaned up before temp directories
are reclaimed to prevent test failures on Windows.
"""

import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing
from pathlib import Path

import pytest

from simplebroker._constants import DEFAULT_DB_NAME
from simplebroker._paths import (
    _find_project_database,
    _is_ancestor_of_working_directory,
    _is_filesystem_root,
    _is_valid_sqlite_db,
)
from simplebroker.cli import (
    _resolve_database_path,
)
from simplebroker.db import BrokerDB

from .conftest import run_cli

TempDBCleanup = tuple[Path, Callable[[], None]]


class TestFilesystemBoundaryDetection:
    """Test filesystem boundary detection for security."""

    def test_filesystem_root_detection_unix(self) -> None:
        """Test detection of Unix filesystem root."""
        root_path = Path("/")
        assert _is_filesystem_root(root_path) is True

    def test_filesystem_root_detection_windows(self) -> None:
        """The real current drive root is recognized on Windows.

        Runs the real _is_filesystem_root against Path(Path.cwd().anchor)
        — no os.name patch, no mocked resolve, no invented drive letters
        (owner review: the old version was a mock-returns-mock shell off
        Windows and this is the live coverage on the Windows CI leg).
        """
        import sys

        if sys.platform != "win32":
            pytest.skip("Windows-specific test")

        drive_root = Path(Path.cwd().anchor)
        assert _is_filesystem_root(drive_root) is True

    def test_home_directory_not_boundary(self) -> None:
        """Test that traversal does NOT stop at user home directory (removed restriction)."""
        assert _is_filesystem_root(Path("/home/testuser")) is False

    def test_normal_directory_not_boundary(self) -> None:
        """Test that normal directories are not boundaries."""
        assert _is_filesystem_root(Path("/tmp/test/path")) is False


class TestDatabaseValidation:
    """Test SimpleBroker database validation during search."""

    def test_valid_simplebroker_db(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test recognition of valid SimpleBroker database."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create a valid SimpleBroker database
        db_path = tmp_path / ".broker.db"
        with BrokerDB(str(db_path)):
            pass  # Database created and initialized

        try:
            assert _is_valid_sqlite_db(db_path) is True
        finally:
            cleanup_func()

    def test_invalid_database_file(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test rejection of invalid database files."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create a non-SQLite file
        fake_db = tmp_path / ".broker.db"
        fake_db.write_text("This is not a database")

        try:
            assert _is_valid_sqlite_db(fake_db) is False
        finally:
            cleanup_func()

    def test_nonexistent_file(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test handling of nonexistent files."""
        tmp_path, cleanup_func = temp_db_cleanup

        nonexistent = tmp_path / "nonexistent.db"
        try:
            assert _is_valid_sqlite_db(nonexistent) is False
        finally:
            cleanup_func()

    def test_wrong_magic_string(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test rejection of SQLite databases with wrong magic string."""
        import sqlite3

        tmp_path, cleanup_func = temp_db_cleanup

        db_path = tmp_path / ".broker.db"
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('magic', 'wrong-magic')"
            )
            conn.commit()

        try:
            assert _is_valid_sqlite_db(db_path) is False
        finally:
            cleanup_func()


class TestProjectDatabaseSearch:
    """Test upward directory traversal for project databases."""

    def test_find_database_in_current_directory(
        self, temp_db_cleanup: TempDBCleanup
    ) -> None:
        """Test finding database in current directory."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create valid database in current directory
        db_path = tmp_path / ".broker.db"
        with BrokerDB(str(db_path)):
            pass

        try:
            result = _find_project_database(".broker.db", tmp_path)
            assert result == db_path.resolve()
        finally:
            cleanup_func()

    def test_find_database_in_parent_directory(
        self, temp_db_cleanup: TempDBCleanup
    ) -> None:
        """Test finding database in parent directory."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create directory structure: tmp_path/.broker.db and tmp_path/sub/
        parent_db = tmp_path / ".broker.db"
        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()

        # Create valid database in parent
        with BrokerDB(str(parent_db)):
            pass

        try:
            # Search from subdirectory
            result = _find_project_database(".broker.db", sub_dir)
            assert result == parent_db.resolve()
        finally:
            cleanup_func()

    def test_find_database_multiple_levels_up(
        self, temp_db_cleanup: TempDBCleanup
    ) -> None:
        """Test finding database multiple directory levels up."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create: tmp_path/.broker.db and tmp_path/a/b/c/
        root_db = tmp_path / ".broker.db"
        deep_dir = tmp_path / "a" / "b" / "c"
        deep_dir.mkdir(parents=True)

        with BrokerDB(str(root_db)):
            pass

        try:
            result = _find_project_database(".broker.db", deep_dir)
            assert result == root_db.resolve()
        finally:
            cleanup_func()

    def test_find_database_stops_before_mount_boundary(
        self, temp_db_cleanup: TempDBCleanup, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        tmp_path, cleanup_func = temp_db_cleanup
        parent_db = tmp_path / ".broker.db"
        nested = tmp_path / "nested"
        nested.mkdir()
        with BrokerDB(str(parent_db)):
            pass
        monkeypatch.setattr(
            "simplebroker._paths._same_filesystem",
            lambda current, parent: current != nested.resolve(),
        )

        try:
            assert _find_project_database(".broker.db", nested) is None
        finally:
            cleanup_func()

    def test_no_database_found(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test behavior when no database is found in hierarchy."""
        tmp_path, cleanup_func = temp_db_cleanup

        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()

        try:
            result = _find_project_database(".broker.db", sub_dir)
            assert result is None
        finally:
            cleanup_func()

    def test_max_depth_limit(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test that search respects maximum depth limit."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create deep directory structure (but not too deep to avoid filesystem limits)
        current = tmp_path
        for i in range(50):  # Create 50 levels
            current = current / f"level{i}"
            current.mkdir()

        # Create database at root
        root_db = tmp_path / ".broker.db"
        with BrokerDB(str(root_db)):
            pass

        try:
            # Search should find database even at depth, but let's test with smaller depth limit
            result = _find_project_database(".broker.db", current, max_depth=10)
            assert result is None  # Should fail due to depth limit

            # Should find with larger limit
            result = _find_project_database(".broker.db", current, max_depth=100)
            assert result == root_db.resolve()
        finally:
            cleanup_func()

    def test_permission_denied_skipped(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test that directories with permission issues are skipped."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create database and directory structure
        parent_db = tmp_path / ".broker.db"
        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()

        with BrokerDB(str(parent_db)):
            pass

        if os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0):
            pytest.skip("chmod-based access denial needs a non-root POSIX user")
        try:
            # Real permission denial instead of a global os.access patch:
            # an unreadable candidate database is treated as invalid and
            # skipped by the upward search.
            parent_db.chmod(0o000)
            try:
                result = _find_project_database(".broker.db", sub_dir)
                assert result is None
            finally:
                parent_db.chmod(0o644)
        finally:
            cleanup_func()


class TestDatabasePathResolution:
    """Test complete database path resolution with precedence rules."""

    def test_absolute_cli_flag_precedence(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test that absolute -f flag takes highest precedence."""
        import argparse

        tmp_path, cleanup_func = temp_db_cleanup

        abs_path = tmp_path / "absolute.db"
        args = argparse.Namespace(file=str(abs_path), dir=Path.cwd(), command="write")
        absolute_path = os.sep.join([os.sep + "some", "otherpath"])
        config = {
            "BROKER_DEFAULT_DB_LOCATION": absolute_path,
            "BROKER_DEFAULT_DB_NAME": "other.db",
            "BROKER_PROJECT_SCOPE": True,
        }

        try:
            result_path, used_scope = _resolve_database_path(args, config=config)
            assert result_path == abs_path
            assert used_scope is False
        finally:
            cleanup_func()

    def test_project_scope_precedence_over_env_defaults(
        self, temp_db_cleanup: TempDBCleanup
    ) -> None:
        """Test that project scoping beats environment defaults."""
        import argparse

        tmp_path, cleanup_func = temp_db_cleanup

        # Set up directory structure with project database
        project_db = tmp_path / ".broker.db"
        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()

        with BrokerDB(str(project_db)):
            pass

        try:
            args = argparse.Namespace(
                file=DEFAULT_DB_NAME,  # Not absolute
                dir=sub_dir,  # real discovery starts here and walks up
                command="write",
            )
            absolute_path = os.sep.join([os.sep + "env", "default", "path"])
            config = {
                "BROKER_DEFAULT_DB_LOCATION": absolute_path,
                "BROKER_DEFAULT_DB_NAME": DEFAULT_DB_NAME,
                "BROKER_PROJECT_SCOPE": True,
            }

            # Real project discovery: the fixture tree holds the database,
            # so precedence is proven against actual search, not a canned
            # mock answer (audit finding — the mocked version could not
            # catch a discovery regression).
            result_path, used_scope = _resolve_database_path(args, config=config)
            assert result_path == project_db
            assert used_scope is True
        finally:
            cleanup_func()

    def test_env_defaults_fallback(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test environment defaults when project scoping disabled."""
        import argparse

        tmp_path, cleanup_func = temp_db_cleanup

        args = argparse.Namespace(file=DEFAULT_DB_NAME, dir=Path.cwd(), command="write")

        config = {
            "BROKER_DEFAULT_DB_LOCATION": str(tmp_path / "env"),
            "BROKER_DEFAULT_DB_NAME": "env.db",
            "BROKER_PROJECT_SCOPE": False,
        }

        try:
            result_path, used_scope = _resolve_database_path(args, config=config)
            expected = Path(tmp_path) / "env" / "env.db"
            assert result_path == expected
            assert used_scope is False
        finally:
            cleanup_func()

    def test_project_scope_not_found_error(
        self, temp_db_cleanup: TempDBCleanup
    ) -> None:
        """Test error when project scoping enabled but no database found."""
        import argparse

        _tmp_path, cleanup_func = temp_db_cleanup

        args = argparse.Namespace(file="missing.db", dir=Path.cwd(), command="write")

        config = {
            "BROKER_DEFAULT_DB_LOCATION": "",
            "BROKER_DEFAULT_DB_NAME": "missing.db",
            "BROKER_PROJECT_SCOPE": True,
        }

        try:
            with pytest.raises(ValueError) as exc_info:
                _resolve_database_path(args, config=config)

            assert "BROKER_PROJECT_SCOPE is enabled" in str(exc_info.value)
            assert "Run 'broker init'" in str(exc_info.value)
        finally:
            cleanup_func()

    def test_init_command_bypasses_project_scope(
        self, temp_db_cleanup: TempDBCleanup
    ) -> None:
        """Test that init command never uses project scoping."""
        import argparse

        tmp_path, cleanup_func = temp_db_cleanup

        # Create project database in parent
        parent_db = tmp_path / ".broker.db"
        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()

        with BrokerDB(str(parent_db)):
            pass

        try:
            args = argparse.Namespace(
                file=DEFAULT_DB_NAME,
                dir=sub_dir,
                command="init",  # Special case
            )

            config = {
                "BROKER_DEFAULT_DB_LOCATION": "",
                "BROKER_DEFAULT_DB_NAME": DEFAULT_DB_NAME,
                "BROKER_PROJECT_SCOPE": True,
            }

            result_path, used_scope = _resolve_database_path(args, config=config)
            expected = sub_dir / DEFAULT_DB_NAME
            assert result_path == expected
            assert used_scope is False  # Project scope not used for init
        finally:
            cleanup_func()


class TestAncestorValidation:
    """Test ancestor validation for project scoped paths."""

    def test_valid_ancestor_relationship(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test valid ancestor relationship validation."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create directory structure
        parent_dir = tmp_path / "parent"
        child_dir = parent_dir / "child" / "subchild"
        child_dir.mkdir(parents=True)

        parent_db = parent_dir / ".broker.db"

        try:
            result = _is_ancestor_of_working_directory(parent_db, child_dir)
            assert result is True
        finally:
            cleanup_func()

    def test_invalid_sibling_relationship(self, temp_db_cleanup: TempDBCleanup) -> None:
        """Test that sibling directories are not considered ancestors."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create directory structure
        sibling1 = tmp_path / "sibling1"
        sibling2 = tmp_path / "sibling2"
        sibling1.mkdir()
        sibling2.mkdir()

        sibling1_db = sibling1 / ".broker.db"

        try:
            result = _is_ancestor_of_working_directory(sibling1_db, sibling2)
            assert result is False
        finally:
            cleanup_func()


class TestCLIIntegration:
    """Test CLI integration with project scoping."""

    @pytest.mark.sqlite_only
    def test_init_command_dispatch(self, temp_db_cleanup: TempDBCleanup) -> None:
        """init dispatches through the real CLI process (folds the old
        parser-accepts-init near-tautology into a behavioral check).

        sqlite_only: asserts the on-disk ``.broker.db`` artifact, which a
        remote-backend ``init`` legitimately never creates."""
        tmp_path, cleanup_func = temp_db_cleanup
        try:
            rc, _out, err = run_cli("--quiet", "init", cwd=tmp_path)
            assert rc == 0, err
            assert (tmp_path / ".broker.db").exists()
        finally:
            cleanup_func()


@pytest.fixture
def temp_db_cleanup(tmp_path: Path) -> Iterator[TempDBCleanup]:
    """pytest-managed temp dir plus an idempotent database-close helper.

    Teardown always runs via the fixture (the old hand-rolled mkdtemp
    leaked the directory whenever a test errored before its own finally);
    the yielded cleanup callable remains for call sites and for Windows,
    where databases must be closed before removal.
    """

    def cleanup_func() -> None:
        for db_file in tmp_path.rglob("*.db"):
            try:
                conn = sqlite3.connect(str(db_file), timeout=1.0)
                conn.close()
            except (sqlite3.Error, OSError):
                pass
            try:
                db_file.unlink(missing_ok=True)
            except (OSError, PermissionError):
                pass

    try:
        yield tmp_path, cleanup_func
    finally:
        cleanup_func()


class TestSecurityEdgeCases:
    """Test security edge cases and attack vectors."""

    def test_database_validation_prevents_foreign_dbs(
        self, temp_db_cleanup: TempDBCleanup
    ) -> None:
        """Test that only SimpleBroker databases are used."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create a non-SimpleBroker SQLite database
        import sqlite3

        fake_db = tmp_path / ".broker.db"
        with closing(sqlite3.connect(str(fake_db))) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.commit()

        try:
            # Should be rejected as not a valid SimpleBroker database
            assert _is_valid_sqlite_db(fake_db) is False
        finally:
            cleanup_func()
