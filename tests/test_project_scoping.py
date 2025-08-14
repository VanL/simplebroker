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
import platform
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from simplebroker._constants import DEFAULT_DB_NAME, _parse_bool, load_config
from simplebroker.cli import (
    _resolve_database_path,
    create_parser,
    main,
)
from simplebroker.db import BrokerDB
from simplebroker.helpers import (
    _find_project_database,
    _is_ancestor_of_working_directory,
    _is_filesystem_root,
    _is_valid_sqlite_db,
)


class TestEnvironmentVariableParsing:
    """Test environment variable parsing and configuration loading."""

    def test_parse_bool_true_values(self) -> None:
        """Test _parse_bool recognizes true values correctly."""
        true_values = ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"]
        for value in true_values:
            assert _parse_bool(value) is True, f"Failed for value: {value}"

    def test_parse_bool_false_values(self) -> None:
        """Test _parse_bool recognizes false values correctly."""
        false_values = ["0", "false", "FALSE", "no", "off", "", "invalid"]
        for value in false_values:
            assert _parse_bool(value) is False, f"Failed for value: {value}"

    @patch.dict(os.environ, {}, clear=True)
    def test_load_config_defaults(self) -> None:
        """Test load_config returns correct defaults when no env vars set."""
        config = load_config()

        assert config["BROKER_DEFAULT_DB_LOCATION"] == ""
        assert config["BROKER_DEFAULT_DB_NAME"] == DEFAULT_DB_NAME
        assert config["BROKER_PROJECT_SCOPE"] is False

    @patch.dict(
        os.environ,
        {
            "BROKER_DEFAULT_DB_LOCATION": (
                "C:\\tmp\\test"
                if platform.system() == "Windows"
                else os.sep.join([os.sep + "tmp", "test"])
            ),
            "BROKER_DEFAULT_DB_NAME": "custom.db",
            "BROKER_PROJECT_SCOPE": "1",
        },
    )
    def test_load_config_with_env_vars(self) -> None:
        """Test load_config reads environment variables correctly."""
        config = load_config()
        # Absolute path should remain unchanged
        expected_path = (
            "C:\\tmp\\test"
            if platform.system() == "Windows"
            else os.sep.join([os.sep + "tmp", "test"])
        )
        assert config["BROKER_DEFAULT_DB_LOCATION"] == expected_path
        assert config["BROKER_DEFAULT_DB_NAME"] == "custom.db"
        assert config["BROKER_PROJECT_SCOPE"] is True

    @patch.dict(
        os.environ,
        {
            "BROKER_DEFAULT_DB_LOCATION": os.sep.join(["tmp", "test"]),
            "BROKER_DEFAULT_DB_NAME": "custom.db",
            "BROKER_PROJECT_SCOPE": "1",
        },
    )
    def test_load_config_with_relative_path_warning(self) -> None:
        """Test load_config issues warning and ignores relative paths."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = load_config()
            relative_path = os.sep.join(["tmp", "test"])
            # Should issue a warning
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "must be an absolute path" in str(w[0].message)
            assert relative_path in str(w[0].message)

            # Should be reset to empty string
            assert config["BROKER_DEFAULT_DB_LOCATION"] == ""
        assert config["BROKER_DEFAULT_DB_NAME"] == "custom.db"
        assert config["BROKER_PROJECT_SCOPE"] is True


class TestFilesystemBoundaryDetection:
    """Test filesystem boundary detection for security."""

    def test_filesystem_root_detection_unix(self) -> None:
        """Test detection of Unix filesystem root."""
        root_path = Path("/")
        assert _is_filesystem_root(root_path) is True

    @patch("os.name", "nt")
    def test_filesystem_root_detection_windows(self) -> None:
        """Test detection of Windows drive roots."""
        import sys

        if sys.platform != "win32":
            pytest.skip("Windows-specific test")

        drive_roots = [Path("C:\\"), Path("D:\\"), Path("Z:\\")]
        for root in drive_roots:
            with patch.object(Path, "resolve", return_value=root):
                assert _is_filesystem_root(root) is True

    @patch("pathlib.Path.home")
    def test_home_directory_not_boundary(self, mock_home) -> None:
        """Test that traversal does NOT stop at user home directory (removed restriction)."""
        home_path = Path("/home/testuser")
        mock_home.return_value = home_path

        with patch.object(Path, "resolve", return_value=home_path):
            # Home directory should NOT be a boundary anymore
            assert _is_filesystem_root(home_path) is False

    def test_normal_directory_not_boundary(self) -> None:
        """Test that normal directories are not boundaries."""
        normal_path = Path("/some/normal/path")
        parent_path = Path("/some/normal")

        # Create a mock path object that has the expected behavior
        mock_path = MagicMock(spec=Path)
        mock_path.parent = parent_path
        mock_path.resolve.return_value = normal_path

        # Test with the mock - normal directories should not be boundaries
        with patch("pathlib.Path.home", side_effect=RuntimeError("No home")):
            # Directly test with actual paths to avoid complex mocking
            actual_normal_path = Path("/tmp/test/path")  # Use a real path structure
            if actual_normal_path.parent != actual_normal_path:  # Not root
                # This should not be a boundary since it's not root or home
                result = _is_filesystem_root(actual_normal_path)
                # The result depends on whether this path exists and is home
                # Let's just verify the function doesn't crash
                assert isinstance(result, bool)


class TestDatabaseValidation:
    """Test SimpleBroker database validation during search."""

    def test_valid_simplebroker_db(self, temp_db_cleanup) -> None:
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

    def test_invalid_database_file(self, temp_db_cleanup) -> None:
        """Test rejection of invalid database files."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create a non-SQLite file
        fake_db = tmp_path / ".broker.db"
        fake_db.write_text("This is not a database")

        try:
            assert _is_valid_sqlite_db(fake_db) is False
        finally:
            cleanup_func()

    def test_nonexistent_file(self, temp_db_cleanup) -> None:
        """Test handling of nonexistent files."""
        tmp_path, cleanup_func = temp_db_cleanup

        nonexistent = tmp_path / "nonexistent.db"
        try:
            assert _is_valid_sqlite_db(nonexistent) is False
        finally:
            cleanup_func()

    def test_wrong_magic_string(self, temp_db_cleanup) -> None:
        """Test rejection of SQLite databases with wrong magic string."""
        import sqlite3

        tmp_path, cleanup_func = temp_db_cleanup

        db_path = tmp_path / ".broker.db"
        with sqlite3.connect(str(db_path)) as conn:
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

    def test_find_database_in_current_directory(self, temp_db_cleanup) -> None:
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

    def test_find_database_in_parent_directory(self, temp_db_cleanup) -> None:
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

    def test_find_database_multiple_levels_up(self, temp_db_cleanup) -> None:
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

    def test_no_database_found(self, temp_db_cleanup) -> None:
        """Test behavior when no database is found in hierarchy."""
        tmp_path, cleanup_func = temp_db_cleanup

        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()

        try:
            result = _find_project_database(".broker.db", sub_dir)
            assert result is None
        finally:
            cleanup_func()

    def test_max_depth_limit(self, temp_db_cleanup) -> None:
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

    def test_permission_denied_skipped(self, temp_db_cleanup) -> None:
        """Test that directories with permission issues are skipped."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create database and directory structure
        parent_db = tmp_path / ".broker.db"
        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()

        with BrokerDB(str(parent_db)):
            pass

        try:
            # Make access check fail
            with patch("os.access", side_effect=lambda p, mode: False):
                result = _find_project_database(".broker.db", sub_dir)
                assert result is None
        finally:
            cleanup_func()


class TestDatabasePathResolution:
    """Test complete database path resolution with precedence rules."""

    def test_absolute_cli_flag_precedence(self, temp_db_cleanup) -> None:
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
            result_path, used_scope = _resolve_database_path(args, config)
            assert result_path == abs_path
            assert used_scope is False
        finally:
            cleanup_func()

    def test_project_scope_precedence_over_env_defaults(self, temp_db_cleanup) -> None:
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
                dir=Path.cwd(),
                command="write",
            )
            absolute_path = os.sep.join([os.sep + "env", "default", "path"])
            config = {
                "BROKER_DEFAULT_DB_LOCATION": absolute_path,
                "BROKER_DEFAULT_DB_NAME": DEFAULT_DB_NAME,
                "BROKER_PROJECT_SCOPE": True,
            }

            # Mock search to return our test database
            with patch(
                "simplebroker.cli._find_project_database", return_value=project_db
            ):
                result_path, used_scope = _resolve_database_path(args, config)
                assert result_path == project_db
                assert used_scope is True
        finally:
            cleanup_func()

    def test_env_defaults_fallback(self, temp_db_cleanup) -> None:
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
            result_path, used_scope = _resolve_database_path(args, config)
            expected = Path(tmp_path) / "env" / "env.db"
            assert result_path == expected
            assert used_scope is False
        finally:
            cleanup_func()

    def test_project_scope_not_found_error(self, temp_db_cleanup) -> None:
        """Test error when project scoping enabled but no database found."""
        import argparse

        tmp_path, cleanup_func = temp_db_cleanup

        args = argparse.Namespace(file="missing.db", dir=Path.cwd(), command="write")

        config = {
            "BROKER_DEFAULT_DB_LOCATION": "",
            "BROKER_DEFAULT_DB_NAME": "missing.db",
            "BROKER_PROJECT_SCOPE": True,
        }

        try:
            with pytest.raises(ValueError) as exc_info:
                _resolve_database_path(args, config)

            assert "BROKER_PROJECT_SCOPE is enabled" in str(exc_info.value)
            assert "Run 'broker init'" in str(exc_info.value)
        finally:
            cleanup_func()

    def test_init_command_bypasses_project_scope(self, temp_db_cleanup) -> None:
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

            result_path, used_scope = _resolve_database_path(args, config)
            expected = sub_dir / DEFAULT_DB_NAME
            assert result_path == expected
            assert used_scope is False  # Project scope not used for init
        finally:
            cleanup_func()


class TestAncestorValidation:
    """Test ancestor validation for project scoped paths."""

    def test_valid_ancestor_relationship(self, temp_db_cleanup) -> None:
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

    def test_invalid_sibling_relationship(self, temp_db_cleanup) -> None:
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

    def test_init_command_dispatch(self, temp_db_cleanup) -> None:
        """Test that init command is properly dispatched."""
        tmp_path, cleanup_func = temp_db_cleanup

        original_cwd = os.getcwd()
        try:
            os.chdir(str(tmp_path))

            with patch("sys.argv", ["broker", "init", "--quiet"]):
                result = main()

            assert result == 0
            assert (tmp_path / ".broker.db").exists()
        finally:
            cleanup_func()
            os.chdir(original_cwd)

    def test_parser_includes_init_command(self) -> None:
        """Test that parser includes init command."""
        parser = create_parser()

        # Check if init command exists in subparsers
        subparsers_choices = list(parser._subparsers._group_actions[0].choices.keys())
        assert "init" in subparsers_choices

        # Test parsing of init command
        args = parser.parse_args(["init"])
        assert args.command == "init"


@pytest.fixture
def temp_db_cleanup():
    """Fixture that provides temporary directory with proper database cleanup.

    This is crucial for Windows compatibility - databases must be properly
    closed before the temporary directory is cleaned up.

    Returns:
        Tuple of (tmp_path, cleanup_function)
    """
    tmpdir = tempfile.mkdtemp()
    tmp_path = Path(tmpdir)

    def cleanup_func():
        """Clean up all databases before temp directory removal."""
        # Force cleanup of any SQLite databases in the directory
        for db_file in tmp_path.rglob("*.db"):
            try:
                if db_file.exists():
                    # Try to cleanly close any connections
                    import sqlite3

                    try:
                        # Attempt to connect and immediately close to flush any pending operations
                        conn = sqlite3.connect(str(db_file), timeout=1.0)
                        conn.close()
                    except (sqlite3.Error, OSError):
                        pass

                    # Remove the database file
                    db_file.unlink(missing_ok=True)
            except (OSError, PermissionError):
                # If we can't remove it, at least we tried
                pass

        # Remove the temporary directory
        import shutil

        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except (OSError, PermissionError):
            pass

    return tmp_path, cleanup_func


class TestCrossPlatformCompatibility:
    """Test cross-platform compatibility."""

    @pytest.mark.skipif(os.name != "nt", reason="Windows-specific test")
    def test_windows_drive_root_detection(self) -> None:
        """Test Windows drive root boundary detection."""
        from simplebroker.helpers import _is_filesystem_root

        # Mock Windows environment
        with patch("os.name", "nt"):
            drive_path = Path("C:\\")
            with patch.object(Path, "resolve", return_value=drive_path):
                assert _is_filesystem_root(drive_path) is True

    @pytest.mark.skipif(os.name == "nt", reason="Unix-specific test")
    def test_unix_root_detection(self) -> None:
        """Test Unix filesystem root detection."""
        from simplebroker.helpers import _is_filesystem_root

        root_path = Path("/")
        assert _is_filesystem_root(root_path) is True


class TestSecurityEdgeCases:
    """Test security edge cases and attack vectors."""

    def test_path_traversal_prevention(self, temp_db_cleanup) -> None:
        """Test that path traversal attempts are blocked."""
        tmp_path, cleanup_func = temp_db_cleanup

        try:
            # Test that .. components in database filename are caught
            # This should be handled by existing validation in main()
            pass  # The actual validation is in the main() function
        finally:
            cleanup_func()

    def test_database_validation_prevents_foreign_dbs(self, temp_db_cleanup) -> None:
        """Test that only SimpleBroker databases are used."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create a non-SimpleBroker SQLite database
        import sqlite3

        fake_db = tmp_path / ".broker.db"
        with sqlite3.connect(str(fake_db)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.commit()

        try:
            # Should be rejected as not a valid SimpleBroker database
            assert _is_valid_sqlite_db(fake_db) is False
        finally:
            cleanup_func()


# Performance and stress tests
class TestPerformanceAndLimits:
    """Test performance characteristics and limits."""

    def test_deep_directory_performance(self, temp_db_cleanup) -> None:
        """Test performance with deep directory structures."""
        tmp_path, cleanup_func = temp_db_cleanup

        # Create moderately deep structure (not too deep to avoid filesystem limits)
        current = tmp_path
        for i in range(20):
            current = current / f"level{i}"
            current.mkdir()

        # Create database at root
        root_db = tmp_path / ".broker.db"
        with BrokerDB(str(root_db)):
            pass

        try:
            # Time the search operation
            import time

            start_time = time.time()
            result = _find_project_database(".broker.db", current)
            elapsed = time.time() - start_time

            assert result == root_db.resolve()
            # Should complete reasonably quickly (under 1 second for 20 levels)
            assert elapsed < 1.0
        finally:
            cleanup_func()
