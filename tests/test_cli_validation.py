"""Tests for CLI argument validation."""

import platform
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from simplebroker._constants import _validate_safe_path_components
from simplebroker.cli import main

from .conftest import run_cli
from .helper_scripts import create_dangerous_path


class TestCliArgumentValidation:
    """Test CLI directory errors and database-name validation."""

    def test_missing_directory_argument_is_clean_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A missing --dir is an invocation error before target creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_dir = Path(temp_dir) / "missing"

            with patch("sys.argv", ["simplebroker", "-d", str(missing_dir), "list"]):
                exit_code = main()
            captured = capsys.readouterr()
            assert exit_code == 1
            assert captured.out == ""
            assert "Directory not found" in captured.err
            assert "Traceback" not in captured.err
            assert not missing_dir.exists()

    def test_file_argument_validation(self) -> None:
        """Test that -f/--file argument is validated for dangerous characters."""
        dangerous_file = "test*file.db"

        with patch("sys.argv", ["simplebroker", "-f", dangerous_file, "list"]):
            exit_code = main()
            assert exit_code != 0  # Should fail with error

    def test_valid_directory_argument(self) -> None:
        """Test that valid directory arguments are accepted."""
        # Create a temporary directory for testing
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch("sys.argv", ["simplebroker", "-d", temp_dir, "list"]),
        ):
            exit_code = main()
            # The command might still fail because there's no database, but not due to validation
            assert exit_code in [
                0,
                2,
            ]  # 0 = success, 2 = queue empty (acceptable outcomes)

    def test_valid_file_argument(self) -> None:
        """Test that valid file arguments are accepted."""
        with tempfile.TemporaryDirectory() as temp_dir:
            valid_file = "valid_database.db"
            with patch(
                "sys.argv", ["simplebroker", "-d", temp_dir, "-f", valid_file, "list"]
            ):
                exit_code = main()
                # The command might fail because there's no database, but not due to validation
                assert exit_code in [
                    0,
                    2,
                ]  # 0 = success, 2 = queue empty (acceptable outcomes)

    @pytest.mark.skipif(
        platform.system() == "Windows", reason="Unix-specific shell chars"
    )
    def test_missing_posix_directory_paths_are_clean_errors(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Shell punctuation does not change the missing-directory error."""
        characters = ["|", "&", ";", "$", "`", '"', "'", "<", ">"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for char in characters:
                missing_path = create_dangerous_path(temp_dir, char)
                with patch("sys.argv", ["simplebroker", "-d", missing_path, "list"]):
                    exit_code = main()
                captured = capsys.readouterr()
                assert exit_code == 1
                assert captured.out == ""
                assert "Directory not found" in captured.err
                assert "Traceback" not in captured.err
                assert not Path(missing_path).exists()

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-specific test")
    def test_invalid_windows_directory_paths_are_clean_errors(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """OS-invalid directory spellings produce clean missing-directory errors."""
        characters = ["*", "?", '"', "<", ">", "|", ":"]

        with tempfile.TemporaryDirectory() as temp_dir:
            for char in characters:
                invalid_path = create_dangerous_path(temp_dir, char)
                with patch("sys.argv", ["simplebroker", "-d", invalid_path, "list"]):
                    exit_code = main()
                captured = capsys.readouterr()
                assert exit_code == 1
                assert captured.out == ""
                assert "Directory not found" in captured.err
                assert "Traceback" not in captured.err

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-specific test")
    def test_windows_drive_letters_allowed_in_paths(self) -> None:
        """Test that legitimate Windows drive letters are allowed in path arguments."""
        # Create temporary directories to test with actual drive letters
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test various drive letter formats that should be allowed
            # Use the actual drive letter from the temporary directory
            drive_letter = (
                str(temp_path).split(":")[0] if ":" in str(temp_path) else "C"
            )

            valid_drive_formats = [
                f"{drive_letter}:\\Windows\\Temp",
                f"{drive_letter}:\\Data",
                f"{drive_letter}:/Windows/Temp",  # Forward slashes
            ]

            for drive_path in valid_drive_formats:
                _validate_safe_path_components(drive_path)


class TestDatabaseTargetValidation:
    @pytest.mark.sqlite_only
    def test_invalid_database_is_clean_cli_error(self, workdir: Path) -> None:
        (workdir / ".broker.db").write_text("not a sqlite database", encoding="utf-8")

        rc, out, err = run_cli("read", "queue", "--after", "0", cwd=workdir)

        assert rc == 1
        assert out == ""
        assert "database" in err.lower()
        assert "valid" in err.lower()
        assert "traceback" not in err.lower()


@pytest.mark.parametrize(
    "args",
    [
        ("--help",),
        ("--version",),
        ("write", "jobs", "payload"),
        ("-f", "safe.db", "read", "jobs"),
    ],
)
def test_invalid_default_database_name_fails_before_parser(tmp_path, args):
    code, out, err = run_cli(
        *args,
        cwd=tmp_path,
        env={"BROKER_TEST_BACKEND": "sqlite", "BROKER_DEFAULT_DB_NAME": "100%.db"},
    )
    assert code == 1
    assert out == ""
    assert "ASCII" in err
    assert "Traceback" not in err
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "name",
    [
        "100%.db",
        "my dir/broker.db",
        "café.db",
        "dir//broker.db",
        "dir/./broker.db",
        "dir\\bad%.db",
    ],
)
def test_invalid_explicit_database_name_is_clean_json_error(tmp_path, name):
    import json

    code, out, err = run_cli(
        "-f",
        name,
        "write",
        "jobs",
        "payload",
        "--json",
        cwd=tmp_path,
        env={"BROKER_TEST_BACKEND": "sqlite"},
    )
    assert code == 1
    assert out == ""
    assert json.loads(err)["error"] == "INVALID_ARGUMENT"
    assert "ASCII" in err
    assert "Traceback" not in err
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("separator", ["/", "\\"])
def test_relative_cli_compound_database_name_normalizes_separators(tmp_path, separator):
    (tmp_path / "data").mkdir()
    name = f"data{separator}broker.db"
    env = {"BROKER_TEST_BACKEND": "sqlite"}
    code, out, err = run_cli(
        "-f", name, "write", "jobs", "payload", cwd=tmp_path, env=env
    )
    assert code == 0, err
    assert (tmp_path / "data" / "broker.db").exists()
    code, out, err = run_cli("-f", name, "read", "jobs", cwd=tmp_path, env=env)
    assert code == 0, err
    assert out == "payload"
