"""Tests for path security validation functions."""

import ast
import json
import os
import platform
import sqlite3
from pathlib import Path
from typing import ClassVar, cast

import pytest

from simplebroker import _constants
from simplebroker._paths import _validate_safe_path_components

from .conftest import run_cli


class _ShellSinkVisitor(ast.NodeVisitor):
    """Find runtime calls that could reinterpret an admitted path as shell code."""

    _SUBPROCESS_CALLS: ClassVar[set[str]] = {
        "Popen",
        "call",
        "check_call",
        "check_output",
        "run",
    }

    def __init__(self) -> None:
        self.module_aliases = {"os": "os", "subprocess": "subprocess"}
        self.direct_calls: dict[str, tuple[str, str]] = {}
        self.lines: list[int] = []

    def visit_Import(self, node: ast.Import) -> None:
        for imported in node.names:
            if imported.name in self.module_aliases:
                self.module_aliases[imported.asname or imported.name] = imported.name

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module in {"os", "subprocess"}:
            for imported in node.names:
                self.direct_calls[imported.asname or imported.name] = (
                    node.module,
                    imported.name,
                )

    def _call_target(self, node: ast.Call) -> tuple[str | None, str | None]:
        if isinstance(node.func, ast.Attribute) and isinstance(
            node.func.value, ast.Name
        ):
            return self.module_aliases.get(node.func.value.id), node.func.attr
        if isinstance(node.func, ast.Name):
            return self.direct_calls.get(node.func.id, (None, None))
        return None, None

    def visit_Call(self, node: ast.Call) -> None:
        owner, name = self._call_target(node)
        shell_interpreting = owner == "os" and name in {"popen", "system"}
        if owner == "subprocess" and name in self._SUBPROCESS_CALLS:
            shell_keyword = next(
                (keyword.value for keyword in node.keywords if keyword.arg == "shell"),
                None,
            )
            shell_interpreting = shell_keyword is not None and not (
                isinstance(shell_keyword, ast.Constant) and shell_keyword.value is False
            )
        if shell_interpreting:
            self.lines.append(node.lineno)
        self.generic_visit(node)


def test_runtime_has_no_shell_interpreting_path_sink() -> None:
    """POSIX path widening remains safe only while runtime subprocesses stay literal."""

    package_root = Path(_constants.__file__).parent
    offenders: list[str] = []
    for source_path in package_root.rglob("*.py"):
        visitor = _ShellSinkVisitor()
        visitor.visit(ast.parse(source_path.read_text(encoding="utf-8")))
        offenders.extend(
            f"{source_path.relative_to(package_root)}:{line}" for line in visitor.lines
        )
    assert offenders == []


class TestValidateSafePathComponents:
    """Test the _validate_safe_path_components security function."""

    def test_valid_simple_paths(self) -> None:
        """Test that valid simple paths pass validation."""
        valid_paths = [
            "broker.db",
            "test.db",
            "my-database.db",
            "database_v2.db",
            "app.sqlite",
            "data123.db",
        ]

        for path in valid_paths:
            # Should not raise any exception
            _validate_safe_path_components(path, "Test path")

    def test_valid_compound_paths(self) -> None:
        """Test that valid compound paths pass validation."""
        valid_paths = [
            "subdir/broker.db",
            ".config/app.db",
            "data-folder/database.db",
            "app_data/broker.sqlite",
        ]

        for path in valid_paths:
            # Should not raise any exception
            _validate_safe_path_components(path, "Test path")

    def test_null_byte_rejection(self) -> None:
        """Test that null bytes are rejected."""
        dangerous_paths = [
            "test\0.db",
            "sub\0dir/test.db",
            "\0",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_control_characters_rejection(self) -> None:
        """Test that control characters are rejected."""
        dangerous_paths = [
            "test\x01.db",
            "test\x08.db",
            "test\r.db",
            "test\n.db",
            "test\t.db",
            "test\x1b.db",
            "test\x1f.db",
            "test\x7f.db",
            "test\u0085.db",
            "test\u009b.db",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_parent_directory_rejection(self) -> None:
        """Test that parent directory references are rejected."""
        dangerous_paths = [
            "..",
            "../test.db",
            "subdir/../test.db",
            "../../../etc/passwd",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="parent directory references"):
                _validate_safe_path_components(path, "Test path")

    def test_current_directory_rejection(self) -> None:
        """Test that current directory references are rejected."""
        dangerous_paths = [
            ".",
            "./test.db",
            "subdir/./test.db",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="current directory references"):
                _validate_safe_path_components(path, "Test path")

    def test_posix_shell_only_punctuation_is_accepted(self) -> None:
        """Shell syntax is inert when the path is passed to filesystem APIs."""
        if platform.system() == "Windows":
            pytest.skip("Unix shell validation not applicable on Windows")

        accepted_chars = [
            "|",
            "&",
            ";",
            "$",
            "`",
            '"',
            "'",
            "<",
            ">",
            "(",
            ")",
            "{",
            "}",
            "^",
            "!",
            "#",
        ]

        for char in accepted_chars:
            path = f"test{char}file.db"
            _validate_safe_path_components(path, "Test path")

    def test_posix_pattern_and_expansion_characters_remain_rejected(self) -> None:
        """Owned glob and expanduser consumers keep their metacharacters blocked."""
        if platform.system() == "Windows":
            pytest.skip("Unix path validation not applicable on Windows")

        for char in ["[", "]", "*", "?", "~"]:
            path = f"test{char}file.db"
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_windows_dangerous_characters_rejection(self) -> None:
        """Test that Windows dangerous characters are rejected on Windows."""
        if platform.system() != "Windows":
            pytest.skip("Windows validation not applicable on non-Windows")

        # Note: colon is handled specially for drive letters
        dangerous_chars = ["*", "?", '"', "<", ">", "|"]

        for char in dangerous_chars:
            path = f"test{char}file.db"
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_windows_drive_letters_allowed(self) -> None:
        """Test that Windows drive letters are allowed."""
        if platform.system() != "Windows":
            pytest.skip("Windows drive letter test not applicable on non-Windows")

        valid_drive_paths = [
            "C:\\temp\\test.db",
            "D:\\data\\broker.db",
            "C:/temp/test.db",  # Forward slashes also work
            "Z:\\project\\database.db",
        ]

        for path in valid_drive_paths:
            # Should not raise any exception
            _validate_safe_path_components(path, "Test path")

    def test_windows_invalid_colons_rejected(self) -> None:
        """Test that colons not part of drive letters are rejected on Windows."""
        if platform.system() != "Windows":
            pytest.skip("Windows colon test not applicable on non-Windows")

        invalid_colon_paths = [
            "test:file.db",  # Colon in middle
            "C:\\test:dir\\file.db",  # Colon after drive letter
            "temp\\file:name.db",  # Colon in filename
        ]

        for path in invalid_colon_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_windows_colon_rules_without_windows_runner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test Windows drive-letter and ADS-style colon handling deterministically."""
        monkeypatch.setattr(_constants.platform, "system", lambda: "Windows")

        valid_drive_paths = [
            "C:\\temp\\test.db",
            "D:\\data\\broker.db",
            "Z:/project/database.db",
        ]
        for path in valid_drive_paths:
            _validate_safe_path_components(path, "Test path")

        invalid_colon_paths = [
            "test:file.db",
            "C:\\test:dir\\file.db",
            "temp\\file:name.db",
        ]
        for path in invalid_colon_paths:
            with pytest.raises(ValueError, match="dangerous character ':'"):
                _validate_safe_path_components(path, "Test path")

    def test_windows_reserved_names_rejection(self) -> None:
        """Test that Windows reserved names are rejected on Windows."""
        if platform.system() != "Windows":
            pytest.skip(
                "Windows reserved name validation not applicable on non-Windows"
            )

        reserved_names = [
            "CON",
            "PRN",
            "AUX",
            "NUL",
            "COM1",
            "COM2",
            "COM3",
            "COM4",
            "COM5",
            "COM6",
            "COM7",
            "COM8",
            "COM9",
            "LPT1",
            "LPT2",
            "LPT3",
            "LPT4",
            "LPT5",
            "LPT6",
            "LPT7",
            "LPT8",
            "LPT9",
        ]

        for name in reserved_names:
            # Test both uppercase and lowercase
            for case_name in [name, name.lower(), name.capitalize()]:
                # Test as filename and with extension
                for path in [case_name, f"{case_name}.db", f"subdir/{case_name}.db"]:
                    with pytest.raises(ValueError, match="Windows reserved name"):
                        _validate_safe_path_components(path, "Test path")

    def test_windows_reserved_names_without_windows_runner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test Windows device-name rejection without relying on Windows CI."""
        monkeypatch.setattr(_constants.platform, "system", lambda: "Windows")

        reserved_names = [
            "CON",
            "PRN",
            "AUX",
            "NUL",
            "COM1",
            "COM9",
            "LPT1",
            "LPT9",
        ]
        for name in reserved_names:
            for case_name in (name, name.lower(), name.capitalize()):
                for path in (case_name, f"{case_name}.db", f"subdir/{case_name}.db"):
                    with pytest.raises(ValueError, match="Windows reserved name"):
                        _validate_safe_path_components(path, "Test path")

    def test_spaces_around_components_rejection(self) -> None:
        """Test that components starting/ending with spaces are rejected."""
        dangerous_paths = [
            " test.db",  # Leading space
            "test.db ",  # Trailing space
            "subdir/ test.db",  # Leading space in component
            "subdir/test.db ",  # Trailing space in component
            " subdir /test.db",  # Spaces around component
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="cannot start or end with spaces"):
                _validate_safe_path_components(path, "Test path")

    def test_long_path_component_rejection(self) -> None:
        """Test that excessively long path components are rejected."""
        long_component = "x" * 256  # Over 255 char limit
        path = f"{long_component}.db"

        with pytest.raises(ValueError, match="component too long"):
            _validate_safe_path_components(path, "Test path")

    def test_total_path_length_uses_platform_policy(self) -> None:
        """POSIX defers total length to the OS while Windows retains its rule."""
        if platform.system() == "Windows":
            with pytest.raises(ValueError, match="too long"):
                _validate_safe_path_components("x" * 261, "Test path")
            return

        long_path = "/".join("x" * 220 for _ in range(5))
        assert len(long_path) > 1024
        _validate_safe_path_components(long_path, "Test path")

    def test_empty_string_rejection(self) -> None:
        """Test that empty strings are rejected."""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            _validate_safe_path_components("", "Test path")

        with pytest.raises(ValueError, match="must be a non-empty string"):
            # Exercise the runtime input-validation boundary with a non-string.
            _validate_safe_path_components(cast(str, None), "Test path")

    def test_custom_context_in_error_messages(self) -> None:
        """Test that custom context appears in error messages."""
        with pytest.raises(ValueError, match="Custom context"):
            _validate_safe_path_components("..", "Custom context")

    def test_cross_platform_path_separators(self) -> None:
        """Test that both forward and back slashes are handled correctly."""
        # These should be valid on all platforms after normalization
        valid_paths = [
            "subdir/test.db",  # Forward slash (universal)
            "subdir\\test.db",  # Backslash (allowed as path separator)
        ]

        for path in valid_paths:
            # Should not raise any exception
            _validate_safe_path_components(path, "Test path")

    def test_backslash_allowed_as_path_separator_on_unix(self) -> None:
        """Test that backslashes are specifically allowed as path separators on Unix."""
        if platform.system() == "Windows":
            pytest.skip("Testing Unix backslash handling")

        # Backslashes should be allowed as path separators
        valid_paths = [
            "subdir\\test.db",
            "a\\b\\c.db",  # Multiple backslashes
            "config\\app.db",
        ]

        for path in valid_paths:
            # Should not raise any exception - backslashes are treated as path separators
            _validate_safe_path_components(path, "Test path")

    def test_realistic_database_names(self) -> None:
        """Test realistic database name patterns that should be allowed."""
        realistic_names = [
            ".broker.db",  # Hidden file
            "app.sqlite3",  # Common extension
            "data-2024.db",  # With year
            "user_sessions.db",  # Underscore
            ".config/broker.db",  # Config directory
            "logs/app.sqlite",  # Logs directory
            "cache/temp.db",  # Cache directory
        ]

        for name in realistic_names:
            # Should not raise any exception
            _validate_safe_path_components(name, "Database name")

    def test_edge_case_valid_names(self) -> None:
        """Test edge cases that should be valid."""
        edge_cases = [
            "a.db",  # Single char
            "123.db",  # Numeric
            "test-v1.2.3.db",  # Version numbers
            "my_app.sqlite3",  # Common pattern
            "data.backup.db",  # Multiple dots
        ]

        for name in edge_cases:
            # Should not raise any exception
            _validate_safe_path_components(name, "Database name")


@pytest.mark.skipif(os.name != "posix", reason="POSIX pathname contract")
@pytest.mark.sqlite_only
def test_posix_punctuation_works_across_explicit_status_and_cleanup_paths(
    tmp_path: Path,
) -> None:
    path_dir = tmp_path / "shell #$`'\"(){};&!^|<> dir"
    path_dir.mkdir()
    db_name = "queue #$`'\"(){};&!^|<>.db"
    db_path = path_dir / db_name
    env = {"BROKER_TEST_BACKEND": "sqlite", "PHASELOCK_ENABLE_XATTRS": "0"}

    code, stdout, stderr = run_cli(
        "-d",
        path_dir,
        "-f",
        db_name,
        "write",
        "jobs",
        "payload",
        cwd=tmp_path,
        env=env,
    )
    assert code == 0, stderr
    assert stdout == ""
    assert db_path.exists()
    assert Path(f"{db_path}.status").exists()

    code, stdout, stderr = run_cli(
        "-d", path_dir, "-f", db_name, "--status", "--json", cwd=tmp_path, env=env
    )
    assert code == 0, stderr
    assert json.loads(stdout)["total_messages"] == 1

    code, stdout, stderr = run_cli(
        "-d", path_dir, "-f", db_name, "--cleanup", cwd=tmp_path, env=env
    )
    assert code == 0, stderr
    assert stdout == ""
    assert not db_path.exists()
    assert not Path(f"{db_path}.status").exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX pathname contract")
@pytest.mark.sqlite_only
def test_posix_punctuation_works_for_init_and_project_discovery(tmp_path: Path) -> None:
    init_name = "init#$(){};&!^|<>.db"
    env = {
        "BROKER_TEST_BACKEND": "sqlite",
        "BROKER_DEFAULT_DB_NAME": init_name,
        "PHASELOCK_ENABLE_XATTRS": "0",
    }

    code, stdout, stderr = run_cli("init", cwd=tmp_path, env=env)
    assert code == 0, stderr
    assert stdout == ""
    assert (tmp_path / init_name).exists()
    assert Path(f"{tmp_path / init_name}.status").exists()

    project = tmp_path / "project#$(){};&!^|<>"
    child = project / "child"
    child.mkdir(parents=True)
    project_db_name = "project#$(){};&!^|<>.db"
    (project / ".broker.toml").write_text(
        f'version = 1\nbackend = "sqlite"\ntarget = "{project_db_name}"\n',
        encoding="utf-8",
    )
    project_env = {
        "BROKER_TEST_BACKEND": "sqlite",
        "BROKER_PROJECT_SCOPE": "1",
    }

    code, stdout, stderr = run_cli(
        "write", "jobs", "project", cwd=child, env=project_env
    )
    assert code == 0, stderr
    assert stdout == ""
    assert (project / project_db_name).exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX pathname contract")
@pytest.mark.sqlite_only
def test_filesystem_supported_posix_path_over_1024_reaches_sqlite(
    tmp_path: Path,
) -> None:
    try:
        path_max = os.pathconf(tmp_path, "PC_PATH_MAX")
    except (OSError, ValueError):
        pytest.skip("filesystem does not report a path limit")
    if path_max <= 1200:
        pytest.skip("filesystem cannot support the over-1024 probe")

    directory = tmp_path
    while len(str(directory / "queue.db")) <= 1100:
        directory /= "x" * 100
    directory.mkdir(parents=True)
    db_path = directory / "queue.db"

    sqlite_accepts_path = True
    try:
        with sqlite3.connect(db_path) as connection:
            connection.execute("PRAGMA user_version").fetchone()
        db_path.unlink()
    except sqlite3.OperationalError as exc:
        assert str(exc) == "unable to open database file"
        sqlite_accepts_path = False

    code, stdout, stderr = run_cli(
        "-f",
        db_path,
        "write",
        "jobs",
        "payload",
        cwd=tmp_path,
        env={"BROKER_TEST_BACKEND": "sqlite"},
    )

    if sqlite_accepts_path:
        assert code == 0, stderr
        assert stdout == ""
        assert db_path.exists()
    else:
        assert code == 1
        assert stdout == ""
        assert stderr == (
            "simplebroker: error: Failed to get database connection: "
            "unable to open database file\n"
        )
        assert not db_path.exists()


@pytest.mark.sqlite_only
def test_live_path_hazard_uses_json_diagnostic_without_side_effects(
    tmp_path: Path,
) -> None:
    code, stdout, stderr = run_cli(
        "-f",
        "queue*.db",
        "--status",
        "--json",
        cwd=tmp_path,
        env={"BROKER_TEST_BACKEND": "sqlite"},
    )

    assert code == 1
    assert stdout == ""
    payload = json.loads(stderr)
    assert payload["error"] == "INVALID_ARGUMENT"
    assert "dangerous character '*'" in payload["message"]
    assert not list(tmp_path.glob("queue*.db"))
