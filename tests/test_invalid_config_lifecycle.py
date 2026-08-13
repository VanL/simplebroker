"""Public configuration-error and import-lifecycle contract tests."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from simplebroker import commands
from simplebroker._constants import (
    _CONFIG_FIELDS,
    _ConfigField,
    load_config,
    resolve_config,
)
from simplebroker.ext import InvalidConfigError

pytestmark = [pytest.mark.shared]

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_python_with_invalid_config(code: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["BROKER_BUSY_TIMEOUT"] = "not-an-integer"
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_cli_with_invalid_config(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["BROKER_BUSY_TIMEOUT"] = "not-an-integer"
    return subprocess.run(
        [sys.executable, "-m", "simplebroker.cli", *args],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_load_config_reports_invalid_environment_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")

    with pytest.raises(InvalidConfigError) as raised:
        load_config()

    error = raised.value
    assert isinstance(error, ValueError)
    assert error.key == "BROKER_BUSY_TIMEOUT"
    assert error.source == "environment"
    assert error.value_display == "'not-an-integer'"
    assert error.expected == "an integer number of milliseconds"


def test_every_recognized_config_field_has_an_expected_form() -> None:
    assert len(_CONFIG_FIELDS) == 32
    assert all(field.expected.strip() for field in _CONFIG_FIELDS.values())


def test_override_failure_reports_source_and_handles_hostile_repr() -> None:
    class HostileValue(str):
        def __repr__(self) -> str:
            raise RuntimeError("repr failed")

    with pytest.raises(InvalidConfigError) as raised:
        resolve_config({"BROKER_BUSY_TIMEOUT": HostileValue("bad")})

    error = raised.value
    assert error.source == "override"
    assert error.value_display == "<HostileValue>"


def test_valid_scalar_subclasses_keep_existing_coercion() -> None:
    class NumericText(str):
        pass

    class NumericInt(int):
        pass

    assert (
        resolve_config({"BROKER_BUSY_TIMEOUT": NumericText("42")})[
            "BROKER_BUSY_TIMEOUT"
        ]
        == 42
    )
    assert (
        resolve_config({"BROKER_BUSY_TIMEOUT": NumericInt(43)})["BROKER_BUSY_TIMEOUT"]
        == 43
    )


def test_config_value_display_escapes_controls_and_is_bounded() -> None:
    hostile = "line\n" + "x" * 300 + "\x7f"

    with pytest.raises(InvalidConfigError) as raised:
        resolve_config({"BROKER_BUSY_TIMEOUT": hostile})

    display = raised.value.value_display
    assert "\n" not in display
    assert "\x7f" not in display
    assert "\\n" in display
    assert len(display) == 160
    assert display.endswith("...")


def test_sensitive_config_failure_redacts_before_formatting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "postgresql://user:top-secret@example.invalid/db"

    def reject(_value: Any) -> str:
        raise ValueError("rejected")

    monkeypatch.setitem(
        _CONFIG_FIELDS,
        "BROKER_BACKEND_TARGET",
        _ConfigField("", reject, "a backend target string"),
    )
    monkeypatch.setenv("BROKER_BACKEND_TARGET", secret)

    with pytest.raises(InvalidConfigError) as raised:
        load_config()

    assert raised.value.value_display == "<redacted>"
    assert secret not in str(raised.value)


@pytest.mark.parametrize(
    "statement",
    [
        "import simplebroker",
        "import simplebroker.ext",
        "import simplebroker.commands",
    ],
)
def test_invalid_environment_does_not_break_package_import(statement: str) -> None:
    result = _run_python_with_invalid_config(statement)

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""


@pytest.mark.parametrize(
    "args", [(), ("--help",), ("--version",), ("--json",), ("--quiet",)]
)
def test_cli_reports_invalid_environment_before_parsing(args: tuple[str, ...]) -> None:
    result = _run_cli_with_invalid_config(*args)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.count("\n") == 1
    assert "BROKER_BUSY_TIMEOUT='not-an-integer'" in result.stderr
    assert "expected an integer number of milliseconds" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("env_updates", "expected_key"),
    [
        ({"BROKER_JITTER_FACTOR": "not-a-float"}, "BROKER_JITTER_FACTOR"),
        (
            {"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": "-1"},
            "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS",
        ),
        ({"BROKER_DEFAULT_DB_NAME": "a/b/c.db"}, "BROKER_DEFAULT_DB_NAME"),
        (
            {
                "BROKER_PROJECT_CONFIG_PATH": ".weft",
                "BROKER_PROJECT_CONFIG_NAME": "config/broker.toml",
            },
            "BROKER_PROJECT_CONFIG_NAME",
        ),
    ],
)
def test_cli_invalid_config_matrix_fails_before_target_creation(
    tmp_path: Path,
    env_updates: dict[str, str],
    expected_key: str,
) -> None:
    target = tmp_path / "must-not-exist.db"
    env = os.environ.copy()
    env.update(env_updates)
    result = subprocess.run(
        [sys.executable, "-m", "simplebroker.cli", "-f", str(target), "list"],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.count("\n") == 1
    assert expected_key in result.stderr
    assert "Traceback" not in result.stderr
    assert not target.exists()


def test_captured_defaults_are_shared_and_public_resolution_stays_fresh() -> None:
    code = """
import os
import simplebroker.commands as commands
import simplebroker.db as db
from simplebroker._constants import _resolve_config_input, resolve_config
print(commands._config is db._config)
print(_resolve_config_input(commands._config)["BROKER_BUSY_TIMEOUT"])
os.environ["BROKER_BUSY_TIMEOUT"] = "37"
print(_resolve_config_input(commands._config)["BROKER_BUSY_TIMEOUT"])
print(resolve_config()["BROKER_BUSY_TIMEOUT"])
"""
    env = os.environ.copy()
    env["BROKER_BUSY_TIMEOUT"] = "19"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["True", "19", "19", "37"]


def test_module_scope_uses_capture_instead_of_strict_load() -> None:
    module_paths = [
        "simplebroker/_broker_session.py",
        "simplebroker/_paths.py",
        "simplebroker/_runner.py",
        "simplebroker/cli.py",
        "simplebroker/commands.py",
        "simplebroker/db.py",
        "simplebroker/sbqueue.py",
        "simplebroker/watcher.py",
    ]
    offenders: list[str] = []
    for relative_path in module_paths:
        tree = ast.parse((PROJECT_ROOT / relative_path).read_text(encoding="utf-8"))
        for node in tree.body:
            value = (
                node.value if isinstance(node, (ast.Assign, ast.AnnAssign)) else None
            )
            if (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == "load_config"
            ):
                offenders.append(relative_path)

    assert offenders == []


COMMAND_CALLS = {
    "cmd_alias_add": "commands.cmd_alias_add(path, 'a', 'q')",
    "cmd_alias_list": "commands.cmd_alias_list(path)",
    "cmd_alias_remove": "commands.cmd_alias_remove(path, 'a')",
    "cmd_broadcast": "commands.cmd_broadcast(path, 'body')",
    "cmd_delete": "commands.cmd_delete(path, 'q')",
    "cmd_dump": "commands.cmd_dump(path)",
    "cmd_exists": "commands.cmd_exists(path, 'q')",
    "cmd_init": "commands.cmd_init(path, True)",
    "cmd_list": "commands.cmd_list(path)",
    "cmd_load": "commands.cmd_load(path)",
    "cmd_move": "commands.cmd_move(path, 'source', 'dest')",
    "cmd_peek": "commands.cmd_peek(path, 'q')",
    "cmd_read": "commands.cmd_read(path, 'q')",
    "cmd_rename": "commands.cmd_rename(path, 'old', 'new')",
    "cmd_stats": "commands.cmd_stats(path, 'q')",
    "cmd_status": "commands.cmd_status(path)",
    "cmd_vacuum": "commands.cmd_vacuum(path)",
    "cmd_watch": "commands.cmd_watch(path, 'q', quiet=True)",
    "cmd_write": "commands.cmd_write(path, 'q', 'body')",
}


@pytest.mark.parametrize(("name", "call"), COMMAND_CALLS.items())
def test_direct_commands_raise_when_their_path_consumes_invalid_config(
    name: str,
    call: str,
) -> None:
    code = f"""
import tempfile
import simplebroker.commands as commands
from simplebroker.ext import InvalidConfigError
path = tempfile.mktemp(suffix='.db')
try:
    {call}
except InvalidConfigError:
    print({name!r})
else:
    raise SystemExit('command did not consume invalid configuration')
"""
    result = _run_python_with_invalid_config(code)

    assert result.returncode == 0, result.stderr
    assert result.stdout == f"{name}\n"


def test_cmd_load_consumes_invalid_config_before_interactive_stdin_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InteractiveInput:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(commands.sys, "stdin", InteractiveInput())

    with pytest.raises(InvalidConfigError):
        commands.cmd_load(
            "unused.db",
            config={"BROKER_BUSY_TIMEOUT": "not-an-integer"},
        )


def test_direct_command_early_validation_can_remain_config_independent() -> None:
    code = """
import simplebroker.commands as commands
result = commands.cmd_delete('unused.db', 'q', 'not-a-message-id')
print(result)
"""
    result = _run_python_with_invalid_config(code)

    assert result.returncode == 0
    assert result.stdout == "1\n"
    assert "invalid message ID" in result.stderr
