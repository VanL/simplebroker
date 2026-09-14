"""Public configuration-error and import-lifecycle contract tests."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path, PurePath, PureWindowsPath
from typing import Any

import pytest

from simplebroker import DEFAULT_CONFIG, BrokerTarget, ConfigField, commands
from simplebroker._constants import (
    resolve_config,
)
from simplebroker.ext import InvalidConfigError

pytestmark = [
    pytest.mark.shared,
    pytest.mark.filterwarnings("ignore:.*ignoring invalid"),
]

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _canonical_module_path(path: PurePath) -> str:
    """Return a host-independent repository module identifier."""
    return path.as_posix()


def _run_python_with_invalid_config(code: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["BROKER_BUSY_TIMEOUT"] = "not-an-integer"
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=60,
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
        timeout=60,
    )


def test_load_config_reports_invalid_environment_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")

    with pytest.raises(InvalidConfigError) as raised:
        resolve_config(env=os.environ)

    error = raised.value
    assert isinstance(error, ValueError)
    assert error.key == "BROKER_BUSY_TIMEOUT"
    assert error.source == "environment"
    assert error.value_display == "'not-an-integer'"
    assert error.expected == "an integer number of milliseconds"


def test_every_recognized_config_field_has_an_expected_form() -> None:
    assert len(DEFAULT_CONFIG) == 32
    assert all(field.description.strip() for field in DEFAULT_CONFIG.values())


def test_override_failure_reports_source_and_handles_hostile_repr() -> None:
    class HostileValue(str):
        def __repr__(self) -> str:
            raise RuntimeError("repr failed")

    with pytest.raises(InvalidConfigError) as raised:
        resolve_config(
            env=os.environ, override={"BROKER_BUSY_TIMEOUT": HostileValue("bad")}
        )

    error = raised.value
    assert error.source == "override"
    assert error.value_display == "<HostileValue>"


def test_valid_scalar_subclasses_keep_existing_coercion() -> None:
    class NumericText(str):
        pass

    class NumericInt(int):
        pass

    assert (
        resolve_config(
            env=os.environ, override={"BROKER_BUSY_TIMEOUT": NumericText("42")}
        )["BUSY_TIMEOUT"]
        == 42
    )
    assert (
        resolve_config(
            env=os.environ, override={"BROKER_BUSY_TIMEOUT": NumericInt(43)}
        )["BUSY_TIMEOUT"]
        == 43
    )


def test_config_value_display_escapes_controls_and_is_bounded() -> None:
    hostile = "line\n" + "x" * 300 + "\x7f"

    with pytest.raises(InvalidConfigError) as raised:
        resolve_config(env=os.environ, override={"BROKER_BUSY_TIMEOUT": hostile})

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

    def reject(value: Any) -> str:
        if value:
            raise ValueError("rejected")
        return ""

    fields = dict(DEFAULT_CONFIG)
    fields["BACKEND_TARGET"] = ConfigField("", "target", reject, sensitive=True)
    monkeypatch.setenv("BROKER_BACKEND_TARGET", secret)

    with pytest.raises(InvalidConfigError) as raised:
        resolve_config(env=os.environ, defaults=fields)

    assert raised.value.source == "environment"
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
    assert result.stderr.splitlines()[0] == (
        "simplebroker: warning: ignoring invalid BROKER_BUSY_TIMEOUT='not-an-integer' "
        "from the environment (expected an integer number of milliseconds)"
    )
    assert "UserWarning" not in result.stderr
    assert "warnings.warn" not in result.stderr
    assert result.stderr.splitlines()[-1].startswith(
        "simplebroker: invalid configuration"
    )
    assert "BROKER_BUSY_TIMEOUT='not-an-integer'" in result.stderr
    assert "expected an integer number of milliseconds" in result.stderr
    assert "Traceback" not in result.stderr


def test_cli_prints_misspelled_environment_name_as_one_warning_line() -> None:
    env = os.environ.copy()
    env["BROKER_busy_timeout"] = "5"
    result = subprocess.run(
        [sys.executable, "-m", "simplebroker.cli", "--version"],
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=60,
    )

    assert result.returncode == 0
    assert result.stderr == (
        "simplebroker: warning: ignoring BROKER_busy_timeout from the environment: "
        "did you mean BROKER_BUSY_TIMEOUT? (value '5')\n"
    )


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
    assert result.stderr.splitlines()[-1].startswith(
        "simplebroker: invalid configuration"
    )
    assert expected_key in result.stderr
    assert "Traceback" not in result.stderr
    assert not target.exists()


def test_public_snapshots_are_explicit_and_fresh_across_calls() -> None:
    code = """
import os
from simplebroker import resolve_config
first = resolve_config(env=os.environ)
print(first["BUSY_TIMEOUT"])
os.environ["BROKER_BUSY_TIMEOUT"] = "37"
print(first["BUSY_TIMEOUT"])
second = resolve_config(env=os.environ)
print(second["BUSY_TIMEOUT"])
print(first is second)
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
    assert result.stdout.splitlines() == ["19", "19", "37", "False"]


def test_each_invalid_snapshot_raises_a_fresh_exception_and_repair_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from simplebroker import resolve_config

    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")
    errors: list[InvalidConfigError] = []
    traceback_shapes: list[list[str]] = []

    for _ in range(2):
        try:
            resolve_config(env=os.environ)
        except InvalidConfigError as error:
            errors.append(error)
            frames: list[str] = []
            traceback = error.__traceback__
            while traceback is not None:
                frames.append(traceback.tb_frame.f_code.co_name)
                traceback = traceback.tb_next
            traceback_shapes.append(frames)

    assert len(errors) == 2
    assert errors[0] is not errors[1]
    assert traceback_shapes[0] == traceback_shapes[1]

    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "23")
    assert resolve_config(env=os.environ)["BUSY_TIMEOUT"] == 23


def _import_time_ambient_resolutions(tree: ast.AST) -> list[int]:
    """Return lines of import-time ``resolve_config`` calls lacking ``env={}``.

    Import evaluates module and class bodies, decorators and default argument
    values, so all of those are checked. Function and lambda bodies run later.
    Only an empty dict literal proves the call is ambient-free, whatever the
    resolver's ``env`` default is.
    """
    lines: list[int] = []

    def visit(node: ast.AST) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            evaluated = [*node.args.defaults, *node.args.kw_defaults]
            if not isinstance(node, ast.Lambda):
                evaluated.extend(node.decorator_list)
            for expression in evaluated:
                if expression is not None:
                    visit(expression)
            return
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            ambient_free = any(
                keyword.arg == "env"
                and isinstance(keyword.value, ast.Dict)
                and not keyword.value.keys
                for keyword in node.keywords
            )
            if name == "resolve_config" and not ambient_free:
                lines.append(node.lineno)
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(tree)
    return lines


@pytest.mark.parametrize(
    ("source", "flagged"),
    [
        ("X = resolve_config()", True),
        ("resolve_config()", True),
        ("X = _constants.resolve_config()", True),
        ("X = resolve_config(env=os.environ)", True),
        ("class A:\n    x = resolve_config()", True),
        ("def f(config=resolve_config()):\n    pass", True),
        ("@wrap(resolve_config())\ndef f():\n    pass", True),
        ("X = resolve_config(env={})", False),
        ("def f():\n    return resolve_config()", False),
        ("F = lambda: resolve_config()", False),
    ],
)
def test_import_time_resolution_detector(source: str, flagged: bool) -> None:
    assert bool(_import_time_ambient_resolutions(ast.parse(source))) is flagged


def test_config_consumers_do_not_resolve_ambient_config_at_module_scope() -> None:
    # Glob-derived so a new module cannot silently escape the guard
    # (audit Task 6.5 — the old hardcoded 17-path list exempted new
    # files by default). Sanity anchors below keep the derivation
    # honest.
    module_paths = sorted(
        _canonical_module_path(path.relative_to(PROJECT_ROOT))
        for pattern in (
            "simplebroker/**/*.py",
            "extensions/simplebroker_pg/simplebroker_pg/**/*.py",
            "extensions/simplebroker_redis/simplebroker_redis/**/*.py",
        )
        for path in PROJECT_ROOT.glob(pattern)
        if "__pycache__" not in path.parts
    )
    assert "simplebroker/cli.py" in module_paths
    assert "extensions/simplebroker_redis/simplebroker_redis/pool.py" in module_paths
    assert len(module_paths) > 17
    offenders = [
        f"{relative_path}:{line}"
        for relative_path in module_paths
        for line in _import_time_ambient_resolutions(
            ast.parse((PROJECT_ROOT / relative_path).read_text(encoding="utf-8"))
        )
    ]

    assert offenders == [], (
        "import-time resolve_config() must pass env={} so importing never "
        f"parses ambient configuration: {offenders}"
    )


def test_module_path_inventory_normalizes_windows_separators() -> None:
    assert (
        _canonical_module_path(PureWindowsPath(r"simplebroker\cli.py"))
        == "simplebroker/cli.py"
    )


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
            config=resolve_config(override={"BROKER_BUSY_TIMEOUT": "not-an-integer"}),
        )


def test_direct_command_early_validation_can_remain_config_independent() -> None:
    code = """
import simplebroker.commands as commands
try:
    commands.cmd_delete('unused.db', 'q', 'not-a-message-id')
except ValueError as error:
    print(type(error).__name__, str(error))
else:
    raise SystemExit('invalid message ID did not raise')
"""
    result = _run_python_with_invalid_config(code)

    assert result.returncode == 0
    assert result.stdout.startswith("ValueError ")
    assert "invalid message ID" in result.stdout
    assert result.stderr == ""


def test_direct_target_init_does_not_translate_invalid_config_to_exit_code(
    tmp_path: Path,
) -> None:
    target = BrokerTarget(
        backend_name="sqlite",
        target=str(tmp_path / "target-init.db"),
        backend_options={},
    )

    with pytest.raises(InvalidConfigError):
        commands.cmd_init(
            target,
            quiet=True,
            config=resolve_config(override={"BROKER_BUSY_TIMEOUT": "not-an-integer"}),
        )


def test_direct_command_calls_ignore_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "5")

    assert commands.cmd_write(str(tmp_path / "commands.db"), "jobs", "123456") == 0


def _inline_config_fallbacks(tree: ast.AST) -> list[int]:
    """Return lines of ``... if config is None else config`` seam expressions."""
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.IfExp)
        and isinstance(node.orelse, ast.Name)
        and node.orelse.id == "config"
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "config"
        and len(node.test.ops) == 1
        and isinstance(node.test.ops[0], ast.Is)
        and isinstance(node.test.comparators[0], ast.Constant)
        and node.test.comparators[0].value is None
    ]


def test_config_seams_route_supplied_configs_through_resolve_config() -> None:
    module_paths = sorted(
        _canonical_module_path(path.relative_to(PROJECT_ROOT))
        for pattern in (
            "simplebroker/**/*.py",
            "extensions/simplebroker_pg/simplebroker_pg/**/*.py",
            "extensions/simplebroker_redis/simplebroker_redis/**/*.py",
            "examples/*.py",
        )
        for path in PROJECT_ROOT.glob(pattern)
        if "__pycache__" not in path.parts
    )
    assert "simplebroker/db.py" in module_paths
    offenders = [
        f"{relative_path}:{line}"
        for relative_path in module_paths
        for line in _inline_config_fallbacks(
            ast.parse((PROJECT_ROOT / relative_path).read_text(encoding="utf-8"))
        )
    ]

    assert offenders == [], (
        "a supplied config must pass through resolve_config(config=...) so a "
        f"non-Config fails at the boundary: {offenders}"
    )
