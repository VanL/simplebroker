"""Tests for .broker.toml project configuration."""

from __future__ import annotations

import json
import os
import pickle
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from enum import IntEnum
from pathlib import Path
from typing import Any, get_type_hints

import pytest

from simplebroker._backend_plugins import get_backend_plugin
from simplebroker._constants import load_config
from simplebroker._exceptions import DatabaseError, UnknownBackendPluginError
from simplebroker._project_config import (
    _same_filesystem,
    find_project_config,
    load_project_config,
    project_config_path_for_directory,
    resolve_project_target,
)
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerDB, _initialize_project_backend_target
from simplebroker.project import (
    broker_root,
    deserialize_broker_target,
    resolve_broker_target,
    serialize_broker_target,
    target_for_directory,
)

from .conftest import run_cli


def _toml_basic_string(value: str) -> str:
    """Return a TOML basic string with required backslash escapes."""
    escapes = {
        "\b": "\\b",
        "\t": "\\t",
        "\n": "\\n",
        "\f": "\\f",
        "\r": "\\r",
        '"': '\\"',
        "\\": "\\\\",
    }
    parts: list[str] = []
    for char in value:
        escaped = escapes.get(char)
        if escaped is not None:
            parts.append(escaped)
            continue
        if ord(char) <= 0x08 or 0x0A <= ord(char) <= 0x1F or ord(char) == 0x7F:
            parts.append(f"\\u{ord(char):04X}")
            continue
        parts.append(char)
    return f'"{"".join(parts)}"'


def _write_project_config(
    path: Path,
    *,
    backend: str,
    target: str,
    backend_options: dict[str, str] | None = None,
) -> None:
    lines = [
        "version = 1",
        f"backend = {_toml_basic_string(backend)}",
        f"target = {_toml_basic_string(target)}",
        "",
    ]
    if backend_options:
        lines.append("[backend_options]")
        for key, value in backend_options.items():
            lines.append(f"{key} = {_toml_basic_string(value)}")
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    path.chmod(0o600)


def _project_backend_config(*, sqlite_target: str) -> tuple[str, str, dict[str, str]]:
    if os.environ.get("BROKER_TEST_BACKEND") == "postgres":
        dsn = os.environ["SIMPLEBROKER_PG_TEST_DSN"]
        return "postgres", dsn, {"schema": f"pytest_{uuid.uuid4().hex[:12]}"}
    return "sqlite", sqlite_target, {}


@pytest.mark.sqlite_only
def test_load_project_config_and_resolve_relative_sqlite_target(
    tmp_path: Path,
) -> None:
    """Relative sqlite targets should resolve from the config file directory."""
    config_path = tmp_path / ".broker.toml"
    _write_project_config(config_path, backend="sqlite", target="data/queue.db")

    config_data = load_project_config(config_path)
    resolved = resolve_project_target(config_path)

    assert config_data["backend"] == "sqlite"
    assert config_data["target"] == "data/queue.db"
    assert resolved.backend_name == "sqlite"
    assert resolved.target_path == (tmp_path / "data" / "queue.db").resolve()


def test_project_config_warns_for_inline_url_password(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / ".broker.toml"
    _write_project_config(
        config_path,
        backend="postgres",
        target="postgresql://user:inline-secret@db.example.com/app",
    )
    config_path.chmod(0o600)

    load_project_config(config_path)

    warning = capsys.readouterr().err
    assert "BROKER_BACKEND_PASSWORD" in warning
    assert "inline-secret" not in warning


def test_project_config_warns_for_inline_conninfo_password(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / ".broker.toml"
    _write_project_config(
        config_path,
        backend="postgres",
        target="host=db.example.com user=app password='inline-secret'",
    )
    config_path.chmod(0o600)

    load_project_config(config_path)

    warning = capsys.readouterr().err
    assert "BROKER_BACKEND_PASSWORD" in warning
    assert "inline-secret" not in warning


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits")
def test_project_config_does_not_judge_group_or_other_mode_bits(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / ".broker.toml"
    _write_project_config(config_path, backend="sqlite", target="queue.db")
    config_path.chmod(0o644)

    load_project_config(config_path)

    assert capsys.readouterr().err == ""


def test_project_config_env_password_does_not_trigger_inline_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / ".broker.toml"
    _write_project_config(
        config_path,
        backend="postgres",
        target="postgresql://user@db.example.com/app",
    )
    config_path.chmod(0o600)
    monkeypatch.setenv("BROKER_BACKEND_PASSWORD", "env-secret")

    load_project_config(config_path)

    assert capsys.readouterr().err == ""


def test_same_filesystem_compares_device_ids() -> None:
    class FakePath:
        def __init__(self, device: int) -> None:
            self._device = device

        def stat(self) -> Any:
            return type("Stat", (), {"st_dev": self._device})()

    assert _same_filesystem(FakePath(1), FakePath(1)) is True  # type: ignore[arg-type]
    assert _same_filesystem(FakePath(1), FakePath(2)) is False  # type: ignore[arg-type]


def test_project_config_discovery_stops_before_mount_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = tmp_path / "project"
    nested = project_root / "nested"
    nested.mkdir(parents=True)
    config_path = project_root / ".broker.toml"
    _write_project_config(config_path, backend="sqlite", target="queue.db")
    monkeypatch.setattr(
        "simplebroker._project_config._same_filesystem",
        lambda current, parent: current != nested.resolve(),
    )

    assert find_project_config(nested) is None


def test_cli_scope_off_uses_only_current_directory_project_config(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    _write_project_config(parent / ".broker.toml", backend="sqlite", target="parent.db")
    _write_project_config(child / ".broker.toml", backend="sqlite", target="child.db")

    env = {"BROKER_PROJECT_SCOPE": "0", "BROKER_TEST_BACKEND": "sqlite"}
    code, _, stderr = run_cli("write", "jobs", "child", cwd=child, env=env)

    assert code == 0, stderr
    assert (child / "child.db").exists()
    assert not (parent / "parent.db").exists()


def test_cli_scope_off_does_not_walk_to_parent_project_config(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    _write_project_config(parent / ".broker.toml", backend="sqlite", target="parent.db")

    env = {"BROKER_PROJECT_SCOPE": "0", "BROKER_TEST_BACKEND": "sqlite"}
    code, _, stderr = run_cli("write", "jobs", "local", cwd=child, env=env)

    assert code == 0, stderr
    assert (child / ".broker.db").exists()
    assert not (parent / "parent.db").exists()


def test_cli_explicit_file_beats_scope_off_current_project_config(
    tmp_path: Path,
) -> None:
    _write_project_config(tmp_path / ".broker.toml", backend="sqlite", target="toml.db")

    code, _, stderr = run_cli(
        "-f",
        "explicit.db",
        "write",
        "jobs",
        "explicit",
        cwd=tmp_path,
        env={"BROKER_PROJECT_SCOPE": "0", "BROKER_TEST_BACKEND": "sqlite"},
    )

    assert code == 0, stderr
    assert (tmp_path / "explicit.db").exists()
    assert not (tmp_path / "toml.db").exists()


def test_scope_off_current_project_config_beats_environment_default(
    tmp_path: Path,
) -> None:
    env_default = tmp_path / "environment"
    env_default.mkdir()
    _write_project_config(tmp_path / ".broker.toml", backend="sqlite", target="toml.db")
    env = {
        "BROKER_PROJECT_SCOPE": "0",
        "BROKER_DEFAULT_DB_LOCATION": str(env_default),
        "BROKER_TEST_BACKEND": "sqlite",
    }

    code, _, stderr = run_cli("write", "jobs", "toml", cwd=tmp_path, env=env)

    assert code == 0, stderr
    assert (tmp_path / "toml.db").exists()
    assert not (env_default / ".broker.db").exists()


def test_target_for_directory_never_walks_to_parent_project_config(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    _write_project_config(parent / ".broker.toml", backend="sqlite", target="parent.db")

    target = target_for_directory(child)

    assert target.config_path is None
    assert target.target_path == (child / ".broker.db").resolve()


@pytest.mark.sqlite_only
def test_project_config_trust_anchor_allows_parent_target(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    config_path = project / ".broker.toml"
    _write_project_config(config_path, backend="sqlite", target="../outside.db")

    target = resolve_project_target(config_path)

    assert target.target_path == (tmp_path / "outside.db").resolve()


@pytest.mark.sqlite_only
def test_project_config_trust_anchor_follows_target_symlink(tmp_path: Path) -> None:
    project = tmp_path / "project"
    outside = tmp_path / "outside"
    project.mkdir()
    outside.mkdir()
    link = project / "linked"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {exc}")
    config_path = project / ".broker.toml"
    _write_project_config(config_path, backend="sqlite", target="linked/queue.db")

    target = resolve_project_target(config_path)

    assert target.target_path == (outside / "queue.db").resolve()


@pytest.mark.sqlite_only
def test_load_project_config_preserves_unicode_sqlite_target(
    tmp_path: Path,
) -> None:
    """TOML basic strings should preserve literal non-ASCII characters."""
    config_path = tmp_path / ".broker.toml"
    target = str(tmp_path / "données" / "queue.db")
    _write_project_config(config_path, backend="sqlite", target=target)

    config_data = load_project_config(config_path)
    resolved = resolve_project_target(config_path)

    assert config_data["target"] == target
    assert resolved.target_path == Path(target).resolve()

    code, stdout, stderr = run_cli(
        "init",
        cwd=tmp_path,
        env={"BROKER_PROJECT_SCOPE": "1", "BROKER_TEST_BACKEND": "sqlite"},
    )
    assert code == 0, stderr
    assert stdout == ""
    assert Path(target).exists()
    assert not (tmp_path / "donnÃ©es" / "queue.db").exists()


def test_load_project_config_decodes_toml_basic_string_escapes(
    tmp_path: Path,
) -> None:
    """Supported TOML basic-string escapes should decode without mojibake."""
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "sqlite"\n'
            'target = "data\\u002Fqueue.db"\n'
            "[backend_options]\n"
            'note = "line\\nquote\\" slash\\\\ emoji\\U0001F600"\n'
        ),
        encoding="utf-8",
    )

    config_data = load_project_config(config_path)

    assert config_data["target"] == "data/queue.db"
    assert config_data["backend_options"]["note"] == 'line\nquote" slash\\ emoji😀'


def test_load_project_config_accepts_toml_literal_strings(
    tmp_path: Path,
) -> None:
    """TOML literal strings should preserve Windows backslashes."""
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            "backend = 'sqlite'\n"
            "target = 'C:\\Users\\runner\\données#1\\queue.db' # comment\n"
            "[backend_options]\n"
            "note = 'slash\\n stays literal'\n"
        ),
        encoding="utf-8",
    )

    config_data = load_project_config(config_path)

    assert config_data["backend"] == "sqlite"
    assert config_data["target"] == "C:\\Users\\runner\\données#1\\queue.db"
    assert config_data["backend_options"]["note"] == "slash\\n stays literal"


def test_load_project_config_rejects_invalid_toml_basic_string_escape(
    tmp_path: Path,
) -> None:
    """Unknown backslash escapes are not valid in TOML basic strings."""
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        ('version = 1\nbackend = "sqlite"\ntarget = "data\\qqueue.db"\n'),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_project_config(config_path)


def test_load_project_config_rejects_duplicate_toml_keys(tmp_path: Path) -> None:
    """Duplicate TOML keys should be rejected instead of silently overwritten."""
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "sqlite"\n'
            'target = "first.db"\n'
            'target = "second.db"\n'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_project_config(config_path)


def test_load_project_config_preserves_backend_option_arrays(tmp_path: Path) -> None:
    """Core parsing preserves recursive TOML values for the owning plugin."""
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "redis"\n'
            'target = "redis://127.0.0.1:6379/0"\n'
            "\n"
            "[backend_options]\n"
            'namespace = ["one", "two"]\n'
        ),
        encoding="utf-8",
    )

    config = load_project_config(config_path)

    assert config["backend_options"] == {"namespace": ["one", "two"]}


def test_load_project_config_preserves_nested_backend_option_tables(
    tmp_path: Path,
) -> None:
    """Nested tables reach backend plugins without a core-owned schema."""
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "redis"\n'
            'target = "redis://127.0.0.1:6379/0"\n'
            "\n"
            "[backend_options.pool]\n"
            "timeout = 5\n"
        ),
        encoding="utf-8",
    )

    config = load_project_config(config_path)

    assert config["backend_options"] == {"pool": {"timeout": 5}}


@pytest.mark.sqlite_only
def test_sqlite_project_options_are_rejected_by_the_sqlite_plugin(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "sqlite"\n'
            'target = "queue.db"\n'
            "\n"
            "[backend_options.pool]\n"
            "timeout = 5\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="SQLite backend does not support backend_options"
    ):
        resolve_project_target(config_path)


@pytest.mark.sqlite_only
def test_sqlite_project_option_rejection_is_one_json_cli_error(tmp_path: Path) -> None:
    (tmp_path / ".broker.toml").write_text(
        (
            "version = 1\n"
            'backend = "sqlite"\n'
            'target = "queue.db"\n'
            "\n"
            "[backend_options]\n"
            "pool_size = 2\n"
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = run_cli(
        "--status",
        "--json",
        cwd=tmp_path,
        env={"BROKER_TEST_BACKEND": "sqlite", "BROKER_PROJECT_SCOPE": "1"},
    )

    assert code == 1
    assert stdout == ""
    payload = json.loads(stderr)
    assert payload["error"] == "ERROR"
    assert payload["message"] == (
        "SQLite backend does not support backend_options; remove them or select a "
        "backend that supports them"
    )
    assert "Traceback" not in stderr
    assert not (tmp_path / "queue.db").exists()


@pytest.mark.sqlite_only
def test_sqlite_plugin_never_silently_discards_backend_options(tmp_path: Path) -> None:
    plugin = get_backend_plugin("sqlite")
    target = str(tmp_path / "queue.db")
    options = {"pool": {"timeout": 5}}
    calls = (
        lambda: plugin.init_backend(
            load_config(), toml_target=target, toml_options=options
        ),
        lambda: plugin.create_runner(target, backend_options=options),
        lambda: plugin.initialize_target(target, backend_options=options),
        lambda: plugin.validate_target(target, backend_options=options),
        lambda: plugin.cleanup_target(target, backend_options=options),
    )

    for call in calls:
        with pytest.raises(
            ValueError, match="SQLite backend does not support backend_options"
        ):
            call()

    assert not Path(target).exists()


def test_plugin_must_normalize_toml_datetime_for_target_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "fixture"\n'
            'target = "fixture://queue"\n'
            "\n"
            "[backend_options]\n"
            "cutoff = 1979-05-27T07:32:00Z\n"
        ),
        encoding="utf-8",
    )

    class PassthroughPlugin:
        def init_backend(self, config, *, toml_target="", toml_options=None):
            del config
            return {
                "target": toml_target,
                "backend_options": dict(toml_options or {}),
            }

    monkeypatch.setattr(
        "simplebroker._project_config.get_backend_plugin",
        lambda name: PassthroughPlugin(),
    )

    with pytest.raises(
        ValueError,
        match=(
            "Backend plugin 'fixture' returned backend_options that are not "
            "lossless through BrokerTarget serialization"
        ),
    ):
        resolve_project_target(config_path)


def test_plugin_must_not_return_json_scalar_subclasses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        'version = 1\nbackend = "fixture"\ntarget = "fixture://queue"\n',
        encoding="utf-8",
    )

    class PoolSize(IntEnum):
        SMALL = 1

    class EnumPlugin:
        def init_backend(self, config, *, toml_target="", toml_options=None):
            del config, toml_options
            return {
                "target": toml_target,
                "backend_options": {"pool_size": PoolSize.SMALL},
            }

    monkeypatch.setattr(
        "simplebroker._project_config.get_backend_plugin",
        lambda name: EnumPlugin(),
    )

    with pytest.raises(
        ValueError,
        match=(
            "Backend plugin 'fixture' returned backend_options that are not "
            "lossless through BrokerTarget serialization"
        ),
    ):
        resolve_project_target(config_path)


def test_plugin_cyclic_options_fail_at_the_controlled_transport_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        'version = 1\nbackend = "fixture"\ntarget = "fixture://queue"\n',
        encoding="utf-8",
    )
    cyclic_options: dict[str, Any] = {}
    cyclic_options["self"] = cyclic_options

    class CyclicPlugin:
        def init_backend(self, config, *, toml_target="", toml_options=None):
            del config, toml_options
            return {"target": toml_target, "backend_options": cyclic_options}

    monkeypatch.setattr(
        "simplebroker._project_config.get_backend_plugin",
        lambda name: CyclicPlugin(),
    )

    with pytest.raises(
        ValueError,
        match=(
            "Backend plugin 'fixture' returned backend_options that are not "
            "lossless through BrokerTarget serialization"
        ),
    ):
        resolve_project_target(config_path)


@pytest.mark.parametrize("backend_name", ["postgres", "redis"])
def test_project_backend_setup_uses_config_file_phase_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend_name: str,
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text("version = 1\n", encoding="utf-8")
    state_lock = threading.Lock()
    initialized = False
    initialize_calls = 0

    class CoordinatedPlugin:
        def validate_target(self, *args, **kwargs) -> None:
            del args, kwargs
            with state_lock:
                if not initialized:
                    raise DatabaseError("not initialized")

        def initialize_target(self, *args, **kwargs) -> None:
            del args, kwargs
            nonlocal initialized, initialize_calls
            with state_lock:
                initialize_calls += 1
            time.sleep(0.05)
            with state_lock:
                initialized = True

    plugin = CoordinatedPlugin()
    monkeypatch.setattr(
        "simplebroker._backend_plugins.get_backend_plugin",
        lambda name: plugin,
    )
    target = BrokerTarget(
        backend_name=backend_name,
        target="backend://fixture",
        backend_options={"scope": "one"},
        project_root=tmp_path,
        config_path=config_path,
        used_project_scope=True,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                _initialize_project_backend_target,
                target,
                config={},
            )
            for _ in range(2)
        ]
        for future in futures:
            future.result()

    assert initialize_calls == 1
    assert Path(f"{config_path}.lock").exists()


def test_nested_options_reach_plugin_and_normalize_losslessly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "fixture"\n'
            'target = "fixture://queue"\n'
            "\n"
            "[backend_options.tls]\n"
            "enabled = true\n"
            'ca_paths = ["one.pem", "two.pem"]\n'
            "not_before = 1979-05-27T07:32:00Z\n"
            "maintenance_date = 1979-05-27\n"
            "cutover_time = 07:32:00\n"
            "\n"
            "[[backend_options.pools]]\n"
            "size = 2\n"
            "\n"
            "[[backend_options.pools]]\n"
            "size = 4\n"
        ),
        encoding="utf-8",
    )
    seen: dict[str, Any] = {}

    class NormalizingPlugin:
        def init_backend(self, config, *, toml_target="", toml_options=None):
            del config
            options = dict(toml_options or {})
            seen.update(options)
            tls = dict(options["tls"])
            for key in ("not_before", "maintenance_date", "cutover_time"):
                tls[key] = tls[key].isoformat()
            return {
                "target": toml_target,
                "backend_options": {"tls": tls, "pools": options["pools"]},
            }

    monkeypatch.setattr(
        "simplebroker._project_config.get_backend_plugin",
        lambda name: NormalizingPlugin(),
    )

    target = resolve_project_target(config_path)
    restored = deserialize_broker_target(serialize_broker_target(target))

    assert seen["tls"]["ca_paths"] == ["one.pem", "two.pem"]
    assert seen["pools"] == [{"size": 2}, {"size": 4}]
    assert seen["tls"]["not_before"].isoformat() == "1979-05-27T07:32:00+00:00"
    assert seen["tls"]["maintenance_date"].isoformat() == "1979-05-27"
    assert seen["tls"]["cutover_time"].isoformat() == "07:32:00"
    assert restored == target
    assert restored.backend_options == {
        "tls": {
            "enabled": True,
            "ca_paths": ["one.pem", "two.pem"],
            "not_before": "1979-05-27T07:32:00+00:00",
            "maintenance_date": "1979-05-27",
            "cutover_time": "07:32:00",
        },
        "pools": [{"size": 2}, {"size": 4}],
    }


def test_plugin_option_rejection_keeps_plugin_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "fixture"\n'
            'target = "fixture://queue"\n'
            "\n"
            "[backend_options.pool]\n"
            "timeout = -1\n"
        ),
        encoding="utf-8",
    )
    failure = ValueError("fixture plugin: pool.timeout must be positive")

    class RejectingPlugin:
        def init_backend(self, config, *, toml_target="", toml_options=None):
            del config, toml_target, toml_options
            raise failure

    monkeypatch.setattr(
        "simplebroker._project_config.get_backend_plugin",
        lambda name: RejectingPlugin(),
    )

    with pytest.raises(ValueError) as exc_info:
        resolve_project_target(config_path)

    assert exc_info.value is failure


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        (
            'version = 2\nbackend = "sqlite"\ntarget = "queue.db"\n',
            ".broker.toml version",
        ),
        ('version = 1\ntarget = "queue.db"\n', "requires a non-empty string 'backend'"),
        ('version = 1\nbackend = "sqlite"\n', "requires a non-empty string 'target'"),
        (
            'version = 1\nbackend = "sqlite"\ntarget = "queue.db"\nbackend_options = "bad"\n',
            "'backend_options' must be a table",
        ),
    ],
)
def test_load_project_config_rejects_invalid_required_fields(
    tmp_path: Path,
    contents: str,
    message: str,
) -> None:
    """Malformed project identity fields must fail before backend resolution."""

    config_path = tmp_path / ".broker.toml"
    config_path.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError) as raised:
        load_project_config(config_path)
    assert message in str(raised.value)


@pytest.mark.sqlite_only
@pytest.mark.parametrize(
    ("contents", "expected_template"),
    [
        (
            'version = 2\nbackend = "sqlite"\ntarget = "queue.db"\n',
            "Unsupported {path} version 2; expected 1",
        ),
        (
            'version = 1\ntarget = "queue.db"\n',
            "{path} requires a non-empty string 'backend'",
        ),
        (
            'version = 1\nbackend = "sqlite"\n',
            "{path} requires a non-empty string 'target'",
        ),
        (
            (
                'version = 1\nbackend = "sqlite"\ntarget = "queue.db"\n'
                'backend_options = "bad"\n'
            ),
            "'backend_options' must be a table in {path}",
        ),
    ],
)
def test_invalid_project_config_diagnostic_names_selected_config_file(
    tmp_path: Path,
    contents: str,
    expected_template: str,
) -> None:
    """A non-default project-config filename must survive into its error."""
    config_dir = tmp_path / ".weft"
    config_dir.mkdir()
    config_path = config_dir / "broker.toml"
    config_path.write_text(contents, encoding="utf-8")

    code, stdout, stderr = run_cli(
        "--status",
        cwd=tmp_path,
        env={
            "BROKER_PROJECT_SCOPE": "1",
            "BROKER_PROJECT_CONFIG_PATH": ".weft",
            "BROKER_PROJECT_CONFIG_NAME": "broker.toml",
            "BROKER_TEST_BACKEND": "sqlite",
        },
    )

    assert code == 1
    assert stdout == ""
    assert stderr == "simplebroker: error: " + expected_template.format(
        path=config_path.resolve()
    )
    assert ".broker.toml" not in stderr


@pytest.mark.sqlite_only
def test_invalid_project_sqlite_target_diagnostic_names_selected_config_file(
    tmp_path: Path,
) -> None:
    """Target validation must use the selected config path as its context."""
    config_dir = tmp_path / ".weft"
    config_dir.mkdir()
    config_path = config_dir / "broker.toml"
    _write_project_config(config_path, backend="sqlite", target=" bad.db")

    code, stdout, stderr = run_cli(
        "--status",
        cwd=tmp_path,
        env={
            "BROKER_PROJECT_SCOPE": "1",
            "BROKER_PROJECT_CONFIG_PATH": ".weft",
            "BROKER_PROJECT_CONFIG_NAME": "broker.toml",
            "BROKER_TEST_BACKEND": "sqlite",
        },
    )

    assert code == 1
    assert stdout == ""
    assert f"{config_path.resolve()} sqlite target" in stderr
    assert ".broker.toml" not in stderr


def test_load_project_config_ignores_unknown_top_level_fields(
    tmp_path: Path,
) -> None:
    """Unknown top-level data is ignored by the current config contract."""
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        (
            "version = 1\n"
            'backend = "sqlite"\n'
            'target = "queue.db"\n'
            'description = "ignored"\n'
            "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS = 0\n"
        ),
        encoding="utf-8",
    )

    config_data = load_project_config(config_path)

    assert config_data == {
        "version": 1,
        "backend": "sqlite",
        "target": "queue.db",
        "backend_options": {},
    }


def test_project_config_preferred_over_legacy_project_database(workdir: Path) -> None:
    """Project config should beat legacy upward .broker.db discovery."""
    project_root = workdir / "project"
    nested = project_root / "src" / "child"
    nested.mkdir(parents=True)

    legacy_db = project_root / ".broker.db"
    with BrokerDB(str(legacy_db)) as db:
        db.write("tasks", "legacy message")

    backend, target, backend_options = _project_backend_config(
        sqlite_target="data/config.db"
    )
    _write_project_config(
        project_root / ".broker.toml",
        backend=backend,
        target=target,
        backend_options=backend_options,
    )

    env = {"BROKER_PROJECT_SCOPE": "1"}

    code, stdout, stderr = run_cli("init", cwd=nested, env=env)
    assert code == 0, stderr

    if backend == "sqlite":
        config_db = project_root / "data" / "config.db"
        assert config_db.exists()

    code, stdout, stderr = run_cli(
        "write", "tasks", "config message", cwd=nested, env=env
    )
    assert code == 0, stderr

    code, stdout, stderr = run_cli("read", "tasks", cwd=nested, env=env)
    assert code == 0, stderr
    assert stdout == "config message"

    code, stdout, stderr = run_cli(
        "-f",
        str(legacy_db),
        "read",
        "tasks",
        cwd=nested,
        env={"BROKER_PROJECT_SCOPE": "0"},
    )
    assert code == 0, stderr
    assert stdout == "legacy message"


def test_project_config_roundtrip_from_nested_directory(workdir: Path) -> None:
    """CLI should round-trip through a project config from a child directory."""
    project_root = workdir / "project"
    nested = project_root / "a" / "b"
    nested.mkdir(parents=True)

    backend, target, backend_options = _project_backend_config(
        sqlite_target="var/app.db"
    )
    _write_project_config(
        project_root / ".broker.toml",
        backend=backend,
        target=target,
        backend_options=backend_options,
    )

    env = {"BROKER_PROJECT_SCOPE": "1"}

    code, stdout, stderr = run_cli("init", cwd=nested, env=env)
    assert code == 0, stderr
    if backend == "sqlite":
        assert (project_root / "var" / "app.db").exists()

    code, stdout, stderr = run_cli("write", "jobs", "hello", cwd=nested, env=env)
    assert code == 0, stderr

    code, stdout, stderr = run_cli("--status", cwd=nested, env=env)
    assert code == 0, stderr
    assert "total_messages: 1" in stdout

    code, stdout, stderr = run_cli("read", "jobs", cwd=nested, env=env)
    assert code == 0, stderr
    assert stdout == "hello"


@pytest.mark.sqlite_only
@pytest.mark.parametrize(
    ("magic_value", "expected_error"),
    [
        (None, "simplebroker: error: Database is missing SimpleBroker metadata:"),
        (
            "wrong-app",
            (
                "simplebroker: error: Database has incorrect magic string "
                "(not a SimpleBroker database):"
            ),
        ),
    ],
)
def test_project_config_foreign_sqlite_db_reports_specific_primary_error(
    workdir: Path,
    magic_value: str | None,
    expected_error: str,
) -> None:
    """Foreign SQLite databases should not be reported as corrupted."""
    project_root = workdir / "project"
    project_root.mkdir()
    _write_project_config(
        project_root / ".broker.toml",
        backend="sqlite",
        target=".broker.db",
    )

    db_path = project_root / ".broker.db"
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        if magic_value is None:
            conn.execute("INSERT INTO meta VALUES ('version', '1.0')")
        else:
            conn.execute("INSERT INTO meta VALUES ('magic', ?)", (magic_value,))
        conn.commit()

    code, stdout, stderr = run_cli(
        "read",
        "myqueue",
        cwd=project_root,
        env={"BROKER_PROJECT_SCOPE": "1", "BROKER_TEST_BACKEND": "sqlite"},
    )

    assert code == 1
    assert stdout == ""
    assert expected_error in stderr
    assert "Database corruption or invalid format" not in stderr


@pytest.mark.sqlite_only
def test_public_resolve_broker_target_discovers_upward_sqlite_project(
    workdir: Path,
) -> None:
    """Public project discovery should find legacy sqlite targets upward."""

    project_root = workdir / "project"
    nested = project_root / "deep" / "child"
    nested.mkdir(parents=True)

    db_path = project_root / ".broker.db"
    with BrokerDB(str(db_path)) as db:
        db.write("tasks", "payload")

    resolved = resolve_broker_target(
        nested,
        config={"BROKER_DEFAULT_DB_NAME": ".broker.db"},
    )

    assert resolved is not None
    assert resolved.backend_name == "sqlite"
    assert resolved.target_path == db_path.resolve()
    assert resolved.project_root == project_root.resolve()
    assert (
        broker_root(
            nested,
            config={"BROKER_DEFAULT_DB_NAME": ".broker.db"},
        )
        == project_root.resolve()
    )


@pytest.mark.sqlite_only
def test_public_resolve_broker_target_prefers_legacy_sqlite_over_env_backend(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Legacy sqlite discovery should beat ambient backend env during discovery."""

    project_root = workdir / "project"
    nested = project_root / "deep" / "child"
    nested.mkdir(parents=True)

    db_path = project_root / ".broker.db"
    with BrokerDB(str(db_path)) as db:
        db.write("tasks", "payload")

    def unexpected_backend(*args: object, **kwargs: object) -> BrokerTarget | None:
        raise AssertionError(
            "env backend synthesis should not run before sqlite discovery"
        )

    monkeypatch.setattr(
        "simplebroker.project._configured_backend_target", unexpected_backend
    )

    resolved = resolve_broker_target(
        nested,
        config={
            "BROKER_BACKEND": "postgres",
            "BROKER_DEFAULT_DB_NAME": ".broker.db",
        },
    )

    assert resolved is not None
    assert resolved.backend_name == "sqlite"
    assert resolved.target_path == db_path.resolve()
    assert resolved.project_root == project_root.resolve()


@pytest.mark.sqlite_only
def test_public_target_for_directory_builds_default_sqlite_target(
    tmp_path: Path,
) -> None:
    """Explicit directory resolution should produce a backend-agnostic target."""

    target = target_for_directory(
        tmp_path,
        config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"},
    )

    assert target.backend_name == "sqlite"
    assert target.target_path == (tmp_path / ".weft" / "broker.db").resolve()
    assert target.project_root == tmp_path.resolve()


def test_public_broker_target_roundtrip_serialization(tmp_path: Path) -> None:
    """Broker targets should serialize cleanly for subprocess transport."""

    original = target_for_directory(
        tmp_path,
        config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"},
    )

    encoded = serialize_broker_target(original)
    decoded = deserialize_broker_target(encoded)

    assert decoded == original


def test_broker_target_shallow_copies_backend_options_without_freezing() -> None:
    """Targets own their option dict while retaining data-object compatibility."""
    nested_options = {"size": 2}
    source_options: dict[str, Any] = {
        "schema": "original",
        "pool": nested_options,
    }

    target = BrokerTarget("postgres", "postgresql://db/app", source_options)
    source_options["schema"] = "changed-after-construction"

    assert target.backend_options["schema"] == "original"
    assert target.backend_options["pool"] is nested_options
    assert isinstance(target.backend_options, dict)
    target.backend_options["mutable"] = True

    replaced = replace(target, target="postgresql://db/other")
    assert replaced.backend_options == target.backend_options
    assert replaced.backend_options is not target.backend_options
    assert pickle.loads(pickle.dumps(target)) == target


def test_broker_target_retains_mutable_backend_options_annotation() -> None:
    assert get_type_hints(BrokerTarget)["backend_options"] == dict[str, Any]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("[]", "must decode to an object"),
        ({"target": "queue.db"}, "missing backend_name"),
        ({"backend_name": "sqlite"}, "missing target"),
        (
            {
                "backend_name": "sqlite",
                "target": "queue.db",
                "backend_options": [],
            },
            "backend_options must be an object",
        ),
    ],
)
def test_deserialize_broker_target_rejects_malformed_transport_payloads(
    payload: str | dict[str, object],
    message: str,
) -> None:
    """Worker transport must reject incomplete or structurally invalid targets."""

    with pytest.raises(ValueError, match=message):
        deserialize_broker_target(payload)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("used_project_scope", "false", "used_project_scope must be a boolean"),
        ("legacy_sqlite_path_mode", 0, "legacy_sqlite_path_mode must be a boolean"),
        ("project_root", 42, "project_root must be a string or null"),
        ("config_path", [], "config_path must be a string or null"),
    ],
)
def test_deserialize_broker_target_rejects_invalid_typed_transport_fields(
    field: str,
    value: object,
    message: str,
) -> None:
    payload: dict[str, object] = {
        "backend_name": "sqlite",
        "target": "queue.db",
        field: value,
    }

    with pytest.raises(TypeError, match=message):
        deserialize_broker_target(payload)


def test_deserialize_broker_target_accepts_explicit_null_paths_and_booleans() -> None:
    target = deserialize_broker_target(
        {
            "backend_name": "sqlite",
            "target": "queue.db",
            "project_root": None,
            "config_path": "",
            "used_project_scope": False,
            "legacy_sqlite_path_mode": True,
            "future_transport_field": {"ignored": True},
        }
    )

    assert target.project_root is None
    assert target.config_path is None
    assert target.used_project_scope is False
    assert target.legacy_sqlite_path_mode is True


def test_resolve_target_defaults_to_sqlite_without_toml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without toml and without BROKER_BACKEND, sqlite discovery still returns None."""
    monkeypatch.chdir(tmp_path)

    target = resolve_broker_target(tmp_path, config=load_config())

    assert target is None
    assert broker_root(tmp_path, config=load_config()) is None


def test_resolve_target_unknown_backend_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unknown backend names should produce a user-facing availability error."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_BACKEND", "mysql")

    with pytest.raises(
        RuntimeError, match="Requested backend 'mysql' is not available"
    ):
        target_for_directory(tmp_path, config=load_config())


def test_resolve_target_missing_postgres_plugin_has_install_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing postgres plugin should recommend the extension package."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    config = load_config()

    def raise_unknown(name: str) -> None:
        del name
        raise UnknownBackendPluginError("wording intentionally does not identify it")

    monkeypatch.setattr("simplebroker.project.get_backend_plugin", raise_unknown)

    with pytest.raises(
        RuntimeError,
        match=(
            r"Requested backend 'postgres' is not available\. "
            r"Install simplebroker-pg or simplebroker\[pg\]\."
        ),
    ):
        target_for_directory(tmp_path, config=config)


def test_resolve_target_does_not_prose_match_other_plugin_runtime_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the typed unknown-plugin failure should receive install guidance."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    config = load_config()
    failure = RuntimeError("Unknown backend plugin: postgres")

    def raise_other_runtime_error(name: str) -> None:
        del name
        raise failure

    monkeypatch.setattr(
        "simplebroker.project.get_backend_plugin",
        raise_other_runtime_error,
    )

    with pytest.raises(RuntimeError) as exc_info:
        target_for_directory(tmp_path, config=config)

    assert exc_info.value is failure


def test_resolve_project_target_prefers_project_values_over_env_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Project configs should pass their own target fields ahead of env state."""

    config_path = tmp_path / ".broker.toml"
    _write_project_config(
        config_path,
        backend="postgres",
        target="postgresql://toml@tomlhost/tomldb",
        backend_options={"schema": "from_toml"},
    )
    monkeypatch.setenv("BROKER_BACKEND_TARGET", "postgresql://env@envhost/envdb")
    monkeypatch.setenv("BROKER_BACKEND_PASSWORD", "secret")

    seen: dict[str, object] = {}

    class DummyPlugin:
        def init_backend(
            self,
            config,
            *,
            toml_target="",
            toml_options=None,
        ):
            seen["config"] = dict(config)
            seen["toml_target"] = toml_target
            seen["toml_options"] = dict(toml_options or {})
            return {
                "target": toml_target,
                "backend_options": dict(toml_options or {}),
            }

    monkeypatch.setattr(
        "simplebroker._project_config.get_backend_plugin",
        lambda name="postgres": DummyPlugin(),
    )

    resolved = resolve_project_target(config_path)

    assert resolved.backend_name == "postgres"
    assert resolved.target == "postgresql://toml@tomlhost/tomldb"
    assert resolved.backend_options == {"schema": "from_toml"}
    assert seen["toml_target"] == "postgresql://toml@tomlhost/tomldb"
    assert seen["toml_options"] == {"schema": "from_toml"}
    assert isinstance(seen["config"], dict)
    config_dict = seen["config"]
    assert isinstance(config_dict, dict)
    assert config_dict["BROKER_BACKEND_TARGET"] == ""
    assert config_dict["BROKER_BACKEND_PASSWORD"] == "secret"


def test_toml_overrides_env_backend_in_public_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A direct project config should win over BROKER_BACKEND env selection."""
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    _write_project_config(tmp_path / ".broker.toml", backend="sqlite", target="x.db")

    target = target_for_directory(tmp_path, config=load_config())

    assert target.backend_name == "sqlite"


@pytest.mark.sqlite_only
def test_project_config_discovery_uses_configured_path_and_name(
    tmp_path: Path,
) -> None:
    """Project TOML discovery should be namespaceable like SQLite DB discovery."""

    project_root = tmp_path / "project"
    nested = project_root / "src" / "tasks"
    nested.mkdir(parents=True)

    _write_project_config(
        project_root / ".broker.toml",
        backend="sqlite",
        target="root.db",
    )
    namespaced_config = project_root / ".weft" / "broker.toml"
    namespaced_config.parent.mkdir()
    _write_project_config(
        namespaced_config,
        backend="sqlite",
        target="weft.db",
    )

    config = {
        "BROKER_PROJECT_SCOPE": True,
        "BROKER_PROJECT_CONFIG_PATH": ".weft",
        "BROKER_PROJECT_CONFIG_NAME": "broker.toml",
    }

    discovered = find_project_config(nested, config=config)
    target = resolve_broker_target(nested, config=config)

    assert discovered == namespaced_config.resolve()
    assert target is not None
    assert target.config_path == namespaced_config.resolve()
    assert target.target_path == (project_root / ".weft" / "weft.db").resolve()


@pytest.mark.sqlite_only
def test_project_config_discovery_honors_an_absolute_config_location(
    tmp_path: Path,
) -> None:
    """An absolute config directory should not be rebased under the caller."""

    config_dir = tmp_path / "central-config"
    config_dir.mkdir()
    config_path = config_dir / "broker.toml"
    _write_project_config(config_path, backend="sqlite", target="queue.db")
    unrelated_start = tmp_path / "workspace" / "nested"
    unrelated_start.mkdir(parents=True)
    config = {
        "BROKER_PROJECT_CONFIG_PATH": str(config_dir),
        "BROKER_PROJECT_CONFIG_NAME": "broker.toml",
    }

    assert project_config_path_for_directory(unrelated_start, config=config) == (
        config_path.resolve()
    )
    assert find_project_config(unrelated_start, config=config) == config_path.resolve()


@pytest.mark.sqlite_only
def test_target_for_directory_uses_configured_project_config_location(
    tmp_path: Path,
) -> None:
    """Explicit-root resolution should check the configured TOML location."""

    config = {
        "BROKER_PROJECT_CONFIG_PATH": ".weft",
        "BROKER_PROJECT_CONFIG_NAME": "broker.toml",
        "BROKER_DEFAULT_DB_NAME": ".weft/broker.db",
    }
    config_path = project_config_path_for_directory(tmp_path, config=config)
    config_path.parent.mkdir()
    _write_project_config(config_path, backend="sqlite", target="pg-owned.db")

    target = target_for_directory(tmp_path, config=config)

    assert config_path == (tmp_path / ".weft" / "broker.toml").resolve()
    assert target.config_path == config_path
    assert target.target_path == (tmp_path / ".weft" / "pg-owned.db").resolve()


@pytest.mark.sqlite_only
def test_cli_project_scope_prefers_legacy_sqlite_over_env_backend(
    workdir: Path,
) -> None:
    """CLI project scope should keep using the discovered sqlite project."""

    project_root = workdir / "project"
    nested = project_root / "src"
    nested.mkdir(parents=True)

    code, stdout, stderr = run_cli("init", cwd=project_root)
    assert code == 0, stderr

    env = {
        "BROKER_PROJECT_SCOPE": "1",
        "BROKER_BACKEND": "postgres",
    }
    code, stdout, stderr = run_cli("write", "jobs", "hello", cwd=nested, env=env)
    assert code == 0, stderr

    code, stdout, stderr = run_cli("read", "jobs", cwd=nested, env=env)
    assert code == 0, stderr
    assert stdout == "hello"


@pytest.mark.sqlite_only
def test_cli_explicit_file_beats_env_backend_selection(workdir: Path) -> None:
    """An explicit sqlite file should beat ambient backend env selection."""

    env = {"BROKER_BACKEND": "postgres"}

    code, stdout, stderr = run_cli(
        "-f",
        "explicit.db",
        "write",
        "jobs",
        "hello",
        cwd=workdir,
        env=env,
    )
    assert code == 0, stderr
    assert (workdir / "explicit.db").exists()

    code, stdout, stderr = run_cli(
        "-f",
        "explicit.db",
        "read",
        "jobs",
        cwd=workdir,
        env=env,
    )
    assert code == 0, stderr
    assert stdout == "hello"
