"""Targeted tests for simplebroker.cli.main helper paths."""

from __future__ import annotations

import argparse
import json
import os
import types
import warnings
from pathlib import Path

import pytest

from simplebroker import _paths, cli
from simplebroker._exceptions import DatabaseError, DataError, InvalidConfigError
from simplebroker._targets import BrokerTarget


class _ForeignBackendError(Exception):
    """Synthetic third-party failure outside SimpleBroker's hierarchy."""


@pytest.mark.parametrize(
    "argv",
    [
        ["broker", "read", "q", "--after", "bad"],
        ["broker", "read", "q", "--before", "bad"],
        ["broker", "peek", "q", "--after", "bad"],
        ["broker", "peek", "q", "--before", "bad"],
        ["broker", "move", "src", "dst", "--after", "bad"],
        ["broker", "move", "src", "dst", "--before", "bad"],
        ["broker", "watch", "q", "--after", "bad"],
    ],
)
def test_invalid_timestamp_never_observes_target(
    tmp_path,
    monkeypatch,
    capsys,
    argv,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.sys, "argv", argv)
    target_observations: list[argparse.Namespace] = []

    def observe_target(args, *, config):
        target_observations.append(args)
        raise AssertionError("invalid timestamp must fail before target resolution")

    monkeypatch.setattr(cli, "_resolve_target", observe_target)

    assert cli.main() == cli.EXIT_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "invalid timestamp" in captured.err.lower()
    assert target_observations == []


def test_quiet_suppresses_only_owned_runtime_warnings(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["broker", "--quiet", "write", "jobs", "payload"],
    )

    def warn_from_command(*_args, **_kwargs):
        warnings.warn("unrelated runtime warning", RuntimeWarning, stacklevel=2)
        return cli.EXIT_SUCCESS

    monkeypatch.setattr(cli.commands, "cmd_write", warn_from_command)

    with pytest.warns(RuntimeWarning, match="unrelated runtime warning"):
        assert cli.main() == cli.EXIT_SUCCESS


def test_omitted_terminal_message_is_invalid_argument_json(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["broker", "write", "--json", "jobs"],
    )
    monkeypatch.setattr(cli.sys, "stdin", types.SimpleNamespace(isatty=lambda: True))

    assert cli.main() == cli.EXIT_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"] == "INVALID_ARGUMENT"
    assert "message is required" in payload["message"]


@pytest.mark.parametrize(
    "error",
    [DataError("dual database value error"), ValueError("generic value error")],
)
def test_dispatch_value_error_fallbacks_remain_error_json(
    tmp_path,
    monkeypatch,
    capsys,
    error,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["broker", "write", "--json", "jobs", "payload"],
    )

    def fail_write(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(cli.commands, "cmd_write", fail_write)

    assert cli.main() == cli.EXIT_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"] == "ERROR"
    assert payload["retryable"] is False


@pytest.mark.parametrize("phase", ["resolution", "preparation"])
@pytest.mark.parametrize(
    "error",
    [
        DatabaseError("database failure"),
        ValueError("generic value failure"),
        RuntimeError("unknown failure"),
        _ForeignBackendError("foreign plugin failure"),
    ],
)
def test_pre_dispatch_failures_use_cause_classifier_json(
    tmp_path,
    monkeypatch,
    capsys,
    phase,
    error,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["broker", "list", "--json"],
    )

    if phase == "resolution":
        monkeypatch.setattr(
            cli,
            "_resolve_target",
            lambda _args, *, config: (_ for _ in ()).throw(error),
        )
    else:
        monkeypatch.setattr(
            cli,
            "_validate_legacy_sqlite_target",
            lambda _args, _target, *, config: (_ for _ in ()).throw(error),
        )

    assert cli.main() == cli.EXIT_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload == {
        "error": "ERROR",
        "message": str(error),
        "retryable": False,
    }
    assert "Traceback" not in captured.err


def test_resolution_invalid_config_keeps_outer_plain_text_boundary(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.sys, "argv", ["broker", "list", "--json"])
    error = InvalidConfigError(
        key="BROKER_BACKEND",
        source="injected test",
        expected="a registered backend",
        value_display="'unknown'",
    )

    def fail_resolution(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(cli, "_resolve_target", fail_resolution)

    assert cli.main() == cli.EXIT_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == f"{cli.PROG_NAME}: {error}\n"
    assert not captured.err.lstrip().startswith("{")
    assert "Traceback" not in captured.err


def test_resolution_keyboard_interrupt_keeps_outer_interrupt_boundary(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.sys, "argv", ["broker", "list", "--json"])

    def interrupt_resolution(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_resolve_target", interrupt_resolution)

    assert cli.main() == cli.EXIT_INTERRUPTED
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "interrupted" in captured.err
    assert not captured.err.lstrip().startswith("{")
    assert "Traceback" not in captured.err


def test_inaccessible_database_parent_remains_operational_error_json(
    tmp_path,
    monkeypatch,
    capsys,
):
    target = tmp_path / "selected" / "broker.db"
    target.parent.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli.sys,
        "argv",
        ["broker", "-f", str(target), "list", "--json"],
    )

    def deny_execute(path, mode):
        return not (Path(path) == target.parent and mode == os.X_OK)

    monkeypatch.setattr(_paths.os, "access", deny_execute)

    assert cli.main() == cli.EXIT_ERROR
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["error"] == "ERROR"
    assert "not accessible" in payload["message"]


def test_main_prints_help_when_no_args(monkeypatch, capsys):
    dummy_sys = types.SimpleNamespace(
        argv=["broker"], stderr=cli.sys.stderr, stdout=cli.sys.stdout
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == cli.EXIT_SUCCESS
    assert "Simple message broker" in captured.out


def test_root_help_advertises_action_only_json(monkeypatch, capsys):
    monkeypatch.setattr(cli.sys, "argv", ["broker", "--help"])

    assert cli.main() == cli.EXIT_SUCCESS
    captured = capsys.readouterr()
    help_text = " ".join(captured.out.split())

    assert "--json" in help_text
    assert "--status, --cleanup, and --vacuum" in help_text


@pytest.mark.parametrize(
    "argv",
    [
        ["broker", "--json"],
        ["broker", "--json", "delete", "jobs"],
        ["broker", "delete", "jobs", "--json"],
    ],
)
def test_action_only_json_remains_invalid_without_compatible_root_action(
    monkeypatch,
    capsys,
    argv,
):
    monkeypatch.setattr(cli.sys, "argv", argv)

    assert cli.main() == cli.EXIT_ERROR
    captured = capsys.readouterr()

    assert captured.out == ""
    assert "--json" in captured.err


@pytest.mark.parametrize(
    "argv",
    [
        ["broker", "--json", "--help"],
        ["broker", "--help", "--json"],
    ],
)
def test_help_remains_terminal_with_action_only_json_in_either_order(
    monkeypatch,
    capsys,
    argv,
):
    monkeypatch.setattr(cli.sys, "argv", argv)

    assert cli.main() == cli.EXIT_SUCCESS
    captured = capsys.readouterr()
    assert "Simple message broker" in captured.out
    assert captured.err == ""


def test_main_status_json_flag_before_status(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    dummy_sys = types.SimpleNamespace(
        argv=["broker", "--json", "--status"],
        stderr=cli.sys.stderr,
        stdout=cli.sys.stdout,
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == cli.EXIT_SUCCESS
    payload = json.loads(captured.out)
    assert payload["total_messages"] == 0


def test_main_preprocesses_each_invocation_once(tmp_path, monkeypatch, capsys):
    """Global action JSON detection must share the normalization scan."""
    monkeypatch.chdir(tmp_path)
    dummy_sys = types.SimpleNamespace(
        argv=["broker", "--json", "--status"],
        stderr=cli.sys.stderr,
        stdout=cli.sys.stdout,
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    calls = 0
    original_process = cli.ArgumentProcessor.process

    def counting_process(self, argv):
        nonlocal calls
        calls += 1
        return original_process(self, argv)

    monkeypatch.setattr(cli.ArgumentProcessor, "process", counting_process)

    assert cli.main() == cli.EXIT_SUCCESS
    assert calls == 1
    assert json.loads(capsys.readouterr().out)["total_messages"] == 0


def test_malformed_legacy_target_fails_closed_before_plugin_use(
    tmp_path, monkeypatch, capsys
):
    """Malformed internal targets use the normal JSON error boundary."""
    monkeypatch.chdir(tmp_path)
    malformed_target = BrokerTarget(
        backend_name="malformed",
        target="password=do-not-leak",
        legacy_sqlite_path_mode=True,
    )
    monkeypatch.setattr(
        cli, "_resolve_target", lambda _args, *, config: malformed_target
    )

    plugin_requests: list[str] = []

    def fail_if_plugin_requested(name: str):
        plugin_requests.append(name)
        raise AssertionError("plugin lookup must not run")

    monkeypatch.setattr(
        "simplebroker._backend_plugins.get_backend_plugin",
        fail_if_plugin_requested,
    )

    for argv in (
        ["broker", "--json", "--cleanup"],
        ["broker", "list", "--json"],
    ):
        dummy_sys = types.SimpleNamespace(
            argv=argv,
            stderr=cli.sys.stderr,
            stdout=cli.sys.stdout,
        )
        monkeypatch.setattr(cli, "sys", dummy_sys)

        assert cli.main() == cli.EXIT_ERROR
        captured = capsys.readouterr()
        assert captured.out == ""
        payload = json.loads(captured.err)
        assert payload["error"] == "ERROR"
        assert payload["message"] == "Legacy SQLite target has no filesystem path"
        assert "do-not-leak" not in captured.err
        assert "Traceback" not in captured.err

    assert plugin_requests == []


def test_main_dispatches_validated_canonical_relative_target(
    tmp_path, monkeypatch, capsys
):
    """Command dispatch should use the contained path that was validated."""
    monkeypatch.chdir(tmp_path)
    target_path = tmp_path / "data" / "queue.db"
    target_path.parent.mkdir()
    link_path = tmp_path / "queue.db"
    try:
        link_path.symlink_to(target_path)
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    dummy_sys = types.SimpleNamespace(
        argv=["broker", "-f", "queue.db", "write", "jobs", "payload"],
        stderr=cli.sys.stderr,
        stdout=cli.sys.stdout,
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    dispatched: list[BrokerTarget] = []

    def capture_dispatch(args, resolved_target, parser, *, config):
        dispatched.append(resolved_target)
        return cli.EXIT_SUCCESS

    monkeypatch.setattr(cli, "_dispatch_command", capture_dispatch)

    assert cli.main() == cli.EXIT_SUCCESS
    assert capsys.readouterr().err == ""
    assert len(dispatched) == 1
    assert dispatched[0].target == str(target_path.resolve())


def test_main_status_uses_validated_canonical_relative_target(
    tmp_path, monkeypatch, capsys
):
    """Status should receive the same contained target that was validated."""
    monkeypatch.chdir(tmp_path)
    target_path = tmp_path / "data" / "queue.db"
    target_path.parent.mkdir()
    link_path = tmp_path / "queue.db"
    try:
        link_path.symlink_to(target_path)
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    dummy_sys = types.SimpleNamespace(
        argv=["broker", "-f", "queue.db", "--status"],
        stderr=cli.sys.stderr,
        stdout=cli.sys.stdout,
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    received: list[BrokerTarget] = []

    def capture_status(resolved_target, *, json_output, config):
        received.append(resolved_target)
        return cli.EXIT_SUCCESS

    monkeypatch.setattr(cli.commands, "cmd_status", capture_status)

    assert cli.main() == cli.EXIT_SUCCESS
    assert capsys.readouterr().err == ""
    assert [target.target for target in received] == [str(target_path.resolve())]


def test_main_vacuum_uses_validated_canonical_relative_target(
    tmp_path, monkeypatch, capsys
):
    """Vacuum should receive the same contained target that was validated."""
    monkeypatch.chdir(tmp_path)
    target_path = tmp_path / "data" / "queue.db"
    target_path.parent.mkdir()
    link_path = tmp_path / "queue.db"
    try:
        link_path.symlink_to(target_path)
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    dummy_sys = types.SimpleNamespace(
        argv=["broker", "-f", "queue.db", "--vacuum"],
        stderr=cli.sys.stderr,
        stdout=cli.sys.stdout,
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    received: list[BrokerTarget] = []

    def capture_vacuum(args, resolved_target, *, status_json_output, config):
        received.append(resolved_target)
        return cli.EXIT_SUCCESS

    monkeypatch.setattr(cli, "_run_vacuum", capture_vacuum)

    assert cli.main() == cli.EXIT_SUCCESS
    assert capsys.readouterr().err == ""
    assert [target.target for target in received] == [str(target_path.resolve())]


def test_relative_target_resolution_error_has_no_lexical_fallback(
    tmp_path, monkeypatch
):
    """A containment-required resolution error must reach error translation."""

    def fail_resolution(path):
        raise RuntimeError("injected resolution failure")

    monkeypatch.setattr(cli, "_resolve_symlinks_safely", fail_resolution)

    with pytest.raises(ValueError, match="Could not safely resolve relative"):
        cli._resolve_legacy_sqlite_path(
            tmp_path / "queue.db",
            working_dir=tmp_path,
            containment_required=True,
        )


def test_compound_default_is_finalized_before_canonical_containment(tmp_path):
    """The configured final candidate, not a provisional name, is checked."""
    target_dir = tmp_path / "data"
    target_dir.mkdir()
    configured_dir = tmp_path / "state"
    try:
        configured_dir.symlink_to(target_dir, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    args = argparse.Namespace(
        dir=tmp_path,
        file=cli.DEFAULT_DB_NAME,
        command="write",
        _file_explicitly_provided=False,
    )
    unresolved_target = BrokerTarget(
        backend_name="sqlite",
        target=str(tmp_path / cli.DEFAULT_DB_NAME),
        legacy_sqlite_path_mode=True,
    )

    prepared = cli._validate_legacy_sqlite_target(
        args,
        unresolved_target,
        config={"BROKER_DEFAULT_DB_NAME": "state/queue.db"},
    )

    assert prepared.target == str((target_dir / "queue.db").resolve())


def test_repeated_main_calls_rebuild_defaults_from_invocation_snapshot(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)

    def run_status() -> int:
        dummy_sys = types.SimpleNamespace(
            argv=["broker", "--status"],
            stderr=cli.sys.stderr,
            stdout=cli.sys.stdout,
        )
        monkeypatch.setattr(cli, "sys", dummy_sys)
        return cli.main()

    monkeypatch.setenv("BROKER_DEFAULT_DB_NAME", "first.db")
    assert run_status() == cli.EXIT_SUCCESS
    capsys.readouterr()

    monkeypatch.setenv("BROKER_DEFAULT_DB_NAME", "second.db")
    assert run_status() == cli.EXIT_SUCCESS
    capsys.readouterr()

    assert (tmp_path / "first.db").is_file()
    assert (tmp_path / "second.db").is_file()


def _run_main_with_argv(monkeypatch, argv: list[str]) -> int:
    dummy_sys = types.SimpleNamespace(
        argv=argv,
        stderr=cli.sys.stderr,
        stdout=cli.sys.stdout,
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)
    return cli.main()


def test_explicit_dir_overrides_default_db_location_for_dispatch_and_cleanup(
    tmp_path,
    monkeypatch,
    capsys,
):
    """Explicit -d wins over BROKER_DEFAULT_DB_LOCATION on one shared target."""
    location_dir = tmp_path / "location"
    explicit_dir = tmp_path / "explicit"
    location_dir.mkdir()
    explicit_dir.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_DEFAULT_DB_LOCATION", str(location_dir.resolve()))

    argv = ["broker", "-d", str(explicit_dir), "write", "jobs", "payload"]
    assert _run_main_with_argv(monkeypatch, argv) == cli.EXIT_SUCCESS
    capsys.readouterr()

    assert (explicit_dir / cli.DEFAULT_DB_NAME).is_file()
    assert not (location_dir / cli.DEFAULT_DB_NAME).exists()

    argv = ["broker", "-d", str(explicit_dir), "--cleanup"]
    assert _run_main_with_argv(monkeypatch, argv) == cli.EXIT_SUCCESS
    capsys.readouterr()

    assert not (explicit_dir / cli.DEFAULT_DB_NAME).exists()


def test_default_db_location_owns_target_without_explicit_dir(
    tmp_path,
    monkeypatch,
    capsys,
):
    """Without -d, BROKER_DEFAULT_DB_LOCATION owns dispatch and cleanup."""
    location_dir = tmp_path / "location"
    work_dir = tmp_path / "work"
    location_dir.mkdir()
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)
    monkeypatch.setenv("BROKER_DEFAULT_DB_LOCATION", str(location_dir.resolve()))

    argv = ["broker", "write", "jobs", "payload"]
    assert _run_main_with_argv(monkeypatch, argv) == cli.EXIT_SUCCESS
    capsys.readouterr()

    assert (location_dir / cli.DEFAULT_DB_NAME).is_file()
    assert not (work_dir / cli.DEFAULT_DB_NAME).exists()

    argv = ["broker", "--cleanup"]
    assert _run_main_with_argv(monkeypatch, argv) == cli.EXIT_SUCCESS
    capsys.readouterr()

    assert not (location_dir / cli.DEFAULT_DB_NAME).exists()
