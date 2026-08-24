"""Targeted tests for simplebroker.cli.main helper paths."""

from __future__ import annotations

import argparse
import json
import types

import pytest

from simplebroker import cli
from simplebroker._targets import BrokerTarget


def test_main_prints_help_when_no_args(monkeypatch, capsys):
    dummy_sys = types.SimpleNamespace(
        argv=["broker"], stderr=cli.sys.stderr, stdout=cli.sys.stdout
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == cli.EXIT_SUCCESS
    assert "Simple message broker" in captured.out


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
