"""Targeted tests for simplebroker.cli.main helper paths."""

from __future__ import annotations

import json
import types

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
