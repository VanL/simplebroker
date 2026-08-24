"""Targeted tests for simplebroker.cli.main helper paths."""

from __future__ import annotations

import json
import types

from simplebroker import cli


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
