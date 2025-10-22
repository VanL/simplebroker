"""Targeted tests for simplebroker.cli.main helper paths."""

from __future__ import annotations

import json
import types

import pytest

import simplebroker.cli as cli


@pytest.fixture(autouse=True)
def reset_parser_cache():
    """Ensure each test starts with a fresh parser cache."""
    cli._PARSER_CACHE = None
    yield
    cli._PARSER_CACHE = None


def test_main_prints_help_when_no_args(monkeypatch, capsys):
    dummy_sys = types.SimpleNamespace(
        argv=["broker"], stderr=cli.sys.stderr, stdout=cli.sys.stdout
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == cli.EXIT_SUCCESS
    assert "Simple message broker" in captured.out


def test_main_status_json_flag(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    dummy_sys = types.SimpleNamespace(
        argv=["broker", "--status", "--json"],
        stderr=cli.sys.stderr,
        stdout=cli.sys.stdout,
    )
    monkeypatch.setattr(cli, "sys", dummy_sys)

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == cli.EXIT_SUCCESS
    payload = json.loads(captured.out)
    assert payload["total_messages"] == 0


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
