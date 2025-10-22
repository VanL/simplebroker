"""Tests for the cmd_status helper in simplebroker.commands."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from simplebroker._constants import EXIT_ERROR, EXIT_SUCCESS
from simplebroker.commands import cmd_status
from simplebroker.sbqueue import Queue


def parse_status_output(output: str) -> dict[str, int]:
    """Convert key:value status output into a dictionary."""
    stats: dict[str, int] = {}
    for line in output.strip().splitlines():
        if not line:
            continue
        key, _, value = line.partition(":")
        stats[key.strip()] = int(value.strip())
    return stats


class TestCmdStatus:
    """Unit tests for commands.cmd_status."""

    def test_cmd_status_success(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """cmd_status prints database metrics and returns success."""
        db_path = tmp_path / ".broker.db"

        # Populate the database with a small amount of data via public API
        queue = Queue("tasks", db_path=str(db_path))
        queue.write("hello")

        rc = cmd_status(str(db_path))
        captured = capsys.readouterr()

        assert rc == EXIT_SUCCESS
        assert captured.err == ""

        stats = parse_status_output(captured.out)
        assert stats["total_messages"] == 1
        assert stats["last_timestamp"] > 0
        assert stats["db_size"] > 0

    def test_cmd_status_json_output(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """cmd_status emits JSON when requested."""
        db_path = tmp_path / ".broker.db"

        queue = Queue("tasks", db_path=str(db_path))
        queue.write("hello")

        rc = cmd_status(str(db_path), json_output=True)
        captured = capsys.readouterr()

        assert rc == EXIT_SUCCESS
        assert captured.err == ""

        payload = json.loads(captured.out)
        assert payload["total_messages"] == 1
        assert payload["last_timestamp"] > 0
        assert payload["db_size"] > 0

    def test_cmd_status_handles_exceptions(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """cmd_status surfaces errors from DBConnection and returns EXIT_ERROR."""

        class BrokenConnection:
            def __init__(self, *_args, **_kwargs) -> None:  # pragma: no cover - trivial
                pass

            def __enter__(self):
                raise RuntimeError("boom")

            def __exit__(self, *_args) -> None:  # pragma: no cover - trivial
                return None

        monkeypatch.setattr("simplebroker.commands.DBConnection", BrokenConnection)

        rc = cmd_status("/tmp/nonexistent.db")
        captured = capsys.readouterr()

        assert rc == EXIT_ERROR
        assert "simplebroker: error" in captured.err
        assert captured.out == ""
