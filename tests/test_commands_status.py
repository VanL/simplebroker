"""Tests for the cmd_status helper in simplebroker.commands."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from simplebroker._constants import EXIT_SUCCESS
from simplebroker._targets import BrokerTarget
from simplebroker.commands import cmd_status

from .helper_scripts.broker_factory import make_queue

pytestmark = [pytest.mark.shared]


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
        self, broker_target: BrokerTarget, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """cmd_status prints database metrics and returns success."""
        # Populate the database with a small amount of data via public API
        queue = make_queue("tasks", broker_target)
        try:
            queue.write("hello")
        finally:
            queue.close()

        rc = cmd_status(broker_target)
        captured = capsys.readouterr()

        assert rc == EXIT_SUCCESS
        assert captured.err == ""

        stats = parse_status_output(captured.out)
        assert stats["total_messages"] == 1
        assert stats["last_timestamp"] > 0
        if broker_target.backend_name == "redis":
            assert stats["db_size"] == 0
        else:
            assert stats["db_size"] > 0

    def test_cmd_status_json_output(
        self, broker_target: BrokerTarget, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """cmd_status emits JSON when requested."""
        queue = make_queue("tasks", broker_target)
        try:
            queue.write("hello")
        finally:
            queue.close()

        rc = cmd_status(broker_target, json_output=True)
        captured = capsys.readouterr()

        assert rc == EXIT_SUCCESS
        assert captured.err == ""

        payload = json.loads(captured.out)
        assert payload["total_messages"] == 1
        assert isinstance(payload["last_timestamp"], str)
        assert len(payload["last_timestamp"]) == 19
        assert int(payload["last_timestamp"]) > 0
        if broker_target.backend_name == "redis":
            assert payload["db_size"] == 0
        else:
            assert payload["db_size"] > 0

    def test_cmd_status_raises_operational_failure(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Direct command callers receive the typed storage failure."""
        corrupt = tmp_path / "corrupt.db"
        corrupt.write_text("not a SQLite database", encoding="utf-8")

        with pytest.raises(RuntimeError, match="Failed to get database connection"):
            cmd_status(str(corrupt))
        captured = capsys.readouterr()

        assert captured.err == ""
        assert captured.out == ""
