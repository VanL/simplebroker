"""Tests for the broker status command."""

from __future__ import annotations

import json

from .conftest import run_cli


def parse_status(output: str) -> dict[str, int]:
    """Parse status command output into a dictionary of ints."""
    stats: dict[str, int] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        key, _, value = line.partition(":")
        stats[key.strip()] = int(value.strip())
    return stats


class TestStatusCommand:
    """Validate CLI status reporting."""

    def test_status_on_new_database(self, workdir):
        rc, out, err = run_cli("--status", cwd=workdir)

        assert rc == 0
        assert err == ""

        stats = parse_status(out)
        assert stats["total_messages"] == 0
        assert stats["last_timestamp"] == 0
        assert stats["db_size"] >= 0

    def test_status_after_writes(self, workdir):
        # Baseline status
        rc, out, err = run_cli("--status", cwd=workdir)
        assert rc == 0
        assert err == ""
        baseline = parse_status(out)

        rc, _, _ = run_cli("write", "tasks", "first", cwd=workdir)
        assert rc == 0
        rc, _, _ = run_cli("write", "tasks", "second", cwd=workdir)
        assert rc == 0

        rc, out, err = run_cli("--status", cwd=workdir)
        assert rc == 0
        assert err == ""

        stats = parse_status(out)
        assert stats["total_messages"] == 2
        assert stats["last_timestamp"] >= baseline["last_timestamp"]
        assert stats["last_timestamp"] > 0
        assert stats["db_size"] >= baseline["db_size"]

    def test_status_json_output(self, workdir):
        rc, out, err = run_cli("--status", "--json", cwd=workdir)

        assert rc == 0
        assert err == ""

        payload = json.loads(out)
        assert payload["total_messages"] == 0
        assert payload["last_timestamp"] == 0
        assert payload["db_size"] >= 0

    def test_status_mutually_exclusive(self, workdir):
        rc, out, err = run_cli(
            "--status", "--json", "write", "queue", "msg", cwd=workdir
        )

        assert rc == 1
        assert "--status cannot be used with commands" in err
