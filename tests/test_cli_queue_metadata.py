"""Black-box CLI coverage for queue metadata commands."""

from __future__ import annotations

import json
from pathlib import Path

from tests.conftest import run_cli


def _json_lines(output: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in output.splitlines()]


def test_cli_list_stats_json_reports_claimed_counts_and_filters(workdir: Path) -> None:
    for queue, message in (
        ("jobs.alpha", "a1"),
        ("jobs.alpha", "a2"),
        ("jobs.beta", "b1"),
        ("events.alpha", "e1"),
    ):
        rc, _, err = run_cli("write", queue, message, cwd=workdir)
        assert rc == 0, err

    rc, out, err = run_cli("read", "jobs.alpha", cwd=workdir)
    assert rc == 0, err
    assert out == "a1"

    rc, out, err = run_cli(
        "list",
        "--stats",
        "--prefix",
        "jobs.",
        "--json",
        cwd=workdir,
    )

    assert rc == 0, err
    rows = sorted(_json_lines(out), key=lambda row: str(row["queue"]))
    assert rows == [
        {
            "queue": "jobs.alpha",
            "pending": 1,
            "claimed": 1,
            "total": 2,
            "exists": True,
        },
        {
            "queue": "jobs.beta",
            "pending": 1,
            "claimed": 0,
            "total": 1,
            "exists": True,
        },
    ]


def test_cli_stats_and_exists_preserve_missing_queue_exit_code(workdir: Path) -> None:
    rc, _, err = run_cli("write", "jobs", "first", cwd=workdir)
    assert rc == 0, err
    rc, _, err = run_cli("read", "jobs", cwd=workdir)
    assert rc == 0, err

    rc, out, err = run_cli("stats", "jobs", "--json", cwd=workdir)
    assert rc == 0, err
    assert json.loads(out) == {
        "queue": "jobs",
        "pending": 0,
        "claimed": 1,
        "total": 1,
        "exists": True,
    }

    rc, out, err = run_cli("exists", "missing", "--json", cwd=workdir)
    assert rc == 2
    assert err == ""
    assert json.loads(out) == {"queue": "missing", "exists": False}
