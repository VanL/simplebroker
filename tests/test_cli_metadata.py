"""CLI tests for targeted queue metadata commands."""

from __future__ import annotations

import json
from pathlib import Path

from .conftest import run_cli


def _json_lines(output: str) -> list[dict[str, object]]:
    if not output:
        return []
    return [json.loads(line) for line in output.splitlines()]


def test_exists_exit_codes(workdir) -> None:
    rc, out, _ = run_cli("exists", "missing", cwd=workdir)
    assert rc == 2
    assert out == ""

    assert run_cli("write", "jobs", "one", cwd=workdir)[0] == 0
    rc, out, _ = run_cli("exists", "jobs", cwd=workdir)
    assert rc == 0
    assert out == ""

    assert run_cli("read", "jobs", cwd=workdir)[0] == 0
    rc, out, _ = run_cli("exists", "jobs", cwd=workdir)
    assert rc == 0
    assert out == ""


def test_exists_json_output(workdir) -> None:
    assert run_cli("write", "jobs", "one", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("exists", "jobs", "--json", cwd=workdir)
    assert rc == 0
    assert json.loads(out) == {"queue": "jobs", "exists": True}

    rc, out, _ = run_cli("exists", "missing", "--json", cwd=workdir)
    assert rc == 2
    assert json.loads(out) == {"queue": "missing", "exists": False}


def test_stats_plain_output(workdir) -> None:
    for message in ("one", "two", "three"):
        assert run_cli("write", "jobs", message, cwd=workdir)[0] == 0

    rc, out, _ = run_cli("stats", "jobs", cwd=workdir)
    assert rc == 0
    assert out == "jobs: 3"

    assert run_cli("read", "jobs", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("stats", "jobs", cwd=workdir)
    assert rc == 0
    assert out == "jobs: 2 (3 total, 1 claimed)"

    rc, out, _ = run_cli("stats", "missing", cwd=workdir)
    assert rc == 0
    assert out == "missing: 0"


def test_stats_json_output(workdir) -> None:
    for message in ("one", "two"):
        assert run_cli("write", "jobs", message, cwd=workdir)[0] == 0
    assert run_cli("read", "jobs", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("stats", "jobs", "--json", cwd=workdir)
    assert rc == 0
    payload = json.loads(out)
    assert payload == {
        "queue": "jobs",
        "pending": 1,
        "claimed": 1,
        "total": 2,
        "exists": True,
    }


def test_list_prefix_without_stats_prints_names_and_includes_claimed_only(
    workdir,
) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        assert run_cli("write", queue, f"message for {queue}", cwd=workdir)[0] == 0
    assert run_cli("read", "weft.jobs.b", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("list", "--prefix", "weft.jobs.", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["weft.jobs.a", "weft.jobs.b"]


def test_list_prefix_with_stats_includes_claimed_only_queues(workdir) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        assert run_cli("write", queue, f"message for {queue}", cwd=workdir)[0] == 0
    assert run_cli("read", "weft.jobs.b", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("list", "--prefix", "weft.jobs.", "--stats", cwd=workdir)
    assert rc == 0
    assert out.splitlines()[:2] == [
        "weft.jobs.a: 1",
        "weft.jobs.b: 0 (1 total, 1 claimed)",
    ]


def test_list_pattern_with_stats(workdir) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        assert run_cli("write", queue, f"message for {queue}", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("list", "--pattern", "weft.jobs.*", "--stats", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["weft.jobs.a: 1", "weft.jobs.b: 1"]


def test_list_json_without_stats_outputs_queue_names(workdir) -> None:
    for queue in ("jobs.a", "jobs.b"):
        assert run_cli("write", queue, f"message for {queue}", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("list", "--prefix", "jobs.", "--json", cwd=workdir)

    assert rc == 0
    assert _json_lines(out) == [{"queue": "jobs.a"}, {"queue": "jobs.b"}]


def test_list_json_stats_output(workdir) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b"):
        assert run_cli("write", queue, f"message for {queue}", cwd=workdir)[0] == 0
    assert run_cli("read", "weft.jobs.b", cwd=workdir)[0] == 0

    rc, out, _ = run_cli(
        "list", "--prefix", "weft.jobs.", "--stats", "--json", cwd=workdir
    )
    assert rc == 0

    payloads = _json_lines(out)
    assert payloads == [
        {
            "queue": "weft.jobs.a",
            "pending": 1,
            "claimed": 0,
            "total": 1,
            "exists": True,
        },
        {
            "queue": "weft.jobs.b",
            "pending": 0,
            "claimed": 1,
            "total": 1,
            "exists": True,
        },
    ]


def test_list_rejects_prefix_and_pattern_together(workdir) -> None:
    rc, _, err = run_cli(
        "list",
        "--prefix",
        "weft.",
        "--pattern",
        "weft.*",
        cwd=workdir,
    )

    assert rc == 1
    assert "not allowed with argument" in err or "error:" in err


# Folded from the retired test_cli_queue_metadata.py (audit Task 7.3).
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
