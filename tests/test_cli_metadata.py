"""CLI tests for targeted queue metadata commands."""

from __future__ import annotations

import json

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
    assert payload["pending"] + payload["claimed"] == payload["total"]


def test_list_prefix_without_stats_hides_claimed_only_queues(workdir) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        assert run_cli("write", queue, f"message for {queue}", cwd=workdir)[0] == 0
    assert run_cli("read", "weft.jobs.b", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("list", "--prefix", "weft.jobs.", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["weft.jobs.a: 1"]


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
