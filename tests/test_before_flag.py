"""CLI tests for the --before timestamp filter."""

from __future__ import annotations

import json

from .conftest import run_cli


def _peek_json(cwd, queue: str) -> list[dict[str, object]]:
    rc, out, err = run_cli("peek", queue, "--all", "--json", cwd=cwd)
    assert rc == 0, err
    return [json.loads(line) for line in out.splitlines()]


def test_before_basic_filtering(workdir):
    run_cli("write", "test_queue", "msg1", cwd=workdir)
    run_cli("write", "test_queue", "msg2", cwd=workdir)

    messages = _peek_json(workdir, "test_queue")
    ts1 = messages[0]["timestamp"]
    ts2 = messages[1]["timestamp"]

    rc, out, err = run_cli(
        "peek", "test_queue", "--all", "--before", str(ts2), cwd=workdir
    )
    assert rc == 0, err
    assert out == "msg1"

    rc, out, err = run_cli(
        "peek", "test_queue", "--all", "--before", str(ts1), cwd=workdir
    )
    assert rc == 2, err
    assert out == ""


def test_read_before_consumes_only_matching_message(workdir):
    run_cli("write", "read_queue", "old", cwd=workdir)
    run_cli("write", "read_queue", "new", cwd=workdir)
    new_ts = _peek_json(workdir, "read_queue")[1]["timestamp"]

    rc, out, err = run_cli("read", "read_queue", "--before", str(new_ts), cwd=workdir)
    assert rc == 0, err
    assert out == "old"

    rc, out, err = run_cli("read", "read_queue", "--all", cwd=workdir)
    assert rc == 0, err
    assert out == "new"


def test_before_exact_boundary_is_exclusive(workdir):
    run_cli("write", "boundary_queue", "message", cwd=workdir)
    ts = _peek_json(workdir, "boundary_queue")[0]["timestamp"]

    rc, out, err = run_cli("peek", "boundary_queue", "--before", str(ts), cwd=workdir)
    assert rc == 2, err
    assert out == ""

    rc, out, err = run_cli(
        "peek", "boundary_queue", "--before", str(int(ts) + 1), cwd=workdir
    )
    assert rc == 0, err
    assert out == "message"


def test_after_and_before_form_open_interval(workdir):
    for message in ("m1", "m2", "m3"):
        run_cli("write", "interval_queue", message, cwd=workdir)
    messages = _peek_json(workdir, "interval_queue")
    ts1 = messages[0]["timestamp"]
    ts3 = messages[2]["timestamp"]

    rc, out, err = run_cli(
        "peek",
        "interval_queue",
        "--all",
        "--after",
        str(ts1),
        "--before",
        str(ts3),
        cwd=workdir,
    )
    assert rc == 0, err
    assert out == "m2"


def test_before_invalid_timestamp_uses_existing_validation(workdir):
    rc, _out, err = run_cli("peek", "q", "--before", "invalid", cwd=workdir)

    assert rc == 1
    assert "Invalid timestamp" in err


def test_message_cannot_be_combined_with_before(workdir):
    run_cli("write", "q", "message", cwd=workdir)

    rc, _out, err = run_cli(
        "peek",
        "q",
        "--message",
        "1234567890123456789",
        "--before",
        "1234567890123456790",
        cwd=workdir,
    )

    assert rc == 1
    assert "--before" in err
