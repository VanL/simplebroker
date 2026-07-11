"""CLI contract: `broker write -t` / `--json` print the committed message ID.

Default `broker write` output stays empty (silent success). The new flags
are recognized before the queue name, after a non-dash literal message, or
after the explicit stdin marker `-`. A dash-leading operand after the queue
is still a LITERAL message (backward compatibility with the operand
protection in cli.py).
"""

import json
import re

from .conftest import run_cli

_ID_RE = re.compile(r"^\d{19}$")


def test_write_default_is_silent(workdir):
    code, stdout, stderr = run_cli("write", "q", "hello", cwd=workdir)

    assert code == 0
    assert stdout == ""


def test_write_timestamps_flag_before_queue_prints_id(workdir):
    code, stdout, stderr = run_cli("write", "-t", "q", "hello", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    # Round-trip: the printed ID addresses exactly this message.
    code, out, _ = run_cli("read", "q", "-m", stdout, cwd=workdir)
    assert code == 0
    assert out == "hello"


def test_write_timestamps_long_flag(workdir):
    code, stdout, stderr = run_cli("write", "--timestamps", "q", "hello", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout


def test_write_json_prints_timestamp_only(workdir):
    code, stdout, stderr = run_cli("write", "--json", "q", "hello", cwd=workdir)

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert set(payload) == {"timestamp"}
    assert type(payload["timestamp"]) is int

    code, out, _ = run_cli("read", "q", "--json", cwd=workdir)
    assert code == 0
    assert json.loads(out)["timestamp"] == payload["timestamp"]


def test_write_json_wins_over_timestamps(workdir):
    code, stdout, stderr = run_cli("write", "--json", "-t", "q", "hello", cwd=workdir)

    assert code == 0, stderr
    assert len(stdout.splitlines()) == 1
    assert set(json.loads(stdout)) == {"timestamp"}


def test_write_flag_after_literal_message(workdir):
    code, stdout, stderr = run_cli("write", "q", "hello", "-t", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout


def test_write_flag_with_stdin_marker(workdir):
    code, stdout, stderr = run_cli(
        "write", "q", "-", "-t", cwd=workdir, stdin="piped body"
    )

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "piped body"


def test_write_flag_with_omitted_message_stdin(workdir):
    code, stdout, stderr = run_cli("write", "-t", "q", cwd=workdir, stdin="piped body")

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout


def test_write_flag_before_queue_with_stdin_marker(workdir):
    code, stdout, stderr = run_cli(
        "write", "-t", "q", "-", cwd=workdir, stdin="piped body"
    )

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "piped body"


def test_dash_leading_queue_operand_still_fails_validation(workdir):
    """A dash-leading queue operand is protected, then rejected by
    queue-name validation instead of being interpreted as an option."""
    code, stdout, stderr = run_cli("write", "--stuff", "message", cwd=workdir)

    assert code != 0
    assert stdout == ""
    assert "Invalid queue name" in stderr
    assert "unrecognized arguments" not in stderr


def test_dash_leading_operand_after_queue_stays_literal(workdir):
    """Backward compatibility: `write q -t` writes the LITERAL message -t."""
    code, stdout, stderr = run_cli("write", "q", "-t", cwd=workdir)

    assert code == 0, stderr
    assert stdout == ""

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "-t"


def test_flag_plus_literal_dash_message_via_escape(workdir):
    code, stdout, stderr = run_cli("write", "-t", "q", "--", "-t", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "-t"
