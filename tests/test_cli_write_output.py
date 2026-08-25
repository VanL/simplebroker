"""CLI contract: `broker write -t` / `--json` print the committed message ID.

Default `broker write` output stays empty (silent success). Output flags are
recognized before the queue name, after a non-dash literal message, or after
the explicit stdin marker `-`. Registered option spellings require an explicit
`--` when used as message data; unknown dash-leading operands remain literal.
"""

import json
import re

import pytest

from simplebroker import Queue, target_for_directory

from .conftest import run_cli

_ID_RE = re.compile(r"^\d{19}$")


def _peek_one(workdir) -> str | None:
    """Verify durable state through the Queue API (no subprocess spawn)."""
    with Queue("q", db_path=target_for_directory(workdir), persistent=True) as queue:
        return queue.peek_one()


def test_write_default_is_silent(workdir):
    code, stdout, _stderr = run_cli("write", "q", "hello", cwd=workdir)

    assert code == 0
    assert stdout == ""


def test_write_timestamps_flag_before_queue_prints_id(workdir):
    code, stdout, stderr = run_cli("write", "-t", "q", "hello", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    # Retained end-to-end representative (plan Task 3.3): a full
    # write -> emitted ID -> separate CLI read round trip.
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
    assert type(payload["timestamp"]) is str
    assert re.fullmatch(r"[0-9]{19}", payload["timestamp"])
    assert re.search(r'"timestamp"\s*:\s*"[0-9]{19}"', stdout)

    # Retained end-to-end JSON representative (plan Task 3.3).
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

    assert _peek_one(workdir) == "piped body"


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

    assert _peek_one(workdir) == "piped body"


def test_dash_leading_queue_operand_still_fails_validation(workdir):
    """A dash-leading queue operand is protected, then rejected by
    queue-name validation instead of being interpreted as an option."""
    code, stdout, stderr = run_cli("write", "--stuff", "message", cwd=workdir)

    assert code != 0
    assert stdout == ""
    assert "Invalid queue name" in stderr
    assert "unrecognized arguments" not in stderr


def test_output_option_after_queue_uses_omitted_message_stdin(workdir):
    code, stdout, stderr = run_cli("write", "q", "-t", cwd=workdir, stdin="piped body")

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    assert _peek_one(workdir) == "piped body"


@pytest.mark.parametrize("message", ["--not-registered", "-t-prefixed"])
def test_unknown_dash_leading_write_message_remains_literal(workdir, message):
    code, stdout, stderr = run_cli("write", "q", message, cwd=workdir)

    assert code == 0, stderr
    assert stdout == ""
    assert _peek_one(workdir) == message


def test_raw_json_does_not_establish_mode_for_registered_token_conflict(workdir):
    code, stdout, stderr = run_cli("write", "q", "--json", "--cleanup", cwd=workdir)

    assert code == 1
    assert stdout == ""
    assert not stderr.startswith("{")
    assert "--cleanup" in stderr
    assert "use --" in stderr.lower()
    assert not (workdir / ".broker.db").exists()


def test_flag_plus_literal_dash_message_via_escape(workdir):
    code, stdout, stderr = run_cli("write", "-t", "q", "--", "-t", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    assert _peek_one(workdir) == "-t"


def test_status_operand_after_double_dash_does_not_disable_write_json(workdir):
    code, stdout, stderr = run_cli(
        "write", "q", "--json", "--", "--status", cwd=workdir
    )

    assert code == 0, stderr
    assert set(json.loads(stdout)) == {"timestamp"}
    assert _peek_one(workdir) == "--status"


@pytest.mark.parametrize("help_token", ["-h", "--help"])
def test_help_operand_after_double_dash_stays_literal_with_write_json(
    workdir, help_token
):
    code, stdout, stderr = run_cli(
        "write", "q", "--json", "--", help_token, cwd=workdir
    )

    assert code == 0, stderr
    assert set(json.loads(stdout)) == {"timestamp"}
    assert _peek_one(workdir) == help_token
