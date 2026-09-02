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


def test_write_keep_newest_preserves_output_and_claims_older_rows(workdir):
    for body in ("first", "second"):
        code, stdout, stderr = run_cli("write", "q", body, cwd=workdir)
        assert code == 0, stderr
        assert stdout == ""

    code, stdout, stderr = run_cli(
        "write", "q", "newest", "--keep-newest=2", "--json", cwd=workdir
    )

    assert code == 0, stderr
    assert set(json.loads(stdout)) == {"timestamp"}
    with Queue("q", db_path=target_for_directory(workdir), persistent=True) as queue:
        assert queue.peek_many(10) == ["second", "newest"]
        stats = queue.stats()
        assert (stats.pending, stats.claimed, stats.total) == (2, 1, 3)


@pytest.mark.parametrize(
    "value",
    ["0", "10000", "12x", "+1", "-1", "1_0", "١", "9" * 10_000, ""],
)
def test_write_keep_newest_rejects_invalid_cli_value_before_target(workdir, value):
    option = f"--keep-newest={value}"

    code, stdout, stderr = run_cli("write", "q", "body", option, "--json", cwd=workdir)

    assert code == 1
    assert stdout == ""
    assert json.loads(stderr)["error"] == "INVALID_ARGUMENT"
    assert not (workdir / ".broker.db").exists()


def test_write_keep_newest_accepts_leading_zeroes(workdir):
    code, stdout, stderr = run_cli(
        "write", "q", "body", "--keep-newest", "0007", cwd=workdir
    )

    assert (code, stdout, stderr) == (0, "", "")
    assert _peek_one(workdir) == "body"


def test_write_keep_newest_rejects_duplicate_values_before_target(workdir):
    code, stdout, stderr = run_cli(
        "write",
        "q",
        "body",
        "--keep-newest",
        "2",
        "--keep-newest=3",
        "--json",
        cwd=workdir,
    )

    assert code == 1
    assert stdout == ""
    assert json.loads(stderr)["error"] == "INVALID_ARGUMENT"
    assert not (workdir / ".broker.db").exists()


@pytest.mark.parametrize(
    "args",
    [
        ("write", "q", "body", "--keep-newest", "--json"),
        ("write", "--json", "q", "body", "--keep-newest"),
    ],
)
def test_write_keep_newest_missing_value_uses_requested_json_error(workdir, args):
    code, stdout, stderr = run_cli(*args, cwd=workdir)

    assert code == 1
    assert stdout == ""
    payload = json.loads(stderr)
    assert payload["error"] == "INVALID_ARGUMENT"
    assert "--keep-newest" in payload["message"]
    assert not (workdir / ".broker.db").exists()


@pytest.mark.parametrize("value", ["-1", "--bogus"])
def test_write_keep_newest_dash_leading_separate_value_uses_json_error(workdir, value):
    code, stdout, stderr = run_cli(
        "write",
        "--json",
        "q",
        "body",
        "--keep-newest",
        value,
        cwd=workdir,
    )

    assert code == 1
    assert stdout == ""
    payload = json.loads(stderr)
    assert payload["error"] == "INVALID_ARGUMENT"
    assert "--keep-newest" in payload["message"]
    assert not (workdir / ".broker.db").exists()


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
