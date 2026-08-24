"""Tests for line-delimited JSON (ndjson) output format in read and peek commands."""

import json
import sys
import threading
import time
import types
import warnings

import pytest

from simplebroker import commands
from simplebroker.cli import main

from .conftest import run_cli
from .helper_scripts.timestamp_validation import validate_timestamp


class _ClassifiedRuntimeError(RuntimeError):
    retryable: bool


def test_read_json_single_message(workdir):
    """Test --json flag with read command for single message."""
    # Write a message
    run_cli("write", "json_test", "hello world", cwd=workdir)

    # Read with JSON format
    rc, out, _ = run_cli("read", "json_test", "--json", cwd=workdir)
    assert rc == 0

    # Parse and verify JSON (single line)
    lines = out.strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["message"] == "hello world"
    assert "timestamp" in data
    validate_timestamp(data["timestamp"])


def test_read_json_multiple_messages(workdir):
    """Test --json flag with read --all for multiple messages (ndjson format)."""
    # Write multiple messages
    messages = ["first message", "second\nmessage", 'third"message"']
    for msg in messages:
        run_cli("write", "json_multi", msg, cwd=workdir)

    # Read all with JSON format
    rc, out, _ = run_cli("read", "json_multi", "--all", "--json", cwd=workdir)
    assert rc == 0

    # Parse and verify each line is valid JSON (ndjson format)
    lines = out.strip().split("\n")
    assert len(lines) == 3

    for i, line in enumerate(lines):
        data = json.loads(line)
        assert data["message"] == messages[i]
        assert "timestamp" in data
        validate_timestamp(data["timestamp"])


def test_peek_json_single_message(workdir):
    """Test --json flag with peek command for single message."""
    # Write a message
    run_cli("write", "peek_json", "test message", cwd=workdir)

    # Peek with JSON format
    rc, out, _ = run_cli("peek", "peek_json", "--json", cwd=workdir)
    assert rc == 0

    # Parse and verify JSON (single line)
    lines = out.strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["message"] == "test message"
    assert "timestamp" in data
    validate_timestamp(data["timestamp"])

    # Verify message still exists
    rc, out, _ = run_cli("read", "peek_json", cwd=workdir)
    assert rc == 0
    assert out == "test message"


def test_peek_json_multiple_messages(workdir):
    """Test --json flag with peek --all for multiple messages (ndjson format)."""
    # Write multiple messages
    messages = ["msg1", "msg2\nwith\nnewlines", "msg3\twith\ttabs"]
    for msg in messages:
        run_cli("write", "peek_multi", msg, cwd=workdir)

    # Peek all with JSON format
    rc, out, _ = run_cli("peek", "peek_multi", "--all", "--json", cwd=workdir)
    assert rc == 0

    # Parse and verify each line is valid JSON (ndjson format)
    lines = out.strip().split("\n")
    assert len(lines) == 3

    for i, line in enumerate(lines):
        data = json.loads(line)
        assert data["message"] == messages[i]
        assert "timestamp" in data
        validate_timestamp(data["timestamp"])

    # Verify messages still exist (read all with plain output)
    rc, out, err = run_cli("read", "peek_multi", "--all", cwd=workdir)
    assert rc == 0
    # When reading multiple messages with newlines in plain mode,
    # each message is printed followed by the next, so newlines within
    # messages become part of the output
    expected_output = "\n".join(messages)
    assert out.strip() == expected_output
    # Also verify the warning was issued
    assert "newline characters" in err


def test_quiet_read_all_suppresses_newline_commentary(workdir):
    run_cli("write", "quiet_newline", "line1\nline2", cwd=workdir)

    rc, out, err = run_cli("-q", "read", "quiet_newline", "--all", cwd=workdir)

    assert rc == 0
    assert out == "line1\nline2"
    assert err == ""


def test_single_plain_read_warns_once_for_embedded_newline(workdir):
    run_cli("write", "single_newline", "line1\nline2", cwd=workdir)

    rc, out, err = run_cli("read", "single_newline", cwd=workdir)

    assert rc == 0
    assert out == "line1\nline2"
    assert err.count("Message contains newline characters") == 1


def test_repeated_direct_invocations_each_emit_newline_warning(workdir):
    db_path = workdir / "broker.db"
    assert commands.cmd_write(str(db_path), "direct_warning", "line1\nline2") == 0

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default", commands._MessageNewlineWarning)
        assert commands.cmd_peek(str(db_path), "direct_warning") == 0
        assert commands.cmd_peek(str(db_path), "direct_warning") == 0

    owned = [
        warning
        for warning in caught
        if warning.category is commands._MessageNewlineWarning
    ]
    assert len(owned) == 2


def test_repeated_in_process_cli_invocations_each_emit_newline_warning(
    workdir,
    monkeypatch,
):
    monkeypatch.chdir(workdir)
    dummy_sys = types.SimpleNamespace(
        argv=["broker", "write", "cli_warning", "line1\nline2"],
        stderr=sys.stderr,
        stdout=sys.stdout,
    )
    monkeypatch.setattr("simplebroker.cli.sys", dummy_sys)
    assert main() == 0
    dummy_sys.argv = ["broker", "peek", "cli_warning"]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default", commands._MessageNewlineWarning)
        assert main() == 0
        assert main() == 0

    owned = [
        warning
        for warning in caught
        if warning.category is commands._MessageNewlineWarning
    ]
    assert len(owned) == 2


@pytest.mark.parametrize("command", ["read", "peek"])
@pytest.mark.parametrize("selector", ["single", "all", "message", "after"])
@pytest.mark.parametrize("timestamps", [False, True])
def test_plain_fetch_newline_warning_covers_selectors_and_timestamps(
    workdir,
    command,
    selector,
    timestamps,
):
    queue = f"warning_{command}_{selector}_{timestamps}"
    args = [command, queue]
    if selector == "after":
        rc, anchor, err = run_cli("write", queue, "anchor", "--timestamps", cwd=workdir)
        assert rc == 0, err
        run_cli("write", queue, "line1\nline2", cwd=workdir)
        args.extend(["--after", anchor])
    else:
        rc, message_id, err = run_cli(
            "write", queue, "line1\nline2", "--timestamps", cwd=workdir
        )
        assert rc == 0, err
        if selector == "all":
            run_cli("write", queue, "again\nnext", cwd=workdir)
            args.append("--all")
        elif selector == "message":
            args.extend(["--message", message_id])
    if timestamps:
        args.append("--timestamps")

    rc, _out, err = run_cli(*args, cwd=workdir)

    assert rc == 0
    assert err.count("Message contains newline characters") == 1


@pytest.mark.parametrize("selector", ["single", "all", "message", "after"])
@pytest.mark.parametrize("timestamps", [False, True])
def test_plain_move_newline_warning_covers_selectors_and_timestamps(
    workdir,
    selector,
    timestamps,
):
    source = f"move_warning_{selector}_{timestamps}"
    destination = f"move_warning_dest_{selector}_{timestamps}"
    args = ["move", source, destination]
    if selector == "after":
        rc, anchor, err = run_cli(
            "write", source, "anchor", "--timestamps", cwd=workdir
        )
        assert rc == 0, err
        run_cli("write", source, "line1\nline2", cwd=workdir)
        args.extend(["--after", anchor])
    else:
        rc, message_id, err = run_cli(
            "write", source, "line1\nline2", "--timestamps", cwd=workdir
        )
        assert rc == 0, err
        if selector == "all":
            run_cli("write", source, "again\nnext", cwd=workdir)
            args.append("--all")
        elif selector == "message":
            args.extend(["--message", message_id])
    if timestamps:
        args.append("--timestamps")

    rc, _out, err = run_cli(*args, cwd=workdir)

    assert rc == 0
    assert err.count("Message contains newline characters") == 1


@pytest.mark.parametrize(
    "args",
    [
        ("read", "json_warning", "--json"),
        ("peek", "json_warning", "--json"),
        ("move", "json_warning", "json_warning_dest", "--json"),
    ],
)
def test_json_message_output_never_warns_about_newlines(workdir, args):
    run_cli("write", "json_warning", "line1\nline2", cwd=workdir)

    rc, _out, err = run_cli(*args, cwd=workdir)

    assert rc == 0
    assert "Message contains newline characters" not in err


@pytest.mark.parametrize(
    "args",
    [
        ("read", "quiet_warning", "--all"),
        ("peek", "quiet_warning", "--all"),
        ("move", "quiet_warning", "quiet_warning_dest", "--all"),
    ],
)
def test_quiet_suppresses_owned_newline_warning_for_message_commands(
    workdir,
    args,
):
    run_cli("write", "quiet_warning", "line1\nline2", cwd=workdir)

    rc, _out, err = run_cli("--quiet", *args, cwd=workdir)

    assert rc == 0
    assert err == ""


def test_quiet_newline_policy_does_not_hide_concurrent_loud_warning(
    monkeypatch,
):
    ready = threading.Barrier(2)
    loud_finished = threading.Barrier(2)
    emitted = []

    monkeypatch.setattr(
        commands.warnings,
        "warn_explicit",
        lambda message, category, **_kwargs: emitted.append((message, category)),
    )

    def quiet_invocation():
        with commands._message_newline_warning_policy(quiet=True):
            ready.wait(timeout=5)
            commands._warn_message_newlines("quiet\nmessage", False)
            loud_finished.wait(timeout=5)

    thread = threading.Thread(target=quiet_invocation)
    thread.start()
    ready.wait(timeout=5)
    commands._warn_message_newlines("loud\nmessage", False)
    loud_finished.wait(timeout=5)
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert emitted == [
        (
            (
                "Message contains newline characters which may break shell pipelines. "
                "Consider using --json for safe handling of special characters."
            ),
            commands._MessageNewlineWarning,
        )
    ]


@pytest.mark.parametrize("command", ["read", "peek"])
def test_empty_plain_selection_does_not_warn(workdir, command):
    rc, out, err = run_cli(command, "empty_warning", "--all", cwd=workdir)

    assert rc == 2
    assert out == ""
    assert "Message contains newline characters" not in err


def test_json_with_special_characters(workdir):
    """Test JSON escaping of special characters."""
    # Message with various special characters
    message = 'Message with "quotes", \nnewlines, \ttabs, and \\ backslashes'

    # Write and read with JSON
    run_cli("write", "special_chars", message, cwd=workdir)
    rc, out, _ = run_cli("read", "special_chars", "--json", cwd=workdir)
    assert rc == 0

    # Verify JSON is properly escaped (single line)
    lines = out.strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["message"] == message
    assert "timestamp" in data
    validate_timestamp(data["timestamp"])


def test_json_empty_queue(workdir):
    """Test JSON output with empty queue still returns exit code 2."""
    # Try to read from empty queue with JSON
    rc, out, _ = run_cli("read", "empty_queue", "--json", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY
    assert out == ""  # No output for empty queue

    # Same for peek
    rc, out, _ = run_cli("peek", "empty_queue", "--json", cwd=workdir)
    assert rc == 2
    assert out == ""


def test_read_json_invalid_timestamp_error_is_json(workdir):
    """Commands with --json emit structured errors on stderr."""
    rc, out, err = run_cli(
        "read",
        "empty_queue",
        "--json",
        "--after",
        "invalid",
        cwd=workdir,
    )

    assert rc == 1
    assert out == ""
    payload = json.loads(err)
    assert payload["error"] == "INVALID_TIMESTAMP"
    assert payload["retryable"] is False
    assert "invalid" in payload["message"].lower()
    assert "timestamp" in payload["message"].lower()
    assert "integer" in payload["message"].lower()


def test_peek_json_invalid_message_id_error_is_json(workdir):
    """Invalid -m values use the shared JSON error formatter under --json."""
    rc, out, err = run_cli(
        "peek",
        "empty_queue",
        "--json",
        "-m",
        "not-an-id",
        cwd=workdir,
    )

    assert rc == 1
    assert out == ""
    payload = json.loads(err)
    assert payload["error"] == "INVALID_MESSAGE_ID"
    assert payload["retryable"] is False
    assert "message id" in payload["message"].lower()
    assert "19 digits" in payload["message"].lower()


def test_move_json_invalid_message_id_error_is_json(workdir):
    """Move uses the same malformed message-ID JSON diagnostic."""
    rc, out, err = run_cli(
        "move",
        "source",
        "dest",
        "--json",
        "-m",
        "9223372036854775808",
        cwd=workdir,
    )

    assert rc == 1
    assert out == ""
    payload = json.loads(err)
    assert payload["error"] == "INVALID_MESSAGE_ID"
    assert payload["retryable"] is False
    assert "message id" in payload["message"].lower()
    assert "19 digits" in payload["message"].lower()


def test_move_json_argument_error_is_json(workdir):
    """Command-local --json also formats validation errors outside DB access."""
    rc, out, err = run_cli("move", "same", "same", "--json", cwd=workdir)

    assert rc == 1
    assert out == ""
    payload = json.loads(err)
    assert payload["error"] == "INVALID_ARGUMENT"
    assert payload["retryable"] is False
    assert "source" in payload["message"].lower()
    assert "destination" in payload["message"].lower()
    assert "same" in payload["message"].lower()


@pytest.mark.sqlite_only
def test_list_json_database_validation_error_is_json(workdir):
    """Pre-dispatch validation errors honor a command's local --json flag."""
    db_path = workdir / ".broker.db"
    db_path.write_text("not sqlite", encoding="utf-8")

    rc, out, err = run_cli("list", "--json", cwd=workdir)

    assert rc == 1
    assert out == ""
    payload = json.loads(err)
    assert payload["error"] == "ERROR"
    assert payload["retryable"] is False
    assert "not a valid SQLite database" in payload["message"]


def test_json_error_reports_explicit_retryable_classification(
    workdir, monkeypatch, capsys
):
    """Only an exception's explicit retryable marker may set the JSON field."""
    error = _ClassifiedRuntimeError("backend is temporarily unavailable")
    error.retryable = True
    monkeypatch.setattr(sys, "argv", ["broker", "-d", str(workdir), "list", "--json"])
    monkeypatch.setattr(
        "simplebroker.commands.cmd_list",
        lambda *args, **kwargs: (_ for _ in ()).throw(error),
    )
    monkeypatch.setattr(
        "simplebroker.cli._validate_command_target", lambda *a, **k: None
    )

    assert main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"] == "ERROR"
    assert payload["retryable"] is True
    assert "temporarily unavailable" in payload["message"].lower()


def test_json_error_reports_explicit_nonretryable_classification(
    workdir, monkeypatch, capsys
):
    error = _ClassifiedRuntimeError("request is permanently invalid")
    error.retryable = False
    monkeypatch.setattr(sys, "argv", ["broker", "-d", str(workdir), "list", "--json"])
    monkeypatch.setattr(
        "simplebroker.commands.cmd_list",
        lambda *args, **kwargs: (_ for _ in ()).throw(error),
    )
    monkeypatch.setattr(
        "simplebroker.cli._validate_command_target", lambda *a, **k: None
    )

    assert main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err)["retryable"] is False


def test_plain_error_stays_plain_without_json(workdir):
    """Plain commands keep line-oriented stderr unless --json is explicit."""
    rc, out, err = run_cli(
        "read",
        "empty_queue",
        "--after",
        "invalid",
        cwd=workdir,
    )

    assert rc == 1
    assert out == ""
    assert err.startswith("simplebroker: error: ")
    with pytest.raises(json.JSONDecodeError):
        json.loads(err)


def test_json_unicode_handling(workdir):
    """Test JSON output with Unicode characters."""
    # Message with Unicode characters
    message = "Hello 世界! 🌍 Émojis and ñ special chars"

    # Write and read with JSON
    run_cli("write", "unicode_test", message, cwd=workdir)
    rc, out, _ = run_cli("read", "unicode_test", "--json", cwd=workdir)
    assert rc == 0

    # Verify JSON properly handles Unicode (single line)
    lines = out.strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["message"] == message
    assert "timestamp" in data
    validate_timestamp(data["timestamp"])


def test_json_includes_timestamps_by_default(workdir):
    """Test that --json automatically includes timestamps."""
    run_cli("write", "test_queue", "test message", cwd=workdir)
    rc, out, _ = run_cli("read", "test_queue", "--json", cwd=workdir)
    assert rc == 0

    data = json.loads(out)
    assert "timestamp" in data
    validate_timestamp(data["timestamp"])


def test_json_with_timestamps_flag_is_noop(workdir):
    """Test that --json -t produces same output as --json alone."""
    run_cli("write", "test_queue", "test message", cwd=workdir)

    # Get output with just --json
    rc1, out1, _ = run_cli("peek", "test_queue", "--json", cwd=workdir)
    assert rc1 == 0

    # Get output with --json -t
    rc2, out2, _ = run_cli("peek", "test_queue", "--json", "-t", cwd=workdir)
    assert rc2 == 0

    # Should be identical
    assert out1 == out2

    # Both should have timestamps
    data1 = json.loads(out1)
    data2 = json.loads(out2)
    assert "timestamp" in data1
    assert "timestamp" in data2
    assert data1 == data2


def test_plain_text_timestamps_unchanged(workdir):
    """Test that -t without --json still works as before."""
    run_cli("write", "test_queue", "test message", cwd=workdir)
    rc, out, _ = run_cli("read", "test_queue", "-t", cwd=workdir)
    assert rc == 0

    # Should be tab-separated: timestamp\tmessage
    assert "\t" in out
    parts = out.strip().split("\t")
    assert len(parts) == 2
    assert parts[1] == "test message"
    # Timestamp should be a valid integer
    assert parts[0].isdigit()
    assert len(parts[0]) == 19
    assert parts[0].isascii() and parts[0].isdecimal()


def test_json_timestamp_edge_cases(workdir):
    """Test JSON output with edge case timestamps."""
    # Note: We can't control the actual timestamps generated by the system,
    # but we can verify they are valid and within expected bounds

    # Write messages
    messages = []
    for i in range(5):
        msg = f"edge_case_{i}"
        messages.append(msg)
        run_cli("write", "edge_test", msg, cwd=workdir)
        time.sleep(0.001)  # Ensure different timestamps

    # Read all with JSON
    rc, out, _ = run_cli("read", "edge_test", "--all", "--json", cwd=workdir)
    assert rc == 0

    lines = out.strip().split("\n")
    assert len(lines) == 5

    timestamps = []
    for i, line in enumerate(lines):
        data = json.loads(line)
        assert data["message"] == messages[i]
        assert "timestamp" in data

        ts = data["timestamp"]
        validate_timestamp(ts)
        timestamps.append(ts)

        # Additional edge case checks
        numeric_ts = int(ts)

        # Verify it's not at the exact boundaries (would be suspicious)
        assert numeric_ts != 1_650_000_000_000_000_000
        assert numeric_ts != 4_300_000_000_000_000_000

        # Verify string representation is exactly 19 digits
        assert len(ts) == 19
        assert ts.isascii() and ts.isdecimal()

        # Verify it can be parsed back from string
        parsed_ts = int(ts)
        assert f"{parsed_ts:019d}" == ts

    # Verify timestamps are unique and increasing
    assert len(set(timestamps)) == 5, "All timestamps should be unique"
    assert timestamps == sorted(timestamps), "Timestamps should be in increasing order"

    # Verify reasonable timestamp differences (microsecond to second range)
    for i in range(1, len(timestamps)):
        diff = int(timestamps[i]) - int(timestamps[i - 1])
        # Difference should be positive but not too large (< 1 minute)
        assert 0 < diff < (60 * 1_000_000 << 12), (
            f"Unexpected timestamp difference: {diff}"
        )
