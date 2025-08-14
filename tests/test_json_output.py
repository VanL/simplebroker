"""Tests for line-delimited JSON (ndjson) output format in read and peek commands."""

import json
import time

from .conftest import run_cli
from .helper_scripts.timestamp_validation import validate_timestamp


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
    validate_timestamp(int(parts[0]))


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
        # Verify it's not at the exact boundaries (would be suspicious)
        assert ts != 1_650_000_000_000_000_000
        assert ts != 4_300_000_000_000_000_000

        # Verify string representation is exactly 19 digits
        ts_str = str(ts)
        assert len(ts_str) == 19
        assert ts_str.isdigit()

        # Verify it can be parsed back from string
        parsed_ts = int(ts_str)
        assert parsed_ts == ts

    # Verify timestamps are unique and increasing
    assert len(set(timestamps)) == 5, "All timestamps should be unique"
    assert timestamps == sorted(timestamps), "Timestamps should be in increasing order"

    # Verify reasonable timestamp differences (microsecond to second range)
    for i in range(1, len(timestamps)):
        diff = timestamps[i] - timestamps[i - 1]
        # Difference should be positive but not too large (< 1 minute)
        assert 0 < diff < (60 * 1_000_000 << 12), (
            f"Unexpected timestamp difference: {diff}"
        )
