"""Tests for line-delimited JSON (ndjson) output format in read and peek commands."""

import json

from .conftest import run_cli


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
    assert data == {"message": "hello world"}


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
        assert data == {"message": messages[i]}


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
    assert data == {"message": "test message"}

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
        assert data == {"message": messages[i]}

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
