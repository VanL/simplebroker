"""Test streaming functionality for memory efficiency."""

import json
from pathlib import Path

import pytest

from .conftest import run_cli


@pytest.mark.slow
def test_streaming_read_all(workdir: Path):
    """Test that read --all uses streaming and doesn't blow memory."""
    db_path = workdir / "test.db"

    # Write many messages to test streaming
    for i in range(1000):
        code, stdout, stderr = run_cli(
            "-f", db_path, "write", "test_queue", f"message_{i}", cwd=workdir
        )
        assert code == 0

    # Read all messages - this should stream without loading all into memory
    code, stdout, stderr = run_cli(
        "-f", db_path, "read", "test_queue", "--all", cwd=workdir
    )
    assert code == 0

    # Verify we got all messages
    lines = stdout.strip().split("\n")
    assert len(lines) == 1000
    assert lines[0] == "message_0"
    assert lines[999] == "message_999"

    # Verify queue is now empty
    code, stdout, stderr = run_cli("-f", db_path, "list", cwd=workdir)
    assert code == 0
    assert stdout.strip() == ""


@pytest.mark.slow
def test_streaming_peek_all(workdir: Path):
    """Test that peek --all uses streaming and doesn't blow memory."""
    db_path = workdir / "test.db"

    # Write many messages to test streaming
    for i in range(1000):
        code, stdout, stderr = run_cli(
            "-f", db_path, "write", "test_queue", f"message_{i}", cwd=workdir
        )
        assert code == 0

    # Peek all messages - this should stream without loading all into memory
    code, stdout, stderr = run_cli(
        "-f", db_path, "peek", "test_queue", "--all", cwd=workdir
    )
    assert code == 0

    # Verify we got all messages
    lines = stdout.strip().split("\n")
    assert len(lines) == 1000
    assert lines[0] == "message_0"
    assert lines[999] == "message_999"

    # Verify messages are still in queue
    code, stdout, stderr = run_cli("-f", db_path, "list", cwd=workdir)
    assert code == 0
    assert "test_queue: 1000" in stdout


def test_json_output_now_ndjson(workdir: Path):
    """Test that JSON output now uses line-delimited JSON (ndjson) format."""
    # Write a few messages
    for i in range(5):
        code, stdout, stderr = run_cli(
            "-f", "test.db", "write", "test_queue", f"message_{i}", cwd=workdir
        )
        assert code == 0

    # Read all with JSON output
    code, stdout, stderr = run_cli(
        "-f", "test.db", "read", "test_queue", "--all", "--json", cwd=workdir
    )
    assert code == 0

    # Parse each line as a separate JSON object (ndjson)
    lines = stdout.strip().split("\n")
    assert len(lines) == 5

    for i, line in enumerate(lines):
        data = json.loads(line)
        assert "message" in data
        assert data["message"] == f"message_{i}"
