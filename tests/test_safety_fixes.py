"""
Tests for critical safety fixes and bug corrections.

Tests verify:
- Purge safety (require queue name or --all flag)
- Message size validation uses UTF-8 byte count
- Timestamp uniqueness across instances
- SQLite version checking
- TOCTOU fix for cleanup
"""

import multiprocessing
import subprocess
import sys

import pytest

from simplebroker.db import BrokerDB

from .conftest import run_cli


def test_delete_safety_no_args(workdir):
    """Test that delete with no arguments is rejected for safety."""
    # Write some messages first
    rc, _, _ = run_cli("write", "test_queue", "message1", cwd=workdir)
    assert rc == 0

    # Try to delete with no arguments - should fail
    rc, _, err = run_cli("delete", cwd=workdir)
    assert rc == 1
    assert "one of the arguments queue --all is required" in err

    # Verify messages still exist
    rc, out, _ = run_cli("peek", "test_queue", cwd=workdir)
    assert rc == 0
    assert out == "message1"


def test_delete_with_queue_name(workdir):
    """Test that delete with specific queue name works."""
    # Write to multiple queues
    run_cli("write", "queue1", "msg1", cwd=workdir)
    run_cli("write", "queue2", "msg2", cwd=workdir)

    # Purge only queue1
    rc, _, _ = run_cli("delete", "queue1", cwd=workdir)
    assert rc == 0

    # Verify queue1 is empty but queue2 still has messages
    rc, _, _ = run_cli("peek", "queue1", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    rc, out, _ = run_cli("peek", "queue2", cwd=workdir)
    assert rc == 0
    assert out == "msg2"


def test_delete_mutually_exclusive(workdir):
    """Test that delete queue name and --all are mutually exclusive."""
    # Write some messages
    run_cli("write", "queue1", "msg1", cwd=workdir)

    # Try to use both queue name and --all flag - should fail
    rc, _, err = run_cli("delete", "queue1", "--all", cwd=workdir)
    assert rc == 1
    assert "not allowed with argument" in err

    # Verify message still exists
    rc, out, _ = run_cli("peek", "queue1", cwd=workdir)
    assert rc == 0
    assert out == "msg1"


def test_delete_with_all_flag(workdir):
    """Test that delete --all works correctly."""
    # Write to multiple queues
    run_cli("write", "queue1", "msg1", cwd=workdir)
    run_cli("write", "queue2", "msg2", cwd=workdir)

    # Purge all queues
    rc, _, _ = run_cli("delete", "--all", cwd=workdir)
    assert rc == 0

    # Verify both queues are empty
    rc, _, _ = run_cli("peek", "queue1", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    rc, _, _ = run_cli("peek", "queue2", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY


def test_message_size_utf8_bytes(workdir):
    """Test that message size validation uses UTF-8 byte count, not char count."""
    # Create a message with multi-byte UTF-8 characters
    # Each emoji is 4 bytes in UTF-8
    emoji = "🎉"

    # Create message just under 10MB in bytes but much smaller in char count
    # Use stdin to avoid command line length limits
    # 2,621,440 emojis * 4 bytes = 10,485,760 bytes (just under 10MB)
    big_message = emoji * 2_621_440

    # This should work (just under 10MB in bytes)
    rc, _, _ = run_cli("write", "test", "-", cwd=workdir, stdin=big_message)
    assert rc == 0

    # Add one more emoji to exceed 10MB
    too_big_message = big_message + emoji

    # This should fail (exceeds 10MB in bytes)
    rc, _, err = run_cli("write", "test", "-", cwd=workdir, stdin=too_big_message)
    assert rc == 1
    assert "exceeds maximum size" in err


def test_message_size_stdin_utf8(workdir):
    """Test UTF-8 size validation for stdin input."""
    # Create message with multi-byte characters
    message = "Здравствуйте мир! 🌍" * 500_000  # Mix of 2-byte and 4-byte chars

    # Check if it would exceed limit
    if len(message.encode("utf-8")) > 10 * 1024 * 1024:
        # Should fail
        rc, _, err = run_cli("write", "test", "-", cwd=workdir, stdin=message)
        assert rc == 1
        assert "exceeds maximum size" in err
    else:
        # Should succeed
        rc, _, _ = run_cli("write", "test", "-", cwd=workdir, stdin=message)
        assert rc == 0


def test_broadcast_size_utf8(workdir):
    """Test that broadcast also uses UTF-8 byte count."""
    # Create queues
    run_cli("write", "q1", "dummy", cwd=workdir)
    run_cli("write", "q2", "dummy", cwd=workdir)

    # Create message with multi-byte UTF-8 characters
    # Use stdin to avoid command line length limits
    emoji = "🎉"
    big_message = emoji * 2_621_440  # Just under 10MB
    too_big_message = big_message + emoji  # Just over 10MB

    # This should work (increase timeout for large message)
    rc, _, _ = run_cli("broadcast", "-", cwd=workdir, stdin=big_message, timeout=30)
    assert rc == 0

    # This should fail
    rc, _, err = run_cli(
        "broadcast", "-", cwd=workdir, stdin=too_big_message, timeout=30
    )
    assert rc == 1
    assert "exceeds maximum size" in err


def write_messages_subprocess(process_id, workdir):
    """Write messages using the broker CLI from a subprocess."""
    errors = []
    messages_written = []

    for i in range(20):
        msg = f"process_{process_id}_msg_{i}"
        # Call the broker CLI directly
        result = subprocess.run(
            [sys.executable, "-m", "simplebroker", "write", f"queue_{process_id}", msg],
            cwd=str(workdir),
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        if result.returncode == 0:
            messages_written.append(msg)
        else:
            errors.append(f"Message {i}: {result.stderr}")

    return messages_written, errors


def test_timestamp_uniqueness_across_instances(workdir):
    """Test that timestamps are unique even across multiple broker CLI instances."""
    # Run multiple subprocesses, each calling the broker CLI
    with multiprocessing.Pool(processes=5) as pool:
        # Create args for each subprocess: (process_id, workdir)
        args = [(i, workdir) for i in range(5)]
        results = pool.starmap(write_messages_subprocess, args)

    # Collect results
    all_messages = {}
    errors = []

    for process_id, (messages_written, process_errors) in enumerate(results):
        for msg in messages_written:
            all_messages[msg] = process_id
        for error in process_errors:
            errors.append(f"Process {process_id}: {error}")

    # Check for errors
    assert not errors, f"Errors occurred: {errors}"

    # Verify message uniqueness by peeking all messages with timestamps using the CLI
    # This directly tests timestamp uniqueness - timestamps should be unique
    messages_read = {}
    all_timestamps = set()

    for process_id in range(5):
        # Use the CLI to peek all messages with timestamps
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "simplebroker",
                "peek",
                f"queue_{process_id}",
                "--all",
                "--timestamps",
            ],
            cwd=str(workdir),
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        assert result.returncode == 0, (
            f"Failed to peek queue_{process_id}: {result.stderr}"
        )

        lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
        assert len(lines) == 20, (
            f"Expected 20 messages for process {process_id}, got {len(lines)}"
        )

        # Verify order is preserved (FIFO) and collect timestamps
        for i, line in enumerate(lines):
            # Parse timestamp and message (format: timestamp\tmessage)
            parts = line.split("\t", 1)
            assert len(parts) == 2, f"Invalid format: {line}"
            timestamp_str, msg = parts
            timestamp = int(timestamp_str)

            # Check for timestamp uniqueness
            assert timestamp not in all_timestamps, f"Duplicate timestamp: {timestamp}"
            all_timestamps.add(timestamp)

            expected = f"process_{process_id}_msg_{i}"
            assert msg == expected, f"Expected {expected}, got {msg}"
            messages_read[msg] = process_id

    # Verify we got all messages back
    assert len(messages_read) == 100, f"Expected 100 messages, got {len(messages_read)}"
    assert set(messages_read.keys()) == set(all_messages.keys()), (
        "Some messages were lost"
    )

    # Additional test: rapid writes to same queue to stress timestamp generation
    for i in range(10):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "simplebroker",
                "write",
                "stress_test",
                f"rapid_{i}",
            ],
            cwd=str(workdir),
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert result.returncode == 0, f"Failed to write rapid_{i}: {result.stderr}"

    # Read back with peek to verify order
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simplebroker",
            "peek",
            "stress_test",
            "--all",
            "--timestamps",
        ],
        cwd=str(workdir),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, f"Failed to peek stress_test: {result.stderr}"
    lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
    assert len(lines) == 10
    for i, line in enumerate(lines):
        # Parse timestamp and message (format: timestamp\tmessage)
        parts = line.split("\t", 1)
        assert len(parts) == 2, f"Invalid format: {line}"
        _, msg = parts
        assert msg == f"rapid_{i}", (
            f"Messages out of order: expected rapid_{i}, got {msg}"
        )


def test_sqlite_version_check(workdir, monkeypatch):
    """Test that old SQLite versions are rejected."""
    db_path = workdir / ".broker.db"

    # Create a mock that simulates old SQLite version
    class MockCursor:
        def fetchone(self):
            return ("3.34.0",)  # Version before 3.35

    class MockConnection:
        def __init__(self, *args, **kwargs):
            self.call_count = 0

        def execute(self, query, params=None):
            self.call_count += 1
            if "sqlite_version" in query:
                return MockCursor()
            elif "PRAGMA" in query:
                # Allow all PRAGMA statements to succeed
                return None
            # For other queries, raise to trigger error
            raise RuntimeError("Mock connection")

        def commit(self):
            pass

        def close(self):
            pass

    # Patch sqlite3.connect to return our mock
    monkeypatch.setattr("sqlite3.connect", MockConnection)

    # Try to create BrokerDB - should fail with version error
    with pytest.raises(RuntimeError) as exc_info:
        BrokerDB(str(db_path))

    assert "SQLite version" in str(exc_info.value)
    assert "too old" in str(exc_info.value)
    assert "3.35.0 or later" in str(exc_info.value)


def test_cleanup_toctou_fix(workdir):
    """Test that cleanup handles TOCTOU race condition gracefully."""
    # Create a database file
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    db_path = workdir / ".broker.db"
    assert db_path.exists()

    # Delete the file manually to simulate race condition
    db_path.unlink()

    # Cleanup should still succeed without error
    rc, out, _ = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert "Database not found, nothing to clean up" in out

    # Run cleanup again on non-existent file - should still succeed
    rc, out, _ = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert "Database not found, nothing to clean up" in out


def test_cleanup_quiet_mode(workdir):
    """Test that cleanup respects quiet mode."""
    # Create a database
    run_cli("write", "test", "message", cwd=workdir)

    # Cleanup with quiet flag
    rc, out, err = run_cli("--quiet", "--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""  # No output in quiet mode
    assert err == ""

    # Cleanup again (file doesn't exist) with quiet flag
    rc, out, err = run_cli("--quiet", "--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""  # No output in quiet mode
    assert err == ""
