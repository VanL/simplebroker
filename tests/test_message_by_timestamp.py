"""
Test suite for message operations by timestamp/ID using the -m/--message flag.

Tests cover:
1. Validation tests (invalid timestamps)
2. Basic functionality (read/peek/delete by timestamp)
3. Not found cases
4. Concurrency tests
5. Integration tests
6. Vacuum integration
7. Workflow tests
"""

import concurrent.futures as cf
import json
import sqlite3
from pathlib import Path

from .conftest import run_cli

# ============================================================================
# Validation Tests (No Database Query)
# ============================================================================


def test_timestamp_wrong_length_returns_error(workdir: Path):
    """Test that timestamps with wrong length return exit code 2 without database query."""
    # Write a message to ensure queue exists
    run_cli("write", "test_queue", "message1", cwd=workdir)

    # Test various wrong lengths
    invalid_timestamps = [
        "123",  # Too short
        "12345678901234567890",  # Too long (20 digits)
        "1",  # Single digit
        "123456789012345678",  # 18 digits (one short)
        "12345678901234567890123",  # Way too long
        "",  # Empty string
    ]

    for ts in invalid_timestamps:
        # Read
        rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2, f"Expected exit code 2 for timestamp '{ts}', got {rc}"
        assert out == "", f"Expected no output for invalid timestamp '{ts}'"

        # Peek
        rc, out, err = run_cli("peek", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2
        assert out == ""

        # Delete
        rc, out, err = run_cli("delete", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2
        assert out == ""


def test_timestamp_non_digits_returns_error(workdir: Path):
    """Test that timestamps with non-digits return exit code 2 without database query."""
    # Write a message to ensure queue exists
    run_cli("write", "test_queue", "message1", cwd=workdir)

    # Test various non-digit patterns (all 19 chars long)
    invalid_timestamps = [
        "abc1234567890123456",  # Letters at start
        "1234567890123456abc",  # Letters at end
        "123456789a123456789",  # Letter in middle
        "1234567890123456.89",  # Decimal point
        "1234567890123456-89",  # Hyphen
        "1234567890123456 89",  # Space
        "1234567890123456789!",  # Special character
        "123456789012345678x",  # Single non-digit
    ]

    for ts in invalid_timestamps:
        # Read
        rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2, f"Expected exit code 2 for timestamp '{ts}', got {rc}"
        assert out == ""

        # Peek
        rc, out, err = run_cli("peek", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2
        assert out == ""

        # Delete
        rc, out, err = run_cli("delete", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2
        assert out == ""


def test_valid_format_proceeds_to_database(workdir: Path):
    """Test that valid format timestamps (19 digits) proceed to database query."""
    # Valid but non-existent timestamp
    valid_ts = "1234567890123456789"

    # Should get exit code 2 because queue doesn't exist, not because of format
    rc, out, err = run_cli("read", "nonexistent_queue", "-m", valid_ts, cwd=workdir)
    assert rc == 2

    # Create queue and verify we still get exit code 2 (timestamp not found)
    run_cli("write", "test_queue", "message1", cwd=workdir)
    rc, out, err = run_cli("read", "test_queue", "-m", valid_ts, cwd=workdir)
    assert rc == 2  # Not found, but format was valid


def test_other_valid_timestamp_formats_rejected(workdir: Path):
    """Test that other valid timestamp formats are rejected for -m flag."""
    # Write a message to ensure queue exists
    run_cli("write", "test_queue", "message1", cwd=workdir)

    # These are all valid timestamps for --since, but NOT for -m/--message
    invalid_for_message_flag = [
        "2024-01-15",  # ISO date
        "2024-01-15T14:30:00Z",  # ISO datetime
        "1705329000",  # Unix seconds (10 digits)
        "1705329000s",  # Unix seconds with suffix
        "1705329000000",  # Unix milliseconds (13 digits)
        "1705329000000ms",  # Unix milliseconds with suffix
        "1705329000000000000ns",  # Unix nanoseconds with suffix
        "1837025672140161024hyb",  # Hybrid timestamp with suffix
        "1.705329e9",  # Scientific notation
        "1705329000.123",  # Float format
    ]

    for ts in invalid_for_message_flag:
        # Read
        rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2, f"Expected exit code 2 for timestamp '{ts}', got {rc}"
        assert out == "", f"Expected no output for timestamp '{ts}'"

        # Peek
        rc, out, err = run_cli("peek", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2
        assert out == ""

        # Delete
        rc, out, err = run_cli("delete", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 2
        assert out == ""


# ============================================================================
# Basic Functionality Tests
# ============================================================================


def test_read_message_by_timestamp(workdir: Path):
    """Test reading a specific message by its timestamp."""
    # Write messages and get their timestamps
    run_cli("write", "test_queue", "message1", cwd=workdir)
    run_cli("write", "test_queue", "message2", cwd=workdir)
    run_cli("write", "test_queue", "message3", cwd=workdir)

    # Get timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    timestamps = [line.split("\t")[0] for line in lines]

    # Read middle message by timestamp
    rc, out, err = run_cli("read", "test_queue", "-m", timestamps[1], cwd=workdir)
    assert rc == 0
    assert out == "message2"

    # Verify message is claimed (can't read again)
    rc, out, err = run_cli("read", "test_queue", "-m", timestamps[1], cwd=workdir)
    assert rc == 2

    # Verify other messages still available
    rc, out, err = run_cli("read", "test_queue", cwd=workdir)
    assert rc == 0
    assert out == "message1"

    rc, out, err = run_cli("read", "test_queue", cwd=workdir)
    assert rc == 0
    assert out == "message3"


def test_peek_message_by_timestamp(workdir: Path):
    """Test peeking at a specific message by its timestamp."""
    # Write messages
    run_cli("write", "test_queue", "message1", cwd=workdir)
    run_cli("write", "test_queue", "message2", cwd=workdir)

    # Get timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    ts2 = lines[1].split("\t")[0]

    # Peek at second message
    rc, out, err = run_cli("peek", "test_queue", "-m", ts2, cwd=workdir)
    assert rc == 0
    assert out == "message2"

    # Peek again - should still work (not claimed)
    rc, out, err = run_cli("peek", "test_queue", "-m", ts2, cwd=workdir)
    assert rc == 0
    assert out == "message2"

    # Verify message can still be read normally
    rc, out, err = run_cli("read", "test_queue", cwd=workdir)
    assert rc == 0
    assert out == "message1"


def test_delete_message_by_timestamp(workdir: Path):
    """Test deleting a specific message by its timestamp."""
    # Write messages
    run_cli("write", "test_queue", "message1", cwd=workdir)
    run_cli("write", "test_queue", "message2", cwd=workdir)
    run_cli("write", "test_queue", "message3", cwd=workdir)

    # Get timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    timestamps = [line.split("\t")[0] for line in lines]

    # Delete middle message
    rc, out, err = run_cli("delete", "test_queue", "-m", timestamps[1], cwd=workdir)
    assert rc == 0
    assert out == ""  # Delete has no output

    # Verify message is gone
    rc, out, err = run_cli("peek", "test_queue", "-m", timestamps[1], cwd=workdir)
    assert rc == 2

    # Verify other messages still exist
    rc, out, err = run_cli("read", "test_queue", "--all", cwd=workdir)
    assert rc == 0
    assert out == "message1\nmessage3"


def test_operations_with_json_output(workdir: Path):
    """Test -m flag with JSON output format."""
    # Write message
    run_cli("write", "test_queue", "test message", cwd=workdir)

    # Get timestamp
    rc, out, err = run_cli("peek", "test_queue", "-t", "--json", cwd=workdir)
    data = json.loads(out)
    ts = str(data["timestamp"])

    # Read with JSON
    rc, out, err = run_cli("read", "test_queue", "-m", ts, "--json", cwd=workdir)
    assert rc == 0
    data = json.loads(out)
    assert data["message"] == "test message"

    # Write another and peek with JSON and timestamps
    run_cli("write", "test_queue", "another message", cwd=workdir)
    rc, out, err = run_cli("peek", "test_queue", "-t", "--json", cwd=workdir)
    data = json.loads(out)
    ts = str(data["timestamp"])

    rc, out, err = run_cli("peek", "test_queue", "-m", ts, "--json", "-t", cwd=workdir)
    assert rc == 0
    data = json.loads(out)
    assert data["message"] == "another message"
    assert "timestamp" in data


def test_operations_on_queue_positions(workdir: Path):
    """Test operations on first, middle, and last messages in queue."""
    # Write 5 messages
    for i in range(5):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Get all timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    timestamps = [line.split("\t")[0] for line in lines]

    # Read first message by timestamp
    rc, out, err = run_cli("read", "test_queue", "-m", timestamps[0], cwd=workdir)
    assert rc == 0
    assert out == "message0"

    # Read last message by timestamp
    rc, out, err = run_cli("read", "test_queue", "-m", timestamps[4], cwd=workdir)
    assert rc == 0
    assert out == "message4"

    # Read middle message by timestamp
    rc, out, err = run_cli("read", "test_queue", "-m", timestamps[2], cwd=workdir)
    assert rc == 0
    assert out == "message2"

    # Verify remaining messages in FIFO order
    rc, out, err = run_cli("read", "test_queue", "--all", cwd=workdir)
    assert rc == 0
    assert out == "message1\nmessage3"


def test_operation_on_single_message_queue(workdir: Path):
    """Test operations when queue has only one message."""
    # Write single message
    run_cli("write", "test_queue", "only message", cwd=workdir)

    # Get timestamp
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]

    # Peek by timestamp
    rc, out, err = run_cli("peek", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 0
    assert out == "only message"

    # Delete by timestamp
    rc, out, err = run_cli("delete", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 0

    # Queue should now be empty
    rc, out, err = run_cli("peek", "test_queue", cwd=workdir)
    assert rc == 2


# ============================================================================
# Not Found Cases (All Return Exit Code 2)
# ============================================================================


def test_nonexistent_timestamp_in_existing_queue(workdir: Path):
    """Test operations with non-existent timestamp in existing queue."""
    # Write a message
    run_cli("write", "test_queue", "message1", cwd=workdir)

    # Use a valid but non-existent timestamp
    fake_ts = "9999999999999999999"

    # All operations should return exit code 2
    rc, out, err = run_cli("read", "test_queue", "-m", fake_ts, cwd=workdir)
    assert rc == 2
    assert out == ""

    rc, out, err = run_cli("peek", "test_queue", "-m", fake_ts, cwd=workdir)
    assert rc == 2
    assert out == ""

    rc, out, err = run_cli("delete", "test_queue", "-m", fake_ts, cwd=workdir)
    assert rc == 2
    assert out == ""


def test_queue_isolation(workdir: Path):
    """Test that timestamp from one queue cannot access another queue."""
    # Write to different queues
    run_cli("write", "queue1", "message in queue1", cwd=workdir)
    run_cli("write", "queue2", "message in queue2", cwd=workdir)

    # Get timestamp from queue1
    rc, out, err = run_cli("peek", "queue1", "-t", cwd=workdir)
    ts_queue1 = out.split("\t")[0]

    # Try to use queue1's timestamp on queue2
    rc, out, err = run_cli("read", "queue2", "-m", ts_queue1, cwd=workdir)
    assert rc == 2

    rc, out, err = run_cli("peek", "queue2", "-m", ts_queue1, cwd=workdir)
    assert rc == 2

    rc, out, err = run_cli("delete", "queue2", "-m", ts_queue1, cwd=workdir)
    assert rc == 2

    # Verify message still in queue1
    rc, out, err = run_cli("read", "queue1", "-m", ts_queue1, cwd=workdir)
    assert rc == 0
    assert out == "message in queue1"


def test_operations_on_nonexistent_queue(workdir: Path):
    """Test operations on non-existent queue."""
    valid_ts = "1234567890123456789"

    # All operations should return exit code 2
    rc, out, err = run_cli("read", "nonexistent", "-m", valid_ts, cwd=workdir)
    assert rc == 2

    rc, out, err = run_cli("peek", "nonexistent", "-m", valid_ts, cwd=workdir)
    assert rc == 2

    rc, out, err = run_cli("delete", "nonexistent", "-m", valid_ts, cwd=workdir)
    assert rc == 2


def test_already_claimed_message(workdir: Path):
    """Test operations on already claimed message."""
    # Write and get timestamp
    run_cli("write", "test_queue", "message1", cwd=workdir)
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]

    # Read to claim it
    rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 0
    assert out == "message1"

    # Try to read again - should fail
    rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 2

    # Peek should also fail on claimed message
    rc, out, err = run_cli("peek", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 2

    # Delete should also fail on claimed message
    rc, out, err = run_cli("delete", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 2


# ============================================================================
# Concurrency Tests
# ============================================================================


def test_concurrent_read_by_timestamp(workdir: Path):
    """Test multiple processes attempting to read same timestamp."""
    # Write a message
    run_cli("write", "test_queue", "concurrent message", cwd=workdir)
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]

    # Run multiple concurrent reads
    def try_read():
        return run_cli("read", "test_queue", "-m", ts, cwd=workdir)

    with cf.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(try_read) for _ in range(5)]
        results = [f.result() for f in cf.as_completed(futures)]

    # Exactly one should succeed
    success_count = sum(1 for rc, out, err in results if rc == 0)
    assert success_count == 1

    # The successful one should have the message
    for rc, out, _err in results:
        if rc == 0:
            assert out == "concurrent message"


def test_concurrent_delete_by_timestamp(workdir: Path):
    """Test multiple processes attempting to delete same timestamp."""
    # Write a message
    run_cli("write", "test_queue", "to be deleted", cwd=workdir)
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]

    # Run multiple concurrent deletes
    def try_delete():
        return run_cli("delete", "test_queue", "-m", ts, cwd=workdir)

    with cf.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(try_delete) for _ in range(5)]
        results = [f.result() for f in cf.as_completed(futures)]

    # Exactly one should succeed
    success_count = sum(1 for rc, out, err in results if rc == 0)
    assert success_count == 1


def test_mixed_operations_no_interference(workdir: Path):
    """Test that FIFO reads and timestamp reads don't interfere."""
    # Write many messages
    for i in range(10):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Get some timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    timestamps = [line.split("\t")[0] for line in lines]

    # Mix of operations
    def fifo_read():
        return run_cli("read", "test_queue", cwd=workdir)

    def timestamp_read(ts):
        return run_cli("read", "test_queue", "-m", ts, cwd=workdir)

    with cf.ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        # Some FIFO reads
        futures.extend([executor.submit(fifo_read) for _ in range(3)])
        # Some timestamp reads
        futures.extend(
            [executor.submit(timestamp_read, timestamps[5]) for _ in range(1)]
        )
        futures.extend(
            [executor.submit(timestamp_read, timestamps[7]) for _ in range(1)]
        )
        futures.extend(
            [executor.submit(timestamp_read, timestamps[9]) for _ in range(1)]
        )

        results = [f.result() for f in cf.as_completed(futures)]

    # All should succeed
    for rc, out, _err in results:
        assert rc == 0
        assert out.startswith("message")

    # Check we got the right messages
    messages = [out for rc, out, err in results]
    assert "message5" in messages
    assert "message7" in messages
    assert "message9" in messages


def test_concurrent_peek_all_succeed(workdir: Path):
    """Test that concurrent peek operations all succeed."""
    # Write a message
    run_cli("write", "test_queue", "peek message", cwd=workdir)
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]

    # Run multiple concurrent peeks
    def try_peek():
        return run_cli("peek", "test_queue", "-m", ts, cwd=workdir)

    with cf.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(try_peek) for _ in range(5)]
        results = [f.result() for f in cf.as_completed(futures)]

    # All should succeed (peek doesn't claim)
    for rc, out, _err in results:
        assert rc == 0
        assert out == "peek message"


# ============================================================================
# Integration Tests
# ============================================================================


def test_read_by_timestamp_affects_peek_all(workdir: Path):
    """Test that message read by timestamp doesn't appear in peek --all."""
    # Write messages
    for i in range(5):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Get timestamp of message2
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    ts_message2 = lines[2].split("\t")[0]

    # Read message2 by timestamp
    rc, out, err = run_cli("read", "test_queue", "-m", ts_message2, cwd=workdir)
    assert rc == 0

    # Peek all - message2 should be missing
    rc, out, err = run_cli("peek", "test_queue", "--all", cwd=workdir)
    assert rc == 0
    messages = out.strip().split("\n")
    assert len(messages) == 4
    assert "message2" not in messages


def test_read_by_timestamp_affects_read_since(workdir: Path):
    """Test that message read by timestamp doesn't appear in read --since."""
    # Write messages
    for i in range(5):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Get timestamp of message1
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    ts_message0 = lines[0].split("\t")[0]
    ts_message3 = lines[3].split("\t")[0]

    # Read message3 by timestamp
    rc, out, err = run_cli("read", "test_queue", "-m", ts_message3, cwd=workdir)
    assert rc == 0

    # Read all messages since message0
    rc, out, err = run_cli(
        "read", "test_queue", "--all", "--since", ts_message0, cwd=workdir
    )
    assert rc == 0
    messages = out.strip().split("\n")
    assert "message3" not in messages
    assert "message1" in messages
    assert "message2" in messages
    assert "message4" in messages


def test_list_command_reflects_operations(workdir: Path):
    """Test that list command shows correct counts after timestamp operations."""
    # Write messages
    for i in range(5):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Check initial count
    rc, out, err = run_cli("list", cwd=workdir)
    assert "test_queue: 5" in out

    # Get and read a message by timestamp
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]
    run_cli("read", "test_queue", "-m", ts, cwd=workdir)

    # list should show 4 unclaimed
    rc, out, err = run_cli("list", cwd=workdir)
    assert "test_queue: 4" in out

    # list with --stats should show claimed
    rc, out, err = run_cli("list", "--stats", cwd=workdir)
    assert "test_queue: 4 (5 total, 1 claimed)" in out


def test_mutual_exclusivity_with_all(workdir: Path):
    """Test that --message cannot be used with --all."""
    # This should be caught at argument parsing level
    rc, out, err = run_cli(
        "read", "test_queue", "-m", "1234567890123456789", "--all", cwd=workdir
    )
    assert rc != 0
    assert "cannot be used with --all" in err or "not allowed with argument" in err


def test_mutual_exclusivity_with_since(workdir: Path):
    """Test that --message cannot be used with --since."""
    # This should be caught at argument parsing level
    rc, out, err = run_cli(
        "read", "test_queue", "-m", "1234567890123456789", "--since", "0", cwd=workdir
    )
    assert rc != 0
    assert "cannot be used with" in err or "not allowed with argument" in err


# ============================================================================
# Vacuum Integration Tests
# ============================================================================


def test_read_by_timestamp_gets_vacuumed(workdir: Path):
    """Test that messages read by timestamp are properly vacuumed."""
    db_path = workdir / ".broker.db"

    # Write messages
    for i in range(3):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Get timestamp and read by it
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]
    run_cli("read", "test_queue", "-m", ts, cwd=workdir)

    # Check message is claimed
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 1
    conn.close()

    # Run vacuum
    run_cli("--vacuum", cwd=workdir)

    # Check message is gone
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 0
    cursor.execute("SELECT COUNT(*) FROM messages")
    total_count = cursor.fetchone()[0]
    assert total_count == 2  # Only 2 messages left
    conn.close()


def test_delete_by_timestamp_gets_vacuumed(workdir: Path):
    """Test that messages deleted by timestamp are properly vacuumed."""
    db_path = workdir / ".broker.db"

    # Write messages
    for i in range(3):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Get timestamp and delete by it
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    ts = lines[1].split("\t")[0]  # Delete middle message
    run_cli("delete", "test_queue", "-m", ts, cwd=workdir)

    # Check message is claimed
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 1
    conn.close()

    # Run vacuum
    run_cli("--vacuum", cwd=workdir)

    # Check claimed message is gone
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages")
    total_count = cursor.fetchone()[0]
    assert total_count == 2  # Only 2 messages left
    conn.close()


def test_peek_by_timestamp_not_vacuumed(workdir: Path):
    """Test that messages peeked by timestamp are not affected by vacuum."""
    db_path = workdir / ".broker.db"

    # Write a message
    run_cli("write", "test_queue", "message1", cwd=workdir)

    # Get timestamp and peek by it
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    ts = out.split("\t")[0]
    run_cli("peek", "test_queue", "-m", ts, cwd=workdir)

    # Run vacuum
    run_cli("--vacuum", cwd=workdir)

    # Message should still be there
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 0")
    unclaimed_count = cursor.fetchone()[0]
    assert unclaimed_count == 1
    conn.close()

    # Can still read it
    rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 0
    assert out == "message1"


# ============================================================================
# Workflow Tests
# ============================================================================


def test_workflow_peek_then_use_timestamp(workdir: Path):
    """Test workflow: get timestamp via peek -t, then use it."""
    # Write message
    run_cli("write", "test_queue", "workflow message", cwd=workdir)

    # Get timestamp via peek
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    assert rc == 0
    ts = out.split("\t")[0]

    # Use timestamp to read
    rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 0
    assert out == "workflow message"


def test_workflow_json_parse_timestamp(workdir: Path):
    """Test workflow: get timestamp via JSON, parse, use with -m."""
    # Write message
    run_cli("write", "test_queue", "json workflow", cwd=workdir)

    # Get timestamp via JSON
    rc, out, err = run_cli("peek", "test_queue", "-t", "--json", cwd=workdir)
    assert rc == 0
    data = json.loads(out)
    ts = str(data["timestamp"])

    # Use parsed timestamp
    rc, out, err = run_cli("read", "test_queue", "-m", ts, cwd=workdir)
    assert rc == 0
    assert out == "json workflow"


def test_workflow_broadcast_then_read_by_timestamp(workdir: Path):
    """Test workflow: broadcast message, read by timestamp from any queue."""
    # Create multiple queues
    run_cli("write", "queue1", "dummy", cwd=workdir)
    run_cli("write", "queue2", "dummy", cwd=workdir)
    run_cli("write", "queue3", "dummy", cwd=workdir)

    # Clear them by reading (not deleting, so queues still exist)
    run_cli("read", "queue1", cwd=workdir)
    run_cli("read", "queue2", cwd=workdir)
    run_cli("read", "queue3", cwd=workdir)

    # Broadcast
    run_cli("broadcast", "broadcast message", cwd=workdir)

    # Get timestamp from queue2
    rc, out, err = run_cli("peek", "queue2", "-t", cwd=workdir)
    ts = out.split("\t")[0]

    # Read from queue2 using timestamp
    rc, out, err = run_cli("read", "queue2", "-m", ts, cwd=workdir)
    assert rc == 0
    assert out == "broadcast message"

    # Can't use same timestamp on different queue
    rc, out, err = run_cli("read", "queue1", "-m", ts, cwd=workdir)
    assert rc == 2


def test_workflow_selective_message_removal(workdir: Path):
    """Test workflow: selectively remove specific messages while preserving others."""
    # Create a queue with mixed messages
    messages = [
        "normal message 1",
        "ERROR: poison pill",
        "normal message 2",
        "ERROR: another bad one",
        "normal message 3",
    ]

    for msg in messages:
        run_cli("write", "test_queue", msg, cwd=workdir)

    # Get all messages with timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")

    # Find and remove error messages by timestamp
    for line in lines:
        ts, msg = line.split("\t", 1)
        if "ERROR:" in msg:
            rc, out, err = run_cli("delete", "test_queue", "-m", ts, cwd=workdir)
            assert rc == 0

    # Verify only normal messages remain
    rc, out, err = run_cli("read", "test_queue", "--all", cwd=workdir)
    assert rc == 0
    remaining = out.strip().split("\n")
    assert len(remaining) == 3
    assert all("ERROR:" not in msg for msg in remaining)
    assert "normal message 1" in remaining
    assert "normal message 2" in remaining
    assert "normal message 3" in remaining


# Edge Cases
# ============================================================================


def test_timestamp_boundary_values(workdir: Path):
    """Test operations with boundary timestamp values."""
    # Write a message to get a real timestamp
    run_cli("write", "test_queue", "test message", cwd=workdir)
    rc, out, err = run_cli("peek", "test_queue", "-t", cwd=workdir)
    real_ts = out.split("\t")[0]

    # Test with minimum valid timestamp (all zeros)
    min_ts = "0" * 19
    rc, out, err = run_cli("read", "test_queue", "-m", min_ts, cwd=workdir)
    assert rc == 2  # Not found

    # Test with maximum valid timestamp (all nines)
    max_ts = "9" * 19
    rc, out, err = run_cli("read", "test_queue", "-m", max_ts, cwd=workdir)
    assert rc == 2  # Not found

    # Test with timestamp near 2^63 (maximum SQLite signed integer)
    # 2^63 - 1 = 9223372036854775807 (19 digits)
    near_max_ts = "9223372036854775807"
    rc, out, err = run_cli("read", "test_queue", "-m", near_max_ts, cwd=workdir)
    assert rc == 2  # Not found, but should be accepted as valid format

    # Test with timestamp at exactly 2^63 (would overflow)
    # 2^63 = 9223372036854775808 (19 digits)
    overflow_ts = "9223372036854775808"
    rc, out, err = run_cli("read", "test_queue", "-m", overflow_ts, cwd=workdir)
    assert rc == 2  # Should still return 2 (not found) not crash

    # The real timestamp should work
    rc, out, err = run_cli("read", "test_queue", "-m", real_ts, cwd=workdir)
    assert rc == 0
    assert out == "test message"


def test_special_characters_in_messages(workdir: Path):
    """Test timestamp operations with messages containing special characters."""
    special_messages = [
        "message\nwith\nnewlines",
        "message\twith\ttabs",
        'message with "quotes"',
        "message with 'quotes'",
        "message with \\ backslash",
        "message with 🚀 emoji",
    ]

    for msg in special_messages:
        run_cli("write", "test_queue", msg, cwd=workdir)

    # Get all timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", "--json", cwd=workdir)
    lines = out.strip().split("\n")

    # Read each by timestamp and verify content
    for i, line in enumerate(lines):
        data = json.loads(line)
        ts = str(data["timestamp"])

        # Read by timestamp with JSON output
        rc, out, err = run_cli("read", "test_queue", "-m", ts, "--json", cwd=workdir)
        assert rc == 0
        result = json.loads(out)
        assert result["message"] == special_messages[i]


def test_rapid_succession_timestamps(workdir: Path):
    """Test operations on messages written in rapid succession."""
    # Write messages as fast as possible
    for i in range(10):
        run_cli("write", "test_queue", f"rapid{i}", cwd=workdir)

    # Get all timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    timestamps = [line.split("\t")[0] for line in lines]

    # Verify all timestamps are unique
    assert len(timestamps) == len(set(timestamps))

    # Verify we can read each message by its timestamp
    for i, ts in enumerate(timestamps):
        rc, out, err = run_cli("peek", "test_queue", "-m", ts, cwd=workdir)
        assert rc == 0
        assert out == f"rapid{i}"
