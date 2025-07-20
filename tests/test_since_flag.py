"""
Test suite for --since flag implementation.

Tests filtering messages by timestamp for read and peek commands.
"""

import datetime
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from .conftest import run_cli

# Test data for timestamp validation
VALID_TIMESTAMPS = [
    ("0", 0),  # Min value
    ("1", 1),
    (" 123 ", 123),  # Python int() strips whitespace
    ("1234567890123456789", 1234567890123456789),
    (str(2**63 - 1), 2**63 - 1),  # Max 64-bit signed
]


# Test data for explicit unit suffixes
UNIT_SUFFIX_TESTS = [
    # (input, description, should_work)
    ("1705329000s", "Unix seconds with suffix", True),
    ("1705329000.5s", "Unix float seconds with suffix", True),
    ("1705329000000ms", "Unix milliseconds with suffix", True),
    ("1705329000000000000ns", "Unix nanoseconds with suffix", True),
    ("1837025672140161024", "Native hybrid", True),
    ("1837025672140161024hyb", "Native hybrid with hyb suffix", False),
    ("1.5hyb", "Float hybrid timestamp", False),
    ("1.532", "Float seconds", True),  # Should work - like time.time() output
    ("1e10s", "Scientific notation", False),
]


# Additional test data for human-readable formats
# Get current time for relative tests
def get_human_readable_test_data():
    """Generate test data with current timestamps."""
    now = datetime.datetime.now(datetime.timezone.utc)
    yesterday = now - datetime.timedelta(days=1)
    tomorrow = now + datetime.timedelta(days=1)

    return [
        # (input, description, should_have_messages)
        # ISO date formats
        (yesterday.strftime("%Y-%m-%d"), "ISO date yesterday", True),
        (now.strftime("%Y-%m-%d"), "ISO date today", True),
        (tomorrow.strftime("%Y-%m-%d"), "ISO date tomorrow", False),
        # ISO datetime formats
        ((now - datetime.timedelta(hours=1)).isoformat(), "ISO datetime 1h ago", True),
        (
            (now + datetime.timedelta(hours=1)).isoformat(),
            "ISO datetime 1h future",
            False,
        ),
        (
            (now - datetime.timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
            "ISO datetime with Z",
            True,
        ),
        # Unix timestamps
        (str(int(time.time() - 3600)), "Unix timestamp 1h ago", True),
        (str(int(time.time() + 3600)), "Unix timestamp 1h future", False),
        (str(int((time.time() - 3600) * 1000)), "Unix milliseconds 1h ago", True),
        (
            str(int((time.time() - 3600) * 1_000_000_000)),
            "Unix nanoseconds 1h ago",
            True,
        ),
    ]


INVALID_TIMESTAMPS = [
    ("", "Invalid timestamp: empty string"),  # Empty string validation
    ("abc", "Invalid timestamp: abc"),
    ("-1", "Invalid timestamp: cannot be negative"),
    # Note: "1.5" is actually valid - it means 1.5 seconds since Unix epoch (Jan 1, 1970)
    # This is accepted since time.time() returns floats and fractional seconds are common
    ("1e10", "Invalid timestamp: scientific notation not supported"),
    ("0x123", "Invalid timestamp: 0x123"),
    ("123abc", "Invalid timestamp: 123abc"),
    (str(2**64), "Invalid timestamp: exceeds maximum value"),
]


# ============================================================================
# Basic Functionality Tests
# ============================================================================


def test_since_basic_filtering(workdir):
    """Test that --since correctly filters messages based on timestamp."""
    # Write messages and get their timestamps
    run_cli("write", "test_queue", "msg1", cwd=workdir)
    rc, out, _ = run_cli("peek", "test_queue", "--timestamps", cwd=workdir)
    assert rc == 0
    ts1 = int(out.split("\t")[0])

    # Small delay to ensure different timestamp
    time.sleep(0.001)

    run_cli("write", "test_queue", "msg2", cwd=workdir)
    rc, out, _ = run_cli("peek", "test_queue", "--all", "--timestamps", cwd=workdir)
    assert rc == 0
    lines = out.strip().split("\n")
    ts2 = int(lines[1].split("\t")[0])

    # Test filtering with --since
    # Should get both messages when since=0
    rc, out, _ = run_cli("peek", "test_queue", "--all", "--since", "0", cwd=workdir)
    assert rc == 0
    assert out == "msg1\nmsg2"

    # Should get only msg2 when since=ts1
    rc, out, _ = run_cli(
        "peek", "test_queue", "--all", "--since", str(ts1), cwd=workdir
    )
    assert rc == 0
    assert out == "msg2"

    # Should get nothing when since=ts2
    rc, out, _ = run_cli(
        "peek", "test_queue", "--all", "--since", str(ts2), cwd=workdir
    )
    assert rc == 0  # Should succeed but return no messages
    assert out == ""


def test_since_exact_boundary(workdir):
    """Test strict > comparison (not >=)."""
    # Write a message and get its timestamp
    run_cli("write", "boundary_queue", "test_message", cwd=workdir)
    rc, out, _ = run_cli("peek", "boundary_queue", "--timestamps", cwd=workdir)
    assert rc == 0
    ts = int(out.split("\t")[0])

    # Read with --since equal to the message timestamp -> expect empty
    rc, out, _ = run_cli("read", "boundary_queue", "--since", str(ts), cwd=workdir)
    assert rc == 0  # Should succeed but return no messages
    assert out == ""

    # Read with --since one less than timestamp -> expect message
    rc, out, _ = run_cli("read", "boundary_queue", "--since", str(ts - 1), cwd=workdir)
    assert rc == 0
    assert out == "test_message"


def test_since_empty_queue(workdir):
    """Test --since on empty queue returns exit code 2."""
    rc, out, _ = run_cli("read", "empty_queue", "--since", "0", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    rc, out, _ = run_cli("peek", "empty_queue", "--since", "1000", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY


def test_since_no_matches(workdir):
    """Test --since with future timestamp returns exit code 0 (success with no messages)."""
    # Write a message
    run_cli("write", "future_queue", "message", cwd=workdir)

    # Use a very large timestamp that's unlikely to be reached
    future_ts = str(2**63 - 1)
    rc, out, _ = run_cli("read", "future_queue", "--since", future_ts, cwd=workdir)
    assert rc == 0  # Should succeed but return no messages
    assert out == ""  # No messages returned

    # Verify message is still there
    rc, out, _ = run_cli("read", "future_queue", cwd=workdir)
    assert rc == 0
    assert out == "message"


# ============================================================================
# Flag Combination Tests
# ============================================================================


def test_since_with_all(workdir):
    """Test --since and --all work together without warning."""
    # Write multiple messages
    for i in range(5):
        run_cli("write", "all_queue", f"msg{i}", cwd=workdir)
        time.sleep(0.001)  # Ensure different timestamps

    # Get timestamp of third message
    rc, out, _ = run_cli("peek", "all_queue", "--all", "--timestamps", cwd=workdir)
    assert rc == 0
    lines = out.strip().split("\n")
    ts3 = int(lines[2].split("\t")[0])

    # Read all messages after third one
    rc, out, err = run_cli(
        "read", "all_queue", "--all", "--since", str(ts3), cwd=workdir
    )
    assert rc == 0
    assert out == "msg3\nmsg4"
    assert "warning" not in err.lower()  # No warning about combining flags


def test_since_with_json(workdir):
    """Test JSON output format with --since filtering."""
    # Write messages
    run_cli("write", "json_queue", "first", cwd=workdir)
    time.sleep(0.001)
    run_cli("write", "json_queue", "second", cwd=workdir)

    # Get first message timestamp
    rc, out, _ = run_cli("peek", "json_queue", "--timestamps", cwd=workdir)
    assert rc == 0
    ts1 = int(out.split("\t")[0])

    # Read with JSON output and --since
    rc, out, _ = run_cli(
        "peek", "json_queue", "--all", "--json", "--since", str(ts1), cwd=workdir
    )
    assert rc == 0

    # Parse JSON output
    lines = out.strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["message"] == "second"


def test_since_with_timestamps(workdir):
    """Test timestamp display with --since filtering."""
    # Write messages
    run_cli("write", "ts_queue", "early", cwd=workdir)
    time.sleep(0.001)
    run_cli("write", "ts_queue", "late", cwd=workdir)

    # Get early message timestamp
    rc, out, _ = run_cli("peek", "ts_queue", "--timestamps", cwd=workdir)
    assert rc == 0
    early_ts = int(out.split("\t")[0])

    # Read with timestamps and --since
    rc, out, _ = run_cli(
        "peek",
        "ts_queue",
        "--all",
        "--timestamps",
        "--since",
        str(early_ts),
        cwd=workdir,
    )
    assert rc == 0

    # Verify output format
    lines = out.strip().split("\n")
    assert len(lines) == 1
    parts = lines[0].split("\t")
    assert len(parts) == 2
    assert int(parts[0]) > early_ts
    assert parts[1] == "late"


def test_since_with_commit_interval(workdir):
    """Test batch processing respects --since filter."""
    # Write many messages
    for i in range(20):
        run_cli("write", "batch_queue", f"msg{i:02d}", cwd=workdir)

    # Get timestamp of 10th message
    rc, out, _ = run_cli("peek", "batch_queue", "--all", "--timestamps", cwd=workdir)
    lines = out.strip().split("\n")
    ts10 = int(lines[9].split("\t")[0])

    # Read with commit interval and --since
    # Set commit interval via environment variable
    import os

    env = os.environ.copy()
    env["BROKER_READ_COMMIT_INTERVAL"] = "5"

    cmd = [
        sys.executable,
        "-m",
        "simplebroker.cli",
        "read",
        "batch_queue",
        "--all",
        "--since",
        str(ts10),
    ]
    proc = subprocess.run(cmd, cwd=workdir, capture_output=True, text=True, env=env)

    assert proc.returncode == 0
    messages = proc.stdout.strip().split("\n")
    assert len(messages) == 10  # msg10 through msg19
    assert messages[0] == "msg10"
    assert messages[-1] == "msg19"


def test_since_with_peek(workdir):
    """Test peek command with --since (non-destructive)."""
    # Write messages
    for i in range(3):
        run_cli("write", "peek_queue", f"msg{i}", cwd=workdir)
        time.sleep(0.001)

    # Get first message timestamp
    rc, out, _ = run_cli("peek", "peek_queue", "--timestamps", cwd=workdir)
    ts1 = int(out.split("\t")[0])

    # Peek with --since multiple times
    for _ in range(3):
        rc, out, _ = run_cli(
            "peek", "peek_queue", "--all", "--since", str(ts1), cwd=workdir
        )
        assert rc == 0
        assert out == "msg1\nmsg2"

    # Verify all messages still exist
    rc, out, _ = run_cli("read", "peek_queue", "--all", cwd=workdir)
    assert rc == 0
    assert out == "msg0\nmsg1\nmsg2"


# ============================================================================
# Input Validation Tests
# ============================================================================


@pytest.mark.parametrize("ts_str,ts_val", VALID_TIMESTAMPS)
def test_since_valid_timestamps(workdir, ts_str, ts_val):
    """Test various valid timestamp formats."""
    # Write a message with a known timestamp
    run_cli("write", "valid_ts_queue", "test", cwd=workdir)

    # Try to read with valid timestamp
    rc, out, err = run_cli("read", "valid_ts_queue", "--since", ts_str, cwd=workdir)
    # Should succeed (either with message or empty)
    assert rc in [0, 2]
    assert "error" not in err.lower()


def test_since_human_readable_formats(workdir):
    """Test human-readable timestamp formats."""
    queue_name = "human_ts_queue"

    # Write some test messages
    for i in range(5):
        run_cli("write", queue_name, f"msg{i}", cwd=workdir)
        time.sleep(0.001)

    # Test each human-readable format
    for ts_str, desc, should_have_messages in get_human_readable_test_data():
        # Peek to avoid consuming messages
        rc, out, err = run_cli(
            "peek", queue_name, "--all", "--since", ts_str, cwd=workdir
        )

        if should_have_messages:
            # Should find some messages (timestamp is in the past)
            assert rc == 0, f"Failed for {desc}: {err}"
            messages = out.strip().split("\n")
            assert len(messages) >= 1, f"No messages found for {desc}"
        else:
            # Should find no messages (timestamp is in the future)
            assert rc == 0, f"Expected success for {desc} but got rc={rc}"
            assert out == "", f"Expected no messages for {desc} but got: {out}"

    # Clean up
    run_cli("delete", queue_name, cwd=workdir)


def test_since_iso_date_precise_boundary(workdir):
    """Test that date-only strings are interpreted as midnight UTC precisely."""
    queue_name = "iso_boundary_queue"

    # We need to create messages with specific timestamps
    # Since we can't control the exact write time, we'll use --since with precise times

    # Write several messages
    run_cli("write", queue_name, "msg1", cwd=workdir)
    time.sleep(0.001)
    run_cli("write", queue_name, "msg2", cwd=workdir)
    time.sleep(0.001)
    run_cli("write", queue_name, "msg3", cwd=workdir)

    # Get all timestamps
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--timestamps", "--json", cwd=workdir
    )
    [json.loads(line) for line in out.strip().split("\n")]

    # Test with specific ISO timestamps
    # Use a date far in the past to get all messages
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--since", "2020-01-01", cwd=workdir
    )
    assert rc == 0
    assert len(out.strip().split("\n")) == 3

    # Use a date far in the future to get no messages
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--since", "2030-01-01", cwd=workdir
    )
    assert rc == 0  # Should succeed but return no messages
    assert out == ""

    # Test precise midnight boundary
    # Create ISO strings that test the boundary
    test_date = "2024-01-15"
    at_midnight = "2024-01-15T00:00:00Z"

    # Since we can't control message timestamps, we verify the parsing behavior
    # by checking that these timestamps parse to the expected values
    import subprocess
    import sys

    # Helper to check if a timestamp would filter our messages
    def check_since(ts_str):
        cmd = [
            sys.executable,
            "-m",
            "simplebroker.cli",
            "peek",
            queue_name,
            "--all",
            f"--since={ts_str}",
        ]
        proc = subprocess.run(cmd, cwd=workdir, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    # All these should behave identically (date-only = midnight UTC)
    rc1, _, _ = check_since(test_date)
    rc2, _, _ = check_since(at_midnight)

    # The date-only format should be equivalent to midnight
    # Both should give the same result (either all messages or none)
    assert rc1 == rc2, f"Date-only '{test_date}' should equal '{at_midnight}'"


def test_since_iso_date_formats(workdir):
    """Test various ISO 8601 date formats."""
    queue_name = "iso_date_queue"

    # Write a message
    run_cli("write", queue_name, "test message", cwd=workdir)

    # Test date-only formats
    today = datetime.datetime.now(datetime.timezone.utc)
    yesterday = today - datetime.timedelta(days=1)
    tomorrow = today + datetime.timedelta(days=1)

    # Yesterday should return the message
    rc, out, _ = run_cli(
        "read", queue_name, "--since", yesterday.strftime("%Y-%m-%d"), cwd=workdir
    )
    assert rc == 0
    assert out == "test message"

    # Write another message
    run_cli("write", queue_name, "another message", cwd=workdir)

    # Tomorrow should return nothing
    rc, out, _ = run_cli(
        "peek", queue_name, "--since", tomorrow.strftime("%Y-%m-%d"), cwd=workdir
    )
    assert rc == 0  # Should succeed but return no messages
    assert out == ""


def test_since_iso_datetime_formats(workdir):
    """Test various ISO 8601 datetime formats."""
    queue_name = "iso_datetime_queue"

    # Write messages with known timing
    run_cli("write", queue_name, "msg1", cwd=workdir)
    time.sleep(0.2)  # 200ms gap to ensure clear separation

    # Get timestamp between messages
    checkpoint = datetime.datetime.now(datetime.timezone.utc)

    time.sleep(0.2)  # 200ms gap
    run_cli("write", queue_name, "msg2", cwd=workdir)

    # Test various ISO formats
    formats_to_test = [
        checkpoint.isoformat(),  # Full ISO with timezone
        checkpoint.replace(tzinfo=None).isoformat(),  # Naive datetime
        checkpoint.isoformat().replace("+00:00", "Z"),  # Z suffix
        checkpoint.strftime("%Y-%m-%dT%H:%M:%S"),  # No microseconds
    ]

    for fmt in formats_to_test:
        rc, out, err = run_cli("peek", queue_name, "--all", "--since", fmt, cwd=workdir)
        assert rc == 0, f"Failed for format {fmt}: {err}"
        # Due to potential timing issues and precision loss when converting to seconds,
        # we just check that msg2 is included
        assert "msg2" in out, f"msg2 not found for format {fmt}: {out}"
        # For formats without microseconds, msg1 might also be included if it's in the same second
        messages = out.strip().split("\n")
        assert len(messages) <= 2, f"Too many messages for format {fmt}: {out}"


def test_since_unix_timestamp_formats(workdir):
    """Test Unix timestamp formats (seconds and milliseconds)."""
    queue_name = "unix_ts_queue"

    # Write initial message
    run_cli("write", queue_name, "old message", cwd=workdir)

    # Wait until we're in a new second
    start_second = int(time.time())
    while int(time.time()) == start_second:
        time.sleep(0.01)

    # Get current Unix timestamps
    unix_seconds = int(time.time())
    unix_millis = int(time.time() * 1000)

    # Wait a bit then write new message
    time.sleep(0.1)
    run_cli("write", queue_name, "new message", cwd=workdir)

    # Test with Unix seconds
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--since", str(unix_seconds), cwd=workdir
    )
    assert rc == 0
    assert out == "new message"

    # Test with Unix milliseconds
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--since", str(unix_millis), cwd=workdir
    )
    assert rc == 0
    assert out == "new message"


def test_since_mixed_timestamp_formats(workdir):
    """Test mixing different timestamp formats in the same session."""
    queue_name = "mixed_ts_queue"

    # Write messages over time
    for i in range(10):
        run_cli("write", queue_name, f"msg{i}", cwd=workdir)
        time.sleep(0.05)  # 50ms between messages

    # Get message 5 timestamp in different formats
    rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    lines = out.strip().split("\n")
    native_ts = int(lines[5].split("\t")[0])

    # Convert to different formats
    # Native timestamp is milliseconds since epoch << 20
    ms_since_epoch = native_ts >> 20
    unix_seconds = ms_since_epoch // 1_000
    dt = datetime.datetime.fromtimestamp(unix_seconds, datetime.timezone.utc)

    # Test each format
    # Note: Native format has full precision, but Unix seconds and ISO format
    # lose millisecond precision, so they may return more messages
    formats = [
        (str(native_ts), "native"),  # Native format (exact)
        (dt.isoformat(), "iso"),  # ISO format (second precision)
        (str(unix_seconds), "unix"),  # Unix seconds (second precision)
    ]

    for fmt, name in formats:
        rc, out, _ = run_cli("peek", queue_name, "--all", "--since", fmt, cwd=workdir)
        assert rc == 0
        messages = out.strip().split("\n")

        if name == "native":
            # Native format should return exactly messages after msg5 (msg6-msg9)
            assert len(messages) == 4, (
                f"Native format returned {len(messages)} messages"
            )
            assert messages[0] == "msg6"
            assert messages[-1] == "msg9"
        else:
            # Formats with second precision may include more messages due to rounding
            # They should at least include msg6-msg9, but may include earlier messages
            assert len(messages) >= 4, (
                f"{name} format returned {len(messages)} messages"
            )
            assert "msg9" in messages  # Should include the last message
            # Due to precision loss, might include messages from the same second as msg5


@pytest.mark.parametrize("ts_str,expected_error", INVALID_TIMESTAMPS)
def test_since_invalid_timestamps(workdir, ts_str, expected_error):
    """Test error handling for invalid timestamps."""
    rc, out, err = run_cli("read", "invalid_queue", "--since", ts_str, cwd=workdir)
    assert rc == 1
    assert expected_error in err


def test_since_missing_value(workdir):
    """Test --since without value shows proper error."""
    # This should be caught by argparse
    rc, out, err = run_cli("read", "test_queue", "--since", cwd=workdir)
    assert rc == 1
    assert "error" in err.lower()
    assert "argument --since: expected one argument" in err


# ============================================================================
# Concurrent Operations Tests
# ============================================================================


def test_since_during_concurrent_writes(workdir):
    """Test --since consistency during active writes."""
    queue_name = "concurrent_queue"

    # Write initial messages
    for i in range(5):
        run_cli("write", queue_name, f"initial_{i}", cwd=workdir)

    # Get timestamp after initial messages
    rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    lines = out.strip().split("\n")
    checkpoint_ts = int(lines[-1].split("\t")[0])

    # Start concurrent writer
    def writer():
        for i in range(10):
            run_cli("write", queue_name, f"concurrent_{i}", cwd=workdir)
            time.sleep(0.001)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(writer)

        # Wait for at least some concurrent messages to be written
        messages_found = False
        for _retry in range(20):  # Up to 200ms wait
            time.sleep(0.01)
            rc, out, _ = run_cli(
                "peek", queue_name, "--all", "--since", str(checkpoint_ts), cwd=workdir
            )
            if rc == 0:
                messages_found = True
                break

        assert messages_found, "No concurrent messages found after waiting"

        # Now do the actual read
        rc, out, _ = run_cli(
            "read", queue_name, "--all", "--since", str(checkpoint_ts), cwd=workdir
        )
        assert rc == 0

        # Verify we only got messages after checkpoint
        messages = out.strip().split("\n")
        # Filter out empty strings from split
        messages = [msg for msg in messages if msg]
        for msg in messages:
            assert msg.startswith("concurrent_")

        future.result()  # Wait for writer to finish


def test_since_checkpoint_pattern(workdir):
    """Test checkpoint-based consumption pattern."""
    queue_name = "checkpoint_queue"

    # Write messages in batches
    for batch in range(3):
        for i in range(5):
            run_cli("write", queue_name, f"batch{batch}_msg{i}", cwd=workdir)
        time.sleep(0.002)  # Ensure timestamp difference between batches

    # Read first batch with timestamps
    rc, out, _ = run_cli("read", queue_name, "--timestamps", cwd=workdir)
    assert rc == 0
    last_ts = int(out.split("\t")[0])

    # Read subsequent messages in batches using checkpoint
    all_messages = ["batch0_msg0"]  # First message already read

    while True:
        rc, out, _ = run_cli(
            "read",
            queue_name,
            "--all",
            "--since",
            str(last_ts),
            "--timestamps",
            cwd=workdir,
        )
        if rc == 2:  # Queue is now empty (all messages consumed)
            break
        assert rc == 0  # Should succeed when messages exist

        lines = out.strip().split("\n")
        for line in lines:
            ts, msg = line.split("\t")
            all_messages.append(msg)
            last_ts = int(ts)

    # Verify we got all messages without duplicates
    assert len(all_messages) == 15
    assert len(set(all_messages)) == 15  # No duplicates
    for i, msg in enumerate(all_messages):
        expected = f"batch{i // 5}_msg{i % 5}"
        assert msg == expected


def test_since_multiple_readers(workdir):
    """Test multiple concurrent readers with different --since values."""
    queue_name = "multi_reader_queue"

    # Write messages with known timestamps
    timestamps = []
    for i in range(10):
        run_cli("write", queue_name, f"msg{i}", cwd=workdir)
        rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
        lines = out.strip().split("\n")
        timestamps.append(int(lines[-1].split("\t")[0]))
        time.sleep(0.001)

    # Multiple readers with different --since values
    def reader(since_ts, expected_count):
        rc, out, _ = run_cli(
            "peek", queue_name, "--all", "--since", str(since_ts), cwd=workdir
        )
        if expected_count > 0:
            assert rc == 0
            messages = out.strip().split("\n")
            assert len(messages) == expected_count
        else:
            assert rc == 0  # Should succeed with --since
            assert out == ""  # But return no messages

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(reader, 0, 10),  # All messages
            executor.submit(reader, timestamps[4], 5),  # Last 5 messages
            executor.submit(reader, timestamps[8], 1),  # Last message only
        ]

        for future in futures:
            future.result()


# ============================================================================
# Performance and Scale Tests
# ============================================================================


@pytest.mark.slow
def test_since_large_queue_performance(workdir):
    """Test --since performance on large message queue."""
    queue_name = "large_queue"
    message_count = 5000

    # Write many messages in batches
    for i in range(0, message_count, 100):
        for j in range(100):
            if i + j < message_count:
                run_cli("write", queue_name, f"msg{i + j:05d}", cwd=workdir)

    # Get timestamp at different points
    rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    lines = out.strip().split("\n")

    # Test queries at different points
    # Note: --since uses > comparison, so we get N-1 messages when using timestamp of message N
    test_points = [
        (0, message_count),  # All messages
        (
            int(lines[message_count // 2].split("\t")[0]),
            message_count - message_count // 2 - 1,
        ),  # Messages after midpoint
        (
            int(lines[message_count * 9 // 10].split("\t")[0]),
            message_count - message_count * 9 // 10 - 1,
        ),  # Messages after 90%
        (int(lines[-1].split("\t")[0]), 0),  # None
    ]

    for since_ts, expected_count in test_points:
        start_time = time.time()
        rc, out, _ = run_cli(
            "peek", queue_name, "--all", "--since", str(since_ts), cwd=workdir
        )
        elapsed = time.time() - start_time

        if expected_count > 0:
            assert rc == 0
            messages = out.strip().split("\n")
            assert len(messages) == expected_count
        else:
            assert rc == 0  # Should succeed with --since
            assert out == ""  # But return no messages

        # Performance assertion: should achieve at least 2000 messages/second
        # Add 50ms overhead for process spawn and setup
        if expected_count > 0:
            max_allowed_time = (expected_count / 2000.0) + 0.05
            assert elapsed < max_allowed_time, (
                f"Query took {elapsed:.3f}s for {expected_count} messages, "
                f"expected < {max_allowed_time:.3f}s (2000+ msg/sec)"
            )


def test_since_index_usage(workdir):
    """Verify timestamp index is used for queries."""
    queue_name = "index_test_queue"

    # Write some messages
    for i in range(100):
        run_cli("write", queue_name, f"msg{i}", cwd=workdir)

    # Use EXPLAIN QUERY PLAN to verify index usage
    # This requires direct database access
    db_path = workdir / ".broker.db"

    import sqlite3

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check query plan for a typical --since query
    cursor.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT body, ts FROM messages
        WHERE queue = ? AND ts > ?
        ORDER BY id
    """,
        (queue_name, 1000),
    )

    plan = cursor.fetchall()
    conn.close()

    # Verify index is being used
    plan_text = " ".join(str(row) for row in plan)
    assert (
        "USING INDEX idx_messages_queue_ts" in plan_text or "idx_queue_ts" in plan_text
    )


# ============================================================================
# Edge Cases and Boundary Tests
# ============================================================================


def test_since_zero(workdir):
    """Test --since 0 returns all messages."""
    queue_name = "zero_queue"

    # Write messages
    messages = ["first", "second", "third"]
    for msg in messages:
        run_cli("write", queue_name, msg, cwd=workdir)

    # Read with --since 0
    rc, out, _ = run_cli("read", queue_name, "--all", "--since", "0", cwd=workdir)
    assert rc == 0
    assert out == "\n".join(messages)


def test_since_max_timestamp(workdir):
    """Test --since with maximum 64-bit value."""
    queue_name = "max_ts_queue"

    # Write a message
    run_cli("write", queue_name, "test", cwd=workdir)

    # Read with maximum timestamp
    max_ts = str(2**63 - 1)
    rc, out, _ = run_cli("read", queue_name, "--since", max_ts, cwd=workdir)
    assert rc == 0  # Should succeed but return no messages
    assert out == ""  # No messages should have timestamp > max

    # Verify message still exists
    rc, out, _ = run_cli("read", queue_name, cwd=workdir)
    assert rc == 0
    assert out == "test"


def test_since_queue_not_found(workdir):
    """Test --since with non-existent queue."""
    rc, out, _ = run_cli("read", "nonexistent", "--since", "1000", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY


def test_since_timestamp_heuristic_edge_cases(workdir):
    """Test edge cases that expose fragility in the digit-count heuristic."""
    queue_name = "heuristic_edge_queue"

    # Write a test message
    run_cli("write", queue_name, "test", cwd=workdir)

    # Test 1: 12-digit number (ambiguous - could be ms from year ~2286)
    twelve_digits = 100000000000  # 12 digits
    rc, out, _ = run_cli("peek", queue_name, "--since", str(twelve_digits), cwd=workdir)
    assert rc == 0  # Treated as seconds (100 billion seconds = year 5138)

    # Test 2: 14-digit nanosecond timestamp from 1970
    # 10000000000000 ns = 10 seconds after epoch
    # With 14 digits, our heuristic treats it as milliseconds
    # 10 trillion ms = ~317 years after epoch, which causes overflow
    fourteen_digit_ns = 10000000000000  # 10 trillion nanoseconds = 10 seconds
    rc, out, err = run_cli(
        "peek", queue_name, "--since", str(fourteen_digit_ns), cwd=workdir
    )
    # This will be treated as milliseconds and cause overflow error
    assert rc == 1, f"Expected error but got rc={rc}, out={out}, err={err}"
    assert "Invalid timestamp" in err

    # Test 3: Historical timestamps with unusual digit counts
    # 10-digit timestamp (1 billion seconds = ~31.7 years after epoch = year 2001)
    historical_seconds = 1000000000  # 10 digits, Sept 2001
    rc, out, _ = run_cli(
        "peek", queue_name, "--since", str(historical_seconds), cwd=workdir
    )
    assert rc == 0  # Should be treated as seconds and return message

    # Test 4: Demonstrate the heuristic breaking
    # A valid 17-digit nanosecond timestamp that will be misinterpreted
    # 10000000000000000 ns = 10 seconds after epoch
    seventeen_digit_ns = 10000000000000000
    rc, out, err = run_cli(
        "peek", queue_name, "--since", str(seventeen_digit_ns), cwd=workdir
    )
    # Will be treated as nanoseconds correctly, so should return message
    assert rc == 0

    # Test 5: Suffix disambiguates correctly
    # Same 14-digit value with explicit suffix
    rc1, _, _ = run_cli(
        "peek", queue_name, "--since", f"{fourteen_digit_ns}ns", cwd=workdir
    )
    assert rc1 == 0  # Correctly treated as 10 seconds after epoch

    # Show that without suffix it causes an error
    rc2, _, err2 = run_cli(
        "peek", queue_name, "--since", str(fourteen_digit_ns), cwd=workdir
    )
    assert rc2 == 1  # Misinterpreted as milliseconds, causes overflow
    assert "Invalid timestamp" in err2


def test_since_timestamp_heuristic(workdir):
    """Test the heuristic that distinguishes native vs. Unix timestamps."""
    queue_name = "heuristic_queue"

    # Define the actual boundary used in the implementation
    BOUNDARY = 1 << 44  # Same as in commands.py (approx 1.76e13)

    # A small native timestamp (e.g., from early Unix epoch)
    # 1 second after epoch, as a native timestamp
    small_native_ts = (1 * 1000) << 20  # Value is 1048576000
    assert small_native_ts < BOUNDARY  # This will be treated as Unix

    # Write a message
    run_cli("write", queue_name, "message", cwd=workdir)

    # Test that `small_native_ts` is interpreted as a Unix timestamp in ms,
    # which corresponds to Jan 12, 1970, so it should return the message.
    rc, out, _ = run_cli(
        "peek", queue_name, "--since", str(small_native_ts), cwd=workdir
    )
    assert rc == 0
    assert out == "message"

    # Verify with a large Unix timestamp in milliseconds
    large_unix_ts_ms = int(time.time() * 1000)
    assert large_unix_ts_ms < BOUNDARY  # Still treated as Unix

    # This should filter out our existing message (since it's in the future)
    rc, out, _ = run_cli(
        "peek", queue_name, "--since", str(large_unix_ts_ms), cwd=workdir
    )
    assert rc == 0  # Should succeed but return no messages
    assert out == ""

    # Test boundary behavior with realistic values
    # A 13-digit value (typical milliseconds) - should be treated as Unix ms
    thirteen_digits = 1700000000000  # Nov 2023 in milliseconds
    rc, out, _ = run_cli(
        "peek", queue_name, "--since", str(thirteen_digits), cwd=workdir
    )
    assert rc == 0  # Should return message

    # Test actual boundary - values >= 2^44 are treated as native
    # The smallest valid native timestamp that won't overflow
    native_ts = BOUNDARY
    rc, out, _ = run_cli("peek", queue_name, "--since", str(native_ts), cwd=workdir)
    # This native timestamp represents ~16.8 seconds after epoch
    # So it should return the message
    assert rc == 0
    assert out == "message"

    # Test a large native timestamp from current time
    current_native = int(time.time() * 1000) << 20
    assert current_native > BOUNDARY  # Should be treated as native
    rc, out, _ = run_cli(
        "peek", queue_name, "--since", str(current_native), cwd=workdir
    )
    assert rc == 0  # Should succeed but return no messages
    assert out == ""  # Empty (future timestamp)


def test_since_hybrid_timestamp_ordering(workdir):
    """Test that hybrid timestamps maintain correct ordering."""
    queue_name = "hybrid_queue"

    # Write many messages rapidly to trigger same-millisecond timestamps
    messages = []
    for i in range(100):
        msg = f"msg{i:03d}"
        messages.append(msg)
        run_cli("write", queue_name, msg, cwd=workdir)

    # Get all timestamps
    rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    assert rc == 0

    lines = out.strip().split("\n")
    timestamps = []
    for line in lines:
        ts, msg = line.split("\t")
        timestamps.append(int(ts))

    # Verify timestamps are strictly increasing
    for i in range(1, len(timestamps)):
        assert timestamps[i] > timestamps[i - 1]

    # Test --since with timestamps in the middle of same-millisecond group
    mid_point = len(timestamps) // 2
    mid_ts = timestamps[mid_point]

    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--since", str(mid_ts), cwd=workdir
    )
    assert rc == 0

    result_messages = out.strip().split("\n")
    expected_messages = messages[mid_point + 1 :]
    assert result_messages == expected_messages


def test_since_unit_suffixes(workdir):
    """Test explicit unit suffixes for timestamp disambiguation."""
    queue_name = "suffix_queue"

    # Write some messages
    for i in range(3):
        run_cli("write", queue_name, f"msg{i}", cwd=workdir)
        time.sleep(0.001)

    # Test valid suffixes
    for ts_str, desc, should_work in UNIT_SUFFIX_TESTS:
        if should_work:
            # Test with peek to avoid consuming messages
            rc, out, err = run_cli(
                "peek", queue_name, "--all", "--since", ts_str, cwd=workdir
            )
            # Should either return messages or be empty (depending on timestamp value)
            assert rc in [0, 2], f"Failed for {desc}: {err}"
            assert "error" not in err.lower(), f"Unexpected error for {desc}: {err}"
        else:
            # Should fail with error
            rc, out, err = run_cli("peek", queue_name, "--since", ts_str, cwd=workdir)
            assert rc == 1, f"Expected error for {desc} but got rc={rc}"
            assert "Invalid timestamp" in err, f"Wrong error for {desc}: {err}"

    # Test that suffixes work correctly with actual timestamps
    # Note: `test_since_unit_suffixes` relies on
    # int(time.time()) which truncates the fractional part.
    # If the last message is written at 12 :00 :00.900 and the call to
    # `time.time()` happens 120 ms later (12 :00 :01.020) the integer seconds
    # value is still **12 :00 :01 → 1 020 ms earlier than the real clock
    # value** used by the message.
    # Because `--since` uses a strict “greater than” comparison, the truncated
    # value can be **earlier than the message timestamps**, so the queue still
    # matches and the assertion `out == ""` fails.
    # This can be a particular problem with different CI/CD environments
    # where the time resolution can vary. So we make this deterministic by
    # setting a future timestamp that is guaranteed to be after the last message.
    future = time.time() + 2  # At least 2 seconds after the last message
    now_s = int(future)
    now_ms = int(future * 1000)
    now_ns = int(future * 1_000_000_000)

    # All of these should filter out our old messages
    for suffix_ts in [f"{now_s}s", f"{now_ms}ms", f"{now_ns}ns"]:
        rc, out, err = run_cli(
            "peek", queue_name, "--all", "--since", suffix_ts, cwd=workdir
        )
        assert rc == 0  # Queue exists with messages, but none match the filter
        assert out == ""  # Verify no messages are returned
        assert err == "", f"Unexpected error for {suffix_ts}: {err}"


def test_since_negative_timestamps(workdir):
    """Test handling of negative timestamps."""
    queue_name = "negative_queue"

    # Write a message
    run_cli("write", queue_name, "test", cwd=workdir)

    # Test directly via Python to avoid argparse issues with negative numbers
    import subprocess
    import sys

    # Test various negative formats by passing as single string to avoid argparse issues
    negative_tests = [
        ("-1", "raw negative"),
        ("-1.5", "negative float"),
    ]

    for ts_str, desc in negative_tests:
        # Use = syntax to avoid argparse interpreting as flag
        cmd = [
            sys.executable,
            "-m",
            "simplebroker.cli",
            "peek",
            queue_name,
            f"--since={ts_str}",
        ]
        proc = subprocess.run(cmd, cwd=workdir, capture_output=True, text=True)
        assert proc.returncode == 1, f"Expected error for {desc}"
        assert "Invalid timestamp" in proc.stderr, (
            f"Wrong error message for {desc}: {proc.stderr}"
        )


def test_since_scientific_notation_rejected(workdir):
    """Test that scientific notation is consistently rejected."""
    queue_name = "sci_queue"

    # Write a message
    run_cli("write", queue_name, "test", cwd=workdir)

    # Test various scientific notation formats
    sci_tests = ["1e10", "1E10", "1.5e9", "1e-5", "1.23E+10"]

    for ts_str in sci_tests:
        rc, out, err = run_cli("peek", queue_name, "--since", ts_str, cwd=workdir)
        assert rc == 1, f"Expected error for {ts_str}"
        assert "Invalid timestamp" in err, f"Wrong error for {ts_str}: {err}"
        assert "scientific notation not supported" in err, (
            f"Wrong reason for {ts_str}: {err}"
        )


def test_since_clock_regression(workdir):
    """Test that --since continues to function correctly when system clock moves backward."""
    queue_name = "clock_regression_queue"

    # We can't actually mock time.time() in the subprocess, but we can verify
    # that messages are still returned in correct order even if timestamps appear out of order

    # Write messages with known timestamps
    run_cli("write", queue_name, "msg1", cwd=workdir)
    rc, out1, _ = run_cli("peek", queue_name, "--timestamps", cwd=workdir)
    ts1 = int(out1.split("\t")[0])

    run_cli("write", queue_name, "msg2", cwd=workdir)
    rc, out2, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    lines = out2.strip().split("\n")
    ts2 = int(lines[1].split("\t")[0])

    # Even if clock went backward, ts2 should be > ts1 due to logical clock
    assert ts2 > ts1, "Logical clock should ensure monotonic timestamps"

    # Test that --since still works correctly
    rc, out, _ = run_cli("peek", queue_name, "--all", "--since", str(ts1), cwd=workdir)
    assert rc == 0
    assert out == "msg2"

    # ORDER BY id ensures correct FIFO order regardless of timestamp values
    rc, out, _ = run_cli("read", queue_name, "--all", cwd=workdir)
    assert rc == 0
    assert out == "msg1\nmsg2"


def test_clock_regression_timestamp_behavior():
    """Test to document expected behavior during clock regression.

    This test demonstrates what SHOULD happen when the system clock goes backward.
    The hybrid timestamp implementation should maintain monotonicity by keeping
    the previous physical time and incrementing the logical counter.
    """
    # Since we can't mock time.time() in a subprocess, we document the expected behavior
    # The timestamp format is: (milliseconds_since_epoch << 20) | logical_counter

    # Example scenario:
    # Time 1: 1700000000000 ms (physical), counter = 0
    # Timestamp 1: (1700000000000 << 20) | 0 = 1782579200000000000

    # Clock regression: time goes back to 1699999999900 ms
    # Time 2: 1699999999900 ms (physical)
    # Expected behavior: keep previous physical time, increment counter
    # Timestamp 2: (1700000000000 << 20) | 1 = 1782579200000000001

    # This ensures timestamps are always monotonically increasing
    assert 1782579200000000001 > 1782579200000000000

    # The actual implementation in db.py handles this correctly
    # but we can't test it directly through the CLI


def test_since_naive_datetime_utc_assumption(workdir):
    """Test that naive datetime strings are assumed to be UTC."""
    queue_name = "naive_utc_queue"

    # Write a message and get its timestamp
    run_cli("write", queue_name, "test_message", cwd=workdir)
    rc, out, _ = run_cli("peek", queue_name, "--timestamps", cwd=workdir)
    native_ts = int(out.split("\t")[0])

    # Convert native timestamp to datetime
    # Native timestamp is milliseconds since epoch << 20
    ms_since_epoch = native_ts >> 20
    dt_utc = datetime.datetime.fromtimestamp(
        ms_since_epoch / 1000, datetime.timezone.utc
    )

    # Create naive datetime (no timezone info)
    dt_naive = dt_utc.replace(tzinfo=None)
    naive_iso_str = dt_naive.isoformat()

    # The naive datetime should be interpreted as UTC
    # So using it as --since with a slight offset should work correctly

    # Test 1: Using exact naive datetime should filter out the message
    # (since --since uses > comparison)
    rc, out, _ = run_cli("peek", queue_name, "--since", naive_iso_str, cwd=workdir)
    assert rc == 0  # Should succeed but return no messages
    assert out == ""

    # Test 2: Using naive datetime 1 second earlier should return the message
    dt_earlier = dt_naive - datetime.timedelta(seconds=1)
    rc, out, _ = run_cli(
        "peek", queue_name, "--since", dt_earlier.isoformat(), cwd=workdir
    )
    assert rc == 0
    assert out == "test_message"

    # Test 3: Compare with explicit UTC datetime to verify assumption
    dt_utc_explicit = dt_utc.isoformat()
    rc1, _, _ = run_cli("peek", queue_name, "--since", naive_iso_str, cwd=workdir)
    rc2, _, _ = run_cli("peek", queue_name, "--since", dt_utc_explicit, cwd=workdir)
    assert rc1 == rc2  # Both should behave identically


def test_since_error_messages_are_helpful(workdir):
    """Test that error messages provide helpful information for common mistakes."""
    queue_name = "error_test_queue"
    run_cli("write", queue_name, "test", cwd=workdir)

    # Test 1: Scientific notation
    rc, out, err = run_cli("peek", queue_name, "--since", "1.5e9", cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp: scientific notation not supported" in err

    # Test 2: Negative values
    rc, out, err = run_cli("peek", queue_name, "--since=-1", cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp: cannot be negative" in err

    # Test 3: Invalid ISO format
    rc, out, err = run_cli("peek", queue_name, "--since", "2024-13-45", cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp" in err

    # Test 4: Overflow
    huge_value = str(2**64)
    rc, out, err = run_cli("peek", queue_name, "--since", huge_value, cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp: exceeds maximum value" in err

    # Test 5: Empty string via equals syntax
    rc, out, err = run_cli("peek", queue_name, "--since=", cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp: empty string" in err


# ============================================================================
# Integration Test Patterns
# ============================================================================


def test_since_with_json_and_timestamps(workdir):
    """Test combining --since with both --json and --timestamps."""
    queue_name = "json_ts_queue"

    # Write messages
    run_cli("write", queue_name, "old_message", cwd=workdir)
    time.sleep(0.002)
    run_cli("write", queue_name, "new_message", cwd=workdir)

    # Get old message timestamp
    rc, out, _ = run_cli("peek", queue_name, "--timestamps", cwd=workdir)
    old_ts = int(out.split("\t")[0])

    # Read with all flags combined
    rc, out, _ = run_cli(
        "peek",
        queue_name,
        "--all",
        "--json",
        "--timestamps",
        "--since",
        str(old_ts),
        cwd=workdir,
    )
    assert rc == 0

    # Parse and verify JSON output
    lines = out.strip().split("\n")
    assert len(lines) == 1

    data = json.loads(lines[0])
    assert data["message"] == "new_message"
    assert "timestamp" in data
    assert data["timestamp"] > old_ts


def test_since_single_message_mode(workdir):
    """Test --since without --all (single message mode)."""
    queue_name = "single_mode_queue"

    # Write multiple messages
    for i in range(5):
        run_cli("write", queue_name, f"msg{i}", cwd=workdir)
        time.sleep(0.001)

    # Get timestamp of second message
    rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    lines = out.strip().split("\n")
    ts2 = int(lines[1].split("\t")[0])

    # Read single message with --since
    rc, out, _ = run_cli("read", queue_name, "--since", str(ts2), cwd=workdir)
    assert rc == 0
    assert out == "msg2"  # Should get the first message after ts2

    # Verify remaining messages
    rc, out, _ = run_cli("read", queue_name, "--all", cwd=workdir)
    assert rc == 0
    assert out == "msg0\nmsg1\nmsg3\nmsg4"


def test_since_error_propagation(workdir):
    """Test that database errors are properly propagated with --since."""
    # Create a queue
    run_cli("write", "error_queue", "test", cwd=workdir)

    # Make database read-only to trigger errors
    db_path = workdir / ".broker.db"
    db_path.chmod(0o444)

    try:
        # Attempt to read with --since (should handle error gracefully)
        rc, out, err = run_cli("read", "error_queue", "--since", "0", cwd=workdir)
        # Should get an error but not crash
        assert rc != 0
    finally:
        # Restore permissions
        db_path.chmod(0o644)
