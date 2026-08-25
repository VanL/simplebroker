"""Tests for the broker move CLI command."""

import datetime
import json
import threading
import time
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from simplebroker import Queue, commands, target_for_directory

from .conftest import run_cli
from .helper_scripts.timestamp_validation import validate_timestamp


def _seed(workdir: Path, queue: str, *messages: str) -> None:
    """Seed setup messages through the Queue API (no subprocess spawn).

    Timestamps are strictly monotonic per database, so no pacing sleeps
    are needed between writes.
    """
    with Queue(queue, db_path=target_for_directory(workdir), persistent=True) as q:
        for message in messages:
            q.write(message)



def _peek_json(cwd, queue: str) -> list[dict[str, object]]:
    rc, out, err = run_cli("peek", queue, "--all", "--json", cwd=cwd)
    assert rc == 0, err
    return [json.loads(line) for line in out.strip().splitlines()]


class TestBasicFunctionality:
    """Test basic move operations."""

    def test_single_message_move_default(self, workdir):
        """Test default behavior: move single oldest message."""
        # Setup: write messages to source queue
        _seed(workdir, "source", "first message")
        _seed(workdir, "source", "second message")
        # Move one message (default behavior)
        rc, out, _ = run_cli("move", "source", "dest", cwd=workdir)
        assert rc == 0
        assert out == "first message"

        # Verify source has one message left
        rc, out, _ = run_cli("read", "source", cwd=workdir)
        assert rc == 0
        assert out == "second message"

        # Verify dest has the moved message
        rc, out, _ = run_cli("read", "dest", cwd=workdir)
        assert rc == 0
        assert out == "first message"

    def test_specific_message_move_by_timestamp(self, workdir):
        """Test moving a specific message by timestamp with -m flag."""
        # Write messages and capture timestamps
        _seed(workdir, "source", "msg1")
        _seed(workdir, "source", "msg2")
        _seed(workdir, "source", "msg3")
        # FIXED: Use peek instead of read to get timestamps without consuming
        rc, out, _ = run_cli("peek", "source", "--all", "--json", cwd=workdir)
        assert rc == 0
        messages = [json.loads(line) for line in out.strip().split("\n")]
        assert len(messages) == 3
        msg2_ts = messages[1]["timestamp"]

        # Move specific message by timestamp
        rc, out, _ = run_cli("move", "source", "dest", "-m", str(msg2_ts), cwd=workdir)
        assert rc == 0
        assert out == "msg2"

        # Verify source still has msg1 and msg3
        rc, out, _ = run_cli("read", "source", "--all", cwd=workdir)
        assert rc == 0
        assert out == "msg1\nmsg3"

        # Verify dest has msg2
        rc, out, _ = run_cli("read", "dest", cwd=workdir)
        assert rc == 0
        assert out == "msg2"

    def test_bulk_move_with_all_flag(self, workdir):
        """Test bulk move with --all flag."""
        # Write multiple messages
        messages = ["msg1", "msg2", "msg3", "msg4"]
        for msg in messages:
            _seed(workdir, "source", msg)
        # Move all messages
        rc, out, _ = run_cli("move", "source", "dest", "--all", cwd=workdir)
        assert rc == 0
        assert out == "\n".join(messages)

        # Verify source is empty
        rc, out, _ = run_cli("read", "source", cwd=workdir)
        assert rc == 2  # EXIT_QUEUE_EMPTY

        # Verify dest has all messages in order
        rc, out, _ = run_cli("read", "dest", "--all", cwd=workdir)
        assert rc == 0
        assert out == "\n".join(messages)

    def test_filtered_move_with_after_flag(self, workdir):
        """Test filtered moves with --after flag."""
        # Write messages with known timestamps
        _seed(workdir, "source", "old1")
        _seed(workdir, "source", "old2")
        # FIXED: Use peek to get timestamp without consuming
        rc, out, _ = run_cli("peek", "source", "--all", "--json", cwd=workdir)
        assert rc == 0
        messages = [json.loads(line) for line in out.strip().split("\n")]
        cutoff_ts = messages[1]["timestamp"]

        # Write newer messages
        _seed(workdir, "source", "new1")
        _seed(workdir, "source", "new2")
        # Move single message with --after (without --all)
        rc, out, _ = run_cli(
            "move", "source", "dest", "--after", str(cutoff_ts), cwd=workdir
        )
        assert rc == 0
        assert out == "new1"

        # Move all remaining messages newer than cutoff
        rc, out, _ = run_cli(
            "move", "source", "dest", "--all", "--after", str(cutoff_ts), cwd=workdir
        )
        assert rc == 0
        assert out == "new2"

        # Verify only old messages remain in source
        rc, out, _ = run_cli("read", "source", "--all", cwd=workdir)
        assert rc == 0
        assert out == "old1\nold2"

    def test_single_move_with_before_flag(self, workdir):
        """Test moving one message older than a timestamp."""
        _seed(workdir, "source", "old1")
        _seed(workdir, "source", "old2")
        _seed(workdir, "source", "new1")
        messages = _peek_json(workdir, "source")
        new1_ts = messages[2]["timestamp"]
        _seed(workdir, "source", "new2")
        rc, out, err = run_cli(
            "move", "source", "dest", "--before", str(new1_ts), cwd=workdir
        )
        assert rc == 0, err
        assert out == "old1"

        rc, out, err = run_cli("peek", "source", "--all", cwd=workdir)
        assert rc == 0, err
        assert out == "old2\nnew1\nnew2"

        rc, out, err = run_cli("peek", "dest", "--all", cwd=workdir)
        assert rc == 0, err
        assert out == "old1"

    def test_bulk_move_with_before_flag(self, workdir):
        """Test moving all messages older than a timestamp."""
        _seed(workdir, "source", "old1")
        _seed(workdir, "source", "old2")
        _seed(workdir, "source", "new1")
        new1_ts = _peek_json(workdir, "source")[2]["timestamp"]

        rc, out, err = run_cli(
            "move", "source", "dest", "--all", "--before", str(new1_ts), cwd=workdir
        )
        assert rc == 0, err
        assert out == "old1\nold2"

        rc, out, err = run_cli("peek", "source", "--all", cwd=workdir)
        assert rc == 0, err
        assert out == "new1"

        rc, out, err = run_cli("peek", "dest", "--all", cwd=workdir)
        assert rc == 0, err
        assert out == "old1\nold2"

    def test_move_before_exact_boundary_is_exclusive(self, workdir):
        """Test strict < comparison for move --before."""
        _seed(workdir, "source", "message")
        ts = _peek_json(workdir, "source")[0]["timestamp"]

        rc, out, err = run_cli(
            "move", "source", "dest", "--before", str(ts), cwd=workdir
        )
        assert rc == 2, err
        assert out == ""

        rc, out, err = run_cli("peek", "source", cwd=workdir)
        assert rc == 0, err
        assert out == "message"

        rc, out, err = run_cli(
            "move", "source", "dest", "--before", str(int(str(ts)) + 1), cwd=workdir
        )
        assert rc == 0, err
        assert out == "message"

    def test_move_after_and_before_form_open_interval(self, workdir):
        """Test moving messages inside an open timestamp interval."""
        for message in ("m1", "m2", "m3"):
            _seed(workdir, "source", message)
        messages = _peek_json(workdir, "source")
        ts1 = messages[0]["timestamp"]
        ts3 = messages[2]["timestamp"]

        rc, out, err = run_cli(
            "move",
            "source",
            "dest",
            "--all",
            "--after",
            str(ts1),
            "--before",
            str(ts3),
            cwd=workdir,
        )
        assert rc == 0, err
        assert out == "m2"

        rc, out, err = run_cli("peek", "source", "--all", cwd=workdir)
        assert rc == 0, err
        assert out == "m1\nm3"

    def test_message_move_cannot_be_combined_with_before(self, workdir):
        """Test --message and --before are mutually exclusive for move."""
        _seed(workdir, "source", "message")
        rc, _out, err = run_cli(
            "move",
            "source",
            "dest",
            "--message",
            "1234567890123456789",
            "--before",
            "1234567890123456790",
            cwd=workdir,
        )

        assert rc == 1
        assert "--before" in err

    def test_non_existent_destination_created(self, workdir):
        """Test that non-existent destination queue is created implicitly."""
        # Write to source
        _seed(workdir, "source", "test message")
        # Move to non-existent destination
        rc, out, _ = run_cli("move", "source", "new_dest", cwd=workdir)
        assert rc == 0
        assert out == "test message"

        # Verify destination was created and has the message
        rc, out, _ = run_cli("read", "new_dest", cwd=workdir)
        assert rc == 0
        assert out == "test message"


def test_filtered_single_move_closes_bounded_generator(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """Moving one filtered item must close the transactional generator."""

    closed: list[bool] = []
    retained_iterators: list[Iterator[tuple[str, int]]] = []

    def tracked_results() -> Iterator[tuple[str, int]]:
        try:
            yield "message", 123
        finally:
            closed.append(True)

    def move_generator(
        self: Queue,
        destination: str,
        **kwargs: Any,
    ) -> Iterator[tuple[str, int]]:
        del self, destination, kwargs
        iterator = tracked_results()
        retained_iterators.append(iterator)
        return iterator

    monkeypatch.setattr(Queue, "move_generator", move_generator)

    result = commands.cmd_move(
        str(tmp_path / "move.db"),
        "source",
        "destination",
        after_str="0",
    )

    assert result == commands.EXIT_SUCCESS
    assert capsys.readouterr().out == "message\n"
    assert retained_iterators
    assert closed == [True]


class TestTimestampFormats:
    """Test various timestamp formats for --after flag."""

    def test_after_unix_timestamp_formats(self, workdir):
        """Test Unix timestamp formats with explicit suffixes."""
        # Write messages
        for i in range(5):
            _seed(workdir, "ts_queue", f"msg{i}")

        # Get a middle timestamp
        rc, out, _ = run_cli("peek", "ts_queue", "--all", "--json", cwd=workdir)
        messages = [json.loads(line) for line in out.strip().split("\n")]
        middle_ts = int(messages[2]["timestamp"])

        # Convert native timestamp to different formats
        # Timestamps are now: microseconds << 12
        us_after_epoch = middle_ts >> 12
        unix_seconds = us_after_epoch // 1_000_000
        unix_millis = us_after_epoch // 1_000
        unix_nanos = us_after_epoch * 1_000

        # Test with explicit suffixes
        test_cases = [
            (f"{unix_seconds}s", "Unix seconds"),
            (f"{unix_millis}ms", "Unix milliseconds"),
            (f"{unix_nanos}ns", "Unix nanoseconds"),
        ]

        for ts_str, desc in test_cases:
            rc, out, err = run_cli(
                "peek", "ts_queue", "--all", "--after", ts_str, cwd=workdir
            )
            assert rc == 0, f"Failed for {desc}: {err}"
            messages = out.strip().split("\n")
            # Should get messages after the middle one
            assert len(messages) >= 2, f"Expected messages for {desc}, got: {out}"

    def test_after_iso_date_formats(self, workdir):
        """Test ISO 8601 date and datetime formats."""
        # Write a message
        _seed(workdir, "iso_queue", "test message")
        # Test date formats
        today = datetime.datetime.now(datetime.UTC)
        yesterday = today - datetime.timedelta(days=1)
        tomorrow = today + datetime.timedelta(days=1)

        # Yesterday should return the message
        rc, out, _ = run_cli(
            "move",
            "iso_queue",
            "dest1",
            "--after",
            yesterday.strftime("%Y-%m-%d"),
            cwd=workdir,
        )
        assert rc == 0
        assert out == "test message"

        # Write another for tomorrow test
        _seed(workdir, "iso_queue", "another message")
        # Tomorrow should return nothing
        rc, out, _ = run_cli(
            "move",
            "iso_queue",
            "dest2",
            "--after",
            tomorrow.strftime("%Y-%m-%d"),
            cwd=workdir,
        )
        assert rc == 2  # EXIT_QUEUE_EMPTY when no messages match filter
        assert out == ""

    def test_after_mixed_timestamp_formats(self, workdir):
        """Test that different timestamp formats work correctly."""
        # Write messages
        for i in range(10):
            _seed(workdir, "mixed_queue", f"msg{i}")

        # Get message 5 timestamp
        rc, out, _ = run_cli("peek", "mixed_queue", "--all", "--json", cwd=workdir)
        messages = [json.loads(line) for line in out.strip().split("\n")]
        native_ts = int(messages[5]["timestamp"])

        # Convert to different formats
        us_after_epoch = native_ts >> 12
        unix_seconds = us_after_epoch // 1_000_000
        dt = datetime.datetime.fromtimestamp(unix_seconds, datetime.UTC)

        # Test each format
        formats = [
            (str(native_ts), "native", 4),  # Should get exactly msg6-msg9
            (dt.isoformat(), "iso", None),  # May get more due to second precision
            (str(unix_seconds), "unix", None),  # May get more due to second precision
        ]

        for fmt, name, expected_count in formats:
            # Create unique destination for each test
            dest = f"dest_{name}"
            rc, out, _ = run_cli(
                "move", "mixed_queue", dest, "--all", "--after", fmt, cwd=workdir
            )
            assert rc == 0

            messages = out.strip().split("\n")
            if expected_count:
                assert len(messages) == expected_count, (
                    f"{name} format returned {len(messages)} messages"
                )
            else:
                # For less precise formats, just verify we got some messages
                assert len(messages) >= 1, f"{name} format returned no messages"

            # Move messages back for next test
            run_cli("move", dest, "mixed_queue", "--all", cwd=workdir)


class TestErrorCases:
    """Test error handling and edge cases."""

    def test_empty_queue_returns_exit_code_2(self, workdir):
        """Test that moving from empty queue returns exit code 2."""
        # Try to move from non-existent queue
        rc, _out, _ = run_cli("move", "empty", "dest", cwd=workdir)
        assert rc == 2

        # Try bulk move from empty queue
        rc, _out, _ = run_cli("move", "empty", "dest", "--all", cwd=workdir)
        assert rc == 2

    def test_message_not_found_returns_exit_code_2(self, workdir):
        """Test that moving non-existent message returns exit code 2."""
        _seed(workdir, "source", "msg1")
        # Try to move with non-existent timestamp
        fake_ts = "1234567890123456789"
        rc, _out, _ = run_cli("move", "source", "dest", "-m", fake_ts, cwd=workdir)
        assert rc == 2

    def test_already_claimed_message_can_be_moved_by_id(self, workdir):
        """Moving a claimed message by ID preserves its claimed state."""
        # Write a message
        _seed(workdir, "source", "msg1")
        # Use peek to get the ID, then read to claim the row.
        rc, out, _ = run_cli("peek", "source", "--json", cwd=workdir)
        assert rc == 0
        msg_data = json.loads(out)
        msg_ts = msg_data["timestamp"]

        # Now claim it with read
        rc, out, _ = run_cli("read", "source", cwd=workdir)
        assert rc == 0
        assert out == "msg1"

        # Exact-ID selection includes claimed rows without releasing the claim.
        rc, out, _ = run_cli("move", "source", "dest", "-m", str(msg_ts), cwd=workdir)
        assert rc == 0
        assert out == "msg1"

        rc, out, _ = run_cli("peek", "dest", cwd=workdir)
        assert rc == 2
        assert out == ""

        rc, out, _ = run_cli("peek", "dest", "--include-claimed", cwd=workdir)
        assert rc == 0
        assert out == "msg1"

    def test_invalid_timestamp_format_returns_exit_code_1(self, workdir):
        """Test that invalid timestamp format returns exit code 1."""
        _seed(workdir, "source", "msg1")
        # Test various invalid timestamp formats
        invalid_timestamps = [
            "123",  # Too short
            "12345678901234567890",  # Too long
            "abcd1234567890123456",  # Non-numeric
            "-1234567890123456789",  # Negative
            "12.34567890123456789",  # Decimal
            "abcdefghijk12345678",  # 19 chars but non-numeric
        ]

        for invalid_ts in invalid_timestamps:
            rc, _out, _err = run_cli(
                "move", "source", "dest", "-m", invalid_ts, cwd=workdir
            )
            assert rc == 1, f"Expected exit code 1 for timestamp {invalid_ts}"

    def test_same_source_dest_queue_returns_exit_code_1(self, workdir):
        """Test that same source and destination returns exit code 1."""
        _seed(workdir, "myqueue", "msg1")
        # Try to move to same queue
        rc, out, err = run_cli("move", "myqueue", "myqueue", cwd=workdir)
        assert rc == 1
        assert (
            "Source and destination queues cannot be the same" in err
            or "Source and destination queues cannot be the same" in out
        )

    def test_after_no_matches_returns_exit_code_2(self, workdir):
        """Test that --after with no matches returns exit code 2."""
        # Write old messages
        _seed(workdir, "source", "old1")
        _seed(workdir, "source", "old2")
        # Use a future timestamp
        future_ts = int(time.time() * 1_000_000) << 12
        future_ts += 1000000000  # Add some buffer

        # Move with --after future timestamp (without --all)
        rc, out, _ = run_cli(
            "move", "source", "dest", "--after", str(future_ts), cwd=workdir
        )
        assert rc == 2  # EXIT_QUEUE_EMPTY when no messages match
        assert out == ""

        # Move with --all --after future timestamp
        rc, out, _ = run_cli(
            "move", "source", "dest", "--all", "--after", str(future_ts), cwd=workdir
        )
        assert rc == 2  # EXIT_QUEUE_EMPTY when no messages match
        assert out == ""

    def test_after_exact_boundary(self, workdir):
        """Test strict > comparison (not >=) for --after."""
        # Write a message and get its timestamp
        _seed(workdir, "boundary_queue", "test_message")
        rc, out, _ = run_cli("peek", "boundary_queue", "--json", cwd=workdir)
        assert rc == 0
        ts = int(json.loads(out)["timestamp"])

        # Move with --after equal to the message timestamp -> expect empty
        rc, out, _ = run_cli(
            "move", "boundary_queue", "dest", "--after", str(ts), cwd=workdir
        )
        assert rc == 2  # EXIT_QUEUE_EMPTY when no messages match (strict > comparison)
        assert out == ""

        # Move with --after one less than timestamp -> expect message
        rc, out, _ = run_cli(
            "move", "boundary_queue", "dest", "--after", str(ts - 1), cwd=workdir
        )
        assert rc == 0
        assert out == "test_message"


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_valid_special_queue_names(self, workdir):
        """Test move with valid queue names containing special characters."""
        valid_queues = [
            "queue-with-dash",
            "queue_with_underscore",
            "queue.with.dots",
            "queue123numeric",
            "_underscore_start",
            "123numeric_start",
        ]

        for queue in valid_queues:
            _seed(workdir, queue, f"msg in {queue}")
            dest = f"{queue}-dest"
            rc, out, _ = run_cli("move", queue, dest, cwd=workdir)
            assert rc == 0, f"Move failed for valid queue name: {queue}"
            assert out == f"msg in {queue}"

            rc, out, _ = run_cli("read", dest, cwd=workdir)
            assert rc == 0
            assert out == f"msg in {queue}"

    def test_invalid_queue_names_are_rejected(self, workdir):
        """Test that move rejects invalid queue names."""
        # Set up a valid source with a message
        _seed(workdir, "valid_source", "test message")
        # Test queue names that are clearly invalid and won't cause argparse issues
        invalid_queues = [
            "queue:with:colons",
            "queue@with@at",
            "queue!with!exclamation",
            "queue#with#hash",
            "queue$with$dollar",
            "queue%with%percent",
            "queue^with^caret",
            "queue&with&ampersand",
            "queue*with*asterisk",
            "queue+with+plus",
            "queue=with=equals",
            "queue{with}braces",
            "queue[with]brackets",
            "queue|with|pipe",
            "queue?with?question",
            "queue<with>angles",
            "queue;with;semicolon",
        ]

        for queue in invalid_queues:
            # Test as source
            rc, out, err = run_cli("move", queue, "dest", cwd=workdir)
            assert rc == 1, f"Move should fail for invalid source queue: {queue}"
            # Check for the actual error message format
            combined_output = (err + out).lower()
            assert "invalid queue name" in combined_output, (
                f"Expected 'invalid queue name' error for {queue}, got: {combined_output}"
            )

            # Test as destination
            rc, out, err = run_cli("move", "valid_source", queue, cwd=workdir)
            assert rc == 1, f"Move should fail for invalid destination queue: {queue}"
            # Check for the actual error message format
            combined_output = (err + out).lower()
            assert "invalid queue name" in combined_output, (
                f"Expected 'invalid queue name' error for {queue}, got: {combined_output}"
            )

    def test_large_messages(self, workdir):
        """Test moving large messages."""
        # Create a large message (1MB)
        large_msg = "x" * (1024 * 1024)
        # Use stdin to avoid command line length limits
        rc, out, _ = run_cli("write", "source", "-", cwd=workdir, stdin=large_msg)
        assert rc == 0

        # Move it
        rc, out, _ = run_cli("move", "source", "dest", cwd=workdir)
        assert rc == 0
        assert out == large_msg

        # Verify in destination
        rc, out, _ = run_cli("read", "dest", cwd=workdir)
        assert rc == 0
        assert out == large_msg

    def test_messages_with_newlines_and_tabs(self, workdir):
        """Test moving messages with newlines and tabs."""
        # Message with newlines and tabs
        complex_msg = "line1\nline2\ttabbed\nline3"
        _seed(workdir, "source", complex_msg)
        # Move it
        rc, out, _ = run_cli("move", "source", "dest", cwd=workdir)
        assert rc == 0
        assert out == complex_msg

        # Verify in destination
        rc, out, _ = run_cli("read", "dest", cwd=workdir)
        assert rc == 0
        assert out == complex_msg

    def test_zero_byte_message(self, workdir):
        """Test moving an empty string message."""
        # Write empty message
        _seed(workdir, "source", "")
        # Move it
        rc, out, _ = run_cli("move", "source", "dest", cwd=workdir)
        assert rc == 0
        assert out == ""

        # Verify it was moved (source should be empty)
        rc, out, _ = run_cli("read", "source", cwd=workdir)
        assert rc == 2  # Empty queue

    def test_move_preserves_timestamps(self, workdir):
        """Test that move preserves original timestamps."""
        # Write messages and capture timestamps
        _seed(workdir, "source", "msg1")
        _seed(workdir, "source", "msg2")
        # FIXED: Use peek to get original timestamps
        rc, out, _ = run_cli("peek", "source", "--all", "--json", cwd=workdir)
        assert rc == 0
        original_msgs = [json.loads(line) for line in out.strip().split("\n")]
        original_timestamps = {
            msg["message"]: msg["timestamp"] for msg in original_msgs
        }

        # Validate timestamps
        for msg in original_msgs:
            validate_timestamp(msg["timestamp"])

        # Move all messages
        rc, out, _ = run_cli("move", "source", "dest", "--all", cwd=workdir)
        assert rc == 0

        # Check timestamps in destination
        rc, out, _ = run_cli("peek", "dest", "--all", "--json", cwd=workdir)
        assert rc == 0
        moved_msgs = [json.loads(line) for line in out.strip().split("\n")]
        moved_timestamps = {msg["message"]: msg["timestamp"] for msg in moved_msgs}

        # Verify timestamps are preserved
        assert original_timestamps == moved_timestamps

    def test_timestamp_preservation_with_specific_move(self, workdir):
        """Test that timestamp is preserved when moving by ID (-m)."""
        # Write a message
        _seed(workdir, "source", "test message")
        # Get its timestamp
        rc, out, _ = run_cli("peek", "source", "--json", cwd=workdir)
        original_data = json.loads(out)
        original_ts = original_data["timestamp"]

        # Move by specific ID
        rc, out, _ = run_cli(
            "move", "source", "dest", "-m", str(original_ts), cwd=workdir
        )
        assert rc == 0

        # Verify timestamp preserved in destination
        rc, out, _ = run_cli("peek", "dest", "--json", cwd=workdir)
        moved_data = json.loads(out)
        assert moved_data["timestamp"] == original_ts

    def test_security_queue_names(self, workdir):
        """Test queue names that could be security issues."""
        # These should be rejected by queue name validation
        dangerous_names = [
            "'; DROP TABLE messages; --",
            "../../../etc/passwd",
            "${HOME}",
            "$(whoami)",
            "`ls -la`",
        ]

        for name in dangerous_names:
            rc, _out, _err = run_cli("move", name, "dest", cwd=workdir)
            assert rc == 1, f"Should reject dangerous queue name: {name!r}"


class TestOutputFormats:
    """Test different output formats."""

    def test_json_output_single_message(self, workdir):
        """Test JSON output format for single message move."""
        _seed(workdir, "source", "test message")
        # Move with JSON output
        rc, out, _ = run_cli("move", "source", "dest", "--json", cwd=workdir)
        assert rc == 0

        # Verify JSON format
        data = json.loads(out)
        assert data["message"] == "test message"
        assert "timestamp" in data
        validate_timestamp(data["timestamp"])

    def test_json_output_multiple_messages(self, workdir):
        """Test JSON output format for bulk move."""
        messages = ["msg1", "msg2", "msg3"]
        for msg in messages:
            _seed(workdir, "source", msg)
        # Move all with JSON output
        rc, out, _ = run_cli("move", "source", "dest", "--all", "--json", cwd=workdir)
        assert rc == 0

        # Verify line-delimited JSON (ndjson)
        lines = out.strip().split("\n")
        assert len(lines) == 3

        parsed_messages = []
        for line in lines:
            data = json.loads(line)
            parsed_messages.append(data["message"])
            assert "timestamp" in data
            validate_timestamp(data["timestamp"])

        assert parsed_messages == messages

    def test_timestamp_output_format(self, workdir):
        """Test timestamp output format with -t flag."""
        _seed(workdir, "source", "test message")
        # Move with timestamp output
        rc, out, _ = run_cli("move", "source", "dest", "-t", cwd=workdir)
        assert rc == 0

        # Verify format: <timestamp>\t<message>
        parts = out.split("\t", 1)
        assert len(parts) == 2
        ts_str, msg = parts
        assert msg == "test message"
        assert len(ts_str) == 19
        assert ts_str.isdigit()

    def test_json_with_timestamp_flag_is_noop(self, workdir):
        """Test that -t flag with --json is a no-op (timestamps always in JSON)."""
        _seed(workdir, "source", "test message")
        # Move with both --json and -t
        rc1, out1, _ = run_cli("move", "source", "dest", "--json", "-t", cwd=workdir)

        # Write another message and move with just --json
        _seed(workdir, "source", "test message")
        rc2, out2, _ = run_cli("move", "source", "dest2", "--json", cwd=workdir)

        # Both should have same format (timestamp always included in JSON)
        assert rc1 == 0 and rc2 == 0
        data1 = json.loads(out1)
        data2 = json.loads(out2)
        assert "timestamp" in data1 and "timestamp" in data2
        assert data1["message"] == data2["message"]

    def test_output_order_preservation(self, workdir):
        """Test that output preserves FIFO order."""
        # Write messages in specific order
        messages = [f"msg{i}" for i in range(10)]
        for msg in messages:
            _seed(workdir, "source", msg)

        # Move all and verify order
        rc, out, _ = run_cli("move", "source", "dest", "--all", cwd=workdir)
        assert rc == 0
        assert out == "\n".join(messages)

        # Verify order preserved in destination
        rc, out, _ = run_cli("read", "dest", "--all", cwd=workdir)
        assert rc == 0
        assert out == "\n".join(messages)


class TestConcurrentOperations:
    """Test concurrent move operations."""

    def test_concurrent_moves_no_duplication(self, workdir):
        """Test that concurrent moves don't duplicate or lose messages."""
        # Write many messages
        num_messages = 100
        for i in range(num_messages):
            _seed(workdir, "source", f"msg{i:03d}")
        # Use barrier for synchronized start
        num_workers = 5
        barrier = threading.Barrier(num_workers)
        results: list[list[str]] = [[] for _ in range(num_workers)]

        def move_worker(worker_id: int):
            """Worker that moves messages one by one."""
            barrier.wait()  # Synchronize start
            moved = []
            while True:
                rc, out, _ = run_cli("move", "source", f"dest{worker_id}", cwd=workdir)
                if rc == 2:  # Queue empty
                    break
                if rc == 0:
                    moved.append(out)
            results[worker_id] = moved

        # Run concurrent workers
        threads = []
        for i in range(num_workers):
            t = threading.Thread(target=move_worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all workers
        for t in threads:
            t.join()

        # Collect all moved messages
        all_moved = []
        for worker_results in results:
            all_moved.extend(worker_results)

        # Verify no duplication and no loss
        assert len(all_moved) == num_messages
        assert len(set(all_moved)) == num_messages  # No duplicates
        assert set(all_moved) == {f"msg{i:03d}" for i in range(num_messages)}

    def test_concurrent_bulk_moves_same_source(self, workdir):
        """Test concurrent bulk moves from the same source queue."""
        # Write messages
        num_messages = 100
        for i in range(num_messages):
            _seed(workdir, "source", f"msg{i:03d}")
        # Multiple threads try to move all from same source
        num_workers = 3
        barrier = threading.Barrier(num_workers)
        results = []

        def bulk_move_worker(worker_id):
            barrier.wait()  # Synchronize start
            rc, out, _ = run_cli(
                "move", "source", f"dest{worker_id}", "--all", cwd=workdir
            )
            return (worker_id, rc, out)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(bulk_move_worker, i) for i in range(num_workers)]
            results = [f.result() for f in futures]

        # Exactly one worker should succeed, others should get empty queue
        success_count = sum(1 for _, rc, _ in results if rc == 0 and _.strip())
        empty_count = sum(
            1 for _, rc, _ in results if rc == 2 or (rc == 0 and not _.strip())
        )

        assert success_count == 1, (
            "Exactly one worker should successfully move messages"
        )
        assert empty_count == num_workers - 1, "Other workers should find empty queue"

        # Verify all messages were moved by the successful worker
        for _worker_id, rc, out in results:
            if rc == 0 and out.strip():
                messages = out.strip().split("\n")
                assert len(messages) == num_messages
                assert set(messages) == {f"msg{i:03d}" for i in range(num_messages)}

    def test_concurrent_specific_message_moves(self, workdir):
        """Test multiple workers trying to move the same specific message."""
        # Write a message and get its timestamp
        _seed(workdir, "source", "target_message")
        rc, out, err = run_cli("peek", "source", "--json", cwd=workdir)
        assert rc == 0, err
        msg_ts = json.loads(out)["timestamp"]

        # Multiple workers try to move the same message
        num_workers = 5
        barrier = threading.Barrier(num_workers)
        results = []

        def specific_move_worker(worker_id):
            barrier.wait()  # Synchronize start
            rc, out, _err = run_cli(
                "move", "source", f"dest{worker_id}", "-m", str(msg_ts), cwd=workdir
            )
            return (worker_id, rc, out)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(specific_move_worker, i) for i in range(num_workers)
            ]
            results = [f.result() for f in futures]

        # Exactly one should succeed, others should get "not found"
        success_count = sum(
            1 for _, rc, out in results if rc == 0 and out == "target_message"
        )
        not_found_count = sum(1 for _, rc, _ in results if rc == 2)

        assert success_count == 1, (
            "Exactly one worker should successfully move the message"
        )
        assert not_found_count == num_workers - 1, (
            "Other workers should get 'not found'"
        )


class TestMutualExclusivity:
    """Test mutual exclusivity of command options."""

    def test_message_and_all_are_mutually_exclusive(self, workdir):
        """Test that -m and --all cannot be used together."""
        _seed(workdir, "source", "msg")
        # Try to use both -m and --all
        rc, _out, err = run_cli(
            "move", "source", "dest", "--all", "-m", "not-a-message-id", cwd=workdir
        )
        assert rc == 1
        assert "not allowed with argument --all" in err
        assert "invalid message ID" not in err

    def test_after_works_with_and_without_all(self, workdir):
        """Test that --after can be used with or without --all."""
        # Write messages
        for i in range(5):
            _seed(workdir, "source", f"msg{i}")

        # FIXED: Use peek to get timestamp
        rc, out, _ = run_cli("peek", "source", "--all", "--json", cwd=workdir)
        assert rc == 0
        messages = [json.loads(line) for line in out.strip().split("\n")]
        middle_ts = messages[2]["timestamp"]

        # Test --after without --all (should work)
        rc, out, _ = run_cli(
            "move", "source", "dest1", "--after", str(middle_ts), cwd=workdir
        )
        assert rc == 0
        assert out == "msg3"  # First message after middle_ts

        # Test --after with --all (should work)
        rc, out, _ = run_cli(
            "move", "source", "dest2", "--all", "--after", str(middle_ts), cwd=workdir
        )
        assert rc == 0
        assert out == "msg4"  # Remaining message after middle_ts


class TestIntegrationScenarios:
    """Test real-world integration scenarios."""

    def test_error_queue_pattern_with_move(self, workdir):
        """Test work queue pattern with error handling using move command."""
        work_items = ["task1", "task2", "bad_task", "task3"]

        for item in work_items:
            _seed(workdir, "work_queue", item)
        processed = []
        while True:
            # Peek at the next message without consuming it
            rc, out, _ = run_cli("peek", "work_queue", "--json", cwd=workdir)
            if rc == 2:  # Empty
                break

            message_data = json.loads(out)
            message_content = message_data["message"]
            message_id = message_data["timestamp"]

            if "bad" in message_content:
                # Move the specific bad task to the error queue
                rc_move, out_move, _ = run_cli(
                    "move",
                    "work_queue",
                    "error_queue",
                    "-m",
                    str(message_id),
                    cwd=workdir,
                )
                assert rc_move == 0
                assert out_move == "bad_task"
            else:
                # Good task: process it by reading (consuming)
                rc_read, out_read, _ = run_cli("read", "work_queue", cwd=workdir)
                assert rc_read == 0
                processed.append(out_read)

        assert processed == ["task1", "task2", "task3"]

        # Check error queue
        rc, out, _ = run_cli("read", "error_queue", cwd=workdir)
        assert rc == 0
        assert out == "bad_task"

    def test_queue_reorganization(self, workdir):
        """Test reorganizing messages between queues."""
        # Create messages in multiple queues
        queues = {
            "urgent": ["urgent1", "urgent2"],
            "normal": ["normal1", "normal2", "normal3"],
            "low": ["low1", "low2", "low3", "low4"],
        }

        for queue, messages in queues.items():
            for msg in messages:
                _seed(workdir, queue, msg)

        # Reorganize: move all to a single priority queue
        for queue in ["urgent", "normal", "low"]:  # Order matters for priority
            rc, out, _ = run_cli("move", queue, "unified", "--all", cwd=workdir)
            assert rc == 0

        # Verify unified queue has all messages
        rc, out, _ = run_cli("read", "unified", "--all", cwd=workdir)
        assert rc == 0
        all_messages = out.strip().split("\n")
        assert len(all_messages) == 9

        # Verify order (urgent first, then normal, then low)
        assert all_messages[:2] == ["urgent1", "urgent2"]
        assert all_messages[2:5] == ["normal1", "normal2", "normal3"]
        assert all_messages[5:] == ["low1", "low2", "low3", "low4"]

    def test_move_with_destination_already_populated(self, workdir):
        """Test moving to a destination that already has messages."""
        # Set up destination with existing messages
        _seed(workdir, "dest", "existing1")
        _seed(workdir, "dest", "existing2")
        # Set up source with new messages
        _seed(workdir, "source", "new1")
        _seed(workdir, "source", "new2")
        # Move all from source to dest
        rc, out, _ = run_cli("move", "source", "dest", "--all", cwd=workdir)
        assert rc == 0
        assert out == "new1\nnew2"

        rc, out, _ = run_cli("peek", "source", "--all", cwd=workdir)
        assert rc == 2
        assert out == ""

        # Destination preserves every pre-existing and moved body. Cross-backend
        # ordering is not part of this contract.
        rc, out, _ = run_cli("read", "dest", "--all", cwd=workdir)
        assert rc == 0
        assert Counter(out.splitlines()) == Counter(
            ["existing1", "existing2", "new1", "new2"]
        )


class TestAtomicity:
    """Test atomicity guarantees of move operations."""

    def test_bulk_move_atomicity_with_claimed_messages(self, workdir):
        """Test that bulk move skips claimed messages atomically."""
        # Write multiple messages
        for i in range(10):
            _seed(workdir, "source", f"msg{i}")
        # Claim some messages (read them)
        for _ in range(3):
            run_cli("read", "source", cwd=workdir)

        # Now do bulk move - should only move unclaimed messages
        rc, out, _ = run_cli("move", "source", "dest", "--all", cwd=workdir)
        assert rc == 0

        messages = out.strip().split("\n")
        assert len(messages) == 7  # 10 - 3 claimed
        assert messages == [f"msg{i}" for i in range(3, 10)]

        # Verify source is empty (claimed messages were already consumed)
        rc, out, _ = run_cli("peek", "source", cwd=workdir)
        assert rc == 2  # Empty

    def test_move_with_after_and_claimed_messages(self, workdir):
        """Test --after filter with mix of claimed and unclaimed messages."""
        # Write messages
        for i in range(10):
            _seed(workdir, "source", f"msg{i}")

        # Get timestamp of msg5
        rc, out, _ = run_cli("peek", "source", "--all", "--json", cwd=workdir)
        messages = [json.loads(line) for line in out.strip().split("\n")]
        cutoff_ts = messages[5]["timestamp"]

        # Claim msg6 and msg8
        run_cli("read", "source", "-m", str(messages[6]["timestamp"]), cwd=workdir)
        run_cli("read", "source", "-m", str(messages[8]["timestamp"]), cwd=workdir)

        # Move all messages after cutoff - should skip claimed ones
        rc, out, _ = run_cli(
            "move", "source", "dest", "--all", "--after", str(cutoff_ts), cwd=workdir
        )
        assert rc == 0

        # Should only get msg7 and msg9 (msg6 and msg8 were claimed)
        moved = out.strip().split("\n")
        assert moved == ["msg7", "msg9"]


class TestCommandLineValidation:
    """Test command-line argument validation."""

    def test_missing_arguments(self, workdir):
        """Test proper error messages for missing arguments."""
        # No arguments
        rc, _out, err = run_cli("move", cwd=workdir)
        assert rc == 1
        assert "required" in err.lower()

        # Only source queue
        rc, _out, err = run_cli("move", "source", cwd=workdir)
        assert rc == 1
        assert "required" in err.lower()

    def test_help_text(self, workdir):
        """Test that help text is available and mentions key options."""
        rc, out, err = run_cli("move", "--help", cwd=workdir)
        assert rc == 0
        # Help text goes to stderr in argparse
        help_text = err if err else out
        assert "move" in help_text.lower()
        assert "--all" in help_text
        assert "--after" in help_text
        assert "-m" in help_text or "--message" in help_text
        assert "--json" in help_text


def test_cmd_move_all_warns_when_materialized_cap_is_hit(workdir, monkeypatch, capsys):
    target = target_for_directory(workdir)
    source = Queue("source", db_path=target)
    for index in range(3):
        source.write(f"msg{index}")
    monkeypatch.setattr(commands, "_MOVE_ALL_LIMIT", 2)

    rc = commands.cmd_move(target, "source", "dest", all_messages=True)

    captured = capsys.readouterr()
    assert rc == 0
    assert "capped at 2" in captured.err
    assert "messages may remain" in captured.err
    assert Queue("dest", db_path=target).peek_many(limit=10, with_timestamps=False) == [
        "msg0",
        "msg1",
    ]
    assert Queue("source", db_path=target).peek_many(
        limit=10, with_timestamps=False
    ) == ["msg2"]
