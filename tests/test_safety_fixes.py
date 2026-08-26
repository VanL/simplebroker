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
import sqlite3
import time
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB

from .conftest import run_cli
from .helper_scripts.timing import scale_timeout_for_calibration, scale_timeout_for_ci


class _OldSQLiteCursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row

    def fetchall(self):
        return [self.row]

    def close(self):
        pass


class _OldSQLiteConnection:
    def __init__(self, *args, **kwargs):
        del args, kwargs
        self.call_count = 0

    def execute(self, query, params=None):
        del params
        self.call_count += 1
        normalized = " ".join(query.lower().split())
        if "left join meta" in normalized:
            raise sqlite3.OperationalError("no such table: meta")
        if "from pragma_schema_version" in normalized:
            return _OldSQLiteCursor((0, 0))
        if "sqlite_version" in normalized:
            return _OldSQLiteCursor(("3.34.0",))
        if "pragma" in normalized:
            return _OldSQLiteCursor((0,))
        if "from meta" in normalized:
            raise sqlite3.OperationalError("no such table: meta")
        raise RuntimeError("Mock connection")

    def commit(self):
        pass

    def close(self):
        pass


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


@pytest.mark.parametrize(
    "delete_args",
    [("missing",), ("--all",)],
    ids=["missing-named-queue", "empty-all"],
)
def test_delete_no_match_uses_queue_empty_exit_without_output(
    workdir,
    delete_args: tuple[str, ...],
) -> None:
    rc, out, err = run_cli("delete", *delete_args, cwd=workdir)

    assert rc == 2
    assert out == ""
    assert err == ""


def test_message_size_utf8_bytes(workdir):
    """Test that message size validation uses UTF-8 byte count, not char count."""
    # Create a message with multi-byte UTF-8 characters
    # Each emoji is 4 bytes in UTF-8
    emoji = "🎉"

    # Create message just under 10MB in bytes but much smaller in char count.
    # Use stdin to avoid command line length limits and exercise the real
    # streaming validation path.
    # 2,621,440 emojis * 4 bytes = 10,485,760 bytes (exactly 10MiB)
    big_message = emoji * 2_621_440
    timeout = scale_timeout_for_calibration(20.0, "write_test")

    # This should work because it is exactly at the byte limit.
    rc, _, _ = run_cli(
        "write",
        "test",
        "-",
        cwd=workdir,
        stdin=big_message,
        timeout=timeout,
    )
    assert rc == 0

    # Add one more emoji to exceed the byte limit while still being far smaller
    # in character count.
    too_big_message = big_message + emoji

    # This should fail because validation counts UTF-8 bytes, not characters.
    rc, _, err = run_cli(
        "write",
        "test",
        "-",
        cwd=workdir,
        stdin=too_big_message,
        timeout=timeout,
    )
    assert rc == 1
    assert "exceeds maximum size" in err


def test_message_size_stdin_utf8(workdir):
    """Test UTF-8 size validation for stdin input."""
    # Create message with multi-byte characters
    message = "Здравствуйте мир! 🌍" * 500_000  # Mix of 2-byte and 4-byte chars

    # Check if it would exceed limit
    if len(message.encode("utf-8")) > 10 * 1024 * 1024:
        # Should fail
        rc, _, err = run_cli(
            "write",
            "test",
            "-",
            cwd=workdir,
            stdin=message,
            timeout=20,
        )
        assert rc == 1
        assert "exceeds maximum size" in err
    else:
        # Should succeed
        rc, _, _ = run_cli(
            "write",
            "test",
            "-",
            cwd=workdir,
            stdin=message,
            timeout=20,
        )
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
    timeout = scale_timeout_for_ci(20.0)

    # This should work (increase timeout for large message)
    rc, _, _ = run_cli(
        "broadcast",
        "-",
        cwd=workdir,
        stdin=big_message,
        timeout=timeout,
    )
    assert rc == 0

    # This should fail
    rc, _, err = run_cli(
        "broadcast",
        "-",
        cwd=workdir,
        stdin=too_big_message,
        timeout=timeout,
    )
    assert rc == 1
    assert "exceeds maximum size" in err


def write_messages_subprocess(
    process_id: int,
    workdir: Path,
) -> tuple[list[str], list[str]]:
    """Write messages through one independent broker process."""
    messages_written = []
    db_path = workdir / ".broker.db"

    with BrokerDB(str(db_path)) as broker:
        for i in range(20):
            msg = f"process_{process_id}_msg_{i}"
            broker.write(f"queue_{process_id}", msg)
            messages_written.append(msg)

    return messages_written, []


def _timestamp_writer(
    process_id: int,
    workdir: Path,
    result_pipe: Connection,
) -> None:
    """Publish one spawned writer's result without a pool shutdown signal."""
    try:
        result_pipe.send(write_messages_subprocess(process_id, workdir))
    finally:
        result_pipe.close()


def _stop_timestamp_writers(processes: list[BaseProcess]) -> list[str]:
    """Kill stuck coverage children without invoking their SIGTERM handlers."""
    for process in processes:
        if process.is_alive():
            process.kill()
    for process in processes:
        if process.pid is not None:
            process.join(timeout=scale_timeout_for_ci(2.0))
    return [process.name for process in processes if process.is_alive()]


def _run_timestamp_writers(
    workdir: Path,
) -> tuple[
    list[tuple[list[str], list[str]]],
    list[str],
    list[str],
    list[int | None],
]:
    """Run independent writers with bounded process and result cleanup."""
    ctx = multiprocessing.get_context("spawn")
    processes: list[BaseProcess] = []
    receivers: list[Connection] = []
    senders: list[Connection] = []

    for process_id in range(5):
        receiver, sender = ctx.Pipe(duplex=False)
        spawned_process = ctx.Process(
            target=_timestamp_writer,
            args=(process_id, workdir, sender),
            name=f"timestamp-writer-{process_id}",
        )
        processes.append(spawned_process)
        receivers.append(receiver)
        senders.append(sender)

    active_at_deadline: list[str] = []
    results: list[tuple[list[str], list[str]]] = []
    cleanup_survivors: list[str] = []
    try:
        for process, sender in zip(processes, senders, strict=True):
            process.start()
            sender.close()

        deadline = time.monotonic() + scale_timeout_for_ci(60.0)
        while active := [process for process in processes if process.is_alive()]:
            if time.monotonic() >= deadline:
                active_at_deadline = [process.name for process in active]
                break
            for process in active:
                process.join(timeout=0.01)
    finally:
        cleanup_survivors = _stop_timestamp_writers(processes)
        for sender in senders:
            sender.close()
        for process, receiver in zip(processes, receivers, strict=True):
            try:
                if receiver.poll():
                    results.append(receiver.recv())
                else:
                    results.append(([], [f"{process.name} returned no result"]))
            finally:
                receiver.close()

    return (
        results,
        active_at_deadline,
        cleanup_survivors,
        [process.exitcode for process in processes],
    )


@pytest.mark.xdist_group(name="sqlite_process_stress")
@pytest.mark.sqlite_only
def test_timestamp_uniqueness_across_instances(workdir):
    """Test timestamp uniqueness across independent broker processes."""
    # Spawn rather than fork so an xdist worker's control descriptors and
    # coverage state are not inherited. Each process keeps one real BrokerDB
    # open instead of starting 20 nested CLI interpreters. Successful workers
    # exit normally; only a deadline failure uses kill(), which bypasses the
    # coverage SIGTERM handler that caused the original shutdown deadlock.
    (
        results,
        active_at_deadline,
        cleanup_survivors,
        writer_exitcodes,
    ) = _run_timestamp_writers(workdir)

    assert not active_at_deadline, (
        f"timestamp writers exceeded the bounded runtime: {active_at_deadline}"
    )
    assert not cleanup_survivors, f"writer cleanup failed: {cleanup_survivors}"
    assert writer_exitcodes == [0] * 5, (
        f"timestamp writers exited unsuccessfully: {writer_exitcodes}"
    )

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
        rc, out, err = run_cli(
            "peek",
            f"queue_{process_id}",
            "--all",
            "--timestamps",
            cwd=workdir,
        )
        assert rc == 0, f"Failed to peek queue_{process_id}: {err}"

        lines = out.strip().split("\n") if out.strip() else []
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
        rc, _, err = run_cli("write", "stress_test", f"rapid_{i}", cwd=workdir)
        assert rc == 0, f"Failed to write rapid_{i}: {err}"

    # Read back with peek to verify order
    rc, out, err = run_cli("peek", "stress_test", "--all", "--timestamps", cwd=workdir)
    assert rc == 0, f"Failed to peek stress_test: {err}"
    lines = out.strip().split("\n") if out.strip() else []
    assert len(lines) == 10
    for i, line in enumerate(lines):
        # Parse timestamp and message (format: timestamp\tmessage)
        parts = line.split("\t", 1)
        assert len(parts) == 2, f"Invalid format: {line}"
        _, msg = parts
        assert msg == f"rapid_{i}", (
            f"Messages out of order: expected rapid_{i}, got {msg}"
        )


@pytest.mark.sqlite_only
def test_sqlite_version_check(workdir, monkeypatch):
    """Test that old SQLite versions are rejected."""
    db_path = workdir / ".broker.db"

    # Patch sqlite3.connect to return our mock
    monkeypatch.setattr("sqlite3.connect", _OldSQLiteConnection)

    # Try to create BrokerDB - should fail with version error
    with pytest.raises(RuntimeError) as exc_info:
        BrokerDB(str(db_path))

    assert "SQLite version" in str(exc_info.value)
    assert "too old" in str(exc_info.value)
    assert "3.35.0 or later" in str(exc_info.value)


@pytest.mark.sqlite_only
def test_cleanup_toctou_fix(workdir):
    """Test that cleanup handles TOCTOU race condition gracefully."""
    # Create a database file
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    db_path = workdir / ".broker.db"
    lock_path = workdir / ".broker.db.lock"
    assert db_path.exists()
    assert lock_path.exists()

    # Delete the file manually to simulate race condition
    db_path.unlink()

    # Cleanup should still succeed and remove the orphaned owned namespace.
    rc, out, err = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert "Database cleaned up" in err
    assert not lock_path.exists()

    # Run cleanup again on non-existent file - should still succeed
    rc, out, err = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert "Database not found, nothing to clean up" in err


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
