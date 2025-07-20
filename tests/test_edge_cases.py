"""
Tests for edge cases identified during code review.

These tests cover specific edge cases and race conditions that might occur
in production environments.
"""

import multiprocessing
import os
import sqlite3
import sys
import time
import unittest.mock
from pathlib import Path
from typing import List

from simplebroker.db import BrokerDB

from .conftest import run_cli


def test_clock_regression_during_claim(workdir: Path):
    """Test behavior when system clock goes backward during read operations."""
    db_path = workdir / "test.db"

    # Write messages with normal timestamps
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")

    # Mock time.time to simulate clock regression
    original_time = time.time
    current_time = original_time()

    def mock_time():
        # Return a time 10 seconds in the past
        return current_time - 10.0

    # Read messages with regressed clock
    with unittest.mock.patch("time.time", mock_time):
        with BrokerDB(str(db_path)) as db:
            # This should still work correctly
            messages = list(db.stream_read("test_queue", peek=False, all_messages=True))
            assert len(messages) == 5
            assert messages == [f"message{i}" for i in range(5)]

    # Verify messages were claimed despite clock regression
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    assert cursor.fetchone()[0] == 5
    conn.close()


def test_vacuum_lock_cleanup_after_crash(workdir: Path):
    """Test that stale vacuum lock files are handled gracefully."""
    db_path = workdir / "test.db"
    lock_path = db_path.with_suffix(".vacuum.lock")  # test.vacuum.lock

    # Create some claimed messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("test_queue", f"message{i}")
        # Read all to claim them
        list(db.stream_read("test_queue", peek=False, all_messages=True))

    # Create a stale lock file (6 minutes old)
    with open(lock_path, "w") as f:
        f.write("12345\n")  # Fake PID

    # Make lock file appear old
    old_time = time.time() - 360  # 6 minutes ago
    os.utime(lock_path, (old_time, old_time))

    # Vacuum should remove stale lock and proceed
    # Need to patch at the module level where it's imported
    with unittest.mock.patch("simplebroker.db.warnings.warn") as mock_warn:
        with BrokerDB(str(db_path)) as db:
            db.vacuum()

            # Check that warning was issued about stale lock
            mock_warn.assert_called()
            warning_msg = str(mock_warn.call_args[0][0])
            assert "stale vacuum lock" in warning_msg.lower()

    # Verify vacuum succeeded
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    assert cursor.fetchone()[0] == 0  # All claimed messages removed
    conn.close()

    # Lock file should be cleaned up
    assert not lock_path.exists()


def test_vacuum_lock_timeout_environment_variable(workdir: Path):
    """Test that BROKER_VACUUM_LOCK_TIMEOUT environment variable works."""
    db_path = workdir / "test.db"
    lock_path = db_path.with_suffix(".vacuum.lock")  # test.vacuum.lock

    # Create some claimed messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")
        list(db.stream_read("test_queue", peek=False, all_messages=True))

    # Create a lock file that's 31 seconds old
    with open(lock_path, "w") as f:
        f.write("99999\n")
    old_time = time.time() - 31
    os.utime(lock_path, (old_time, old_time))

    # With default timeout (300s), lock should NOT be removed
    with unittest.mock.patch("simplebroker.db.warnings.warn") as mock_warn:
        with BrokerDB(str(db_path)) as db:
            db.vacuum()
            # Should not warn about stale lock
            if mock_warn.called:
                warning_msg = str(mock_warn.call_args[0][0])
                assert "stale vacuum lock" not in warning_msg.lower()

    # Lock should still exist
    assert lock_path.exists()

    # Now set timeout to 30 seconds
    os.environ["BROKER_VACUUM_LOCK_TIMEOUT"] = "30"
    try:
        with unittest.mock.patch("simplebroker.db.warnings.warn") as mock_warn:
            with BrokerDB(str(db_path)) as db:
                db.vacuum()
                # Should warn about stale lock
                mock_warn.assert_called()
                warning_msg = str(mock_warn.call_args[0][0])
                assert "stale vacuum lock" in warning_msg.lower()
    finally:
        del os.environ["BROKER_VACUUM_LOCK_TIMEOUT"]

    # Lock should be removed now
    assert not lock_path.exists()


def _schema_migration_worker(db_path: str, worker_id: int, results: List):
    """Worker process for concurrent schema migration test."""
    try:
        # Each worker tries to open database and trigger migration
        db = BrokerDB(db_path)
        # Write a message to ensure schema is used
        db.write(f"queue_{worker_id}", f"message_{worker_id}")
        # Read to verify migration worked
        messages = list(db.stream_read(f"queue_{worker_id}", peek=True))
        db.close()
        results.append((worker_id, "success", len(messages)))
    except Exception as e:
        results.append((worker_id, "error", str(e)))


def test_concurrent_schema_migration(workdir: Path):
    """Test multiple processes trying to migrate schema simultaneously."""
    db_path = workdir / "test.db"

    # Create old schema database without claimed column
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Enable WAL mode
    cursor.execute("PRAGMA journal_mode=WAL")

    # Create old schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue TEXT NOT NULL,
            body TEXT NOT NULL,
            ts INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        )
    """)
    cursor.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('last_ts', 0)")

    # Add some test data
    cursor.execute(
        "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
        ("old_queue", "old_message", 12345),
    )
    conn.commit()
    conn.close()

    # Launch multiple processes that will try to migrate simultaneously
    with multiprocessing.Manager() as manager:
        results = manager.list()
        processes = []

        for i in range(4):
            p = multiprocessing.Process(
                target=_schema_migration_worker, args=(str(db_path), i, results)
            )
            processes.append(p)
            p.start()

        # Wait for all processes
        for p in processes:
            p.join()

        # Convert to regular list
        results_list = list(results)

    # All workers should succeed
    assert len(results_list) == 4
    for worker_id, status, data in results_list:
        assert status == "success", f"Worker {worker_id} failed: {data}"
        assert data == 1  # Each worker should see their message

    # Verify schema was migrated correctly (only once)
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check claimed column exists
    cursor.execute("PRAGMA table_info(messages)")
    columns = {row[1] for row in cursor.fetchall()}
    assert "claimed" in columns

    # Check partial index exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%unclaimed%'"
    )
    indexes = cursor.fetchall()
    assert len(indexes) == 1

    conn.close()


def test_large_batch_claim_rollback_performance(workdir: Path):
    """Test that rollback is fast when a large batch claim is interrupted."""
    db_path = workdir / "test.db"

    # Write many messages
    message_count = 5000
    with BrokerDB(str(db_path)) as db:
        for i in range(message_count):
            db.write("test_queue", f"message{i:04d}")

    # Test rollback by simulating generator not being fully consumed
    # This tests the exactly-once vs at-least-once delivery semantics

    # First test with commit_interval=1 (exactly-once)
    messages_read = []
    start_time = time.time()

    with BrokerDB(str(db_path)) as db:
        # Read only first 100 messages then stop (simulating crash/interrupt)
        for i, msg in enumerate(
            db.stream_read(
                "test_queue",
                peek=False,
                all_messages=True,
                commit_interval=1,  # Exactly-once delivery
            )
        ):
            messages_read.append(msg)
            if i >= 99:  # Read 100 messages (0-99)
                break  # Simulate interrupt

    elapsed = time.time() - start_time
    timeout = 2.0 if sys.platform == "win32" else 1.5
    assert elapsed < timeout, (
        f"Reading 100 messages took too long: {elapsed:.2f}s (timeout: {timeout}s)"
    )

    # With exactly-once delivery, all 100 messages should be committed
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 100, f"Expected 100 claimed messages, got {claimed_count}"

    # Now test with large commit_interval (at-least-once)
    messages_read2 = []
    start_time = time.time()

    with BrokerDB(str(db_path)) as db:
        # Read 500 more messages with large batch
        for i, msg in enumerate(
            db.stream_read(
                "test_queue",
                peek=False,
                all_messages=True,
                commit_interval=1000,  # At-least-once delivery
            )
        ):
            messages_read2.append(msg)
            if i >= 499:  # Read 500 messages
                break  # Simulate interrupt mid-batch

    elapsed = time.time() - start_time
    timeout = 2.0 if sys.platform == "win32" else 1.5
    assert elapsed < timeout, (
        f"Reading 500 messages took too long: {elapsed:.2f}s (timeout: {timeout}s)"
    )

    # With at-least-once delivery and commit_interval=1000:
    # We read 500 messages but didn't complete the batch
    # So no additional messages should be committed
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    new_claimed_count = cursor.fetchone()[0]
    assert new_claimed_count == 100, (
        f"Expected 100 claimed messages (no new commits), got {new_claimed_count}"
    )

    # Verify unclaimed messages
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 0")
    unclaimed_count = cursor.fetchone()[0]
    assert unclaimed_count == 4900, (
        f"Expected 4900 unclaimed messages, got {unclaimed_count}"
    )

    conn.close()


def test_vacuum_with_concurrent_reads(workdir: Path):
    """Test vacuum operation while other processes are reading."""
    db_path = workdir / "test.db"

    # Create messages
    with BrokerDB(str(db_path)) as db:
        for i in range(100):
            db.write("test_queue", f"message{i}")

    # Read half to claim them
    with BrokerDB(str(db_path)) as db:
        for _ in range(50):
            list(db.stream_read("test_queue", peek=False, all_messages=False))

    # Instead of multiprocessing, use threading for simpler test
    import threading

    stop_event = threading.Event()
    reader_errors = []

    def continuous_reader():
        """Continuously read messages until stopped."""
        try:
            with BrokerDB(str(db_path)) as db:
                while not stop_event.is_set():
                    try:
                        # Peek at messages (non-destructive)
                        list(
                            db.stream_read("test_queue", peek=True, all_messages=False)
                        )
                        time.sleep(0.01)  # Small delay
                    except Exception as e:
                        reader_errors.append(e)
        except Exception as e:
            reader_errors.append(e)

    # Start reader thread
    reader_thread = threading.Thread(target=continuous_reader)
    reader_thread.start()

    try:
        # Give reader time to start
        time.sleep(0.1)

        # Run vacuum while reader is active
        with BrokerDB(str(db_path)) as db:
            db.vacuum()

        # Vacuum should complete successfully
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
        assert cursor.fetchone()[0] == 0  # All claimed messages removed
        cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 0")
        assert cursor.fetchone()[0] == 50  # Unclaimed messages remain
        conn.close()

    finally:
        # Stop reader
        stop_event.set()
        reader_thread.join(timeout=2)


def test_timestamp_overflow_protection(workdir: Path):
    """Test that user-provided timestamps are validated for overflow."""
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message1")
        db.write("test_queue", "message2")

    # Test with timestamp that would overflow when shifted
    # Maximum safe value is (2^63 - 1) >> 20 = 2^43 - 1
    max_safe_ms = (1 << 43) - 1
    overflow_ms = 1 << 43  # This will overflow when shifted by 20 bits

    # Test via CLI with --since flag
    # This should fail gracefully
    rc, out, err = run_cli(
        "read", "test_queue", "--all", f"--since={overflow_ms}ms", cwd=workdir
    )
    assert rc == 1  # Should fail
    assert "too far in future" in err.lower()

    # Test with maximum safe value (should work)
    rc, out, err = run_cli(
        "read", "test_queue", "--all", f"--since={max_safe_ms}ms", cwd=workdir
    )
    # Should succeed - queue exists but no messages match filter
    # Note: rc could be 0 (no messages match filter) or 2 (queue empty after filter)
    # Both are valid since the queue exists
    assert rc in [0, 2], f"Expected exit code 0 or 2, got {rc}"
    assert out == ""  # No messages returned
