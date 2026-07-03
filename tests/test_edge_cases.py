"""Tests for edge cases identified during code review.

These tests cover specific edge cases and race conditions that might occur
in production environments.
"""

from __future__ import annotations

import multiprocessing
import sqlite3
import time
import unittest.mock
from typing import TYPE_CHECKING

from simplebroker._backends.sqlite.maintenance import vacuum_lock_path
from simplebroker.db import BrokerDB

from .conftest import run_cli

if TYPE_CHECKING:
    from pathlib import Path


def test_clock_regression_during_claim(workdir: Path) -> None:
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
    with unittest.mock.patch("time.time", mock_time), BrokerDB(str(db_path)) as db:
        # This should still work correctly
        messages = db.claim_many("test_queue", limit=100, with_timestamps=False)
        assert len(messages) == 5
        assert messages == [f"message{i}" for i in range(5)]

    # Verify messages were claimed despite clock regression
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    assert cursor.fetchone()[0] == 5
    conn.close()


def test_vacuum_lock_cleanup_after_crash(workdir: Path) -> None:
    """A post-crash leftover vacuum lock file must not block the next vacuum.

    Crash recovery is now "the kernel released the flock when the holder died",
    not "an mtime-staleness sweep removes the file". A leftover lock file with
    no live flock holder must therefore let the very next vacuum proceed
    immediately -- no staleness timeout, no warning.
    """
    db_path = workdir / "test.db"
    lock_path = vacuum_lock_path(db_path)

    # Create some claimed messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("test_queue", f"message{i}")
        # Read all to claim them
        db.claim_many("test_queue", limit=100)

    # Simulate a SIGKILL-leftover lock file: it exists (with fresh mtime and a
    # dead PID) but NO process holds the flock.
    lock_path.write_text("12345\n", encoding="utf-8")

    # Vacuum proceeds immediately (kernel already released any flock).
    with BrokerDB(str(db_path)) as db:
        db.vacuum()

    # Verify vacuum succeeded
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    assert cursor.fetchone()[0] == 0  # All claimed messages removed
    conn.close()

    # The lock file is never unlinked (flock is ownership, the file is
    # permanent), so it remains after a successful vacuum.
    assert lock_path.exists()


def test_vacuum_lock_timeout_key_is_removed(workdir: Path) -> None:
    """BROKER_VACUUM_LOCK_TIMEOUT was removed in 5.0 (no backwards compat).

    The kernel-released vacuum flock made mtime-staleness detection
    unnecessary. Guard against reintroduction: the key is absent from config,
    and setting the env var has no effect on vacuum.
    """
    import os as _os
    from unittest.mock import patch as _patch

    from simplebroker._constants import load_config

    with _patch.dict(_os.environ, {"BROKER_VACUUM_LOCK_TIMEOUT": "0"}):
        config = load_config()
    assert "BROKER_VACUUM_LOCK_TIMEOUT" not in config

    db_path = workdir / "test.db"
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")
        db.claim_many("test_queue", limit=100)

    with _patch.dict(_os.environ, {"BROKER_VACUUM_LOCK_TIMEOUT": "0"}):
        with BrokerDB(str(db_path)) as db:
            db.vacuum()


def _schema_migration_worker(db_path: str, worker_id: int, results: list) -> None:
    """Worker process for concurrent schema migration test."""
    try:
        # Each worker tries to open database and trigger migration
        db = BrokerDB(db_path)
        # Write a message to ensure schema is used
        db.write(f"queue_{worker_id}", f"message_{worker_id}")
        # Read to verify migration worked
        messages = db.peek_many(f"queue_{worker_id}", limit=100)
        db.close()
        results.append((worker_id, "success", len(messages)))
    except Exception as e:
        results.append((worker_id, "error", str(e)))


def test_concurrent_schema_migration(workdir: Path) -> None:
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
                target=_schema_migration_worker,
                args=(str(db_path), i, results),
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
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%unclaimed%'",
    )
    indexes = cursor.fetchall()
    assert len(indexes) == 1

    conn.close()


def test_vacuum_with_concurrent_reads(workdir: Path) -> None:
    """Test vacuum operation while other processes are reading."""
    db_path = workdir / "test.db"

    # Create messages
    with BrokerDB(str(db_path)) as db:
        for i in range(100):
            db.write("test_queue", f"message{i}")

    # Read half to claim them
    with BrokerDB(str(db_path)) as db:
        for _ in range(50):
            db.claim_one("test_queue")

    # Instead of multiprocessing, use threading for simpler test
    import threading

    stop_event = threading.Event()
    reader_errors = []

    def continuous_reader() -> None:
        """Continuously read messages until stopped."""
        try:
            with BrokerDB(str(db_path)) as db:
                while not stop_event.is_set():
                    try:
                        # Peek at messages (non-destructive)
                        db.peek_one("test_queue")
                        time.sleep(0.01)  # Small delay
                    except Exception as e:
                        reader_errors.append(e)
        except Exception as e:
            reader_errors.append(e)

    # Start reader thread
    reader_thread = threading.Thread(target=continuous_reader)
    reader_started = threading.Event()

    def continuous_reader_with_signal() -> None:
        reader_started.set()  # Signal that reader has started
        continuous_reader()

    reader_thread = threading.Thread(target=continuous_reader_with_signal)
    reader_thread.start()

    try:
        # Wait for reader to actually start (with timeout)
        if not reader_started.wait(timeout=2.0):
            msg = "Reader thread did not start within timeout"
            raise TimeoutError(msg)

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


def test_timestamp_overflow_protection(workdir: Path) -> None:
    """Test that user-provided timestamps are validated for overflow."""
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message1")
        db.write("test_queue", "message2")

    # Test with timestamp that would overflow when shifted
    # For millisecond input: value * 1000 * 2^12 must be < 2^63
    # Maximum safe ms value is approximately 2^63 / (1000 * 2^12) ≈ 2.25e12
    max_safe_ms = 2_251_799_813_685  # Safe value for ms input
    overflow_ms = 10_000_000_000_000  # 10 trillion ms - will overflow

    # Test via CLI with --after flag
    # This should fail gracefully
    rc, out, err = run_cli(
        "read",
        "test_queue",
        "--all",
        f"--after={overflow_ms}ms",
        cwd=workdir,
    )
    assert rc == 1  # Should fail
    assert "too far in future" in err.lower()

    # Test with maximum safe value (should work)
    rc, out, err = run_cli(
        "read",
        "test_queue",
        "--all",
        f"--after={max_safe_ms}ms",
        cwd=workdir,
    )
    # Should succeed - queue exists but no messages match filter
    # Note: rc could be 0 (no messages match filter) or 2 (queue empty after filter)
    # Both are valid after the queue exists
    assert rc in [0, 2], f"Expected exit code 0 or 2, got {rc}"
    assert out == ""  # No messages returned
