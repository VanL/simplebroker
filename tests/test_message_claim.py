"""
Tests for message claim feature - messages marked as claimed instead of deleted.

Tests cover:
1. Messages being marked as claimed (claimed=1) instead of deleted during read operations
2. Claimed messages not being returned by subsequent reads
3. Vacuum process removing claimed messages
4. Automatic vacuum triggers based on threshold
5. Concurrent reads not delivering the same message twice
6. Performance improvement verification
7. Schema migration adding claimed column
8. Backward compatibility
"""

import concurrent.futures as cf
import sqlite3
import sys
import time
from pathlib import Path
from typing import List, Tuple

import pytest

from simplebroker.db import BrokerDB

from .conftest import run_cli


def test_messages_marked_as_claimed_not_deleted(workdir: Path):
    """Test that messages are marked as claimed (claimed=1) instead of being deleted."""
    db_path = workdir / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message1")
        db.write("test_queue", "message2")
        db.write("test_queue", "message3")

    # Read one message
    with BrokerDB(str(db_path)) as db:
        messages = list(db.stream_read("test_queue", peek=False, all_messages=False))
        assert len(messages) == 1
        assert messages[0] == "message1"

    # Verify message is claimed, not deleted
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check that claimed column exists
    cursor.execute("PRAGMA table_info(messages)")
    columns = {row[1] for row in cursor.fetchall()}
    assert "claimed" in columns

    # Check claimed messages
    cursor.execute(
        "SELECT body, claimed FROM messages WHERE queue = 'test_queue' ORDER BY id"
    )
    all_messages = cursor.fetchall()

    assert len(all_messages) == 3
    assert all_messages[0] == ("message1", 1)  # First message claimed
    assert all_messages[1] == ("message2", 0)  # Others not claimed
    assert all_messages[2] == ("message3", 0)

    conn.close()


def test_claimed_messages_not_returned_by_subsequent_reads(workdir: Path):
    """Test that claimed messages are not returned by subsequent read operations."""
    db_path = workdir / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")

    # Read first two messages
    with BrokerDB(str(db_path)) as db:
        msg1 = list(db.stream_read("test_queue", peek=False, all_messages=False))[0]
        msg2 = list(db.stream_read("test_queue", peek=False, all_messages=False))[0]
        assert msg1 == "message0"
        assert msg2 == "message1"

    # Read remaining messages with --all
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.stream_read("test_queue", peek=False, all_messages=True))
        assert len(remaining) == 3
        assert remaining == ["message2", "message3", "message4"]

    # Verify no more messages available
    with BrokerDB(str(db_path)) as db:
        last_check = list(db.stream_read("test_queue", peek=False, all_messages=True))
        assert len(last_check) == 0


def test_vacuum_removes_claimed_messages(workdir: Path):
    """Test that vacuum process properly removes claimed messages."""
    db_path = workdir / "test.db"

    # Write and read messages to create claimed entries
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("test_queue", f"message{i}")

    # Read all messages to claim them
    with BrokerDB(str(db_path)) as db:
        messages = list(db.stream_read("test_queue", peek=False, all_messages=True))
        assert len(messages) == 10

    # Check claimed messages exist
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 10

    # Run vacuum
    with BrokerDB(str(db_path)) as db:
        db.vacuum()

    # Verify claimed messages are removed
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_after_vacuum = cursor.fetchone()[0]
    assert claimed_after_vacuum == 0

    cursor.execute("SELECT COUNT(*) FROM messages")
    total_messages = cursor.fetchone()[0]
    assert total_messages == 0

    conn.close()


def test_automatic_vacuum_trigger(workdir: Path):
    """Test automatic vacuum triggers based on 10% threshold."""

    db_path = workdir / "test.db"

    # Write 100 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(100):
            db.write("test_queue", f"message{i}")

    # Read 10 messages (10% threshold)
    with BrokerDB(str(db_path)) as db:
        for _ in range(10):
            list(db.stream_read("test_queue", peek=False, all_messages=False))

    # Check claimed count before potential auto-vacuum
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 10

    # Force vacuum to check - directly test the vacuum logic
    with BrokerDB(str(db_path)) as db:
        # Verify _should_vacuum returns True
        assert db._should_vacuum(), "Should trigger vacuum at 10% threshold"

        # Run vacuum manually
        db.vacuum()

    # After vacuum, claimed messages should be removed
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    final_claimed = cursor.fetchone()[0]
    assert final_claimed == 0, "All claimed messages should be removed after vacuum"

    conn.close()


def _concurrent_reader_worker(args: Tuple[int, str, str]) -> List[str]:
    """Worker function for concurrent read tests."""
    worker_id, db_path, queue_name = args
    messages = []

    with BrokerDB(db_path) as db:
        # Each worker tries to read 5 messages
        for _ in range(5):
            msgs = list(db.stream_read(queue_name, peek=False, all_messages=False))
            if msgs:
                messages.extend(msgs)
            else:
                break  # Queue empty

    return messages


def test_concurrent_reads_no_duplicate_delivery(workdir: Path):
    """Test that concurrent reads don't deliver the same message twice."""
    db_path = workdir / "test.db"

    # Write 20 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(20):
            db.write("concurrent_queue", f"message{i:02d}")

    # Start 4 concurrent readers
    with cf.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(4):
            future = executor.submit(
                _concurrent_reader_worker, (i, str(db_path), "concurrent_queue")
            )
            futures.append(future)

        # Collect all messages read
        all_messages = []
        for future in cf.as_completed(futures):
            messages = future.result()
            all_messages.extend(messages)

    # Verify no duplicates
    assert len(all_messages) == 20
    assert len(set(all_messages)) == 20  # All unique

    # Verify all messages were read
    expected = {f"message{i:02d}" for i in range(20)}
    assert set(all_messages) == expected


@pytest.mark.skipif(
    sys.platform == "win32" and sys.version_info[:2] == (3, 8),
    reason="Python 3.8 performance on Windows is not guaranteed",
)
def test_performance_improvement_with_claims(workdir: Path):
    """Test performance improvement when using claimed vs delete operations."""
    db_path = workdir / "test.db"

    # Write a large batch of messages
    message_count = 1000
    with BrokerDB(str(db_path)) as db:
        for i in range(message_count):
            db.write("perf_queue", f"msg{i:04d}")

    # Time reading all messages
    start_time = time.time()
    with BrokerDB(str(db_path)) as db:
        messages = list(db.stream_read("perf_queue", peek=False, all_messages=True))
    read_time = time.time() - start_time

    assert len(messages) == message_count

    # Verify all messages are claimed
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == message_count

    # Performance assertion - reading should be fast
    # Claimed approach should handle 1000 messages quickly
    # Windows filesystem operations are slower, so we allow more time
    timeout = 6.0 if sys.platform == "win32" else 1.5
    assert read_time < timeout, (
        f"Reading {message_count} messages took {read_time:.2f}s"
    )

    conn.close()


def test_schema_migration_adds_claimed_column(workdir: Path):
    """Test that schema migration properly adds claimed column to existing databases."""
    db_path = workdir / "test.db"

    # Create database with old schema (simulate pre-claim version)
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create old schema without claimed column
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue TEXT NOT NULL,
            body TEXT NOT NULL,
            ts INTEGER NOT NULL
        )
    """)

    # Add some messages
    cursor.execute(
        "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
        ("old_queue", "old_message", 12345),
    )
    conn.commit()
    conn.close()

    # Open with BrokerDB - should trigger migration
    with BrokerDB(str(db_path)) as db:
        # Try to read - should work with migrated schema
        messages = list(db.stream_read("old_queue", peek=False, all_messages=True))
        assert len(messages) == 1
        assert messages[0] == "old_message"

    # Verify claimed column was added
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(messages)")
    columns = {row[1] for row in cursor.fetchall()}
    assert "claimed" in columns

    # Verify old message has claimed=1 after read
    cursor.execute("SELECT claimed FROM messages WHERE body = 'old_message'")
    result = cursor.fetchone()
    assert result[0] == 1

    conn.close()


def test_schema_migration_idempotent(workdir: Path):
    """Test that schema migration is idempotent and can run multiple times safely."""
    db_path = workdir / "test.db"

    # Create and migrate database multiple times
    for i in range(3):
        with BrokerDB(str(db_path)) as db:
            db.write(f"queue_{i}", f"message_{i}")

    # Verify schema is correct
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Check columns
    cursor.execute("PRAGMA table_info(messages)")
    columns = {row[1] for row in cursor.fetchall()}
    assert "claimed" in columns

    # Check indexes
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%claimed%'"
    )
    indexes = [row[0] for row in cursor.fetchall()]
    assert any("claimed" in idx for idx in indexes)  # Should have partial index

    # Verify all messages are intact
    cursor.execute("SELECT COUNT(*) FROM messages")
    assert cursor.fetchone()[0] == 3

    conn.close()


def test_backward_compatibility_cli(workdir: Path):
    """Test backward compatibility through CLI interface."""
    # Write messages using CLI
    rc, _, _ = run_cli("write", "compat_queue", "msg1", cwd=workdir)
    assert rc == 0
    rc, _, _ = run_cli("write", "compat_queue", "msg2", cwd=workdir)
    assert rc == 0
    rc, _, _ = run_cli("write", "compat_queue", "msg3", cwd=workdir)
    assert rc == 0

    # Read single message
    rc, out, _ = run_cli("read", "compat_queue", cwd=workdir)
    assert rc == 0
    assert out == "msg1"

    # Read remaining with --all
    rc, out, _ = run_cli("read", "compat_queue", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["msg2", "msg3"]

    # Queue should be empty
    rc, _, _ = run_cli("read", "compat_queue", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    # List queues should still work with --stats flag to show claimed messages
    rc, out, _ = run_cli("list", "--stats", cwd=workdir)
    assert rc == 0
    assert "compat_queue" in out


def test_peek_does_not_claim(workdir: Path):
    """Test that peek operations do not mark messages as claimed."""
    db_path = workdir / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        for i in range(3):
            db.write("peek_queue", f"message{i}")

    # Peek all messages
    with BrokerDB(str(db_path)) as db:
        peeked = list(db.stream_read("peek_queue", peek=True, all_messages=True))
        assert len(peeked) == 3

    # Verify no messages are claimed
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 0

    # All messages should still be available for reading
    with BrokerDB(str(db_path)) as db:
        messages = list(db.stream_read("peek_queue", peek=False, all_messages=True))
        assert len(messages) == 3

    # Now all should be claimed
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    claimed_count = cursor.fetchone()[0]
    assert claimed_count == 3

    conn.close()


def test_vacuum_with_mixed_queues(workdir: Path):
    """Test vacuum behavior with multiple queues having claimed and unclaimed messages."""
    db_path = workdir / "test.db"

    # Create messages in different queues
    with BrokerDB(str(db_path)) as db:
        # Queue 1: Some claimed, some not
        for i in range(5):
            db.write("queue1", f"q1_msg{i}")

        # Queue 2: All will be claimed
        for i in range(3):
            db.write("queue2", f"q2_msg{i}")

        # Queue 3: None will be claimed
        for i in range(4):
            db.write("queue3", f"q3_msg{i}")

    # Read some messages from queue1 and all from queue2
    with BrokerDB(str(db_path)) as db:
        # Read 2 from queue1
        for _ in range(2):
            list(db.stream_read("queue1", peek=False, all_messages=False))

        # Read all from queue2
        list(db.stream_read("queue2", peek=False, all_messages=True))

    # Check state before vacuum
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT queue, COUNT(*), SUM(claimed) FROM messages GROUP BY queue")
    pre_vacuum_state = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    assert pre_vacuum_state["queue1"] == (5, 2)  # 5 total, 2 claimed
    assert pre_vacuum_state["queue2"] == (3, 3)  # 3 total, 3 claimed
    assert pre_vacuum_state["queue3"] == (4, 0)  # 4 total, 0 claimed

    # Run vacuum
    with BrokerDB(str(db_path)) as db:
        db.vacuum()

    # Check state after vacuum
    cursor.execute("SELECT queue, COUNT(*), SUM(claimed) FROM messages GROUP BY queue")
    post_vacuum_state = {row[0]: (row[1], row[2] or 0) for row in cursor.fetchall()}

    assert post_vacuum_state["queue1"] == (3, 0)  # 3 unclaimed remain
    assert "queue2" not in post_vacuum_state  # All were claimed, queue removed
    assert post_vacuum_state["queue3"] == (4, 0)  # Unchanged

    conn.close()


def test_vacuum_with_no_claimed_messages(workdir: Path):
    """Test vacuum operation when there are no claimed messages."""
    db_path = workdir / "test.db"

    # Write messages but don't read any
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")

    # Run vacuum - should be a no-op
    with BrokerDB(str(db_path)) as db:
        db.vacuum()

    # Verify all messages still exist
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages")
    assert cursor.fetchone()[0] == 5
    conn.close()


def test_vacuum_batch_size_limits(workdir: Path):
    """Test that vacuum respects batch size limits for performance."""
    db_path = workdir / "test.db"

    # Write and claim a large number of messages
    message_count = 10000
    with BrokerDB(str(db_path)) as db:
        # Write in batches for efficiency
        for i in range(0, message_count, 100):
            for j in range(100):
                if i + j < message_count:
                    db.write("large_queue", f"msg{i + j:05d}")

    # Read all messages to claim them
    with BrokerDB(str(db_path)) as db:
        count = 0
        for _msg in db.stream_read("large_queue", peek=False, all_messages=True):
            count += 1
        assert count == message_count

    # Verify all are claimed
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    assert cursor.fetchone()[0] == message_count

    # Run vacuum - should handle large batch efficiently
    start_time = time.time()
    with BrokerDB(str(db_path)) as db:
        db.vacuum()
    vacuum_time = time.time() - start_time

    # Verify all claimed messages removed
    cursor.execute("SELECT COUNT(*) FROM messages")
    assert cursor.fetchone()[0] == 0

    # Vacuum should complete reasonably quickly even with many messages
    assert vacuum_time < 5.0, (
        f"Vacuum of {message_count} messages took {vacuum_time:.2f}s"
    )

    conn.close()


def test_vacuum_lock_prevents_concurrent_vacuum(workdir: Path):
    """Test that vacuum lock prevents concurrent vacuum operations."""
    db_path = workdir / "test.db"

    # Write and claim some messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("test_queue", f"message{i}")
        list(db.stream_read("test_queue", peek=False, all_messages=True))

    # Simulate concurrent vacuum attempts
    def vacuum_worker(db_path: str) -> bool:
        """Try to run vacuum, return True if successful."""
        try:
            with BrokerDB(db_path) as db:
                db.vacuum()
            return True
        except Exception:
            return False

    # Run multiple vacuum operations concurrently
    with cf.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(vacuum_worker, str(db_path)) for _ in range(3)]
        results = [f.result() for f in cf.as_completed(futures)]

    # At least one should succeed, others might fail due to lock
    assert any(results), "At least one vacuum should succeed"

    # Verify messages were cleaned up
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages")
    assert cursor.fetchone()[0] == 0
    conn.close()


def test_partial_index_on_claimed_column(workdir: Path):
    """Test that partial index is created on claimed column for performance."""
    db_path = workdir / "test.db"

    # Create database
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message")

    # Check for partial index
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Get index information
    cursor.execute("""
        SELECT sql FROM sqlite_master
        WHERE type = 'index' AND sql LIKE '%claimed%'
    """)
    index_sql = cursor.fetchone()

    # Should have a partial index WHERE claimed = 0
    assert index_sql is not None
    assert "WHERE claimed = 0" in index_sql[0] or "WHERE claimed=0" in index_sql[0]

    conn.close()


def test_all_flag_with_mixed_claimed_unclaimed(workdir: Path):
    """Test --all flag behavior with mix of claimed and unclaimed messages."""
    db_path = workdir / "test.db"

    # Write 10 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("mixed_queue", f"message{i}")

    # Read (claim) first 3 messages
    with BrokerDB(str(db_path)) as db:
        for _ in range(3):
            list(db.stream_read("mixed_queue", peek=False, all_messages=False))

    # Read with --all should get only unclaimed messages
    with BrokerDB(str(db_path)) as db:
        messages = list(db.stream_read("mixed_queue", peek=False, all_messages=True))
        assert len(messages) == 7
        assert messages == [f"message{i}" for i in range(3, 10)]

    # All messages should now be claimed
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
    assert cursor.fetchone()[0] == 10
    conn.close()


@pytest.mark.skipif(
    sys.platform == "win32" and sys.version_info[:2] == (3, 8),
    reason="Python 3.8 performance on Windows is not guaranteed",
)
def test_write_performance_not_regressed(workdir: Path):
    """Test that write performance is not affected by claim feature."""
    db_path = workdir / "test.db"

    # Measure write performance
    message_count = 1000
    start_time = time.time()

    with BrokerDB(str(db_path)) as db:
        for i in range(message_count):
            db.write("write_perf_queue", f"msg{i:04d}")

    write_time = time.time() - start_time

    # Writing should still be fast
    # Windows needs more time due to filesystem differences
    timeout = 6.0 if sys.platform == "win32" else 1.5
    assert write_time < timeout, (
        f"Writing {message_count} messages took {write_time:.2f}s"
    )

    # Verify messages were written correctly
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages")
    assert cursor.fetchone()[0] == message_count
    conn.close()


def test_manual_vacuum_cli_command(workdir: Path):
    """Test manual vacuum command through CLI."""
    # Write and read messages to create claimed entries
    for i in range(5):
        rc, _, _ = run_cli("write", "vacuum_test", f"msg{i}", cwd=workdir)
        assert rc == 0

    # Read all messages
    rc, out, _ = run_cli("read", "vacuum_test", "--all", cwd=workdir)
    assert rc == 0
    assert len(out.splitlines()) == 5

    # Run vacuum command
    rc, out, _ = run_cli("--vacuum", cwd=workdir)
    assert rc == 0

    # Verify queue is empty after vacuum
    rc, _, _ = run_cli("read", "vacuum_test", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    # List should show no messages in queue
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    # Queue might not appear if all messages were vacuumed
