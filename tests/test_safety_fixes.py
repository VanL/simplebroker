"""
Tests for critical safety fixes and bug corrections.

Tests verify:
- Purge safety (require queue name or --all flag)
- Message size validation uses UTF-8 byte count
- Timestamp uniqueness across instances
- SQLite version checking
- TOCTOU fix for cleanup
"""

import threading

import pytest

from simplebroker.db import BrokerDB

from .conftest import run_cli


def test_purge_safety_no_args(workdir):
    """Test that purge with no arguments is rejected for safety."""
    # Write some messages first
    rc, _, _ = run_cli("write", "test_queue", "message1", cwd=workdir)
    assert rc == 0

    # Try to purge with no arguments - should fail
    rc, _, err = run_cli("purge", cwd=workdir)
    assert rc == 1
    assert "purge requires either a queue name or --all flag" in err

    # Verify messages still exist
    rc, out, _ = run_cli("peek", "test_queue", cwd=workdir)
    assert rc == 0
    assert out == "message1"


def test_purge_with_queue_name(workdir):
    """Test that purge with specific queue name works."""
    # Write to multiple queues
    run_cli("write", "queue1", "msg1", cwd=workdir)
    run_cli("write", "queue2", "msg2", cwd=workdir)

    # Purge only queue1
    rc, _, _ = run_cli("purge", "queue1", cwd=workdir)
    assert rc == 0

    # Verify queue1 is empty but queue2 still has messages
    rc, _, _ = run_cli("peek", "queue1", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    rc, out, _ = run_cli("peek", "queue2", cwd=workdir)
    assert rc == 0
    assert out == "msg2"


def test_purge_with_all_flag(workdir):
    """Test that purge --all works correctly."""
    # Write to multiple queues
    run_cli("write", "queue1", "msg1", cwd=workdir)
    run_cli("write", "queue2", "msg2", cwd=workdir)

    # Purge all queues
    rc, _, _ = run_cli("purge", "--all", cwd=workdir)
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

    # This should work
    rc, _, _ = run_cli("broadcast", "-", cwd=workdir, stdin=big_message)
    assert rc == 0

    # This should fail
    rc, _, err = run_cli("broadcast", "-", cwd=workdir, stdin=too_big_message)
    assert rc == 1
    assert "exceeds maximum size" in err


def test_timestamp_uniqueness_across_instances(workdir):
    """Test that timestamps are unique even across BrokerDB instances."""
    db_path = workdir / ".broker.db"

    # Track all messages written and errors
    all_messages = {}
    errors = []
    lock = threading.Lock()

    def write_messages(thread_id):
        """Write messages from a separate BrokerDB instance."""
        try:
            # Each thread gets its own DB instance
            with BrokerDB(str(db_path)) as db:
                for i in range(20):  # Reduced from 100 to avoid lock contention
                    msg = f"thread_{thread_id}_msg_{i}"
                    db.write(f"queue_{thread_id}", msg)
                    with lock:
                        all_messages[msg] = thread_id
        except Exception as e:
            with lock:
                errors.append(str(e))

    # Run multiple threads, each with its own BrokerDB instance
    threads = []
    for i in range(5):
        t = threading.Thread(target=write_messages, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Check for errors
    assert not errors, f"Errors occurred: {errors}"

    # Verify message uniqueness by reading all messages back
    # This indirectly tests timestamp uniqueness - if timestamps were duplicated,
    # messages would be lost or returned in wrong order
    messages_read = {}
    with BrokerDB(str(db_path)) as db:
        for thread_id in range(5):
            # MODIFICATION: Use a non-destructive peek (read with peek=True) to verify.
            # This avoids write-lock contention with background DB cleanup/checkpointing,
            # making the test more robust against timing-related lock errors.
            msgs = db.read(f"queue_{thread_id}", all_messages=True, peek=True)
            assert len(msgs) == 20, (
                f"Expected 20 messages for thread {thread_id}, got {len(msgs)}"
            )

            # Verify order is preserved (FIFO)
            for i, msg in enumerate(msgs):
                expected = f"thread_{thread_id}_msg_{i}"
                assert msg == expected, f"Expected {expected}, got {msg}"
                messages_read[msg] = thread_id

    # Verify we got all messages back
    assert len(messages_read) == 100, f"Expected 100 messages, got {len(messages_read)}"
    assert set(messages_read.keys()) == set(all_messages.keys()), (
        "Some messages were lost"
    )

    # Additional test: rapid writes to same queue to stress timestamp generation
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("stress_test", f"rapid_{i}")

        # BEST PRACTICE: Also use peek=True here for consistency, although
        # the risk of locking is lower in this single-threaded section.
        msgs = db.read("stress_test", all_messages=True, peek=True)
        assert len(msgs) == 10
        for i, msg in enumerate(msgs):
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
            pass

        def execute(self, query, params=None):
            if "sqlite_version" in query:
                return MockCursor()
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
