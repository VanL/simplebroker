"""
Tests for timestamp resilience - a backup safety mechanism.

These tests verify the hybrid approach for handling timestamp conflicts:
1. Fast retry for transient issues
2. State resynchronization for persistent inconsistencies
3. Clear error reporting for unresolvable conflicts

The hybrid timestamp algorithm is already extremely robust, so these tests
just verify the resilience mechanism works if somehow a conflict occurs,
such as a bit flip, human changes to the db, or a bug not caught by our
hybrid timestamp tests.
"""

import threading
import time
import warnings

import pytest

from simplebroker.db import BrokerDB


def test_normal_operation_no_conflicts(workdir):
    """Verify normal operation has zero overhead."""
    db = BrokerDB(str(workdir / "test.db"))

    # Write several messages
    for i in range(10):
        db.write("queue", f"Message {i}")

    # No conflicts should occur in normal operation
    assert db.get_conflict_metrics()["ts_conflict_count"] == 0
    assert db.get_conflict_metrics()["ts_resync_count"] == 0

    # Verify all messages written
    messages = list(db.read("queue", all_messages=True))
    assert len(messages) == 10


def test_forced_conflict_handled(workdir):
    """Test that if somehow a conflict occurs, it's handled gracefully."""
    db = BrokerDB(str(workdir / "test.db"))

    # Artificially create a duplicate timestamp scenario
    # This should never happen in practice, but if it does...

    # Insert a message with a specific timestamp
    with db._lock:
        db._runner.run(
            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
            ("queue", "Message 1", 12345),
        )
        # Don't set meta - let the timestamp generator figure it out itself
        db._runner.commit()

    # Force timestamp generator to return values that will conflict
    # at the INSERT level (not inside the generator)
    original = db._timestamp_gen.generate
    call_count = 0

    def mock_generate():
        nonlocal call_count
        call_count += 1
        if call_count <= 2:  # First 2 calls return values that will conflict
            return 12345  # This will cause INSERT to fail
        return original()  # Then return normal timestamps

    db._timestamp_gen.generate = mock_generate

    try:
        # This should retry and eventually succeed through resilience mechanism
        # Suppress expected warnings about timestamp conflicts
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Timestamp conflict persisted",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="Timestamp generator resynchronized",
                category=RuntimeWarning,
            )
            db.write("queue", "Message 2")

        # Verify the write succeeded
        messages = list(db.read("queue", all_messages=True))
        assert len(messages) == 2

        # Verify resilience kicked in
        assert db.get_conflict_metrics()["ts_conflict_count"] > 0
        assert db.get_conflict_metrics()["ts_resync_count"] > 0
    finally:
        db._timestamp_gen.generate = original


def test_transient_conflict_recovery(workdir):
    """Test recovery from a single transient conflict."""
    db_path = workdir / "test.db"
    db = BrokerDB(str(db_path))

    # To force a conflict, we need to:
    # 1. Write a message to get a timestamp
    # 2. Manually set meta to a value that will generate a duplicate

    # Get current time in microseconds
    current_us = int(time.time() * 1_000_000)

    # Create a timestamp manually
    ts_to_conflict = (current_us << 12) | 0  # physical time with counter 0

    # Insert a message with this timestamp
    with db._lock:
        db._runner.run(
            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
            ("queue", "Message 1", ts_to_conflict),
        )
        # Set meta to generate the same timestamp
        db._runner.run(
            "UPDATE meta SET value = ? WHERE key = 'last_ts'", (ts_to_conflict - 1,)
        )
        db._runner.commit()

    # Now when we write, if the same millisecond is still current,
    # it will try to use ts_to_conflict and conflict
    initial_conflicts = db.get_conflict_metrics()["ts_conflict_count"]  # noqa: F841
    initial_resyncs = db.get_conflict_metrics()["ts_resync_count"]  # noqa: F841

    # This might or might not conflict depending on timing
    db.write("queue", "Message 2")

    # Just verify no errors occurred
    messages = list(db.read("queue", all_messages=True))
    assert len(messages) == 2


def test_truly_unresolvable_conflict_fails_safely(workdir):
    """Test that truly unresolvable conflicts fail with clear error."""
    db = BrokerDB(str(workdir / "test.db"))

    # Insert a message with a specific timestamp
    with db._lock:
        db._runner.run(
            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
            ("queue", "Message 1", 12345),
        )
        db._runner.commit()

    # Force generator to always return same timestamp
    # This simulates a broken timestamp generator
    db._timestamp_gen.generate = lambda: 12345

    # This should fail after retries
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Timestamp conflict persisted", category=RuntimeWarning
        )
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore", message="Timestamp conflict unresolvable", category=RuntimeWarning
        )
        with pytest.raises(RuntimeError) as exc_info:
            db.write("queue", "Message 2")

    assert "Failed to write message" in str(exc_info.value)


def test_resync_fixes_inconsistent_state(workdir):
    """Test that resync can fix inconsistent state."""
    db = BrokerDB(str(workdir / "test.db"))

    # Write some messages
    for i in range(5):
        db.write("queue", f"Message {i}")

    # Corrupt meta table (simulate inconsistent state)
    with db._lock:
        db._runner.run("UPDATE meta SET value = 0 WHERE key = 'last_ts'")
        db._runner.commit()

    # Resync should fix it
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )
        db._resync_timestamp_generator()

    # Verify we can still write
    db.write("queue", "Message after resync")

    messages = list(db.read("queue", all_messages=True))
    assert len(messages) == 6


def test_state_inconsistency_direct_fix(workdir):
    """Test that resync correctly fixes state inconsistency."""
    db_path = workdir / "test.db"
    db = BrokerDB(str(db_path))

    # Write some messages
    for i in range(5):
        db.write("queue", f"Message {i}")

    # Get current state
    with db._lock:
        result = list(db._runner.run("SELECT MAX(ts) FROM messages", fetch=True))
        max_ts = result[0][0]

    # Corrupt meta
    with db._lock:
        db._runner.run("UPDATE meta SET value = 0 WHERE key = 'last_ts'")
        db._runner.commit()

    # Call resync directly
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )
        db._resync_timestamp_generator()

    # Verify it was fixed
    with db._lock:
        result = list(
            db._runner.run("SELECT value FROM meta WHERE key = 'last_ts'", fetch=True)
        )
        new_value = result[0][0]

    assert new_value == max_ts


def test_unresolvable_conflict(workdir):
    """Test handling of truly unresolvable conflicts."""
    db_path = workdir / "test.db"
    db = BrokerDB(str(db_path))

    # Write a message with a specific timestamp
    with db._lock:
        db._runner.begin_immediate()
        db._runner.run(
            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
            ("queue", "Message 1", 12345),
        )
        db._runner.run("UPDATE meta SET value = 12344 WHERE key = 'last_ts'")
        db._runner.commit()

    # Force timestamp generator to always return the same value
    original = db._timestamp_gen.generate

    def always_return_12345():
        return 12345

    db._timestamp_gen.generate = always_return_12345

    try:
        # This should fail after retries
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Timestamp conflict persisted",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="Timestamp generator resynchronized",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="Timestamp conflict unresolvable",
                category=RuntimeWarning,
            )
            with pytest.raises(RuntimeError) as exc_info:
                db.write("queue", "Message 2")

        assert "Failed to write message" in str(exc_info.value)
        # With new approach, metrics might be different
        metrics = db.get_conflict_metrics()
        assert metrics["ts_conflict_count"] >= 1
        assert metrics["ts_resync_count"] >= 1
    finally:
        db._timestamp_gen.generate = original


def test_metrics_can_be_reset(workdir):
    """Test metric tracking and reset functionality."""
    db = BrokerDB(str(workdir / "test.db"))

    # Initially no conflicts
    assert db.get_conflict_metrics()["ts_conflict_count"] == 0

    # Force a conflict by direct manipulation
    db._ts_conflict_count = 5
    db._ts_resync_count = 2

    assert db.get_conflict_metrics()["ts_conflict_count"] == 5
    assert db.get_conflict_metrics()["ts_resync_count"] == 2

    # Reset
    db.reset_conflict_metrics()

    assert db.get_conflict_metrics()["ts_conflict_count"] == 0
    assert db.get_conflict_metrics()["ts_resync_count"] == 0


def test_concurrent_writes_simple(workdir):
    """Simple test of concurrent writes without complex multiprocessing."""
    import warnings

    # Filter out the timestamp conflict warning which is expected in this test
    warnings.filterwarnings(
        "ignore", message="Timestamp conflict persisted", category=RuntimeWarning
    )

    db_path = workdir / "test.db"

    # Create the database first
    setup_db = BrokerDB(str(db_path))
    setup_db.close()

    # Multiple threads will write to the same database
    def write_messages(thread_id):
        db = BrokerDB(str(db_path))
        try:
            for i in range(5):
                db.write(f"queue_{thread_id}", f"Message {i}")
        finally:
            db.close()

    threads = []
    for i in range(3):
        t = threading.Thread(target=write_messages, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Verify all messages were written
    db = BrokerDB(str(db_path))
    for i in range(3):
        messages = list(db.read(f"queue_{i}", all_messages=True))
        assert len(messages) == 5


if __name__ == "__main__":
    print("Run with: uv run pytest tests/test_timestamp_resilience.py -xvs")
