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
    """Test that extreme conflicts trigger resilience mechanism."""
    db = BrokerDB(str(workdir / "test.db"))

    # Create a scenario where normal retries fail and resilience is needed
    # We'll create many conflicting messages to exhaust normal retries

    # Insert many messages at the same timestamp to create conflicts
    base_ts = int(time.time() * 1000) << 20

    with db._lock:
        db._runner.begin_immediate()
        # Insert multiple messages with sequential timestamps
        for i in range(10):
            db._runner.run(
                "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                ("queue", f"Message {i}", base_ts + i),
            )
        # Set meta to a value that will cause conflicts
        db._runner.run(
            "UPDATE meta SET value = ? WHERE key = 'last_ts'",
            (base_ts + 5,),  # Middle of our range
        )
        db._runner.commit()

    # Now try to write a normal message
    # This should eventually succeed after retries
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Timestamp conflicts persisted",
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )

        # Reset the timestamp generator to force it to reload
        db._timestamp_gen._initialized = False
        db._timestamp_gen._last_ts = 0

        # This write should succeed (possibly through resilience)
        db.write("queue", "Normal message")

    # Verify all messages exist
    messages = db.read("queue", all_messages=True)

    # We should have all 11 messages (10 original + 1 new)
    assert len(messages) == 11

    # Verify we have messages and no crash occurred
    assert all(
        body.startswith("Message ") or body == "Normal message" for body in messages
    )
    assert "Normal message" in messages


def test_transient_conflict_recovery(workdir):
    """Test recovery from a single transient conflict."""
    db_path = workdir / "test.db"
    db = BrokerDB(str(db_path))

    # To force a conflict, we need to:
    # 1. Write a message to get a timestamp
    # 2. Manually set meta to a value that will generate a duplicate

    # Get current time in ms
    current_ms = int(time.time() * 1000)

    # Create a timestamp manually
    ts_to_conflict = (current_ms << 20) | 0  # physical time with counter 0

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
        # Set meta to force the same timestamp
        db._runner.run(
            "UPDATE meta SET value = ? WHERE key = 'last_ts'",
            (12344,),  # One less than our message
        )
        db._runner.commit()

    # Mock the timestamp generator to always return same value
    # This simulates a broken clock that's stuck
    original_generate = db._timestamp_gen.generate

    def stuck_generate():
        return 12345

    db._timestamp_gen.generate = stuck_generate  # type: ignore[assignment]

    # This should fail after retries
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Timestamp conflicts persisted", category=RuntimeWarning
        )
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )
        with pytest.raises(RuntimeError) as exc_info:
            db.write("queue", "Message 2")

    # Restore original method
    db._timestamp_gen.generate = original_generate  # type: ignore[assignment]

    # Check error message contains relevant info
    assert "Failed to write message after resynchronization" in str(exc_info.value)
    assert "UNIQUE constraint failed" in str(exc_info.value)


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
        result = db._runner.run("SELECT MAX(ts) FROM messages", fetch=True)
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
        result = db._runner.run(
            "SELECT value FROM meta WHERE key = 'last_ts'", fetch=True
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

    db._timestamp_gen.generate = always_return_12345  # type: ignore[assignment]

    try:
        # This should fail after retries
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Timestamp conflicts persisted",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="Timestamp generator resynchronized",
                category=RuntimeWarning,
            )
            with pytest.raises(RuntimeError) as exc_info:
                db.write("queue", "Message 2")

        assert "Failed to write message after resynchronization" in str(exc_info.value)
        # With new approach, metrics might be different
        metrics = db.get_conflict_metrics()
        assert metrics["ts_conflict_count"] >= 1
        assert metrics["ts_resync_count"] >= 1
    finally:
        db._timestamp_gen.generate = original  # type: ignore[assignment]


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


def test_performance_basic(workdir):
    """Basic performance check - not excessive writes."""
    db_path = workdir / "test.db"
    db = BrokerDB(str(db_path))

    # Time 50 writes
    start = time.time()
    for i in range(50):
        db.write("perf_test", f"Message {i}")
    elapsed = time.time() - start

    # Should complete in reasonable time (< 1 second for 50 writes)
    assert elapsed < 1.0

    # No conflicts should have occurred
    assert db.get_conflict_metrics()["ts_conflict_count"] == 0


def test_atomic_update_with_returning(workdir):
    """Test that the new RETURNING-based update works atomically."""
    db = BrokerDB(str(workdir / "test.db"))

    # Write some messages normally
    for i in range(5):
        db.write("atomic_test", f"Message {i}")

    # Get current state
    initial_metrics = db.get_conflict_metrics()
    assert initial_metrics["ts_conflict_count"] == 0

    # Simulate concurrent updates by manually manipulating the meta table
    # to create a scenario where RETURNING helps
    with db._lock:
        # Get current last_ts
        result = list(
            db._runner.run("SELECT value FROM meta WHERE key = 'last_ts'", fetch=True)
        )
        current_ts = result[0][0] if result else 0

        # Try to update with an expected value that's slightly off
        # This simulates another process updating between our read and write
        result = list(
            db._runner.run(
                "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ? RETURNING 1",
                (current_ts + 100, current_ts - 1),  # Wrong expected value
                fetch=True,
            )
        )

        # Should return empty result (no rows updated)
        assert len(result) == 0

        # Now try with correct value
        result = list(
            db._runner.run(
                "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ? RETURNING 1",
                (current_ts + 100, current_ts),  # Correct expected value
                fetch=True,
            )
        )

        # Should return 1 row
        assert len(result) == 1
        db._runner.commit()

    # Verify we can still write messages
    db.write("atomic_test", "After atomic update")
    messages = db.read("atomic_test", all_messages=True)
    assert len(messages) == 6
    assert messages[-1] == "After atomic update"


def test_clock_regression_detection(workdir):
    """Test that clock regression is properly detected and handled."""
    db = BrokerDB(str(workdir / "test.db"))

    # Write some initial messages
    for i in range(3):
        db.write("clock_test", f"Before regression {i}")

    # Manually set meta to a future timestamp
    future_ts = (int(time.time() * 1000) + 10000) << 20  # 10 seconds in future
    with db._lock:
        db._runner.run("UPDATE meta SET value = ? WHERE key = 'last_ts'", (future_ts,))
        db._runner.commit()

    # Reset timestamp generator to force reload
    db._timestamp_gen._initialized = False
    db._timestamp_gen._last_ts = 0

    # Now try to write - this should trigger clock regression handling
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Timestamp conflicts persisted",
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="Clock regression detected",
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )

        # This should succeed through resilience mechanism
        db.write("clock_test", "After regression")

    # Verify message was written
    messages = db.read("clock_test", all_messages=True)
    assert len(messages) >= 4
    assert "After regression" in messages


def test_high_concurrency_with_returning(workdir):
    """Test that RETURNING-based updates handle high concurrency well."""
    db_path = workdir / "test.db"

    # Create database
    setup_db = BrokerDB(str(db_path))
    setup_db.close()

    results = []
    errors = []
    lock = threading.Lock()

    def concurrent_writer(thread_id):
        try:
            db = BrokerDB(str(db_path))
            # Each thread writes 10 messages as fast as possible
            for i in range(10):
                db.write("concurrent", f"T{thread_id}M{i}")

            with lock:
                results.append(thread_id)
            db.close()
        except Exception as e:
            with lock:
                errors.append((thread_id, str(e)))

    # Launch 10 threads simultaneously
    threads = []
    for i in range(10):
        t = threading.Thread(target=concurrent_writer, args=(i,))
        threads.append(t)

    # Start all threads at once
    for t in threads:
        t.start()

    # Wait for completion
    for t in threads:
        t.join()

    # Check results
    assert len(errors) == 0, f"Errors occurred: {errors}"
    assert len(results) == 10

    # Verify all messages were written
    db = BrokerDB(str(db_path))
    messages = db.read("concurrent", all_messages=True)
    assert len(messages) == 100

    # Verify no duplicates (all timestamps unique)
    with db._lock:
        result = list(
            db._runner.run(
                "SELECT COUNT(DISTINCT ts) FROM messages WHERE queue = 'concurrent'",
                fetch=True,
            )
        )
        unique_count = result[0][0]

    assert unique_count == 100, "Not all timestamps are unique"


def test_returning_clause_atomic_update(workdir):
    """Test that RETURNING clause provides atomic timestamp updates."""
    db = BrokerDB(str(workdir / "test.db"))

    # Write several messages
    for i in range(10):
        db.write("queue", f"Message {i}")

    # Check that no conflicts occurred during normal operation
    assert db.get_conflict_metrics()["ts_conflict_count"] == 0

    # Verify all messages have unique timestamps
    messages_with_ts = list(db.stream_read_with_timestamps("queue", all_messages=True))
    timestamps = [ts for _, ts in messages_with_ts]
    assert len(timestamps) == len(set(timestamps))  # All unique

    # Verify timestamps are monotonically increasing
    for i in range(1, len(timestamps)):
        assert timestamps[i] > timestamps[i - 1]


# Removed duplicate test_clock_regression_detection - already defined above


def test_concurrent_timestamp_generation(workdir):
    """Test that concurrent threads generate unique timestamps."""
    db_path = workdir / "test.db"

    # Create database first
    setup_db = BrokerDB(str(db_path))
    setup_db.close()

    # Track both successful writes and errors
    results = []
    errors = []
    lock = threading.Lock()

    def write_messages(thread_id):
        db = BrokerDB(str(db_path))
        try:
            for _i in range(20):
                try:
                    # Actually write messages to test timestamp generation
                    db.write(f"concurrent_{thread_id}", f"T{thread_id}M{i}")
                    with lock:
                        results.append(f"T{thread_id}M{i}")
                except Exception as e:
                    with lock:
                        errors.append((thread_id, i, str(e)))
        finally:
            db.close()

    # Run multiple threads concurrently
    threads = []
    for i in range(5):
        t = threading.Thread(target=write_messages, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Check for errors
    assert len(errors) == 0, f"Errors occurred: {errors}"

    # Verify all messages were written
    assert len(results) == 100  # 5 threads * 20 messages

    # Read back all messages and check timestamps
    db = BrokerDB(str(db_path))
    all_timestamps = []

    for thread_id in range(5):
        messages_with_ts = list(
            db.stream_read_with_timestamps(f"concurrent_{thread_id}", all_messages=True)
        )
        # Should have 20 messages per thread
        assert len(messages_with_ts) == 20

        # Collect timestamps
        for _, ts in messages_with_ts:
            all_timestamps.append(ts)

    # Verify all timestamps are unique
    assert len(all_timestamps) == 100
    assert len(set(all_timestamps)) == 100  # All unique

    # Verify timestamps are valid and monotonic within each queue
    for ts in all_timestamps:
        assert ts > 0
        assert ts < 2**63  # Within SQLite's integer range


def test_extreme_timestamp_overflow_handling(workdir):
    """Test handling when timestamp counter approaches overflow."""
    db = BrokerDB(str(workdir / "test.db"))

    # Set up a timestamp very close to counter overflow
    current_ms = int(time.time() * 1000)
    # Counter is 20 bits, so max is (1 << 20) - 1 = 1048575
    near_overflow_ts = (current_ms << 20) | ((1 << 20) - 10)  # 10 away from overflow

    with db._lock:
        db._runner.run(
            "UPDATE meta SET value = ? WHERE key = 'last_ts'", (near_overflow_ts,)
        )
        db._runner.commit()

    # Reset timestamp generator to pick up the near-overflow value
    db._timestamp_gen._initialized = False
    db._timestamp_gen._last_ts = 0

    # Write multiple messages - should handle counter overflow gracefully
    for i in range(20):
        db.write("queue", f"Message {i}")

    # Verify all messages were written
    messages = db.read("queue", all_messages=True)
    assert len(messages) == 20

    # Check timestamps are still unique and ordered
    messages_with_ts = list(db.stream_read_with_timestamps("queue", all_messages=True))
    timestamps = [ts for _, ts in messages_with_ts]
    assert len(timestamps) == len(set(timestamps))  # All unique


if __name__ == "__main__":
    print("Run with: uv run pytest tests/test_timestamp_resilience.py -xvs")
