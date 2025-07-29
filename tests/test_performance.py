"""Performance tests for SimpleBroker.

All performance-sensitive tests are collected here and run serially
to ensure accurate timing measurements without interference from
parallel test execution.
"""

import sqlite3
import sys
import time
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB, _validate_queue_name_cached
from simplebroker.watcher import QueueMoveWatcher

from .conftest import run_cli
from .performance_calibration import get_machine_performance_ratio

# Mark all tests in this module to run serially
pytestmark = pytest.mark.xdist_group(name="performance_serial")

# ============================================================================
# Test Configuration Constants
# ============================================================================

# Performance test parameters
BASIC_WRITE_COUNT = 50
VALIDATION_ITERATIONS = 5000
BULK_MOVE_MESSAGE_COUNT = 5000
LARGE_BATCH_CLAIM_COUNT = 5000
LARGE_BATCH_READ_LIMIT = 100
SMALL_BATCH_COUNT = 500
SMALL_BATCH_READ_LIMIT = 250
SINCE_LARGE_QUEUE_COUNT = 5000
SINCE_BATCH_SIZE = 100
TIMESTAMP_LOOKUP_COUNT = 1000
CONCURRENT_OPS_BASE_COUNT = 100
CONCURRENT_OPS_COUNT = 20
MOVE_LARGE_BATCH_COUNT = 1000
CLAIM_PERF_MESSAGE_COUNT = 1000
VACUUM_LARGE_MESSAGE_COUNT = 10000
VACUUM_BATCH_WRITE_SIZE = 100
WRITE_PERF_MESSAGE_COUNT = 1000
MOVE_WATCHER_MESSAGE_COUNT = 100

# Performance buffer percentage (33% as requested)
PERF_BUFFER_PERCENT = 0.33

# Machine performance ratio (calculated once per test session)
# 1.0 = same as baseline machine, 0.5 = half as fast, 2.0 = twice as fast
CURRENT_MACHINE_PERFORMANCE = None  # Lazy-loaded
MACHINE_PERFORMANCE_RATIO = 1.0  # Default to baseline performance

# Baseline performance times measured on Apple M2 Air
# These are the actual measured times for each test scenario
BASELINE_TIMES = {
    "basic_write_50": 0.015,  # Writing 50 messages
    "validation_cached": 0.001,  # 1000 cached validations (rounded up from 0.000)
    "bulk_move_5k": 5.0,  # Estimated: 5x the 1k move time
    "large_batch_claim_rollback": 0.025,  # Reading 100 of 5000 messages
    "at_least_once_rollback": 0.004,  # Reading 250 of 500 messages
    "since_query_2000_msgs": 1.0,  # Estimated based on query performance
    "timestamp_lookup": 0.1,  # Estimated based on index lookup
    "concurrent_mixed_ops": 2.0,  # Estimated for mixed operations
    "move_1k_messages": 0.093,  # Moving 1000 messages individually
    "claim_1k_messages": 0.056,  # Claiming 1000 messages
    "vacuum_10k_messages": 3.0,  # Estimated based on batch operations
    "write_1k_messages": 0.719,  # Writing 1000 messages
    "move_watcher_100": 2.0,  # Estimated for watcher operations
}

# Minimum performance thresholds (messages per second)
MIN_BULK_MOVE_RATE = 500  # messages/second
MIN_SINCE_QUERY_RATE = 2000  # messages/second


def get_timeout(baseline_key: str, platform_specific: bool = True) -> float:
    """Get timeout for a test based on baseline time and buffer.

    Args:
        baseline_key: Key in BASELINE_TIMES dict
        platform_specific: Whether to apply platform-specific adjustments

    Returns:
        Timeout in seconds
    """
    global CURRENT_MACHINE_PERFORMANCE

    # Lazy-load machine performance ratio
    if CURRENT_MACHINE_PERFORMANCE is None:
        CURRENT_MACHINE_PERFORMANCE = get_machine_performance_ratio()

    base_time = BASELINE_TIMES[baseline_key]

    # Adjust for machine performance
    # If machine is slower (ratio < 1.0), allow more time
    # If machine is faster (ratio > 1.0), require less time
    timeout = base_time / CURRENT_MACHINE_PERFORMANCE * (1 + PERF_BUFFER_PERCENT)

    # Platform-specific adjustments
    if platform_specific and sys.platform == "win32":
        # Windows filesystem operations are typically slower
        timeout *= 1.5

    return timeout


# ============================================================================
# Timestamp Performance Tests
# ============================================================================


@pytest.mark.slow
def test_timestamp_performance_basic(workdir):
    """Basic performance check - not excessive writes."""
    db_path = workdir / "test.db"

    # Use context manager for proper cleanup
    with BrokerDB(str(db_path)) as db:
        # Time BASIC_WRITE_COUNT writes
        start = time.time()
        for i in range(BASIC_WRITE_COUNT):
            db.write("perf_test", f"Message {i}")
        elapsed = time.time() - start

        # Should complete within timeout
        timeout = get_timeout("basic_write_50")
        assert elapsed < timeout, (
            f"Writing {BASIC_WRITE_COUNT} messages took {elapsed:.3f}s, "
            f"expected < {timeout:.3f}s"
        )

        # No conflicts should have occurred
        assert db.get_conflict_metrics()["ts_conflict_count"] == 0


# ============================================================================
# Queue Validation Performance Tests
# ============================================================================


@pytest.mark.slow
def test_queue_validation_performance():
    """Test that cached validation is faster than uncached."""
    # Clear the cache
    _validate_queue_name_cached.cache_clear()

    # Time first validation (cache miss)
    start = time.perf_counter()
    for _ in range(VALIDATION_ITERATIONS):
        _validate_queue_name_cached("test_queue_performance")
    cached_time = time.perf_counter() - start

    # Should have 1 miss and VALIDATION_ITERATIONS-1 hits
    info = _validate_queue_name_cached.cache_info()
    assert info.misses == 1
    assert info.hits == VALIDATION_ITERATIONS - 1

    # Clear cache and time without caching benefit
    _validate_queue_name_cached.cache_clear()

    # Simulate uncached by using different queue names
    start = time.perf_counter()
    for i in range(VALIDATION_ITERATIONS):
        _validate_queue_name_cached(f"test_queue_{i}")
    uncached_time = time.perf_counter() - start

    # Cached should be significantly faster
    # Even on fast machines, regex matching VALIDATION_ITERATIONS times should be measurably slower
    assert cached_time < uncached_time, (
        f"Cached: {cached_time:.3f}s, Uncached: {uncached_time:.3f}s"
    )

    # Also check that cached time is within expected bounds
    timeout = get_timeout("validation_cached", platform_specific=False)
    assert cached_time < timeout, (
        f"Cached validation took {cached_time:.3f}s, expected < {timeout:.3f}s"
    )


# ============================================================================
# CLI Move Performance Tests
# ============================================================================


@pytest.mark.slow
def test_bulk_move_performance_5k_messages(workdir):
    """Test bulk move with BULK_MOVE_MESSAGE_COUNT messages maintains good performance."""
    # Write messages
    num_messages = BULK_MOVE_MESSAGE_COUNT

    for i in range(num_messages):
        run_cli("write", "perf_source", f"perfmsg{i:04d}", cwd=workdir)

    # Bulk move all messages
    start_move = time.time()
    rc, out, _ = run_cli("move", "perf_source", "perf_dest", "--all", cwd=workdir)
    move_time = time.time() - start_move

    assert rc == 0
    lines = out.strip().split("\n")
    assert len(lines) == num_messages

    # Performance check: should move > MIN_BULK_MOVE_RATE messages/second
    messages_per_second = num_messages / move_time
    assert messages_per_second > MIN_BULK_MOVE_RATE, (
        f"Move performance too slow: {messages_per_second:.0f} msg/s "
        f"(took {move_time:.2f}s for {num_messages} messages)"
    )

    # Also check against timeout
    timeout = get_timeout("bulk_move_5k")
    assert move_time < timeout, (
        f"Bulk move took {move_time:.3f}s, expected < {timeout:.3f}s"
    )

    # Verify messages are in destination
    rc, out, _ = run_cli("peek", "perf_dest", "--all", cwd=workdir)
    assert rc == 0
    lines = out.strip().split("\n")
    assert len(lines) == num_messages


# ============================================================================
# Edge Cases Performance Tests
# ============================================================================


@pytest.mark.slow
def test_large_batch_claim_rollback_performance(workdir: Path):
    """Test that rollback is fast when a large batch claim is interrupted."""
    db_path = workdir / "test.db"

    # Write many messages
    message_count = LARGE_BATCH_CLAIM_COUNT
    with BrokerDB(str(db_path)) as db:
        for i in range(message_count):
            db.write("test_queue", f"message{i:04d}")

    # Test rollback by simulating generator not being fully consumed
    # This tests the exactly-once vs at-least-once delivery semantics

    # First test with commit_interval=1 (exactly-once)
    messages_read = []
    start_time = time.time()

    with BrokerDB(str(db_path)) as db:
        # Read only first LARGE_BATCH_READ_LIMIT messages then stop (simulating crash/interrupt)
        for i, msg in enumerate(
            db.stream_read(
                "test_queue",
                peek=False,
                all_messages=True,
                commit_interval=1,  # exactly-once semantics
            )
        ):
            messages_read.append(msg)
            if i >= LARGE_BATCH_READ_LIMIT - 1:  # Read LARGE_BATCH_READ_LIMIT messages
                break
        # Generator not fully consumed - rollback should happen

    elapsed = time.time() - start_time
    timeout = get_timeout("large_batch_claim_rollback")
    assert elapsed < timeout, (
        f"Reading {LARGE_BATCH_READ_LIMIT} messages took too long: {elapsed:.2f}s (timeout: {timeout}s)"
    )

    # Verify only LARGE_BATCH_READ_LIMIT messages were consumed
    assert len(messages_read) == LARGE_BATCH_READ_LIMIT

    # Read remaining messages (should be message_count - LARGE_BATCH_READ_LIMIT)
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.read("test_queue", all_messages=True))
    assert len(remaining) == message_count - LARGE_BATCH_READ_LIMIT

    # Now test at-least-once semantics (commit_interval > 1)
    # Write fresh messages
    with BrokerDB(str(db_path)) as db:
        for i in range(SMALL_BATCH_COUNT):
            db.write("test_queue2", f"batch{i:03d}")

    messages_read = []
    start_time = time.time()

    with BrokerDB(str(db_path)) as db:
        # Read messages but simulate crash after reading some
        for i, msg in enumerate(
            db.stream_read(
                "test_queue2",
                peek=False,
                all_messages=True,
                commit_interval=100,  # at-least-once semantics
            )
        ):
            messages_read.append(msg)
            if i >= SMALL_BATCH_READ_LIMIT - 1:  # Read SMALL_BATCH_READ_LIMIT messages
                break
        # Generator not fully consumed - partial rollback should happen

    elapsed = time.time() - start_time
    timeout = get_timeout("at_least_once_rollback")
    assert elapsed < timeout, (
        f"Reading {SMALL_BATCH_COUNT} messages took too long: {elapsed:.2f}s (timeout: {timeout}s)"
    )

    # Due to at-least-once semantics, some messages may be re-delivered
    # Only first 200 messages (2 complete batches) should be committed
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.read("test_queue2", all_messages=True))
    # Should have 300 messages remaining (500 - 200 committed)
    assert len(remaining) == SMALL_BATCH_COUNT - 200


# ============================================================================
# Since Flag Performance Tests
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
            int(lines[message_count - 100].split("\t")[0]),
            99,
        ),  # Last 99 messages (100th message's timestamp)
        (
            int(lines[message_count - 1].split("\t")[0]),
            0,
        ),  # No messages (last message's timestamp)
    ]

    for since_ts, expected_count in test_points:
        # Time the query
        start = time.time()
        rc, out, _ = run_cli(
            "peek", queue_name, "--all", "--since", str(since_ts), cwd=workdir
        )
        elapsed = time.time() - start

        assert rc == 0
        if expected_count > 0:
            result_lines = out.strip().split("\n")
            assert len(result_lines) == expected_count, (
                f"Expected {expected_count} messages for since={since_ts}, got {len(result_lines)}"
            )
        else:
            assert out == ""  # But return no messages

        # Performance assertion: should achieve at least 2000 messages/second
        # Add 50ms overhead for process spawn and setup
        if expected_count > 0:
            max_allowed_time = (expected_count / 2000.0) + 0.05
            assert elapsed < max_allowed_time, (
                f"Query took {elapsed:.3f}s for {expected_count} messages, "
                f"expected < {max_allowed_time:.3f}s (2000+ msg/sec)"
            )


# ============================================================================
# Message by Timestamp Performance Tests
# ============================================================================


@pytest.mark.slow
def test_timestamp_lookup_performance(workdir: Path):
    """Test that timestamp lookups are efficient even with many messages."""

    # Create queue with many messages
    num_messages = 1000
    for i in range(num_messages):
        run_cli("write", "perf_queue", f"msg_{i}", cwd=workdir)

    # Get middle message timestamp
    rc, out, err = run_cli("peek", "perf_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    middle_ts = lines[num_messages // 2].split("\t")[0]

    # Time timestamp-based read
    start = time.time()
    rc, out, err = run_cli("read", "perf_queue", "-m", middle_ts, cwd=workdir)
    ts_read_time = time.time() - start
    assert rc == 0

    # Time normal FIFO read
    start = time.time()
    rc, out, err = run_cli("read", "perf_queue", cwd=workdir)
    fifo_read_time = time.time() - start
    assert rc == 0

    # Timestamp read should be comparable to FIFO read (within 2x)
    assert ts_read_time < fifo_read_time * 2


@pytest.mark.slow
def test_concurrent_mixed_operations_performance(workdir: Path):
    """Test performance of mixed read/write/peek operations.

    Note: Some reads exit with EXIT_QUEUE_EMPTY as messages get consumed,
    which is expected."""
    # Write many messages
    for i in range(100):
        run_cli("write", "test_queue", f"message{i}", cwd=workdir)

    # Get some timestamps
    rc, out, err = run_cli("peek", "test_queue", "--all", "-t", cwd=workdir)
    lines = out.strip().split("\n")
    timestamps = [line.split("\t")[0] for line in lines[:10]]

    # Perform mixed operations concurrently
    operations = []
    start = time.time()

    # Mix of different operations
    for i in range(20):
        if i % 4 == 0:
            # Timestamp read
            operations.append(
                run_cli("read", "test_queue", "-m", timestamps[i % 10], cwd=workdir)
            )
        elif i % 4 == 1:
            # Normal read
            operations.append(run_cli("read", "test_queue", cwd=workdir))
        elif i % 4 == 2:
            # Write
            operations.append(
                run_cli("write", "test_queue", f"new_message_{i}", cwd=workdir)
            )
        else:
            # Peek with timestamp
            operations.append(
                run_cli("peek", "test_queue", "-m", timestamps[i % 10], cwd=workdir)
            )

    elapsed = time.time() - start

    # Should complete reasonably quickly (under 2 seconds for this workload)
    assert elapsed < get_timeout("concurrent_mixed_ops")

    # Verify operations succeeded (allowing EXIT_QUEUE_EMPTY for reads)
    success_count = 0
    empty_count = 0
    for rc, _out, err in operations:
        if rc == 0:
            success_count += 1
        elif rc == 2:  # EXIT_QUEUE_EMPTY - expected for some reads
            empty_count += 1
        else:
            raise AssertionError(f"Unexpected error (rc={rc}): {err}")

    # At least some operations should succeed
    assert success_count > 0, "No operations succeeded"
    # Writes (5 total) should always succeed
    assert success_count >= 5, f"Too few successful operations: {success_count}"


# ============================================================================
# Move Claim Patterns Performance Tests
# ============================================================================


@pytest.mark.slow
@pytest.mark.skipif(
    sys.platform == "win32" and sys.version_info[:2] in ((3, 8), (3, 9)),
    reason="Older Python performance on Windows is not guaranteed",
)
def test_move_performance_with_large_batches(workdir: Path):
    """Test move performance with large number of messages."""
    db_path = workdir / "test.db"

    # Write a large batch of messages
    message_count = 1000
    with BrokerDB(str(db_path)) as db:
        for i in range(message_count):
            db.write("perf_source", f"msg{i:04d}")

    # Time moving all messages
    start_time = time.time()
    moved_count = 0

    with BrokerDB(str(db_path)) as db:
        while True:
            result = db.move("perf_source", "perf_dest")
            if result is None:
                break
            moved_count += 1

    move_time = time.time() - start_time

    assert moved_count == message_count

    # Performance assertion - moves should be fast
    # Windows filesystem operations are slower, so we allow more time
    timeout = 6.0 if sys.platform == "win32" else 2.0
    assert move_time < timeout, f"Moving {message_count} messages took {move_time:.2f}s"

    # Verify all messages are in destination
    with BrokerDB(str(db_path)) as db:
        dest_messages = list(db.read("perf_dest", all_messages=True))
    assert len(dest_messages) == message_count


# ============================================================================
# Message Claim Performance Tests
# ============================================================================


@pytest.mark.slow
@pytest.mark.skipif(
    sys.platform == "win32" and sys.version_info[:2] in ((3, 8), (3, 9)),
    reason="Older Python performance on Windows is not guaranteed",
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


@pytest.mark.slow
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


@pytest.mark.slow
@pytest.mark.skipif(
    sys.platform == "win32" and sys.version_info[:2] in ((3, 8), (3, 9)),
    reason="Older Python performance on Windows is not guaranteed",
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

    # Use calibrated timeout system
    timeout = get_timeout("write_1k_messages")
    assert write_time < timeout, (
        f"Writing {message_count} messages took {write_time:.2f}s, "
        f"expected < {timeout:.2f}s"
    )

    # Verify messages were written correctly
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages WHERE queue = 'write_perf_queue'")
    assert cursor.fetchone()[0] == message_count

    # All should be unclaimed
    cursor.execute(
        "SELECT COUNT(*) FROM messages WHERE queue = 'write_perf_queue' AND claimed = 0"
    )
    assert cursor.fetchone()[0] == message_count
    conn.close()


# ============================================================================
# Queue Move Watcher Performance Tests
# ============================================================================


@pytest.fixture
def broker(tmp_path):
    """Create a broker instance for testing."""
    db_path = tmp_path / "test.db"
    return BrokerDB(str(db_path))


@pytest.mark.slow
def test_large_volume_move(broker, tmp_path):
    """Test moving a large number of messages."""
    num_messages = 100

    # Add many messages
    for i in range(num_messages):
        broker.write("source", f"msg_{i:04d}")

    moved_count = 0

    def count_handler(body: str, ts: int):
        nonlocal moved_count
        moved_count += 1

    start_time = time.time()

    watcher = QueueMoveWatcher(
        broker=broker,
        source_queue="source",
        dest_queue="dest",
        handler=count_handler,
        max_interval=0.001,  # Fast polling for performance test
        max_messages=num_messages,  # Stop after moving all messages
    )

    # Run in thread with timeout to prevent hanging
    thread = watcher.run_in_thread()

    # Wait for completion with timeout
    thread.join(timeout=10.0)  # 10 second timeout

    # Stop watcher if still running
    if thread.is_alive():
        watcher.stop()
        thread.join(timeout=2.0)
        if thread.is_alive():
            pytest.fail(
                f"QueueMoveWatcher did not stop within timeout. Moved {watcher.move_count}/{num_messages} messages"
            )

    elapsed = time.time() - start_time

    # Verify all moved
    assert watcher.move_count == num_messages
    assert moved_count == num_messages

    # Verify destination has all messages
    dest_messages = list(broker.read("dest", all_messages=True))
    assert len(dest_messages) == num_messages

    # Source should be empty
    source_messages = list(broker.read("source", all_messages=True))
    assert len(source_messages) == 0

    # Performance check (should be reasonably fast)
    assert elapsed < 5.0, f"Move took {elapsed:.2f}s, expected < 5s"
