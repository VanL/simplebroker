"""Test suite for QueueWatcher race conditions and concurrency.

Tests to ensure the pre-check optimization doesn't introduce race conditions
or message loss in concurrent scenarios.
"""

from __future__ import annotations

import concurrent.futures
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher


class ConcurrencyTestWatcher(QueueWatcher):
    """Watcher with hooks for testing concurrent behavior."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._pre_check_enabled = True
        self._pre_check_delay = 0
        self._drain_delay = 0
        self._dispatch_delay = 0
        self.pre_check_count = 0
        self.drain_count = 0
        self.dispatch_count = 0
        self._lock = threading.Lock()

    def _has_pending_messages(self, db: BrokerDB) -> bool:
        """Add instrumentation to pre-check."""
        with self._lock:
            self.pre_check_count += 1

        if self._pre_check_delay > 0:
            time.sleep(self._pre_check_delay)

        sql = "SELECT EXISTS(SELECT 1 FROM messages WHERE queue = ? AND claimed = 0 LIMIT 1)"
        rows = list(db._runner.run(sql, (self._queue,), fetch=True))
        return bool(rows[0][0]) if rows else False

    def _drain_queue(self) -> None:
        """Add instrumentation and optional pre-check."""
        with self._lock:
            self.drain_count += 1

        if self._drain_delay > 0:
            time.sleep(self._drain_delay)

        if self._pre_check_enabled:
            db = self._get_db()
            if not self._has_pending_messages(db):
                return

        super()._drain_queue()

    def _dispatch(self, message: str, timestamp: int) -> None:
        """Add instrumentation to dispatch."""
        with self._lock:
            self.dispatch_count += 1

        if self._dispatch_delay > 0:
            time.sleep(self._dispatch_delay)

        super()._dispatch(message, timestamp)


def test_pre_check_race_no_message_loss() -> None:
    """Verify no messages are lost due to pre-check race conditions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        # Shared state for tracking
        processed_messages = []
        lock = threading.Lock()

        def handler(msg, ts) -> None:
            with lock:
                processed_messages.append((msg, ts))

        # Create multiple watchers on the same queue
        num_watchers = 5
        watchers = []
        for _ in range(num_watchers):
            w = ConcurrencyTestWatcher(db_path, "shared_queue", handler)
            watchers.append(w)
            w.run_in_thread()

        time.sleep(0.2)

        # Rapidly add messages while watchers are running
        expected_messages = []
        for i in range(100):
            msg = f"message_{i}"
            expected_messages.append(msg)
            broker.write("shared_queue", msg)
            if i % 10 == 0:
                time.sleep(0.01)  # Small breaks to vary timing

        # Wait for all messages to be processed
        time.sleep(2.0)

        # Stop all watchers
        for w in watchers:
            w.stop()

        # Verify all messages were processed exactly once
        processed_bodies = [msg for msg, ts in processed_messages]
        assert sorted(processed_bodies) == sorted(expected_messages)
        assert len(processed_bodies) == len(expected_messages)

        # Check for duplicates
        msg_counts = Counter(processed_bodies)
        for msg, count in msg_counts.items():
            assert count == 1, f"Message {msg} processed {count} times"


def test_concurrent_writers_readers() -> None:
    """Test concurrent writing and reading with pre-check."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        processed = []
        processed_lock = threading.Lock()

        def handler(msg, ts) -> None:
            with processed_lock:
                processed.append(msg)

        # Create watcher
        watcher = ConcurrencyTestWatcher(db_path, "test_queue", handler)
        watcher.run_in_thread()

        # Function to write messages
        def writer_task(writer_id, count) -> None:
            for i in range(count):
                broker.write("test_queue", f"writer_{writer_id}_msg_{i}")
                time.sleep(0.001)  # Small delay between writes

        # Launch concurrent writers
        num_writers = 5
        messages_per_writer = 20

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_writers) as executor:
            futures = []
            for i in range(num_writers):
                future = executor.submit(writer_task, i, messages_per_writer)
                futures.append(future)

            # Wait for all writers to complete
            concurrent.futures.wait(futures)

        # Wait for processing
        time.sleep(1.0)
        watcher.stop()

        # Verify all messages were processed
        expected_total = num_writers * messages_per_writer
        assert len(processed) == expected_total

        # Verify no duplicates
        assert len(set(processed)) == expected_total


def test_pre_check_drain_race() -> None:
    """Test race between pre-check and actual drain."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        processed = []

        def handler(msg, ts) -> None:
            processed.append(msg)

        # Create watcher with delay to increase race window
        watcher = ConcurrencyTestWatcher(db_path, "test_queue", handler)
        watcher._pre_check_delay = 0.01  # 10ms delay after pre-check
        watcher.run_in_thread()

        # Function to consume messages from another connection
        def consume_messages():
            # Use a separate broker instance
            other_broker = BrokerDB(db_path)
            consumed = []
            for msg in other_broker.stream_read("test_queue", all_messages=True):
                consumed.append(msg)
            return consumed

        # Add messages
        for i in range(50):
            broker.write("test_queue", f"message_{i}")

        # Start consuming from another thread during watcher operation
        consume_future = concurrent.futures.ThreadPoolExecutor().submit(
            consume_messages,
        )

        # Let watcher run
        time.sleep(1.0)
        watcher.stop()

        # Get messages consumed by other thread
        other_consumed = consume_future.result()

        # Total messages processed should equal messages written
        total_processed = len(processed) + len(other_consumed)
        assert total_processed == 50

        # Verify no message was processed twice
        all_messages = processed + other_consumed
        assert len(set(all_messages)) == total_processed


def test_multiple_queues_concurrent_activity() -> None:
    """Test multiple queues with concurrent activity."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        num_queues = 10
        messages_per_queue = 50

        # Track processed messages per queue
        processed_by_queue: dict[str, list[str]] = {}
        queue_locks: dict[str, threading.Lock] = {}

        for i in range(num_queues):
            queue = f"queue_{i}"
            processed_by_queue[queue] = []
            queue_locks[queue] = threading.Lock()

        # Create watchers
        watchers = []
        for i in range(num_queues):
            queue = f"queue_{i}"

            def make_handler(q):
                def handler(msg, ts) -> None:
                    with queue_locks[q]:
                        processed_by_queue[q].append(msg)

                return handler

            w = ConcurrencyTestWatcher(db_path, queue, make_handler(queue))
            watchers.append(w)
            w.run_in_thread()

        # Concurrent writer function
        def write_to_queue(queue_name, start_idx) -> None:
            for i in range(messages_per_queue):
                broker.write(queue_name, f"{queue_name}_msg_{start_idx + i}")
                if i % 10 == 0:
                    time.sleep(0.001)

        # Launch concurrent writes to all queues
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_queues) as executor:
            futures = []
            for i in range(num_queues):
                queue = f"queue_{i}"
                future = executor.submit(write_to_queue, queue, i * 1000)
                futures.append(future)

            concurrent.futures.wait(futures)

        # Wait for processing
        time.sleep(2.0)

        # Stop all watchers
        for w in watchers:
            w.stop()

        # Verify each queue processed its messages
        for i in range(num_queues):
            queue = f"queue_{i}"
            messages = processed_by_queue[queue]
            assert len(messages) == messages_per_queue

            # Verify correct messages for this queue
            for msg in messages:
                assert msg.startswith(f"{queue}_msg_")


def test_watcher_stop_during_pre_check() -> None:
    """Test stopping watcher during pre-check doesn't hang."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        processed = []

        def handler(msg, ts) -> None:
            processed.append(msg)

        # Create watcher with long pre-check delay
        watcher = ConcurrencyTestWatcher(db_path, "test_queue", handler)
        watcher._pre_check_delay = 0.5  # 500ms delay

        # Add messages
        for i in range(10):
            broker.write("test_queue", f"message_{i}")

        # Start watcher
        thread = watcher.run_in_thread()

        # Wait a bit then stop during pre-check
        time.sleep(0.1)
        start_stop = time.time()
        watcher.stop(timeout=1.0)
        stop_duration = time.time() - start_stop

        # Should stop quickly despite pre-check delay
        assert stop_duration < 1.5
        assert not thread.is_alive()


def test_pre_check_with_peek_mode() -> None:
    """Test pre-check behavior with peek mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        peek_count = 0
        peek_lock = threading.Lock()

        def handler(msg, ts) -> None:
            nonlocal peek_count
            with peek_lock:
                peek_count += 1

        # Create peek watcher
        watcher = ConcurrencyTestWatcher(
            db_path,
            "test_queue",
            handler,
            peek=True,
            max_interval=0.01,  # Fast polling
        )
        watcher.run_in_thread()

        # Add a message
        broker.write("test_queue", "test_message")

        # Let it run for a bit
        time.sleep(0.5)

        # In peek mode with timestamp tracking, we should see exactly one peek
        # The watcher updates its timestamp after successful dispatch to avoid
        # reprocessing the same message
        with peek_lock:
            assert peek_count == 1  # Should peek exactly once due to timestamp tracking

        # Message should still be in queue
        messages = list(broker.read("test_queue", all_messages=True))
        assert len(messages) == 1
        assert messages[0] == "test_message"

        watcher.stop()


def test_concurrent_pre_checks() -> None:
    """Test multiple watchers doing pre-checks simultaneously."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        # Track pre-check timing
        pre_check_times: list[float] = []
        times_lock = threading.Lock()

        class TimingWatcher(ConcurrencyTestWatcher):
            def _has_pending_messages(self, db):
                start = time.perf_counter()
                result = super()._has_pending_messages(db)
                elapsed = time.perf_counter() - start
                with times_lock:
                    pre_check_times.append(elapsed)
                return result

        # Create many watchers
        num_watchers = 20
        watchers = []

        def handler(msg, ts) -> None:
            pass

        for i in range(num_watchers):
            w = TimingWatcher(db_path, f"queue_{i}", handler)
            watchers.append(w)
            w.run_in_thread()

        # Trigger concurrent pre-checks by writing to one queue
        broker.write("queue_0", "trigger")

        # Wait for pre-checks
        time.sleep(0.5)

        # Stop watchers
        for w in watchers:
            w.stop()

        # Analyze pre-check times
        with times_lock:
            if pre_check_times:
                avg_time = sum(pre_check_times) / len(pre_check_times)
                max_time = max(pre_check_times)

                # Even with many concurrent pre-checks, they should be fast
                assert avg_time < 0.002  # < 2ms average
                assert max_time < 0.02  # < 20ms max (relaxed for CI)


def test_pre_check_database_contention() -> None:
    """Test pre-check performance under database contention."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        broker = BrokerDB(db_path)

        processed_counts = {}

        # Create watchers
        num_watchers = 10
        watchers = []

        for i in range(num_watchers):
            queue = f"queue_{i}"
            processed_counts[queue] = 0

            def make_handler(q):
                def handler(msg, ts) -> None:
                    processed_counts[q] += 1

                return handler

            w = ConcurrencyTestWatcher(db_path, queue, make_handler(queue))
            watchers.append(w)
            w.run_in_thread()

        # Function to create write contention
        def create_contention() -> None:
            for _ in range(100):
                # Write to random queues
                import random

                queue_idx = random.randint(0, num_watchers - 1)
                broker.write(f"queue_{queue_idx}", "contention_message")
                time.sleep(0.001)

        # Run contention in background
        contention_thread = threading.Thread(target=create_contention)
        contention_thread.start()

        # Let it run
        contention_thread.join()
        time.sleep(1.0)

        # Stop watchers
        for w in watchers:
            w.stop()

        # Verify all watchers still functioned under contention
        total_processed = sum(processed_counts.values())
        assert total_processed == 100  # All messages should be processed

        # Check pre-check efficiency
        total_pre_checks = sum(w.pre_check_count for w in watchers)
        total_drains = sum(w.drain_count for w in watchers)

        # Pre-checks should prevent most empty drains
        assert total_pre_checks > total_drains


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
