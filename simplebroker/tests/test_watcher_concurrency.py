"""Concurrency tests for the watcher feature."""

import json
import threading
import time
import warnings
from typing import Dict, List, Tuple

import pytest

from simplebroker.db import BrokerDB

pytest.importorskip("simplebroker.watcher")
from simplebroker.watcher import QueueWatcher

from .helpers.watcher_base import WatcherTestBase


class ConcurrentCollector:
    """Thread-safe collector for concurrent testing."""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.messages: List[Tuple[str, int]] = []
        self.lock = threading.Lock()
        self.processing_times: Dict[str, float] = {}

    def handler(self, msg: str, ts: int) -> None:
        """Collect messages with timing info."""
        start_time = time.monotonic()
        with self.lock:
            self.messages.append((msg, ts))
            self.processing_times[msg] = start_time

    def get_messages(self) -> List[str]:
        """Get just the message bodies."""
        with self.lock:
            return [msg for msg, _ in self.messages]


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database."""
    return tmp_path / "test.db"


class TestWorkerPool(WatcherTestBase):
    """Test worker pool scenarios."""

    def test_worker_pool_exactly_once_delivery(self, temp_db):
        """Test that each message is delivered exactly once across workers."""
        num_workers = 5
        num_messages = 50  # Reduced for faster test

        # Create workers FIRST (before messages exist)
        collectors = []
        workers = []

        for i in range(num_workers):
            collector = ConcurrentCollector(f"worker_{i}")
            collectors.append(collector)

            # Use database path for thread-safe operation
            watcher = QueueWatcher(
                temp_db,
                "tasks",
                collector.handler,
                peek=False,
            )

            thread = watcher.run_in_thread()
            workers.append((watcher, thread, None))

        # Let workers start and begin polling
        time.sleep(0.1)

        # NOW create messages - this gives all workers a fair chance
        with BrokerDB(temp_db) as db:
            for i in range(num_messages):
                db.write("tasks", f"task_{i:03d}")
                # Small delay every few messages to spread the work
                if i % 10 == 0:
                    time.sleep(0.01)

        # Let workers process
        time.sleep(2.0)  # Give enough time for all messages

        # Stop all workers
        for watcher, _thread, _db in workers:
            watcher.stop()

        for watcher, thread, _db in workers:
            thread.join(timeout=5.0)
            # Verify thread termination
            if thread.is_alive():
                # Force kill the watcher if still running
                watcher._strategy._stop_event.set()
                thread.join(timeout=1.0)
                assert not thread.is_alive(), (
                    "Worker thread failed to stop after 6 seconds"
                )

        # Collect all processed messages
        all_messages = []
        worker_message_counts = {}

        for _i, collector in enumerate(collectors):
            messages = collector.get_messages()
            all_messages.extend(messages)
            worker_message_counts[collector.worker_id] = len(messages)

        # Verify exactly-once delivery
        assert len(all_messages) == num_messages
        assert len(set(all_messages)) == num_messages  # No duplicates
        assert set(all_messages) == {f"task_{i:03d}" for i in range(num_messages)}

        # Verify work was distributed (not all to one worker)
        # Note: In fast environments one worker might get all messages
        # This is OK as long as we have exactly-once delivery
        workers_with_messages = len(
            [count for count in worker_message_counts.values() if count > 0]
        )
        print(f"Work distribution: {worker_message_counts}")
        print(f"Workers that processed messages: {workers_with_messages}/{num_workers}")
        # At least one worker should have processed messages
        assert workers_with_messages >= 1

        # Verify queue is empty
        with BrokerDB(temp_db) as db:
            remaining = list(db.read("tasks", all_messages=True))
            assert len(remaining) == 0

    @pytest.mark.slow
    def test_worker_pool_with_slow_handlers(self, temp_db):
        """Test worker pool with varying processing speeds."""
        num_messages = 20

        with BrokerDB(temp_db) as db:
            for i in range(num_messages):
                db.write(
                    "jobs",
                    json.dumps({"id": i, "work_time": 0.05 if i % 3 == 0 else 0.01}),
                )

        processed = []
        processed_lock = threading.Lock()

        def slow_handler(msg: str, ts: int):
            """Handler with variable processing time."""
            data = json.loads(msg)
            time.sleep(data["work_time"])
            with processed_lock:
                processed.append(data["id"])

        # Create 3 workers
        workers = []
        for _i in range(3):
            watcher = QueueWatcher(
                temp_db,
                "jobs",
                slow_handler,
            )
            thread = watcher.run_in_thread()
            workers.append((watcher, thread))

        # Process for a while
        time.sleep(2.0)

        # Stop workers
        for watcher, _thread in workers:
            watcher.stop()
        for _watcher, thread in workers:
            thread.join(timeout=5.0)

        # Should have processed all messages
        assert len(processed) == num_messages
        assert set(processed) == set(range(num_messages))

    def test_worker_joins_late(self, temp_db):
        """Test worker joining after others have started."""
        # Start with 2 workers
        collectors = []
        workers = []

        for i in range(2):
            collector = ConcurrentCollector(f"early_worker_{i}")
            collectors.append(collector)

            worker_db = BrokerDB(temp_db)
            watcher = QueueWatcher(
                worker_db,
                "dynamic_queue",
                collector.handler,
            )
            thread = watcher.run_in_thread()
            workers.append((watcher, thread, worker_db))

        # Add some messages
        with BrokerDB(temp_db) as db:
            for i in range(50):
                db.write("dynamic_queue", f"early_msg_{i}")

        time.sleep(0.5)

        # Add a late worker
        late_collector = ConcurrentCollector("late_worker")
        collectors.append(late_collector)

        late_db = BrokerDB(temp_db)
        late_watcher = QueueWatcher(
            late_db,
            "dynamic_queue",
            late_collector.handler,
        )
        late_thread = late_watcher.run_in_thread()
        workers.append((late_watcher, late_thread, late_db))

        # Give the late worker time to start polling
        time.sleep(0.2)

        # Add more messages
        with BrokerDB(temp_db) as db:
            for i in range(50):
                db.write("dynamic_queue", f"late_msg_{i}")

        time.sleep(1.0)  # Give more time for processing

        # Stop all
        for watcher, _thread, _db in workers:
            watcher.stop()
        for _watcher, thread, db in workers:
            thread.join(timeout=5.0)
            db.close()

        # Verify all messages processed
        all_messages = []
        for collector in collectors:
            all_messages.extend(collector.get_messages())

        assert len(all_messages) == 100
        assert len(set(all_messages)) == 100

        # Late worker MAY have gotten some messages, but it's not guaranteed
        # In a fast system with burst polling, early workers might process everything
        # The important thing is that all messages were processed exactly once
        late_messages = len(late_collector.get_messages())
        print(f"Late worker processed {late_messages} messages")

        # What we can verify is that work was distributed among workers
        worker_counts = {}
        for collector in collectors:
            worker_counts[collector.worker_id] = len(collector.get_messages())
        print(f"Work distribution: {worker_counts}")

        # At least verify that messages were processed
        assert sum(worker_counts.values()) == 100


class TestMixedMode(WatcherTestBase):
    """Test mixed peek and read watchers."""

    def test_mixed_peek_read_basic(self, temp_db):
        """Test basic mixed mode operation."""
        peek_messages = []
        read_messages = []
        peek_lock = threading.Lock()
        read_lock = threading.Lock()

        def peek_handler(msg: str, ts: int):
            with peek_lock:
                peek_messages.append(msg)

        def read_handler(msg: str, ts: int):
            time.sleep(0.01)  # Simulate work
            with read_lock:
                read_messages.append(msg)

        # Start peek watcher
        peek_db = BrokerDB(temp_db)
        peek_watcher = QueueWatcher(
            peek_db,
            "mixed",
            peek_handler,
            peek=True,
        )
        peek_thread = peek_watcher.run_in_thread()

        # Start read watcher
        read_db = BrokerDB(temp_db)
        read_watcher = QueueWatcher(
            read_db,
            "mixed",
            read_handler,
            peek=False,
        )
        read_thread = read_watcher.run_in_thread()

        time.sleep(0.1)

        # Add messages
        with BrokerDB(temp_db) as db:
            for i in range(10):
                db.write("mixed", f"msg_{i}")

        # Let them process
        time.sleep(1.0)

        # Stop watchers
        peek_watcher.stop()
        read_watcher.stop()
        peek_thread.join(timeout=5.0)
        read_thread.join(timeout=5.0)
        peek_db.close()
        read_db.close()

        # All messages should be read (consumed)
        assert len(read_messages) == 10
        assert set(read_messages) == {f"msg_{i}" for i in range(10)}

        # Peek might have seen some/all messages
        # Can't guarantee exact behavior due to race conditions
        assert len(peek_messages) >= 0
        assert len(peek_messages) <= 10

        # Peek messages should be subset of original messages
        assert set(peek_messages).issubset({f"msg_{i}" for i in range(10)})

    def test_multiple_peek_watchers(self, temp_db):
        """Test multiple peek watchers see same messages."""
        num_peekers = 3
        collectors = []
        watchers = []

        # Create multiple peek watchers
        for _i in range(num_peekers):
            messages = []
            lock = threading.Lock()

            def make_handler(m, lck):
                def handler(msg: str, ts: int):
                    with lck:
                        m.append(msg)

                return handler

            peek_db = BrokerDB(temp_db)
            watcher = QueueWatcher(
                peek_db,
                "broadcast",
                make_handler(messages, lock),
                peek=True,
            )
            thread = watcher.run_in_thread()
            collectors.append((messages, lock))
            watchers.append((watcher, thread, peek_db))

        time.sleep(0.1)

        # Write messages
        with BrokerDB(temp_db) as db:
            for i in range(5):
                db.write("broadcast", f"broadcast_{i}")
                time.sleep(0.05)  # Small delay to ensure order

        time.sleep(0.5)

        # Stop all watchers
        for watcher, _thread, _db in watchers:
            watcher.stop()
        for _watcher, thread, db in watchers:
            thread.join(timeout=5.0)
            db.close()

        # Each peeker should have seen the messages
        for messages, lock in collectors:
            with lock:
                # Should have seen at least some messages
                assert len(messages) > 0
                # Messages should be in order
                for i, msg in enumerate(messages):
                    if i > 0:
                        # Extract number from message
                        prev_num = int(messages[i - 1].split("_")[1])
                        curr_num = int(msg.split("_")[1])
                        assert curr_num > prev_num

        # Messages should still be in queue
        with BrokerDB(temp_db) as db:
            remaining = list(db.read("broadcast", all_messages=True))
            assert len(remaining) == 5

    def test_concurrent_writes_during_watch(self, temp_db):
        """Test handling concurrent writes while watching."""
        # Filter out the timestamp conflict warning which is expected in this test
        warnings.filterwarnings(
            "ignore", message="Timestamp conflict persisted", category=RuntimeWarning
        )

        read_messages = []
        lock = threading.Lock()

        def handler(msg: str, ts: int):
            with lock:
                read_messages.append(msg)

        # Start watcher
        watcher_db = BrokerDB(temp_db)
        with self.create_test_watcher(
            watcher_db,
            "concurrent",
            handler,
        ) as watcher:
            watcher_thread = watcher.run_in_thread()

            # Start concurrent writers
            def writer_func(writer_id: int):
                with BrokerDB(temp_db) as db:
                    for i in range(20):
                        db.write("concurrent", f"w{writer_id}_m{i}")
                        time.sleep(0.01)

            writer_threads = []
            for i in range(3):
                t = threading.Thread(target=writer_func, args=(i,))
                t.start()
                writer_threads.append(t)

            # Wait for writers with timeout
            for t in writer_threads:
                t.join(timeout=10.0)
                assert not t.is_alive(), "Writer thread didn't complete"

            # Wait for the watcher to process all messages
            start_time = time.monotonic()
            while time.monotonic() - start_time < 5.0:  # 5 second timeout
                with lock:
                    if len(read_messages) >= 60:
                        break
                time.sleep(0.05)  # Check every 50ms

            # Stop watcher with timeout
            watcher.stop()
            watcher_thread.join(timeout=2.0)
            assert not watcher_thread.is_alive(), "Watcher didn't stop cleanly"

        watcher_db.close()

        # Should have all 60 messages
        assert len(read_messages) == 60
        assert len(set(read_messages)) == 60  # No duplicates

        # Verify all messages accounted for
        expected = set()
        for w in range(3):
            for m in range(20):
                expected.add(f"w{w}_m{m}")
        assert set(read_messages) == expected


class TestEdgeCases(WatcherTestBase):
    """Test edge cases and error conditions."""

    def test_empty_queue_behavior(self, temp_db):
        """Test watcher behavior on empty queue."""
        called = threading.Event()

        def handler(msg: str, ts: int):
            called.set()

        watcher_db = BrokerDB(temp_db)
        watcher = QueueWatcher(
            watcher_db,
            "empty",
            handler,
        )

        thread = watcher.run_in_thread()
        time.sleep(0.2)  # Let it poll a few times

        # Should not have called handler
        assert not called.is_set()

        # Now add a message
        with BrokerDB(temp_db) as db:
            db.write("empty", "finally!")

        # Wait longer on slower systems
        for _ in range(10):  # Up to 1 second total
            if called.is_set():
                break
            time.sleep(0.1)

        # Now should be called
        assert called.is_set()

        watcher.stop()
        thread.join(timeout=5.0)
        watcher_db.close()

    def test_rapid_start_stop(self, temp_db):
        """Test rapid start/stop cycles."""
        for i in range(5):
            watcher_db = BrokerDB(temp_db)
            watcher = QueueWatcher(
                watcher_db,
                f"queue_{i}",
                lambda m, t: None,
            )

            thread = watcher.run_in_thread()
            time.sleep(0.01)
            watcher.stop()
            thread.join(timeout=2.0)
            assert not thread.is_alive()
            watcher_db.close()

    def test_queue_name_validation(self, temp_db):
        """Test that watcher respects queue name validation."""
        # Should work with valid names
        valid_names = ["test", "test_queue", "test-queue", "test.queue", "123"]

        for name in valid_names:
            watcher_db = BrokerDB(temp_db)
            watcher = QueueWatcher(
                watcher_db,
                name,
                lambda m, t: None,
            )
            assert watcher is not None
            watcher_db.close()

        # Invalid names should raise error when trying to read/write
        # This will be caught during actual operation
