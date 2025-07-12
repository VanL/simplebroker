"""Test thread safety of BrokerDB class."""

import concurrent.futures as cf
import tempfile
import threading
import time
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB


def test_shared_brokerdb_thread_safety():
    """Test that a single BrokerDB instance can be safely shared across threads."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create a single shared BrokerDB instance
        db = BrokerDB(str(db_path))

        try:
            messages_written = []
            messages_read = []
            write_lock = threading.Lock()
            read_lock = threading.Lock()

            def writer(thread_id, count):
                """Write messages from a thread."""
                for i in range(count):
                    msg = f"thread{thread_id}_msg{i}"
                    db.write("shared_queue", msg)
                    with write_lock:
                        messages_written.append(msg)
                    # Small random delay to increase contention
                    time.sleep(0.001)

            def reader(thread_id, count):
                """Read messages from a thread."""
                for _ in range(count):
                    msgs = db.read("shared_queue", peek=False, all_messages=False)
                    if msgs:
                        with read_lock:
                            messages_read.extend(msgs)
                    # Small random delay to increase contention
                    time.sleep(0.001)

            # Run multiple writers and readers concurrently
            num_writers = 5
            num_readers = 3
            messages_per_writer = 20

            with cf.ThreadPoolExecutor(max_workers=num_writers + num_readers) as pool:
                # Submit writer tasks
                writer_futures = []
                for i in range(num_writers):
                    future = pool.submit(writer, i, messages_per_writer)
                    writer_futures.append(future)

                # Give writers a small head start
                time.sleep(0.05)

                # Submit reader tasks
                reader_futures = []
                for i in range(num_readers):
                    # Each reader tries to read some messages
                    future = pool.submit(reader, i, messages_per_writer)
                    reader_futures.append(future)

                # Wait for all tasks to complete
                for future in writer_futures + reader_futures:
                    future.result()

            # Read any remaining messages
            remaining = db.read("shared_queue", peek=False, all_messages=True)
            messages_read.extend(remaining)

            # Verify data integrity
            assert len(messages_written) == num_writers * messages_per_writer
            assert len(messages_read) <= len(messages_written)

            # All read messages should be from the written set
            for msg in messages_read:
                assert msg in messages_written

            # Check no duplicates were read
            assert len(messages_read) == len(set(messages_read))

        finally:
            db.close()


def test_concurrent_operations_different_queues():
    """Test concurrent operations on different queues."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = BrokerDB(str(db_path))

        try:

            def queue_worker(queue_name, operations):
                """Perform various operations on a queue."""
                for op, value in operations:
                    if op == "write":
                        db.write(queue_name, value)
                    elif op == "read":
                        db.read(queue_name)
                    elif op == "peek":
                        db.read(queue_name, peek=True)
                    elif op == "list":
                        db.list_queues()
                    elif op == "delete":
                        db.delete(queue_name)
                    time.sleep(0.001)

            # Define operations for different queues
            queue_ops = {
                "queue1": [
                    ("write", "msg1"),
                    ("write", "msg2"),
                    ("read", None),
                    ("list", None),
                ],
                "queue2": [
                    ("write", "msgA"),
                    ("peek", None),
                    ("write", "msgB"),
                    ("delete", None),
                ],
                "queue3": [
                    ("write", "msgX"),
                    ("write", "msgY"),
                    ("write", "msgZ"),
                    ("read", None),
                ],
            }

            with cf.ThreadPoolExecutor(max_workers=3) as pool:
                futures = []
                for queue_name, ops in queue_ops.items():
                    future = pool.submit(queue_worker, queue_name, ops)
                    futures.append(future)

                # Wait for all to complete
                for future in futures:
                    future.result()

            # Verify final state
            queues = dict(db.list_queues())

            # queue1 should have 1 message (wrote 2, read 1)
            assert queues.get("queue1", 0) == 1

            # queue2 should be empty (deleted)
            assert queues.get("queue2", 0) == 0

            # queue3 should have 2 messages (wrote 3, read 1)
            assert queues.get("queue3", 0) == 2

        finally:
            db.close()


def test_brokerdb_not_picklable():
    """Test that BrokerDB instances cannot be pickled."""
    import pickle

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = BrokerDB(str(db_path))

        try:
            # Attempting to pickle should raise TypeError
            with pytest.raises(TypeError, match="cannot be pickled"):
                pickle.dumps(db)
        finally:
            db.close()


def test_database_lock_timeout():
    """Test that database operations handle lock timeouts gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create two separate BrokerDB instances (simulating different threads)
        db1 = BrokerDB(str(db_path))
        db2 = BrokerDB(str(db_path))

        try:
            # Both should be able to write without issues due to WAL mode
            db1.write("test_queue", "msg1")
            db2.write("test_queue", "msg2")

            # Both should see all messages
            msgs1 = db1.read("test_queue", peek=True, all_messages=True)
            msgs2 = db2.read("test_queue", peek=True, all_messages=True)

            assert len(msgs1) == 2
            assert len(msgs2) == 2
            assert set(msgs1) == {"msg1", "msg2"}
            assert set(msgs2) == {"msg1", "msg2"}

        finally:
            db1.close()
            db2.close()
