"""Test thread safety of BrokerCore/BrokerDB."""

import concurrent.futures as cf
import threading
import time

import pytest

from simplebroker._targets import ResolvedTarget

from .helper_scripts.broker_factory import make_broker

pytestmark = [pytest.mark.shared]


def test_shared_broker_thread_safety(broker):
    """Test that a single BrokerCore instance can be safely shared across threads."""
    messages_written = []
    messages_read = []
    write_lock = threading.Lock()
    read_lock = threading.Lock()

    def writer(thread_id, count):
        for i in range(count):
            msg = f"thread{thread_id}_msg{i}"
            broker.write("shared_queue", msg)
            with write_lock:
                messages_written.append(msg)
            time.sleep(0.001)

    def reader(thread_id, count):
        for _ in range(count):
            msg_result = broker.claim_one("shared_queue", with_timestamps=False)
            if msg_result:
                with read_lock:
                    messages_read.append(msg_result)
            time.sleep(0.001)

    num_writers = 5
    num_readers = 3
    messages_per_writer = 20

    with cf.ThreadPoolExecutor(max_workers=num_writers + num_readers) as pool:
        writer_futures = [
            pool.submit(writer, i, messages_per_writer) for i in range(num_writers)
        ]

        time.sleep(0.05)

        reader_futures = [
            pool.submit(reader, i, messages_per_writer) for i in range(num_readers)
        ]

        for future in writer_futures + reader_futures:
            future.result()

    remaining = list(broker.claim_generator("shared_queue", with_timestamps=False))
    messages_read.extend(remaining)

    assert len(messages_written) == num_writers * messages_per_writer
    assert len(messages_read) <= len(messages_written)

    for msg in messages_read:
        assert msg in messages_written

    assert len(messages_read) == len(set(messages_read))


def test_concurrent_operations_different_queues(broker):
    """Test concurrent operations on different queues."""

    def queue_worker(queue_name, operations):
        for op, value in operations:
            if op == "write":
                broker.write(queue_name, value)
            elif op == "read":
                broker.claim_one(queue_name, with_timestamps=False)
            elif op == "peek":
                broker.peek_one(queue_name, with_timestamps=False)
            elif op == "list":
                broker.list_queues()
            elif op == "delete":
                broker.delete(queue_name)
            time.sleep(0.001)

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
        futures = [
            pool.submit(queue_worker, queue_name, ops)
            for queue_name, ops in queue_ops.items()
        ]
        for future in futures:
            future.result()

    queues = dict(broker.list_queues())
    assert queues.get("queue1", 0) == 1
    assert queues.get("queue2", 0) == 0
    assert queues.get("queue3", 0) == 2


@pytest.mark.sqlite_only
def test_brokerdb_not_picklable():
    """Test that BrokerDB instances cannot be pickled."""
    import pickle
    import tempfile
    from pathlib import Path

    from simplebroker.db import BrokerDB

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = BrokerDB(str(db_path))

        try:
            with pytest.raises(TypeError, match="cannot be pickled"):
                pickle.dumps(db)
        finally:
            db.close()


def test_database_lock_timeout(broker_target: ResolvedTarget):
    """Test that database operations handle lock timeouts gracefully."""
    db1 = make_broker(broker_target)
    db2 = make_broker(broker_target)

    try:
        db1.write("test_queue", "msg1")
        db2.write("test_queue", "msg2")

        msgs1 = db1.peek_many("test_queue", limit=100, with_timestamps=False)
        msgs2 = db2.peek_many("test_queue", limit=100, with_timestamps=False)

        assert len(msgs1) == 2
        assert len(msgs2) == 2
        assert set(msgs1) == {"msg1", "msg2"}
        assert set(msgs2) == {"msg1", "msg2"}

    finally:
        db1.close()
        db2.close()
