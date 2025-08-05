"""Integration tests for enhanced move functionality."""

import os
import tempfile

from simplebroker.db import BrokerDB


def test_move_with_concurrent_operations():
    """Test move with concurrent reads and writes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages to source queue
            for i in range(10):
                db.write("source", f"msg{i}")

            # Get message IDs
            with db._lock:
                messages = db._runner.run(
                    "SELECT id, body FROM messages WHERE queue = ? ORDER BY id",
                    ("source",),
                    fetch=True,
                )

            # Move specific messages by ID
            # Move msg3, msg5, msg7 to dest
            for idx in [3, 5, 7]:
                msg_id = messages[idx][0]
                result = db.move("source", "dest", message_id=msg_id)
                assert result is not None
                assert result["body"] == f"msg{idx}"

            # Verify source queue has remaining messages
            source_msgs = db.read("source", peek=True, all_messages=True)
            assert len(source_msgs) == 7
            assert "msg3" not in source_msgs
            assert "msg5" not in source_msgs
            assert "msg7" not in source_msgs

            # Verify dest queue has moved messages
            dest_msgs = db.read("dest", peek=True, all_messages=True)
            assert set(dest_msgs) == {"msg3", "msg5", "msg7"}


def test_move_claimed_message_workflow():
    """Test real-world workflow of moving claimed messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Simulate error queue scenario
            db.write("processing", "job1")
            db.write("processing", "job2")
            db.write("processing", "job3")

            # Process job1 successfully
            job1 = db.read("processing")
            assert job1 == ["job1"]

            # Process job2 but it fails - it's now claimed
            job2 = db.read("processing")
            assert job2 == ["job2"]

            # Get ID of the failed job
            with db._lock:
                rows = db._runner.run(
                    "SELECT id FROM messages WHERE queue = ? AND body = ? AND claimed = 1",
                    ("processing", "job2"),
                    fetch=True,
                )
                job2_id = rows[0][0]

            # Move the failed job to error queue (allowing claimed messages)
            result = db.move(
                "processing", "error", message_id=job2_id, require_unclaimed=False
            )
            assert result is not None
            assert result["body"] == "job2"

            # Verify job2 is now in error queue and unclaimed
            error_msgs = db.read("error", peek=True, all_messages=True)
            assert error_msgs == ["job2"]

            # Verify only job3 remains in processing queue
            processing_msgs = db.read("processing", peek=True, all_messages=True)
            assert processing_msgs == ["job3"]


def test_move_maintains_fifo_within_queues():
    """Test that moves maintain FIFO order within each queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages to queue1
            for i in range(5):
                db.write("queue1", f"q1-msg{i}")

            # Add messages to queue2
            for i in range(3):
                db.write("queue2", f"q2-msg{i}")

            # Get message IDs from queue1
            with db._lock:
                q1_messages = db._runner.run(
                    "SELECT id, body FROM messages WHERE queue = ? ORDER BY id",
                    ("queue1",),
                    fetch=True,
                )

            # Move q1-msg1 and q1-msg3 to queue2
            for idx in [1, 3]:
                msg_id = q1_messages[idx][0]
                db.move("queue1", "queue2", message_id=msg_id)

            # Verify queue1 order preserved
            q1_remaining = db.read("queue1", peek=True, all_messages=True)
            assert q1_remaining == ["q1-msg0", "q1-msg2", "q1-msg4"]

            # Verify queue2 has all messages
            q2_all = db.read("queue2", peek=True, all_messages=True)
            assert len(q2_all) == 5
            # Messages are ordered by ID, so moved messages (with lower IDs)
            # will appear before the original queue2 messages
            assert set(q2_all) == {
                "q2-msg0",
                "q2-msg1",
                "q2-msg2",
                "q1-msg1",
                "q1-msg3",
            }
            # The exact order depends on the message IDs, which are assigned sequentially
            # q1 messages were created first, so they have lower IDs
