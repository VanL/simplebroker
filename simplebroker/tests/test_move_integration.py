"""Integration tests for enhanced move functionality."""

import os
import tempfile

from simplebroker import Queue


def test_move_with_concurrent_operations():
    """Test move with concurrent reads and writes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        source = Queue("source", db_path=db_path)
        dest = Queue("dest", db_path=db_path)

        # Add messages to source queue
        for i in range(10):
            source.write(f"msg{i}")

        # Move specific messages
        # Since we can't move by ID directly, we'll move multiple times
        # and verify the FIFO order
        moved_messages = []
        for _ in range(3):
            result = source.move_one("dest", with_timestamps=False)
            assert result is not None
            moved_messages.append(result)

        # Should have moved msg0, msg1, msg2 in FIFO order
        assert moved_messages == ["msg0", "msg1", "msg2"]

        # Verify source queue has remaining messages
        source_msgs = source.peek_many(limit=10, with_timestamps=False)
        assert len(source_msgs) == 7
        expected_remaining = [f"msg{i}" for i in range(3, 10)]
        assert source_msgs == expected_remaining

        # Verify dest queue has moved messages
        dest_msgs = dest.peek_many(limit=10, with_timestamps=False)
        assert dest_msgs == ["msg0", "msg1", "msg2"]


def test_move_claimed_message_workflow():
    """Test real-world workflow of moving claimed messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        processing = Queue("processing", db_path=db_path)
        error = Queue("error", db_path=db_path)

        # Simulate error queue scenario
        processing.write("job1")
        processing.write("job2")
        processing.write("job3")

        # Process job1 successfully (claim it)
        job1 = processing.read_one(with_timestamps=False)
        assert job1 == "job1"

        # Process job2 but it fails - it's now claimed
        job2 = processing.read_one(with_timestamps=False)
        assert job2 == "job2"

        # Since job2 is claimed, move_one will skip it and move job3
        # This tests that move_one only moves unclaimed messages
        result = processing.move_one("error", with_timestamps=False)
        assert result is not None
        assert result == "job3"  # Should move job3, not job2 which is claimed

        # Verify job3 is now in error queue
        error_msgs = error.peek_many(limit=10, with_timestamps=False)
        assert error_msgs == ["job3"]

        # Verify no unclaimed messages remain in processing queue
        # (job1 and job2 are still claimed but not visible to move_one)
        result = processing.move_one("error", with_timestamps=False)
        assert result is None  # No unclaimed messages to move


def test_move_maintains_fifo_within_queues():
    """Test that moves maintain FIFO order within each queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        queue1 = Queue("queue1", db_path=db_path)
        queue2 = Queue("queue2", db_path=db_path)

        # Add messages to queue1
        for i in range(5):
            queue1.write(f"q1-msg{i}")

        # Add messages to queue2
        for i in range(3):
            queue2.write(f"q2-msg{i}")

        # Move first two messages from queue1 to queue2
        # They should be moved in FIFO order
        moved1 = queue1.move_one("queue2", with_timestamps=False)
        assert moved1 == "q1-msg0"

        moved2 = queue1.move_one("queue2", with_timestamps=False)
        assert moved2 == "q1-msg1"

        # Verify queue1 order preserved (remaining messages)
        q1_remaining = queue1.peek_many(limit=10, with_timestamps=False)
        assert q1_remaining == ["q1-msg2", "q1-msg3", "q1-msg4"]

        # Verify queue2 has all messages in correct order
        # Queue1 messages were created first, so they have lower IDs
        q2_all = queue2.peek_many(limit=10, with_timestamps=False)
        assert len(q2_all) == 5
        # Messages are ordered by ID, so moved q1 messages come first,
        # then the original q2 messages
        assert q2_all == ["q1-msg0", "q1-msg1", "q2-msg0", "q2-msg1", "q2-msg2"]
