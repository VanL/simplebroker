"""Test message timestamp-based move functionality."""

import os
import tempfile

from simplebroker.db import BrokerDB
from simplebroker.sbqueue import Queue


def test_move_by_message_timestamp():
    """Test moving a specific message by timestamp."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages to source queue
            db.write("source", "msg1")
            db.write("source", "msg2")
            db.write("source", "msg3")

            # Get message timestamps using _runner
            with db._lock:
                messages = list(
                    db._runner.run(
                        "SELECT ts, body FROM messages WHERE queue = ? ORDER BY ts",
                        ("source",),
                        fetch=True,
                    )
                )

            # Move the middle message (msg2) by timestamp
            msg2_timestamp = messages[1][0]
            result = db.move_one(
                "source", "dest", exact_timestamp=msg2_timestamp, with_timestamps=True
            )

            assert result is not None
            assert result[0] == "msg2"  # body
            assert result[1] == msg2_timestamp  # timestamp

            # Verify source still has msg1 and msg3
            source_queue = Queue("source", db_path=db_path)
            remaining = list(source_queue.peek_generator())
            assert remaining == ["msg1", "msg3"]

            # Verify dest has msg2
            dest_queue = Queue("dest", db_path=db_path)
            dest_messages = list(dest_queue.peek_generator())
            assert dest_messages == ["msg2"]


def test_move_by_timestamp_not_found():
    """Test moving a non-existent message timestamp returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")

            # Try to move non-existent timestamp
            result = db.move_one(
                "source", "dest", exact_timestamp=99999999, with_timestamps=True
            )
            assert result is None

            # Original message should still be in source
            source_queue = Queue("source", db_path=db_path)
            messages = list(source_queue.peek_generator())
            assert messages == ["msg1"]


def test_move_by_id_wrong_queue():
    """Test moving a message timestamp from wrong queue returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add message to queue1
            db.write("queue1", "msg1")

            # Get the message timestamp using _runner
            with db._lock:
                result = list(
                    db._runner.run(
                        "SELECT ts FROM messages WHERE queue = ?",
                        ("queue1",),
                        fetch=True,
                    )
                )
                msg_ts = result[0][0]

            # Try to move from queue2 (wrong queue) using timestamp
            result = db.move_one("queue2", "dest", exact_timestamp=msg_ts)
            assert result is None

            # Message should still be in queue1
            queue1 = Queue("queue1", db_path=db_path)
            messages = list(queue1.peek_generator())
            assert messages == ["msg1"]


def test_move_claimed_message_with_require_unclaimed():
    """Test that claimed messages are not moved when require_unclaimed=True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")
            db.write("source", "msg2")

            # Claim msg1
            source_queue = Queue("source", db_path=db_path)
            claimed = source_queue.read_one()
            assert claimed == "msg1"

            # Get message timestamps using _runner
            with db._lock:
                messages = list(
                    db._runner.run(
                        "SELECT ts, body, claimed FROM messages WHERE queue = ? ORDER BY ts",
                        ("source",),
                        fetch=True,
                    )
                )

            # Try to move claimed message (msg1) with require_unclaimed=True (default)
            msg1_ts = messages[0][0]
            result = db.move_one("source", "dest", exact_timestamp=msg1_ts)
            assert result is None  # Should not move claimed message

            # Try to move unclaimed message (msg2)
            msg2_ts = messages[1][0]
            result = db.move_one("source", "dest", exact_timestamp=msg2_ts)
            assert result is not None
            assert result == "msg2" or (
                isinstance(result, tuple) and result[0] == "msg2"
            )


def test_move_claimed_message_without_require_unclaimed():
    """Test that claimed messages CAN be moved when require_unclaimed=False."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")
            db.write("source", "msg2")

            # Claim msg1
            source_queue = Queue("source", db_path=db_path)
            claimed = source_queue.read_one()
            assert claimed == "msg1"

            # Get message timestamp of claimed message using _runner
            with db._lock:
                result = list(
                    db._runner.run(
                        "SELECT ts FROM messages WHERE queue = ? AND body = ?",
                        ("source", "msg1"),
                        fetch=True,
                    )
                )
                msg1_ts = result[0][0]

            # Move claimed message with require_unclaimed=False
            result = db.move_one(
                "source", "dest", exact_timestamp=msg1_ts, require_unclaimed=False
            )
            assert result is not None
            assert result == "msg1" or (
                isinstance(result, tuple) and result[0] == "msg1"
            )

            # Verify msg1 is now in dest
            dest_queue = Queue("dest", db_path=db_path)
            dest_messages = list(dest_queue.peek_generator())
            assert dest_messages == ["msg1"]

            # Verify only msg2 remains in source
            source_messages = list(source_queue.peek_generator())
            assert source_messages == ["msg2"]


def test_move_by_id_preserves_timestamp():
    """Test that move preserves the original timestamp."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")

            # Get message timestamp using _runner
            with db._lock:
                result = list(
                    db._runner.run(
                        "SELECT ts FROM messages WHERE queue = ?",
                        ("source",),
                        fetch=True,
                    )
                )
                original_ts = result[0][0]

            # Move the message
            result = db.move_one(
                "source", "dest", exact_timestamp=original_ts, with_timestamps=True
            )
            assert result is not None
            if isinstance(result, tuple):
                assert result[1] == original_ts

            # Verify timestamp is preserved in destination using _runner
            with db._lock:
                result = list(
                    db._runner.run(
                        "SELECT ts FROM messages WHERE queue = ? AND body = ?",
                        ("dest", "msg1"),
                        fetch=True,
                    )
                )
                dest_ts = result[0][0]

            assert dest_ts == original_ts


def test_move_mixed_mode():
    """Test mixing timestamp-based and bulk move modes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages
            for i in range(5):
                db.write("source", f"msg{i}")

            # Get message timestamps using _runner
            with db._lock:
                messages = list(
                    db._runner.run(
                        "SELECT ts, body FROM messages WHERE queue = ? ORDER BY ts",
                        ("source",),
                        fetch=True,
                    )
                )

            # Move specific message by timestamp (msg2)
            msg2_ts = messages[2][0]
            result = db.move_one("source", "dest1", exact_timestamp=msg2_ts)
            assert result == "msg2" or (
                isinstance(result, tuple) and result[0] == "msg2"
            )

            # Move oldest unclaimed (should be msg0)
            result = db.move_one("source", "dest2")
            assert result == "msg0" or (
                isinstance(result, tuple) and result[0] == "msg0"
            )

            # Move by timestamp again (msg4)
            msg4_ts = messages[4][0]
            result = db.move_one("source", "dest1", exact_timestamp=msg4_ts)
            assert result == "msg4" or (
                isinstance(result, tuple) and result[0] == "msg4"
            )

            # Verify remaining messages
            source_queue = Queue("source", db_path=db_path)
            remaining = list(source_queue.peek_generator())
            assert set(remaining) == {"msg1", "msg3"}

            # Verify destinations
            dest1_queue = Queue("dest1", db_path=db_path)
            dest1_msgs = list(dest1_queue.peek_generator())
            assert set(dest1_msgs) == {"msg2", "msg4"}

            dest2_queue = Queue("dest2", db_path=db_path)
            dest2_msgs = list(dest2_queue.peek_generator())
            assert dest2_msgs == ["msg0"]
