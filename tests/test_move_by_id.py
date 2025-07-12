"""Test message ID-based move functionality."""

import os
import tempfile

from simplebroker.db import BrokerDB


def test_move_by_message_id():
    """Test moving a specific message by ID."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages to source queue
            db.write("source", "msg1")
            db.write("source", "msg2")
            db.write("source", "msg3")

            # Get message IDs by peeking
            with db._lock:
                cursor = db.conn.execute(
                    "SELECT id, body FROM messages WHERE queue = ? ORDER BY id",
                    ("source",),
                )
                messages = cursor.fetchall()

            # Move the middle message (msg2) by ID
            msg2_id = messages[1][0]
            result = db.move("source", "dest", message_id=msg2_id)

            assert result is not None
            assert result["body"] == "msg2"
            assert "ts" in result

            # Verify source still has msg1 and msg3
            remaining = db.read("source", peek=True, all_messages=True)
            assert remaining == ["msg1", "msg3"]

            # Verify dest has msg2
            dest_messages = db.read("dest", peek=True, all_messages=True)
            assert dest_messages == ["msg2"]


def test_move_by_id_not_found():
    """Test moving a non-existent message ID returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")

            # Try to move non-existent ID
            result = db.move("source", "dest", message_id=99999)
            assert result is None

            # Original message should still be in source
            messages = db.read("source", peek=True, all_messages=True)
            assert messages == ["msg1"]


def test_move_by_id_wrong_queue():
    """Test moving a message ID from wrong queue returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add message to queue1
            db.write("queue1", "msg1")

            # Get the message ID
            with db._lock:
                cursor = db.conn.execute(
                    "SELECT id FROM messages WHERE queue = ?", ("queue1",)
                )
                msg_id = cursor.fetchone()[0]

            # Try to move from queue2 (wrong queue)
            result = db.move("queue2", "dest", message_id=msg_id)
            assert result is None

            # Message should still be in queue1
            messages = db.read("queue1", peek=True, all_messages=True)
            assert messages == ["msg1"]


def test_move_claimed_message_with_require_unclaimed():
    """Test that claimed messages are not moved when require_unclaimed=True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")
            db.write("source", "msg2")

            # Claim msg1
            claimed = db.read("source")
            assert claimed == ["msg1"]

            # Get message IDs
            with db._lock:
                cursor = db.conn.execute(
                    "SELECT id, body, claimed FROM messages WHERE queue = ? ORDER BY id",
                    ("source",),
                )
                messages = cursor.fetchall()

            # Try to move claimed message (msg1) with require_unclaimed=True (default)
            msg1_id = messages[0][0]
            result = db.move("source", "dest", message_id=msg1_id)
            assert result is None  # Should not move claimed message

            # Try to move unclaimed message (msg2)
            msg2_id = messages[1][0]
            result = db.move("source", "dest", message_id=msg2_id)
            assert result is not None
            assert result["body"] == "msg2"


def test_move_claimed_message_without_require_unclaimed():
    """Test that claimed messages CAN be moved when require_unclaimed=False."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")
            db.write("source", "msg2")

            # Claim msg1
            claimed = db.read("source")
            assert claimed == ["msg1"]

            # Get message ID of claimed message
            with db._lock:
                cursor = db.conn.execute(
                    "SELECT id FROM messages WHERE queue = ? AND body = ?",
                    ("source", "msg1"),
                )
                msg1_id = cursor.fetchone()[0]

            # Move claimed message with require_unclaimed=False
            result = db.move(
                "source", "dest", message_id=msg1_id, require_unclaimed=False
            )
            assert result is not None
            assert result["body"] == "msg1"

            # Verify msg1 is now in dest and is unclaimed
            dest_messages = db.read("dest", peek=True, all_messages=True)
            assert dest_messages == ["msg1"]

            # Verify only msg2 remains in source
            source_messages = db.read("source", peek=True, all_messages=True)
            assert source_messages == ["msg2"]


def test_move_by_id_preserves_timestamp():
    """Test that move preserves the original timestamp."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            db.write("source", "msg1")

            # Get message ID and timestamp
            with db._lock:
                cursor = db.conn.execute(
                    "SELECT id, ts FROM messages WHERE queue = ?", ("source",)
                )
                msg_id, original_ts = cursor.fetchone()

            # Move the message
            result = db.move("source", "dest", message_id=msg_id)
            assert result is not None
            assert result["ts"] == original_ts

            # Verify timestamp is preserved in destination
            with db._lock:
                cursor = db.conn.execute(
                    "SELECT ts FROM messages WHERE queue = ? AND body = ?",
                    ("dest", "msg1"),
                )
                dest_ts = cursor.fetchone()[0]

            assert dest_ts == original_ts


def test_move_mixed_mode():
    """Test mixing ID-based and bulk move modes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with BrokerDB(db_path) as db:
            # Add messages
            for i in range(5):
                db.write("source", f"msg{i}")

            # Get message IDs
            with db._lock:
                cursor = db.conn.execute(
                    "SELECT id, body FROM messages WHERE queue = ? ORDER BY id",
                    ("source",),
                )
                messages = cursor.fetchall()

            # Move specific message by ID (msg2)
            msg2_id = messages[2][0]
            result = db.move("source", "dest1", message_id=msg2_id)
            assert result["body"] == "msg2"

            # Move oldest unclaimed (should be msg0)
            result = db.move("source", "dest2")
            assert result["body"] == "msg0"

            # Move by ID again (msg4)
            msg4_id = messages[4][0]
            result = db.move("source", "dest1", message_id=msg4_id)
            assert result["body"] == "msg4"

            # Verify remaining messages
            remaining = db.read("source", peek=True, all_messages=True)
            assert set(remaining) == {"msg1", "msg3"}

            # Verify destinations
            dest1_msgs = db.read("dest1", peek=True, all_messages=True)
            assert set(dest1_msgs) == {"msg2", "msg4"}

            dest2_msgs = db.read("dest2", peek=True, all_messages=True)
            assert dest2_msgs == ["msg0"]
