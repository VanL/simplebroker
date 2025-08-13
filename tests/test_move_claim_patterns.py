"""
Test patterns from message claim feature applied to move functionality.

This tests the patterns that make sense for move operations:
1. Concurrent moves don't move the same message twice
2. Move performance with large batches
3. Move with vacuum interaction
4. Schema verification for move operations with claimed column
5. Move operations update claimed status correctly
"""

import concurrent.futures as cf
import sqlite3
from pathlib import Path
from typing import List, Tuple, Union

from simplebroker.db import BrokerDB


def _concurrent_move_worker(
    args: Tuple[int, str, str, str],
) -> List[Union[str, Tuple[str, int]]]:
    """Worker function for concurrent move tests."""
    worker_id, db_path, source_queue, dest_queue = args
    moved = []

    with BrokerDB(db_path) as db:
        # Each worker tries to move 5 messages
        for _ in range(5):
            result = db.move_one(source_queue, dest_queue, with_timestamps=False)
            if result:
                moved.append(result)
            else:
                break  # No more messages

    return moved


def test_concurrent_moves_no_duplicate_move(workdir: Path):
    """Test that concurrent moves don't move the same message twice."""
    db_path = workdir / "test.db"

    # Write 20 messages to source queue
    with BrokerDB(str(db_path)) as db:
        for i in range(20):
            db.write("source_queue", f"message{i:02d}")

    # Start 4 concurrent move workers
    with cf.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(4):
            future = executor.submit(
                _concurrent_move_worker,
                (i, str(db_path), "source_queue", "dest_queue"),
            )
            futures.append(future)

        # Collect all moved messages
        all_moved = []
        for future in cf.as_completed(futures):
            messages = future.result()
            all_moved.extend(messages)

    # Verify no duplicates
    assert len(all_moved) == 20
    moved_bodies = [msg if isinstance(msg, str) else msg[0] for msg in all_moved]
    assert len(set(moved_bodies)) == 20  # All unique

    # Verify all messages were moved
    expected = {f"message{i:02d}" for i in range(20)}
    assert set(moved_bodies) == expected

    # Verify source queue is empty
    with BrokerDB(str(db_path)) as db:
        remaining = list(db.peek_generator("source_queue", with_timestamps=False))
        assert len(remaining) == 0

    # Verify dest queue has all messages
    with BrokerDB(str(db_path)) as db:
        dest_messages = list(db.peek_generator("dest_queue", with_timestamps=False))
        assert len(dest_messages) == 20


def test_move_updates_claimed_status(workdir: Path):
    """Test that move operations move messages by updating queue column."""
    db_path = workdir / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("source", f"message{i}")

    # Move first message
    with BrokerDB(str(db_path)) as db:
        result = db.move_one("source", "dest", with_timestamps=False)
        assert result is not None
        assert result == "message0"

    # Check database state
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Source should have 4 messages remaining (message0 was moved)
    cursor.execute(
        "SELECT body, claimed FROM messages WHERE queue = 'source' ORDER BY id"
    )
    source_messages = cursor.fetchall()
    assert len(source_messages) == 4
    assert source_messages[0] == ("message1", 0)  # Next message is unclaimed
    assert all(msg[1] == 0 for msg in source_messages)  # All unclaimed

    # Dest should have the moved message
    cursor.execute(
        "SELECT body, claimed FROM messages WHERE queue = 'dest' ORDER BY id"
    )
    dest_messages = cursor.fetchall()
    assert len(dest_messages) == 1
    assert dest_messages[0] == ("message0", 0)  # Movered message is unclaimed

    conn.close()


def test_move_with_vacuum_interaction(workdir: Path):
    """Test that moved messages interact correctly with vacuum."""
    db_path = workdir / "test.db"

    # Create messages in source queue
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("vacuum_source", f"msg{i}")

    # Move half the messages
    with BrokerDB(str(db_path)) as db:
        for _ in range(5):
            result = db.move_one("vacuum_source", "vacuum_dest", with_timestamps=False)
            assert result is not None

    # Check state before vacuum
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Source should have 5 messages remaining (5 were moved)
    cursor.execute(
        "SELECT COUNT(*), SUM(claimed) FROM messages WHERE queue = 'vacuum_source'"
    )
    source_stats = cursor.fetchone()
    assert source_stats == (5, 0)  # 5 unclaimed messages remain

    # Dest should have 5 messages, 0 claimed
    cursor.execute(
        "SELECT COUNT(*), SUM(claimed) FROM messages WHERE queue = 'vacuum_dest'"
    )
    dest_stats = cursor.fetchone()
    assert dest_stats == (5, 0)

    # Now claim some messages in source to test vacuum interaction
    with BrokerDB(str(db_path)) as db:
        # Read 2 messages from source to claim them
        for _ in range(2):
            db.claim_one("vacuum_source", with_timestamps=False)

    # Run vacuum
    with BrokerDB(str(db_path)) as db:
        db.vacuum()

    # Check state after vacuum
    # Source should have 3 unclaimed messages (2 claimed ones removed)
    cursor.execute(
        "SELECT COUNT(*), SUM(claimed) FROM messages WHERE queue = 'vacuum_source'"
    )
    source_after = cursor.fetchone()
    assert source_after == (3, 0)

    # Dest should be unchanged
    cursor.execute(
        "SELECT COUNT(*), SUM(claimed) FROM messages WHERE queue = 'vacuum_dest'"
    )
    dest_after = cursor.fetchone()
    assert dest_after == (5, 0)

    conn.close()


def test_move_schema_verification(workdir: Path):
    """Test that move works correctly with claimed column schema."""
    db_path = workdir / "test.db"

    # Create database with messages
    with BrokerDB(str(db_path)) as db:
        db.write("schema_test", "test_message")

    # Verify schema includes claimed column
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(messages)")
    columns = {row[1]: row[2] for row in cursor.fetchall()}
    assert "claimed" in columns
    assert columns["claimed"] == "INTEGER"

    # Verify partial index exists for performance
    cursor.execute("""
        SELECT sql FROM sqlite_master
        WHERE type = 'index' AND sql LIKE '%claimed%'
    """)
    index_sql = cursor.fetchone()
    assert index_sql is not None
    assert "WHERE claimed = 0" in index_sql[0] or "WHERE claimed=0" in index_sql[0]

    # Test move uses the index correctly
    with BrokerDB(str(db_path)) as db:
        result = db.move_one("schema_test", "schema_dest", with_timestamps=False)
        assert result is not None
        assert result == "test_message"

    conn.close()


def test_move_with_mixed_claimed_unclaimed(workdir: Path):
    """Test move behavior with mix of claimed and unclaimed messages."""
    db_path = workdir / "test.db"

    # Write 10 messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("mixed_source", f"message{i}")

    # Read (claim) messages 0, 1, 2, 3, 4
    with BrokerDB(str(db_path)) as db:
        # Read first 5 messages to claim them
        for i in range(5):
            msg = db.claim_one("mixed_source", with_timestamps=False)
            assert msg == f"message{i}"

    # Now move - should only get unclaimed messages in order
    moved = []
    with BrokerDB(str(db_path)) as db:
        for _ in range(5):
            result = db.move_one("mixed_source", "mixed_dest", with_timestamps=False)
            if result:
                moved.append(result)

    # Should have moved the 5 unclaimed messages
    assert len(moved) == 5
    assert moved == ["message5", "message6", "message7", "message8", "message9"]

    # Verify no more messages to move
    with BrokerDB(str(db_path)) as db:
        result = db.move_one("mixed_source", "mixed_dest", with_timestamps=False)
        assert result is None


def test_move_atomicity(workdir: Path):
    """Test that move is atomic - either completes fully or not at all."""
    db_path = workdir / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("atomic_source", f"message{i}")

    # Get initial state
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM messages WHERE queue = 'atomic_source'")
    initial_source_count = cursor.fetchone()[0]
    assert initial_source_count == 5

    # Perform move
    with BrokerDB(str(db_path)) as db:
        result = db.move_one("atomic_source", "atomic_dest", with_timestamps=False)
        assert result is not None

    # Verify atomicity - exactly one message moved
    cursor.execute("SELECT COUNT(*) FROM messages WHERE queue = 'atomic_source'")
    source_count = cursor.fetchone()[0]
    assert source_count == 4  # One message was moved

    cursor.execute("SELECT COUNT(*) FROM messages WHERE queue = 'atomic_dest'")
    dest_count = cursor.fetchone()[0]
    assert dest_count == 1

    # Verify total message count is preserved
    cursor.execute("SELECT COUNT(*) FROM messages")
    total_count = cursor.fetchone()[0]
    assert total_count == 5  # Same total, just moved between queues

    conn.close()


def test_move_preserves_message_ordering(workdir: Path):
    """Test that moves preserve strict FIFO ordering."""
    db_path = workdir / "test.db"

    # Write messages with specific content to verify order
    messages = []
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            msg = f"ordered_message_{i:03d}"
            messages.append(msg)
            db.write("order_source", msg)

    # Move all messages
    moved = []
    with BrokerDB(str(db_path)) as db:
        while True:
            result = db.move_one("order_source", "order_dest", with_timestamps=False)
            if result is None:
                break
            moved.append(result)

    # Verify order is preserved
    assert moved == messages

    # Read from destination to verify order is maintained
    with BrokerDB(str(db_path)) as db:
        dest_messages = list(db.peek_generator("order_dest", with_timestamps=False))
        assert dest_messages == messages


def test_move_empty_to_empty_queue(workdir: Path):
    """Test move between non-existent/empty queues."""
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as db:
        # Move from non-existent queue
        result = db.move_one(
            "does_not_exist", "also_does_not_exist", with_timestamps=False
        )
        assert result is None

        # Create empty source queue
        db.write("empty_source", "temp")
        db.claim_one("empty_source", with_timestamps=False)  # Claim the message

        # Move from empty queue
        result = db.move_one("empty_source", "empty_dest", with_timestamps=False)
        assert result is None


def test_multiple_sequential_moves(workdir: Path):
    """Test multiple sequential moves maintain consistency."""
    db_path = workdir / "test.db"

    # Create messages in multiple source queues
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("source1", f"s1_msg{i}")
            db.write("source2", f"s2_msg{i}")

    # Move from alternating sources
    moved = []
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            source = "source1" if i % 2 == 0 else "source2"
            result = db.move_one(source, "combined_dest", with_timestamps=False)
            if result:
                moved.append(result)

    assert len(moved) == 10

    # Verify both sources are empty
    with BrokerDB(str(db_path)) as db:
        assert list(db.peek_generator("source1", with_timestamps=False)) == []
        assert list(db.peek_generator("source2", with_timestamps=False)) == []

        # Verify destination has all messages
        dest_messages = list(db.peek_generator("combined_dest", with_timestamps=False))
        assert len(dest_messages) == 10
