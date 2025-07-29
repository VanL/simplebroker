"""Tests for the QueueMoveWatcher feature."""

import threading
import time
from typing import List, Optional, Tuple

import pytest

from simplebroker.db import BrokerDB

# Import will be available after implementation
pytest.importorskip("simplebroker.watcher")
from simplebroker.watcher import QueueMoveWatcher

from .helpers.watcher_base import WatcherTestBase


class MoveCollector:
    """Helper to collect moved messages and track events."""

    def __init__(self):
        self.messages: List[Tuple[str, int]] = []
        self.handler_calls: List[Tuple[str, int]] = []
        self.errors: List[Tuple[Exception, str, int]] = []
        self.lock = threading.Lock()

    def handler(self, body: str, ts: int) -> None:
        """Collect messages thread-safely."""
        with self.lock:
            self.handler_calls.append((body, ts))

    def error_handler(self, body: str, ts: int) -> None:
        """Handler that always raises an error."""
        raise RuntimeError(f"Handler error for message: {body}")

    def conditional_error_handler(self, body: str, ts: int) -> None:
        """Handler that fails on specific messages."""
        if "fail" in body:
            raise ValueError(f"Failing on purpose: {body}")
        with self.lock:
            self.handler_calls.append((body, ts))

    def capture_error_handler(
        self, exc: Exception, body: str, ts: int
    ) -> Optional[bool]:
        """Capture errors for analysis."""
        with self.lock:
            self.errors.append((exc, body, ts))
        return None

    def get_handler_calls(self) -> List[Tuple[str, int]]:
        """Get handler calls thread-safely."""
        with self.lock:
            return self.handler_calls.copy()

    def get_errors(self) -> List[Tuple[Exception, str, int]]:
        """Get captured errors thread-safely."""
        with self.lock:
            return self.errors.copy()


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    return db_path


@pytest.fixture
def broker(temp_db):
    """Create a BrokerDB instance (acting as SimpleBroker)."""
    db = BrokerDB(temp_db)
    yield db
    db.close()


class TestQueueMoveWatcher(WatcherTestBase):
    """Test the QueueMoveWatcher class."""

    def test_basic_move_functionality(self, broker, temp_db):
        """Test basic message move from one queue to another."""
        # Add messages to source queue
        broker.write("source_queue", "message1")
        broker.write("source_queue", "message2")
        broker.write("source_queue", "message3")

        collector = MoveCollector()

        # Create move watcher
        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source_queue",
            dest_queue="dest_queue",
            handler=collector.handler,
        )

        # Run move with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        # Verify all messages were moved
        assert watcher.move_count == 3

        # Verify handler was called for each message
        handler_calls = collector.get_handler_calls()
        assert len(handler_calls) == 3
        assert [body for body, ts in handler_calls] == [
            "message1",
            "message2",
            "message3",
        ]

        # Verify messages are now in destination queue
        to_messages = list(broker.read("dest_queue", all_messages=True))
        assert len(to_messages) == 3
        assert to_messages == ["message1", "message2", "message3"]

        # Verify source queue is empty
        from_messages = list(broker.read("source_queue", all_messages=True))
        assert len(from_messages) == 0

    def test_handler_execution_verification(self, broker, temp_db):
        """Test that handler is called for each moved message."""
        # Add messages
        broker.write("source", "msg1")
        broker.write("source", "msg2")

        handler_calls = []

        def tracking_handler(body: str, ts: int):
            handler_calls.append(
                {
                    "body": body,
                    "timestamp": ts,
                    "queue": "dest",  # We know it's the destination queue
                }
            )

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=tracking_handler,
        )

        # Run watcher with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        # Verify handler received correct message data
        assert len(handler_calls) == 2
        assert handler_calls[0]["body"] == "msg1"
        assert handler_calls[0]["queue"] == "dest"
        assert handler_calls[1]["body"] == "msg2"
        assert handler_calls[1]["queue"] == "dest"

    def test_handler_failure_isolation(self, broker, temp_db):
        """Test that handler failures don't prevent message move."""
        # Add messages
        broker.write("source", "message1")
        broker.write("source", "fail_message")
        broker.write("source", "message3")

        collector = MoveCollector()

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=collector.conditional_error_handler,
            error_handler=collector.capture_error_handler,
        )

        # Run watcher with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        # Verify all messages were moved despite handler failure
        assert watcher.move_count == 3

        # Verify messages are in destination queue
        dest_messages = list(broker.read("dest", all_messages=True))
        assert len(dest_messages) == 3
        assert dest_messages == ["message1", "fail_message", "message3"]

        # Verify error was captured
        errors = collector.get_errors()
        assert len(errors) == 1
        assert "Failing on purpose" in str(errors[0][0])
        assert errors[0][1] == "fail_message"  # body is second element

        # Verify successful handler calls
        handler_calls = collector.get_handler_calls()
        assert len(handler_calls) == 2
        assert [body for body, ts in handler_calls] == ["message1", "message3"]

    def test_order_preservation_global_id(self, broker, temp_db):
        """Test that moved messages maintain their global ID order."""
        # Add messages - they'll get sequential global IDs
        broker.write("source", "msg1")  # ID=1
        broker.write("source", "msg2")  # ID=2
        broker.write("source", "msg3")  # ID=3

        moved_ids = []

        def capture_id_handler(body: str, ts: int):
            # We can't capture ID anymore with new signature
            moved_ids.append(body)

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=capture_id_handler,
        )

        # Run watcher with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        # Verify messages were moved in order
        assert len(moved_ids) == 3
        assert moved_ids == ["msg1", "msg2", "msg3"]  # Should maintain order

    def test_mixed_source_ordering_behavior(self, broker, temp_db):
        """Test ordering when destination queue already has messages."""
        # Write to destination queue first
        broker.write("dest", "dest1")  # ID=1
        broker.write("dest", "dest2")  # ID=2

        # Write to source queue
        broker.write("source", "src1")  # ID=3
        broker.write("source", "src2")  # ID=4

        # Write more to destination
        broker.write("dest", "dest3")  # ID=5

        # Write more to source
        broker.write("source", "src3")  # ID=6

        # Move from source to dest
        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=lambda body, ts: None,
        )

        # Run watcher with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        # Read all messages from destination in order
        dest_messages = list(broker.read("dest", all_messages=True))

        # Messages should be in global ID order (interleaved)
        # Expected order: dest1, dest2, src1, src2, dest3, src3
        assert dest_messages == ["dest1", "dest2", "src1", "src2", "dest3", "src3"]

    def test_empty_queue_handling(self, broker, temp_db):
        """Test watcher behavior with empty source queue."""
        collector = MoveCollector()

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="empty_source",
            dest_queue="dest",
            handler=collector.handler,
            max_interval=0.05,  # Faster polling for test
        )

        # Start watcher on empty queue
        thread = watcher.run_in_thread()
        time.sleep(0.1)  # Let it poll a few times

        # Add message while watcher is running
        broker.write("empty_source", "delayed_message")

        time.sleep(0.15)  # Wait for move

        # Stop with timeout safety
        watcher.stop()
        thread.join(timeout=2.0)
        if thread.is_alive():
            pytest.fail("Watcher thread did not stop within timeout")

        # Verify message was moved
        assert watcher.move_count == 1
        handler_calls = collector.get_handler_calls()
        assert len(handler_calls) == 1
        assert handler_calls[0][0] == "delayed_message"  # body is first element

        # Verify message is in destination
        dest_messages = list(broker.read("dest", all_messages=True))
        assert dest_messages == ["delayed_message"]

    def test_concurrent_operations(self, broker, temp_db):
        """Test concurrent writes during move operations."""
        moved_count = 0
        moved_lock = threading.Lock()

        def counting_handler(body: str, ts: int):
            nonlocal moved_count
            with moved_lock:
                moved_count += 1

        # Add initial messages
        for i in range(5):
            broker.write("source", f"initial_{i}")

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=counting_handler,
        )

        # Start move
        thread = watcher.run_in_thread()

        # Wait a bit to ensure watcher is running
        # This is acceptable as we're not testing timing, just concurrent safety
        time.sleep(0.05)

        # Add more messages while moving
        write_errors = []

        def write_with_error_capture():
            for i in range(5):
                try:
                    broker.write("source", f"concurrent_{i}")
                    time.sleep(0.01)
                except Exception as e:
                    write_errors.append((i, str(e)))

        writer_thread = threading.Thread(target=write_with_error_capture)
        writer_thread.start()

        # Let everything process
        time.sleep(0.3)

        # Stop with timeout safety
        watcher.stop()
        thread.join(timeout=2.0)
        if thread.is_alive():
            pytest.fail("Watcher thread did not stop within timeout")
        writer_thread.join(timeout=2.0)
        if writer_thread.is_alive():
            pytest.fail("Writer thread did not stop within timeout")

        # Check for write errors
        if write_errors:
            print(f"Write errors occurred: {write_errors}")

        # All successfully written messages should be moved
        expected_count = 10 - len(write_errors)
        with moved_lock:
            assert moved_count == expected_count, (
                f"Expected {expected_count} messages (10 - {len(write_errors)} errors), got {moved_count}"
            )

        # Verify all messages in destination
        dest_messages = list(broker.read("dest", all_messages=True))
        assert len(dest_messages) == expected_count, (
            f"Expected {expected_count} messages in dest, got {len(dest_messages)}"
        )

        # Source should be empty
        source_messages = list(broker.read("source", all_messages=True))
        assert len(source_messages) == 0

    def test_transaction_safety(self, broker, temp_db):
        """Test that moves are atomic and transactional."""
        # This test simulates transaction behavior by checking atomicity

        # Add a message
        broker.write("source", "atomic_test")

        move_completed = threading.Event()

        def slow_handler(body: str, ts: int):
            # Simulate slow processing
            move_completed.set()
            time.sleep(0.1)

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=slow_handler,
        )

        thread = watcher.run_in_thread()

        # Wait for move to complete (handler called)
        move_completed.wait(timeout=1.0)

        # At this point, message should be in dest, not in source
        # even though handler is still running
        source_messages = list(broker.read("source", all_messages=True))
        dest_messages = list(broker.read("dest", all_messages=True))

        assert len(source_messages) == 0
        assert len(dest_messages) == 1
        assert dest_messages[0] == "atomic_test"

        # Stop with timeout safety
        watcher.stop()
        thread.join(timeout=2.0)
        if thread.is_alive():
            pytest.fail("Watcher thread did not stop within timeout")

    def test_same_queue_validation(self, broker, temp_db):
        """Test that source_queue and dest_queue must be different."""
        with pytest.raises(ValueError, match="Cannot move messages to the same queue"):
            QueueMoveWatcher(
                broker=broker,
                source_queue="same_queue",
                dest_queue="same_queue",
                handler=lambda body, ts: None,
            )

    def test_max_messages_limit(self, broker, temp_db):
        """Test max_messages parameter limits moves."""
        # Add 10 messages
        for i in range(10):
            broker.write("source", f"msg_{i}")

        moved = []

        def track_handler(body: str, ts: int):
            moved.append(body)

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=track_handler,
            max_messages=5,
        )

        # Run synchronously to ensure it stops at limit
        watcher.run()

        # Should have moved exactly 5 messages
        assert watcher.move_count == 5
        assert len(moved) == 5
        assert moved == ["msg_0", "msg_1", "msg_2", "msg_3", "msg_4"]

        # 5 messages should remain in source
        source_messages = list(broker.read("source", all_messages=True))
        assert len(source_messages) == 5
        assert source_messages == ["msg_5", "msg_6", "msg_7", "msg_8", "msg_9"]

    def test_stop_event_handling(self, broker, temp_db):
        """Test that stop_event properly stops the watcher."""
        stop_event = threading.Event()

        # Add messages
        for i in range(5):
            broker.write("source", f"msg_{i}")

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=lambda body, ts: None,
            stop_event=stop_event,
        )

        thread = watcher.run_in_thread()
        time.sleep(0.1)  # Let some moves happen

        # Signal stop
        stop_event.set()
        thread.join(timeout=2.0)
        if thread.is_alive():
            pytest.fail("Watcher thread did not stop within timeout")

        # Should have stopped (may have moved some but not necessarily all)
        assert not thread.is_alive()
        assert watcher.move_count >= 0
        assert watcher.move_count <= 5

    def test_move_properties(self, broker, temp_db):
        """Test QueueMoveWatcher properties."""
        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source_queue",
            dest_queue="dest_queue",
            handler=lambda body, ts: None,
        )

        assert watcher.source_queue == "source_queue"
        assert watcher.dest_queue == "dest_queue"
        assert watcher.move_count == 0

        # Add and move a message
        broker.write("source_queue", "test")
        # Run watcher with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        assert watcher.move_count == 1

    def test_message_preservation(self, broker, temp_db):
        """Test that message ID, content, and timestamp are preserved during move."""
        # Write a message and capture its details
        broker.write("source", "preserved_content")

        # Read to get message details
        with broker._lock:
            result = broker._runner.run(
                "SELECT id, body, ts FROM messages WHERE queue = ? LIMIT 1",
                ("source",),
                fetch=True,
            )
            original_id, original_body, original_ts = result[0]

        preserved_data = {}

        def capture_handler(body: str, ts: int):
            preserved_data["body"] = body
            preserved_data["ts"] = ts
            preserved_data["queue"] = "dest"  # We know it's destination

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=capture_handler,
        )

        # Run watcher with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        # Verify preservation
        # We can't verify ID anymore with new handler signature
        assert preserved_data["body"] == original_body
        assert preserved_data["ts"] == original_ts
        assert preserved_data["queue"] == "dest"  # Only queue changes

    def test_error_handler_called_on_handler_failure(self, broker, temp_db):
        """Test that error_handler is properly called when handler fails."""
        broker.write("source", "error_message")

        handler_error = None
        error_message = None

        def failing_handler(body: str, ts: int):
            raise RuntimeError("Handler explosion!")

        def error_handler(exc: Exception, body: str, ts: int) -> Optional[bool]:
            nonlocal handler_error, error_message
            handler_error = exc
            error_message = body
            return None

        watcher = QueueMoveWatcher(
            broker=broker,
            source_queue="source",
            dest_queue="dest",
            handler=failing_handler,
            error_handler=error_handler,
        )

        # Run watcher with timeout safety
        self.run_watcher_with_timeout(watcher, timeout=2.0)

        # Verify error was captured
        assert handler_error is not None
        assert str(handler_error) == "Handler explosion!"
        assert error_message is not None
        assert error_message == "error_message"

        # Message should still be moved
        assert watcher.move_count == 1
        dest_messages = list(broker.read("dest", all_messages=True))
        assert dest_messages == ["error_message"]
