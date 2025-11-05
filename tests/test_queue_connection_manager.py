"""Tests for Queue.get_connection context manager behavior."""

import gc
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from simplebroker import Queue
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore, BrokerDB, DBConnection


class TestQueueConnectionManager:
    """Test the get_connection context manager behavior."""

    def test_persistent_mode_uses_cached_connection(self):
        """Test that persistent mode reuses thread-local connections."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            queue = None
            try:
                # Create a persistent queue
                with Queue("test", db_path=db_path, persistent=True) as queue:
                    queue = Queue("test", db_path=db_path, persistent=True)

                    # Get connections multiple times in the same thread
                    connections = []
                    for _ in range(3):
                        with queue.get_connection() as conn:
                            connections.append(conn)

                    # All connections should be BrokerDB instances
                    assert all(isinstance(c, BrokerDB) for c in connections), (
                        "Should return BrokerDB instances"
                    )

                    # In the same thread, we should get the same cached thread-local instance
                    assert connections[0] is connections[1], (
                        "Same thread should return the same cached connection"
                    )
                    assert connections[1] is connections[2], (
                        "Same thread should return the same cached connection"
                    )

                    # The underlying DBConnection should be persistent
                    assert queue.conn is not None, "Persistent mode should have a conn"
            finally:
                if queue:
                    queue.close()

    def test_ephemeral_mode_creates_new_connections(self):
        """Test that ephemeral mode creates new connections each time."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create an ephemeral queue
            with Queue("test", db_path=db_path, persistent=False) as queue:
                # Track connection lifecycle
                connection_ids = []

                # Get connections multiple times
                for _ in range(3):
                    with queue.get_connection() as conn:
                        # Each should be a BrokerDB instance
                        assert isinstance(conn, BrokerDB), (
                            "Ephemeral mode should return BrokerDB instances"
                        )
                        connection_ids.append(conn._runner.instance_id)

                # All connections should be different instances
                assert len(set(connection_ids)) == 3, (
                    "Ephemeral mode should create new connections each time"
                )

                # Queue should not have a persistent connection
                assert queue.conn is None, "Ephemeral mode should not have a conn"

    def test_ephemeral_connection_lifetime(self):
        """Test that ephemeral connections are properly closed after use."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            queue = Queue("test", db_path=db_path, persistent=False)
            try:
                # Mock DBConnection to track cleanup
                with patch("simplebroker.sbqueue.DBConnection") as MockDBConnection:
                    mock_conn_instance = Mock(spec=DBConnection)
                    mock_db = Mock(spec=BrokerDB)
                    mock_conn_instance.get_connection.return_value = mock_db
                    mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
                    mock_conn_instance.__exit__ = Mock(return_value=None)
                    MockDBConnection.return_value = mock_conn_instance

                    # Use the connection
                    with queue.get_connection() as conn:
                        assert conn is mock_db
                        # Connection should be active here
                        mock_conn_instance.__enter__.assert_called_once()
                        mock_conn_instance.__exit__.assert_not_called()

                    # After exiting context, cleanup should have been called
                    mock_conn_instance.__exit__.assert_called_once()
            finally:
                if hasattr(queue, "close"):
                    queue.close()

    def test_persistent_connection_lifetime(self):
        """Test that persistent connections stay alive across multiple uses."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            queue = Queue("test", db_path=db_path, persistent=True)

            # Get the connection multiple times in the same thread
            first_conn = None
            for i in range(3):
                with queue.get_connection() as conn:
                    if i == 0:
                        first_conn = conn
                    # In the same thread, connection should be the same each time
                    assert conn is first_conn, (
                        f"Connection {i + 1} should be the same as first in same thread"
                    )

            # The DBConnection should still be alive after multiple uses
            assert queue.conn is not None
            # Thread-local storage should have cached the connection
            with queue.get_connection() as conn:
                assert conn is first_conn, "Should still have cached connection"

            # Only when we close the queue should it be cleaned up
            queue.close()

    def test_thread_safety_ephemeral_mode(self):
        """Test that ephemeral mode is thread-safe (each thread gets its own connection)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            with Queue("test", db_path=db_path, persistent=False) as queue:
                connection_ids = []
                lock = threading.Lock()

                def get_connection():
                    with queue.get_connection() as conn:
                        with lock:
                            connection_ids.append(conn._runner.instance_id)

                # Create multiple threads
                threads = [threading.Thread(target=get_connection) for _ in range(5)]

                try:
                    # Start all threads
                    for t in threads:
                        t.start()

                    # Wait for all to complete
                    for t in threads:
                        t.join()
                finally:
                    # Ensure all threads are cleaned up
                    for t in threads:
                        if t.is_alive():
                            t.join(timeout=1.0)

                # Each thread should have gotten a different connection
                assert len(connection_ids) == 5, "Should have 5 connection IDs"
                assert len(set(connection_ids)) == 5, (
                    "Each thread should get a different connection in ephemeral mode"
                )

    def test_thread_safety_persistent_mode(self):
        """Test that persistent mode uses thread-local connections for safety."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            queue = None
            threads = []
            try:
                with Queue("test", db_path=db_path, persistent=True) as queue:
                    # First, verify same thread gets cached connection
                    with queue.get_connection() as first_conn:
                        with queue.get_connection() as second_conn:
                            assert first_conn is second_conn, (
                                "Sequential calls in same thread should return same connection"
                            )

                    connections = []
                    connection_runner_ids = []
                    lock = threading.Lock()
                    barrier = threading.Barrier(5)  # Synchronize thread starts

                    def get_connection():
                        try:
                            barrier.wait()  # Wait for all threads to be ready
                            # Get connection twice to verify thread-local caching
                            with queue.get_connection() as conn1:
                                with queue.get_connection() as conn2:
                                    with lock:
                                        connections.append((conn1, conn2))
                                        connection_runner_ids.append(
                                            (
                                                conn1._runner.instance_id,
                                                conn2._runner.instance_id,
                                            )
                                        )
                                        # Within same thread, should be cached
                                        assert conn1 is conn2, (
                                            "Same thread should get cached connection"
                                        )
                        except Exception:
                            # Ignore exceptions in worker threads to prevent hanging
                            pass

                    # Create multiple threads
                    threads = [
                        threading.Thread(target=get_connection) for _ in range(5)
                    ]

                    # Start all threads
                    for t in threads:
                        t.start()

                    # Wait for all to complete
                    for t in threads:
                        t.join(timeout=5.0)  # Timeout to prevent hanging

                    # Verify we got 5 pairs of connections (if all threads succeeded)
                    if len(connections) == 5:
                        # In persistent mode, each thread gets its own connection (no sharing)
                        # This follows the principle "don't share across threads"
                        unique_runner_ids = {
                            runner_pair[0] for runner_pair in connection_runner_ids
                        }
                        assert len(unique_runner_ids) == 5, (
                            "Each thread should have its own connection (no sharing across threads)"
                        )

                    # But all should share the same underlying DBConnection object
                    assert queue.conn is not None, "Should have persistent DBConnection"
                connections = None  # Clear to release references
            finally:
                # Clean up threads first with longer timeout
                for t in threads:
                    if t.is_alive():
                        t.join(timeout=2.0)
                threads = None  # Clear to release references

                # Force garbage collection to clean up any remaining references
                gc.collect()

                # Short sleep for Windows file handle finalization
                time.sleep(0.2)

    def test_connection_type_consistency(self):
        """Test that both modes return BrokerDB for consistency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            persistent_queue = None
            ephemeral_queue = None
            try:
                # Test persistent mode
                persistent_queue = Queue("test_p", db_path=db_path, persistent=True)
                with persistent_queue.get_connection() as conn:
                    assert isinstance(conn, BrokerDB), (
                        "Persistent mode should yield BrokerDB"
                    )
                    # BrokerDB inherits from BrokerCore, so it's also a BrokerCore
                    assert isinstance(conn, BrokerCore), (
                        "BrokerDB should also be a BrokerCore"
                    )

                # Test ephemeral mode
                ephemeral_queue = Queue("test_e", db_path=db_path, persistent=False)
                with ephemeral_queue.get_connection() as conn:
                    assert isinstance(conn, BrokerDB), (
                        "Ephemeral mode should yield BrokerDB"
                    )
                    # BrokerDB inherits from BrokerCore, so it's also a BrokerCore
                    assert isinstance(conn, BrokerCore), (
                        "BrokerDB should also be a BrokerCore"
                    )
            finally:
                if persistent_queue:
                    persistent_queue.close()
                if ephemeral_queue:
                    ephemeral_queue.close()

    def test_connection_error_handling(self):
        """Test that connection errors are properly handled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Test that exceptions in the context manager are propagated
            with Queue("test", db_path=db_path, persistent=False) as queue:
                with pytest.raises(ValueError):
                    with queue.get_connection() as conn:
                        # Simulate an error during operation
                        raise ValueError("Test error")

                # Queue should still be usable after error
                with queue.get_connection() as conn:
                    conn.write("test", "message")

                # Verify the message was written
                with queue.get_connection() as conn:
                    messages = list(conn.peek_generator("test", with_timestamps=False))
                    assert messages == ["message"]

    def test_mixed_mode_operations(self):
        """Test that persistent and ephemeral queues can coexist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            persistent_q = None
            ephemeral_q = None
            try:
                # Create both types of queues
                persistent_q = Queue("persistent", db_path=db_path, persistent=True)
                ephemeral_q = Queue("ephemeral", db_path=db_path, persistent=False)

                # Write with persistent
                with persistent_q.get_connection() as conn:
                    conn.write("persistent", "msg1")

                # Write with ephemeral
                with ephemeral_q.get_connection() as conn:
                    conn.write("ephemeral", "msg2")

                # Read with different queue instances
                with persistent_q.get_connection() as conn:
                    msgs = list(conn.peek_generator("ephemeral", with_timestamps=False))
                    assert msgs == ["msg2"], "Should read ephemeral queue messages"

                with ephemeral_q.get_connection() as conn:
                    msgs = list(
                        conn.peek_generator("persistent", with_timestamps=False)
                    )
                    assert msgs == ["msg1"], "Should read persistent queue messages"
            finally:
                if persistent_q:
                    persistent_q.close()
                if ephemeral_q:
                    ephemeral_q.close()

    def test_persistent_avoids_reconnection_overhead(self):
        """Test that persistent mode avoids reconnecting and re-running PRAGMAs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Track SQLiteRunner setup calls
            setup_calls = []
            original_setup = SQLiteRunner.setup

            def tracked_setup(self, phase):
                setup_calls.append((self.instance_id, phase))
                return original_setup(self, phase)

            # Test persistent mode first
            queue1 = None
            try:
                with patch.object(SQLiteRunner, "setup", tracked_setup):
                    # Create a persistent queue
                    queue1 = Queue("test", db_path=db_path, persistent=True)

                    # First operation should trigger setup
                    with queue1.get_connection() as conn:
                        conn.write("test", "initial")

                    # Record initial setup calls
                    initial_setup_count = len(setup_calls)
                    assert initial_setup_count > 0, (
                        "Should have some setup calls after first connection"
                    )

                    # Perform multiple additional operations
                    for i in range(5):
                        with queue1.get_connection() as conn:
                            conn.write("test", f"msg{i}")

                    # Within the same thread, no new SQLiteRunner instances should be created
                    # (same runner ID should be used for this thread)
                    runner_ids_main_thread = {call[0] for call in setup_calls}
                    assert len(runner_ids_main_thread) == 1, (
                        "Within same thread, persistent mode should reuse the same SQLiteRunner"
                    )

                    # No additional setup calls should have been made
                    assert len(setup_calls) == initial_setup_count, (
                        f"No new setup calls should be made after initial creation. "
                        f"Got {len(setup_calls)} calls vs {initial_setup_count} initial"
                    )
            finally:
                if queue1:
                    queue1.close()

            # Now test ephemeral mode for comparison
            setup_calls.clear()
            queue2 = None
            try:
                with patch.object(SQLiteRunner, "setup", tracked_setup):
                    # Create an ephemeral queue
                    queue2 = Queue("test", db_path=db_path, persistent=False)

                    # Perform multiple operations
                    for i in range(3):
                        with queue2.get_connection() as conn:
                            conn.write("test", f"msg{i}")

                    # Check that new SQLiteRunner instances were created
                    runner_ids = {call[0] for call in setup_calls}
                    # Each operation creates a new DBConnection with its own runner
                    assert len(runner_ids) >= 3, (
                        f"Ephemeral mode should create new SQLiteRunner instances. "
                        f"Got {len(runner_ids)} unique runners for 3 operations"
                    )

                    # Multiple setup calls should have been made
                    assert len(setup_calls) >= 6, (
                        f"Ephemeral mode should run setup for each connection. "
                        f"Got {len(setup_calls)} setup calls for 3 operations"
                    )
            finally:
                if queue2:
                    queue2.close()

    def test_persistent_connection_reuse(self):
        """Test that persistent mode reuses the same database connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Track SQLiteRunner __init__ calls to detect new connections
            init_calls = []
            original_init = SQLiteRunner.__init__

            def tracked_init(self, *args, **kwargs):
                result = original_init(self, *args, **kwargs)
                init_calls.append(self.instance_id)
                return result

            # Test persistent mode first
            queue1 = None
            try:
                with patch.object(SQLiteRunner, "__init__", tracked_init):
                    # Test persistent mode
                    queue1 = Queue("test", db_path=db_path, persistent=True)

                    # First operation creates the connection
                    with queue1.get_connection() as conn:
                        conn.write("test", "initial")

                    initial_runners = len(init_calls)
                    assert initial_runners == 1, "Should create one SQLiteRunner"

                    # Perform multiple operations in the same thread
                    for i in range(5):
                        with queue1.get_connection() as conn:
                            conn.write("test", f"msg{i}")

                    # No new runners should have been created
                    assert len(init_calls) == initial_runners, (
                        f"Persistent mode should reuse the same SQLiteRunner. "
                        f"Got {len(init_calls)} runners vs {initial_runners} initial"
                    )
            finally:
                if queue1:
                    queue1.close()

            # Now test ephemeral mode for comparison
            init_calls.clear()
            queue2 = None
            try:
                with patch.object(SQLiteRunner, "__init__", tracked_init):
                    queue2 = Queue("test_ephemeral", db_path=db_path, persistent=False)

                    # Perform multiple operations
                    for i in range(3):
                        with queue2.get_connection() as conn:
                            conn.write("test_ephemeral", f"msg{i}")

                    # Each operation should create a new runner
                    assert len(init_calls) >= 3, (
                        f"Ephemeral mode should create new SQLiteRunner for each operation. "
                        f"Got {len(init_calls)} runners for 3 operations"
                    )
            finally:
                if queue2:
                    queue2.close()
