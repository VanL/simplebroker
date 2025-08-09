"""Tests for Queue.get_connection context manager behavior."""

import tempfile
import threading
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

            # Create a persistent queue
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

            queue.close()

    def test_ephemeral_mode_creates_new_connections(self):
        """Test that ephemeral mode creates new connections each time."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create an ephemeral queue
            queue = Queue("test", db_path=db_path, persistent=False)
            try:
                # Track connection lifecycle
                connection_ids = []

                # Get connections multiple times
                for _ in range(3):
                    with queue.get_connection() as conn:
                        # Each should be a BrokerDB instance
                        assert isinstance(conn, BrokerDB), (
                            "Ephemeral mode should return BrokerDB instances"
                        )
                        connection_ids.append(id(conn))

                # All connections should be different instances
                assert len(set(connection_ids)) == 3, (
                    "Ephemeral mode should create new connections each time"
                )

                # Queue should not have a persistent connection
                assert queue.conn is None, "Ephemeral mode should not have a conn"
            finally:
                # Ephemeral queues don't need explicit close, but good practice
                if hasattr(queue, "close"):
                    queue.close()

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

            queue = Queue("test", db_path=db_path, persistent=False)
            try:
                connection_ids = []
                lock = threading.Lock()

                def get_connection():
                    with queue.get_connection() as conn:
                        with lock:
                            connection_ids.append(id(conn))

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
            finally:
                if hasattr(queue, "close"):
                    queue.close()

    def test_thread_safety_persistent_mode(self):
        """Test that persistent mode uses thread-local connections for safety."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            queue = Queue("test", db_path=db_path, persistent=True)

            # First, verify same thread gets cached connection
            with queue.get_connection() as first_conn:
                with queue.get_connection() as second_conn:
                    assert first_conn is second_conn, (
                        "Sequential calls in same thread should return same connection"
                    )

            connections = []
            connection_ids = []
            lock = threading.Lock()
            barrier = threading.Barrier(5)  # Synchronize thread starts

            def get_connection():
                barrier.wait()  # Wait for all threads to be ready
                # Get connection twice to verify thread-local caching
                with queue.get_connection() as conn1:
                    with queue.get_connection() as conn2:
                        with lock:
                            connections.append((conn1, conn2))
                            connection_ids.append((id(conn1), id(conn2)))
                            # Within same thread, should be cached
                            assert conn1 is conn2, (
                                "Same thread should get cached connection"
                            )

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

            # Verify we got 5 pairs of connections
            assert len(connections) == 5, "Should have 5 connection pairs"

            # In persistent mode, each thread gets its own connection (no sharing)
            # This follows the principle "don't share across threads"
            unique_ids = {id_pair[0] for id_pair in connection_ids}
            assert len(unique_ids) == 5, (
                "Each thread should have its own connection (no sharing across threads)"
            )

            # But all should share the same underlying DBConnection object
            assert queue.conn is not None, "Should have persistent DBConnection"

            queue.close()

    def test_connection_type_consistency(self):
        """Test that both modes return BrokerDB for consistency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

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
            persistent_queue.close()

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

    def test_connection_error_handling(self):
        """Test that connection errors are properly handled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Test that exceptions in the context manager are propagated
            queue = Queue("test", db_path=db_path, persistent=False)
            try:
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
            finally:
                if hasattr(queue, "close"):
                    queue.close()

    def test_mixed_mode_operations(self):
        """Test that persistent and ephemeral queues can coexist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

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
                msgs = list(conn.peek_generator("persistent", with_timestamps=False))
                assert msgs == ["msg1"], "Should read persistent queue messages"

            persistent_q.close()

    def test_persistent_avoids_reconnection_overhead(self):
        """Test that persistent mode avoids reconnecting and re-running PRAGMAs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Track SQLiteRunner setup calls
            setup_calls = []
            original_setup = SQLiteRunner.setup

            def tracked_setup(self, phase):
                setup_calls.append((id(self), phase))
                return original_setup(self, phase)

            with patch.object(SQLiteRunner, "setup", tracked_setup):
                # Create a persistent queue
                queue = Queue("test", db_path=db_path, persistent=True)

                # First operation should trigger setup
                with queue.get_connection() as conn:
                    conn.write("test", "initial")

                # Record initial setup calls
                initial_setup_count = len(setup_calls)
                assert initial_setup_count > 0, (
                    "Should have some setup calls after first connection"
                )

                # Perform multiple additional operations
                for i in range(5):
                    with queue.get_connection() as conn:
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

                queue.close()

            # Now test ephemeral mode for comparison
            setup_calls.clear()

            with patch.object(SQLiteRunner, "setup", tracked_setup):
                # Create an ephemeral queue
                queue = Queue("test", db_path=db_path, persistent=False)

                # Perform multiple operations
                for i in range(3):
                    with queue.get_connection() as conn:
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

    def test_persistent_connection_reuse(self):
        """Test that persistent mode reuses the same database connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Track SQLiteRunner __init__ calls to detect new connections
            init_calls = []
            original_init = SQLiteRunner.__init__

            def tracked_init(self, *args, **kwargs):
                init_calls.append(id(self))
                return original_init(self, *args, **kwargs)

            with patch.object(SQLiteRunner, "__init__", tracked_init):
                # Test persistent mode
                queue = Queue("test", db_path=db_path, persistent=True)

                # First operation creates the connection
                with queue.get_connection() as conn:
                    conn.write("test", "initial")

                initial_runners = len(init_calls)
                assert initial_runners == 1, "Should create one SQLiteRunner"

                # Perform multiple operations in the same thread
                for i in range(5):
                    with queue.get_connection() as conn:
                        conn.write("test", f"msg{i}")

                # No new runners should have been created
                assert len(init_calls) == initial_runners, (
                    f"Persistent mode should reuse the same SQLiteRunner. "
                    f"Got {len(init_calls)} runners vs {initial_runners} initial"
                )

                queue.close()

            # Now test ephemeral mode for comparison
            init_calls.clear()

            with patch.object(SQLiteRunner, "__init__", tracked_init):
                queue = Queue("test_ephemeral", db_path=db_path, persistent=False)

                # Perform multiple operations
                for i in range(3):
                    with queue.get_connection() as conn:
                        conn.write("test_ephemeral", f"msg{i}")

                # Each operation should create a new runner
                assert len(init_calls) >= 3, (
                    f"Ephemeral mode should create new SQLiteRunner for each operation. "
                    f"Got {len(init_calls)} runners for 3 operations"
                )
