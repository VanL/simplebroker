"""Tests for Queue.get_connection context manager behavior."""

import concurrent.futures
import gc
import sqlite3
import tempfile
import threading
import time
import warnings
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

from simplebroker import Queue, _retry_policy
from simplebroker._exceptions import StopException
from simplebroker._retry_policy import _execute_connection_retry
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerConnection, BrokerDB, DBConnection
from tests.helper_scripts.timing import scale_timeout_for_ci

_THREAD_FUTURE_TIMEOUT = scale_timeout_for_ci(10.0)


def test_connection_retry_sleep_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture(wait: float, stop_event: threading.Event | None = None) -> bool:
        sleeps.append(wait)
        return True

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", capture)

    def fail() -> None:
        raise RuntimeError("connection failed")

    with pytest.raises(RuntimeError):
        _execute_connection_retry(fail, max_retries=3)

    assert sleeps == [2.0, 4.0]


def test_connection_stop_during_sleep_raises_stop_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()

    def interrupt_sleep(_wait: float, _ev: threading.Event | None) -> bool:
        return False

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", interrupt_sleep)

    def fail() -> None:
        raise RuntimeError("connection failed")

    with pytest.raises(StopException, match="Connection interrupted"):
        _execute_connection_retry(fail, max_retries=3, stop_event=stop_event)


class TestQueueConnectionManager:
    """Test the get_connection context manager behavior."""

    def test_cleanup_connections_releases_watcher_connection(
        self, tmp_path: Path
    ) -> None:
        queue = Queue("test", db_path=str(tmp_path / "test.db"))
        watcher_connection = Mock(spec=DBConnection)
        queue._watcher_conn = watcher_connection  # type: ignore[attr-defined]  # intentional dormant cleanup seam

        try:
            queue.cleanup_connections()
            watcher_connection.cleanup.assert_called_once_with()
            assert not hasattr(queue, "_watcher_conn")
        finally:
            queue.close()

    def test_persistent_mode_uses_cached_connection(self) -> None:
        """Context exit closes the reused handle without losing committed data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            with Queue("test", db_path=db_path, persistent=True) as queue:
                with queue.get_connection() as first:
                    first.write("test", "one")
                with queue.get_connection() as second:
                    second.write("test", "two")
                with queue.get_connection() as third:
                    assert list(
                        third.peek_generator("test", with_timestamps=False)
                    ) == ["one", "two"]

                assert first is second is third
                sqlite_connection = cast(Any, third)._runner.get_connection()
                assert sqlite_connection.execute("SELECT 1").fetchone() == (1,)

            with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
                sqlite_connection.execute("SELECT 1")
            with Queue("test", db_path=db_path) as observer:
                assert observer.peek_many(10, with_timestamps=False) == ["one", "two"]

    def test_ephemeral_mode_creates_new_connections(self) -> None:
        """Ephemeral leases are distinct and each supports broker operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            with Queue("test", db_path=db_path, persistent=False) as queue:
                with queue.get_connection() as first:
                    first.write("test", "one")
                with queue.get_connection() as second:
                    second.write("test", "two")
                with queue.get_connection() as third:
                    assert list(
                        third.peek_generator("test", with_timestamps=False)
                    ) == ["one", "two"]

                assert first is not second
                assert second is not third
                assert first is not third

    def test_ephemeral_connection_lifetime(self) -> None:
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

    def test_persistent_connection_lifetime(self) -> None:
        """A persistent handle remains usable until explicit queue close."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            queue = Queue("test", db_path=db_path, persistent=True)

            with queue.get_connection() as first:
                first.write("test", "before-close")
            with queue.get_connection() as second:
                assert second is first
                assert list(second.peek_generator("test", with_timestamps=False)) == [
                    "before-close"
                ]

            queue.close()
            with Queue("test", db_path=db_path) as observer:
                assert observer.read_one(with_timestamps=False) == "before-close"

    def test_thread_safety_ephemeral_mode(self) -> None:
        """Test that ephemeral mode is thread-safe (each thread gets its own connection)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            with Queue("test", db_path=db_path, persistent=False) as queue:
                connection_ids = []
                lock = threading.Lock()

                def get_connection() -> None:
                    with queue.get_connection() as conn, lock:
                        runner = cast(SQLiteRunner, cast(Any, conn)._runner)
                        connection_ids.append(runner.instance_id)

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

    def test_thread_safety_persistent_mode(self) -> None:
        """Test that persistent mode uses thread-local connections for safety."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            queue = None
            try:
                with Queue("test", db_path=db_path, persistent=True) as queue:
                    # First, verify same thread gets cached connection
                    with (
                        queue.get_connection() as first_conn,
                        queue.get_connection() as second_conn,
                    ):
                        assert first_conn is second_conn, (
                            "Sequential calls in same thread should return same connection"
                        )

                    barrier = threading.Barrier(5)  # Synchronize thread starts

                    def get_connection() -> tuple[
                        tuple[BrokerConnection, BrokerConnection], tuple[int, int]
                    ]:
                        barrier.wait()  # Wait for all threads to be ready
                        with (
                            queue.get_connection() as conn1,
                            queue.get_connection() as conn2,
                        ):
                            assert conn1 is conn2, (
                                "Same thread should get cached connection"
                            )
                            return (
                                (conn1, conn2),
                                (
                                    cast(
                                        SQLiteRunner, cast(Any, conn1)._runner
                                    ).instance_id,
                                    cast(
                                        SQLiteRunner, cast(Any, conn2)._runner
                                    ).instance_id,
                                ),
                            )

                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=5
                    ) as executor:
                        futures = [executor.submit(get_connection) for _ in range(5)]
                        results = [
                            future.result(timeout=_THREAD_FUTURE_TIMEOUT)
                            for future in futures
                        ]

                    connections = [pair for pair, _runner_ids in results]
                    connection_runner_ids = [
                        runner_ids for _pair, runner_ids in results
                    ]
                    assert len(connections) == 5
                    unique_runner_ids = {
                        runner_pair[0] for runner_pair in connection_runner_ids
                    }
                    assert len(unique_runner_ids) == 5, (
                        "Each thread should have its own connection (no sharing across threads)"
                    )

                    # But all should share the same underlying DBConnection object
                    assert queue.conn is not None, "Should have persistent DBConnection"
                connections.clear()  # Clear to release references
            finally:
                # Force garbage collection to clean up any remaining references
                gc.collect()

                # Short sleep for Windows file handle finalization
                time.sleep(0.2)

    def test_persistent_queue_cross_thread_close_does_not_warn(
        self, tmp_path: Path
    ) -> None:
        """Persistent queues should close cleanly from a different thread."""
        queue = Queue("test", db_path=str(tmp_path / "test.db"), persistent=True)
        queue.write("hello")
        assert list(queue.peek_generator()) == ["hello"]

        def close_queue() -> list[warnings.WarningMessage]:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", ResourceWarning)
                queue.close()
                gc.collect()
                return list(caught)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            caught_warnings = executor.submit(close_queue).result(
                timeout=_THREAD_FUTURE_TIMEOUT
            )
        resource_warnings = [
            warning
            for warning in caught_warnings
            if issubclass(warning.category, ResourceWarning)
        ]
        assert resource_warnings == []

    def test_persistent_queue_close_cleans_worker_thread_connections(
        self, tmp_path: Path
    ) -> None:
        """Closing a persistent queue cleans connections created by workers."""
        queue = Queue("test", db_path=str(tmp_path / "test.db"), persistent=True)

        def use_queue(index: int) -> None:
            queue.write(f"message-{index}")
            assert list(queue.peek_generator())

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(use_queue, index) for index in range(2)]
            for future in futures:
                future.result(timeout=_THREAD_FUTURE_TIMEOUT)

        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always", ResourceWarning)
            queue.close()
            gc.collect()

        resource_warnings = [
            warning
            for warning in caught_warnings
            if issubclass(warning.category, ResourceWarning)
        ]
        assert resource_warnings == []

    def test_connection_error_handling(self) -> None:
        """Test that connection errors are properly handled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Test that exceptions in the context manager are propagated
            with Queue("test", db_path=db_path, persistent=False) as queue:
                with (
                    pytest.raises(ValueError),
                    queue.get_connection() as conn,
                ):
                    raise ValueError("Test error")

                # Queue should still be usable after error
                with queue.get_connection() as conn:
                    conn.write("test", "message")

                # Verify the message was written
                with queue.get_connection() as conn:
                    messages = list(conn.peek_generator("test", with_timestamps=False))
                    assert messages == ["message"]

    def test_mixed_mode_operations(self) -> None:
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
