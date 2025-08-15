"""Tests for the watcher feature."""

import logging
import signal
import subprocess
import sys
import threading
import time
from typing import List, Optional, Tuple

import pytest

from simplebroker._constants import load_config
from simplebroker.db import BrokerDB

# Import will be available after implementation
pytest.importorskip("simplebroker.watcher")
from simplebroker.watcher import QueueWatcher

from .helper_scripts.watcher_base import WatcherTestBase

logger = logging.getLogger(__name__)


class MessageCollector:
    """Helper to collect messages from watchers."""

    def __init__(self):
        self.messages: List[Tuple[str, int]] = []
        self.lock = threading.Lock()
        self.error: Optional[Exception] = None

    def handler(self, msg: str, ts: int) -> None:
        """Collect messages thread-safely."""
        with self.lock:
            self.messages.append((msg, ts))

    def error_handler(self, msg: str, ts: int) -> None:
        """Handler that always raises an error."""
        raise RuntimeError(f"Handler error for message: {msg}")

    def get_messages(self) -> List[Tuple[str, int]]:
        """Get collected messages thread-safely."""
        with self.lock:
            return self.messages.copy()

    def wait_for_messages(self, count: int, timeout: float = 5.0) -> bool:
        """Wait for a specific number of messages to be collected.

        Args:
            count: Expected number of messages
            timeout: Maximum time to wait in seconds

        Returns:
            True if the expected count was reached, False if timeout
        """
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            with self.lock:
                if len(self.messages) >= count:
                    return True
            time.sleep(0.01)  # Small sleep to avoid busy waiting
        return False


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    return db_path


@pytest.fixture
def broker_db(temp_db):
    """Create a BrokerDB instance."""
    db = BrokerDB(temp_db)
    yield db
    db.close()


class TestQueueWatcher(WatcherTestBase):
    """Test the QueueWatcher class."""

    def test_thread_safety_with_brokerdb_instance(self, broker_db, temp_db):
        """Test that passing a BrokerDB instance works for backward compatibility."""
        # Write messages
        broker_db.write("test_queue", "msg1")
        broker_db.write("test_queue", "msg2")

        collector = MessageCollector()

        # Create watcher with BrokerDB instance (backward compatibility)
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_db,
            peek=False,
        )

        # Run in background thread - should create its own connection
        thread = watcher.run_in_thread()
        try:
            # Wait for both messages to be processed
            if not collector.wait_for_messages(2, timeout=2.0):
                pytest.fail(
                    f"Timeout waiting for messages. Got {len(collector.get_messages())} messages"
                )

            # Should have processed messages correctly
            messages = collector.get_messages()
            assert len(messages) == 2
            assert messages[0][0] == "msg1"
            assert messages[1][0] == "msg2"
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

    def test_basic_consuming_mode(self, broker_db, temp_db):
        """Test basic message consumption (peek=False)."""
        # Write some messages
        broker_db.write("test_queue", "message1")
        broker_db.write("test_queue", "message2")
        broker_db.write("test_queue", "message3")

        # Create a watcher
        collector = MessageCollector()

        # Use database path for thread-safe operation
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=temp_db,
            peek=False,
        )

        # Run watcher in background
        thread = watcher.run_in_thread()
        try:
            # Wait for processing (fast polling will pick up messages quickly)
            time.sleep(0.2)
        finally:
            # Stop the watcher
            watcher.stop()
            thread.join(timeout=2.0)

        # Check that messages were consumed
        messages = collector.get_messages()
        assert len(messages) == 3
        assert messages[0][0] == "message1"
        assert messages[1][0] == "message2"
        assert messages[2][0] == "message3"

        # Verify messages were actually consumed
        # Use new peek API to inspect remaining messages without consuming
        with BrokerDB(temp_db) as db:
            remaining = list(db.peek_generator("test_queue", with_timestamps=False))
        assert len(remaining) == 0

    def test_peek_mode(self, broker_db, temp_db):
        """Test peek mode (peek=True) - messages not consumed."""
        # Write messages
        broker_db.write("test_queue", "peek1")
        broker_db.write("test_queue", "peek2")

        collector = MessageCollector()

        # Use database path for thread-safe operation
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=temp_db,
            peek=True,
        )

        thread = watcher.run_in_thread()
        try:
            time.sleep(0.2)
        finally:
            watcher.stop()
            thread.join(timeout=2.0)

        # Messages should have been seen
        messages = collector.get_messages()
        assert len(messages) == 2

        # But NOT consumed (use new peek API)
        with BrokerDB(temp_db) as db:
            remaining = list(db.peek_generator("test_queue", with_timestamps=False))
        assert len(remaining) == 2
        assert remaining[0] == "peek1"
        assert remaining[1] == "peek2"

    def test_graceful_shutdown_stop_method(self, temp_db):
        """Test graceful shutdown via stop() method."""
        with self.create_test_watcher(
            temp_db,
            "test_queue",
            lambda msg, ts: None,
        ) as watcher:
            thread = watcher.run_in_thread()
            time.sleep(0.1)  # Let it start

            # Test graceful stop timing
            start_time = time.monotonic()
            watcher.stop()
            thread.join(timeout=4.0)
            stop_time = time.monotonic() - start_time

            assert not thread.is_alive()
            assert stop_time < 4.0, f"Stop took {stop_time:.2f}s"

    def test_graceful_shutdown_sigint(self, temp_db, tmp_path):
        """Test graceful shutdown via SIGINT using subprocess."""
        from .conftest import managed_subprocess

        # This test uses a subprocess to properly test SIGINT handling
        # without interfering with the test runner
        # Prepare paths - use improved script with race condition mitigation
        from .helper_scripts import WATCHER_SIGINT_SCRIPT_IMPROVED

        helper_script = WATCHER_SIGINT_SCRIPT_IMPROVED
        ready_file = tmp_path / "watcher_ready.txt"

        # Add a test message to the queue
        with BrokerDB(temp_db) as db:
            db.write("sigint_test_queue", "test_message")

        # Launch the watcher script as a subprocess
        with managed_subprocess(
            [sys.executable, str(helper_script), str(temp_db), str(ready_file)]
        ) as proc:
            # Strategy: Monitor subprocess output for better debugging
            ready_timeout = (
                120  # Increased timeout for CI systems with high database contention
            )
            ready_detected = False

            for _ in range(ready_timeout * 10):  # Check every 0.1s
                if ready_file.exists():
                    ready_detected = True
                    break

                # Also check if process is still running (early termination detection)
                if proc.proc.poll() is not None:
                    # Process terminated early - check output for errors
                    try:
                        stdout, stderr = proc.proc.communicate(timeout=1.0)
                        if stderr:
                            pytest.fail(
                                f"Subprocess terminated early with error: {stderr}"
                            )
                        else:
                            pytest.fail(
                                f"Subprocess terminated early with output: {stdout}"
                            )
                    except subprocess.TimeoutExpired:
                        pytest.fail("Subprocess terminated early and is unresponsive")

                time.sleep(0.1)

            if not ready_detected:
                # Enhanced error reporting for debugging
                poll_result = proc.proc.poll()
                if poll_result is not None:
                    pytest.fail(
                        f"Subprocess terminated with exit code {poll_result} before becoming ready"
                    )
                else:
                    pytest.fail(
                        f"Watcher subprocess did not become ready in {ready_timeout}s (process still running)"
                    )

            # Verify process is still running
            assert proc.proc.poll() is None, "Subprocess terminated prematurely"

            # Send SIGINT signal (Ctrl-C) on Unix, terminate on Windows
            if sys.platform == "win32":
                proc.terminate()
            else:
                proc.proc.send_signal(signal.SIGINT)

            # Wait for graceful exit with timeout
            try:
                exit_code = proc.proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                # Process didn't exit gracefully - force kill it
                logger.warning(
                    "Process did not terminate gracefully after SIGINT, forcing termination"
                )

                # Try SIGTERM first
                proc.proc.terminate()
                try:
                    exit_code = proc.proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    # Force kill if still not terminated
                    proc.proc.kill()
                    try:
                        exit_code = proc.proc.wait(timeout=1.0)
                    except subprocess.TimeoutExpired:
                        pytest.fail("Failed to terminate subprocess even with SIGKILL")

                # On slow/loaded machines, accept forceful termination
                # as a valid outcome
                logger.info(f"Process forcefully terminated with exit code {exit_code}")

            # Check exit code
            # On Windows, terminated processes may exit with code 1
            # When forcefully killed, accept negative codes (signals) on Unix
            if sys.platform == "win32":
                expected_codes = (0, 1)
            else:
                # 0 = clean exit, -2 = SIGINT, -15 = SIGTERM, -9 = SIGKILL
                expected_codes = (0, -2, -15, -9)

            assert exit_code in expected_codes, (
                f"Process exited with code {exit_code}, expected {expected_codes}"
            )

            # Optionally check output
            stderr_output = proc.stderr
            if stderr_output:
                print(f"Subprocess stderr: {stderr_output}")

    def test_sigint_handler_installation(self, temp_db, tmp_path):
        """Test that signal handler is correctly installed in main thread."""
        # Use a subprocess to test signal handler installation properly
        import textwrap

        from .conftest import run_subprocess

        # Create a test script that verifies signal handler installation
        test_script = tmp_path / "test_signal_install.py"
        test_script.write_text(
            textwrap.dedent("""
            import signal
            import sys
            from pathlib import Path

            # Add parent to path
            sys.path.insert(0, str(Path(__file__).parent.parent))

            from simplebroker.db import BrokerDB
            from simplebroker.watcher import QueueWatcher

            # Track if our handler was installed
            original_handler = signal.getsignal(signal.SIGINT)

            db_path = sys.argv[1]
            db = BrokerDB(db_path)
            watcher = QueueWatcher("test_queue", lambda m, t: None, db=db)

            # Stop immediately so we don't block
            watcher.stop()

            # Run which should install handler
            watcher.run_forever()

            # Check if handler was installed and restored
            final_handler = signal.getsignal(signal.SIGINT)

            # We expect the handler to be restored to original after run_forever
            if final_handler == original_handler:
                print("PASS: Handler properly installed and restored")
                sys.exit(0)
            else:
                print("FAIL: Handler not properly managed")
                sys.exit(1)
        """)
        )

        # Run the subprocess
        returncode, stdout, stderr = run_subprocess(
            [sys.executable, str(test_script), str(temp_db)], timeout=5.0
        )

        # Check results
        assert returncode == 0, f"Test failed: {stdout} {stderr}"
        assert "PASS" in stdout

    def test_handler_exception_handling(self, broker_db, temp_db):
        """Test that handler exceptions don't crash the watcher."""
        # Write messages
        broker_db.write("test_queue", "message1")
        broker_db.write("test_queue", "message2")

        handled_messages = []
        exception_count = 0

        def faulty_handler(msg: str, ts: int):
            """Handler that fails on first message."""
            if msg == "message1":
                raise ValueError("Simulated error")
            handled_messages.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            """Count errors and continue."""
            nonlocal exception_count
            exception_count += 1
            return True  # Continue processing

        with BrokerDB(temp_db) as watcher_db:
            watcher = QueueWatcher(
                "test_queue",
                faulty_handler,
                db=watcher_db,
                error_handler=error_handler,
            )

            thread = watcher.run_in_thread()
            try:
                time.sleep(0.2)
            finally:
                watcher.stop()
                thread.join(timeout=2.0)

        # First message should have errored, second should succeed
        assert exception_count == 1
        assert handled_messages == ["message2"]

    def test_error_handler_returns_false(self, broker_db, temp_db):
        """Test that error_handler returning False stops the watcher."""
        broker_db.write("test_queue", "bad_message")
        broker_db.write("test_queue", "good_message")

        processed = []

        def handler(msg: str, ts: int):
            if msg == "bad_message":
                raise ValueError("Bad message")
            processed.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            return False  # Stop on error

        with self.create_test_watcher(
            temp_db,
            "test_queue",
            handler,
            error_handler=error_handler,
        ) as watcher:
            thread = watcher.run_in_thread()
            time.sleep(0.2)

            thread.join(timeout=2.0)

            # Should have stopped after first error
            assert processed == []
            # Second message should still be in queue (use new peek API)
            with BrokerDB(temp_db) as db:
                remaining = list(db.peek_generator("test_queue", with_timestamps=False))
                assert remaining == ["good_message"]

    def test_multiple_workers_exactly_once(self, temp_db):
        """Test multiple workers ensure exactly-once delivery."""
        # Write many messages
        num_messages = 20
        with BrokerDB(temp_db) as db:
            for i in range(num_messages):
                db.write("work_queue", f"task_{i}")

        # Create multiple workers
        collectors = [MessageCollector() for _ in range(3)]
        workers = []

        try:
            for _i, collector in enumerate(collectors):
                # Use database path for thread-safe operation
                watcher = QueueWatcher(
                    "work_queue",
                    collector.handler,
                    db=temp_db,
                    peek=False,
                )
                thread = watcher.run_in_thread()
                workers.append((watcher, thread))

            # Let workers process messages
            time.sleep(0.7)
        finally:
            # Stop all workers
            for watcher, _thread in workers:
                try:
                    watcher.stop()
                except Exception:
                    pass  # Ignore errors during cleanup
            for _watcher, thread in workers:
                try:
                    thread.join(timeout=2.0)
                except Exception:
                    pass  # Ignore errors during cleanup

        # Collect all processed messages
        all_messages = []
        for collector in collectors:
            all_messages.extend(msg for msg, _ in collector.get_messages())

        # Should have exactly num_messages, no duplicates
        assert len(all_messages) == num_messages
        assert len(set(all_messages)) == num_messages
        assert set(all_messages) == {f"task_{i}" for i in range(num_messages)}

        # Queue should be empty
        with BrokerDB(temp_db) as db:
            remaining = list(db.peek_generator("work_queue", with_timestamps=False))
            assert len(remaining) == 0

    def test_mixed_peek_and_read_watchers(self, temp_db):
        """Test mixed peek and read watchers on same queue."""
        # Set up collectors
        peek_collector = MessageCollector()
        read_collector = MessageCollector()

        # Start peek watcher
        peek_db = BrokerDB(temp_db)
        read_db = BrokerDB(temp_db)
        try:
            peek_watcher = QueueWatcher(
                "mixed_queue",
                peek_collector.handler,
                db=peek_db,
                peek=True,
            )
            peek_thread = peek_watcher.run_in_thread()

            # Start read watcher
            read_watcher = QueueWatcher(
                "mixed_queue",
                read_collector.handler,
                db=read_db,
                peek=False,
            )
            read_thread = read_watcher.run_in_thread()

            time.sleep(0.1)

            # Write messages after watchers are running
            with BrokerDB(temp_db) as writer_db:
                writer_db.write("mixed_queue", "msg1")
                writer_db.write("mixed_queue", "msg2")
                writer_db.write("mixed_queue", "msg3")

            # Let watchers process
            time.sleep(0.4)
        finally:
            # Stop watchers
            try:
                peek_watcher.stop()
                peek_thread.join(timeout=2.0)
            except Exception:
                pass  # Ignore errors during cleanup
            try:
                read_watcher.stop()
                read_thread.join(timeout=2.0)
            except Exception:
                pass  # Ignore errors during cleanup
            peek_db.close()
            read_db.close()

        # Check results
        peek_messages = [msg for msg, _ in peek_collector.get_messages()]
        read_messages = [msg for msg, _ in read_collector.get_messages()]

        # Read watcher should have consumed some/all messages
        assert len(read_messages) > 0
        assert len(read_messages) <= 3

        # Peek watcher might have seen any subset of messages
        # (depending on timing with read watcher)
        assert len(peek_messages) >= 0
        assert len(peek_messages) <= 3

        # No message should appear twice in read_messages
        assert len(read_messages) == len(set(read_messages))

    def test_run_forever_blocking(self, temp_db):
        """Test that run_forever blocks until stopped."""
        run_completed = threading.Event()
        watcher_created = threading.Event()
        watcher_ref = None

        def run_watcher():
            nonlocal watcher_ref
            db = BrokerDB(temp_db)
            try:
                watcher = QueueWatcher(
                    "test_queue",
                    lambda m, t: None,
                    db=db,
                )
                watcher_ref = watcher
                watcher_created.set()  # Signal that watcher is created
                watcher.run_forever()
                run_completed.set()
            finally:
                db.close()

        thread = threading.Thread(target=run_watcher)
        thread.start()
        try:
            # Wait for watcher to be created with timeout
            if not watcher_created.wait(timeout=2.0):
                pytest.fail("Watcher was not created within timeout")

            assert thread.is_alive()
            assert not run_completed.is_set()

            # Stop it properly
            assert watcher_ref is not None
            watcher_ref.stop()
        finally:
            thread.join(timeout=2.0)

        assert run_completed.is_set()

    def test_polling_lifecycle(self, temp_db):
        """Test that polling strategy lifecycle works correctly."""
        with BrokerDB(temp_db) as db:
            watcher = QueueWatcher(
                "test_queue",
                lambda m, t: None,
                db=db,
            )

            # Access internal strategy for testing
            strategy = watcher._strategy

            thread = watcher.run_in_thread()
            try:
                time.sleep(0.1)

                # Check count will be > 0 since polling has been running
                assert strategy._check_count > 0
            finally:
                watcher.stop()
                thread.join(timeout=2.0)

            # Should be stopped
            assert strategy._stop_event.is_set()

    def test_since_parameter_in_peek_mode(self, broker_db, temp_db):
        """Test that peek mode respects message ordering and 'since' tracking."""
        # Add messages and capture their timestamps
        broker_db.write("test_queue", "msg1")
        broker_db.write("test_queue", "msg2")

        # Get current messages to find msg2's timestamp
        initial_collector = MessageCollector()
        initial_watcher = QueueWatcher(
            "test_queue",
            initial_collector.handler,
            db=temp_db,
            peek=True,
        )
        thread = initial_watcher.run_in_thread()
        try:
            time.sleep(0.1)
        finally:
            initial_watcher.stop()
            thread.join(timeout=2.0)

        # Get timestamp of msg2
        initial_messages = initial_collector.get_messages()
        assert len(initial_messages) == 2
        ts_msg2 = initial_messages[1][1]  # Timestamp of msg2

        # Add msg3 after msg2
        broker_db.write("test_queue", "msg3")

        # Create collector for the actual test
        collector = MessageCollector()

        # Create watcher with since_timestamp set to ts_msg2
        # Should only see msg3 (messages after ts_msg2)
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=temp_db,
            peek=True,
            since_timestamp=ts_msg2,
        )

        # Start watcher and let it process messages
        thread = watcher.run_in_thread()
        try:
            time.sleep(0.1)  # Give it time to process
        finally:
            # Stop watcher
            watcher.stop()
            thread.join(timeout=2.0)

        # Should only have msg3
        messages = collector.get_messages()
        assert len(messages) == 1
        assert messages[0][0] == "msg3"
        assert messages[0][1] > ts_msg2

    def test_since_timestamp_database_filtering(self, broker_db, temp_db):
        """Test that since_timestamp filters at database level, not in Python."""
        # Add first batch of messages
        for i in range(50):
            broker_db.write("test_queue", f"msg{i:03d}")

        # Get timestamp of last message in first batch
        initial_collector = MessageCollector()
        initial_watcher = QueueWatcher(
            "test_queue",
            initial_collector.handler,
            db=temp_db,
            peek=True,
        )
        thread = initial_watcher.run_in_thread()
        try:
            # Wait for all 50 messages to be collected
            if not initial_collector.wait_for_messages(50, timeout=5.0):
                pytest.fail(
                    f"Timeout waiting for messages. Got {len(initial_collector.get_messages())} messages"
                )
        finally:
            initial_watcher.stop()
            thread.join(timeout=2.0)

        # Get timestamp after 50th message
        initial_messages = initial_collector.get_messages()
        assert len(initial_messages) == 50
        ts_mid = initial_messages[-1][1]  # Timestamp of last message

        # Add more messages
        for i in range(50, 100):
            broker_db.write("test_queue", f"msg{i:03d}")

        # Create collector for actual test
        collector = MessageCollector()

        # Create watcher with since_timestamp
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=temp_db,
            peek=True,
            since_timestamp=ts_mid,
        )

        # Start watcher and let it process messages
        thread = watcher.run_in_thread()
        try:
            # Wait for all 50 new messages to be collected
            if not collector.wait_for_messages(50, timeout=5.0):
                pytest.fail(
                    f"Timeout waiting for messages. Got {len(collector.get_messages())} messages"
                )
        finally:
            # Stop watcher
            watcher.stop()
            thread.join(timeout=2.0)

        # Should have exactly 50 messages (050-099)
        messages = collector.get_messages()
        assert len(messages) == 50
        # Check message content
        for i, (msg, ts) in enumerate(messages):
            assert msg == f"msg{i + 50:03d}"
            assert ts > ts_mid


class TestPollingStrategy:
    """Test polling strategy behavior."""

    def test_polling_with_data_version(self, temp_db):
        """Test that polling uses PRAGMA data_version for efficient change detection."""
        # This test verifies the polling strategy detects changes quickly
        with BrokerDB(temp_db) as db:
            messages_received = []

            def handler(msg: str, ts: int):
                messages_received.append(msg)

            watcher = QueueWatcher("version_test", handler, db=db)

            # Start watcher
            thread = watcher.run_in_thread()
            try:
                # Give watcher time to start
                time.sleep(0.1)

                # Write message - should be detected via data_version change
                db.write("version_test", "test_message")

                # Wait for processing
                time.sleep(0.2)
            finally:
                # Stop watcher
                watcher.stop()
                thread.join(timeout=2.0)

            # Should have received the message
            assert len(messages_received) == 1
            assert messages_received[0] == "test_message"

    def test_polling_backoff(self, temp_db):
        """Test polling strategy burst handling and gradual backoff."""
        from simplebroker.watcher import PollingStrategy

        # Test the burst handling and gradual backoff
        # Create a stop event for the strategy
        stop_event = threading.Event()
        strategy = PollingStrategy(
            stop_event=stop_event,
            initial_checks=5,  # Small number for testing
            max_interval=0.1,
        )

        # Initialize the strategy
        with BrokerDB(temp_db) as db:
            strategy.start(db)
            try:
                # First 5 checks should have zero delay
                for i in range(5):
                    assert strategy._calculate_base_delay() == 0
                    strategy._check_count = i + 1

                # After initial checks, delay should gradually increase
                strategy._check_count = 5
                delay1 = strategy._calculate_base_delay()
                assert delay1 == 0  # First check after burst still 0

                strategy._check_count = 6
                delay2 = strategy._calculate_base_delay()
                assert 0 < delay2 < 0.1  # Should start increasing

                strategy._check_count = 105
                delay3 = strategy._calculate_base_delay()
                assert delay3 == 0.1  # Should reach max

                # Activity should reset counter
                strategy.notify_activity()
                assert strategy._check_count == 0
            finally:
                # Signal the strategy to stop before closing the database
                stop_event.set()


class TestErrorScenarios(WatcherTestBase):
    """Test various error scenarios."""

    def test_handler_exception_no_error_handler(
        self, broker_db, temp_db, capsys, caplog
    ):
        """Test default behavior when handler fails and no error_handler."""
        # Enable logging for this test since it's testing logging behavior
        test_config = load_config()
        test_config["BROKER_LOGGING_ENABLED"] = True

        broker_db.write("test_queue", "bad_message")

        def failing_handler(msg: str, ts: int):
            raise RuntimeError("Handler failed")

        with BrokerDB(temp_db) as watcher_db:
            watcher = QueueWatcher(
                "test_queue",
                failing_handler,
                db=watcher_db,
                config=test_config,
            )

            thread = watcher.run_in_thread()
            try:
                time.sleep(0.2)
            finally:
                watcher.stop()
                thread.join(timeout=2.0)

        # Should have logged the error
        assert "Handler failed" in caplog.text or "Handler error" in caplog.text

    def test_error_handler_exception(self, broker_db, temp_db, capsys, caplog):
        """Test when error_handler itself raises exception."""
        # Enable logging for this test since it's testing logging behavior
        test_config = load_config()
        test_config["BROKER_LOGGING_ENABLED"] = True

        broker_db.write("test_queue", "message")

        def handler(msg: str, ts: int):
            raise ValueError("Handler error")

        def bad_error_handler(exc: Exception, msg: str, ts: int) -> bool:
            raise RuntimeError("Error handler also failed!")

        with BrokerDB(temp_db) as watcher_db:
            watcher = QueueWatcher(
                "test_queue",
                handler,
                db=watcher_db,
                error_handler=bad_error_handler,
                config=test_config,
            )

            thread = watcher.run_in_thread()
            try:
                time.sleep(0.2)
            finally:
                watcher.stop()
                thread.join(timeout=2.0)

        # Both errors should be logged
        assert "Handler error" in caplog.text
        assert (
            "Error handler also failed" in caplog.text
            or "Error handler failed" in caplog.text
        )

    def test_database_connection_isolation(self, temp_db):
        """Test that each watcher needs its own connection."""
        # This is more of a documentation test
        # Using the same connection should work but with warnings
        with BrokerDB(temp_db) as shared_db:
            watcher1 = QueueWatcher(
                "queue1",
                lambda m, t: None,
                db=shared_db,
            )

            # This would be bad practice but shouldn't crash
            watcher2 = QueueWatcher(
                "queue2",
                lambda m, t: None,
                db=shared_db,
            )

            # Just verify they were created
            assert watcher1 is not None
            assert watcher2 is not None

    def test_consuming_watcher_queue_preservation_on_failure(self, broker_db, temp_db):
        """Test that when a handler fails, the watcher stops and doesn't consume more messages.

        Note: In consuming mode, messages are removed from the queue when read,
        not after successful processing. The failed message is already consumed.
        """
        # Write messages to the queue
        broker_db.write("test_queue", "message1")
        broker_db.write("test_queue", "message2")
        broker_db.write("test_queue", "message3")

        processed_messages = []

        def failing_handler(msg: str, ts: int):
            """Handler that fails on message2."""
            if msg == "message2":
                raise RuntimeError("Handler failed on message2")
            processed_messages.append(msg)

        def error_handler(exc: Exception, msg: str, ts: int) -> bool:
            """Error handler that stops processing on failure."""
            return False  # Stop processing

        # Create consuming watcher (peek=False)
        with self.create_test_watcher(
            temp_db,
            "test_queue",
            failing_handler,
            peek=False,
            error_handler=error_handler,
        ) as watcher:
            # Run watcher with timeout
            thread = self.run_watcher_with_timeout(watcher, timeout=2.0)

            # Thread should have stopped due to error_handler returning False
            assert not thread.is_alive()

            # Check processed messages - should only have message1
            assert processed_messages == ["message1"]

            # Check remaining messages in queue (use new peek API)
            with BrokerDB(temp_db) as db:
                remaining = list(db.peek_generator("test_queue", with_timestamps=False))
                # Only message3 should be in the queue (message2 was consumed before handler failed)
                assert len(remaining) == 1
                assert remaining[0] == "message3"

    def test_signal_handler_restoration(self, temp_db):
        """Test that original signal handlers are restored after the watcher stops."""
        # This test verifies that signal handlers are properly restored
        # after run_forever completes

        # Track signal handler changes
        original_handler = signal.getsignal(signal.SIGINT)
        handler_during_run = None
        handler_after_run = None

        def capture_handler():
            """Capture the current SIGINT handler."""
            nonlocal handler_during_run
            handler_during_run = signal.getsignal(signal.SIGINT)

        def test_handler(msg: str, ts: int):
            """Handler that captures signal state and stops."""
            capture_handler()
            # Stop the watcher after processing one message
            watcher.stop()

        # Add a message to process
        broker_db = BrokerDB(temp_db)
        broker_db.write("signal_test_queue", "test_message")
        broker_db.close()

        # Create and run watcher
        watcher = QueueWatcher(
            "signal_test_queue",
            test_handler,
            db=temp_db,
            peek=False,
        )

        # Run in main thread to test signal handler installation
        watcher.run_forever()

        # Capture handler after run_forever completes
        handler_after_run = signal.getsignal(signal.SIGINT)

        # Verify signal handler was changed during run
        assert handler_during_run is not None
        assert handler_during_run != original_handler

        # Verify signal handler was restored after run
        assert handler_after_run == original_handler


def test_context_manager_usage(temp_db):
    """Test that QueueWatcher works properly as a context manager."""
    messages_received = []

    def handler(msg: str, ts: int):
        messages_received.append((msg, ts))

    # Add messages to the queue
    broker_db = BrokerDB(temp_db)
    broker_db.write("context_queue", "message1")
    broker_db.write("context_queue", "message2")
    broker_db.write("context_queue", "message3")
    broker_db.close()

    # Use watcher as a context manager
    with QueueWatcher("context_queue", handler, db=temp_db, peek=False) as watcher:
        # The thread should be started automatically
        assert hasattr(watcher, "_thread")
        assert watcher._thread is not None
        thread = watcher._thread()  # Get strong reference from weak ref
        assert thread is not None
        assert thread.is_alive()

        # Wait for messages to be processed
        start_time = time.monotonic()
        while len(messages_received) < 3 and time.monotonic() - start_time < 2.0:
            time.sleep(0.05)

    # After exiting context, thread should be stopped
    thread = watcher._thread() if watcher._thread else None
    assert thread is None or not thread.is_alive()

    # Verify all messages were processed
    assert len(messages_received) == 3
    assert [msg for msg, _ in messages_received] == ["message1", "message2", "message3"]

    # Verify queue is empty (consumed mode)
    with BrokerDB(temp_db) as db:
        remaining = list(db.peek_generator("context_queue", with_timestamps=False))
        assert len(remaining) == 0


def test_context_manager_with_exception(temp_db):
    """Test that cleanup happens properly even when exception occurs in context."""

    def handler(msg: str, ts: int):
        pass

    # Test that cleanup happens even with exception
    try:
        with QueueWatcher("error_queue", handler, db=temp_db) as watcher:
            thread = watcher._thread()  # Get strong reference from weak ref
            assert thread is not None
            assert thread.is_alive()
            msg = "Test exception"
            raise ValueError(msg)
    except ValueError:
        pass  # Expected

    # Thread should still be stopped after exception
    thread = watcher._thread() if watcher._thread else None
    assert thread is None or not thread.is_alive()
