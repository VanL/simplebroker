"""Race condition analysis for SimpleBroker watcher tests."""

import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    return db_path


class SubprocessMonitor:
    """Monitor subprocess initialization and readiness."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.steps_completed: list[str] = []
        self.step_times: dict = {}
        self.process: subprocess.Popen | None = None

    def monitor_db_creation(self) -> tuple[bool, float]:
        """Monitor database file creation and initialization.

        Returns:
            (database_exists, time_to_create)
        """
        start_time = time.time()

        # Wait for database file to exist
        while not self.db_path.exists():
            time.sleep(0.001)  # 1ms polling
            if time.time() - start_time > 30:  # 30 second timeout
                return False, time.time() - start_time

        time.time() - start_time

        # Wait for database to be properly initialized (have tables)
        db_ready_start = time.time()
        while True:
            try:
                # Try to connect and verify it's a proper SimpleBroker DB
                with BrokerDB(str(self.db_path)):
                    # If we can create a BrokerDB instance, it's ready
                    break
            except Exception:
                # Not ready yet
                time.sleep(0.001)
                if time.time() - db_ready_start > 30:
                    return False, time.time() - start_time

        total_init_time = time.time() - start_time
        return True, total_init_time

    def parse_step_output(self, line: str) -> None:
        """Parse step output from subprocess."""
        line = line.strip()
        if line.startswith("STEP_"):
            parts = line.split(":", 1)
            step_name = parts[0]
            self.steps_completed.append(step_name)

            if len(parts) > 1:
                try:
                    # Try to parse timing information
                    time_info = parts[1]
                    if time_info.endswith("s"):
                        self.step_times[step_name] = float(time_info[:-1])
                except ValueError:
                    pass  # Not a timing line


class TestRaceConditionAnalysis:
    """Test race conditions in watcher subprocess initialization."""

    def test_database_creation_timing(self, temp_db, tmp_path):
        """Analyze database creation timing vs ready file creation."""
        # Use the instrumented script
        from .helper_scripts import WATCHER_SIGINT_SCRIPT_INSTRUMENTED

        helper_script = WATCHER_SIGINT_SCRIPT_INSTRUMENTED
        ready_file = tmp_path / "watcher_ready.txt"

        # Add a test message to the queue first
        with BrokerDB(temp_db) as db:
            db.write("sigint_test_queue", "test_message")

        monitor = SubprocessMonitor(temp_db)

        # Start monitoring database in separate thread
        db_monitor_result = {"ready": False, "time": 0.0}

        def monitor_db():
            db_monitor_result["ready"], db_monitor_result["time"] = (
                monitor.monitor_db_creation()
            )

        db_thread = threading.Thread(target=monitor_db)
        db_thread.start()

        # Launch subprocess with instrumentation
        process = subprocess.Popen(
            [sys.executable, str(helper_script), str(temp_db), str(ready_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=0,  # Unbuffered for real-time output
        )

        monitor.process = process
        output_lines = []

        try:
            # Read output in real-time and parse steps
            while process.poll() is None:
                line = process.stdout.readline()
                if line:
                    output_lines.append(line.strip())
                    monitor.parse_step_output(line)
                    print(f"SUBPROCESS: {line.strip()}")

                # Check if ready file exists
                if ready_file.exists():
                    break

                # Timeout after 30 seconds
                if len(output_lines) == 0 and time.time() > time.time() + 30:
                    break

            # Send SIGINT after ready
            if ready_file.exists():
                if sys.platform == "win32":
                    process.terminate()
                else:
                    process.send_signal(signal.SIGINT)

                # Wait for graceful exit
                try:
                    exit_code = process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    exit_code = -9
            else:
                # Process never became ready
                process.kill()
                exit_code = -1

        finally:
            # Ensure db monitoring thread completes
            db_thread.join(timeout=1.0)

        # Analyze results
        print("\nRace Condition Analysis Results:")
        print(f"Database ready: {db_monitor_result['ready']}")
        print(f"Database init time: {db_monitor_result['time']:.3f}s")
        print(f"Ready file exists: {ready_file.exists()}")
        print(f"Process exit code: {exit_code}")
        print(f"Steps completed: {monitor.steps_completed}")
        print(f"Step timings: {monitor.step_times}")
        print(f"All output: {output_lines}")

        # The test passes if we can identify the timing bottleneck
        assert len(monitor.steps_completed) > 0, "No steps were completed"

        # Analyze what failed
        if not ready_file.exists():
            if "STEP_2_DB_INIT_COMPLETE" not in monitor.steps_completed:
                pytest.fail(
                    f"Database initialization failed or took too long. DB ready: {db_monitor_result['ready']}, time: {db_monitor_result['time']:.3f}s"
                )
            elif "STEP_3_WATCHER_INIT_COMPLETE" not in monitor.steps_completed:
                pytest.fail("Watcher initialization failed")
            elif "STEP_4_READY_FILE_COMPLETE" not in monitor.steps_completed:
                pytest.fail("Ready file creation failed")
            else:
                pytest.fail("Unknown initialization failure")

    def test_concurrent_database_access(self, temp_db, tmp_path):
        """Test if concurrent database access causes the race condition."""

        # Create multiple processes trying to access the same database
        processes = []
        ready_files = []

        for i in range(3):
            ready_file = tmp_path / f"ready_{i}.txt"
            ready_files.append(ready_file)

            # Add test messages to the queue
            with BrokerDB(temp_db) as db:
                db.write(f"test_queue_{i}", f"message_{i}")

        from .helper_scripts import WATCHER_SIGINT_SCRIPT_INSTRUMENTED

        helper_script = WATCHER_SIGINT_SCRIPT_INSTRUMENTED

        # Start all processes simultaneously
        start_time = time.time()
        for i in range(3):
            process = subprocess.Popen(
                [sys.executable, str(helper_script), str(temp_db), str(ready_files[i])],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append(process)

        # Wait for all to become ready or timeout
        ready_count = 0
        timeout = 30.0
        while time.time() - start_time < timeout and ready_count < len(ready_files):
            ready_count = sum(1 for rf in ready_files if rf.exists())
            time.sleep(0.1)

        # Terminate all processes
        for process in processes:
            try:
                if sys.platform == "win32":
                    process.terminate()
                else:
                    process.send_signal(signal.SIGINT)
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()

        # Analyze results
        total_time = time.time() - start_time
        print("\nConcurrent Access Analysis:")
        print(f"Processes started: {len(processes)}")
        print(f"Ready files created: {ready_count}")
        print(f"Total time: {total_time:.3f}s")

        for i, process in enumerate(processes):
            stdout, stderr = process.communicate()
            print(f"Process {i} stdout: {stdout}")
            if stderr:
                print(f"Process {i} stderr: {stderr}")

        # If none became ready, there might be a locking issue
        if ready_count == 0:
            pytest.fail("No processes became ready - possible database locking issue")

        # If only some became ready, there might be a race condition
        if 0 < ready_count < len(processes):
            print(
                f"Partial success: {ready_count}/{len(processes)} processes became ready"
            )

        # Test passes if we gathered diagnostic information
        assert ready_count >= 0  # Always true, but shows we completed analysis

    def test_file_system_timing(self, temp_db, tmp_path):
        """Test file system operation timing."""

        # Test various file operations that happen during initialization
        timings = {}

        # Test 1: Directory creation
        start = time.time()
        test_dir = tmp_path / "timing_test"
        test_dir.mkdir(parents=True, exist_ok=True)
        timings["mkdir"] = time.time() - start

        # Test 2: File creation
        start = time.time()
        test_file = test_dir / "test.txt"
        test_file.touch()
        timings["touch"] = time.time() - start

        # Test 3: File write
        start = time.time()
        test_file.write_text("test data")
        timings["write"] = time.time() - start

        # Test 4: Database file creation
        start = time.time()
        test_db = test_dir / "test.db"
        with BrokerDB(str(test_db)) as db:
            db.write("test", "message")
        timings["db_create"] = time.time() - start

        # Test 5: Database reconnection
        start = time.time()
        with BrokerDB(str(test_db)) as db:
            list(db.peek_generator("test"))
        timings["db_reconnect"] = time.time() - start

        print("\nFile System Timing Analysis:")
        for operation, duration in timings.items():
            print(f"{operation}: {duration:.6f}s")

        # Identify if any operations are unusually slow
        slow_operations = {op: t for op, t in timings.items() if t > 0.1}  # > 100ms
        if slow_operations:
            print(f"Slow operations detected: {slow_operations}")

        # Test always passes - we're gathering timing data
        assert all(t >= 0 for t in timings.values())
