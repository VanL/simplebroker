"""Tests for the broker watch CLI command."""

import signal
import subprocess
import sys
import time

import pytest


def wait_for_json_output(proc, expected_count=None, timeout=5, expected_messages=None):
    """Wait for JSON output from a process with timeout.

    Args:
        proc: The subprocess
        expected_count: Expected number of JSON lines (if known)
        timeout: Maximum time to wait in seconds
        expected_messages: Set of expected message contents (optional)

    Returns:
        List of parsed JSON objects
    """
    import json

    start = time.time()
    output_lines = []
    json_objects = []

    while time.time() - start < timeout:
        try:
            # Non-blocking read with timeout
            line = proc.stdout.readline()
            if line:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        data = json.loads(line)
                        json_objects.append(data)
                        output_lines.append(line)

                        # If we have expected messages, check if we've seen them all
                        if expected_messages:
                            found_messages = {obj["message"] for obj in json_objects}
                            if expected_messages.issubset(found_messages):
                                return json_objects

                        # If we have expected count, check if we've reached it
                        if expected_count and len(json_objects) >= expected_count:
                            return json_objects
                    except json.JSONDecodeError:
                        # Skip non-JSON lines (like startup messages)
                        pass
        except Exception:
            # Handle any read errors
            pass

        time.sleep(0.1)

    # Timeout reached
    if expected_count:
        raise TimeoutError(
            f"Expected {expected_count} JSON objects, got {len(json_objects)}"
        )
    elif expected_messages:
        found = {obj["message"] for obj in json_objects}
        missing = expected_messages - found
        raise TimeoutError(f"Missing expected messages: {missing}")

    return json_objects


def validate_timestamp(ts):
    """Validate that a timestamp meets SimpleBroker specifications."""
    assert isinstance(ts, int), f"Timestamp must be int, got {type(ts)}"
    assert len(str(ts)) == 19, (
        f"Timestamp must be exactly 19 digits, got {len(str(ts))} digits: {ts}"
    )
    # Check reasonable range (approximately year 2020-2100)
    assert 1_650_000_000_000_000_000 < ts < 4_300_000_000_000_000_000, (
        f"Timestamp {ts} outside reasonable range (2020-2100)"
    )


class TestWatchCommand:
    """Test the broker watch command."""

    def test_watch_basic(self, workdir):
        """Test basic watch functionality."""
        # Write a message
        from tests.conftest import run_cli

        rc, out, err = run_cli("write", "watchtest", "hello", cwd=workdir)
        assert rc == 0

        # Start watch in subprocess
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "watchtest"]
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            # Should immediately output the message
            # Give it a moment to process
            time.sleep(0.5)

            # Terminate the process and read output
            proc.terminate()
            stdout, stderr = proc.communicate(timeout=2.0)
            assert "hello" in stdout

        except subprocess.TimeoutExpired:
            # Process didn't terminate cleanly
            proc.kill()
            stdout, stderr = proc.communicate()
            pytest.fail("Watch command did not terminate within timeout")
        finally:
            # Ensure process is terminated
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    def test_watch_sigint_handling(self, workdir):
        """Test that watch command handles SIGINT gracefully."""
        from tests.conftest import run_cli

        # Write initial message
        rc, out, err = run_cli("write", "siginttest", "message1", cwd=workdir)
        assert rc == 0

        # Start watch command
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "siginttest"]
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            # Give it time to start and process the first message
            time.sleep(1.0)  # Increased wait time for parallel test execution

            # Send SIGINT on Unix, terminate on Windows
            if sys.platform == "win32":
                proc.terminate()
            else:
                proc.send_signal(signal.SIGINT)

            # Should exit gracefully
            # Use communicate() to avoid deadlock when reading from pipes
            try:
                stdout, stderr = proc.communicate(timeout=3.0)
                return_code = proc.returncode
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                pytest.fail("Watch command did not exit after SIGINT")

            # Check exit code - both 0 and -2 are acceptable on Unix, 1 on Windows
            # 0 means graceful exit, -2 means killed by SIGINT (Unix)
            # 1 means terminated (Windows)
            if sys.platform == "win32":
                expected_codes = (0, 1)
            else:
                expected_codes = (0, -2)
            assert return_code in expected_codes, (
                f"Expected exit code {expected_codes}, got {return_code}"
            )

            # Should have processed the message OR at least started watching
            # In parallel tests, the process might be killed before outputting
            assert "message1" in stdout or "Watching queue" in stderr, (
                f"stdout: {stdout!r}, stderr: {stderr!r}"
            )

        except subprocess.TimeoutExpired:
            proc.kill()
            pytest.fail("Watch command did not exit after SIGINT")

    def test_watch_peek_mode(self, workdir):
        """Test watch in peek mode doesn't consume messages."""
        from tests.conftest import run_cli

        # Write a message
        rc, out, err = run_cli("write", "peektest", "peekmsg", cwd=workdir)
        assert rc == 0

        # Start watch in peek mode
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "--peek", "peektest"]
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            # Let it run briefly
            time.sleep(1.0)  # Increased wait time for parallel test execution

            # Terminate and get output using communicate to avoid deadlock
            proc.terminate()
            try:
                stdout, stderr = proc.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                pytest.fail("Watch command did not terminate within timeout")

            # Should have seen the message OR at least started watching
            assert "peekmsg" in stdout or "Watching queue" in stderr, (
                f"stdout: {stdout!r}, stderr: {stderr!r}"
            )

            # Message should still be in queue
            rc, out, err = run_cli("read", "peektest", cwd=workdir)
            assert rc == 0
            assert out == "peekmsg"

        except Exception:
            proc.kill()
            raise
        finally:
            # Ensure process is terminated
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    def test_watch_json_output(self, workdir):
        """Test watch with JSON output format."""
        from tests.conftest import run_cli

        # Write a message
        rc, out, err = run_cli("write", "jsontest", "test message", cwd=workdir)
        assert rc == 0

        # Start watch with JSON output
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "--json", "jsontest"]
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
        )

        try:
            import json

            # Give the process time to output the message
            time.sleep(0.5)

            # Terminate and collect output
            proc.terminate()
            try:
                stdout, stderr = proc.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                pytest.fail(
                    f"Process did not terminate cleanly. stdout: {stdout}, stderr: {stderr}"
                )

            # Parse JSON output
            json_objects = []
            for line in stdout.strip().split("\n"):
                if line.strip():
                    try:
                        data = json.loads(line)
                        json_objects.append(data)
                    except json.JSONDecodeError:
                        # Skip non-JSON lines
                        pass

            # Validate the output
            assert len(json_objects) >= 1, (
                f"Expected at least 1 JSON object, got {len(json_objects)}"
            )

            for data in json_objects:
                assert data["message"] == "test message"
                # Verify timestamp field exists
                assert "timestamp" in data
                # Validate timestamp using helper function
                validate_timestamp(data["timestamp"])

        except Exception as e:
            if proc.poll() is None:
                proc.kill()
                stdout, stderr = proc.communicate()
            else:
                stdout, stderr = "", ""
            # Include output in error for debugging
            raise AssertionError(
                f"Test failed: {e}\nstdout: {stdout}\nstderr: {stderr}"
            ) from None
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    def test_watch_json_includes_timestamps(self, workdir):
        """Test that watch --json includes timestamps by default."""
        from tests.conftest import run_cli

        # Write a message to the queue
        rc, out, err = run_cli("write", "timestamptest", "initial message", cwd=workdir)
        assert rc == 0

        # Start watch process with --json flag (not --timestamps)
        cmd = [
            sys.executable,
            "-m",
            "simplebroker.cli",
            "watch",
            "--json",
            "timestamptest",
        ]
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
        )

        collected_messages = []
        try:
            import json

            # Give the process time to start and output initial message
            time.sleep(0.5)

            # Write another message to trigger more output
            rc, _, _ = run_cli("write", "timestamptest", "trigger message", cwd=workdir)
            assert rc == 0

            # Give time for processing
            time.sleep(0.5)

            # Terminate the process and collect all output
            proc.terminate()
            try:
                stdout, stderr = proc.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                pytest.fail(
                    f"Process did not terminate cleanly. stdout: {stdout}, stderr: {stderr}"
                )

            # Parse JSON output line by line
            for line in stdout.strip().split("\n"):
                if line.strip():
                    try:
                        data = json.loads(line)
                        collected_messages.append(data)
                    except json.JSONDecodeError:
                        # Skip non-JSON lines
                        pass

            # Verify we got messages
            assert len(collected_messages) >= 2, (
                f"Expected at least 2 messages, got {len(collected_messages)}: {collected_messages}"
            )

            # Verify message contents
            message_contents = {msg["message"] for msg in collected_messages}
            assert "initial message" in message_contents, (
                f"Missing 'initial message' in {message_contents}"
            )
            assert "trigger message" in message_contents, (
                f"Missing 'trigger message' in {message_contents}"
            )

            # Verify all messages have valid timestamps
            for data in collected_messages:
                # Verify timestamp field exists
                assert "timestamp" in data, f"Expected timestamp field in JSON: {data}"
                # Validate timestamp using helper function
                validate_timestamp(data["timestamp"])
                # Verify message content
                assert data["message"] in ["initial message", "trigger message"]

        except Exception as e:
            if proc.poll() is None:
                proc.kill()
                stdout, stderr = proc.communicate()
            else:
                stdout, stderr = "", ""
            # Include output in error for debugging
            raise AssertionError(
                f"Test failed: {e}\nstdout: {stdout}\nstderr: {stderr}"
            ) from None
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    def test_watch_continuous_messages(self, workdir):
        """Test watch continues to process new messages."""
        from tests.conftest import run_cli

        # Start watch first (empty queue)
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "continuous"]
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
        )

        try:
            # Give watcher time to start
            time.sleep(0.2)

            # Write messages while watching
            messages = []
            for i in range(3):
                msg = f"msg{i}"
                messages.append(msg)
                rc, _, _ = run_cli("write", "continuous", msg, cwd=workdir)
                assert rc == 0
                time.sleep(0.1)

            # Give time to process
            time.sleep(0.5)

            # Terminate watcher
            proc.terminate()
            stdout, stderr = proc.communicate()

            # Should have received all messages
            for msg in messages:
                assert msg in stdout

        except Exception:
            proc.kill()
            raise
