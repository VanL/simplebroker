"""Tests for the broker watch CLI command."""

import signal
import sys
import time

from .conftest import managed_subprocess
from .helper_scripts.timestamp_validation import validate_timestamp


def wait_for_json_output(proc, expected_count=None, timeout=5, expected_messages=None):
    """Wait for JSON output from a process with timeout.

    Args:
        proc: The ManagedProcess instance
        expected_count: Expected number of JSON lines (if known)
        timeout: Maximum time to wait in seconds
        expected_messages: Set of expected message contents (optional)

    Returns:
        list of parsed JSON objects
    """
    import json

    start = time.monotonic()
    json_objects = []

    while time.monotonic() - start < timeout:
        # Get current stdout
        output = proc.stdout
        lines = output.strip().split("\n") if output else []

        # Parse all lines as JSON
        for line in lines[len(json_objects) :]:  # Only process new lines
            if line.strip():
                try:
                    data = json.loads(line)
                    json_objects.append(data)

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


class TestWatchCommand:
    """Test the broker watch command."""

    def test_watch_basic(self, workdir):
        """Test basic watch functionality."""
        # Write a message
        from .conftest import run_cli

        rc, out, err = run_cli("write", "watchtest", "hello", cwd=workdir)
        assert rc == 0

        # Start watch in subprocess
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "watchtest"]
        with managed_subprocess(cmd, cwd=workdir) as proc:
            # Wait for the message to appear in output
            assert proc.wait_for_output("hello", timeout=2.0)
            # Process automatically terminated on exit

    def test_watch_sigint_handling(self, workdir):
        """Test that watch command handles SIGINT gracefully."""
        from .conftest import run_cli

        # Write initial message
        rc, out, err = run_cli("write", "siginttest", "message1", cwd=workdir)
        assert rc == 0

        # Start watch command
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "siginttest"]
        with managed_subprocess(cmd, cwd=workdir) as proc:
            # Wait for it to start and process the first message
            proc.wait_for_output("message1", timeout=2.0)

            # Send SIGINT on Unix, terminate on Windows
            if sys.platform == "win32":
                proc.terminate()
            else:
                proc.proc.send_signal(signal.SIGINT)

            # Wait for process to exit
            proc.proc.wait(timeout=10.0)
            return_code = proc.proc.returncode

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
            stdout = proc.stdout
            stderr = proc.stderr
            assert "message1" in stdout or "Watching queue" in stderr, (
                f"stdout: {stdout!r}, stderr: {stderr!r}"
            )

    def test_watch_peek_mode(self, workdir):
        """Test watch in peek mode doesn't consume messages."""
        from .conftest import run_cli

        # Write a message
        rc, out, err = run_cli("write", "peektest", "peekmsg", cwd=workdir)
        assert rc == 0

        # Start watch in peek mode
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "--peek", "peektest"]
        with managed_subprocess(cmd, cwd=workdir) as proc:
            # Wait for the message to appear in output
            assert proc.wait_for_output("peekmsg", timeout=2.0)

            # Get output
            stdout = proc.stdout
            stderr = proc.stderr

            # Should have seen the message OR at least started watching
            assert "peekmsg" in stdout or "Watching queue" in stderr, (
                f"stdout: {stdout!r}, stderr: {stderr!r}"
            )

        # Message should still be in queue
        rc, out, err = run_cli("read", "peektest", cwd=workdir)
        assert rc == 0
        assert out == "peekmsg"

    def test_watch_json_output(self, workdir):
        """Test watch with JSON output format."""

        from .conftest import run_cli

        # Write a message
        rc, out, err = run_cli("write", "jsontest", "test message", cwd=workdir)
        assert rc == 0

        # Start watch with JSON output
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "--json", "jsontest"]
        with managed_subprocess(cmd, cwd=workdir) as proc:
            # Wait for JSON output
            json_objects = wait_for_json_output(proc, expected_count=1, timeout=2.0)

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

    def test_watch_json_includes_timestamps(self, workdir):
        """Test that watch --json includes timestamps by default."""
        from .conftest import run_cli

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
        with managed_subprocess(cmd, cwd=workdir) as proc:
            # Wait for initial message
            wait_for_json_output(proc, expected_count=1, timeout=2.0)

            # Write another message to trigger more output
            rc, _, _ = run_cli("write", "timestamptest", "trigger message", cwd=workdir)
            assert rc == 0

            # Wait for both messages
            collected_messages = wait_for_json_output(
                proc,
                expected_messages={"initial message", "trigger message"},
                timeout=2.0,
            )

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

    def test_watch_continuous_messages(self, workdir):
        """Test watch continues to process new messages."""
        from .conftest import run_cli

        # Start watch first (empty queue)
        cmd = [sys.executable, "-m", "simplebroker.cli", "watch", "continuous"]
        with managed_subprocess(cmd, cwd=workdir) as proc:
            assert proc.wait_for_output("Watching queue", timeout=2.0, stream="stderr")

            # Write messages while watching
            messages = []
            for i in range(3):
                msg = f"msg{i}"
                messages.append(msg)
                rc, _, _ = run_cli("write", "continuous", msg, cwd=workdir)
                assert rc == 0
                # Wait for each message to appear
                assert proc.wait_for_output(msg, timeout=1.0)

            # Get final output
            stdout = proc.stdout

            # Should have received all messages
            for msg in messages:
                assert msg in stdout
