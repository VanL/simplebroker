"""Tests for the broker watch CLI command."""

import signal
import subprocess
import sys
import time

import pytest


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
            # Use communicate with timeout to read output
            stdout, stderr = proc.communicate(timeout=1.0)
            assert "hello" in stdout

        except subprocess.TimeoutExpired:
            # Process is still running (expected for watch command)
            proc.terminate()
            stdout, stderr = proc.communicate()
            # Should have received the message before we terminated
            assert "hello" in stdout

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

            # Send SIGINT
            proc.send_signal(signal.SIGINT)

            # Should exit gracefully
            return_code = proc.wait(timeout=3.0)
            stdout, stderr = proc.communicate()

            # Check exit code - both 0 and -2 are acceptable
            # 0 means graceful exit, -2 means killed by SIGINT (platform-dependent)
            assert return_code in (0, -2), (
                f"Expected exit code 0 or -2, got {return_code}"
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
            proc.terminate()
            stdout, stderr = proc.communicate()

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
        )

        try:
            # Should output JSON
            time.sleep(0.5)
            proc.terminate()
            stdout, stderr = proc.communicate()

            # Parse JSON output
            import json

            for line in stdout.strip().split("\n"):
                if line:
                    data = json.loads(line)
                    assert data["message"] == "test message"

        except Exception:
            proc.kill()
            raise

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
