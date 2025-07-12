"""Test that --move and --since are mutually exclusive."""

import subprocess
import sys
from pathlib import Path


def test_move_since_mutual_exclusion(tmp_path: Path) -> None:
    """Test that --move and --since cannot be used together."""
    db_path = tmp_path / "test.db"

    # Try to use both --move and --since together
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simplebroker",
            "-f",
            str(db_path),
            "watch",
            "source",
            "--move",
            "destination",
            "--since",
            "2024-01-01",
        ],
        capture_output=True,
        text=True,
    )

    # Should fail with error
    assert result.returncode == 1
    assert "incompatible with --since" in result.stderr.lower()
    assert "--move drains all messages" in result.stderr.lower()


def test_move_without_since_works(tmp_path: Path) -> None:
    """Test that --move works without --since."""
    db_path = tmp_path / "test.db"

    # First write a message
    subprocess.run(
        [
            sys.executable,
            "-m",
            "simplebroker",
            "-f",
            str(db_path),
            "write",
            "source",
            "test message",
        ],
        check=True,
    )

    # Start move in background
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "simplebroker",
            "-f",
            str(db_path),
            "watch",
            "source",
            "--move",
            "destination",
            "--quiet",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Give it a moment to move the message
    import time

    time.sleep(1)

    # Terminate it (it would run forever waiting for new messages)
    proc.terminate()
    stdout, stderr = proc.communicate()

    # Should have moved the message
    assert "test message" in stdout

    # Verify message was moved
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simplebroker",
            "-f",
            str(db_path),
            "read",
            "destination",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "test message" in result.stdout


def test_watch_with_since_without_move_works(tmp_path: Path) -> None:
    """Test that watch with --since works when not using --move."""
    db_path = tmp_path / "test.db"

    # Write a message
    subprocess.run(
        [
            sys.executable,
            "-m",
            "simplebroker",
            "-f",
            str(db_path),
            "write",
            "source",
            "test message",
        ],
        check=True,
    )

    # Start watch with --since in background
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "simplebroker",
            "-f",
            str(db_path),
            "watch",
            "source",
            "--since",
            "0",  # From beginning
            "--peek",
            "--quiet",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Give it a moment to start
    import time

    time.sleep(0.5)

    # Terminate it
    proc.terminate()
    stdout, stderr = proc.communicate()

    # Should have seen the message
    assert "test message" in stdout
