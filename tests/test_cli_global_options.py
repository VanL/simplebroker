"""Test global options can appear anywhere in command line."""

from pathlib import Path

from .conftest import run_cli


def test_global_options_after_subcommand(workdir: Path):
    """Test that global options work when placed after the subcommand."""
    # Test -q after subcommand
    code, stdout, stderr = run_cli("write", "q1", "msg1", "-q", cwd=workdir)
    assert code == 0
    assert stdout == ""
    assert stderr == ""

    # Verify message was written
    code, stdout, stderr = run_cli("read", "q1", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "msg1"


def test_global_options_between_args(workdir: Path):
    """Test global options between subcommand arguments."""
    # Test -q between subcommand and args
    code, stdout, stderr = run_cli("write", "-q", "q2", "msg2", cwd=workdir)
    assert code == 0
    assert stdout == ""

    # Test --dir is not needed since we're using workdir fixture
    code, stdout, stderr = run_cli("write", "q3", "msg3", cwd=workdir)
    assert code == 0

    # Verify both messages
    code, stdout, stderr = run_cli("list", cwd=workdir)
    assert code == 0
    assert "q2: 1" in stdout
    assert "q3: 1" in stdout


def test_multiple_global_options_mixed(workdir: Path):
    """Test multiple global options in various positions."""
    # Mix global options before and after subcommand
    code, stdout, stderr = run_cli(
        "-f", "custom.db", "write", "q4", "msg4", "-q", cwd=workdir
    )
    assert code == 0
    assert stdout == ""

    # Verify with same mixed options
    code, stdout, stderr = run_cli("read", "-f", "custom.db", "q4", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "msg4"


def test_version_flag_anywhere(workdir: Path):
    """Test --version flag works in any position."""
    # After subcommand
    code, stdout, stderr = run_cli("write", "dummy", "msg", "--version", cwd=workdir)
    assert code == 0
    assert "simplebroker" in stdout
    assert "write" not in stdout  # Should not execute write command

    # Between args
    code, stdout, stderr = run_cli("write", "--version", "dummy", "msg", cwd=workdir)
    assert code == 0
    assert "simplebroker" in stdout


def test_cleanup_flag_anywhere(workdir: Path):
    """Test --cleanup flag works in any position."""
    # Create a database first
    code, stdout, stderr = run_cli("write", "q", "msg", cwd=workdir)
    assert code == 0

    # Cleanup with flag after subcommand
    code, stdout, stderr = run_cli("list", "--cleanup", "-q", cwd=workdir)
    assert code == 0

    # Verify database was removed (new write should create fresh db)
    code, stdout, stderr = run_cli("list", cwd=workdir)
    assert code == 0
    assert stdout.strip() == ""  # No queues
