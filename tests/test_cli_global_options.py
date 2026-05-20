"""Test global option placement."""

from pathlib import Path

import pytest

from .conftest import run_cli


def test_free_form_message_can_look_like_global_option(workdir: Path):
    """Global-looking message text after write is treated as data."""
    code, stdout, stderr = run_cli("write", "q1", "--cleanup", cwd=workdir)
    assert code == 0

    # Verify message was written
    code, stdout, stderr = run_cli("read", "q1", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "--cleanup"


def test_broadcast_message_can_look_like_global_option(workdir: Path):
    """Global-looking message text after broadcast is treated as data."""
    code, stdout, stderr = run_cli("write", "q1", "seed", cwd=workdir)
    assert code == 0

    code, stdout, stderr = run_cli("broadcast", "--cleanup", cwd=workdir)
    assert code == 0

    code, stdout, stderr = run_cli("read", "q1", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "seed"

    code, stdout, stderr = run_cli("read", "q1", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "--cleanup"


def test_global_options_before_subcommand(workdir: Path):
    """Global options work before the subcommand."""
    code, stdout, stderr = run_cli("-q", "write", "q2", "msg2", cwd=workdir)
    assert code == 0
    assert stdout == ""

    code, stdout, stderr = run_cli("write", "q3", "msg3", cwd=workdir)
    assert code == 0

    # Verify both messages
    code, stdout, stderr = run_cli("list", cwd=workdir)
    assert code == 0
    assert stdout.splitlines() == ["q2", "q3"]


@pytest.mark.sqlite_only
def test_multiple_global_options_mixed(workdir: Path):
    """Test multiple global options before the subcommand."""
    code, stdout, stderr = run_cli(
        "-f", "custom.db", "-q", "write", "q4", "msg4", cwd=workdir
    )
    assert code == 0
    assert stdout == ""

    code, stdout, stderr = run_cli("-f", "custom.db", "read", "q4", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "msg4"


def test_version_flag_before_command(workdir: Path):
    """Test --version before a command exits before executing the command."""
    code, stdout, stderr = run_cli("--version", "write", "dummy", "msg", cwd=workdir)
    assert code == 0
    assert "simplebroker" in stdout
    assert "write" not in stdout  # Should not execute write command

    code, stdout, stderr = run_cli("--version", cwd=workdir)
    assert code == 0
    assert "simplebroker" in stdout


def test_cleanup_flag_after_subcommand_is_not_global(workdir: Path):
    """Post-command --cleanup is not hoisted into destructive global cleanup."""
    # Create a database first
    code, stdout, stderr = run_cli("write", "q", "msg", cwd=workdir)
    assert code == 0

    code, stdout, stderr = run_cli("list", "--cleanup", "-q", cwd=workdir)
    assert code != 0

    # Verify database was not cleaned up.
    code, stdout, stderr = run_cli("list", cwd=workdir)
    assert code == 0
    assert stdout.splitlines() == ["q"]
