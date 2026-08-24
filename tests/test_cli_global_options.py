"""Test global option placement."""

from pathlib import Path

import pytest

from .conftest import run_cli


def test_registered_write_message_requires_explicit_escape(workdir: Path):
    code, stdout, stderr = run_cli("write", "q1", "--cleanup", cwd=workdir)
    assert code == 1
    assert stdout == ""
    assert "use --" in stderr.lower()

    code, stdout, stderr = run_cli("write", "q1", "--", "--cleanup", cwd=workdir)
    assert code == 0, stderr
    assert run_cli("read", "q1", cwd=workdir)[1] == "--cleanup"


@pytest.mark.parametrize(
    "token",
    ["--cleanup", "--json", "--after=1s", "--target=other", "-m123", "-d/tmp"],
)
def test_registered_broadcast_message_requires_explicit_escape(
    workdir: Path, token: str
):
    code, stdout, _stderr = run_cli("write", "q1", "seed", cwd=workdir)
    assert code == 0

    code, stdout, stderr = run_cli("broadcast", token, cwd=workdir)
    assert code == 1
    assert stdout == ""
    assert "use --" in stderr.lower()

    code, stdout, _stderr = run_cli("read", "q1", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "seed"

    code, stdout, stderr = run_cli("broadcast", "--", token, cwd=workdir)
    assert code == 0, stderr

    code, stdout, _stderr = run_cli("read", "q1", cwd=workdir)
    assert code == 0
    assert stdout.strip() == token


def test_global_options_before_subcommand(workdir: Path):
    """Global options work before the subcommand."""
    code, stdout, _stderr = run_cli("-q", "write", "q2", "msg2", cwd=workdir)
    assert code == 0
    assert stdout == ""

    code, stdout, _stderr = run_cli("write", "q3", "msg3", cwd=workdir)
    assert code == 0

    # Verify both messages
    code, stdout, _stderr = run_cli("list", cwd=workdir)
    assert code == 0
    assert stdout.splitlines() == ["q2", "q3"]


@pytest.mark.sqlite_only
def test_multiple_global_options_mixed(workdir: Path):
    """Test multiple global options before the subcommand."""
    code, stdout, _stderr = run_cli(
        "-f", "custom.db", "-q", "write", "q4", "msg4", cwd=workdir
    )
    assert code == 0
    assert stdout == ""

    code, stdout, _stderr = run_cli("-f", "custom.db", "read", "q4", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "msg4"


def test_version_flag_before_command(workdir: Path):
    """Test --version before a command exits before executing the command."""
    code, stdout, _stderr = run_cli("--version", "write", "dummy", "msg", cwd=workdir)
    assert code == 0
    assert "simplebroker" in stdout
    assert "write" not in stdout  # Should not execute write command

    code, stdout, _stderr = run_cli("--version", cwd=workdir)
    assert code == 0
    assert "simplebroker" in stdout


def test_cleanup_flag_after_subcommand_is_not_global(workdir: Path):
    """Post-command --cleanup is not hoisted into destructive global cleanup."""
    # Create a database first
    code, stdout, _stderr = run_cli("write", "q", "msg", cwd=workdir)
    assert code == 0

    code, stdout, _stderr = run_cli("list", "--cleanup", "-q", cwd=workdir)
    assert code != 0

    # Verify database was not cleaned up.
    code, stdout, _stderr = run_cli("list", cwd=workdir)
    assert code == 0
    assert stdout.splitlines() == ["q"]
