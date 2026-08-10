"""Test security fixes."""

from pathlib import Path
from unittest.mock import patch

import pytest

from simplebroker import Queue, commands, target_for_directory
from simplebroker.cli import main

from .conftest import run_cli


def test_stdin_size_limit_streaming(workdir: Path):
    """Test that stdin size limit is enforced during streaming, not after loading all data."""
    # Create a large input that exceeds 10MB
    # Use a simple pattern that's easy to generate
    large_input = "x" * (11 * 1024 * 1024)  # 11MB of 'x' characters

    # Try to write via stdin
    code, _stdout, stderr = run_cli(
        "write", "test_queue", "-", cwd=workdir, stdin=large_input
    )

    # Should fail with size limit error
    assert code == 1
    assert "exceeds maximum size" in stderr.lower()


@pytest.mark.sqlite_only
def test_parent_traversal_rejects_valid_outside_broker(workdir: Path):
    invocation_dir = workdir / "project"
    invocation_dir.mkdir()
    outside = workdir / "outside.db"
    with Queue("existing", db_path=str(outside)) as queue:
        queue.write("preserved")

    code, stdout, stderr = run_cli(
        "--file",
        f"../{outside.name}",
        "write",
        "existing",
        "mutant",
        cwd=invocation_dir,
    )

    assert code == 1
    assert stdout == ""
    assert "parent directory references" in stderr.lower()
    with Queue("existing", db_path=str(outside)) as queue:
        assert list(queue.peek_generator()) == ["preserved"]


@pytest.mark.sqlite_only
def test_absolute_file_target_round_trips(workdir: Path):
    target = (workdir / "absolute.db").resolve()

    code, stdout, stderr = run_cli(
        "--file", str(target), "write", "queue", "message", cwd=workdir
    )
    assert code == 0, stderr
    assert stdout == ""

    code, stdout, stderr = run_cli("--file", str(target), "read", "queue", cwd=workdir)
    assert code == 0, stderr
    assert stdout == "message"


@pytest.mark.sqlite_only
def test_safe_path_within_directory(workdir: Path):
    """Test that legitimate paths within the directory still work."""
    # Create a subdirectory
    subdir = workdir / "data"
    subdir.mkdir()

    # These should work
    safe_paths = [
        ("--file", "custom.db"),
        ("--file", "data.db"),
        ("-f", "test.db"),
        ("--file=mydb.db",),
    ]

    for i, path_args in enumerate(safe_paths):
        if len(path_args) == 1:
            # Single argument with equals
            code, _stdout, stderr = run_cli(
                path_args[0], "write", "test_queue", f"message{i}", cwd=workdir
            )
        else:
            # Separate flag and value
            code, _stdout, stderr = run_cli(
                *path_args, "write", "test_queue", f"message{i}", cwd=workdir
            )

        assert code == 0, f"Safe path {path_args} should have succeeded: {stderr}"


def test_direct_argv_message_size_limit(
    workdir: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.chdir(workdir)
    monkeypatch.setitem(commands._config, "BROKER_MAX_MESSAGE_SIZE", 4)
    with patch(
        "sys.argv",
        ["broker", "-d", str(workdir), "write", "test_queue", "abcde"],
    ):
        assert main() == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "exceeds maximum size" in captured.err.lower()
    with Queue("test_queue", db_path=target_for_directory(workdir)) as queue:
        assert queue.peek_one() is None


def test_normal_sized_messages_work(workdir: Path):
    """Test that normal sized messages still work."""
    # Just under 10MB
    large_but_ok = "x" * (9 * 1024 * 1024)

    # Via stdin
    code, stdout, _stderr = run_cli(
        "write", "test_queue", "-", cwd=workdir, stdin=large_but_ok
    )
    assert code == 0

    # Direct
    code, stdout, _stderr = run_cli(
        "write", "test_queue", "normal message", cwd=workdir
    )
    assert code == 0

    # Read them back
    code, stdout, _stderr = run_cli("read", "test_queue", "--all", cwd=workdir)
    assert code == 0
    messages = stdout.strip().split("\n")
    assert len(messages) == 2
    assert messages[0] == large_but_ok
    assert messages[1] == "normal message"
