"""
Miscellaneous tests for SimpleBroker.

T5 – list queues and delete operations
"""

import os

import pytest

from .conftest import run_cli


def test_list_and_delete(workdir):
    """T5: list shows names, delete removes messages."""
    # Create two queues with messages
    rc, _, _ = run_cli("write", "q1", "x", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "q2", "y", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "q2", "z", cwd=workdir)
    assert rc == 0

    # list should show both queue names
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    lines = sorted(out.splitlines())
    assert lines == ["q1", "q2"]

    rc, out, _ = run_cli("list", "--stats", cwd=workdir)
    assert rc == 0
    lines = sorted(out.splitlines())
    assert lines == ["q1: 1", "q2: 2"]

    # Purge q1
    rc, _, _ = run_cli("delete", "q1", cwd=workdir)
    assert rc == 0

    # list should show q1 is gone
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    assert "q1" not in out
    assert out.splitlines() == ["q2"]

    # Purge all
    rc, _, _ = run_cli("delete", "--all", cwd=workdir)
    assert rc == 0

    # list should be empty
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    assert out == ""


def test_list_with_pattern_filter(workdir):
    """List should honor fnmatch-style patterns when provided."""
    rc, _, _ = run_cli("write", "alpha", "a1", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "alerts", "alert", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "beta", "b1", cwd=workdir)
    assert rc == 0

    rc, out, _ = run_cli("list", "--pattern", "a*", cwd=workdir)
    assert rc == 0
    assert sorted(out.splitlines()) == ["alerts", "alpha"]

    rc, out, _ = run_cli("list", "--pattern", "beta", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["beta"]

    rc, out, _ = run_cli("list", "--pattern", "z*", cwd=workdir)
    assert rc == 0
    assert out == ""


def test_peek_command(workdir):
    """Peek reads without removing messages."""
    # Write a message
    rc, _, _ = run_cli("write", "peek_test", "visible", cwd=workdir)
    assert rc == 0

    # Peek at it
    rc, out, _ = run_cli("peek", "peek_test", cwd=workdir)
    assert rc == 0
    assert out == "visible"

    # Peek again - message should still be there
    rc, out, _ = run_cli("peek", "peek_test", cwd=workdir)
    assert rc == 0
    assert out == "visible"

    # Read removes it
    rc, out, _ = run_cli("read", "peek_test", cwd=workdir)
    assert rc == 0
    assert out == "visible"

    # Now it's gone
    rc, _, _ = run_cli("peek", "peek_test", cwd=workdir)
    assert rc == 2  # Empty


def test_exit_codes(workdir):
    """Verify documented exit codes."""
    # Read from non-existent queue (now returns empty queue code)
    rc, _, _ = run_cli("read", "no_such_queue", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    # Read from empty queue (create it first)
    rc, _, _ = run_cli("write", "empty", "x", cwd=workdir)
    assert rc == 0
    rc, _, _ = run_cli("read", "empty", cwd=workdir)
    assert rc == 0
    rc, _, _ = run_cli("read", "empty", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY


@pytest.mark.sqlite_only
def test_ambient_broker_env_is_sanitized():
    """pytest_configure strips developer-ambient BROKER_* vars.

    BROKER_TEST_BACKEND is the bin/pytest-pg|redis channel and is
    allowlisted; anything else BROKER_-prefixed at session start is
    developer machine state that would leak into every run_cli
    subprocess.  Per-test monkeypatch.setenv still works (it runs after
    the scrub).
    """
    leaked = {k for k in os.environ if k.startswith("BROKER_")} - {
        "BROKER_TEST_BACKEND"
    }
    assert leaked == set(), f"ambient BROKER_* vars leaked into the suite: {leaked}"
