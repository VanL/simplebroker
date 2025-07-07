"""
Miscellaneous tests for SimpleBroker.

T5 – List queues and purge operations
"""

from .conftest import run_cli


def test_list_and_purge(workdir):
    """T5: List shows counts, purge removes messages."""
    # Create two queues with messages
    rc, _, _ = run_cli("write", "q1", "x", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "q2", "y", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "q2", "z", cwd=workdir)
    assert rc == 0

    # List should show both queues with counts
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    lines = sorted(out.splitlines())
    assert lines == ["q1: 1", "q2: 2"]

    # Purge q1
    rc, _, _ = run_cli("purge", "q1", cwd=workdir)
    assert rc == 0

    # List should show q1 is gone
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    assert "q1" not in out
    assert "q2: 2" in out

    # Purge all
    rc, _, _ = run_cli("purge", "--all", cwd=workdir)
    assert rc == 0

    # List should be empty
    rc, out, _ = run_cli("list", cwd=workdir)
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
