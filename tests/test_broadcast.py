"""
Broadcast tests for SimpleBroker.

T4 – Broadcast fan-out to all existing queues
"""

from .conftest import run_cli


def test_broadcast(workdir):
    """T4: Broadcast sends message to all existing queues."""
    # Create two queues with initial messages
    rc, _, _ = run_cli("write", "alpha", "seed_a", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "beta", "seed_b", cwd=workdir)
    assert rc == 0

    # Broadcast a message to all queues
    rc, _, _ = run_cli("broadcast", "announcement", cwd=workdir)
    assert rc == 0

    # Check alpha queue has both messages
    rc, out, _ = run_cli("read", "alpha", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_a", "announcement"]

    # Check beta queue has both messages
    rc, out, _ = run_cli("read", "beta", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_b", "announcement"]


def test_broadcast_no_queues(workdir):
    """Broadcast when no queues exist should be a no-op."""
    # Broadcast to no queues
    rc, _, _ = run_cli("broadcast", "hello", cwd=workdir)
    assert rc == 0  # Should succeed even with no queues

    # Verify no queues were created
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    assert out == ""  # No queues
