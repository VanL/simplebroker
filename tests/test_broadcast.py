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


def test_broadcast_visible_to_new_connections(workdir):
    """Broadcast messages should be visible to new database connections."""
    # Create two queues with initial messages
    rc, _, _ = run_cli("write", "queue1", "initial1", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "queue2", "initial2", cwd=workdir)
    assert rc == 0

    # Broadcast a message to all queues
    rc, _, _ = run_cli("broadcast", "broadcast_msg", cwd=workdir)
    assert rc == 0

    # Important: Each CLI call creates a new BrokerDB connection
    # This tests that the broadcast was properly committed

    # Verify broadcast message exists in queue1 with new connection
    rc, out, _ = run_cli("read", "queue1", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["initial1", "broadcast_msg"]

    # Verify broadcast message exists in queue2 with new connection
    rc, out, _ = run_cli("read", "queue2", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["initial2", "broadcast_msg"]
