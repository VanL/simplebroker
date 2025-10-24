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


def test_broadcast_with_pattern(workdir):
    """Selective broadcast should only reach queues matching the glob."""
    rc, _, _ = run_cli("write", "alpha", "seed_a", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "beta", "seed_b", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "alerts", "seed_alert", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("broadcast", "--pattern", "a*", "notice", cwd=workdir)
    assert rc == 0

    rc, out, _ = run_cli("read", "alpha", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_a", "notice"]

    rc, out, _ = run_cli("read", "alerts", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_alert", "notice"]

    rc, out, _ = run_cli("read", "beta", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_b"]


def test_broadcast_with_pattern_no_matches(workdir):
    """Selective broadcast with no matches should return EXIT_QUEUE_EMPTY."""
    rc, _, _ = run_cli("write", "alpha", "seed_a", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("broadcast", "--pattern", "z*", "notice", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    rc, out, _ = run_cli("read", "alpha", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_a"]


def test_broadcast_no_queues(workdir):
    """Broadcast when no queues exist should return EXIT_QUEUE_EMPTY."""
    # Broadcast to no queues
    rc, _, _ = run_cli("broadcast", "hello", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY when no queues exist

    # Verify no queues were created
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    assert out == ""  # No queues


def test_broadcast_return_values(workdir):
    """Test that broadcast returns correct exit codes based on queue matches."""
    # Test 1: Broadcast with no queues returns EXIT_QUEUE_EMPTY
    rc, _, _ = run_cli("broadcast", "msg", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    # Test 2: Broadcast with queues returns EXIT_SUCCESS
    run_cli("write", "queue1", "m1", cwd=workdir)
    run_cli("write", "queue2", "m2", cwd=workdir)
    rc, _, _ = run_cli("broadcast", "broadcast_msg", cwd=workdir)
    assert rc == 0  # EXIT_SUCCESS

    # Test 3: Broadcast with pattern matching some queues returns EXIT_SUCCESS
    rc, _, _ = run_cli("broadcast", "--pattern", "queue1", "msg", cwd=workdir)
    assert rc == 0  # EXIT_SUCCESS

    # Test 4: Broadcast with pattern matching no queues returns EXIT_QUEUE_EMPTY
    rc, _, _ = run_cli("broadcast", "--pattern", "nonexistent*", "msg", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    # Test 5: Broadcast with * pattern matching existing queues returns EXIT_SUCCESS
    rc, _, _ = run_cli("broadcast", "--pattern", "*", "msg", cwd=workdir)
    assert rc == 0  # EXIT_SUCCESS


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
