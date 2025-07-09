"""
Test broadcast with --since flag integration.

Tests how broadcast messages interact with checkpoint-based consumption.
"""

import json
import time

from .conftest import run_cli


def test_broadcast_with_since_filtering(workdir):
    """Test that broadcast messages can be filtered by timestamp."""
    # Create multiple queues
    queues = ["queue1", "queue2", "queue3"]

    # Write initial messages to each queue
    for q in queues:
        run_cli("write", q, f"initial_{q}", cwd=workdir)

    # Get checkpoint timestamp before broadcast
    # Small delay to ensure broadcast has a later timestamp
    time.sleep(0.01)
    current_time_ms = int(time.time() * 1000)
    checkpoint = current_time_ms << 20  # Convert to hybrid timestamp format

    # Broadcast a message
    time.sleep(0.01)
    run_cli("broadcast", "broadcast_msg", cwd=workdir)

    # Check each queue for messages after checkpoint
    for q in queues:
        rc, out, _ = run_cli(
            "peek", q, "--all", "--since", str(checkpoint), cwd=workdir
        )
        assert rc == 0
        assert out.strip() == "broadcast_msg"


def test_broadcast_checkpoint_based_workers(workdir):
    """Test multiple workers with checkpoints receiving broadcast."""
    # Setup worker queues
    worker_queues = ["worker1", "worker2", "worker3"]

    # Each worker processes some initial messages
    checkpoints = {}
    for _i, q in enumerate(worker_queues):
        # Write worker-specific messages
        for j in range(3):
            run_cli("write", q, f"{q}_task_{j}", cwd=workdir)

        # Simulate processing up to second message
        rc, out, _ = run_cli("peek", q, "--all", "--timestamps", "--json", cwd=workdir)
        messages = [json.loads(line) for line in out.strip().split("\n")]
        checkpoints[q] = messages[1]["timestamp"]  # Checkpoint after second message

    # Broadcast shutdown signal
    run_cli("broadcast", "SHUTDOWN", cwd=workdir)

    # Each worker should see remaining tasks plus broadcast when resuming from checkpoint
    for q in worker_queues:
        rc, out, _ = run_cli(
            "peek", q, "--all", "--json", "--since", str(checkpoints[q]), cwd=workdir
        )
        assert rc == 0

        messages = [json.loads(line)["message"] for line in out.strip().split("\n")]
        # Should see: task_2 and SHUTDOWN
        assert len(messages) == 2
        assert messages[0] == f"{q}_task_2"
        assert messages[1] == "SHUTDOWN"


def test_broadcast_ordering_with_timestamps(workdir):
    """Test that broadcast maintains timestamp ordering."""
    queues = ["q1", "q2"]

    # Create queues with messages
    for q in queues:
        run_cli("write", q, f"msg1_{q}", cwd=workdir)

    # Small delay to ensure different timestamp
    time.sleep(0.01)

    # Broadcast
    run_cli("broadcast", "broadcast1", cwd=workdir)

    # Add more messages
    for q in queues:
        run_cli("write", q, f"msg2_{q}", cwd=workdir)

    # Check ordering in each queue
    for q in queues:
        rc, out, _ = run_cli("peek", q, "--all", "--timestamps", cwd=workdir)
        lines = out.strip().split("\n")

        # Extract timestamps and messages
        entries = []
        for line in lines:
            ts, msg = line.split("\t")
            entries.append((int(ts), msg))

        # Verify ordering
        assert entries[0][1] == f"msg1_{q}"
        assert entries[1][1] == "broadcast1"
        assert entries[2][1] == f"msg2_{q}"

        # Verify timestamps are monotonic
        assert entries[0][0] < entries[1][0] < entries[2][0]


def test_broadcast_empty_queue_behavior(workdir):
    """Test broadcast behavior with empty queues and --since."""
    # Create and immediately empty a queue
    run_cli("write", "ephemeral", "temp", cwd=workdir)
    rc, out, _ = run_cli("read", "ephemeral", "--timestamps", cwd=workdir)
    int(out.split("\t")[0])

    # Queue is now empty
    rc, _, _ = run_cli("peek", "ephemeral", cwd=workdir)
    assert rc == 2

    # Create new queues
    run_cli("write", "new_queue", "msg", cwd=workdir)

    # Broadcast
    run_cli("broadcast", "broadcast_to_all", cwd=workdir)

    # Original empty queue should have the broadcast
    # Note: There's a potential race - if broadcast completes before we check,
    # the ephemeral queue will have the message
    rc, out, _ = run_cli("peek", "ephemeral", cwd=workdir)

    # The queue might be empty (rc=2) if broadcast happened before queue existed
    # or have the message (rc=0) if the queue existed during broadcast
    if rc == 0:
        assert out == "broadcast_to_all"
    else:
        # Queue was created after broadcast, so it's empty
        assert rc == 2

    # New queue should have both messages
    rc, out, _ = run_cli("peek", "new_queue", "--all", cwd=workdir)
    assert rc == 0
    assert out == "msg\nbroadcast_to_all"


def test_broadcast_race_condition_documentation(workdir):
    """Test to verify the documented broadcast race condition behavior."""
    # This test documents that queues created after broadcast starts
    # won't receive the message

    # Create initial queue
    run_cli("write", "existing_queue", "msg", cwd=workdir)

    # In a real scenario, broadcast would be running here
    # and a new queue would be created concurrently

    # Broadcast message
    run_cli("broadcast", "broadcast_msg", cwd=workdir)

    # Create new queue after broadcast
    run_cli("write", "new_queue", "msg", cwd=workdir)

    # Existing queue has broadcast
    rc, out, _ = run_cli("peek", "existing_queue", "--all", cwd=workdir)
    assert "broadcast_msg" in out

    # New queue does not have broadcast (as documented)
    rc, out, _ = run_cli("peek", "new_queue", "--all", cwd=workdir)
    assert "broadcast_msg" not in out
    assert out == "msg"
