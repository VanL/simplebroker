"""
Test broadcast with --after flag integration.

Tests how broadcast messages interact with checkpoint-based consumption.
"""

import json

from .conftest import run_cli
from .helper_scripts.timing import wait_for_condition


def _queue_has_broadcast(workdir, queue_name, checkpoint):
    rc, out, _ = run_cli(
        "peek",
        queue_name,
        "--all",
        "--after",
        str(checkpoint + 1),
        cwd=workdir,
    )
    return rc == 0 and out.strip() == "broadcast_msg"


def _has_expected_messages(workdir, queue_name, expected_count):
    rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    if rc != 0:
        return False

    lines = [line for line in out.strip().split("\n") if line]
    return len(lines) >= expected_count


def test_broadcast_with_after_filtering(workdir):
    """Test that broadcast messages can be filtered by timestamp."""
    # Create multiple queues
    queues = ["queue1", "queue2", "queue3"]

    # Write initial messages to each queue
    for q in queues:
        run_cli("write", q, f"initial_{q}", cwd=workdir)

    # Get timestamp of last initial message to use as checkpoint
    rc, out, err = run_cli(
        "peek", queues[-1], "--all", "--timestamps", "--json", cwd=workdir
    )
    assert rc == 0, err
    initial_msg = json.loads(out.strip())
    checkpoint = initial_msg["timestamp"]

    # Broadcast a message after the checkpoint
    run_cli("broadcast", "broadcast_msg", cwd=workdir)

    # Check each queue for messages after checkpoint
    for q in queues:
        assert wait_for_condition(
            lambda q=q: _queue_has_broadcast(workdir, q, int(checkpoint)),  # type: ignore[misc]
            timeout=3.0,
            interval=0.05,
        )


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
            "peek", q, "--all", "--json", "--after", str(checkpoints[q]), cwd=workdir
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

    # Broadcast
    run_cli("broadcast", "broadcast1", cwd=workdir)

    # Add more messages
    for q in queues:
        run_cli("write", q, f"msg2_{q}", cwd=workdir)

    # Check ordering in each queue
    for q in queues:
        assert wait_for_condition(
            lambda q=q: _has_expected_messages(workdir, q, 3),  # type: ignore[misc]
            timeout=3.0,
            interval=0.05,
        )
        rc, out, err = run_cli("peek", q, "--all", "--timestamps", cwd=workdir)
        assert rc == 0, err
        lines = [line for line in out.strip().split("\n") if line]

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
    """A claimed-only queue remains an existing broadcast target."""
    # Create a queue, then leave it with one claimed row and no pending rows.
    run_cli("write", "ephemeral", "temp", cwd=workdir)
    rc, out, _ = run_cli("read", "ephemeral", "--timestamps", cwd=workdir)
    int(out.split("\t")[0])

    # The queue has no pending message but still exists.
    rc, _, _ = run_cli("peek", "ephemeral", cwd=workdir)
    assert rc == 2

    # Create new queues
    run_cli("write", "new_queue", "msg", cwd=workdir)

    rc, out, err = run_cli("broadcast", "broadcast_to_all", cwd=workdir)
    assert rc == 0, err
    assert out == ""

    # Claimed rows still keep the queue in the broadcast target set.
    rc, out, _ = run_cli("peek", "ephemeral", cwd=workdir)
    assert rc == 0
    assert out == "broadcast_to_all"

    rc, out, err = run_cli("stats", "ephemeral", "--json", cwd=workdir)
    assert rc == 0, err
    assert json.loads(out) == {
        "queue": "ephemeral",
        "pending": 1,
        "claimed": 1,
        "total": 2,
        "exists": True,
    }

    # New queue should have both messages
    rc, out, _ = run_cli("peek", "new_queue", "--all", cwd=workdir)
    assert rc == 0
    assert out == "msg\nbroadcast_to_all"
