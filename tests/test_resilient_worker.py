"""
Test resilient worker patterns with --since flag.

Tests the checkpoint-based consumption pattern for building robust workers.
"""

import json

from .conftest import run_cli


def test_resilient_worker_checkpoint_recovery(workdir):
    """Test checkpoint-based recovery pattern."""
    queue_name = "worker_queue"

    # Write initial messages
    for i in range(5):
        run_cli("write", queue_name, f"task_{i}", cwd=workdir)

    # Get all messages with timestamps to simulate processing
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--timestamps", "--json", cwd=workdir
    )
    assert rc == 0

    messages = [json.loads(line) for line in out.strip().split("\n")]
    assert len(messages) == 5

    # Simulate processing first 3 messages and saving checkpoint
    checkpoint = messages[2]["timestamp"]

    # Add more messages while "worker was down"
    for i in range(5, 8):
        run_cli("write", queue_name, f"task_{i}", cwd=workdir)

    # Resume from checkpoint - should only get messages after task_2
    rc, out, _ = run_cli(
        "peek",
        queue_name,
        "--all",
        "--json",
        "--timestamps",
        "--since",
        str(checkpoint),
        cwd=workdir,
    )
    assert rc == 0

    resumed_messages = [json.loads(line) for line in out.strip().split("\n")]
    # Should get task_3, task_4, task_5, task_6, task_7
    assert len(resumed_messages) == 5
    assert resumed_messages[0]["message"] == "task_3"
    assert resumed_messages[-1]["message"] == "task_7"


def test_batch_processing_with_checkpoint(workdir):
    """Test processing messages in batches with checkpoint updates."""
    queue_name = "batch_queue"
    batch_size = 3

    # Write 10 messages
    for i in range(10):
        run_cli("write", queue_name, f"msg_{i}", cwd=workdir)

    checkpoint = 0
    processed_count = 0

    # Process in batches using peek to see what's available
    while processed_count < 10:
        # Peek at available messages
        rc, out, _ = run_cli(
            "peek",
            queue_name,
            "--all",
            "--json",
            "--timestamps",
            "--since",
            str(checkpoint),
            cwd=workdir,
        )

        if rc == 2 or not out.strip():  # Empty queue
            break

        assert rc == 0
        available = [json.loads(line) for line in out.strip().split("\n") if line]

        if not available:
            break

        # Process batch_size messages
        batch = available[:batch_size]

        # Actually read the messages we're going to process
        for msg in batch:
            rc, out, _ = run_cli(
                "read", queue_name, "--json", "--timestamps", cwd=workdir
            )
            assert rc == 0
            read_msg = json.loads(out)
            assert read_msg["message"] == msg["message"]
            processed_count += 1
            checkpoint = read_msg["timestamp"]

    # All messages should have been processed
    assert processed_count == 10

    # Queue should be empty
    rc, _, _ = run_cli("peek", queue_name, cwd=workdir)
    assert rc == 2


def test_worker_failure_retry(workdir):
    """Test that failed messages can be retried."""
    queue_name = "retry_queue"

    # Write messages
    messages = ["good_1", "bad_message", "good_2", "good_3"]
    for msg in messages:
        run_cli("write", queue_name, msg, cwd=workdir)

    # Get timestamps for tracking
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--timestamps", "--json", cwd=workdir
    )
    all_msgs = [json.loads(line) for line in out.strip().split("\n")]

    # Process first message successfully
    checkpoint = all_msgs[0]["timestamp"]

    # Try to process from checkpoint (includes bad_message)
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--json", "--since", str(checkpoint), cwd=workdir
    )
    next_batch = [json.loads(line) for line in out.strip().split("\n")]

    # Simulate failure on bad_message - checkpoint NOT updated
    assert next_batch[0]["message"] == "bad_message"

    # On retry, we get the same messages again
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--json", "--since", str(checkpoint), cwd=workdir
    )
    retry_batch = [json.loads(line) for line in out.strip().split("\n")]

    # Should get exact same messages
    assert len(retry_batch) == len(next_batch)
    assert retry_batch[0]["message"] == "bad_message"


def test_concurrent_workers_with_checkpoints(workdir):
    """Test multiple workers processing with their own checkpoints."""
    queue_name = "shared_queue"

    # Write messages
    for i in range(20):
        run_cli("write", queue_name, f"job_{i}", cwd=workdir)

    # Worker 1 processes even messages
    # Worker 2 processes odd messages
    worker1_checkpoint = 0
    worker2_checkpoint = 0

    # Get all messages
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--timestamps", "--json", cwd=workdir
    )
    all_messages = [json.loads(line) for line in out.strip().split("\n")]

    # Simulate workers processing different subsets
    worker1_processed = []
    worker2_processed = []

    for msg in all_messages:
        job_num = int(msg["message"].split("_")[1])

        if job_num % 2 == 0:
            # Worker 1 processes even jobs
            if msg["timestamp"] > worker1_checkpoint:
                worker1_processed.append(msg["message"])
                worker1_checkpoint = msg["timestamp"]
        else:
            # Worker 2 processes odd jobs
            if msg["timestamp"] > worker2_checkpoint:
                worker2_processed.append(msg["message"])
                worker2_checkpoint = msg["timestamp"]

    # Verify each worker processed their subset
    assert len(worker1_processed) == 10
    assert len(worker2_processed) == 10
    assert all(int(msg.split("_")[1]) % 2 == 0 for msg in worker1_processed)
    assert all(int(msg.split("_")[1]) % 2 == 1 for msg in worker2_processed)


def test_checkpoint_with_empty_queue_recovery(workdir):
    """Test checkpoint behavior when queue becomes empty and refills."""
    queue_name = "refill_queue"

    # Initial messages
    run_cli("write", queue_name, "msg_1", cwd=workdir)
    run_cli("write", queue_name, "msg_2", cwd=workdir)

    # Read all and get checkpoint
    rc, out, _ = run_cli(
        "read", queue_name, "--all", "--timestamps", "--json", cwd=workdir
    )
    messages = [json.loads(line) for line in out.strip().split("\n")]
    checkpoint = messages[-1]["timestamp"]

    # Queue is now empty
    rc, _, _ = run_cli("peek", queue_name, cwd=workdir)
    assert rc == 2

    # Check from checkpoint - should be empty
    rc, _, _ = run_cli(
        "peek", queue_name, "--all", "--since", str(checkpoint), cwd=workdir
    )
    assert rc == 2

    # Add new messages
    run_cli("write", queue_name, "msg_3", cwd=workdir)
    run_cli("write", queue_name, "msg_4", cwd=workdir)

    # Check from checkpoint - should see only new messages
    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--json", "--since", str(checkpoint), cwd=workdir
    )
    assert rc == 0

    new_messages = [json.loads(line) for line in out.strip().split("\n")]
    assert len(new_messages) == 2
    assert new_messages[0]["message"] == "msg_3"
    assert new_messages[1]["message"] == "msg_4"
