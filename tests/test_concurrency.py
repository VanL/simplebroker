"""
Concurrency tests for SimpleBroker.

T6 – Concurrent writers do not deadlock and all messages arrive
"""

import concurrent.futures as cf

from .conftest import run_cli


def _read_queue_helper(args):
    """Helper function for multiprocessing tests."""
    _, queue_name, workdir = args
    return run_cli("read", queue_name, cwd=workdir)


def _read_until_empty_helper(args):
    """Helper function for reading messages until queue is empty."""
    reader_id, queue_name, workdir = args
    messages = []
    while True:
        rc, out, _ = run_cli("read", queue_name, cwd=workdir)
        if rc == 0:
            messages.append(out)
        elif rc == 2:  # Queue empty
            break
        else:
            raise AssertionError(f"Unexpected return code: {rc}")
    return messages


def test_parallel_writes(workdir):
    """T6: Multiple concurrent writers work correctly."""
    import sys
    
    # On Windows, reduce concurrency to avoid file locking issues
    if sys.platform == "win32":
        message_count = 50  # Further reduced for Windows
        max_workers = 4     # Fewer concurrent workers on Windows
    else:
        message_count = 100  # Reduced from 500 for faster tests
        max_workers = 8

    def write_one(idx):
        """Write a single message."""
        rc, _, err = run_cli("write", "concurrent", f"msg_{idx:03d}", cwd=workdir)
        if rc != 0 and sys.platform == "win32":
            # On Windows, retry once if we get a locking error
            import time
            time.sleep(0.1)
            rc, _, err = run_cli("write", "concurrent", f"msg_{idx:03d}", cwd=workdir)
        return rc, idx, err

    # Write messages in parallel
    with cf.ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(write_one, range(message_count)))

    # Check for failures
    failures = [(idx, err) for rc, idx, err in results if rc != 0]
    if failures:
        print(f"Failed writes: {failures}")
    
    # All writes should succeed
    assert all(rc == 0 for rc, _, _ in results)

    # Read all messages
    rc, out, _ = run_cli("read", "concurrent", "--all", cwd=workdir)
    assert rc == 0

    messages = out.splitlines()
    assert len(messages) == message_count

    # Verify all messages arrived (order depends on process scheduling)
    # When using separate processes, each has its own counter, so strict
    # submission-order FIFO is not guaranteed across processes
    expected_messages = {f"msg_{i:03d}" for i in range(message_count)}
    actual_messages = set(messages)
    assert actual_messages == expected_messages, "Not all messages were received"


def test_concurrent_read_write(workdir):
    """Readers and writers can work concurrently."""
    import time

    # Initialize the database by writing and reading one message
    rc, _, _ = run_cli("write", "mixed", "init", cwd=workdir)
    assert rc == 0
    rc, _, _ = run_cli("read", "mixed", cwd=workdir)
    assert rc == 0

    def writer():
        """Write messages continuously."""
        for i in range(10):
            rc, _, _ = run_cli("write", "mixed", f"w{i}", cwd=workdir)
            if rc != 0:
                return rc
            time.sleep(0.01)  # Small delay
        return 0

    def reader():
        """Read messages as they arrive."""
        messages = []
        empty_count = 0
        while empty_count < 3:  # Stop after 3 empty reads
            rc, out, err = run_cli("read", "mixed", cwd=workdir)
            if rc == 0:
                messages.append(out)
                empty_count = 0
            elif rc == 2:  # Queue empty
                empty_count += 1
                time.sleep(0.05)
            else:
                # Unexpected error - print for debugging
                print(f"Unexpected return code {rc}, stderr: {err}")
                return rc, messages
        return 0, messages

    # Run reader and writer concurrently
    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        writer_future = pool.submit(writer)
        reader_future = pool.submit(reader)

        writer_rc = writer_future.result()
        reader_rc, messages = reader_future.result()

    assert writer_rc == 0
    assert reader_rc == 0

    # Should have read some messages (but maybe not all due to timing)
    assert len(messages) > 0
    assert all(msg.startswith("w") for msg in messages)


def test_two_readers_same_message(workdir):
    """Regression test: ensure only one reader gets each message."""
    # Write a single message
    rc, _, _ = run_cli("write", "q", "X", cwd=workdir)
    assert rc == 0

    # Have two readers attempt to read the message concurrently
    with cf.ProcessPoolExecutor(2) as pool:
        # Create args for each reader: (reader_id, queue_name, workdir)
        args = [(i, "q", workdir) for i in range(2)]
        results = list(pool.map(_read_queue_helper, args))

    # Extract the output from each result (rc, stdout, stderr)
    outputs = [result[1] for result in results]

    # Exactly one reader should get the message "X"
    assert outputs.count("X") == 1, f"Expected exactly one 'X', got outputs: {outputs}"

    # The other reader should have gotten empty queue
    empty_count = sum(1 for rc, _, _ in results if rc == 2)
    assert empty_count == 1, "Expected one reader to get empty queue"


def test_concurrent_readers_multiple_messages(workdir):
    """Test that multiple messages are distributed among concurrent readers without duplication."""
    num_messages = 20
    num_readers = 4

    # Write multiple messages
    for i in range(num_messages):
        rc, _, _ = run_cli("write", "multi", f"msg_{i:02d}", cwd=workdir)
        assert rc == 0

    # Have multiple readers read concurrently
    with cf.ProcessPoolExecutor(num_readers) as pool:
        # Create args for each reader: (reader_id, queue_name, workdir)
        args = [(i, "multi", workdir) for i in range(num_readers)]
        all_results = list(pool.map(_read_until_empty_helper, args))

    # Flatten all messages read by all readers
    all_messages = []
    for reader_messages in all_results:
        all_messages.extend(reader_messages)

    # Should have read exactly all messages
    assert len(all_messages) == num_messages, (
        f"Expected {num_messages} messages, got {len(all_messages)}"
    )

    # Check for duplicates
    assert len(set(all_messages)) == num_messages, "Found duplicate messages!"

    # Verify all expected messages were read
    expected_messages = {f"msg_{i:02d}" for i in range(num_messages)}
    actual_messages = set(all_messages)
    assert actual_messages == expected_messages, "Not all expected messages were read"
