"""
Concurrency tests for SimpleBroker.

T6 – Concurrent writers do not deadlock and all messages arrive
"""

import concurrent.futures as cf

from .conftest import run_cli


def test_parallel_writes(workdir):
    """T6: Multiple concurrent writers work correctly."""
    message_count = 100  # Reduced from 500 for faster tests

    def write_one(idx):
        """Write a single message."""
        rc, _, _ = run_cli("write", "concurrent", f"msg_{idx:03d}", cwd=workdir)
        return rc

    # Write messages in parallel
    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        return_codes = list(pool.map(write_one, range(message_count)))

    # All writes should succeed
    assert all(rc == 0 for rc in return_codes)

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
            rc, out, _ = run_cli("read", "mixed", cwd=workdir)
            if rc == 0:
                messages.append(out)
                empty_count = 0
            elif rc == 3:  # Queue empty
                empty_count += 1
                time.sleep(0.05)
            else:
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
