"""Test fork safety for BrokerDB."""

import multiprocessing
import os
import sys
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB


def _worker_write(db_path: str, worker_id: int, count: int):
    """Worker that writes messages (module-level for pickling)."""
    # Each worker creates its own BrokerDB instance
    with BrokerDB(db_path) as db:
        for i in range(count):
            db.write("test_queue", f"worker{worker_id}_msg{i}")


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_safety_protection(workdir: Path):
    """Test that BrokerDB detects and prevents usage after fork."""
    db_path = workdir / "test.db"

    # Create DB in parent process
    db = BrokerDB(str(db_path))
    db.write("test_queue", "message1")

    # Fork a child process
    pid = os.fork()

    if pid == 0:  # Child process
        # Try to use the parent's DB instance - should fail
        try:
            db.write("test_queue", "message2")
            # If we get here, the test failed
            os._exit(1)
        except RuntimeError as e:
            # Expected error
            assert "forked process" in str(e)
            os._exit(0)
        except Exception:
            # Unexpected error
            os._exit(2)
    else:  # Parent process
        # Wait for child
        _, status = os.waitpid(pid, 0)
        exit_code = os.WEXITSTATUS(status)
        assert exit_code == 0  # Child exited successfully with expected error

        # Parent can still use the DB
        db.write("test_queue", "message3")
        messages = list(db.peek_generator("test_queue", with_timestamps=False))
        assert len(messages) == 2
        assert "message1" in messages
        assert "message3" in messages

        db.close()


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_new_instance_after_fork_works(workdir: Path):
    """Test that creating a new BrokerDB instance after fork works fine."""
    db_path = workdir / "test.db"

    # Create DB and write in parent
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "parent_message")

    pid = os.fork()

    if pid == 0:  # Child process
        # Create new instance in child - should work
        try:
            with BrokerDB(str(db_path)) as child_db:
                child_db.write("test_queue", "child_message")
                messages = list(
                    child_db.peek_generator("test_queue", with_timestamps=False)
                )
                assert len(messages) == 2
                assert "parent_message" in messages
                assert "child_message" in messages
            os._exit(0)
        except Exception:
            os._exit(1)
    else:  # Parent process
        # Wait for child
        _, status = os.waitpid(pid, 0)
        exit_code = os.WEXITSTATUS(status)
        assert exit_code == 0

        # Verify both messages exist
        with BrokerDB(str(db_path)) as db:
            messages = list(db.peek_generator("test_queue", with_timestamps=False))
            assert len(messages) == 2


def test_multiprocessing_with_separate_instances(workdir: Path):
    """Test that multiprocessing works when each process creates its own instance."""
    db_path = workdir / "test.db"

    # Start multiple workers
    processes = []
    try:
        for i in range(3):
            p = multiprocessing.Process(target=_worker_write, args=(str(db_path), i, 5))
            p.start()
            processes.append(p)

        # Wait for all to complete
        for p in processes:
            p.join(timeout=10)  # Add timeout to prevent hanging
    finally:
        # Ensure all processes are terminated even if test fails
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)

    # Verify all messages were written
    with BrokerDB(str(db_path)) as db:
        messages = list(db.peek_generator("test_queue", with_timestamps=False))
        assert len(messages) == 15  # 3 workers * 5 messages each

        # Verify all workers contributed
        for worker_id in range(3):
            worker_messages = [m for m in messages if f"worker{worker_id}_" in m]
            assert len(worker_messages) == 5
