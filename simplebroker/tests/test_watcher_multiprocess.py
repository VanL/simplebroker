"""Test suite for QueueWatcher multi-process scenarios.

Tests watcher behavior across process boundaries to ensure proper
isolation and coordination.
"""

import multiprocessing
import queue
import tempfile
import time
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher


def watcher_process(
    db_path: str,
    queue_name: str,
    result_queue: multiprocessing.Queue,
    control_queue: multiprocessing.Queue,
    process_id: int,
    enable_pre_check: bool = True,
) -> None:
    """Worker process that runs a QueueWatcher."""
    try:
        # Track messages processed
        processed = []

        def handler(msg, ts) -> None:
            processed.append((msg, ts))
            result_queue.put(("message", process_id, msg))

        # Create watcher
        class ProcessWatcher(QueueWatcher):
            def __init__(self, *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self._enable_pre_check = enable_pre_check

            def _has_pending_messages(self, db=None):
                if not self._enable_pre_check:
                    return True
                # Use the parent class implementation which handles DB properly
                return super()._has_pending_messages()

            def _drain_queue(self) -> None:
                if self._enable_pre_check:
                    if not self._has_pending_messages():
                        return
                super()._drain_queue()

        watcher = ProcessWatcher(queue_name, handler, db=db_path)

        # Signal ready
        result_queue.put(("ready", process_id, None))

        # Run until stop signal
        thread = watcher.run_in_thread()

        while True:
            try:
                command = control_queue.get(timeout=0.1)
                if command == "stop":
                    break
            except queue.Empty:
                continue

        watcher.stop()
        thread.join(timeout=2.0)

        # Send final stats
        result_queue.put(
            ("stats", process_id, {"processed": len(processed), "messages": processed}),
        )

    except Exception as e:
        result_queue.put(("error", process_id, str(e)))


def shutdown_test_process(
    db_path, queue_name, result_queue, control_queue, process_id
) -> None:
    """Process function for testing graceful shutdown."""
    try:
        processed_before_stop = []
        processed_after_stop = []
        stop_requested = False

        def handler(msg, ts) -> None:
            if stop_requested:
                processed_after_stop.append(msg)
            else:
                processed_before_stop.append(msg)

        watcher = QueueWatcher(queue_name, handler, db=db_path)
        thread = watcher.run_in_thread()

        result_queue.put(("ready", process_id, None))

        # Wait for stop signal
        while True:
            try:
                command = control_queue.get(timeout=0.1)
                if command == "stop":
                    stop_requested = True
                    watcher.stop()
                    break
            except queue.Empty:
                continue

        # Ensure thread stops
        thread.join(timeout=5.0)

        result_queue.put(
            (
                "shutdown_stats",
                process_id,
                {
                    "before_stop": len(processed_before_stop),
                    "after_stop": len(processed_after_stop),
                    "thread_alive": thread.is_alive(),
                },
            ),
        )

    except Exception as e:
        result_queue.put(("error", process_id, str(e)))


def lock_test_process(
    db_path, queue_name, result_queue, control_queue, process_id
) -> None:
    """Process function for testing database locking behavior."""
    try:
        from simplebroker._exceptions import OperationalError

        lock_attempts = 0
        lock_failures = 0

        def handler(msg, ts) -> None:
            pass

        class LockTrackingWatcher(QueueWatcher):
            def _drain_queue(self) -> None:
                nonlocal lock_attempts, lock_failures
                lock_attempts += 1
                try:
                    super()._drain_queue()
                except OperationalError as e:
                    if "locked" in str(e).lower():
                        lock_failures += 1
                    raise

        watcher = LockTrackingWatcher(queue_name, handler, db=db_path)
        thread = watcher.run_in_thread()

        result_queue.put(("ready", process_id, None))

        # Run for a fixed time
        start_time = time.monotonic()
        while time.monotonic() - start_time < 2.0:
            try:
                command = control_queue.get(timeout=0.1)
                if command == "stop":
                    break
            except queue.Empty:
                continue

        watcher.stop()
        thread.join(timeout=2.0)

        result_queue.put(
            (
                "lock_stats",
                process_id,
                {
                    "attempts": lock_attempts,
                    "failures": lock_failures,
                    "failure_rate": lock_failures / max(1, lock_attempts),
                },
            ),
        )

    except Exception as e:
        result_queue.put(("error", process_id, str(e)))


def test_multiprocess_single_queue() -> None:
    """Test multiple processes watching the same queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        broker = BrokerDB(db_path)

        # Create multiprocessing queues for communication
        result_queue = multiprocessing.Queue()
        control_queues = []
        processes = []

        try:
            # Start multiple watcher processes
            num_processes = 4

            for i in range(num_processes):
                control_queue = multiprocessing.Queue()
                control_queues.append(control_queue)

                p = multiprocessing.Process(
                    target=watcher_process,
                    args=(db_path, "shared_queue", result_queue, control_queue, i),
                )
                p.start()
                processes.append(p)

            # Wait for all processes to be ready
            ready_count = 0
            while ready_count < num_processes:
                msg_type, proc_id, data = result_queue.get(timeout=5.0)
                if msg_type == "ready":
                    ready_count += 1

            # Write messages
            num_messages = 100
            for i in range(num_messages):
                broker.write("shared_queue", f"message_{i}")

            # Collect processed messages
            processed_messages = []
            timeout_start = time.monotonic()

            while (
                len(processed_messages) < num_messages
                and time.monotonic() - timeout_start < 10
            ):
                try:
                    msg_type, proc_id, data = result_queue.get(timeout=0.1)
                    if msg_type == "message":
                        processed_messages.append((proc_id, data))
                except queue.Empty:
                    continue

            # Verify all messages were processed
            message_bodies = [msg for _, msg in processed_messages]
            assert len(message_bodies) == num_messages
            # Order is not guaranteed with multiple consumers, just verify all messages present
            assert set(message_bodies) == {f"message_{i}" for i in range(num_messages)}

            # Check distribution across processes
            process_counts = {}
            for proc_id, _ in processed_messages:
                process_counts[proc_id] = process_counts.get(proc_id, 0) + 1

            # At least one process should have processed messages
            # Note: With competitive consumption, there's no guarantee all processes get work
            assert len(process_counts) >= 1
            assert sum(process_counts.values()) == num_messages
        finally:
            # Stop all processes
            for control_queue in control_queues:
                try:
                    control_queue.put("stop")
                except Exception:
                    pass  # Queue may be closed already

            # Wait for processes to finish and ensure cleanup
            for p in processes:
                try:
                    p.join(timeout=5.0)
                    if p.is_alive():
                        p.terminate()
                        p.join()
                except Exception:
                    pass  # Process may already be terminated

            broker.close()


def test_multiprocess_separate_queues() -> None:
    """Test multiple processes each watching their own queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        broker = BrokerDB(db_path)

        num_processes = 5
        messages_per_queue = 20

        # Create communication queues
        result_queue = multiprocessing.Queue()
        control_queues = []
        processes = []

        try:
            # Start processes, each watching its own queue
            for i in range(num_processes):
                control_queue = multiprocessing.Queue()
                control_queues.append(control_queue)

                p = multiprocessing.Process(
                    target=watcher_process,
                    args=(db_path, f"queue_{i}", result_queue, control_queue, i),
                )
                p.start()
                processes.append(p)

            # Wait for ready
            ready_count = 0
            while ready_count < num_processes:
                msg_type, proc_id, data = result_queue.get(timeout=5.0)
                if msg_type == "ready":
                    ready_count += 1

            # Write messages to each queue
            for i in range(num_processes):
                for j in range(messages_per_queue):
                    broker.write(f"queue_{i}", f"queue_{i}_msg_{j}")

            # Collect results
            process_message_counts = dict.fromkeys(range(num_processes), 0)
            timeout_start = time.monotonic()
            expected_total = num_processes * messages_per_queue
            total_collected = 0

            while (
                total_collected < expected_total
                and time.monotonic() - timeout_start < 10
            ):
                try:
                    msg_type, proc_id, data = result_queue.get(timeout=0.1)
                    if msg_type == "message":
                        process_message_counts[proc_id] += 1
                        total_collected += 1
                except queue.Empty:
                    continue

            # Each process should have processed exactly its queue's messages
            for i in range(num_processes):
                assert process_message_counts[i] == messages_per_queue
        finally:
            # Stop processes
            for control_queue in control_queues:
                try:
                    control_queue.put("stop")
                except Exception:
                    pass  # Queue may be closed already

            # Wait for processes to finish and ensure cleanup
            for p in processes:
                try:
                    p.join(timeout=5.0)
                    if p.is_alive():
                        p.terminate()
                        p.join()
                except Exception:
                    pass  # Process may already be terminated

            broker.close()


def test_multiprocess_thundering_herd() -> None:
    """Test thundering herd mitigation across processes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Test both with and without pre-check
        for enable_pre_check in [False, True]:
            # Pre-populate database with a single message and close connection
            with BrokerDB(db_path) as broker:
                # Clear any previous messages
                broker._runner.run("DELETE FROM messages")
                # Write to only one queue
                broker.write("queue_0", "test_message")

            num_processes = 10
            result_queue = multiprocessing.Queue()
            control_queues = []
            processes = []

            try:
                # Start processes watching different queues
                for i in range(num_processes):
                    control_queue = multiprocessing.Queue()
                    control_queues.append(control_queue)

                    p = multiprocessing.Process(
                        target=watcher_process,
                        args=(
                            db_path,
                            f"queue_{i}",
                            result_queue,
                            control_queue,
                            i,
                            enable_pre_check,
                        ),
                    )
                    p.start()
                    processes.append(p)

                # Wait for ready
                ready_count = 0
                while ready_count < num_processes:
                    msg_type, proc_id, data = result_queue.get(timeout=5.0)
                    if msg_type == "ready":
                        ready_count += 1

                # Wait a bit for processing
                time.sleep(1.0)

                # Stop processes
                for control_queue in control_queues:
                    control_queue.put("stop")

                # Collect stats with robust error handling
                stats = {}
                errors = []
                timeout_start = time.monotonic()

                # Wait for all processes to send their stats
                while (
                    len(stats) + len(errors) < num_processes
                    and time.monotonic() - timeout_start < 10
                ):
                    try:
                        msg_type, proc_id, data = result_queue.get(timeout=0.5)
                        if msg_type == "stats":
                            stats[proc_id] = data
                        elif msg_type == "error":
                            errors.append((proc_id, data))
                        # Also check for "message" type which indicates processing
                        elif msg_type == "message":
                            # Process 0 processed the message, ignore this
                            pass
                    except queue.Empty:
                        continue

                # Check for errors
                if errors:
                    raise AssertionError(f"Process errors occurred: {errors}")

                # Only process 0 should have processed the message
                for i in range(num_processes):
                    if i == 0:
                        assert i in stats, (
                            f"Missing stats for process {i} (which should have processed the message)"
                        )
                        assert stats[i]["processed"] == 1, (
                            f"Process 0 should have processed 1 message, got {stats[i]['processed']}"
                        )
                    # Other processes should not have processed anything
                    elif i in stats:
                        assert stats[i]["processed"] == 0, (
                            f"Process {i} should not have processed any messages"
                        )
            finally:
                # Wait for processes to finish and ensure cleanup
                for p in processes:
                    try:
                        p.join(timeout=5.0)
                        if p.is_alive():
                            p.terminate()
                            p.join()
                    except Exception:
                        pass  # Process may already be terminated


def test_multiprocess_graceful_shutdown() -> None:
    """Test graceful shutdown of watchers across processes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Pre-populate database and close connection before starting processes
        with BrokerDB(db_path) as broker:
            for i in range(3):
                for j in range(10):
                    broker.write(f"queue_{i}", f"msg_{j}")

        # Start processes
        num_processes = 3
        result_queue = multiprocessing.Queue()
        control_queues = []
        processes = []

        try:
            for i in range(num_processes):
                control_queue = multiprocessing.Queue()
                control_queues.append(control_queue)

                p = multiprocessing.Process(
                    target=shutdown_test_process,
                    args=(db_path, f"queue_{i}", result_queue, control_queue, i),
                )
                p.start()
                processes.append(p)

            # Wait for ready
            ready_count = 0
            while ready_count < num_processes:
                msg_type, proc_id, data = result_queue.get(timeout=5.0)
                if msg_type == "ready":
                    ready_count += 1

            # Let them process some messages
            time.sleep(0.5)

            # Send stop signals
            for control_queue in control_queues:
                control_queue.put("stop")

            # Collect shutdown stats with robust error handling
            shutdown_stats = {}
            errors = []
            timeout_start = time.monotonic()
            messages_received = 0

            while (
                messages_received < num_processes
                and time.monotonic() - timeout_start < 10
            ):
                try:
                    msg_type, proc_id, data = result_queue.get(timeout=1.0)
                    messages_received += 1

                    if msg_type == "shutdown_stats":
                        shutdown_stats[proc_id] = data
                    elif msg_type == "error":
                        errors.append((proc_id, data))
                except queue.Empty:
                    continue

            # Check for errors before assertions
            if errors:
                raise AssertionError(f"Process errors occurred: {errors}")

            # Verify clean shutdown
            for i in range(num_processes):
                assert i in shutdown_stats, f"Missing shutdown stats for process {i}"
                stats = shutdown_stats[i]
                assert stats["before_stop"] > 0  # Processed some messages
                assert stats["after_stop"] == 0  # No processing after stop
                assert not stats["thread_alive"]  # Thread stopped cleanly
        finally:
            # Ensure all processes are cleaned up
            for p in processes:
                try:
                    p.join(timeout=5.0)
                    if p.is_alive():
                        p.terminate()
                        p.join()
                except Exception:
                    pass  # Process may already be terminated


def test_multiprocess_database_locking() -> None:
    """Test database locking behavior with multiple processes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        broker = BrokerDB(db_path)

        # Start multiple processes on same queue to create contention
        num_processes = 5
        result_queue = multiprocessing.Queue()
        control_queues = []
        processes = []

        try:
            for i in range(num_processes):
                control_queue = multiprocessing.Queue()
                control_queues.append(control_queue)

                p = multiprocessing.Process(
                    target=lock_test_process,
                    args=(db_path, "shared_queue", result_queue, control_queue, i),
                )
                p.start()
                processes.append(p)

            # Wait for ready
            ready_count = 0
            while ready_count < num_processes:
                msg_type, proc_id, data = result_queue.get(timeout=5.0)
                if msg_type == "ready":
                    ready_count += 1

            # Create high write load
            for i in range(100):
                broker.write("shared_queue", f"message_{i}")
                time.sleep(0.01)

            # Let them run
            time.sleep(2.0)

            # Collect lock stats
            lock_stats = []
            for _ in range(num_processes):
                try:
                    msg_type, proc_id, data = result_queue.get(timeout=2.0)
                    if msg_type == "lock_stats":
                        lock_stats.append(data)
                except queue.Empty:
                    pass

            # With pre-check optimization, lock contention should be minimal
            if lock_stats:
                total_attempts = sum(s["attempts"] for s in lock_stats)
                total_failures = sum(s["failures"] for s in lock_stats)
                overall_failure_rate = total_failures / max(1, total_attempts)

                # Failure rate should be low with proper implementation
                assert overall_failure_rate < 0.3  # Less than 30% lock failures
        finally:
            # Stop all processes
            for control_queue in control_queues:
                try:
                    control_queue.put("stop")
                except Exception:
                    pass  # Queue may be closed already

            # Clean up processes
            for p in processes:
                try:
                    p.join(timeout=5.0)
                    if p.is_alive():
                        p.terminate()
                        p.join()
                except Exception:
                    pass  # Process may already be terminated

            broker.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
