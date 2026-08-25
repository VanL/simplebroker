"""Test suite for QueueWatcher multi-process scenarios.

Tests watcher behavior across process boundaries to ensure proper
isolation and coordination.
"""

import contextlib
import multiprocessing.queues
import queue
import sqlite3
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher

from .helper_scripts.timing import scale_timeout_for_ci

_SHARED_QUEUE_BULK_TIMEOUT = 20.0


class ProcessRecordingQueue(Queue):
    """Count real delivery entries for parent-visible multiprocess evidence."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.delivery_calls = 0
        self._delivery_lock = threading.Lock()

    def read_many(self, *args: Any, **kwargs: Any) -> Any:
        with self._delivery_lock:
            self.delivery_calls += 1
        return super().read_many(*args, **kwargs)

    def delivery_call_count(self) -> int:
        with self._delivery_lock:
            return self.delivery_calls


def _queue_state_for_diagnostics(db_path: str, queue_name: str) -> str:
    """Read queue state without extending a timeout failure materially."""

    database_uri = f"{Path(db_path).resolve().as_uri()}?mode=ro"
    try:
        with contextlib.closing(
            sqlite3.connect(database_uri, uri=True, timeout=0.1)
        ) as connection:
            row = connection.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN claimed = 0 THEN 1 ELSE 0 END), 0),
                    COUNT(*)
                FROM messages
                WHERE queue = ?
                """,
                (queue_name,),
            ).fetchone()
    except sqlite3.Error as exc:
        return f"unavailable ({type(exc).__name__}: {exc})"

    if row is None:
        return "unavailable (query returned no row)"
    pending, total = (int(value) for value in row)
    return f"pending={pending}, claimed={total - pending}, total={total}"


def _process_diagnostics(
    processes: list[multiprocessing.Process],
) -> list[tuple[int, int | None, int | None, bool]]:
    return [
        (i, process.pid, process.exitcode, process.is_alive())
        for i, process in enumerate(processes)
    ]


def _deadline_after(timeout: float) -> float:
    return time.monotonic() + scale_timeout_for_ci(timeout)


def _get_before_deadline(
    result_queue: queue.Queue | multiprocessing.queues.Queue,
    *,
    deadline: float,
    poll_interval: float = 0.1,
):
    """Poll one protocol queue without extending its aggregate deadline."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise queue.Empty
    return result_queue.get(timeout=min(remaining, scale_timeout_for_ci(poll_interval)))


def _cleanup_process(process: multiprocessing.Process) -> None:
    """Bound graceful join, terminate fallback, and final reap on every runner."""

    process.join(timeout=scale_timeout_for_ci(5.0))
    if process.is_alive():
        process.terminate()
        process.join(timeout=scale_timeout_for_ci(2.0))
    if process.is_alive():
        process.kill()
        process.join(timeout=scale_timeout_for_ci(2.0))
    if process.is_alive():
        raise AssertionError(f"multiprocess watcher child {process.pid} leaked")


def watcher_process(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-034] exception
    db_path: str,
    queue_name: str,
    result_queue: multiprocessing.Queue,
    control_queue: multiprocessing.Queue,
    process_id: int,
    enable_pre_check: bool = True,
) -> None:
    """Worker process that runs a QueueWatcher."""
    del enable_pre_check
    try:
        # Track messages processed
        processed = []

        def handler(msg, ts) -> None:
            processed.append((msg, ts))
            result_queue.put(("message", process_id, msg))

        watched_queue = ProcessRecordingQueue(
            queue_name,
            db_path=db_path,
            persistent=True,
        )
        watcher = QueueWatcher(watched_queue, handler)

        # Run until stop signal
        thread = watcher.run_in_thread()

        ready_deadline = _deadline_after(10.0)
        while watched_queue.delivery_call_count() < 1:
            if not thread.is_alive():
                raise RuntimeError("Watcher thread exited before initial drain")
            if time.monotonic() >= ready_deadline:
                raise TimeoutError("Watcher thread did not complete initial drain")
            time.sleep(scale_timeout_for_ci(0.01))

        # Signal ready after the watcher has completed startup drain.
        result_queue.put(("ready", process_id, None))

        while True:
            try:
                command = control_queue.get(timeout=scale_timeout_for_ci(0.1))
                if command == "stop":
                    break
            except queue.Empty:
                if not thread.is_alive():
                    raise RuntimeError(
                        "Watcher thread exited before stop signal"
                    ) from None
                continue

        watcher.stop()
        thread.join(timeout=scale_timeout_for_ci(2.0))
        if thread.is_alive():
            raise TimeoutError("Watcher thread did not stop before deadline")

        # Send final stats
        result_queue.put(
            (
                "stats",
                process_id,
                {
                    "processed": len(processed),
                    "messages": processed,
                    "delivery_calls": watched_queue.delivery_call_count(),
                },
            ),
        )
        watched_queue.close()

    except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
        result_queue.put(("error", process_id, str(e)))


def contention_watcher_process(
    db_path: str,
    result_queue: multiprocessing.Queue,
    control_queue: multiprocessing.Queue,
    process_id: int,
) -> None:
    """Run one real watcher and report every delivered body and ID to its parent."""
    watched_queue: ProcessRecordingQueue | None = None
    watcher: QueueWatcher | None = None
    try:
        watched_queue = ProcessRecordingQueue(
            "shared_queue",
            db_path=db_path,
            persistent=True,
        )

        def handler(message: str, timestamp: int) -> None:
            result_queue.put(("message", process_id, (message, timestamp)))

        watcher = QueueWatcher(watched_queue, handler)
        thread = watcher.run_in_thread()
        deadline = _deadline_after(10.0)
        while watched_queue.delivery_call_count() < 1:
            if not thread.is_alive():
                raise RuntimeError("contention watcher exited during startup")
            if time.monotonic() >= deadline:
                raise TimeoutError("contention watcher did not finish initial drain")
            time.sleep(scale_timeout_for_ci(0.01))
        result_queue.put(("ready", process_id, None))

        while control_queue.get(timeout=scale_timeout_for_ci(30.0)) != "stop":
            pass

        watcher.stop()
        thread.join(timeout=scale_timeout_for_ci(5.0))
        if thread.is_alive():
            raise TimeoutError("contention watcher did not stop")
        result_queue.put(("stats", process_id, None))
    except Exception as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
        result_queue.put(("error", process_id, str(exc)))
    finally:
        if watcher is not None:
            with contextlib.suppress(Exception):
                watcher.stop()
        if watched_queue is not None:
            with contextlib.suppress(Exception):
                watched_queue.close()


def shutdown_test_process(
    db_path, queue_name, result_queue, control_queue, process_id
) -> None:
    """Process function for testing graceful shutdown."""
    try:
        processed_before_stop = []
        processed_after_stop = []
        stop_requested = False
        reported_processed = False

        def handler(msg, ts) -> None:
            nonlocal reported_processed
            if stop_requested:
                processed_after_stop.append(msg)
            else:
                processed_before_stop.append(msg)
                if not reported_processed:
                    reported_processed = True
                    result_queue.put(("processed", process_id, msg))

        watcher = QueueWatcher(queue_name, handler, db=db_path)
        thread = watcher.run_in_thread()

        result_queue.put(("ready", process_id, None))

        # Wait for stop signal
        while True:
            try:
                command = control_queue.get(timeout=scale_timeout_for_ci(0.1))
                if command == "stop":
                    watcher.stop()
                    stop_requested = True
                    break
            except queue.Empty:
                continue

        # Ensure thread stops
        thread.join(timeout=scale_timeout_for_ci(5.0))

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

    except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
        result_queue.put(("error", process_id, str(e)))


def lock_test_process(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-034] exception
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

        # Do not start the fixed observation window until every sibling has
        # spawned. Otherwise a fast child can finish before a slow Windows
        # child is ready, weakening the intended contention phase.
        while True:
            try:
                command = control_queue.get(timeout=scale_timeout_for_ci(0.1))
            except queue.Empty:
                continue
            if command == "start":
                break
            if command == "stop":
                watcher.stop()
                thread.join(timeout=scale_timeout_for_ci(2.0))
                if thread.is_alive():
                    raise TimeoutError("Lock watcher did not stop before deadline")
                return

        # Run for a fixed time
        observation_deadline = _deadline_after(2.0)
        while time.monotonic() < observation_deadline:
            try:
                command = control_queue.get(
                    timeout=min(
                        max(0.0, observation_deadline - time.monotonic()),
                        scale_timeout_for_ci(0.1),
                    )
                )
                if command == "stop":
                    break
            except queue.Empty:
                continue

        watcher.stop()
        thread.join(timeout=scale_timeout_for_ci(2.0))
        if thread.is_alive():
            raise TimeoutError("Lock watcher did not stop before deadline")

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

    except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
        result_queue.put(("error", process_id, str(e)))


def test_queue_state_diagnostics_are_best_effort(tmp_path: Path) -> None:
    db_path = tmp_path / "diagnostics.db"
    with BrokerDB(str(db_path)) as broker:
        broker.write("shared_queue", "one")
        broker.write("shared_queue", "two")
        assert _queue_state_for_diagnostics(str(db_path), "shared_queue") == (
            "pending=2, claimed=0, total=2"
        )

    unavailable = _queue_state_for_diagnostics(
        str(tmp_path / "missing.db"),
        "shared_queue",
    )
    assert unavailable.startswith("unavailable (OperationalError:")


def test_multiprocess_separate_queues() -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-034] exception
    """Test multiple processes each watching their own queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        broker = BrokerDB(db_path)

        num_processes = 5
        messages_per_queue = 20

        # Create communication queues
        result_queue: multiprocessing.queues.Queue[Any] = multiprocessing.Queue()
        control_queues = []
        processes: list[multiprocessing.Process] = []

        try:

            def process_diagnostics() -> list[tuple[int, int | None, int | None, bool]]:
                return [
                    (i, p.pid, p.exitcode, p.is_alive())
                    for i, p in enumerate(processes)
                ]

            errors: list[tuple[int, str]] = []
            process_message_counts = dict.fromkeys(range(num_processes), 0)

            def collect_until_counts(
                targets: dict[int, int], *, timeout: float
            ) -> None:
                deadline = _deadline_after(timeout)
                while time.monotonic() < deadline:
                    if all(
                        process_message_counts[i] >= target
                        for i, target in targets.items()
                    ):
                        return

                    try:
                        msg_type, proc_id, data = _get_before_deadline(
                            result_queue,
                            deadline=deadline,
                        )
                    except queue.Empty:
                        continue

                    if msg_type == "message":
                        process_message_counts[proc_id] += 1
                    elif msg_type == "error":
                        errors.append((proc_id, data))
                        break

                raise AssertionError(
                    "Timed out waiting for watcher processes to consume messages: "
                    f"targets={targets}, counts={process_message_counts}, "
                    f"errors={errors}, processes={process_diagnostics()}"
                )

            # Start processes, each watching its own queue
            for i in range(num_processes):
                control_queue: multiprocessing.queues.Queue[Any] = (
                    multiprocessing.Queue()
                )
                control_queues.append(control_queue)

                p = multiprocessing.Process(
                    target=watcher_process,
                    args=(db_path, f"queue_{i}", result_queue, control_queue, i),
                )
                p.start()
                processes.append(p)

            # Wait for ready
            ready_processes: set[int] = set()
            ready_deadline = _deadline_after(10.0)
            while (
                len(ready_processes) < num_processes
                and time.monotonic() < ready_deadline
            ):
                try:
                    msg_type, proc_id, data = _get_before_deadline(
                        result_queue,
                        deadline=ready_deadline,
                    )
                except queue.Empty:
                    continue

                if msg_type == "ready":
                    ready_processes.add(proc_id)
                elif msg_type == "error":
                    errors.append((proc_id, data))
                    break

            if errors or len(ready_processes) != num_processes:
                raise AssertionError(
                    "Timed out waiting for watcher processes to start: "
                    f"ready={sorted(ready_processes)}, errors={errors}, "
                    f"processes={process_diagnostics()}"
                )

            # Prove each watcher can consume from its own queue before the bulk phase.
            for i in range(num_processes):
                broker.write(f"queue_{i}", f"queue_{i}_probe")
            collect_until_counts(dict.fromkeys(range(num_processes), 1), timeout=10.0)

            # Write messages to each queue
            for i in range(num_processes):
                for j in range(1, messages_per_queue):
                    broker.write(f"queue_{i}", f"queue_{i}_msg_{j}")

            # Collect results
            collect_until_counts(
                dict.fromkeys(range(num_processes), messages_per_queue),
                timeout=10.0,
            )

            # Each process should have processed exactly its queue's messages
            for i in range(num_processes):
                assert process_message_counts[i] == messages_per_queue
        finally:
            # Stop processes
            for control_queue in control_queues:
                # Queue may be closed already
                with contextlib.suppress(Exception):
                    control_queue.put("stop")

            # Wait for processes to finish and ensure cleanup
            for p in processes:
                _cleanup_process(p)

            broker.close()


def test_multiprocess_unrelated_write_does_not_drain_idle_watchers() -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-034] exception
    """Unrelated child watchers perform no delivery work after a targeted write."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        with BrokerDB(db_path):
            pass

        process_count = 6
        result_queue: multiprocessing.queues.Queue[Any] = multiprocessing.Queue()
        control_queues: list[multiprocessing.queues.Queue[Any]] = []
        processes: list[multiprocessing.Process] = []
        stats: dict[int, dict[str, Any]] = {}
        errors: list[tuple[int, str]] = []
        try:
            for process_id in range(process_count):
                control_queue: multiprocessing.queues.Queue[Any] = (
                    multiprocessing.Queue()
                )
                control_queues.append(control_queue)
                process = multiprocessing.Process(
                    target=watcher_process,
                    args=(
                        db_path,
                        f"queue_{process_id}",
                        result_queue,
                        control_queue,
                        process_id,
                    ),
                )
                process.start()
                processes.append(process)

            ready: set[int] = set()
            deadline = _deadline_after(15.0)
            while len(ready) < process_count and time.monotonic() < deadline:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=deadline
                    )
                except queue.Empty:
                    continue
                if kind == "ready":
                    ready.add(process_id)
                elif kind == "error":
                    errors.append((process_id, data))
                    break
            assert errors == []
            assert ready == set(range(process_count))

            with BrokerDB(db_path) as broker:
                broker.write("queue_0", "target-only")

            delivered: list[tuple[int, str]] = []
            deadline = _deadline_after(10.0)
            while not delivered and time.monotonic() < deadline:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=deadline
                    )
                except queue.Empty:
                    continue
                if kind == "message":
                    delivered.append((process_id, data))
                elif kind == "error":
                    errors.append((process_id, data))
                    break
            assert errors == []
            assert delivered == [(0, "target-only")]

            for control_queue in control_queues:
                control_queue.put("stop")

            deadline = _deadline_after(15.0)
            while len(stats) < process_count and time.monotonic() < deadline:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=deadline
                    )
                except queue.Empty:
                    continue
                if kind == "stats":
                    stats[process_id] = data
                elif kind == "message":
                    delivered.append((process_id, data))
                elif kind == "error":
                    errors.append((process_id, data))
            assert errors == []
            assert set(stats) == set(range(process_count))
            assert delivered == [(0, "target-only")]
            assert stats[0]["processed"] == 1
            assert stats[0]["delivery_calls"] > 1
            for process_id in range(1, process_count):
                assert stats[process_id]["processed"] == 0
                assert stats[process_id]["delivery_calls"] == 1
        finally:
            for control_queue in control_queues:
                with contextlib.suppress(Exception):
                    control_queue.put("stop")
            for process in processes:
                _cleanup_process(process)

        assert all(process.exitcode == 0 for process in processes)


def test_multiprocess_graceful_shutdown() -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-034] exception
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
        result_queue: multiprocessing.queues.Queue[Any] = multiprocessing.Queue()
        control_queues = []
        processes: list[multiprocessing.Process] = []

        try:
            for i in range(num_processes):
                control_queue: multiprocessing.queues.Queue[Any] = (
                    multiprocessing.Queue()
                )
                control_queues.append(control_queue)

                p = multiprocessing.Process(
                    target=shutdown_test_process,
                    args=(db_path, f"queue_{i}", result_queue, control_queue, i),
                )
                p.start()
                processes.append(p)

            # Wait until each child has both started and processed real work.
            ready_processes: set[int] = set()
            processed_processes: set[int] = set()
            errors = []
            deadline = _deadline_after(10.0)

            while (
                len(ready_processes) < num_processes
                or len(processed_processes) < num_processes
            ) and time.monotonic() < deadline:
                try:
                    msg_type, proc_id, data = _get_before_deadline(
                        result_queue,
                        deadline=deadline,
                    )
                except queue.Empty:
                    continue

                if msg_type == "ready":
                    ready_processes.add(proc_id)
                elif msg_type == "processed":
                    processed_processes.add(proc_id)
                elif msg_type == "error":
                    errors.append((proc_id, data))

            if errors:
                raise AssertionError(f"Process errors occurred: {errors}")

            assert ready_processes == set(range(num_processes)), (
                "Processes did not become ready before shutdown: "
                f"ready={sorted(ready_processes)}"
            )
            assert processed_processes == set(range(num_processes)), (
                "Processes did not process messages before shutdown: "
                f"processed={sorted(processed_processes)}"
            )

            # Send stop signals
            for control_queue in control_queues:
                control_queue.put("stop")

            # Collect shutdown stats with robust error handling
            shutdown_stats: dict[int, Any] = {}
            shutdown_deadline = _deadline_after(10.0)

            while (
                len(shutdown_stats) + len(errors) < num_processes
                and time.monotonic() < shutdown_deadline
            ):
                try:
                    msg_type, proc_id, data = _get_before_deadline(
                        result_queue,
                        deadline=shutdown_deadline,
                        poll_interval=1.0,
                    )

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
                assert stats["after_stop"] == 0  # No processing after stop() returned
                assert not stats["thread_alive"]  # Thread stopped cleanly
        finally:
            # Ensure all processes are cleaned up
            for p in processes:
                _cleanup_process(p)


def test_multiprocess_contention_preserves_exact_delivery() -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-034] exception
    """Real child watchers deliver every inserted body and ID exactly once."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        with BrokerDB(db_path):
            pass

        process_count = 5
        result_queue: multiprocessing.queues.Queue[Any] = multiprocessing.Queue()
        control_queues: list[multiprocessing.queues.Queue[Any]] = []
        processes: list[multiprocessing.Process] = []
        errors: list[tuple[int, str]] = []
        try:
            for process_id in range(process_count):
                control_queue: multiprocessing.queues.Queue[Any] = (
                    multiprocessing.Queue()
                )
                control_queues.append(control_queue)
                process = multiprocessing.Process(
                    target=contention_watcher_process,
                    args=(db_path, result_queue, control_queue, process_id),
                )
                process.start()
                processes.append(process)

            ready: set[int] = set()
            deadline = _deadline_after(15.0)
            while len(ready) < process_count and time.monotonic() < deadline:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=deadline
                    )
                except queue.Empty:
                    continue
                if kind == "ready":
                    ready.add(process_id)
                elif kind == "error":
                    errors.append((process_id, data))
                    break
            assert errors == []
            assert ready == set(range(process_count))

            expected: dict[int, str] = {}
            # Phased-delivery probe (ported from the deleted
            # test_multiprocess_single_queue per the plan's equivalence
            # invariant): prove at least one child consumes before the
            # bulk writes land.
            with BrokerDB(db_path) as broker:
                expected[broker.write("shared_queue", "message_0")] = "message_0"
            first_delivery_deadline = _deadline_after(15.0)
            first_delivered: list[tuple[str, int]] = []
            while not first_delivered and time.monotonic() < first_delivery_deadline:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=first_delivery_deadline
                    )
                except queue.Empty:
                    continue
                if kind == "message":
                    first_delivered.append(data)
                elif kind == "error":
                    errors.append((process_id, data))
                    break
            assert errors == []
            assert first_delivered and first_delivered[0][0] == "message_0"

            with BrokerDB(db_path) as broker:
                for index in range(1, 100):
                    message = f"message_{index}"
                    expected[broker.write("shared_queue", message)] = message

            delivered: list[tuple[str, int]] = list(first_delivered)
            deadline = _deadline_after(_SHARED_QUEUE_BULK_TIMEOUT)
            while len(delivered) < len(expected) and time.monotonic() < deadline:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=deadline
                    )
                except queue.Empty:
                    continue
                if kind == "message":
                    delivered.append(data)
                elif kind == "error":
                    errors.append((process_id, data))
                    break

            assert errors == []
            assert len(delivered) == len(expected)
            assert {timestamp: body for body, timestamp in delivered} == expected
            assert len({timestamp for _body, timestamp in delivered}) == len(expected)

            with BrokerDB(db_path) as observer:
                assert (
                    observer.peek_many("shared_queue", limit=1, with_timestamps=False)
                    == []
                )

            for control_queue in control_queues:
                control_queue.put("stop")

            stopped: set[int] = set()
            deadline = _deadline_after(15.0)
            while len(stopped) < process_count and time.monotonic() < deadline:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=deadline
                    )
                except queue.Empty:
                    continue
                if kind == "stats":
                    stopped.add(process_id)
                elif kind == "error":
                    errors.append((process_id, data))
            assert errors == []
            assert stopped == set(range(process_count))

            # No processing after stop (ported evidence): a message
            # written after every child reported its final stats must
            # stay pending.
            with BrokerDB(db_path) as broker:
                broker.write("shared_queue", "post_stop_message")
            time_budget = _deadline_after(1.0)
            while time.monotonic() < time_budget:
                try:
                    kind, process_id, data = _get_before_deadline(
                        result_queue, deadline=time_budget
                    )
                except queue.Empty:
                    break
                assert kind != "message", (
                    f"stopped child {process_id} processed {data!r}"
                )
            with BrokerDB(db_path) as observer:
                assert observer.peek_many(
                    "shared_queue", limit=1, with_timestamps=False
                ) == ["post_stop_message"]
        finally:
            for control_queue in control_queues:
                with contextlib.suppress(Exception):
                    control_queue.put("stop")
            for process in processes:
                _cleanup_process(process)

        assert all(process.exitcode == 0 for process in processes)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
