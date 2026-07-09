#!/usr/bin/env python3
"""Reference reactor layered on the multi-queue watcher example.

This example is intentionally stricter than the general ``MultiQueueWatcher``
demo. It separates the reusable reactor mechanism from the demo policy:

* ``BaseReactor`` owns the process/wait/stop loop and local wakeups;
* ``Reactor`` owns the sidecar checkpoint, output replay, and control policy;
* the reactor thread is the only thread that touches reactor-owned persistent
  ``Queue`` handles or mutates reactor sidecar tables during normal operation;
* worker threads receive plain Python dataclasses through ``queue.Queue`` and
  return plain result envelopes the same way;
* source queues are observed with peek-plus-sidecar-checkpoint semantics;
* control messages are handled on a separate control lane by the reactor; and
* result publication is at-least-once and replayable: the reactor records a
  pending output row in a sidecar table, publishes with an exact message ID,
  then marks it written.
  External queue writes are detected through SimpleBroker's polling strategy:
  SQLite ``PRAGMA data_version`` observes database changes, then the reactor's
  checkpoint-aware pending check decides whether there is actually work here.

The point is not to make a complete framework. It is a small executable shape
to compare against real codebases such as Taut, Summon, and Weft. SimpleBroker
already handles storage-level multi-process access to one SQLite database with
short write transactions and retry. The reactor contract is about logical
workstream ownership: because source and control queues use
peek-plus-checkpoint semantics, processing is at-least-once across restart even
with one reactor, and two live reactors on the same lane add another duplicate
execution path. Output publication is also at-least-once: a crash after the
exact-ID outbox write but before ``output_written`` can replay the message if a
downstream consumer has already vacuumed the claimed output row. A production
reactor should keep SQLite transactions short, make processors and control
commands idempotent, deduplicate downstream by output message ID rather than
payload, add a retention or compaction policy appropriate to its durability
contract, and add worker timeouts if processors are not tightly bounded. Output
replay failures backpressure new input dispatch, but the control lane stays
responsive so STATUS and STOP can still get through. Input, output, and control
role names must all be distinct. Plain-text control commands are supported; JSON
control payloads must be objects. Pending outputs retain their recorded output
queue, and a configured-route mismatch raises instead of silently rerouting. In
background mode that error ends the drive thread, whose finalizer closes reactor
resources. Replay budgets limit rows returned and materialized, though SQLite may
scan more rows without a supporting index. Constructing ``Reactor`` performs
durable setup and starts idle workers; the first driven turn replays pending
output. Construction is not a side-effect-free configuration step.

Run from the repository root:

    python examples/reference_reactor.py
"""

from __future__ import annotations

import itertools
import json
import queue as thread_queue
import sys
import tempfile
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

# Support running the file directly from the repository root.
sys.path.insert(0, str(Path(__file__).parent))

from multi_queue_watcher import MultiQueueWatcher  # noqa: E402

from simplebroker import Queue  # noqa: E402
from simplebroker.ext import (  # noqa: E402
    IntegrityError,
    OperationalError,
    StopWatching,
)

JsonMapping = Mapping[str, Any]
Processor = Callable[["WorkItem"], JsonMapping]
TimestampedRows = list[tuple[str, int]]


def _timestamped_rows(rows: list[str] | TimestampedRows) -> TimestampedRows:
    return cast(TimestampedRows, rows)


@dataclass(frozen=True, slots=True)
class WorkItem:
    """Broker-free work payload passed from the reactor to a worker thread."""

    source_queue: str
    timestamp: int
    body: str


@dataclass(frozen=True, slots=True)
class WorkerResult:
    """Broker-free result payload passed from a worker thread to the reactor."""

    source_queue: str
    timestamp: int
    value: JsonMapping | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class PendingOutput:
    """Durable output row waiting to be published or confirmed."""

    source_queue: str
    input_timestamp: int
    output_queue: str
    output_message_id: int
    payload: str


def default_processor(item: WorkItem) -> JsonMapping:
    """Example worker body. It has no Queue handles and does no durable I/O."""

    try:
        payload: Any = json.loads(item.body)
    except json.JSONDecodeError:
        payload = {"text": item.body}

    return {
        "source_queue": item.source_queue,
        "input_timestamp": item.timestamp,
        "payload": payload,
        "processed_by": threading.current_thread().name,
    }


class BaseReactor(MultiQueueWatcher):
    """BaseTask-shaped fixed-topology reactor over ``MultiQueueWatcher``.

    Subclasses inherit queue ownership, activity detection, a single-owner
    process/wait loop, and shutdown ordering. They provide the concrete policy:
    how to drain local results, how to process queue activity, and which extra
    resources must close with the reactor.

    The default queue topology is fixed at construction. A subclass that needs
    dynamic lanes should expose role-aware methods such as ``add_input_queue()``
    and update its checkpoint/sidecar state before delegating to
    ``MultiQueueWatcher.add_queue()``.
    """

    reactor_result_budget = 100

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._reactor_stop_event = threading.Event()
        self._reactor_activity_event = threading.Event()
        self._drive_owner_lock = threading.Lock()
        self._drive_owner_ident: int | None = None
        self._drive_thread: threading.Thread | None = None
        self._stop_once_lock = threading.Lock()
        self._stop_requested = False
        self._resources_closed = False
        self._strategy_started = False
        super().__init__(*args, **kwargs)

    def _managed_queue(self, queue_name: str) -> Queue:
        queue_obj = self.get_queue(queue_name)
        if queue_obj is None:
            raise RuntimeError(f"queue is not managed by this reactor: {queue_name}")
        return queue_obj

    def _claim_reactor_thread(self) -> None:
        """Ensure exactly one thread drives reactor turns for this instance."""

        ident = threading.get_ident()
        with self._drive_owner_lock:
            if self._drive_owner_ident is None:
                self._drive_owner_ident = ident
                self._drive_thread = threading.current_thread()
                return
            if self._drive_owner_ident != ident:
                raise RuntimeError(
                    "Reactor turns are single-owner; do not call process_once() "
                    "from a second thread after the reactor has started"
                )

    def _ensure_polling_strategy_started(self) -> None:
        if self._strategy_started:
            return
        self._start_strategy()
        self._strategy_started = True

    def add_queue(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise NotImplementedError(
            "BaseReactor queues are fixed at construction so checkpoints, "
            "sidecars, and ownership stay coherent"
        )

    def remove_queue(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise NotImplementedError(
            "BaseReactor queues are fixed at construction so checkpoints, "
            "sidecars, and ownership stay coherent"
        )

    def notify_reactor_activity(self) -> None:
        """Wake ``wait_for_activity`` after local broker-free work completes."""

        self._reactor_activity_event.set()
        strategy = getattr(self, "_strategy", None)
        if strategy is not None:
            strategy.notify_activity()

    def _clear_reactor_activity(self) -> None:
        self._reactor_activity_event.clear()

    def _check_stop(self) -> None:
        if self._reactor_stop_event.is_set() or self._stop_event.is_set():
            raise StopWatching

    def request_stop(self) -> None:
        """Request a graceful reactor-loop stop without closing resources yet."""

        self._reactor_stop_event.set()
        self.notify_reactor_activity()

    def _has_pending_reactor_results(self) -> bool:
        """Return whether broker-free local work is waiting for the reactor."""

        return False

    def _drain_reactor_results(self, *, max_results: int = 100) -> int:
        """Apply broker-free local results on the reactor thread."""

        del max_results
        return 0

    def _drain_reactor_backlog(self) -> bool:
        """Drain durable retry work before ordinary queue dispatch."""

        return True

    def _has_pending_reactor_backlog(self) -> bool:
        """Return whether durable retry work should shorten the next wait."""

        return False

    def _request_reactor_workers_stop(self) -> None:
        """Ask local workers to stop. Subclasses with workers override."""

    def _join_reactor_workers(self, *, deadline: float) -> None:
        """Join local workers. Subclasses with workers override."""

        del deadline

    def _close_reactor_resources(self) -> None:
        """Close reactor-owned queue handles after the drive thread exits."""

        for queue_info in self._queues.values():
            queue_info["queue"].close()

    def process_once(self) -> None:
        """Process one reactor turn, matching Weft ``BaseTask`` shape.

        1. drain local worker results and apply durable effects;
        2. process one round of control/input queue activity;
        3. drain any worker results that completed during the turn.
        """

        self._claim_reactor_thread()
        if not self._drain_reactor_backlog():
            return
        worker_results_handled = self._drain_reactor_results(
            max_results=self.reactor_result_budget
        )
        if not self._drain_reactor_backlog():
            return
        if not self._reactor_stop_event.is_set():
            self._drain_queue()
        remaining_budget = max(0, self.reactor_result_budget - worker_results_handled)
        if remaining_budget:
            self._drain_reactor_results(max_results=remaining_budget)
        self._drain_reactor_backlog()

    def wait_for_activity(self, timeout: float = 0.05) -> None:
        """Wait for broker queue activity or local reactor activity."""

        if self._reactor_stop_event.is_set():
            return
        if self._has_pending_reactor_results() or self._has_pending_messages():
            return
        if self._has_pending_reactor_backlog():
            timeout = min(timeout, 0.05)

        deadline = time.monotonic() + max(0.0, timeout)
        self._ensure_polling_strategy_started()
        while not self._reactor_stop_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return

            wait_timeout = min(remaining, 0.05)
            if self._reactor_activity_event.wait(timeout=min(wait_timeout, 0.01)):
                return

            if self._reactor_activity_event.is_set():
                return

            self._strategy.wait_for_activity()
            if self._has_pending_messages():
                return

    def run_until_stopped(
        self,
        *,
        poll_interval: float = 0.05,
        max_iterations: int | None = None,
    ) -> None:
        """Repeatedly call ``process_once`` until the reactor stops."""

        self._claim_reactor_thread()
        iterations = 0
        try:
            if self._reactor_stop_event.is_set():
                return
            self._ensure_polling_strategy_started()
            while not self._reactor_stop_event.is_set():
                self._process_with_retry(self.process_once, "reactor turn")
                iterations += 1
                if max_iterations is not None and iterations >= max_iterations:
                    break
                self.wait_for_activity(poll_interval)
        except StopWatching:
            if not (self._reactor_stop_event.is_set() or self._stop_event.is_set()):
                raise
        finally:
            self.stop(join=False)

    def run_forever(self) -> None:
        """Run the reactor loop used by ``BaseWatcher.start()``."""
        signal_context = None
        try:
            signal_context = self._setup_signal_handler()
            self.run_until_stopped()
        finally:
            if signal_context is not None:
                signal_context.__exit__(None, None, None)

    def stop(self, *, join: bool = True, timeout: float = 2.0) -> None:
        current_thread = threading.current_thread()
        with self._stop_once_lock:
            first_request = not self._stop_requested
            if first_request:
                self._stop_requested = True
                self._reactor_stop_event.set()
                self.notify_reactor_activity()
                self._strategy.notify_activity()
                self._request_reactor_workers_stop()
            else:
                self._reactor_stop_event.set()
                self.notify_reactor_activity()
                self._strategy.notify_activity()

        deadline = time.monotonic() + timeout
        drive_thread = self._drive_thread
        if drive_thread is None:
            thread_ref = getattr(self, "_thread", None)
            drive_thread = thread_ref() if thread_ref is not None else None
        if (
            join
            and drive_thread is not None
            and drive_thread is not current_thread
            and drive_thread.is_alive()
        ):
            drive_thread.join(timeout=max(0.0, deadline - time.monotonic()))

        if join:
            self._join_reactor_workers(deadline=deadline)

        drive_alive_elsewhere = (
            drive_thread is not None
            and drive_thread is not current_thread
            and drive_thread.is_alive()
        )
        if drive_alive_elsewhere:
            return

        with self._stop_once_lock:
            if self._resources_closed:
                return
            self._resources_closed = True

        self._stop_event.set()
        super().stop(join=False, timeout=timeout)
        self._close_reactor_resources()


class Reactor(BaseReactor):
    """Sidecar-aware single-writer reactor built on ``BaseReactor``.

    This concrete example replaces ``MultiQueueWatcher``'s consuming handler
    path with peek/checkpoint dispatch. Subclasses should not call
    ``super()._drain_queue()`` from this class unless they intentionally want
    the parent consume-and-handler semantics.
    """

    def __init__(
        self,
        *,
        input_queues: Iterable[str],
        output_queue: str,
        control_in_queue: str,
        control_out_queue: str,
        db: str | Path,
        processor: Processor = default_processor,
        worker_count: int = 2,
    ) -> None:
        self.input_queues = tuple(input_queues)
        if not self.input_queues:
            raise ValueError("input_queues cannot be empty")
        role_names = [
            *self.input_queues,
            output_queue,
            control_in_queue,
            control_out_queue,
        ]
        duplicates = sorted(
            name for name, count in Counter(role_names).items() if count > 1
        )
        if duplicates:
            names = ", ".join(repr(name) for name in duplicates)
            raise ValueError(
                f"reactor queue roles must use distinct names; duplicated: {names}"
            )

        self.output_queue_name = output_queue
        self.control_in_queue = control_in_queue
        self.control_out_queue_name = control_out_queue
        self._processor = processor
        self._checkpoints: dict[str, int] = {}
        self._inflight: set[tuple[str, int]] = set()
        self._worker_stopping = threading.Event()
        self._work_queue: thread_queue.Queue[WorkItem | None] = thread_queue.Queue()
        self._worker_results: thread_queue.Queue[WorkerResult] = thread_queue.Queue()
        self._worker_threads: list[threading.Thread] = []
        self._output_backlog_blocked = False
        self._outputs_published = 0
        self._control_messages_handled = 0

        queue_names = [*self.input_queues, control_in_queue]
        super().__init__(
            queues=queue_names,
            default_handler=self._unexpected_direct_handler,
            db=db,
            persistent=True,
            check_interval=1,
        )

        metadata_queue = self._managed_queue(self.input_queues[0])
        db_target = metadata_queue.db_target
        self._metadata_queue = metadata_queue
        self._output_queue = Queue(output_queue, db_path=db_target, persistent=True)
        self._control_out_queue = Queue(
            control_out_queue,
            db_path=db_target,
            persistent=True,
        )
        self._output_queue.set_stop_event(self._stop_event)
        self._control_out_queue.set_stop_event(self._stop_event)

        self._ensure_sidecar_schema()
        self._checkpoints = self._load_checkpoints()
        for queue_name in self.input_queues:
            self._checkpoints.setdefault(queue_name, 0)
        self._checkpoints.setdefault(control_in_queue, 0)
        self._start_workers(max(1, worker_count))

    @staticmethod
    def _unexpected_direct_handler(message: str, timestamp: int) -> None:
        del message, timestamp
        raise RuntimeError("Reactor bypassed its explicit dispatch path")

    # ------------------------------------------------------------------
    # Sidecar schema and checkpoint state
    # ------------------------------------------------------------------
    def _ensure_sidecar_schema(self) -> None:
        with self._metadata_queue.sidecar(transaction=True) as session:
            session.run(
                """
                CREATE TABLE IF NOT EXISTS reactor_checkpoints (
                    queue_name TEXT PRIMARY KEY,
                    last_processed_ts INTEGER NOT NULL,
                    updated_at_ns INTEGER NOT NULL
                )
                """
            )
            session.run(
                """
                CREATE TABLE IF NOT EXISTS reactor_results (
                    source_queue TEXT NOT NULL,
                    input_ts INTEGER NOT NULL,
                    output_queue TEXT NOT NULL,
                    output_message_id INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    updated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY (source_queue, input_ts)
                )
                """
            )
            session.run(
                """
                CREATE TABLE IF NOT EXISTS reactor_seen (
                    source_queue TEXT NOT NULL,
                    input_ts INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    updated_at_ns INTEGER NOT NULL,
                    PRIMARY KEY (source_queue, input_ts)
                )
                """
            )
            session.run(
                """
                CREATE TABLE IF NOT EXISTS reactor_audit (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lane TEXT NOT NULL,
                    message_ts INTEGER NOT NULL,
                    event TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at_ns INTEGER NOT NULL
                )
                """
            )

    def _load_checkpoints(self) -> dict[str, int]:
        with self._metadata_queue.sidecar() as session:
            rows = list(
                session.run(
                    """
                    SELECT queue_name, last_processed_ts
                    FROM reactor_checkpoints
                    """,
                    fetch=True,
                )
            )
        return {str(queue_name): int(last_ts) for queue_name, last_ts in rows}

    def _checkpoint_after(self, queue_name: str) -> int | None:
        checkpoint = self._checkpoints.get(queue_name, 0)
        return checkpoint if checkpoint > 0 else None

    @staticmethod
    def _advance_checkpoint(
        session: Any,
        *,
        queue_name: str,
        timestamp: int,
        now_ns: int,
    ) -> None:
        session.run(
            """
            INSERT INTO reactor_checkpoints
                (queue_name, last_processed_ts, updated_at_ns)
            VALUES (?, ?, ?)
            ON CONFLICT(queue_name) DO UPDATE SET
                last_processed_ts = CASE
                    WHEN excluded.last_processed_ts
                        > reactor_checkpoints.last_processed_ts
                    THEN excluded.last_processed_ts
                    ELSE reactor_checkpoints.last_processed_ts
                END,
                updated_at_ns = excluded.updated_at_ns
            """,
            (queue_name, timestamp, now_ns),
        )

    def _record_dispatch(self, item: WorkItem) -> None:
        now = time.time_ns()
        with self._metadata_queue.sidecar(transaction=True) as session:
            session.run(
                """
                INSERT INTO reactor_seen
                    (source_queue, input_ts, status, updated_at_ns)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_queue, input_ts) DO UPDATE SET
                    status = excluded.status,
                    updated_at_ns = excluded.updated_at_ns
                """,
                (item.source_queue, item.timestamp, "inflight", now),
            )
            session.run(
                """
                INSERT INTO reactor_audit
                    (lane, message_ts, event, detail, created_at_ns)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    item.source_queue,
                    item.timestamp,
                    "dispatch",
                    "queued broker-free work item",
                    now,
                ),
            )

    def _record_pending_result(self, result: WorkerResult) -> PendingOutput | None:
        now = time.time_ns()

        with self._metadata_queue.sidecar(transaction=True) as session:
            existing = list(
                session.run(
                    """
                    SELECT output_queue, output_message_id, payload, status
                    FROM reactor_results
                    WHERE source_queue = ? AND input_ts = ?
                    """,
                    (result.source_queue, result.timestamp),
                    fetch=True,
                )
            )
            if existing:
                output_queue, output_id, payload, status = existing[0]
                if status == "output_written":
                    return None
                pending = PendingOutput(
                    source_queue=result.source_queue,
                    input_timestamp=result.timestamp,
                    output_queue=str(output_queue),
                    output_message_id=int(output_id),
                    payload=str(payload),
                )
            else:
                pending = PendingOutput(
                    source_queue=result.source_queue,
                    input_timestamp=result.timestamp,
                    output_queue=self.output_queue_name,
                    output_message_id=self._output_queue.generate_timestamp(),
                    payload=self._result_payload(result),
                )
                session.run(
                    """
                    INSERT INTO reactor_results
                        (
                            source_queue,
                            input_ts,
                            output_queue,
                            output_message_id,
                            payload,
                            status,
                            error,
                            updated_at_ns
                        )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        result.source_queue,
                        result.timestamp,
                        self.output_queue_name,
                        pending.output_message_id,
                        pending.payload,
                        "output_pending",
                        result.error,
                        now,
                    ),
                )

            self._advance_checkpoint(
                session,
                queue_name=result.source_queue,
                timestamp=result.timestamp,
                now_ns=now,
            )
            session.run(
                """
                INSERT INTO reactor_seen
                    (source_queue, input_ts, status, updated_at_ns)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_queue, input_ts) DO UPDATE SET
                    status = excluded.status,
                    updated_at_ns = excluded.updated_at_ns
                """,
                (result.source_queue, result.timestamp, "result_recorded", now),
            )
            session.run(
                """
                INSERT INTO reactor_audit
                    (lane, message_ts, event, detail, created_at_ns)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    result.source_queue,
                    result.timestamp,
                    "result_recorded",
                    "durable pending output recorded",
                    now,
                ),
            )

        self._checkpoints[result.source_queue] = max(
            self._checkpoints.get(result.source_queue, 0),
            result.timestamp,
        )
        return pending

    def _mark_output_written(self, pending: PendingOutput) -> None:
        now = time.time_ns()
        with self._metadata_queue.sidecar(transaction=True) as session:
            session.run(
                """
                UPDATE reactor_results
                SET status = ?, updated_at_ns = ?
                WHERE source_queue = ? AND input_ts = ?
                """,
                (
                    "output_written",
                    now,
                    pending.source_queue,
                    pending.input_timestamp,
                ),
            )
            session.run(
                """
                INSERT INTO reactor_seen
                    (source_queue, input_ts, status, updated_at_ns)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_queue, input_ts) DO UPDATE SET
                    status = excluded.status,
                    updated_at_ns = excluded.updated_at_ns
                """,
                (
                    pending.source_queue,
                    pending.input_timestamp,
                    "output_written",
                    now,
                ),
            )
            session.run(
                """
                INSERT INTO reactor_audit
                    (lane, message_ts, event, detail, created_at_ns)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    pending.source_queue,
                    pending.input_timestamp,
                    "output_written",
                    str(pending.output_message_id),
                    now,
                ),
            )

    # ------------------------------------------------------------------
    # Worker lanes
    # ------------------------------------------------------------------
    def _start_workers(self, count: int) -> None:
        for index in range(count):
            thread = threading.Thread(
                target=self._worker_loop,
                name=f"reactor-worker-{index}",
                daemon=True,
            )
            thread.start()
            self._worker_threads.append(thread)

    def _worker_loop(self) -> None:
        while not self._worker_stopping.is_set():
            item = self._work_queue.get()
            try:
                if item is None:
                    return
                try:
                    value = self._processor(item)
                    result = WorkerResult(
                        source_queue=item.source_queue,
                        timestamp=item.timestamp,
                        value=value,
                    )
                except Exception as exc:
                    result = WorkerResult(
                        source_queue=item.source_queue,
                        timestamp=item.timestamp,
                        error=repr(exc),
                    )
                self._worker_results.put(result)
                self.notify_reactor_activity()
            finally:
                self._work_queue.task_done()

    def _has_pending_reactor_results(self) -> bool:
        return self._reactor_activity_event.is_set() or not self._worker_results.empty()

    def _drain_reactor_results(self, *, max_results: int = 100) -> int:
        handled = 0
        while handled < max_results:
            try:
                result = self._worker_results.get_nowait()
            except thread_queue.Empty:
                # Advisory wake flag; the Queue is the source of truth.
                self._clear_reactor_activity()
                if not self._worker_results.empty():
                    self.notify_reactor_activity()
                return handled

            self._handle_worker_result(result)
            self._worker_results.task_done()
            handled += 1

        if not self._worker_results.empty():
            self.notify_reactor_activity()
        return handled

    def _handle_worker_result(self, result: WorkerResult) -> None:
        self._inflight.discard((result.source_queue, result.timestamp))
        pending = self._record_pending_result(result)
        if pending is not None:
            if not self._try_publish_output(pending):
                self._output_backlog_blocked = True

    def _request_reactor_workers_stop(self) -> None:
        self._worker_stopping.set()
        for _ in self._worker_threads:
            self._work_queue.put(None)

    def _join_reactor_workers(self, *, deadline: float) -> None:
        for thread in self._worker_threads:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)

    # ------------------------------------------------------------------
    # Queue processing
    # ------------------------------------------------------------------
    def _queue_ready_for_dispatch(self, queue_name: str) -> bool:
        if self._reactor_stop_event.is_set():
            return False
        if queue_name == self.control_in_queue:
            try:
                return self._managed_queue(queue_name).has_pending(
                    self._checkpoint_after(queue_name)
                )
            except OperationalError:
                if self._reactor_stop_event.is_set():
                    return False
                raise
        if queue_name not in self.input_queues:
            return False
        if self._output_backlog_blocked:
            return False
        if any(active_queue == queue_name for active_queue, _ts in self._inflight):
            return False
        queue_obj = self._managed_queue(queue_name)
        try:
            return queue_obj.has_pending(self._checkpoint_after(queue_name))
        except OperationalError:
            if self._reactor_stop_event.is_set():
                return False
            raise

    def _control_queue_ready(self) -> bool:
        if self._reactor_stop_event.is_set():
            return False
        try:
            return self._managed_queue(self.control_in_queue).has_pending(
                self._checkpoint_after(self.control_in_queue)
            )
        except OperationalError:
            if self._reactor_stop_event.is_set():
                return False
            raise

    def _has_pending_messages(self) -> bool:
        if self._reactor_stop_event.is_set():
            return False
        return any(self._queue_ready_for_dispatch(name) for name in self._queues)

    def _update_active_queues(self) -> None:
        still_active = [
            name for name in self._active_queues if self._queue_ready_for_dispatch(name)
        ]

        for name in self._queues:
            if name not in still_active and self._queue_ready_for_dispatch(name):
                still_active.append(name)

        if set(still_active) != set(self._active_queues):
            self._active_queues = still_active
            self._queue_iterator = (
                itertools.cycle(self._active_queues)
                if self._active_queues
                else itertools.cycle([])
            )
        self._check_counter += 1

    def _drain_queue(self) -> None:
        self._update_active_queues()
        if not self._active_queues:
            return

        processed = 0
        inactive_candidates: set[str] = set()
        for _ in range(len(self._active_queues)):
            try:
                queue_name = next(self._queue_iterator)
            except StopIteration:
                break

            if queue_name == self.control_in_queue:
                processed += int(self._process_control_message())
                if self._reactor_stop_event.is_set():
                    break
                if not self._queue_ready_for_dispatch(queue_name):
                    inactive_candidates.add(queue_name)
                continue

            processed += int(self._dispatch_next_input(queue_name))
            if self._reactor_stop_event.is_set():
                break
            if not self._queue_ready_for_dispatch(queue_name):
                inactive_candidates.add(queue_name)

        if inactive_candidates:
            self._active_queues = [
                name for name in self._active_queues if name not in inactive_candidates
            ]
            self._queue_iterator = (
                itertools.cycle(self._active_queues)
                if self._active_queues
                else itertools.cycle([])
            )

        if processed:
            self._strategy.notify_activity()

    def _dispatch_next_input(self, queue_name: str) -> bool:
        queue_obj = self._managed_queue(queue_name)
        rows = _timestamped_rows(
            queue_obj.peek_many(
                1,
                with_timestamps=True,
                after_timestamp=self._checkpoint_after(queue_name),
            )
        )
        if not rows:
            return False

        body, timestamp = rows[0]
        key = (queue_name, timestamp)
        if key in self._inflight:
            return False

        item = WorkItem(source_queue=queue_name, timestamp=timestamp, body=body)
        self._record_dispatch(item)
        self._inflight.add(key)
        self._work_queue.put(item)
        return True

    def _process_control_message(self) -> bool:
        queue_obj = self._managed_queue(self.control_in_queue)
        rows = _timestamped_rows(
            queue_obj.peek_many(
                1,
                with_timestamps=True,
                after_timestamp=self._checkpoint_after(self.control_in_queue),
            )
        )
        if not rows:
            return False
        body, timestamp = rows[0]

        try:
            payload: Any = json.loads(body)
        except json.JSONDecodeError:
            payload = {"command": body}
        stop_after_response = False

        if not isinstance(payload, Mapping):
            command = "<invalid>"
            response: dict[str, Any] = {
                "request_id": None,
                "command": command,
                "input_timestamp": timestamp,
                "ok": False,
                "error": (
                    "control payload must be a JSON object or plain-text command"
                ),
            }
        else:
            command = str(payload.get("command", "")).strip().upper()
            response = {
                "request_id": payload.get("request_id"),
                "command": command,
                "input_timestamp": timestamp,
            }

            if command == "PING":
                response["ok"] = True
                response["message"] = "PONG"
            elif command == "STATUS":
                response["ok"] = True
                response["checkpoints"] = self._load_checkpoints()
                response["live_inflight"] = [
                    {"queue": queue_name, "timestamp": ts}
                    for queue_name, ts in sorted(self._inflight)
                ]
                response["published_outputs_live"] = self._outputs_published
                response["pending_output_backlog"] = self._pending_output_count()
                response["output_backlog_blocked"] = self._output_backlog_blocked
                response["result_status_counts"] = self._result_status_counts()
                response["seen_status_counts"] = self._seen_status_counts()
            elif command == "STOP":
                response["ok"] = True
                response["message"] = "stopping"
                stop_after_response = True
            else:
                response["ok"] = False
                response["error"] = f"unknown command: {command or '<empty>'}"

        self._control_out_queue.write(
            json.dumps(response, sort_keys=True, separators=(",", ":"))
        )
        self._control_messages_handled += 1
        self._record_control_processed(
            timestamp=timestamp,
            command=command or "<empty>",
        )
        if stop_after_response:
            self.request_stop()
        return True

    def _record_control_processed(
        self,
        *,
        timestamp: int,
        command: str,
    ) -> None:
        now = time.time_ns()
        with self._metadata_queue.sidecar(transaction=True) as session:
            self._advance_checkpoint(
                session,
                queue_name=self.control_in_queue,
                timestamp=timestamp,
                now_ns=now,
            )
            session.run(
                """
                INSERT INTO reactor_audit
                    (lane, message_ts, event, detail, created_at_ns)
                VALUES (?, ?, ?, ?, ?)
                """,
                (self.control_in_queue, timestamp, "control", command, now),
            )
        self._checkpoints[self.control_in_queue] = max(
            self._checkpoints.get(self.control_in_queue, 0),
            timestamp,
        )

    # ------------------------------------------------------------------
    # Durable output publication
    # ------------------------------------------------------------------
    @staticmethod
    def _result_payload(result: WorkerResult) -> str:
        envelope = {
            "source_queue": result.source_queue,
            "input_timestamp": result.timestamp,
            "ok": result.error is None,
            "value": result.value,
            "error": result.error,
        }
        try:
            return json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            fallback = {
                "source_queue": result.source_queue,
                "input_timestamp": result.timestamp,
                "ok": False,
                "value": None,
                "error": (
                    f"processor result was not JSON serializable: {type(exc).__name__}"
                ),
            }
            return json.dumps(fallback, sort_keys=True, separators=(",", ":"))

    def _publish_output(self, pending: PendingOutput) -> None:
        if pending.output_queue != self.output_queue_name:
            raise RuntimeError(
                "pending output route mismatch: "
                f"stored {pending.output_queue!r}, "
                f"configured {self.output_queue_name!r}; row was not published"
            )
        try:
            self._output_queue.insert_messages(
                [(pending.payload, pending.output_message_id)]
            )
        except IntegrityError:
            existing = self._output_queue.peek_one(
                exact_timestamp=pending.output_message_id,
                include_claimed=True,
            )
            if existing is None:
                raise
        self._mark_output_written(pending)
        self._outputs_published += 1

    def _try_publish_output(self, pending: PendingOutput) -> bool:
        try:
            self._publish_output(pending)
        except OperationalError:
            return False
        return True

    def _pending_output_rows(self, *, limit: int) -> list[PendingOutput]:
        with self._metadata_queue.sidecar() as session:
            rows = list(
                session.run(
                    """
                    SELECT
                        source_queue,
                        input_ts,
                        output_queue,
                        output_message_id,
                        payload
                    FROM reactor_results
                    WHERE status = ?
                    ORDER BY input_ts
                    LIMIT ?
                    """,
                    ("output_pending", limit),
                    fetch=True,
                )
            )
        return [
            PendingOutput(
                source_queue=str(source_queue),
                input_timestamp=int(input_ts),
                output_queue=str(output_queue),
                output_message_id=int(output_message_id),
                payload=str(payload),
            )
            for source_queue, input_ts, output_queue, output_message_id, payload in rows
        ]

    def _drain_reactor_backlog(self) -> bool:
        max_outputs = 1 if self._control_queue_ready() else 100
        self._output_backlog_blocked = not self._drain_pending_outputs(
            max_outputs=max_outputs
        )
        # Output backlog failure backpressures new input dispatch, but the
        # control lane must stay live so STATUS and STOP can still get through.
        return True

    def _has_pending_reactor_backlog(self) -> bool:
        if self._output_backlog_blocked:
            return True
        try:
            return self._pending_output_count() > 0
        except OperationalError:
            return False

    def _drain_pending_outputs(self, *, max_outputs: int = 100) -> bool:
        pending_rows = self._pending_output_rows(limit=max_outputs + 1)
        has_more = len(pending_rows) > max_outputs
        for index, pending in enumerate(pending_rows):
            if index == max_outputs:
                break
            if not self._try_publish_output(pending):
                return False
        return not has_more

    def _result_status_counts(self) -> dict[str, int]:
        with self._metadata_queue.sidecar() as session:
            rows = list(
                session.run(
                    """
                    SELECT status, COUNT(*)
                    FROM reactor_results
                    GROUP BY status
                    """,
                    fetch=True,
                )
            )
        return {str(status): int(count) for status, count in rows}

    def _pending_output_count(self) -> int:
        with self._metadata_queue.sidecar() as session:
            rows = list(
                session.run(
                    """
                    SELECT COUNT(*)
                    FROM reactor_results
                    WHERE status = ?
                    """,
                    ("output_pending",),
                    fetch=True,
                )
            )
        return int(rows[0][0]) if rows else 0

    def _seen_status_counts(self) -> dict[str, int]:
        with self._metadata_queue.sidecar() as session:
            rows = list(
                session.run(
                    """
                    SELECT status, COUNT(*)
                    FROM reactor_seen
                    GROUP BY status
                    """,
                    fetch=True,
                )
            )
        return {str(status): int(count) for status, count in rows}

    def _close_reactor_resources(self) -> None:
        self._output_queue.close()
        self._control_out_queue.close()
        super()._close_reactor_resources()


def _write_json(queue: Queue, payload: JsonMapping) -> None:
    queue.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def _read_json_messages(
    queue_name: str, db_path: Path
) -> list[tuple[dict[str, Any], int]]:
    queue = Queue(queue_name, db_path=str(db_path))
    messages: list[tuple[dict[str, Any], int]] = []
    for body, timestamp in _timestamped_rows(queue.peek_many(with_timestamps=True)):
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"raw": body}
        messages.append((payload, timestamp))
    return messages


def _send_control(db_path: Path, command: str, request_id: str) -> None:
    control_in = Queue("reactor.control.in", db_path=str(db_path))
    _write_json(control_in, {"command": command, "request_id": request_id})


def _wait_for_control_reply(
    db_path: Path,
    *,
    request_id: str,
    timeout: float = 2.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for payload, _timestamp in _read_json_messages("reactor.control.out", db_path):
            if payload.get("request_id") == request_id:
                return payload
        time.sleep(0.01)
    raise TimeoutError(f"timed out waiting for control reply {request_id!r}")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "reference_reactor.db"
        inbox_a = Queue("reactor.inbox.a", db_path=str(db_path))
        inbox_b = Queue("reactor.inbox.b", db_path=str(db_path))

        _write_json(inbox_a, {"kind": "email", "id": 1})
        _write_json(inbox_b, {"kind": "report", "id": 2})
        _write_json(inbox_a, {"kind": "email", "id": 3})

        reactor = Reactor(
            input_queues=["reactor.inbox.a", "reactor.inbox.b"],
            output_queue="reactor.outbox",
            control_in_queue="reactor.control.in",
            control_out_queue="reactor.control.out",
            db=db_path,
            worker_count=2,
        )

        reactor_thread = reactor.start()
        final_status: dict[str, Any] | None = None
        try:
            deadline = time.monotonic() + 5.0
            attempt = 0
            while time.monotonic() < deadline:
                request_id = f"status-{attempt}"
                _send_control(db_path, "STATUS", request_id)
                status = _wait_for_control_reply(db_path, request_id=request_id)
                final_status = status
                if int(status.get("published_outputs_live", 0)) >= 3:
                    break
                attempt += 1
                time.sleep(0.02)
            else:
                raise TimeoutError("reactor did not publish all outputs")

            _send_control(db_path, "STOP", "stop-demo")
            stop_reply = _wait_for_control_reply(db_path, request_id="stop-demo")
            reactor_thread.join(timeout=2.0)
            if reactor_thread.is_alive():
                raise TimeoutError("reactor thread did not stop")

            print("Output messages:")
            for payload, timestamp in _read_json_messages("reactor.outbox", db_path):
                print(f"  {timestamp}: {json.dumps(payload, sort_keys=True)}")
            print("Final STATUS:")
            print(f"  {json.dumps(final_status, sort_keys=True)}")
            print("STOP reply:")
            print(f"  {json.dumps(stop_reply, sort_keys=True)}")
        finally:
            reactor.stop()


if __name__ == "__main__":
    main()
