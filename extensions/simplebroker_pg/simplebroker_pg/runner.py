"""Postgres SQLRunner implementation for SimpleBroker."""

from __future__ import annotations

import contextlib
import os
import selectors
import socket
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, replace
from typing import Any, cast

import psycopg
from psycopg import conninfo
from psycopg.pq import TransactionStatus
from psycopg_pool import ConnectionPool, PoolTimeout

from simplebroker._backend_plugins import BackendPlugin
from simplebroker._exceptions import DataError, IntegrityError, OperationalError
from simplebroker._runner import SetupPhase

from ._connection_owner import _owner
from ._failure_order import capture_ordinary_pg_cleanup
from ._identifiers import activity_channel_name, queue_activity_channel_name
from .validation import quote_ident

# The listener owns one separate connection, so the default bounds each active
# process-session target to at most four PostgreSQL connections in total.
_DEFAULT_MIN_SIZE = 0
_DEFAULT_MAX_SIZE = 3

# Seconds per psycopg-pool idle check ([SB-API-3]). Each check closes at most
# one pooled connection that stayed unused for the whole preceding check, so a
# quiet process's last data connection closes 10-20 s after its last use and a
# full three-connection pool drains within 40 s. Coupled with
# _ACTIVITY_SAFETY_RECHECK_SECONDS below: keep 2 x this value under that
# interval, or a healthy LISTEN waiter's safety recheck keeps a data
# connection open forever.
_POOL_MAX_IDLE_SECONDS = 10.0

# Seconds between safety rechecks that PostgreSQL activity waiters declare to
# PollingStrategy ([SB-API-6]); each quiet recheck is drawn from [R, 2R). A
# live LISTEN session receives every committed NOTIFY, so the recheck only
# bounds recovery from an undetected listener-session loss (or a writer that
# bypasses SimpleBroker and skips pg_notify). Must stay above
# 2 x _POOL_MAX_IDLE_SECONDS (see above).
_ACTIVITY_SAFETY_RECHECK_SECONDS = 30.0

# A forked child must not finalize a pool whose locks belong to parent threads.
_ABANDONED_FORK_POSTGRES_POOLS: list[Any] = []

# Module-owned capability seam for PostgresRunner.run_transaction(): libpq
# pipeline mode needs libpq 14 or newer. Tests patch this alias, not psycopg.
_pipeline_supported = psycopg.Pipeline.is_supported


def _abandon_inherited_pool(pool: Any) -> None:
    if not any(pool is existing for existing in _ABANDONED_FORK_POSTGRES_POOLS):
        _ABANDONED_FORK_POSTGRES_POOLS.append(pool)


def _run_activity_waiter_cleanup(actions: Iterable[Callable[[], None]]) -> None:
    first_error: Exception | None = None
    for action in actions:
        try:
            action()
        except Exception as error:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-035] exception
            if first_error is None:
                first_error = error
            else:
                later_notes = (
                    ()
                    if error is first_error
                    else tuple(getattr(error, "__notes__", ()))
                )
                first_error.add_note(
                    f"cleanup failure: {type(error).__qualname__}: {error}"
                )
                for note in later_notes:
                    first_error.add_note(note)
    if first_error is not None:
        raise first_error


@dataclass(frozen=True, slots=True)
class RunnerMetaState:
    """Cached broker metadata for one schema-bound runner."""

    magic: str
    schema_version: int
    last_ts: int
    alias_version: int


@dataclass(slots=True)
class _ThreadLease:
    """One thread's retained checkout and nested lease count."""

    conn: psycopg.Connection[Any] | None
    depth: int


@dataclass(slots=True)
class _FanInWaiterEntry:
    """Listener-local fan-in waiter registration."""

    condition: threading.Condition
    queue_names: tuple[str, ...]
    queue_set: set[str]


def _quoted_sql_end(sql: str, start: int) -> int:
    """Return the end of one quoted PostgreSQL token."""
    quote = sql[start]
    escape_prefixed = (
        quote == "'"
        and start > 0
        and sql[start - 1] in {"e", "E"}
        and (start < 2 or not (sql[start - 2].isalnum() or sql[start - 2] in "_$"))
    ) or (
        start > 1
        and sql[start - 2 : start].lower() == "u&"
        and (start < 3 or not (sql[start - 3].isalnum() or sql[start - 3] in "_$"))
    )
    index = start + 1
    while index < len(sql):
        if escape_prefixed and sql[index] == "\\" and index + 1 < len(sql):
            index += 2
        elif sql[index] != quote:
            index += 1
        elif index + 1 < len(sql) and sql[index + 1] == quote:
            index += 2
        else:
            return index + 1
    return index


def _block_comment_end(sql: str, start: int) -> int:
    """Return the end of one possibly nested PostgreSQL block comment."""
    index = start + 2
    depth = 1
    while index < len(sql) and depth:
        if sql.startswith("/*", index):
            depth += 1
            index += 2
        elif sql.startswith("*/", index):
            depth -= 1
            index += 2
        else:
            index += 1
    return index


def _dollar_quoted_sql_end(sql: str, start: int) -> int | None:
    """Return the end of a dollar-quoted token, or None for an ordinary dollar."""
    if start > 0 and (sql[start - 1].isalnum() or sql[start - 1] in "_$"):
        return None
    delimiter_end = sql.find("$", start + 1)
    if delimiter_end == -1:
        return None
    tag = sql[start + 1 : delimiter_end]
    if tag and (
        not (tag[0].isalpha() or tag[0] == "_")
        or any(not (part.isalnum() or part == "_") for part in tag[1:])
    ):
        return None
    delimiter = sql[start : delimiter_end + 1]
    body_end = sql.find(delimiter, delimiter_end + 1)
    return len(sql) if body_end == -1 else body_end + len(delimiter)


def _adapt_sql(sql: str) -> str:
    """Adapt qmark placeholders outside PostgreSQL lexical literals."""
    adapted: list[str] = []
    index = 0
    while index < len(sql):
        end: int | None = None
        if sql[index] in {"'", '"'}:
            end = _quoted_sql_end(sql, index)
        elif sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            end = len(sql) if newline == -1 else newline + 1
        elif sql.startswith("/*", index):
            end = _block_comment_end(sql, index)
        elif sql[index] == "$":
            end = _dollar_quoted_sql_end(sql, index)

        if end is not None:
            adapted.append(sql[index:end].replace("%", "%%"))
            index = end
        elif sql.startswith("??", index):
            adapted.append("?")
            index += 2
        elif sql[index] == "?":
            adapted.append("%s")
            index += 1
        elif sql[index] == "%":
            adapted.append("%%")
            index += 1
        else:
            adapted.append(sql[index])
            index += 1
    return "".join(adapted)


def _should_prepare(sql: str) -> bool:
    """Return whether this statement is hot enough to force preparation."""
    normalized = " ".join(sql.split())

    if normalized.startswith("UPDATE meta SET last_ts = %s"):
        return True
    if normalized.startswith("WITH inserted AS ( INSERT INTO messages"):
        return True
    if normalized.startswith("WITH allocated AS ( UPDATE meta"):
        return True
    if normalized == "SELECT last_ts FROM meta WHERE singleton = TRUE FOR UPDATE":
        return True
    if (
        normalized.startswith("WITH selected AS MATERIALIZED ( SELECT ts")
        and "UPDATE messages AS m SET claimed = TRUE" in normalized
    ):
        return True
    return (
        normalized.startswith("WITH target_queue AS ( SELECT %s AS queue_name )")
        and "UPDATE messages AS m SET queue = (SELECT queue_name FROM target_queue)"
        in normalized
    )


# Postgres contention SQLSTATEs that must be retried even though their
# messages do not contain SQLite's lock/busy phrases.
_RETRYABLE_SQLSTATES = frozenset(
    {
        "40001",  # serialization_failure
        "40P01",  # deadlock_detected
        "55P03",  # lock_not_available
    }
)


def _translate_error(exc: psycopg.Error) -> Exception:
    """Translate psycopg errors into SimpleBroker exceptions."""
    sqlstate = getattr(exc, "sqlstate", None) or ""
    if sqlstate.startswith("23"):
        return IntegrityError(str(exc))
    if sqlstate.startswith("22"):
        return DataError(str(exc))
    err = OperationalError(str(exc))
    if sqlstate in _RETRYABLE_SQLSTATES:
        err.retryable = True
    return err


_FAILED_STATEMENT_ATTRIBUTE = "_failed_statement"


class _TransactionFailure(Exception):
    """A driver failure inside ``run_transaction`` and the statement it reached."""

    def __init__(self, cause: psycopg.Error, statement: int | None) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.statement = statement


def failed_statement(error: BaseException) -> int | None:
    """Return the ``run_transaction`` statement index a failure reached.

    The index names the first statement that did not complete (it failed, was
    aborted with the transaction, or was never sent). ``None`` means the
    failure was outside every statement: the commit, or ``BEGIN`` on the
    sequential transport, or the error did not come from ``run_transaction``.
    """
    value = getattr(error, _FAILED_STATEMENT_ATTRIBUTE, None)
    return value if isinstance(value, int) else None


def _connection_idle(conn: psycopg.Connection[Any]) -> bool:
    """Return whether a checkout is open, out of any transaction, and reusable."""
    return not conn.closed and conn.info.transaction_status == TransactionStatus.IDLE


def _invalidates_bootstrap(exc: psycopg.Error) -> bool:
    """Return whether an error means cached schema state is no longer safe."""
    sqlstate = getattr(exc, "sqlstate", None) or ""
    return sqlstate in {"3F000", "42P01"}


_LISTENER_POLL_INTERVAL = 0.1
"""Longest listener wait slice; bounds stop observation without a wake byte."""


class _SharedActivityListener:
    """Process-local LISTEN worker shared by pg watchers for one schema.

    State machine ``SM-PG-LISTENER`` (docs/implementation/07). The listener
    thread is the only user of its connection. From startup it LISTENs the
    schema channel, which carries the ``*`` wildcard and notifications from
    writers that predate per-queue channels. For every registered queue it
    also LISTENs that queue's channel; registration returns only after that
    LISTEN committed, so every write committed afterwards reaches this
    session. Routing stays payload-based, so channel collisions and
    schema-channel traffic cannot wake an unregistered queue.
    Plan: docs/plans/2026-11-08-pg-notify-fanout-plan.md.
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str,
        startup_timeout: float = 5.0,
    ) -> None:
        self._dsn = dsn
        self._schema = schema
        self._connection_kwargs = _owner.capture()
        self._channel = activity_channel_name(schema)
        self._subscription_timeout = startup_timeout
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._conditions: dict[str, threading.Condition] = {}
        self._versions: dict[str, int] = {}
        self._queue_refcounts: dict[str, int] = {}
        self._fan_in_entries: dict[int, _FanInWaiterEntry] = {}
        self._next_fan_in_id = 0
        self._wildcard_version = 0
        # Queue-channel subscriptions. Registrants advance the requested
        # generation; the listener thread publishes the generation it applied.
        # ``_listening`` holds channels whose LISTEN committed and whose
        # UNLISTEN has not started, so it alone may justify the fast path.
        self._queue_channels: dict[str, str] = {}
        self._listening: set[str] = set()
        self._subscription_requested = 0
        self._subscription_applied = 0
        self._subscription_changed = threading.Condition(self._lock)
        self._ready = threading.Event()
        self._conn: psycopg.Connection[Any] | None = None
        self._error: Exception | None = None
        # The listener thread owns and closes both ends after it starts. Any
        # thread may wake it, so sends and the writer's close are serialized:
        # a send after close could reach a descriptor that reused the number.
        self._wake_reader, self._wake_writer = socket.socketpair()
        self._wake_lock = threading.Lock()
        self._wake_closed = False
        self._wake_reader.setblocking(False)
        self._wake_writer.setblocking(False)
        self._thread = threading.Thread(
            target=self._run,
            name=f"simplebroker-pg-listener-{self._channel}",
            daemon=True,
        )
        try:
            self._thread.start()
        except BaseException:
            self._wake_reader.close()
            self._close_wake_writer()
            raise
        if not self._ready.wait(timeout=startup_timeout):
            self.close()
            raise OperationalError("Postgres activity listener did not start")
        if self._error is not None:
            self.close()
            if isinstance(self._error, psycopg.Error):
                raise _translate_error(self._error) from self._error
            raise OperationalError(str(self._error)) from self._error

    def _run(self) -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-019] exception
        selector: selectors.BaseSelector | None = None
        try:
            conn = psycopg.connect(
                self._dsn, autocommit=True, **self._connection_kwargs
            )
            self._conn = conn
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {quote_ident(self._channel)}")
            selector = selectors.DefaultSelector()
            selector.register(conn.fileno(), selectors.EVENT_READ)
            selector.register(self._wake_reader, selectors.EVENT_READ)
            self._ready.set()

            while not self._stop_event.is_set():
                self._apply_subscriptions(conn)
                # One pass drains psycopg's backlog (notifications received
                # during a LISTEN) and polls once without blocking. A single
                # commit can emit several notifications (rename fires old +
                # new); consume the whole batch, because closing the generator
                # early discards notifications already pulled from libpq.
                for notify in conn.notifies(timeout=0):
                    with self._lock:
                        if notify.payload == "*":
                            self._wildcard_version += 1
                            for condition in self._conditions.values():
                                condition.notify_all()
                            for entry in self._fan_in_entries.values():
                                entry.condition.notify_all()
                        elif notify.payload in self._queue_refcounts:
                            self._versions[notify.payload] += 1
                            self._conditions[notify.payload].notify_all()
                            for entry in self._fan_in_entries.values():
                                if notify.payload in entry.queue_set:
                                    entry.condition.notify_all()
                for key, _mask in selector.select(timeout=_LISTENER_POLL_INTERVAL):
                    if key.fileobj is self._wake_reader:
                        self._drain_wake()
        except Exception as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-004] exception
            if not self._stop_event.is_set():
                self._error = exc
                self._ready.set()
                with self._lock:
                    for condition in self._conditions.values():
                        condition.notify_all()
                    for entry in self._fan_in_entries.values():
                        entry.condition.notify_all()
                    self._subscription_changed.notify_all()
        finally:
            self._ready.set()
            if selector is not None:
                selector.close()
            # The listener thread alone executes statements on its connection,
            # so it alone closes it, after any in-flight statement returned.
            if self._conn is not None:
                with contextlib.suppress(Exception):
                    self._conn.close()
            self._wake_reader.close()
            self._close_wake_writer()

    def _apply_subscriptions(self, conn: psycopg.Connection[Any]) -> None:
        """Reconcile queue-channel LISTENs with registrations (listener thread)."""
        with self._lock:
            requested = self._subscription_requested
            if requested == self._subscription_applied:
                return
            desired = set(self._queue_channels.values())
            to_listen = sorted(desired - self._listening)
            to_unlisten = sorted(self._listening - desired)
            # Leave the committed set before UNLISTEN runs so a concurrent
            # registrant cannot take the fast path on a closing subscription.
            self._listening.difference_update(to_unlisten)
        statements = [f"UNLISTEN {quote_ident(channel)}" for channel in to_unlisten]
        statements.extend(f"LISTEN {quote_ident(channel)}" for channel in to_listen)
        if statements:
            # One simple-protocol batch: one implicit transaction per pass.
            with conn.cursor() as cur:
                cur.execute("; ".join(statements))
        with self._lock:
            self._listening.update(to_listen)
            self._subscription_applied = requested
            self._subscription_changed.notify_all()

    def _drain_wake(self) -> None:
        with contextlib.suppress(BlockingIOError, InterruptedError):
            while self._wake_reader.recv(4096):
                pass

    def _wake(self) -> None:
        # Coalescing and best effort: a full buffer already guarantees a wake,
        # and a closed writer means the listener thread has exited.
        with self._wake_lock:
            if self._wake_closed:
                return
            with contextlib.suppress(OSError):
                self._wake_writer.send(b"\0")

    def _close_wake_writer(self) -> None:
        with self._wake_lock:
            self._wake_closed = True
            self._wake_writer.close()

    def _request_subscriptions_locked(self) -> int:
        self._subscription_requested += 1
        return self._subscription_requested

    def _await_subscriptions(self, generation: int) -> bool:
        """Wait until the listener applied ``generation``.

        Returns ``False`` only on timeout. A stopped or failed listener
        returns ``True`` at once: its next ``wait()`` reports that state.
        """
        self._wake()
        deadline = time.monotonic() + self._subscription_timeout
        with self._lock:
            while (
                self._subscription_applied < generation
                and self._error is None
                and not self._stop_event.is_set()
                and self._thread.is_alive()
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._subscription_changed.wait(timeout=remaining)
            return True

    def _register_queue_locked(self, queue_name: str) -> bool:
        """Count one reference; return whether its channel needs a LISTEN."""
        refcount = self._queue_refcounts.get(queue_name, 0)
        self._queue_refcounts[queue_name] = refcount + 1
        if refcount == 0:
            self._conditions[queue_name] = threading.Condition(self._lock)
            self._versions[queue_name] = 0
            self._queue_channels[queue_name] = queue_activity_channel_name(
                self._schema, queue_name
            )
        return self._queue_channels[queue_name] not in self._listening

    def register_queue(self, queue_name: str) -> tuple[int, int]:
        """Register a waiter queue and return its current notification versions.

        Versions are captured before the subscription is awaited, so a
        notification routed while it is pending counts as activity.
        """
        with self._lock:
            needs_listen = self._register_queue_locked(queue_name)
            versions = self._versions[queue_name], self._wildcard_version
            if not needs_listen:
                return versions
            generation = self._request_subscriptions_locked()
        try:
            subscribed = self._await_subscriptions(generation)
        except BaseException:
            # Any interruption (including KeyboardInterrupt) leaves no owner
            # for this reference; unwind it so the channel can be UNLISTENed.
            self.unregister_queue(queue_name)
            raise
        if not subscribed:
            self.unregister_queue(queue_name)
            raise OperationalError("Postgres activity listener did not subscribe")
        return versions

    def unregister_queue(self, queue_name: str) -> None:
        """Release a waiter queue registration."""
        with self._lock:
            refcount = self._queue_refcounts.get(queue_name)
            if refcount is None:
                return
            if refcount > 1:
                self._queue_refcounts[queue_name] = refcount - 1
                return
            condition = self._conditions.pop(queue_name, None)
            self._versions.pop(queue_name, None)
            self._queue_channels.pop(queue_name, None)
            del self._queue_refcounts[queue_name]
            if condition is not None:
                condition.notify_all()
            self._request_subscriptions_locked()
        self._wake()

    def register_queue_set(
        self, queue_names: tuple[str, ...]
    ) -> tuple[int, dict[str, int], int]:
        """Register one fan-in waiter over multiple queues."""
        with self._lock:
            fan_in_id = self._next_fan_in_id
            self._next_fan_in_id += 1
            entry = _FanInWaiterEntry(
                condition=threading.Condition(self._lock),
                queue_names=queue_names,
                queue_set=set(queue_names),
            )
            self._fan_in_entries[fan_in_id] = entry

            versions: dict[str, int] = {}
            needs_listen = False
            for queue_name in queue_names:
                needs_listen = self._register_queue_locked(queue_name) or needs_listen
                versions[queue_name] = self._versions[queue_name]
            wildcard_version = self._wildcard_version
            if not needs_listen:
                return fan_in_id, versions, wildcard_version
            generation = self._request_subscriptions_locked()
        try:
            subscribed = self._await_subscriptions(generation)
        except BaseException:
            # Same ownership rule as register_queue: no waiter will release it.
            self.unregister_queue_set(fan_in_id)
            raise
        if not subscribed:
            self.unregister_queue_set(fan_in_id)
            raise OperationalError("Postgres activity listener did not subscribe")
        return fan_in_id, versions, wildcard_version

    def unregister_queue_set(self, fan_in_id: int) -> None:
        """Release one fan-in waiter registration."""
        with self._lock:
            entry = self._fan_in_entries.pop(fan_in_id, None)
            if entry is None:
                return
            entry.condition.notify_all()
            queue_names = entry.queue_names

        for queue_name in queue_names:
            self.unregister_queue(queue_name)

    def wait(
        self,
        *,
        queue_name: str,
        stop_event: threading.Event,
        timeout: float,
        last_queue_version: int,
        last_wildcard_version: int,
    ) -> tuple[bool, int, int]:
        deadline = time.monotonic() + max(timeout, 0.0)
        with self._lock:
            condition = self._conditions.get(queue_name)
            if condition is None:
                return False, last_queue_version, self._wildcard_version

            while not stop_event.is_set() and not self._stop_event.is_set():
                if self._error is not None:
                    raise self._error
                queue_version = self._versions.get(queue_name, last_queue_version)
                wildcard_version = self._wildcard_version
                if (
                    queue_version != last_queue_version
                    or wildcard_version != last_wildcard_version
                ):
                    return True, queue_version, wildcard_version

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False, queue_version, wildcard_version
                condition.wait(timeout=remaining)
        with self._lock:
            return (
                False,
                self._versions.get(queue_name, last_queue_version),
                self._wildcard_version,
            )

    def wait_any(
        self,
        *,
        fan_in_id: int,
        stop_event: threading.Event,
        timeout: float,
        last_queue_versions: dict[str, int],
        last_wildcard_version: int,
    ) -> tuple[bool, dict[str, int], int]:
        deadline = time.monotonic() + max(timeout, 0.0)
        with self._lock:
            entry = self._fan_in_entries.get(fan_in_id)
            if entry is None:
                return False, dict(last_queue_versions), self._wildcard_version

            while not stop_event.is_set() and not self._stop_event.is_set():
                if self._error is not None:
                    raise self._error
                queue_versions = {
                    queue_name: self._versions.get(
                        queue_name, last_queue_versions.get(queue_name, 0)
                    )
                    for queue_name in entry.queue_names
                }
                wildcard_version = self._wildcard_version
                if (
                    any(
                        queue_versions[queue_name]
                        != last_queue_versions.get(queue_name, 0)
                        for queue_name in entry.queue_names
                    )
                    or wildcard_version != last_wildcard_version
                ):
                    return True, queue_versions, wildcard_version

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False, queue_versions, wildcard_version
                entry.condition.wait(timeout=remaining)

        with self._lock:
            entry = self._fan_in_entries.get(fan_in_id)
            if entry is None:
                return False, dict(last_queue_versions), self._wildcard_version
            return (
                False,
                {
                    queue_name: self._versions.get(
                        queue_name, last_queue_versions.get(queue_name, 0)
                    )
                    for queue_name in entry.queue_names
                },
                self._wildcard_version,
            )

    def close(self) -> None:
        """Stop the listener; its thread closes its own connection.

        Never touches the connection: the listener thread may be inside a
        LISTEN/UNLISTEN batch on it, and psycopg's ``close()`` frees the libpq
        connection without taking the connection lock. The thread sees the
        stop request after that statement returns and closes the connection in
        ``_run``'s ``finally``; this call returns after the bounded join either
        way. Plan: docs/plans/2026-11-10-backend-thread-ownership-plan.md (D1).
        """
        self._stop_event.set()
        self._wake()
        with self._lock:
            for condition in self._conditions.values():
                condition.notify_all()
            for entry in self._fan_in_entries.values():
                entry.condition.notify_all()
            self._subscription_changed.notify_all()
        self._thread.join(timeout=1.0)


class _SharedActivityRegistry:
    """Reference-counted process-local listener registry."""

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._listeners: dict[
            tuple[int, str, str], tuple[_SharedActivityListener, int]
        ] = {}

    def _reset_after_fork_if_needed(self) -> None:
        current_pid = os.getpid()
        if current_pid == self._pid:
            return
        self._pid = current_pid
        self._lock = threading.Lock()
        self._listeners = {}

    def acquire(self, dsn: str, *, schema: str) -> _SharedActivityListener:
        """Return a counted reference to a listener that has not failed.

        A listener whose thread published a failure is never handed out again;
        a new listener replaces its entry. Holders of the failed listener keep
        it: each sees the stored failure on its next wait and recovers by
        acquiring again (the watcher's ordinary retry).
        """
        self._reset_after_fork_if_needed()
        key = (self._pid, dsn, schema)
        with self._lock:
            entry = self._listeners.get(key)
            if entry is not None:
                listener, refcount = entry
                if listener._error is None:
                    self._listeners[key] = (listener, refcount + 1)
                    return listener

            listener = _SharedActivityListener(dsn, schema=schema)
            self._listeners[key] = (listener, 1)
            return listener

    def release(
        self,
        dsn: str,
        *,
        schema: str,
        listener: _SharedActivityListener,
    ) -> None:
        """Surrender one reference to ``listener``; close it after the last one.

        Releasing a listener that ``acquire`` already replaced is a no-op. Its
        failed thread owns its cleanup and closes its connection in
        ``finally``. Closing it here would also turn the stored failure into
        quiet wait results for holders that have not observed it yet.
        """
        self._reset_after_fork_if_needed()
        key = (self._pid, dsn, schema)
        with self._lock:
            entry = self._listeners.get(key)
            if entry is None:
                return
            current_listener, refcount = entry
            if current_listener is not listener:
                return
            if refcount > 1:
                self._listeners[key] = (current_listener, refcount - 1)
                return
            del self._listeners[key]
        listener.close()


_activity_registry = _SharedActivityRegistry()


_INHERITED_WAITER_MESSAGE = (
    "activity waiter was inherited across fork(); create a new waiter in this process"
)


def _raise_if_inherited(owner_pid: int) -> None:
    """Refuse to wait on a listener that belongs to the parent process.

    A forked child inherits the waiter and its listener object but not the
    listener thread, and a lock a parent thread held at the fork stays held.
    Plan: docs/plans/2026-11-10-fork-inherited-activity-waiter-plan.md (D3).
    """
    if os.getpid() != owner_pid:
        raise OperationalError(_INHERITED_WAITER_MESSAGE)


class PostgresActivityWaiter:
    """LISTEN/NOTIFY-backed wake-up waiter for pg watchers.

    ``safety_recheck_interval`` declares PostgreSQL's quiet safety-recheck
    cadence to ``PollingStrategy`` ([SB-API-6]).
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str,
        queue_name: str,
        stop_event: threading.Event,
    ) -> None:
        self.safety_recheck_interval = _ACTIVITY_SAFETY_RECHECK_SECONDS
        self._dsn = dsn
        self._schema = schema
        self._queue_name = queue_name
        self._stop_event = stop_event
        self._listener = _activity_registry.acquire(dsn, schema=schema)
        self._closed = False
        self._pid = os.getpid()
        try:
            self._last_queue_version, self._last_wildcard_version = (
                self._listener.register_queue(queue_name)
            )
        except BaseException:
            # Registration failed and rolled itself back; release our reference.
            _activity_registry.release(dsn, schema=schema, listener=self._listener)
            raise

    def wait(self, timeout: float) -> bool:
        """Wait for a relevant queue notification until timeout expires."""
        _raise_if_inherited(self._pid)
        try:
            previous_queue_version = self._last_queue_version
            previous_wildcard_version = self._last_wildcard_version
            changed, queue_version, wildcard_version = self._listener.wait(
                queue_name=self._queue_name,
                stop_event=self._stop_event,
                timeout=timeout,
                last_queue_version=self._last_queue_version,
                last_wildcard_version=self._last_wildcard_version,
            )
            if changed:
                if queue_version > previous_queue_version:
                    self._last_queue_version = previous_queue_version + 1
                else:
                    self._last_queue_version = queue_version

                if wildcard_version > previous_wildcard_version:
                    self._last_wildcard_version = previous_wildcard_version + 1
                else:
                    self._last_wildcard_version = wildcard_version
            else:
                self._last_queue_version = queue_version
                self._last_wildcard_version = wildcard_version
            return changed
        except psycopg.Error as exc:
            raise _translate_error(exc) from exc

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if os.getpid() != self._pid:
            # Inherited across fork(): abandon the parent's listener and
            # registry entry instead of locking them (D2).
            return
        _run_activity_waiter_cleanup(
            (
                lambda: self._listener.unregister_queue(self._queue_name),
                lambda: _activity_registry.release(
                    self._dsn,
                    schema=self._schema,
                    listener=self._listener,
                ),
            )
        )


class PostgresMultiQueueActivityWaiter:
    """LISTEN/NOTIFY-backed fan-in waiter for multiple pg queues.

    ``safety_recheck_interval`` declares PostgreSQL's quiet safety-recheck
    cadence to ``PollingStrategy`` ([SB-API-6]).
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str,
        queue_names: tuple[str, ...],
        stop_event: threading.Event,
    ) -> None:
        if not queue_names:
            raise ValueError("queue_names cannot be empty")
        self.safety_recheck_interval = _ACTIVITY_SAFETY_RECHECK_SECONDS
        self._dsn = dsn
        self._schema = schema
        self._queue_names = queue_names
        self._stop_event = stop_event
        self._listener = _activity_registry.acquire(dsn, schema=schema)
        self._closed = False
        self._pid = os.getpid()
        try:
            (
                self._fan_in_id,
                self._last_queue_versions,
                self._last_wildcard_version,
            ) = self._listener.register_queue_set(queue_names)
        except BaseException:
            # Registration failed and rolled itself back; release our reference.
            _activity_registry.release(dsn, schema=schema, listener=self._listener)
            raise

    def wait(self, timeout: float) -> bool:
        """Wait for any watched queue notification until timeout expires."""
        _raise_if_inherited(self._pid)
        try:
            previous_queue_versions = dict(self._last_queue_versions)
            previous_wildcard_version = self._last_wildcard_version
            changed, queue_versions, wildcard_version = self._listener.wait_any(
                fan_in_id=self._fan_in_id,
                stop_event=self._stop_event,
                timeout=timeout,
                last_queue_versions=self._last_queue_versions,
                last_wildcard_version=self._last_wildcard_version,
            )
            if changed:
                next_queue_versions: dict[str, int] = {}
                for queue_name in self._queue_names:
                    previous_queue_version = previous_queue_versions.get(queue_name, 0)
                    queue_version = queue_versions.get(
                        queue_name, previous_queue_version
                    )
                    if queue_version > previous_queue_version:
                        next_queue_versions[queue_name] = previous_queue_version + 1
                    else:
                        next_queue_versions[queue_name] = queue_version
                self._last_queue_versions = next_queue_versions

                if wildcard_version > previous_wildcard_version:
                    self._last_wildcard_version = previous_wildcard_version + 1
                else:
                    self._last_wildcard_version = wildcard_version
            else:
                self._last_queue_versions = queue_versions
                self._last_wildcard_version = wildcard_version
            return changed
        except psycopg.Error as exc:
            raise _translate_error(exc) from exc

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if os.getpid() != self._pid:
            # Inherited across fork(): abandon the parent's listener and
            # registry entry instead of locking them (D2).
            return
        _run_activity_waiter_cleanup(
            (
                lambda: self._listener.unregister_queue_set(self._fan_in_id),
                lambda: _activity_registry.release(
                    self._dsn,
                    schema=self._schema,
                    listener=self._listener,
                ),
            )
        )


class PostgresRunner:
    """Thread-safe synchronous Postgres runner with connection pooling."""

    def __init__(
        self,
        dsn: str,
        *,
        schema: str,
        pool_min_size: int = _DEFAULT_MIN_SIZE,
        pool_max_size: int = _DEFAULT_MAX_SIZE,
    ):
        self._dsn = dsn
        self._schema = schema
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool = self._create_pool()
        self._thread_local = threading.local()
        self._setup_lock = threading.RLock()
        self._completed_phases: set[SetupPhase] = set()
        self._lease_lock = threading.RLock()
        self._lease_entries: dict[threading.Thread, _ThreadLease] = {}
        self._meta_cache_lock = threading.Lock()
        self._meta_cache: RunnerMetaState | None = None
        self._schema_bootstrapped = False
        self._pid = os.getpid()

    def _create_pool(self) -> ConnectionPool:
        """Create a new connection pool with the configured schema search path."""

        def configure(conn: psycopg.Connection[Any]) -> None:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {quote_ident(self._schema)}, public")

        # [SB-API-14]: creation metadata never enters target/session identity.
        connection_kwargs = _owner.capture()
        pool_dsn = conninfo.make_conninfo(self._dsn)

        pool = ConnectionPool(
            pool_dsn,
            kwargs=connection_kwargs,
            min_size=self._pool_min_size,
            max_size=self._pool_max_size,
            # Return quiet processes' data connections to the server [SB-API-3].
            max_idle=_POOL_MAX_IDLE_SECONDS,
            configure=configure,
            # Don't open in background — we need connections ready immediately.
            open=False,
            timeout=30.0,
        )
        pool.open(wait=True, timeout=10.0)
        return pool

    @property
    def dsn(self) -> str:
        """Return the managed DSN for the runner."""
        return self._dsn

    @property
    def schema(self) -> str:
        """Return the managed schema for the runner."""
        return self._schema

    @property
    def _leased_conn(self) -> psycopg.Connection[Any] | None:
        """Expose the calling thread's retained checkout for diagnostics."""

        with self._lease_lock:
            entry = self._lease_entries.get(threading.current_thread())
            return None if entry is None else entry.conn

    @property
    def _lease_depth(self) -> int:
        """Expose the calling thread's nested lease count for diagnostics."""

        with self._lease_lock:
            entry = self._lease_entries.get(threading.current_thread())
            return 0 if entry is None else entry.depth

    @property
    def backend_plugin(self) -> BackendPlugin:
        from .plugin import get_backend_plugin

        return get_backend_plugin()

    def _get_thread_conn(self) -> psycopg.Connection[Any]:
        """Get or checkout a pooled connection for the current thread."""
        self._check_fork()

        owner = threading.current_thread()
        with self._lease_lock:
            entry = self._lease_entries.get(owner)
            if entry is not None and entry.conn is not None:
                return entry.conn

        if entry is not None:
            candidate = self._checkout_pool_connection()
            installed = False
            try:
                with self._lease_lock:
                    current = self._lease_entries.get(owner)
                    if current is entry and current.conn is None:
                        current.conn = candidate
                        installed = True
                        return candidate
            finally:
                if not installed:
                    with contextlib.suppress(BaseException):
                        self._pool.putconn(candidate)

        if not hasattr(self._thread_local, "conn"):
            self._thread_local.conn = self._checkout_pool_connection()
        return cast(psycopg.Connection[Any], self._thread_local.conn)

    def _checkout_pool_connection(self) -> psycopg.Connection[Any]:
        """Checkout one connection and keep pool exhaustion backend-neutral."""

        try:
            return self._pool.getconn()
        except PoolTimeout as exc:
            raise OperationalError(str(exc)) from exc

    def _check_fork(self) -> None:
        """Recreate pool-owned process state after fork."""
        current_pid = os.getpid()
        if current_pid == self._pid:
            return

        # Fork detected — the parent's pool, connections, and locks are unusable.
        _abandon_inherited_pool(self._pool)
        self._thread_local = threading.local()
        self._setup_lock = threading.RLock()
        self._lease_lock = threading.RLock()
        self._meta_cache_lock = threading.Lock()
        self._lease_entries = {}
        self._pool = self._create_pool()
        self._completed_phases.clear()
        self._meta_cache = None
        self._schema_bootstrapped = False
        self._pid = current_pid

    def lease_thread_connection(self) -> None:
        """Keep one pooled connection checked out for this runner's active leases."""

        self._check_fork()

        owner = threading.current_thread()
        with self._lease_lock:
            entry = self._lease_entries.get(owner)
            if entry is not None:
                entry.depth += 1
                return

        conn = getattr(self._thread_local, "conn", None)
        borrowed_from_pool = conn is None
        if not borrowed_from_pool:
            delattr(self._thread_local, "conn")
            self._clear_transaction_state()
        else:
            conn = self._checkout_pool_connection()
        leased_conn = cast(psycopg.Connection[Any], conn)

        installed = False
        try:
            with self._lease_lock:
                self._lease_entries[owner] = _ThreadLease(conn=leased_conn, depth=1)
                installed = True
        finally:
            if not installed:
                if borrowed_from_pool:
                    with contextlib.suppress(BaseException):
                        self._pool.putconn(leased_conn)
                else:
                    self._thread_local.conn = leased_conn

    def lease_session_core_connection(self) -> bool:
        """Use operation-scoped pool checkouts for cached session cores."""

        return False

    def _has_leased_connection(self) -> bool:
        with self._lease_lock:
            return threading.current_thread() in self._lease_entries

    def _thread_connection_leased(self) -> bool:
        return self._has_leased_connection()

    def _uses_leased_connection(self, conn: psycopg.Connection[Any]) -> bool:
        with self._lease_lock:
            entry = self._lease_entries.get(threading.current_thread())
            return entry is not None and conn is entry.conn

    def _return_thread_conn(self) -> None:
        """Return the current thread's connection to the pool."""
        conn = getattr(self._thread_local, "conn", None)
        if conn is not None:
            delattr(self._thread_local, "conn")
            if hasattr(self._thread_local, "in_transaction"):
                delattr(self._thread_local, "in_transaction")
            if hasattr(self._thread_local, "transaction_uses_leased_conn"):
                delattr(self._thread_local, "transaction_uses_leased_conn")
            # Pool may already be closed during teardown
            with contextlib.suppress(Exception):
                self._pool.putconn(conn)

    def _return_thread_conn_after_operation(self) -> None:
        """Return non-leased operation checkouts to the pool."""

        if not self._thread_connection_leased():
            self._return_thread_conn()

    def _discard_thread_connection(self) -> None:
        """Close an uncertain checkout without releasing its logical lease."""

        self._check_fork()
        conn_to_discard: psycopg.Connection[Any] | None = None
        with self._lease_lock:
            entry = self._lease_entries.get(threading.current_thread())
            if entry is not None:
                conn_to_discard = entry.conn
                entry.conn = None
        if entry is None:
            conn_to_discard = getattr(self._thread_local, "conn", None)
            if conn_to_discard is not None:
                delattr(self._thread_local, "conn")
        self._clear_transaction_state()

        if conn_to_discard is None:
            return
        try:
            conn_to_discard.close()
        finally:
            # Even if close() fails, the pool must get its slot back so a
            # replacement checkout can be created; putconn discards a broken
            # or closed connection rather than serving it again.
            self._pool.putconn(conn_to_discard)

    def _discard_thread_connection_after_failure(
        self,
        failure: BaseException,
    ) -> None:
        """Detach a failed leased checkout without hiding its primary failure."""

        capture_ordinary_pg_cleanup(
            primary=failure,
            phase="connection discard",
            action=self._discard_thread_connection,
        )

    def _clear_transaction_state(self) -> None:
        if hasattr(self._thread_local, "in_transaction"):
            delattr(self._thread_local, "in_transaction")
        if hasattr(self._thread_local, "transaction_uses_leased_conn"):
            delattr(self._thread_local, "transaction_uses_leased_conn")

    def _finish_transaction(self, *, leased: bool) -> None:
        self._clear_transaction_state()
        if not leased:
            self._return_thread_conn()

    def _in_transaction(self) -> bool:
        return bool(getattr(self._thread_local, "in_transaction", False))

    def _transaction_uses_leased_connection(self) -> bool:
        return bool(getattr(self._thread_local, "transaction_uses_leased_conn", False))

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        self._check_fork()
        return self._run(sql, params, fetch=fetch)

    @contextlib.contextmanager
    def _operation_scope(self) -> Iterator[None]:
        """Own this thread's checkout from acquisition through its cleanup.

        A ``release_thread_connection()`` or ``shutdown()`` that runs on this
        thread while an operation is inside its scope (a finalizer, a callback,
        a core closed re-entrantly) is deferred to the outermost scope's exit,
        after the operation's own cleanup: before that the operation may be
        between checkout and ``execute()``, inside a statement, or between the
        statements of its transaction. Plan:
        docs/plans/2026-11-10-backend-thread-ownership-plan.md (D3, D4).
        """
        depth = int(getattr(self._thread_local, "operation_depth", 0))
        self._thread_local.operation_depth = depth + 1
        try:
            yield
        finally:
            if depth:
                self._thread_local.operation_depth = depth
            else:
                del self._thread_local.operation_depth
                self._run_deferred_thread_cleanup()

    def _in_operation_scope(self) -> bool:
        return bool(getattr(self._thread_local, "operation_depth", 0))

    def _run_deferred_thread_cleanup(self) -> None:
        releases = int(getattr(self._thread_local, "pending_releases", 0))
        shutdown = bool(getattr(self._thread_local, "pending_shutdown", False))
        for name in ("pending_releases", "pending_shutdown"):
            if hasattr(self._thread_local, name):
                delattr(self._thread_local, name)
        for _ in range(releases):
            self.release_thread_connection()
        if shutdown:
            self._return_calling_thread_checkouts()

    def _run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        # Fork recovery replaces thread-local state, so it precedes the scope.
        self._check_fork()
        with self._operation_scope():
            return self._run_in_scope(sql, params, fetch=fetch)

    def _run_in_scope(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        conn = self._get_thread_conn()
        leased = self._uses_leased_connection(conn)
        failure: Exception | None = None
        cause: psycopg.Error | None = None
        invalidate_bootstrap = False
        try:
            with conn.cursor() as cur:
                adapted_sql = _adapt_sql(sql) if params else sql
                cur.execute(adapted_sql, params, prepare=_should_prepare(adapted_sql))
                if fetch:
                    return cur.fetchall()
                return []
        except psycopg.Error as exc:
            failure = _translate_error(exc)
            cause = exc
            invalidate_bootstrap = _invalidates_bootstrap(exc)
        finally:
            if not leased and not self._in_transaction():
                self._return_thread_conn_after_operation()

        if invalidate_bootstrap:
            self.invalidate_bootstrap_state()
        assert failure is not None
        raise failure from cause

    def run_transaction(
        self,
        statements: Sequence[tuple[str, tuple[Any, ...]]],
    ) -> list[list[tuple[Any, ...]]]:
        """Run statements as one transaction without a client round trip inside it.

        With libpq pipeline mode every statement and one closing Sync are sent
        without waiting for a result; the server executes them as one implicit
        transaction and commits at the Sync, so locks taken by an earlier
        statement are never held while the server waits for this client to
        read a result. Without pipeline support the same statements run inside
        an explicit BEGIN/COMMIT (same atomicity, more round trips). Any error
        rolls the whole transaction back. Returns each statement's rows, or an
        empty list for a statement that returns none.

        Callers own statement order: a statement's relation locks are taken
        when the server reaches it, so order expresses lock order. A raised
        error reports, through :func:`failed_statement`, which statement the
        failure reached, so a caller can tell a lock-acquisition failure from
        a later one.

        Must not be called while this runner has an open transaction.
        """
        # Fork recovery replaces thread-local state, so it precedes the scope.
        self._check_fork()
        with self._operation_scope():
            return self._run_transaction_in_scope(statements)

    def _run_transaction_in_scope(
        self,
        statements: Sequence[tuple[str, tuple[Any, ...]]],
    ) -> list[list[tuple[Any, ...]]]:
        """Body of :meth:`run_transaction`, inside its operation scope."""
        self._check_fork()
        if self._in_transaction():
            raise RuntimeError("run_transaction cannot join an open runner transaction")
        conn = self._get_thread_conn()
        leased = self._uses_leased_connection(conn)
        failure: Exception | None = None
        cause: psycopg.Error | None = None
        invalidate_bootstrap = False
        try:
            if _pipeline_supported():
                return self._run_pipelined_transaction(conn, statements)
            return self._run_explicit_transaction(conn, statements)
        except _TransactionFailure as exc:
            failure = _translate_error(exc.cause)
            setattr(failure, _FAILED_STATEMENT_ATTRIBUTE, exc.statement)
            cause = exc.cause
            invalidate_bootstrap = _invalidates_bootstrap(exc.cause)
        except BaseException as exc:
            if leased and not _connection_idle(conn):
                self._discard_thread_connection_after_failure(exc)
            raise
        finally:
            if not leased:
                self._return_thread_conn_after_operation()

        assert failure is not None
        if leased and not _connection_idle(conn):
            self._discard_thread_connection_after_failure(failure)
        if invalidate_bootstrap:
            self.invalidate_bootstrap_state()
        raise failure from cause

    @staticmethod
    def _run_pipelined_transaction(
        conn: psycopg.Connection[Any],
        statements: Sequence[tuple[str, tuple[Any, ...]]],
    ) -> list[list[tuple[Any, ...]]]:
        cursors: list[psycopg.Cursor[Any]] = []
        try:
            # Autocommit connection, no BEGIN: everything up to the Sync sent
            # when the block exits is one implicit transaction.
            with conn.pipeline():
                for sql, params in statements:
                    cursor = conn.cursor()
                    cursors.append(cursor)
                    adapted_sql = _adapt_sql(sql) if params else sql
                    cursor.execute(
                        adapted_sql, params, prepare=_should_prepare(adapted_sql)
                    )
            return [
                cursor.fetchall() if cursor.description is not None else []
                for cursor in cursors
            ]
        except psycopg.Error as exc:
            # A statement that failed, was aborted with the pipeline, or was
            # never sent has no result; when every statement returned one, the
            # failure came at the Sync (the commit).
            missing = (
                index for index, cursor in enumerate(cursors) if cursor.pgresult is None
            )
            reached = next(
                missing, len(cursors) if len(cursors) < len(statements) else None
            )
            raise _TransactionFailure(exc, reached) from exc
        finally:
            for cursor in cursors:
                cursor.close()

    @staticmethod
    def _run_explicit_transaction(
        conn: psycopg.Connection[Any],
        statements: Sequence[tuple[str, tuple[Any, ...]]],
    ) -> list[list[tuple[Any, ...]]]:
        results: list[list[tuple[Any, ...]]] = []
        reached: int | None = None
        try:
            with conn.cursor() as cursor:
                cursor.execute("BEGIN")
                for index, (sql, params) in enumerate(statements):
                    reached = index
                    adapted_sql = _adapt_sql(sql) if params else sql
                    cursor.execute(
                        adapted_sql, params, prepare=_should_prepare(adapted_sql)
                    )
                    results.append(
                        cursor.fetchall() if cursor.description is not None else []
                    )
                reached = None
                cursor.execute("COMMIT")
        except BaseException as exc:
            if not _connection_idle(conn):
                with contextlib.suppress(psycopg.Error):
                    conn.rollback()
            if isinstance(exc, psycopg.Error):
                raise _TransactionFailure(exc, reached) from exc
            raise
        return results

    def _checkout_begin_connection(
        self,
    ) -> tuple[psycopg.Connection[Any], bool]:
        """Checkout the current thread's connection for BEGIN."""

        conn = self._get_thread_conn()
        return conn, self._uses_leased_connection(conn)

    def begin_immediate(self) -> None:
        self._check_fork()
        with self._operation_scope():
            self._begin_immediate_in_scope()

    def _begin_immediate_in_scope(self) -> None:
        conn, leased = self._checkout_begin_connection()

        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
            self._thread_local.in_transaction = True
            self._thread_local.transaction_uses_leased_conn = leased
        except psycopg.Error as exc:
            failure = _translate_error(exc)
            if leased:
                self._discard_thread_connection_after_failure(failure)
            else:
                self._return_thread_conn()
            raise failure from exc
        except BaseException as failure:
            if leased:
                self._discard_thread_connection_after_failure(failure)
            else:
                self._return_thread_conn()
            raise

    def commit(self) -> None:
        self._check_fork()
        with self._operation_scope():
            self._commit_in_scope()

    def _commit_in_scope(self) -> None:
        failed = False
        leased = self._transaction_uses_leased_connection()
        conn = self._get_thread_conn()
        try:
            conn.commit()
        except psycopg.Error as exc:
            failed = True
            failure = _translate_error(exc)
            if leased:
                self._discard_thread_connection_after_failure(failure)
            else:
                self._return_thread_conn()
            raise failure from exc
        except BaseException as failure:
            failed = True
            if leased:
                self._discard_thread_connection_after_failure(failure)
            else:
                self._return_thread_conn()
            raise
        finally:
            if not failed:
                self._finish_transaction(leased=leased)

    def rollback(self) -> None:
        self._check_fork()
        with self._operation_scope():
            self._rollback_in_scope()

    def _rollback_in_scope(self) -> None:
        failed = False
        leased = self._transaction_uses_leased_connection()
        conn = self._get_thread_conn()
        try:
            conn.rollback()
            self.invalidate_meta_cache()
        except psycopg.Error as exc:
            failed = True
            failure = _translate_error(exc)
            if leased:
                self._discard_thread_connection_after_failure(failure)
            else:
                self._return_thread_conn()
            raise failure from exc
        except BaseException as failure:
            failed = True
            if leased:
                self._discard_thread_connection_after_failure(failure)
            else:
                self._return_thread_conn()
            raise
        finally:
            if not failed:
                self._finish_transaction(leased=leased)

    def release_thread_connection(self) -> None:
        """Release this thread's lease, returning its checkout on last close.

        Inside an operation scope on this thread the release is counted and
        performed after that operation's own cleanup (D3).
        """

        self._check_fork()
        if self._in_operation_scope():
            self._thread_local.pending_releases = (
                int(getattr(self._thread_local, "pending_releases", 0)) + 1
            )
            return
        owner = threading.current_thread()
        with self._lease_lock:
            entry = self._lease_entries.get(owner)
            if entry is None:
                conn_to_return = None
            elif entry.depth > 1:
                entry.depth -= 1
                return
            else:
                conn_to_return = entry.conn
                del self._lease_entries[owner]

        if entry is None:
            if self._in_transaction():
                return
            self._return_thread_conn()
            return

        self._clear_transaction_state()
        # Pool may already be closed during teardown
        if conn_to_return is not None:
            with contextlib.suppress(Exception):
                self._pool.putconn(conn_to_return)

    def close(self) -> None:
        """Permanently close the connection pool and all connections."""
        self.shutdown()

    def shutdown(self) -> None:
        """Permanently close the connection pool and all connections.

        A live thread's leased checkout stays with that thread: its own
        ``release_thread_connection()`` returns it into the closed pool, which
        closes it there. Leases of threads that have exited are returned here.
        The calling thread's own checkouts are returned now, or after its
        current operation when shutdown runs inside one (D4).
        """
        self._check_fork()
        current = threading.current_thread()
        with self._lease_lock:
            exited = [
                owner
                for owner in self._lease_entries
                if owner is not current and not owner.is_alive()
            ]
            orphaned = [self._lease_entries.pop(owner).conn for owner in exited]
        for leased_conn in orphaned:
            if leased_conn is not None:
                with contextlib.suppress(Exception):
                    self._pool.putconn(leased_conn)
        if self._in_operation_scope():
            self._thread_local.pending_shutdown = True
        else:
            self._return_calling_thread_checkouts()
        with contextlib.suppress(Exception):
            self._pool.close()

    def _return_calling_thread_checkouts(self) -> None:
        """Surrender this thread's lease and checkout to the pool."""

        with self._lease_lock:
            entry = self._lease_entries.pop(threading.current_thread(), None)
        if entry is not None and entry.conn is not None:
            with contextlib.suppress(Exception):
                self._pool.putconn(entry.conn)
        self._clear_transaction_state()
        self._return_thread_conn()

    def __del__(self) -> None:
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            pool = getattr(self, "_pool", None)
            if pool is not None:
                _abandon_inherited_pool(pool)
            return
        with contextlib.suppress(Exception):
            self._pool.close()

    def setup(self, phase: SetupPhase) -> None:
        self._check_fork()
        with self._setup_lock:
            self._completed_phases.add(phase)

    def run_exclusive_setup(
        self,
        phase: SetupPhase,
        operation: Callable[[], None],
    ) -> bool:
        """Run a setup operation once for this runner instance."""
        self._check_fork()
        with self._setup_lock:
            if phase in self._completed_phases:
                return False
            operation()
            self._completed_phases.add(phase)
            return True

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        self._check_fork()
        return phase in self._completed_phases

    def is_schema_bootstrapped(self) -> bool:
        """Return whether schema DDL has already completed for this runner."""
        self._check_fork()
        with self._meta_cache_lock:
            return self._schema_bootstrapped

    def mark_schema_bootstrapped(self) -> None:
        """Mark schema bootstrap as complete without mutating cached meta."""
        self._check_fork()
        with self._meta_cache_lock:
            self._schema_bootstrapped = True

    def get_meta_cache(self) -> RunnerMetaState | None:
        """Return cached meta state, if available."""
        self._check_fork()
        with self._meta_cache_lock:
            return self._meta_cache

    def prime_meta_cache(self, state: RunnerMetaState) -> None:
        """Store fresh metadata discovered from the database."""
        self._check_fork()
        with self._meta_cache_lock:
            self._meta_cache = state
            self._schema_bootstrapped = True

    def invalidate_meta_cache(self) -> None:
        """Discard cached metadata while keeping bootstrap state."""
        self._check_fork()
        with self._meta_cache_lock:
            self._meta_cache = None

    def invalidate_bootstrap_state(self) -> None:
        """Discard all cached bootstrap state after missing-object errors."""
        self._check_fork()
        with self._setup_lock:
            self._completed_phases.discard(SetupPhase.SCHEMA)
        with self._meta_cache_lock:
            self._meta_cache = None
            self._schema_bootstrapped = False

    def update_meta_cache(
        self,
        *,
        magic: str | None = None,
        schema_version: int | None = None,
        last_ts: int | None = None,
        alias_version: int | None = None,
    ) -> None:
        """Update cached metadata after successful writes."""
        self._check_fork()
        with self._meta_cache_lock:
            if self._meta_cache is None:
                return
            self._meta_cache = replace(
                self._meta_cache,
                magic=self._meta_cache.magic if magic is None else magic,
                schema_version=(
                    self._meta_cache.schema_version
                    if schema_version is None
                    else schema_version
                ),
                last_ts=self._meta_cache.last_ts if last_ts is None else last_ts,
                alias_version=(
                    self._meta_cache.alias_version
                    if alias_version is None
                    else alias_version
                ),
            )


__all__ = ["PostgresActivityWaiter", "PostgresRunner", "RunnerMetaState"]
