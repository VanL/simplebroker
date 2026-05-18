"""Postgres SQLRunner implementation for SimpleBroker."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from typing import Any, cast

import psycopg
from psycopg import conninfo
from psycopg_pool import ConnectionPool

from simplebroker._backend_plugins import BackendPlugin
from simplebroker._exceptions import DataError, IntegrityError, OperationalError
from simplebroker._runner import SetupPhase

from ._identifiers import activity_channel_name
from .validation import quote_ident

# Default pool bounds — enough for typical watcher workloads, low enough
# to stay well under Postgres's default max_connections=100.
_DEFAULT_MIN_SIZE = 0
_DEFAULT_MAX_SIZE = 16


@dataclass(frozen=True, slots=True)
class RunnerMetaState:
    """Cached broker metadata for one schema-bound runner."""

    magic: str
    schema_version: int
    last_ts: int
    alias_version: int


@dataclass(slots=True)
class _FanInWaiterEntry:
    """Listener-local fan-in waiter registration."""

    condition: threading.Condition
    queue_names: tuple[str, ...]
    queue_set: set[str]


def _adapt_sql(sql: str) -> str:
    """Adapt SimpleBroker's qmark placeholders for psycopg."""
    return sql.replace("?", "%s")


def _should_prepare(sql: str) -> bool:
    """Return whether this statement is hot enough to force preparation."""
    normalized = " ".join(sql.split())

    if normalized.startswith("UPDATE meta SET last_ts = %s"):
        return True
    if normalized.startswith("WITH inserted AS ( INSERT INTO messages"):
        return True
    if (
        normalized.startswith("WITH selected AS MATERIALIZED ( SELECT order_id")
        and "UPDATE messages AS m SET claimed = TRUE" in normalized
    ):
        return True
    if (
        normalized.startswith("WITH target_queue AS ( SELECT %s AS queue_name )")
        and "UPDATE messages AS m SET queue = (SELECT queue_name FROM target_queue)"
        in normalized
    ):
        return True
    return False


def _translate_error(exc: psycopg.Error) -> Exception:
    """Translate psycopg errors into SimpleBroker exceptions."""
    sqlstate = getattr(exc, "sqlstate", None) or ""
    if sqlstate.startswith("23"):
        return IntegrityError(str(exc))
    if sqlstate.startswith("22"):
        return DataError(str(exc))
    return OperationalError(str(exc))


def _invalidates_bootstrap(exc: psycopg.Error) -> bool:
    """Return whether an error means cached schema state is no longer safe."""
    sqlstate = getattr(exc, "sqlstate", None) or ""
    return sqlstate in {"3F000", "42P01"}


class _SharedActivityListener:
    """Process-local LISTEN worker shared by pg watchers for one schema."""

    def __init__(
        self,
        dsn: str,
        *,
        schema: str,
        startup_timeout: float = 5.0,
    ) -> None:
        self._dsn = dsn
        self._channel = activity_channel_name(schema)
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._conditions: dict[str, threading.Condition] = {}
        self._versions: dict[str, int] = {}
        self._queue_refcounts: dict[str, int] = {}
        self._fan_in_entries: dict[int, _FanInWaiterEntry] = {}
        self._next_fan_in_id = 0
        self._wildcard_version = 0
        self._ready = threading.Event()
        self._conn: psycopg.Connection[Any] | None = None
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"simplebroker-pg-listener-{self._channel}",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=startup_timeout):
            self.close()
            raise OperationalError("Postgres activity listener did not start")
        if self._error is not None:
            self.close()
            if isinstance(self._error, psycopg.Error):
                raise _translate_error(self._error) from self._error
            raise OperationalError(str(self._error)) from self._error

    def _run(self) -> None:
        try:
            conn = psycopg.connect(self._dsn, autocommit=True)
            self._conn = conn
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {quote_ident(self._channel)}")
            self._ready.set()

            while not self._stop_event.is_set():
                for notify in conn.notifies(timeout=0.1, stop_after=1):
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
                    break
        except Exception as exc:
            if not self._stop_event.is_set():
                self._error = exc
                self._ready.set()
                with self._lock:
                    for condition in self._conditions.values():
                        condition.notify_all()
                    for entry in self._fan_in_entries.values():
                        entry.condition.notify_all()
        finally:
            self._ready.set()
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass

    def register_queue(self, queue_name: str) -> tuple[int, int]:
        """Register a waiter queue and return its current notification versions."""
        with self._lock:
            refcount = self._queue_refcounts.get(queue_name, 0)
            self._queue_refcounts[queue_name] = refcount + 1
            if refcount == 0:
                self._conditions[queue_name] = threading.Condition(self._lock)
                self._versions[queue_name] = 0
            return self._versions[queue_name], self._wildcard_version

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
            del self._queue_refcounts[queue_name]
            if condition is not None:
                condition.notify_all()

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
            for queue_name in queue_names:
                queue_version, _ = self.register_queue(queue_name)
                versions[queue_name] = queue_version
            return fan_in_id, versions, self._wildcard_version

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
        self._stop_event.set()
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        with self._lock:
            for condition in self._conditions.values():
                condition.notify_all()
            for entry in self._fan_in_entries.values():
                entry.condition.notify_all()
        self._thread.join(timeout=1.0)


class _SharedActivityRegistry:
    """Reference-counted process-local listener registry."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._listeners: dict[
            tuple[int, str, str], tuple[_SharedActivityListener, int]
        ] = {}

    def acquire(self, dsn: str, *, schema: str) -> _SharedActivityListener:
        key = (os.getpid(), dsn, schema)
        with self._lock:
            entry = self._listeners.get(key)
            if entry is not None:
                listener, refcount = entry
                self._listeners[key] = (listener, refcount + 1)
                return listener

            listener = _SharedActivityListener(dsn, schema=schema)
            self._listeners[key] = (listener, 1)
            return listener

    def release(self, dsn: str, *, schema: str) -> None:
        key = (os.getpid(), dsn, schema)
        listener: _SharedActivityListener | None = None
        with self._lock:
            entry = self._listeners.get(key)
            if entry is None:
                return
            current_listener, refcount = entry
            if refcount > 1:
                self._listeners[key] = (current_listener, refcount - 1)
                return
            listener = current_listener
            del self._listeners[key]
        listener.close()


_activity_registry = _SharedActivityRegistry()


class PostgresActivityWaiter:
    """LISTEN/NOTIFY-backed wake-up waiter for pg watchers."""

    def __init__(
        self,
        dsn: str,
        *,
        schema: str,
        queue_name: str,
        stop_event: threading.Event,
    ) -> None:
        self._dsn = dsn
        self._schema = schema
        self._queue_name = queue_name
        self._stop_event = stop_event
        self._listener = _activity_registry.acquire(dsn, schema=schema)
        self._closed = False
        self._last_queue_version, self._last_wildcard_version = (
            self._listener.register_queue(queue_name)
        )

    def wait(self, timeout: float) -> bool:
        """Wait for a relevant queue notification until timeout expires."""
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
        self._listener.unregister_queue(self._queue_name)
        _activity_registry.release(self._dsn, schema=self._schema)


class PostgresMultiQueueActivityWaiter:
    """LISTEN/NOTIFY-backed fan-in waiter for multiple pg queues."""

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
        self._dsn = dsn
        self._schema = schema
        self._queue_names = queue_names
        self._stop_event = stop_event
        self._listener = _activity_registry.acquire(dsn, schema=schema)
        self._closed = False
        (
            self._fan_in_id,
            self._last_queue_versions,
            self._last_wildcard_version,
        ) = self._listener.register_queue_set(queue_names)

    def wait(self, timeout: float) -> bool:
        """Wait for any watched queue notification until timeout expires."""
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
        self._listener.unregister_queue_set(self._fan_in_id)
        _activity_registry.release(self._dsn, schema=self._schema)


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

        pool_dsn = conninfo.make_conninfo(self._dsn)

        pool = ConnectionPool(
            pool_dsn,
            min_size=self._pool_min_size,
            max_size=self._pool_max_size,
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
    def backend_plugin(self) -> BackendPlugin:
        from .plugin import get_backend_plugin

        return get_backend_plugin()

    def _get_thread_conn(self) -> psycopg.Connection[Any]:
        """Get or checkout a pooled connection for the current thread."""
        current_pid = os.getpid()
        if current_pid != self._pid:
            # Fork detected — the parent's pool and connections are unusable.
            self._thread_local = threading.local()
            self._pool = self._create_pool()
            with self._setup_lock:
                self._completed_phases.clear()
            with self._meta_cache_lock:
                self._meta_cache = None
                self._schema_bootstrapped = False
            self._pid = current_pid

        if not hasattr(self._thread_local, "conn"):
            self._thread_local.conn = self._pool.getconn()
        return cast(psycopg.Connection[Any], self._thread_local.conn)

    def lease_thread_connection(self) -> None:
        """Keep this thread's pooled connection checked out until release."""

        lease_depth = int(getattr(self._thread_local, "lease_depth", 0))
        self._thread_local.lease_depth = lease_depth + 1
        self._get_thread_conn()

    def _thread_connection_leased(self) -> bool:
        return int(getattr(self._thread_local, "lease_depth", 0)) > 0

    def _return_thread_conn(self) -> None:
        """Return the current thread's connection to the pool."""
        conn = getattr(self._thread_local, "conn", None)
        if conn is not None:
            delattr(self._thread_local, "conn")
            if hasattr(self._thread_local, "in_transaction"):
                delattr(self._thread_local, "in_transaction")
            try:
                self._pool.putconn(conn)
            except Exception:
                pass  # Pool may already be closed during teardown

    def _return_thread_conn_after_operation(self) -> None:
        """Return non-leased operation checkouts to the pool."""

        if not self._thread_connection_leased():
            self._return_thread_conn()

    def _finish_transaction(self) -> None:
        if self._thread_connection_leased():
            if hasattr(self._thread_local, "in_transaction"):
                delattr(self._thread_local, "in_transaction")
            return
        self._return_thread_conn()

    def _in_transaction(self) -> bool:
        return bool(getattr(self._thread_local, "in_transaction", False))

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        conn = self._get_thread_conn()
        try:
            with conn.cursor() as cur:
                adapted_sql = _adapt_sql(sql)
                cur.execute(adapted_sql, params, prepare=_should_prepare(adapted_sql))
                if fetch:
                    return cur.fetchall()
                return []
        except psycopg.Error as exc:
            if _invalidates_bootstrap(exc):
                self.invalidate_bootstrap_state()
            raise _translate_error(exc) from exc
        finally:
            if not self._in_transaction():
                self._return_thread_conn_after_operation()

    def begin_immediate(self) -> None:
        conn = self._get_thread_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
            self._thread_local.in_transaction = True
        except psycopg.Error as exc:
            self._return_thread_conn()
            raise _translate_error(exc) from exc

    def commit(self) -> None:
        failed = False
        try:
            self._get_thread_conn().commit()
        except psycopg.Error as exc:
            failed = True
            self._return_thread_conn()
            raise _translate_error(exc) from exc
        finally:
            if not failed:
                self._finish_transaction()

    def rollback(self) -> None:
        failed = False
        try:
            self._get_thread_conn().rollback()
            self.invalidate_meta_cache()
        except psycopg.Error as exc:
            failed = True
            self._return_thread_conn()
            raise _translate_error(exc) from exc
        finally:
            if not failed:
                self._finish_transaction()

    def release_thread_connection(self) -> None:
        """Return the current thread's connection to the pool."""
        lease_depth = int(getattr(self._thread_local, "lease_depth", 0))
        if lease_depth > 1:
            self._thread_local.lease_depth = lease_depth - 1
            return
        if lease_depth == 1:
            delattr(self._thread_local, "lease_depth")
        self._return_thread_conn()

    def close(self) -> None:
        """Permanently close the connection pool and all connections."""
        self.shutdown()

    def shutdown(self) -> None:
        """Permanently close the connection pool and all connections."""
        self._return_thread_conn()
        try:
            self._pool.close()
        except Exception:
            pass

    def __del__(self) -> None:
        try:
            self._pool.close()
        except Exception:
            pass

    def setup(self, phase: SetupPhase) -> None:
        with self._setup_lock:
            self._completed_phases.add(phase)

    def run_exclusive_setup(
        self,
        phase: SetupPhase,
        operation: Callable[[], None],
    ) -> bool:
        """Run a setup operation once for this runner instance."""
        with self._setup_lock:
            if phase in self._completed_phases:
                return False
            operation()
            self._completed_phases.add(phase)
            return True

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return phase in self._completed_phases

    def is_schema_bootstrapped(self) -> bool:
        """Return whether schema DDL has already completed for this runner."""
        with self._meta_cache_lock:
            return self._schema_bootstrapped

    def mark_schema_bootstrapped(self) -> None:
        """Mark schema bootstrap as complete without mutating cached meta."""
        with self._meta_cache_lock:
            self._schema_bootstrapped = True

    def get_meta_cache(self) -> RunnerMetaState | None:
        """Return cached meta state, if available."""
        with self._meta_cache_lock:
            return self._meta_cache

    def prime_meta_cache(self, state: RunnerMetaState) -> None:
        """Store fresh metadata discovered from the database."""
        with self._meta_cache_lock:
            self._meta_cache = state
            self._schema_bootstrapped = True

    def invalidate_meta_cache(self) -> None:
        """Discard cached metadata while keeping bootstrap state."""
        with self._meta_cache_lock:
            self._meta_cache = None

    def invalidate_bootstrap_state(self) -> None:
        """Discard all cached bootstrap state after missing-object errors."""
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
