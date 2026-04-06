"""Postgres SQLRunner implementation for SimpleBroker."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterable
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
        self._queue_name = queue_name
        self._stop_event = stop_event
        self._conn = psycopg.connect(dsn, autocommit=True)
        self._channel = activity_channel_name(schema)
        with self._conn.cursor() as cur:
            cur.execute(f"LISTEN {quote_ident(self._channel)}")

    def wait(self, timeout: float) -> bool:
        """Wait for a relevant queue notification until timeout expires."""
        deadline = time.monotonic() + max(timeout, 0.0)

        while not self._stop_event.is_set():
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                return False
            try:
                received = False
                for notify in self._conn.notifies(timeout=remaining, stop_after=1):
                    received = True
                    if notify.payload in {self._queue_name, "*"}:
                        return True
                if not received:
                    return False
            except psycopg.Error as exc:
                raise _translate_error(exc) from exc
        return False

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


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
        self._setup_lock = threading.Lock()
        self._completed_phases: set[SetupPhase] = set()
        self._meta_cache_lock = threading.Lock()
        self._meta_cache: RunnerMetaState | None = None
        self._schema_bootstrapped = False
        self._pid = os.getpid()

    def _create_pool(self) -> ConnectionPool:
        """Create a new connection pool with the configured schema search path."""

        def configure(conn: psycopg.Connection[Any]) -> None:
            conn.autocommit = True

        pool_dsn = conninfo.make_conninfo(
            self._dsn,
            options=f"-csearch_path={self._schema},public",
        )

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

    def _return_thread_conn(self) -> None:
        """Return the current thread's connection to the pool."""
        conn = getattr(self._thread_local, "conn", None)
        if conn is not None:
            delattr(self._thread_local, "conn")
            try:
                self._pool.putconn(conn)
            except Exception:
                pass  # Pool may already be closed during teardown

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

    def begin_immediate(self) -> None:
        conn = self._get_thread_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
        except psycopg.Error as exc:
            raise _translate_error(exc) from exc

    def commit(self) -> None:
        try:
            self._get_thread_conn().commit()
        except psycopg.Error as exc:
            raise _translate_error(exc) from exc

    def rollback(self) -> None:
        try:
            self._get_thread_conn().rollback()
            self.invalidate_meta_cache()
        except psycopg.Error as exc:
            raise _translate_error(exc) from exc

    def close(self) -> None:
        """Return the current thread's connection to the pool.

        This does NOT shut down the pool — the runner remains usable by
        other threads or by later calls on the same thread.  Call
        :meth:`shutdown` to permanently close the pool and all its
        connections.
        """
        self._return_thread_conn()

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
