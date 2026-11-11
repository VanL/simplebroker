"""Valkey/Redis resource handle for SimpleBroker."""

from __future__ import annotations

import contextlib
import os
import threading
import time
from collections.abc import Iterable
from queue import Empty, LifoQueue
from typing import Any

import redis

from simplebroker import Config, resolve_config

from ._constants import DEFAULT_STALE_BATCH_SECONDS, RUNNER_CLOSE_IN_FLIGHT_WAIT_SECONDS
from .pool import RedisPoolOptions, pool_options_from_config
from .validation import require_namespace

# Forked children must not finalize inherited redis-py resources: cleanup can
# enter locks held forever by a vanished parent thread.
_ABANDONED_FORK_REDIS_RESOURCES: list[Any] = []


class _PoolSlots(LifoQueue[Any]):
    """Make final retirement atomic with slot publication."""

    def __init__(self, maxsize: int) -> None:
        super().__init__(maxsize)
        self._retirement_lock = threading.Lock()
        self._retired = False

    def _get(self) -> Any:
        # The permanent placeholder wakes all get() callers, including those
        # already blocked inside redis-py. Never let one consume it.
        if self._retired:
            raise redis.ConnectionError("Redis runner is closed")
        return super()._get()

    def put_nowait(self, item: Any) -> None:
        with self._retirement_lock:
            if not self._retired:
                super().put_nowait(item)
                return
        if item is not None:
            item.disconnect()

    def retire(self) -> list[Any]:
        with self._retirement_lock, self.not_empty:
            slots = list(self.queue)
            self.queue.clear()
            self._retired = True
            self._put(None)
            self.not_empty.notify_all()
            return slots


class _ClosingConnectionPool(redis.BlockingConnectionPool):
    """Blocking pool whose close waits for checked-out connections.

    Before retirement the pool has ``max_connections`` slots, each a connection
    or a placeholder; a checkout takes one and its release returns it. Closing
    acquires every slot (bounded), so each connection it disconnects is its own
    and none is touched while a thread still holds it. A slot still out at the
    bound comes back through its owner's release, which disconnects the
    connection on the owner's thread. Plan:
    2026-11-10-backend-thread-ownership-plan.md at 4cea0cb (D5, owner decision
    Q4 option b).
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs["queue_class"] = _PoolSlots
        super().__init__(**kwargs)
        self._closing = False

    def get_connection(self, *args: Any, **kwargs: Any) -> Any:
        if self._closing:
            raise redis.ConnectionError("Redis runner is closed")
        connection = super().get_connection(*args, **kwargs)
        if self._closing:
            # Acquired after close began: hand the slot back instead of using it.
            self.release(connection)
            raise redis.ConnectionError("Redis runner is closed")
        return connection

    def close_when_idle(self, timeout: float) -> None:
        self._closing = True
        deadline = time.monotonic() + timeout
        slots: list[Any] = []
        while len(slots) < self.max_connections:
            remaining = deadline - time.monotonic()
            try:
                if remaining > 0:
                    slots.append(self.pool.get(timeout=remaining))
                else:
                    slots.append(self.pool.get_nowait())
            except Empty:
                break
        slots.extend(self.pool.retire())
        for slot in slots:
            if slot is not None:
                slot.disconnect()


class RedisRunner:
    """Resource handle for one Valkey/Redis target and namespace."""

    def __init__(
        self,
        target: str,
        *,
        namespace: str | None = None,
        backend_options: dict[str, Any] | None = None,
        config: Config | None = None,
        pool_options: RedisPoolOptions | None = None,
        stale_batch_seconds: int = DEFAULT_STALE_BATCH_SECONDS,
    ) -> None:
        options = dict(backend_options or {})
        if namespace is not None:
            options["namespace"] = namespace
        self.target = target
        self.namespace = require_namespace(options)
        if pool_options is None:
            resolved_config = resolve_config(config=config)
            pool_options = pool_options_from_config(resolved_config, options)
        self.pool_options = pool_options
        self.stale_batch_seconds = stale_batch_seconds
        self._pid = os.getpid()
        self._pool: _ClosingConnectionPool | None = None
        self._client: redis.Redis | None = None
        self._client_lock = threading.Lock()
        self._init_lock = threading.Lock()
        self._target_initialized = False
        self._recovery_lock = threading.Lock()
        self._last_recovery_check_ns = 0

    @property
    def backend_plugin(self) -> Any:
        from .plugin import get_backend_plugin

        return get_backend_plugin()

    @property
    def client(self) -> redis.Redis:
        self._check_fork()
        # Read once: a concurrent close() may clear the attribute between a
        # check and a second read. Plan:
        # 2026-11-10-backend-thread-ownership-plan.md at 4cea0cb (D6).
        client = self._client
        if client is None:
            with self._client_lock:
                client = self._client
                if client is None:
                    pool = self._create_pool()
                    with contextlib.ExitStack() as cleanup:
                        cleanup.callback(pool.disconnect)
                        client = redis.Redis(connection_pool=pool)
                        cleanup.pop_all()
                    self._pool = pool
                    self._client = client
        return client

    def _create_pool(self) -> _ClosingConnectionPool:
        return _ClosingConnectionPool.from_url(
            self.target,
            decode_responses=True,
            max_connections=self.pool_options.max_connections,
            timeout=self.pool_options.timeout,
        )

    def _check_fork(self) -> None:
        current_pid = os.getpid()
        if current_pid == self._pid:
            return

        client = self._client
        pool = self._pool
        for resource in (client, pool):
            if resource is not None and not any(
                resource is existing for existing in _ABANDONED_FORK_REDIS_RESOURCES
            ):
                _ABANDONED_FORK_REDIS_RESOURCES.append(resource)
        self._client_lock = threading.Lock()
        self._init_lock = threading.Lock()
        self._recovery_lock = threading.Lock()
        self._client = None
        self._pool = None
        self._target_initialized = False
        self._last_recovery_check_ns = 0
        self._pid = current_pid

    def ping(self) -> bool:
        return bool(self.client.ping())

    def release_thread_connection(self) -> None:
        return None

    def close(self) -> None:
        self._check_fork()
        with self._client_lock:
            client = self._client
            pool = self._pool
            self._client = None
            self._pool = None
        if client is not None:
            client.close()
        if pool is not None:
            pool.close_when_idle(RUNNER_CLOSE_IN_FLIGHT_WAIT_SECONDS)

    def shutdown(self) -> None:
        self.close()

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        del sql, params, fetch
        raise NotImplementedError(
            "RedisRunner is not SQL-capable; use RedisBackendPlugin.create_core(...)"
        )

    def begin_immediate(self) -> None:
        raise NotImplementedError(
            "RedisRunner is not SQL-capable; use RedisBackendPlugin.create_core(...)"
        )

    commit = begin_immediate
    rollback = begin_immediate

    def setup(self, phase: Any) -> None:
        del phase

    def is_setup_complete(self, phase: Any) -> bool:
        del phase
        return True
