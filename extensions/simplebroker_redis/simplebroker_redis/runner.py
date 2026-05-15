"""Valkey/Redis resource handle for SimpleBroker."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterable
from typing import Any

import redis

from ._constants import DEFAULT_STALE_BATCH_SECONDS
from .pool import RedisPoolOptions, pool_options_from_config
from .validation import require_namespace


class RedisRunner:
    """Resource handle for one Valkey/Redis target and namespace."""

    def __init__(
        self,
        target: str,
        *,
        namespace: str | None = None,
        backend_options: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        pool_options: RedisPoolOptions | None = None,
        stale_batch_seconds: int = DEFAULT_STALE_BATCH_SECONDS,
    ) -> None:
        options = dict(backend_options or {})
        if namespace is not None:
            options["namespace"] = namespace
        self.target = target
        self.namespace = require_namespace(options)
        self.pool_options = pool_options or pool_options_from_config(config, options)
        self.stale_batch_seconds = stale_batch_seconds
        self._pid = os.getpid()
        self._pool: redis.BlockingConnectionPool | None = None
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
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._pool = self._create_pool()
                    self._client = redis.Redis(connection_pool=self._pool)
        return self._client

    def _create_pool(self) -> redis.BlockingConnectionPool:
        return redis.BlockingConnectionPool.from_url(
            self.target,
            decode_responses=True,
            max_connections=self.pool_options.max_connections,
            timeout=self.pool_options.timeout,
        )

    def _check_fork(self) -> None:
        current_pid = os.getpid()
        if current_pid == self._pid:
            return
        self.close()
        self._pid = current_pid

    def ping(self) -> bool:
        return bool(self.client.ping())

    def release_thread_connection(self) -> None:
        return None

    def close(self) -> None:
        client = self._client
        pool = self._pool
        self._client = None
        self._pool = None
        if client is not None:
            client.close()
        if pool is not None:
            pool.disconnect()

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
