"""RedisRunner close never tears down a connection another thread is using.

The thread running a command owns its checked-out connection until the command
returns. ``RedisRunner.close()`` may run concurrently from another thread
([SB-API-11]); it must not close that connection under the command or hand a
caller a half-closed client.

Plan: docs/plans/2026-11-10-backend-thread-ownership-plan.md (F6, F7; Q4 option b).
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, cast

import pytest
import redis
import simplebroker_redis.runner as redis_runner_module
from simplebroker_redis.pool import RedisPoolOptions
from simplebroker_redis.runner import RedisRunner

pytestmark = [pytest.mark.redis_only]


class _CloseAfterClientRead(RedisRunner):
    """Lands one real ``close()`` right after a read of ``_client``."""

    armed = False

    def __getattribute__(self, name: str) -> Any:
        value = super().__getattribute__(name)
        if name == "_client" and type(self).armed:
            type(self).armed = False
            RedisRunner.close(self)
        return value


def test_client_returns_the_client_it_observed_when_close_races_it(
    redis_url: str, redis_namespace: str
) -> None:
    runner = _CloseAfterClientRead(redis_url, namespace=redis_namespace)
    try:
        assert runner.client is not None
        _CloseAfterClientRead.armed = True
        assert runner.client is not None
    finally:
        _CloseAfterClientRead.armed = False
        runner.shutdown()


class _BlockedCommand:
    """One BLPOP on a connection checked out from the runner's pool.

    The worker records its server-side client id, so the test can wait for this
    exact connection to be blocked on the server.
    """

    def __init__(self, pool: redis.BlockingConnectionPool, key: str) -> None:
        self._pool = pool
        self._key = key
        self.client_id: int | None = None
        self.id_known = threading.Event()

    def run(self) -> Any:
        conn = self._pool.get_connection()
        try:
            conn.send_command("CLIENT", "ID")
            self.client_id = int(conn.read_response())
            self.id_known.set()
            conn.send_command("BLPOP", self._key, 10)
            return conn.read_response()
        finally:
            self._pool.release(conn)

    def is_blocked(self, observer: redis.Redis) -> bool:
        return any(
            int(c.get("id", -1)) == self.client_id and c.get("cmd") == "blpop"
            for c in observer.client_list()
        )

    def is_connected(self, observer: redis.Redis) -> bool:
        return any(
            int(c.get("id", -1)) == self.client_id for c in observer.client_list()
        )


def _wait_until(predicate: Any, message: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, message
        time.sleep(0.01)


def _closing(pool: redis.BlockingConnectionPool) -> bool:
    """Passive witness: the pool has started closing."""
    return bool(getattr(pool, "_closing", False))


def test_close_waits_for_an_in_flight_command(
    redis_url: str, redis_namespace: str
) -> None:
    """``close()`` stays pending until a running command returns its connection."""
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    pool = cast(redis.BlockingConnectionPool, runner.client.connection_pool)
    key = f"{redis_namespace}:owned-blpop"
    command = _BlockedCommand(pool, key)
    observer = redis.Redis.from_url(redis_url, decode_responses=True)
    workers = ThreadPoolExecutor(max_workers=2, thread_name_prefix="redis-owner")
    try:
        outcome = workers.submit(command.run)
        assert command.id_known.wait(5.0)
        _wait_until(lambda: command.is_blocked(observer), "BLPOP never blocked")
        closing = workers.submit(runner.close)
        _wait_until(lambda: _closing(pool) or closing.done(), "close never started")
        # While the command holds its connection, close keeps waiting.
        done, _ = wait([closing], timeout=0.3)
        assert not done
        assert command.is_blocked(observer)
        observer.rpush(key, "delivered")
        assert outcome.result(timeout=10.0) == [key, "delivered"]
        closing.result(timeout=5.0)
    finally:
        workers.shutdown(wait=True)
        observer.delete(key)
        observer.close()
        runner.shutdown()


def test_close_refuses_checkout_instead_of_handing_out_a_closing_connection(
    redis_url: str, redis_namespace: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A checkout racing ``close()`` is refused, never given a closing socket."""
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    client = runner.client
    assert client.ping()
    pool = cast(redis.BlockingConnectionPool, client.connection_pool)
    [idle] = [conn for conn in pool.pool.queue if conn is not None]
    handed: list[Any] = []
    refused: list[BaseException] = []
    real_disconnect = type(idle).disconnect

    def checkout() -> None:
        try:
            handed.append(pool.get_connection())
        except redis.ConnectionError as exc:
            refused.append(exc)

    def disconnect(self: Any, *args: Any, **kwargs: Any) -> Any:
        if self is idle and not (handed or refused):
            other = threading.Thread(target=checkout, name="stale-client-checkout")
            other.start()
            other.join(timeout=5.0)
        return real_disconnect(self, *args, **kwargs)

    monkeypatch.setattr(type(idle), "disconnect", disconnect)
    try:
        runner.close()
        assert handed == []
        assert len(refused) == 1
    finally:
        monkeypatch.undo()
        for conn in handed:
            pool.release(conn)
        runner.shutdown()


def test_checkout_waiting_for_a_slot_when_close_begins_is_refused(
    redis_url: str, redis_namespace: str
) -> None:
    """A checkout admitted before close, still waiting for a slot, gets none.

    With one slot held by a running command, a second checkout waits. Close
    begins; the command's release returns the slot. Whichever side takes it,
    the waiting checkout is refused and never holds a connection that close
    disconnects.
    """
    runner = RedisRunner(
        redis_url,
        namespace=redis_namespace,
        pool_options=RedisPoolOptions(max_connections=1, timeout=3.0),
    )
    pool = cast(redis.BlockingConnectionPool, runner.client.connection_pool)
    key = f"{redis_namespace}:single-slot"
    command = _BlockedCommand(pool, key)
    observer = redis.Redis.from_url(redis_url, decode_responses=True)
    slot_waiting = threading.Event()
    real_get = pool.pool.get

    def observed_get(*args: Any, **kwargs: Any) -> Any:
        if threading.current_thread().name.startswith("slot-waiter"):
            slot_waiting.set()
        return real_get(*args, **kwargs)

    pool.pool.get = observed_get
    workers = ThreadPoolExecutor(max_workers=1, thread_name_prefix="redis-owner")
    waiter = ThreadPoolExecutor(max_workers=1, thread_name_prefix="slot-waiter")
    try:
        outcome = workers.submit(command.run)
        assert command.id_known.wait(5.0)
        _wait_until(lambda: command.is_blocked(observer), "BLPOP never blocked")
        waiting = waiter.submit(pool.get_connection)
        assert slot_waiting.wait(5.0)
        closing = ThreadPoolExecutor(max_workers=1, thread_name_prefix="closer")
        try:
            closed = closing.submit(runner.close)
            _wait_until(lambda: _closing(pool), "close never started")
            observer.rpush(key, "delivered")
            assert outcome.result(timeout=10.0) == [key, "delivered"]
            with pytest.raises(redis.ConnectionError):
                waiting.result(timeout=10.0)
            closed.result(timeout=10.0)
        finally:
            closing.shutdown(wait=True)
    finally:
        workers.shutdown(wait=True)
        waiter.shutdown(wait=True)
        observer.delete(key)
        observer.close()
        runner.shutdown()


def test_close_bound_leaves_a_long_command_to_its_owner(
    redis_url: str, redis_namespace: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``close()`` returns at its bound; the command's owner disconnects it."""
    bound = 0.3
    monkeypatch.setattr(
        redis_runner_module, "RUNNER_CLOSE_IN_FLIGHT_WAIT_SECONDS", bound, raising=False
    )
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    pool = cast(redis.BlockingConnectionPool, runner.client.connection_pool)
    key = f"{redis_namespace}:long-blpop"
    command = _BlockedCommand(pool, key)
    observer = redis.Redis.from_url(redis_url, decode_responses=True)
    workers = ThreadPoolExecutor(max_workers=1, thread_name_prefix="redis-owner")
    try:
        outcome = workers.submit(command.run)
        assert command.id_known.wait(5.0)
        _wait_until(lambda: command.is_blocked(observer), "BLPOP never blocked")
        started = time.monotonic()
        runner.close()
        elapsed = time.monotonic() - started
        # close() waited for the command up to its bound, then returned.
        assert bound <= elapsed < bound + 2.0
        assert command.is_blocked(observer)
        observer.rpush(key, "delivered")
        assert outcome.result(timeout=10.0) == [key, "delivered"]
        # On release into the closing pool the owner disconnected it.
        _wait_until(
            lambda: not command.is_connected(observer),
            "the owner never disconnected its connection",
        )
    finally:
        workers.shutdown(wait=True)
        observer.delete(key)
        observer.close()
        runner.shutdown()
