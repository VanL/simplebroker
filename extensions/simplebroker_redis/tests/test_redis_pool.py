"""Valkey/Redis command connection-pool integration tests."""

from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path
from types import FrameType
from typing import Any, cast

import pytest
import redis
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis import plugin as redis_plugin_module
from simplebroker_redis.core import RedisBrokerCore

from simplebroker import resolve_config
from simplebroker._exceptions import DatabaseError, OperationalError

pytestmark = [pytest.mark.redis_only]


def test_runner_uses_blocking_connection_pool(
    redis_url: str, redis_namespace: str
) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    try:
        runner.client.ping()
        assert isinstance(runner._pool, redis.BlockingConnectionPool)
    finally:
        runner.shutdown()


def test_runner_reuses_one_pool_for_multiple_client_accesses(
    redis_url: str, redis_namespace: str
) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    try:
        first_client = runner.client
        first_pool = runner._pool
        assert first_client is runner.client
        assert first_pool is runner._pool
    finally:
        runner.shutdown()


def test_pool_options_from_backend_options(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = plugin.create_runner(
        redis_url,
        backend_options={
            "namespace": redis_namespace,
            "max_connections": 3,
            "pool_timeout": 0.25,
        },
    )
    try:
        assert runner.pool_options.max_connections == 3
        assert runner.pool_options.timeout == 0.25
        runner.client.ping()
        assert runner._pool is not None
        assert runner._pool.max_connections == 3
    finally:
        runner.shutdown()


@pytest.mark.parametrize(
    "backend_options",
    [
        {"max_connections": 0},
        {"max_connections": True},
        {"max_connections": "nope"},
        {"pool_timeout": 0},
        {"pool_timeout": -1},
        {"pool_timeout": True},
        {"pool_timeout": "nope"},
        {"unexpected": "value"},
    ],
)
def test_invalid_pool_options_raise_database_error(
    redis_url: str, redis_namespace: str, backend_options: dict[str, object]
) -> None:
    plugin = get_backend_plugin()
    options = {"namespace": redis_namespace, **backend_options}
    with pytest.raises(DatabaseError):
        plugin.create_runner(redis_url, backend_options=options)


def test_schema_and_namespace_must_not_disagree(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    with pytest.raises(DatabaseError):
        plugin.create_runner(
            redis_url,
            backend_options={
                "namespace": redis_namespace,
                "schema": f"{redis_namespace}_other",
            },
        )


def test_pool_exhaustion_is_bounded(redis_url: str, redis_namespace: str) -> None:
    plugin = get_backend_plugin()
    runner = plugin.create_runner(
        redis_url,
        backend_options={
            "namespace": redis_namespace,
            "max_connections": 1,
            "pool_timeout": 0.1,
        },
    )
    core = RedisBrokerCore(runner)
    held_connection = None
    try:
        core.write("jobs", "before")
        assert runner._pool is not None
        held_connection = runner._pool.get_connection()
        started = time.monotonic()
        with pytest.raises(OperationalError):
            core.write("jobs", "blocked")
        assert time.monotonic() - started < 1.0
    finally:
        if held_connection is not None and runner._pool is not None:
            runner._pool.release(held_connection)
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_shutdown_disconnects_pool(redis_url: str, redis_namespace: str) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    runner.client.ping()
    first_pool = runner._pool
    assert first_pool is not None

    runner.shutdown()

    assert runner._pool is None
    assert runner._client is None

    runner.client.ping()
    try:
        assert runner._pool is not None
        assert runner._pool is not first_pool
    finally:
        runner.shutdown()


def test_fork_check_recreates_pool(redis_url: str, redis_namespace: str) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    runner.client.ping()
    first_pool = runner._pool
    assert first_pool is not None

    runner._pid = -1
    runner.client.ping()

    try:
        assert runner._pid == os.getpid()
        assert runner._pool is not None
        assert runner._pool is not first_pool
    finally:
        runner.shutdown()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_redis_core_init_recovers_before_inherited_runner_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = get_backend_plugin()
    monkeypatch.setattr(plugin, "initialize_target", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        RedisRunner,
        "backend_plugin",
        property(lambda _runner: plugin),
    )
    runner = RedisRunner("redis://transport.invalid/0", namespace="fork_init")
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_init_lock() -> None:
        with runner._init_lock:
            lock_held.set()
            release_lock.wait(10.0)

    holder = threading.Thread(target=hold_init_lock)
    holder.start()
    assert lock_held.wait(2.0)

    pid = os.fork()
    if pid == 0:
        try:
            child_core = RedisBrokerCore(runner)
            os._exit(0 if child_core._pid == os.getpid() else 2)
        except BaseException:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            os._exit(1)

    try:
        deadline = time.monotonic() + 2.0
        status: int | None = None
        while time.monotonic() < deadline:
            waited_pid, child_status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                status = child_status
                break
            time.sleep(0.01)
        if status is None:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            pytest.fail("Redis core initialization blocked on inherited init lock")
        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == 0
    finally:
        release_lock.set()
        holder.join(timeout=2.0)

    parent_core = RedisBrokerCore(runner)
    parent_core.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_redis_core_maintenance_recovers_before_inherited_core_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = get_backend_plugin()
    monkeypatch.setattr(plugin, "initialize_target", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        RedisRunner,
        "backend_plugin",
        property(lambda _runner: plugin),
    )
    runner = RedisRunner("redis://transport.invalid/0", namespace="fork_maintenance")
    core = RedisBrokerCore(
        runner,
        config=resolve_config(
            override={"BROKER_AUTO_VACUUM": 1, "BROKER_AUTO_VACUUM_INTERVAL": 10_000}
        ),
    )
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_core_lock() -> None:
        with core._lock:
            lock_held.set()
            release_lock.wait(10.0)

    holder = threading.Thread(target=hold_core_lock)
    holder.start()
    assert lock_held.wait(2.0)

    pid = os.fork()
    if pid == 0:
        try:
            core._record_maintenance_activity(1)
            os._exit(0)
        except BaseException:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            os._exit(1)

    try:
        deadline = time.monotonic() + 2.0
        status: int | None = None
        while time.monotonic() < deadline:
            waited_pid, child_status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                status = child_status
                break
            time.sleep(0.01)
        if status is None:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            pytest.fail("Redis maintenance blocked on the inherited core lock")
        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == 0
    finally:
        release_lock.set()
        holder.join(timeout=2.0)

    core._record_maintenance_activity(1)
    core.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("operation", ["check", "close"])
def test_redis_runner_abandons_inherited_pool_without_entering_its_fork_lock(
    operation: str,
) -> None:
    runner = RedisRunner("redis://transport.invalid/0", namespace="fork_pool")
    _ = runner.client
    pool = runner._pool
    assert pool is not None
    fork_lock = cast(Any, pool)._fork_lock
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_pool_fork_lock() -> None:
        with fork_lock:
            lock_held.set()
            release_lock.wait(10.0)

    holder = threading.Thread(target=hold_pool_fork_lock)
    holder.start()
    assert lock_held.wait(2.0)

    pid = os.fork()
    if pid == 0:
        try:
            if operation == "check":
                runner._check_fork()
            else:
                runner.close()
            os._exit(0)
        except BaseException:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            os._exit(1)

    try:
        deadline = time.monotonic() + 2.0
        status: int | None = None
        while time.monotonic() < deadline:
            waited_pid, child_status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                status = child_status
                break
            time.sleep(0.01)
        if status is None:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            pytest.fail("Redis fork recovery entered the inherited pool lock")
        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == 0
    finally:
        release_lock.set()
        holder.join(timeout=2.0)

    runner.close()


def test_shared_activity_registry_is_pid_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeListener(redis_plugin_module._SharedRedisActivityListener):
        def __init__(self, target: str, namespace: str) -> None:
            self._target = target
            self._namespace = namespace
            self.closed = False

        def close(self) -> None:
            self.closed = True

    current_pid = 100
    monkeypatch.setattr(
        redis_plugin_module, "_SharedRedisActivityListener", FakeListener
    )
    monkeypatch.setattr(redis_plugin_module.os, "getpid", lambda: current_pid)
    registry = redis_plugin_module._RedisActivityRegistry()

    parent_first = registry.listener("redis://example.test/0", "namespace")
    parent_second = registry.listener("redis://example.test/0", "namespace")
    assert parent_second is parent_first

    current_pid = 200
    child = registry.listener("redis://example.test/0", "namespace")
    assert child is not parent_first


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_redis_activity_registry_recovers_before_inherited_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeListener(redis_plugin_module._SharedRedisActivityListener):
        def __init__(self, target: str, namespace: str) -> None:
            self._target = target
            self._namespace = namespace
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr(
        redis_plugin_module,
        "_SharedRedisActivityListener",
        FakeListener,
    )
    registry = redis_plugin_module._RedisActivityRegistry()
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_registry_lock() -> None:
        with registry._lock:
            lock_held.set()
            release_lock.wait(10.0)

    holder = threading.Thread(target=hold_registry_lock)
    holder.start()
    assert lock_held.wait(2.0)

    pid = os.fork()
    if pid == 0:
        try:
            child_listener = cast(
                FakeListener,
                registry.listener("redis://example/0", "tenant"),
            )
            registry.release(child_listener)
            os._exit(0 if child_listener.close_calls == 1 else 2)
        except BaseException:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            os._exit(1)

    try:
        deadline = time.monotonic() + 2.0
        status: int | None = None
        while time.monotonic() < deadline:
            waited_pid, child_status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                status = child_status
                break
            time.sleep(0.01)
        if status is None:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            pytest.fail("Redis activity registry blocked on inherited lock")
        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == 0
    finally:
        release_lock.set()
        holder.join(timeout=2.0)

    parent_listener = cast(
        FakeListener,
        registry.listener("redis://example/0", "tenant"),
    )
    registry.release(parent_listener)
    assert parent_listener.close_calls == 1


def test_activity_waiter_does_not_consume_command_pool_slot(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = plugin.create_runner(
        redis_url,
        backend_options={
            "namespace": redis_namespace,
            "max_connections": 1,
            "pool_timeout": 0.25,
        },
    )
    waiter = plugin.create_activity_waiter(
        target=None,
        runner=runner,
        queue_name="jobs",
        stop_event=None,
    )
    assert waiter is not None
    core = RedisBrokerCore(runner)
    try:
        core.write("jobs", "payload")
        assert waiter.wait(2.0) is True
        assert core.claim_one("jobs", with_timestamps=False) == "payload"
    finally:
        waiter.close()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("held", ["admission", "project", "none"])
def test_public_persistent_queue_recovers_child_owned_session(
    redis_url: str, redis_namespace: str, tmp_path: Path, held: str
) -> None:
    import inspect
    import select
    import sys
    from concurrent.futures import ThreadPoolExecutor

    from simplebroker import BrokerTarget, Queue
    from simplebroker._broker_session import _ProcessBrokerSession
    from simplebroker.db import DBConnection

    config_path = tmp_path / "project.toml"
    config_path.write_text("version = 1\n")
    target = BrokerTarget(
        "redis", redis_url, {"namespace": redis_namespace}, config_path=config_path
    )
    paused = threading.Event()
    resume = threading.Event()
    method = (
        DBConnection._ensure_project_target_initialized
        if held == "project"
        else _ProcessBrokerSession._begin_operation
    )
    source, first_line = inspect.getsourcelines(method)
    fragment = (
        "if self._project_setup_complete:"
        if held == "project"
        else "if self._closed or self._closing:"
    )
    line_number = first_line + next(
        i for i, line in enumerate(source) if line.strip() == fragment
    )

    def trace(frame: FrameType, event: str, arg: object) -> Any:
        if (
            event == "line"
            and frame.f_code is method.__code__
            and frame.f_lineno == line_number
        ):
            paused.set()
            assert resume.wait(15)
        return trace

    try:
        with Queue("fork_jobs", db_path=target, persistent=True) as queue:
            if held != "project":
                queue.write("before")
                assert queue.read() == "before"
            assert queue.conn is not None
            inherited = queue.conn._shared_session
            inherited_key = queue.conn._shared_key
            assert inherited is not None

            def parent_write() -> None:
                try:
                    if held != "none":
                        sys.settrace(trace)
                    else:
                        paused.set()
                    queue.write("parent")
                finally:
                    sys.settrace(None)

            executor = ThreadPoolExecutor(max_workers=1)
            future = executor.submit(parent_write)
            read_fd, write_fd = os.pipe()
            child_pid = None
            try:
                assert paused.wait(5)
                if held == "none":
                    future.result(timeout=5)
                child_pid = os.fork()
                if child_pid == 0:
                    os.close(read_fd)
                    status = 1
                    try:
                        # Cleanup alone must not acquire or finalize an inherited session.
                        queue.conn.cleanup()
                        message_id = queue.write("child")
                        assert queue.conn._shared_key != inherited_key
                        assert queue.conn._shared_key is not None
                        assert queue.conn._shared_key.pid == os.getpid()
                        assert queue.conn._shared_session is not inherited
                        assert queue.peek_one(exact_timestamp=message_id) == "child"
                        assert queue.read_one(exact_timestamp=message_id) == "child"
                        queue.conn.get_core()
                        queue.conn.cleanup()
                        queue.close()
                        assert not inherited._closed
                        os.write(write_fd, b"child session recovered")
                        status = 0
                    finally:
                        os._exit(status)
                os.close(write_fd)
                write_fd = -1
                ready, _, _ = select.select([read_fd], [], [], 5)
                assert ready, "Redis Queue waited on inherited manager/session state"
                assert os.read(read_fd, 4096) == b"child session recovered"
                _, status = os.waitpid(child_pid, 0)
                child_pid = None
                assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
            finally:
                if child_pid is not None:
                    os.kill(child_pid, signal.SIGKILL)
                    os.waitpid(child_pid, 0)
                os.close(read_fd)
                if write_fd != -1:
                    os.close(write_fd)
                resume.set()
                executor.shutdown(wait=True)
            future.result(timeout=5)
            queue.write("after")
            assert queue.read_many(3) == ["parent", "after"]
    finally:
        get_backend_plugin().cleanup_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )
