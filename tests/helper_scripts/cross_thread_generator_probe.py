"""Process-isolated probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import multiprocessing as mp
import os
import threading
from pathlib import Path
from typing import Any


def _build_backend(
    backend: str,
    target: str,
    scope: str,
) -> tuple[Any, Any, Any, Any]:
    if backend == "sqlite":
        from simplebroker._runner import SQLiteRunner
        from simplebroker.db import BrokerCore

        config = {"BROKER_BUSY_TIMEOUT": 0}
        runner = SQLiteRunner(str(Path(target)), config=config)
        second_runner = SQLiteRunner(str(Path(target)), config=config)
        return (
            runner,
            BrokerCore(runner, config=config),
            second_runner,
            BrokerCore(second_runner, config=config),
        )

    if backend == "postgres":
        from simplebroker_pg import PostgresRunner, get_backend_plugin

        from simplebroker.db import BrokerCore

        plugin = get_backend_plugin()
        runner = PostgresRunner(target, schema=scope)
        second_runner = PostgresRunner(target, schema=scope)
        return (
            runner,
            BrokerCore(runner, backend_plugin=plugin),
            second_runner,
            BrokerCore(second_runner, backend_plugin=plugin),
        )

    if backend == "redis":
        from simplebroker_redis import RedisRunner, get_backend_plugin
        from simplebroker_redis.core import RedisBrokerCore

        plugin = get_backend_plugin()
        plugin.initialize_target(target, backend_options={"namespace": scope})
        runner = RedisRunner(target, namespace=scope)
        second_runner = RedisRunner(target, namespace=scope)
        return (
            runner,
            RedisBrokerCore(runner),
            second_runner,
            RedisBrokerCore(second_runner),
        )

    raise ValueError(f"unknown backend: {backend}")


def _transaction_state(backend: str, runner: Any, owner_connection: Any) -> Any:
    if backend == "sqlite":
        return bool(owner_connection.in_transaction)
    if backend == "postgres":
        return {
            "runner_thread_local": bool(runner._in_transaction()),
            "driver_status": str(owner_connection.info.transaction_status),
        }
    return None


def _execute_probe(backend: str, target: str, scope: str) -> dict[str, Any]:
    runner, core, _second_runner, second_core = _build_backend(
        backend,
        target,
        scope,
    )
    core.write("jobs", "one")
    core.write("jobs", "two")

    state: dict[str, Any] = {"backend": backend}
    generator_box: dict[str, Any] = {}
    advanced = threading.Event()
    inspect_owner = threading.Event()
    owner_inspected = threading.Event()

    def owner() -> None:
        owner_connection = None
        if backend == "sqlite":
            owner_connection = runner.get_connection()
        elif backend == "postgres":
            owner_connection = runner._get_thread_conn()

        generator = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=2,
            with_timestamps=False,
        )
        generator_box["generator"] = generator
        try:
            state["first_yield"] = next(generator)
        except BaseException as exc:
            state["owner_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            advanced.set()

        inspect_owner.wait(timeout=3.0)
        if owner_connection is not None:
            state["owner_transaction_after_foreign_close"] = _transaction_state(
                backend,
                runner,
                owner_connection,
            )
        else:
            state["owner_transaction_after_foreign_close"] = None
        state["active_batch_after_foreign_close"] = getattr(
            core,
            "_active_generator_batch",
            None,
        )
        owner_inspected.set()

    owner_thread = threading.Thread(target=owner, daemon=True)
    owner_thread.start()
    if not advanced.wait(timeout=4.0):
        return {"backend": backend, "probe_error": "owner did not yield"}

    waiter_started = threading.Event()
    waiter_finished = threading.Event()

    def same_core_waiter() -> None:
        waiter_started.set()
        try:
            core.get_meta()
            state["same_core_waiter_error"] = None
        except BaseException as exc:
            state["same_core_waiter_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            waiter_finished.set()

    waiter = threading.Thread(target=same_core_waiter, daemon=True)
    waiter.start()
    waiter_started.wait(timeout=1.0)
    waiter.join(timeout=0.25)
    state["same_core_waiter_blocked_before_close"] = not waiter_finished.is_set()

    def foreign_close() -> None:
        try:
            generator_box["generator"].close()
            state["foreign_close_error"] = None
        except BaseException as exc:
            state["foreign_close_error"] = f"{type(exc).__name__}: {exc}"

    closer = threading.Thread(target=foreign_close, daemon=True)
    closer.start()
    closer.join(timeout=2.0)
    state["foreign_close_blocked"] = closer.is_alive()

    inspect_owner.set()
    owner_inspected.wait(timeout=2.0)
    state["owner_inspection_completed"] = owner_inspected.is_set()
    state["core_lock_available_after_close"] = bool(
        getattr(core, "_lock", None) is None or core._lock.acquire(blocking=False)
    )
    if state["core_lock_available_after_close"] and getattr(core, "_lock", None):
        core._lock.release()

    waiter.join(timeout=0.5)
    state["same_core_waiter_blocked_after_close"] = not waiter_finished.is_set()

    try:
        state["messages_visible_after_close"] = second_core.peek_many(
            "jobs",
            10,
            with_timestamps=False,
        )
    except BaseException as exc:
        state["messages_visible_after_close"] = f"{type(exc).__name__}: {exc}"

    writer_finished = threading.Event()

    def second_writer() -> None:
        try:
            second_core.write("jobs", "after-close")
            state["second_writer_error"] = None
        except BaseException as exc:
            state["second_writer_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            writer_finished.set()

    writer = threading.Thread(target=second_writer, daemon=True)
    writer.start()
    writer.join(timeout=1.0)
    state["second_writer_blocked"] = not writer_finished.is_set()
    state["owner_thread_alive"] = owner_thread.is_alive()
    return state


def _probe_child(send_connection: Any, backend: str, target: str, scope: str) -> None:
    try:
        result = _execute_probe(backend, target, scope)
    except BaseException as exc:
        result = {
            "backend": backend,
            "probe_error": f"{type(exc).__name__}: {exc}",
        }
    try:
        send_connection.send(result)
        send_connection.close()
    finally:
        os._exit(0)


def run_cross_thread_generator_probe(
    backend: str,
    target: str,
    scope: str = "",
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Run one backend probe in a spawned process with a hard timeout."""
    context = mp.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_probe_child,
        args=(send_connection, backend, target, scope),
    )
    process.start()
    send_connection.close()
    try:
        if not receive_connection.poll(timeout):
            process.terminate()
            process.join(timeout=2.0)
            return {
                "backend": backend,
                "parent_timeout": True,
                "process_exitcode": process.exitcode,
            }
        result = receive_connection.recv()
    finally:
        receive_connection.close()

    process.join(timeout=2.0)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2.0)
    result["parent_timeout"] = False
    result["process_exitcode"] = process.exitcode
    return result
