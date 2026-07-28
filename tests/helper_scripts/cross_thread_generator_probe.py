"""Process-isolated probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import multiprocessing as mp
import os
import threading
import time
import warnings
from pathlib import Path
from typing import Any, cast


def _build_backend(
    backend: str,
    target: str,
    scope: str,
    *,
    sqlite_default_config: bool,
) -> tuple[Any, Any, Any, Any]:
    runner: Any
    second_runner: Any
    plugin: Any
    if backend == "sqlite":
        from simplebroker._runner import SQLiteRunner
        from simplebroker.db import BrokerCore

        config = {} if sqlite_default_config else {"BROKER_BUSY_TIMEOUT": 0}
        runner = SQLiteRunner(str(Path(target)), config=config)
        second_runner = SQLiteRunner(str(Path(target)), config=config)
        return (
            runner,
            BrokerCore(runner, config=config),
            second_runner,
            BrokerCore(second_runner, config=config),
        )

    if backend == "postgres":
        from simplebroker_pg import PostgresRunner
        from simplebroker_pg import get_backend_plugin as get_pg_backend_plugin

        from simplebroker.db import BrokerCore

        plugin = get_pg_backend_plugin()
        runner = PostgresRunner(target, schema=scope)
        second_runner = PostgresRunner(target, schema=scope)
        return (
            runner,
            BrokerCore(runner, backend_plugin=plugin),
            second_runner,
            BrokerCore(second_runner, backend_plugin=plugin),
        )

    if backend == "redis":
        from simplebroker_redis import RedisRunner
        from simplebroker_redis import get_backend_plugin as get_redis_backend_plugin
        from simplebroker_redis.core import RedisBrokerCore

        plugin = get_redis_backend_plugin()
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


def _execute_probe(
    backend: str,
    target: str,
    scope: str,
    operation: str,
    sqlite_default_config: bool,
    second_writer_timeout: float,
) -> dict[str, Any]:
    runner, core, _second_runner, second_core = _build_backend(
        backend,
        target,
        scope,
        sqlite_default_config=sqlite_default_config,
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

        if operation == "move":
            generator = core.move_generator(
                "jobs",
                "done",
                delivery_guarantee="at_least_once",
                batch_size=2,
                with_timestamps=False,
            )
        else:
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
        try:
            core.get_meta()
            state["owner_error_after_poison"] = None
        except BaseException as exc:
            state["owner_error_after_poison"] = f"{type(exc).__name__}: {exc}"
        try:
            core.write("jobs", "owner-write-after-poison")
            state["owner_mutation_error_after_poison"] = None
        except BaseException as exc:
            state["owner_mutation_error_after_poison"] = f"{type(exc).__name__}: {exc}"
        if bool(getattr(core, "_poisoned", False)):
            for method_name in ("close", "shutdown"):
                try:
                    getattr(core, method_name)()
                    state[f"owner_{method_name}_error"] = None
                except BaseException as exc:
                    state[f"owner_{method_name}_error"] = f"{type(exc).__name__}: {exc}"
        else:
            state["owner_close_error"] = None
            state["owner_shutdown_error"] = None
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
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                generator_box["generator"].close()
                state["foreign_close_error"] = None
            except BaseException as exc:
                state["foreign_close_error"] = f"{type(exc).__name__}: {exc}"
            state["foreign_warning_count"] = sum(
                issubclass(warning.category, RuntimeWarning)
                and "finalized on a foreign thread" in str(warning.message)
                for warning in caught
            )

    closer = threading.Thread(target=foreign_close, daemon=True)
    closer.start()
    closer.join(timeout=2.0)
    state["foreign_close_blocked"] = closer.is_alive()

    inspect_owner.set()
    owner_inspected.wait(timeout=2.0)
    state["owner_inspection_completed"] = owner_inspected.is_set()
    state["poisoned_after_foreign_close"] = bool(getattr(core, "_poisoned", False))
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
        started_at = time.monotonic()
        try:
            second_core.write("jobs", "after-close")
            state["second_writer_error"] = None
        except BaseException as exc:
            state["second_writer_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            state["second_writer_elapsed"] = time.monotonic() - started_at
            writer_finished.set()

    writer = threading.Thread(target=second_writer, daemon=True)
    writer.start()
    writer.join(timeout=second_writer_timeout)
    state["second_writer_blocked"] = not writer_finished.is_set()
    state["owner_thread_alive"] = owner_thread.is_alive()
    return state


def _probe_child(
    send_connection: Any,
    backend: str,
    target: str,
    scope: str,
    operation: str,
    sqlite_default_config: bool,
    second_writer_timeout: float,
) -> None:
    try:
        result = _execute_probe(
            backend,
            target,
            scope,
            operation,
            sqlite_default_config,
            second_writer_timeout,
        )
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


def _execute_sidecar_probe(
    backend: str,
    target: str,
    scope: str,
    transaction: bool,
    action: str,
) -> dict[str, Any]:
    runner, core, _second_runner, _second_core = _build_backend(
        backend,
        target,
        scope,
        sqlite_default_config=False,
    )
    state: dict[str, Any] = {"backend": backend}
    manager_box: dict[str, Any] = {}
    session_box: dict[str, Any] = {}
    entered = threading.Event()
    inspect_owner = threading.Event()
    owner_inspected = threading.Event()

    def owner() -> None:
        owner_connection = None
        if backend == "sqlite":
            owner_connection = runner.get_connection()
        elif backend == "postgres":
            owner_connection = runner._get_thread_conn()

        manager = core.sidecar(transaction=transaction)
        manager_box["manager"] = manager
        try:
            session_box["session"] = manager.__enter__()
            state["owner_enter_error"] = None
        except BaseException as exc:
            state["owner_enter_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            entered.set()

        inspect_owner.wait(timeout=3.0)
        if owner_connection is not None:
            state["owner_transaction_after_foreign_close"] = _transaction_state(
                backend,
                runner,
                owner_connection,
            )
        try:
            core.close()
            state["owner_close_error"] = None
        except BaseException as exc:
            state["owner_close_error"] = f"{type(exc).__name__}: {exc}"
        owner_inspected.set()

    owner_thread = threading.Thread(target=owner, daemon=True)
    owner_thread.start()
    if not entered.wait(timeout=4.0):
        return {"backend": backend, "probe_error": "sidecar owner did not enter"}
    if state.get("owner_enter_error") is not None:
        return state

    thrown = RuntimeError("foreign sidecar throw")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            if action == "clean_exit":
                state["foreign_close_result"] = manager_box["manager"].__exit__(
                    None,
                    None,
                    None,
                )
            else:
                state["foreign_close_result"] = manager_box["manager"].gen.throw(thrown)
            state["foreign_close_error"] = None
        except BaseException as exc:
            state["foreign_close_error"] = f"{type(exc).__name__}: {exc}"
        state["foreign_warning_count"] = sum(
            issubclass(warning.category, RuntimeWarning)
            and "sidecar session finalized on a foreign thread" in str(warning.message)
            for warning in caught
        )

    state["poisoned_after_foreign_close"] = bool(getattr(core, "_poisoned", False))
    try:
        session_box["session"].run("SELECT 1", fetch=True)
        state["retained_session_error"] = None
    except BaseException as exc:
        state["retained_session_error"] = f"{type(exc).__name__}: {exc}"

    inspect_owner.set()
    owner_inspected.wait(timeout=2.0)
    state["owner_inspection_completed"] = owner_inspected.is_set()
    state["owner_thread_alive"] = owner_thread.is_alive()
    return state


def _sidecar_probe_child(
    send_connection: Any,
    backend: str,
    target: str,
    scope: str,
    transaction: bool,
    action: str,
) -> None:
    try:
        result = _execute_sidecar_probe(
            backend,
            target,
            scope,
            transaction,
            action,
        )
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
    operation: str = "claim",
    sqlite_default_config: bool = False,
    second_writer_timeout: float = 1.0,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Run one backend probe in a spawned process with a hard timeout."""
    context = mp.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_probe_child,
        args=(
            send_connection,
            backend,
            target,
            scope,
            operation,
            sqlite_default_config,
            second_writer_timeout,
        ),
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
        result = cast(dict[str, Any], receive_connection.recv())
    finally:
        receive_connection.close()

    process.join(timeout=2.0)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2.0)
    result["parent_timeout"] = False
    result["process_exitcode"] = process.exitcode
    return result


def run_cross_thread_sidecar_probe(
    backend: str,
    target: str,
    scope: str = "",
    *,
    transaction: bool = True,
    action: str = "clean_exit",
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Run one sidecar foreign-resumption probe in a spawned process."""

    context = mp.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_sidecar_probe_child,
        args=(send_connection, backend, target, scope, transaction, action),
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
        result = cast(dict[str, Any], receive_connection.recv())
    finally:
        receive_connection.close()

    process.join(timeout=2.0)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2.0)
    result["parent_timeout"] = False
    result["process_exitcode"] = process.exitcode
    return result


def _queue_close_probe_child(
    send_connection: Any,
    backend: str,
    target: str,
    scope: str,
    mode: str,
) -> None:
    try:
        from simplebroker import Queue
        from simplebroker._targets import BrokerTarget

        broker_target = BrokerTarget(
            backend,
            target,
            backend_options={"schema": scope} if backend == "postgres" else {},
        )
        runner = None
        if mode == "private_persistent":
            if backend != "postgres":
                raise ValueError("private persistent probe currently requires postgres")
            from simplebroker_pg import PostgresRunner

            runner = PostgresRunner(target, schema=scope)
        persistent = mode != "ephemeral"
        queue = Queue(
            "jobs",
            db_path=broker_target,
            persistent=persistent,
            runner=runner,
        )
        sibling = (
            Queue("other", db_path=broker_target, persistent=True)
            if mode == "shared_non_last"
            else None
        )
        queue.write("one")
        queue.write("two")
        generator = cast(
            Any,
            queue.read_generator(
                with_timestamps=False,
                delivery_guarantee="at_least_once",
            ),
        )
        next(generator)
        caught: list[warnings.WarningMessage] = []

        def foreign_close() -> None:
            nonlocal caught
            with warnings.catch_warnings(record=True) as recorded:
                warnings.simplefilter("always")
                generator.close()
                caught = list(recorded)

        closer = threading.Thread(target=foreign_close, daemon=True)
        closer.start()
        closer.join(timeout=2.0)
        if closer.is_alive():
            raise RuntimeError("foreign Queue generator close did not finish")
        close_errors: list[str | None] = []
        for handle in (queue, sibling):
            if handle is None:
                continue
            try:
                handle.close()
                close_errors.append(None)
            except BaseException as exc:
                close_errors.append(f"{type(exc).__name__}: {exc}")
        if mode in {"shared_last", "shared_non_last"}:
            final_handle = sibling if sibling is not None else queue
            try:
                final_handle.close()
                repeated_close_error = None
            except BaseException as exc:
                repeated_close_error = f"{type(exc).__name__}: {exc}"
        else:
            repeated_close_error = None
        result = {
            "close_errors": close_errors,
            "repeated_close_error": repeated_close_error,
            "warning_count": len(caught),
        }
    except BaseException as exc:
        result = {"probe_error": f"{type(exc).__name__}: {exc}"}
    try:
        send_connection.send(result)
        send_connection.close()
    finally:
        os._exit(0)


def run_queue_close_mode_probe(
    backend: str,
    target: str,
    scope: str,
    mode: str,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Run a public Queue close-mode poison probe in a spawned process."""
    context = mp.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_queue_close_probe_child,
        args=(send_connection, backend, target, scope, mode),
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
        result = cast(dict[str, Any], receive_connection.recv())
    finally:
        receive_connection.close()
    process.join(timeout=2.0)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2.0)
    result["parent_timeout"] = False
    result["process_exitcode"] = process.exitcode
    return result
