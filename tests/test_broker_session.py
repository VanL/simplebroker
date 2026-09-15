"""Public BrokerSession lifecycle contract [SB-API-1/3/11]."""

from __future__ import annotations

import gc
import os
import select
import sqlite3
import sys
import threading
import weakref
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker import BrokerSession, BrokerTarget, Queue, resolve_config
from simplebroker.db import BrokerDB, SQLiteRunner

pytestmark = [pytest.mark.shared]


@pytest.mark.parametrize("implicit_target", [None, ""])
def test_connect_implicit_target_matches_queue_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    implicit_target: str | None,
) -> None:
    monkeypatch.chdir(tmp_path)
    queue = Queue("direct", db_path=implicit_target, persistent=True)
    session = BrokerSession.connect(implicit_target)

    assert session.target == queue.db_target
    assert session.backend_name == queue.backend_name == "sqlite"

    session.close()
    queue.close()


def test_connect_and_minted_queues_share_one_process_session(tmp_path: Path) -> None:
    target = str(tmp_path / "shared.db")
    first = BrokerSession.connect(target)
    second = BrokerSession.connect(target)
    one = first.queue("one")
    two = second.queue("two")
    assert one.conn is not None
    assert two.conn is not None
    assert one.conn._shared_session is two.conn._shared_session
    assert one.session is first
    assert two.session is second
    first.close()
    second.close()


def test_recycle_through_one_handle_releases_another_handles_thread_core(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "shared-recycle.db")
    first = BrokerSession.connect(target)
    second = BrokerSession.connect(target)
    queue = second.queue("jobs")
    queue.write("payload")
    assert queue.conn is not None
    old_core = queue.conn.get_core()
    assert isinstance(old_core, BrokerDB)
    old_raw = cast(SQLiteRunner, old_core._runner).get_connection()

    first.recycle_thread()

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        old_raw.execute("SELECT 1")
    assert queue.conn.get_core() is not old_core
    first.close()
    second.close()


def test_closed_handle_recycle_is_noop_while_sibling_handle_is_live(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "closed-sibling-recycle.db")
    closed = BrokerSession.connect(target)
    live = BrokerSession.connect(target)
    queue = live.queue("jobs")
    queue.write("payload")
    assert queue.conn is not None

    closed.close()
    core = queue.conn.get_core()
    raw = cast(SQLiteRunner, cast(BrokerDB, core)._runner).get_connection()
    closed.recycle_thread()
    raw.execute("SELECT 1")
    assert queue.conn.get_core() is core

    live.recycle_thread()
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        raw.execute("SELECT 1")
    live.close()


def test_session_scope_closes_early_reused_queue_lease(tmp_path: Path) -> None:
    session = BrokerSession.connect(str(tmp_path / "scope.db"))
    queue = session.queue("jobs")
    queue.write("first")
    assert queue.conn is not None
    process_session = queue.conn._shared_session
    assert process_session is not None
    queue.close()
    queue.write("second")
    assert not queue.conn._shared_released

    session.close()

    assert queue.conn._shared_released
    assert process_session._closed
    assert queue.session is session
    assert queue.read_many(2) == ["first", "second"]
    queue.close()


def test_retained_queue_reuse_starts_a_new_process_session_and_runner(
    tmp_path: Path,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "retained.db"))
    queue = session.queue("jobs")
    queue.write("payload")
    assert queue.conn is not None
    original_session = queue.conn._shared_session
    original_core = queue.conn.get_core()
    assert original_session is not None
    assert isinstance(original_core, BrokerDB)
    original_runner = original_core._runner

    session.close()
    assert original_session._closed

    assert queue.read_one() == "payload"
    replacement_session = queue.conn._shared_session
    replacement_core = queue.conn.get_core()
    assert replacement_session is not None
    assert replacement_session is not original_session
    assert replacement_core is not original_core
    assert isinstance(replacement_core, BrokerDB)
    assert replacement_core._runner is not original_runner
    queue.close()


def test_early_close_reuse_releases_each_queue_lease_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from simplebroker import _broker_session
    from simplebroker import db as db_module

    releases = 0
    real_release = _broker_session.release_process_broker_session

    def counted_release(key: Any) -> None:
        nonlocal releases
        releases += 1
        real_release(key)

    monkeypatch.setattr(
        _broker_session, "release_process_broker_session", counted_release
    )
    monkeypatch.setattr(db_module, "release_process_broker_session", counted_release)
    session = BrokerSession.connect(str(tmp_path / "leases.db"))
    queue = session.queue("jobs")
    queue.write("first")
    queue.close()
    assert releases == 1

    queue.write("second")
    session.close()

    assert releases == 3


def test_worker_context_recycles_its_thread_cache(tmp_path: Path) -> None:
    target = str(tmp_path / "worker.db")
    anchor = Queue("anchor", db_path=target, persistent=True)
    observed: dict[str, Any] = {}

    def worker() -> None:
        with BrokerSession.connect(target) as session:
            queue = session.queue("jobs")
            queue.write("payload")
            assert queue.conn is not None
            process_session = queue.conn._shared_session
            assert process_session is not None
            core = queue.conn.get_core()
            observed["session"] = process_session
            observed["core"] = core

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert observed["core"] not in observed["session"]._cores
    anchor.close()


def test_worker_contexts_leave_no_thread_cores_even_when_body_raises(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "workers.db")
    anchor = Queue("anchor", db_path=target, persistent=True)
    anchor.write("keep-session-live")
    assert anchor.conn is not None
    process_session = anchor.conn._shared_session
    assert process_session is not None
    anchor_core = anchor.conn.get_core()
    failures: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            with BrokerSession.connect(target) as session:
                session.queue(f"jobs-{index}").write("payload")
                if index == 0:
                    raise ValueError("task failed")
        except ValueError as failure:
            failures.append(failure)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert all(not thread.is_alive() for thread in threads)
    assert len(failures) == 1
    assert process_session._cores == {anchor_core}
    anchor.close()


def test_recycle_thread_closes_sqlite_core_and_queue_reacquires(tmp_path: Path) -> None:
    session = BrokerSession.connect(str(tmp_path / "recycle.db"))
    queue = session.queue("jobs")
    queue.write("payload")
    assert queue.conn is not None
    old_core = queue.conn.get_core()
    assert isinstance(old_core, BrokerDB)
    old_raw = cast(SQLiteRunner, old_core._runner).get_connection()

    session.recycle_thread()

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        old_raw.execute("SELECT 1")
    assert queue.conn.get_core() is not old_core
    session.close()


def test_recycle_thread_is_deferred_until_open_operation_exits(tmp_path: Path) -> None:
    session = BrokerSession.connect(str(tmp_path / "deferred.db"))
    queue = session.queue("jobs")
    assert queue.conn is not None

    with queue.get_connection() as core:
        session.recycle_thread()
        assert core in session._process_session._cores

    assert core not in session._process_session._cores
    session.close()


def test_close_rejects_an_open_same_key_same_thread_operation_without_closing_scope(
    tmp_path: Path,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "open-operation.db"))
    queue = session.queue("jobs")

    with (
        queue.get_connection(),
        pytest.raises(RuntimeError, match="same process-session key"),
    ):
        session.close()

    session.queue("still-open").write("payload")
    session.close()


@pytest.mark.parametrize("operation_kind", ["queue_iterator", "session_connection"])
@pytest.mark.parametrize("body_failure_type", [None, ValueError, KeyboardInterrupt])
def test_context_exit_preserves_body_failure_when_same_key_operation_blocks_close(
    tmp_path: Path,
    operation_kind: str,
    body_failure_type: type[BaseException] | None,
) -> None:
    target = str(tmp_path / f"exit-{operation_kind}.db")
    session = BrokerSession.connect(target)
    operation: Any
    operation_owner: Queue | BrokerSession
    if operation_kind == "queue_iterator":
        queue = Queue("jobs", db_path=target, persistent=True)
        operation_owner = queue
        queue.write("payload")
        operation = queue.read(all_messages=True)
        assert next(operation) == "payload"
    else:
        sibling = BrokerSession.connect(target)
        operation_owner = sibling
        operation = sibling.connection()
        operation.__enter__()

    try:
        if body_failure_type is None:
            with (
                pytest.raises(RuntimeError, match="same process-session key"),
                session,
            ):
                pass
        else:
            body_failure = body_failure_type("body")
            with pytest.raises(body_failure_type) as raised, session:
                raise body_failure
            assert raised.value is body_failure
            assert str(raised.value) == "body"
            assert any(
                "same process-session key" in note
                for note in getattr(raised.value, "__notes__", ())
            )
    finally:
        if operation_kind == "queue_iterator":
            operation.close()
        else:
            operation.__exit__(None, None, None)
        session.close()
        operation_owner.close()


def test_foreign_thread_close_does_not_recycle_worker_cache(tmp_path: Path) -> None:
    target = str(tmp_path / "foreign-close.db")
    anchor = Queue("anchor", db_path=target, persistent=True)
    anchor.write("keep-session-live")
    assert anchor.conn is not None
    process_session = anchor.conn._shared_session
    assert process_session is not None
    ready = threading.Event()
    finish = threading.Event()
    state: dict[str, Any] = {}

    def worker() -> None:
        session = BrokerSession.connect(target)
        queue = session.queue("jobs")
        queue.write("payload")
        assert queue.conn is not None
        state.update(session=session, core=queue.conn.get_core())
        ready.set()
        assert finish.wait(5.0)

    thread = threading.Thread(target=worker)
    thread.start()
    assert ready.wait(5.0)
    state["session"].close()

    assert state["core"] in process_session._cores
    finish.set()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    anchor.close()


def test_connection_uses_the_shared_session(tmp_path: Path) -> None:
    session = BrokerSession.connect(str(tmp_path / "connection.db"))
    queue = session.queue("jobs")
    queue.write("payload")
    assert queue.conn is not None
    process_session = queue.conn._shared_session
    assert process_session is not None

    with session.connection() as connection:
        assert "jobs" in connection.list_queues()
        assert len(process_session._cores) == 1

    session.close()


def test_connection_lease_keeps_process_session_live_during_concurrent_close(
    tmp_path: Path,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "connection-close.db"))
    process_session = session._process_session
    connection_open = threading.Event()
    allow_connection_close = threading.Event()

    def hold_connection() -> None:
        with session.connection():
            connection_open.set()
            assert allow_connection_close.wait(5.0)

    thread = threading.Thread(target=hold_connection)
    thread.start()
    assert connection_open.wait(5.0)

    session.close()
    assert session._released
    assert not process_session._closed

    allow_connection_close.set()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert process_session._closed


def test_only_session_minted_queues_name_a_session(tmp_path: Path) -> None:
    direct = Queue("direct", db_path=str(tmp_path / "direct.db"), persistent=True)
    ephemeral = Queue("ephemeral", db_path=str(tmp_path / "ephemeral.db"))
    session = BrokerSession.connect(str(tmp_path / "session.db"))
    minted = session.queue("minted")

    assert direct.session is None
    assert ephemeral.session is None
    assert minted.session is session

    direct.close()
    ephemeral.close()
    session.close()


def test_injected_runner_queue_has_no_session(tmp_path: Path) -> None:
    runner = SQLiteRunner(str(tmp_path / "injected.db"))
    queue = Queue("injected", runner=runner)

    assert queue.session is None

    queue.close()
    runner.close()


def test_session_descriptors_report_detached_identity(tmp_path: Path) -> None:
    target = BrokerTarget(
        backend_name="sqlite",
        target=str(tmp_path / "descriptors.db"),
        backend_options={"outer": {"value": "bound"}},
    )
    config = resolve_config(override={"BROKER_BUSY_TIMEOUT": 1234})
    session = BrokerSession.connect(target, config=config)

    first_target = cast(BrokerTarget, session.target)
    first_target.backend_options["outer"]["value"] = "mutated"
    first_target.backend_options["added"] = True
    second_target = cast(BrokerTarget, session.target)

    assert second_target is not first_target
    assert second_target.target == str((tmp_path / "descriptors.db").resolve())
    assert second_target.backend_options == {"outer": {"value": "bound"}}
    assert session.backend_name == "sqlite"
    assert session.config is config
    assert session.config["BUSY_TIMEOUT"] == 1234
    assert session.config.prefix == config.prefix

    session.close()


def test_closed_session_rejects_admission_with_recovery_action(tmp_path: Path) -> None:
    session = BrokerSession.connect(str(tmp_path / "closed.db"))
    session.close()

    for action in (
        lambda: session.queue("jobs"),
        lambda: session.__enter__(),
    ):
        with pytest.raises(RuntimeError, match=r"BrokerSession\.connect"):
            action()
    with (
        pytest.raises(RuntimeError, match=r"BrokerSession\.connect"),
        session.connection(),
    ):
        pass
    session.recycle_thread()
    session.close()


def test_dropped_handle_releases_only_its_lease(tmp_path: Path) -> None:
    anchor = Queue("anchor", db_path=str(tmp_path / "finalizer.db"), persistent=True)
    anchor.write("payload")
    assert anchor.conn is not None
    process_session = anchor.conn._shared_session
    assert process_session is not None
    core = anchor.conn.get_core()
    session = BrokerSession.connect(anchor.db_target)
    minted = session.queue("minted")
    reference = weakref.ref(session)

    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        del session
        assert reference() is None
    finally:
        if gc_was_enabled:
            gc.enable()

    assert minted.session is None
    assert core in process_session._cores
    minted.close()
    anchor.close()


def test_close_on_thread_without_cache_builds_no_core(tmp_path: Path) -> None:
    session = BrokerSession.connect(str(tmp_path / "unused.db"))
    assert not session._process_session._cores

    thread = threading.Thread(target=session.close)
    thread.start()
    thread.join(timeout=5.0)

    assert not thread.is_alive()
    assert not session._process_session._cores


def test_queue_admission_racing_close_is_included_in_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import simplebroker.session as session_module

    session = BrokerSession.connect(str(tmp_path / "admission.db"))
    constructor_entered = threading.Event()
    finish_constructor = threading.Event()
    real_queue = session_module.Queue
    result: dict[str, Queue] = {}

    def delayed_queue(*args: Any, **kwargs: Any) -> Queue:
        constructor_entered.set()
        assert finish_constructor.wait(5.0)
        return real_queue(*args, **kwargs)

    monkeypatch.setattr(session_module, "Queue", delayed_queue)

    admitting = threading.Thread(
        target=lambda: result.setdefault("queue", session.queue("jobs"))
    )
    admitting.start()
    assert constructor_entered.wait(5.0)
    closing = threading.Thread(target=session.close)
    closing.start()
    finish_constructor.set()
    admitting.join(timeout=5.0)
    closing.join(timeout=5.0)

    assert not admitting.is_alive()
    assert not closing.is_alive()
    queue = result["queue"]
    assert queue.conn is not None
    assert queue.conn._shared_released


def test_handle_lock_is_free_while_queue_close_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "close-lock.db"))
    queue = session.queue("jobs")
    close_entered = threading.Event()
    finish_close = threading.Event()
    real_close = queue.close

    def delayed_close() -> None:
        close_entered.set()
        assert finish_close.wait(5.0)
        real_close()

    monkeypatch.setattr(queue, "close", delayed_close)
    closing = threading.Thread(target=session.close)
    closing.start()
    assert close_entered.wait(5.0)

    with pytest.raises(RuntimeError, match="Create a new session"):
        session.queue("late")

    finish_close.set()
    closing.join(timeout=5.0)
    assert not closing.is_alive()


def test_close_attempts_remaining_steps_after_ordinary_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "close-failure.db"))
    queue = session.queue("jobs")
    queue.write("payload")
    assert queue.conn is not None
    real_shutdown = BrokerDB.shutdown
    shutdown_calls = 0

    def fail_first_shutdown(core: BrokerDB) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        if shutdown_calls == 1:
            raise RuntimeError("recycle failed")
        real_shutdown(core)

    monkeypatch.setattr(BrokerDB, "shutdown", fail_first_shutdown)
    with pytest.raises(RuntimeError, match="recycle failed"):
        session.close()

    assert queue.conn._shared_released
    assert session._released
    assert shutdown_calls == 2


def test_close_retains_first_ordinary_failure_and_notes_later_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "close-failures.db"))
    first_queue = session.queue("first")
    second_queue = session.queue("second")
    second_queue.write("payload")
    real_first_close = first_queue.close
    real_shutdown = BrokerDB.shutdown
    shutdown_calls = 0

    def fail_first_shutdown(core: BrokerDB) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        if shutdown_calls == 1:
            raise RuntimeError("recycle failed")
        real_shutdown(core)

    def fail_queue_close() -> None:
        raise ValueError("queue close failed")

    monkeypatch.setattr(BrokerDB, "shutdown", fail_first_shutdown)
    monkeypatch.setattr(first_queue, "close", fail_queue_close)
    with pytest.raises(RuntimeError, match="recycle failed") as raised:
        session.close()

    assert any("queue close failed" in note for note in raised.value.__notes__)
    assert session._released
    assert second_queue.conn is not None
    assert second_queue.conn._shared_released
    real_first_close()
    assert shutdown_calls == 2


def test_interrupted_close_keeps_admission_closed_and_later_close_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "close-interrupt.db"))
    queue = session.queue("jobs")
    queue.write("payload")
    real_recycle = session._process_session.cleanup_current_thread

    class CloseInterrupted(BaseException):
        pass

    def interrupt() -> None:
        raise CloseInterrupted

    monkeypatch.setattr(session._process_session, "cleanup_current_thread", interrupt)
    with pytest.raises(CloseInterrupted):
        session.close()
    with pytest.raises(RuntimeError, match="Create a new session"):
        session.queue("late")
    assert not session._released

    monkeypatch.setattr(
        session._process_session, "cleanup_current_thread", real_recycle
    )
    session.close()
    assert session._released


def test_dropped_interrupted_handle_releases_its_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = BrokerSession.connect(str(tmp_path / "interrupt-drop.db"))
    with session.connection():
        pass
    process_session = session._process_session

    class CloseInterrupted(BaseException):
        pass

    def interrupt() -> None:
        raise CloseInterrupted

    monkeypatch.setattr(process_session, "cleanup_current_thread", interrupt)
    reference = weakref.ref(session)
    with pytest.raises(CloseInterrupted):
        session.close()
    del session
    gc.collect()

    assert reference() is None
    assert process_session._closed


def test_interruption_after_registry_release_marks_handle_released_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from simplebroker import _broker_session

    session = BrokerSession.connect(str(tmp_path / "release-interrupt.db"))
    process_session = session._process_session
    real_release = _broker_session.release_process_broker_session
    release_calls = 0

    class CloseInterrupted(BaseException):
        pass

    def release_then_interrupt(key: Any) -> None:
        nonlocal release_calls
        release_calls += 1
        real_release(key)
        raise CloseInterrupted

    monkeypatch.setattr(
        _broker_session, "release_process_broker_session", release_then_interrupt
    )
    with pytest.raises(CloseInterrupted):
        session.close()
    assert session._released
    assert not session._finalizer.alive
    assert process_session._closed

    session.close()
    reference = weakref.ref(session)
    del session
    gc.collect()
    assert reference() is None
    assert release_calls == 1

    monkeypatch.setattr(_broker_session, "release_process_broker_session", real_release)
    fresh = BrokerSession.connect(str(tmp_path / "release-interrupt.db"))
    assert fresh._process_session is not process_session
    fresh.close()


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
def test_inherited_handle_rejects_use_and_close_is_silent(tmp_path: Path) -> None:
    session = BrokerSession.connect(str(tmp_path / "fork.db"))
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_handle_lock() -> None:
        with session._lock:
            lock_held.set()
            assert release_lock.wait(5.0)

    holder = threading.Thread(target=hold_handle_lock)
    holder.start()
    assert lock_held.wait(5.0)
    read_fd, write_fd = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(read_fd)
        child_status = b"failed"
        try:
            actions = (
                lambda: session.queue("child"),
                session.recycle_thread,
                session.__enter__,
            )
            for action in actions:
                with pytest.raises(RuntimeError, match="Create a new session"):
                    action()
            with (
                pytest.raises(RuntimeError, match="Create a new session"),
                session.connection(),
            ):
                pass
            session.close()
            fresh = BrokerSession.connect(str(tmp_path / "fork.db"))
            assert fresh._process_session is not session._process_session
            fresh.close()
            child_status = b"passed"
        finally:
            os.write(write_fd, child_status)
            os._exit(0)

    os.close(write_fd)
    readable, _, _ = select.select([read_fd], [], [], 5.0)
    assert readable
    assert os.read(read_fd, 32) == b"passed"
    _, wait_status = os.waitpid(child, 0)
    assert os.WIFEXITED(wait_status) and os.WEXITSTATUS(wait_status) == 0
    release_lock.set()
    holder.join(timeout=5.0)
    assert not holder.is_alive()
    session.close()
