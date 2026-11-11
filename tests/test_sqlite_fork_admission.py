"""Real-process SQLite lifetime and fork admission regressions [SB-API-11]."""

from __future__ import annotations

import gc
import json
import multiprocessing
import os
import sqlite3
import subprocess
import sys
import threading
import weakref
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker import Queue
from simplebroker._runner import SetupPhase, SQLiteRunner
from tests.helpers.state_machine_contracts import TransitionCase, fires_transition_table


def _write_child(path: str, pipe: Any) -> None:
    acknowledged = 0
    error = None
    queue = Queue("child", db_path=path, persistent=True)
    try:
        try:
            queue.write("child-0")
            acknowledged += 1
        except RuntimeError as exc:
            error = str(exc)
        pipe.send("first-attempt-complete")
        assert pipe.recv() == "parent-closed"
        if error is None:
            for index in range(1, 20):
                queue.write(f"child-{index}")
                acknowledged += 1
    finally:
        queue.close()
    pipe.send({"acknowledged": acknowledged, "error": error})
    pipe.close()


def _parent_close_probe(root: Path) -> dict[str, Any]:
    path = str(root / "fork.db")
    parent = Queue("parent", db_path=path, persistent=True)
    parent.write("parent-survives")
    context = multiprocessing.get_context("fork")
    reader, sender = context.Pipe()
    child = context.Process(target=_write_child, args=(path, sender))
    try:
        child.start()
        sender.close()
        assert reader.poll(10), "child did not finish its first write attempt"
        assert reader.recv() == "first-attempt-complete"
        parent.close()
        reader.send("parent-closed")
        assert reader.poll(10), "child did not report its remaining writes"
        report: dict[str, Any] = reader.recv()
        child.join(10)
        assert child.exitcode == 0
    finally:
        parent.close()
        reader.close()
        sender.close()
        if child.is_alive():
            child.kill()
            child.join(5)
        child.close()
    with Queue("parent", db_path=path) as observer:
        report["parent"] = observer.peek_many()
    with Queue("child", db_path=path) as observer:
        report["surviving_child_writes"] = len(observer.peek_many())
    return report


def _runner_action(runner, action):
    if action == "get_connection":
        runner.get_connection()
    elif action == "run":
        runner.run("SELECT 1", fetch=True)
    elif action == "owned_connection":
        with runner.owned_connection("fork-probe"):
            pass
    elif action == "_settings_connection":
        with runner._settings_connection():
            pass
    elif action in {"begin_immediate", "commit", "rollback"}:
        getattr(runner, action)()
    elif action == "setup":
        runner.setup(SetupPhase.CONNECTION)
    elif action == "setup_stop":
        runner.setup_with_stop_event(SetupPhase.CONNECTION, None)
    elif action == "setup_conditional":
        runner.run_exclusive_setup_conditionally_with_stop_event(
            SetupPhase.CONNECTION,
            lambda: None,
            None,
            completion_is_valid=lambda: False,
        )
    else:
        raise AssertionError(action)


def _action_child(path, action, pipe, runner):
    result: dict[str, Any] = {"error": None}
    try:
        if action == "nested":
            result = _one_child(path, "write")
        elif action == "memory":
            local = SQLiteRunner(":memory:")
            try:
                local.run("SELECT 1", fetch=True)
            finally:
                local.close()
        elif action == "injected":
            local = SQLiteRunner(path)
            try:
                with Queue("child", db_path=path, runner=local) as queue:
                    queue.write("child")
            finally:
                local.close()
        elif action == "cleanup":
            runner.close()
            runner.is_setup_complete(SetupPhase.CONNECTION)
            result["error"] = _one_child(path, "write")["error"]
        elif runner is not None:
            _runner_action(runner, action)
        else:
            with Queue("child", db_path=path) as queue:
                queue.write("child")
                result["value"] = queue.read_one()
    except RuntimeError as exc:
        result["error"] = str(exc)
    pipe.send(result)
    pipe.close()


def _one_child(path, action="write", runner=None, method="fork"):
    context = multiprocessing.get_context(method)
    receiver, sender = context.Pipe(duplex=False)
    child = context.Process(target=_action_child, args=(path, action, sender, runner))
    try:
        child.start()
        sender.close()
        assert receiver.poll(10), f"{action} child did not report"
        result = receiver.recv()
        child.join(10)
        assert child.exitcode == 0, child.exitcode
        return result
    finally:
        receiver.close()
        sender.close()
        if child.is_alive():
            child.kill()
            child.join(5)
        child.close()


def _unsafe_probe(root, action):
    with Queue("parent", db_path=str(root / "parent.db"), persistent=True) as parent:
        parent.write("before")
        result = _one_child(str(root / "unrelated.db"), action)
        parent.write("after")
        result["parent"] = parent.read_many(3)
        return result


def _clean_probe(root, mode):
    path = str(root / "parent.db")
    if mode == "never-used":
        runner = SQLiteRunner(path)
    elif mode == "ephemeral":
        with Queue("parent", db_path=path) as queue:
            queue.write("parent")
    elif mode == "cantopen":
        from simplebroker._sqlite_fork import connect

        for failed_path in [root / "absent" / "queue.db", root]:
            try:
                connect(str(failed_path))
            except sqlite3.OperationalError as exc:
                assert exc.sqlite_errorcode & 0xFF == sqlite3.SQLITE_CANTOPEN
            else:
                raise AssertionError("native open unexpectedly succeeded")
        gc.collect()
    else:
        runner = SQLiteRunner(path)
        connection = runner.get_connection()
        reference = weakref.ref(connection)
        if mode in {"cycle", "foreign-gc"}:
            cast(Any, connection).cycle = connection
        if mode == "explicit":
            runner.close()
            connection.close()
            connection.close()
        del connection, runner
        if mode == "foreign-gc":
            collector = threading.Thread(target=gc.collect)
            collector.start()
            collector.join(5)
            assert not collector.is_alive()
        else:
            gc.collect()
        assert reference() is None, "creator GC did not release its connection"
    return _one_child(str(root / "child.db"))


def _held_lock_probe(root, kind, action):
    from simplebroker import _sqlite_fork

    runner = SQLiteRunner(str(root / "parent.db"))
    runner.get_connection()
    held = _sqlite_fork._lock if kind == "registry" else getattr(runner, kind)
    ready, release = threading.Event(), threading.Event()

    def hold():
        with held:
            ready.set()
            assert release.wait(15)

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert ready.wait(5)
        result = _one_child(str(root / "child.db"), action, runner)
    finally:
        release.set()
        thread.join(5)
        runner.close()
    assert not thread.is_alive()
    return result


def _temporary_probe(root, route):
    from simplebroker._backends.sqlite import runtime, validation
    from simplebroker._constants import resolve_config

    path = str(root / "parent.db")
    with Queue("seed", db_path=path) as seed:
        seed.write("seed")
    ready, release = threading.Event(), threading.Event()
    actual_connect = sqlite3.connect
    errors = []
    worker_id = None
    if route == "opening":

        def audit(event, args):
            if event == "sqlite3.connect" and threading.get_ident() == worker_id:
                ready.set()
                assert release.wait(15)

        sys.addaudithook(audit)
    else:

        def paused_connect(*args, **kwargs):
            connection = actual_connect(*args, **kwargs)
            ready.set()
            assert release.wait(15)
            return connection

        sqlite3.connect = paused_connect

    def worker():
        nonlocal worker_id
        worker_id = threading.get_ident()
        try:
            if route == "validation":
                validation.validate_database(Path(path))
            elif route == "setup":
                runtime.setup_connection_phase(path, config=resolve_config())
            elif route == "version":
                runtime.check_version()
            else:
                runner = SQLiteRunner(path)
                try:
                    runner.get_connection()
                finally:
                    runner.close()
        except (sqlite3.Error, RuntimeError, AssertionError, OSError) as exc:
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert ready.wait(5), errors
        result = _one_child(str(root / "child.db"))
    finally:
        release.set()
        thread.join(10)
        sqlite3.connect = actual_connect
    assert not thread.is_alive()
    assert not errors, errors
    result["after_close"] = _one_child(str(root / "child.db"))
    return result


def _gc_close_probe(root):
    from simplebroker import _sqlite_fork

    ready, release = threading.Event(), threading.Event()
    actual_retire = _sqlite_fork._Lifetime.retire

    def paused_retire(lifetime):
        ready.set()
        assert release.wait(15)
        actual_retire(lifetime)

    runner = SQLiteRunner(str(root / "parent.db"))
    connection = runner.get_connection()
    cast(Any, connection).cycle = connection
    del connection, runner
    _sqlite_fork._Lifetime.retire = paused_retire  # type: ignore[method-assign]
    collector = threading.Thread(target=gc.collect)
    collector.start()
    try:
        assert ready.wait(5)
        result = _one_child(str(root / "child.db"))
    finally:
        release.set()
        collector.join(10)
        _sqlite_fork._Lifetime.retire = actual_retire  # type: ignore[method-assign]
    assert not collector.is_alive()
    result["after_close"] = _one_child(str(root / "child.db"))
    return result


def _start_method_probe(root, method):
    if method == "preloaded-forkserver":
        preload = root / "sqlite_fork_preload.py"
        preload.write_text(
            "from simplebroker._runner import SQLiteRunner\n"
            "runner = SQLiteRunner(':memory:')\nrunner.get_connection()\n"
        )
        sys.path.insert(0, str(root))
        os.environ["PYTHONPATH"] = (
            str(root) + os.pathsep + os.environ.get("PYTHONPATH", "")
        )
        multiprocessing.set_forkserver_preload([preload.stem])
        method = "forkserver"
    with Queue("parent", db_path=str(root / "parent.db"), persistent=True) as parent:
        parent.write("parent")
        return _one_child(str(root / "child.db"), method=method)


def _failed_close_probe(root):
    from simplebroker._sqlite_fork import _ManagedConnection

    # Real driver thread-affinity rejection supplies a close failure without
    # replacing the accounting owner. The normal factory disables this driver
    # restriction; other native close errors must preserve the same invariant.
    connection = _ManagedConnection(str(root / "close.db"), check_same_thread=True)
    errors = []

    def wrong_thread():
        try:
            connection.close()
        except sqlite3.ProgrammingError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=wrong_thread)
    thread.start()
    thread.join(5)
    assert not thread.is_alive() and len(errors) == 1
    result = _one_child(str(root / "child.db"))
    assert connection.execute("SELECT 1").fetchone() == (1,)
    connection.close()
    result["after_close"] = _one_child(str(root / "child.db"))
    return result


def _ambiguous_open_probe(root):
    from simplebroker._sqlite_fork import connect

    target = str(root / "audit.db")

    def reject_handle(event, args):
        if event == "sqlite3.connect/handle":
            raise RuntimeError("audit denied after native allocation")

    sys.addaudithook(reject_handle)
    try:
        connect(target)
    except RuntimeError as exc:
        assert "audit denied" in str(exc)
    else:
        raise AssertionError("audit failure did not fire")
    gc.collect()
    # The custom audit hook is outside the detection guarantee. This verifies
    # its ambiguous failure is not mistaken for the qualified CANTOPEN path.
    return _one_child(str(root / "child.db"))


def _child_gc_probe(root):
    from simplebroker import _sqlite_fork
    from tests.test_fork_safety import _wait_for_fork_child

    connection = _sqlite_fork.connect(str(root / "parent.db"))
    cast(Any, connection).cycle = connection
    identity = id(connection)
    read_fd, write_fd = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(read_fd)
        try:
            del connection
            gc.collect()
            retained = [
                c for c in _sqlite_fork._ABANDONED_CONNECTIONS if id(c) == identity
            ]
            assert len(retained) == 1
            assert gc.is_finalized(retained[0])
            result = _one_child(str(root / "child.db"))
            result["retained"] = len(retained)
            os.write(write_fd, json.dumps(result).encode())
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        status = _wait_for_fork_child(child)
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
        result = json.loads(os.read(read_fd, 4096))
        assert connection.execute("SELECT 1").fetchone() == (1,)
    finally:
        os.close(read_fd)
        connection.close()
    return result


def _nested_cantopen_probe(root):
    from simplebroker import _sqlite_fork

    retained: list[sqlite3.Connection] = []

    def reject_handle(event, args):
        if event == "sqlite3.connect/handle" and not retained:
            retained.append(args[0])
            sqlite3.connect(str(root / "missing" / "nested.db"))

    sys.addaudithook(reject_handle)
    try:
        _sqlite_fork.connect(str(root / "outer.db"))
    except sqlite3.OperationalError as exc:
        assert exc.sqlite_errorcode & 0xFF == sqlite3.SQLITE_CANTOPEN
    else:
        raise AssertionError("nested native failure did not fire")
    assert len(retained) == 1
    result = _one_child(str(root / "child.db"))
    result["retained"] = len(retained)
    return result


def _construction_gc_probe(root, mode):
    from simplebroker import _sqlite_fork

    retained = []

    class ResurrectConnection:
        def __init__(self, connection):
            self.connection = connection

        def __del__(self):
            retained.append(self.connection)

    def reject_handle(event, args):
        if event == "sqlite3.connect/handle":
            if mode == "resurrect":
                args[0].cycle = ResurrectConnection(args[0])
            # Avoid retaining the connection through this exception traceback.
            del args
            raise RuntimeError("audit denied after native allocation")

    actual_retire = _sqlite_fork._ConnectionFactory.retire_failed_construction

    def collect_then_retire(attempt):
        old_reference = attempt.reference
        if mode == "foreign":
            collector = threading.Thread(target=gc.collect)
            collector.start()
            collector.join(5)
            assert not collector.is_alive()
            assert attempt.finalizer_thread != threading.get_ident()
            assert old_reference() is None
        else:
            gc.collect()
            assert len(retained) == 1
            # Whether the construction-time weakref survives resurrection
            # differs by CPython version; the finalizer's fresh one must.
            assert attempt.reference() is retained[0]
        actual_retire(attempt)

    sys.addaudithook(reject_handle)
    _sqlite_fork._ConnectionFactory.retire_failed_construction = collect_then_retire  # type: ignore[method-assign]
    try:
        try:
            _sqlite_fork.connect(str(root / "outer.db"))
        except RuntimeError as exc:
            assert "audit denied" in str(exc)
        else:
            raise AssertionError("audit failure did not fire")
    finally:
        _sqlite_fork._ConnectionFactory.retire_failed_construction = actual_retire  # type: ignore[method-assign]
    return _one_child(str(root / "child.db"))


def _default_schedule_probe(root):
    context = multiprocessing.get_context()
    path = str(root / "default.db")
    parent = Queue("parent", db_path=path, persistent=True)
    parent.write("parent")
    pipes = [context.Pipe() for _ in range(3)]
    children = [
        context.Process(target=_write_child, args=(path, sender)) for _, sender in pipes
    ]
    try:
        for child in children:
            child.start()
        for receiver, sender in pipes:
            sender.close()
            assert receiver.poll(10)
            assert receiver.recv() == "first-attempt-complete"
        parent.close()
        for receiver, _ in pipes:
            receiver.send("parent-closed")
        reports = []
        for (receiver, _), child in zip(pipes, children, strict=True):
            assert receiver.poll(10)
            reports.append(receiver.recv())
            child.join(10)
            assert child.exitcode == 0
    finally:
        parent.close()
        for receiver, sender in pipes:
            receiver.close()
            sender.close()
        for child in children:
            if child.is_alive():
                child.kill()
                child.join(5)
            child.close()
    with Queue("child", db_path=path) as observer:
        retained = len(observer.peek_many())
    return {
        "method": context.get_start_method(),
        "acknowledged": sum(r["acknowledged"] for r in reports),
        "retained": retained,
    }


def _run_probe(tmp_path: Path, mode: str) -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, __file__, mode, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=40,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report: dict[str, Any] = json.loads(result.stdout)
    return report


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_fresh_child_queue_refuses_before_acknowledging_unsafe_writes(
    tmp_path: Path,
) -> None:
    # The original regression acknowledges 20 writes but retains only the first
    # when the parent closes. A new Python object must not bypass the C-library
    # hazard. Exec isolation also keeps unrelated pytest SQLite handles out.
    report = _run_probe(tmp_path, "parent-close")
    assert report["acknowledged"] == 0, report
    assert "fork" in report["error"] and "SQLite" in report["error"]
    assert report["surviving_child_writes"] == 0
    assert report["parent"] == ["parent-survives"]


SQLITE_FORK_ADMISSION_TRANSITIONS = tuple(
    TransitionCase(
        transition_id=mode.replace(":", "_").replace("-", "_").upper(),
        start_state=start,
        event=event,
        guard="real managed lifetime and explicit Python fork",
        next_state="tainted child" if refused else "clean child",
        effects="refuse before child writes; parent remains usable"
        if refused
        else "child writes and reads its own message",
        expected_result="RuntimeError" if refused else "child",
        payload=(mode, refused),
    )
    for mode, start, event, refused in (
        ("unsafe:write", "parent live", "new child Queue on unrelated target", True),
        ("unsafe:injected", "parent live", "new built-in injected runner", True),
        ("unsafe:memory", "parent live", "new memory target", True),
        ("unsafe:nested", "tainted child", "fork a grandchild", True),
        ("clean:never-used", "never opened", "fork", False),
        ("clean:ephemeral", "ephemeral operation completed", "fork", False),
        ("clean:explicit", "explicitly closed", "repeated close then fork", False),
        ("clean:gc", "dropped injected runner", "GC completes then fork", False),
        ("clean:cycle", "cyclic connection", "GC completes then fork", False),
        ("clean:foreign-gc", "cyclic connection", "foreign-thread GC then fork", False),
        ("clean:cantopen", "native open failed", "CANTOPEN then fork", False),
    )
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@fires_transition_table("SM-SQLITE-FORK-ADMISSION", SQLITE_FORK_ADMISSION_TRANSITIONS)
def test_sqlite_fork_admission_fires_transition_table(tmp_path, transition_case):
    mode, refused = transition_case.payload
    report = _run_probe(tmp_path, mode)
    if refused:
        assert "SQLite access is unsafe after fork" in report["error"]
        assert report["parent"] == ["before", "after"]
    else:
        assert report == {"error": None, "value": "child"}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@pytest.mark.parametrize(
    "route", ["runner", "validation", "setup", "version", "opening"]
)
def test_every_managed_opening_publishes_its_lifetime_before_fork(tmp_path, route):
    # Barriers wrap the real driver, or its pre-open audit boundary. Neither
    # fake connections nor simulated PIDs can establish the inheritance claim.
    report = _run_probe(tmp_path, "temporary:" + route)
    assert "SQLite access is unsafe after fork" in report["error"]
    assert report["after_close"] == {"error": None, "value": "child"}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@pytest.mark.parametrize(
    "action",
    [
        "get_connection",
        "run",
        "owned_connection",
        "_settings_connection",
        "begin_immediate",
        "commit",
        "rollback",
        "setup",
        "setup_stop",
        "setup_conditional",
        "cleanup",
    ],
)
def test_inherited_runner_entries_refuse_before_parent_locks(tmp_path, action):
    report = _run_probe(tmp_path, "held:_operation_lock:" + action)
    assert "SQLite access is unsafe after fork" in report["error"]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@pytest.mark.parametrize(
    "kind", ["registry", "_connections_lock", "_setup_lock", "_transaction_condition"]
)
def test_child_never_waits_for_inherited_accounting_or_runner_mutexes(tmp_path, kind):
    report = _run_probe(tmp_path, "held:" + kind + ":get_connection")
    assert "SQLite access is unsafe after fork" in report["error"]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_gc_keeps_lifetime_counted_until_close_retires(tmp_path):
    report = _run_probe(tmp_path, "gc-close")
    assert "SQLite access is unsafe after fork" in report["error"]
    assert report["after_close"] == {"error": None, "value": "child"}


@pytest.mark.parametrize("method", ["spawn", "forkserver", "preloaded-forkserver"])
def test_clean_process_images_and_unsafe_forkserver_preload(tmp_path, method):
    if (
        "forkserver" in method
        and "forkserver" not in multiprocessing.get_all_start_methods()
    ):
        pytest.skip("forkserver is unavailable on this platform")
    report = _run_probe(tmp_path, "method:" + method)
    if method == "preloaded-forkserver":
        assert "SQLite access is unsafe after fork" in report["error"]
    else:
        assert report == {"error": None, "value": "child"}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_failed_native_close_keeps_hazard_until_successful_retry(tmp_path):
    report = _run_probe(tmp_path, "failed-close")
    assert "SQLite access is unsafe after fork" in report["error"]
    assert report["after_close"] == {"error": None, "value": "child"}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_ambiguous_constructor_failure_is_not_classified_as_closed(tmp_path):
    report = _run_probe(tmp_path, "ambiguous-open")
    assert "SQLite access is unsafe after fork" in report["error"]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_nested_cantopen_cannot_retire_retained_outer_connection(tmp_path):
    report = _run_probe(tmp_path, "nested-cantopen")
    assert report["retained"] == 1
    assert "SQLite access is unsafe after fork" in (report["error"] or ""), report


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
@pytest.mark.parametrize("mode", ["foreign", "resurrect"])
def test_failed_construction_requires_complete_deallocation_proof(tmp_path, mode):
    report = _run_probe(tmp_path, "construction-gc:" + mode)
    assert "SQLite access is unsafe after fork" in (report["error"] or ""), report


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_child_gc_resurrects_inherited_connection_without_native_close(tmp_path):
    report = _run_probe(tmp_path, "child-gc")
    assert report["retained"] == 1
    assert "SQLite access is unsafe after fork" in report["error"]


def test_default_multiprocessing_schedule_never_acknowledges_lost_writes(tmp_path):
    report = _run_probe(tmp_path, "default-schedule")
    expected = 0 if report["method"] == "fork" else 60
    assert report["acknowledged"] == report["retained"] == expected


if __name__ == "__main__":
    mode, root = sys.argv[1], Path(sys.argv[2])
    if mode == "parent-close":
        report = _parent_close_probe(root)
    elif mode.startswith("unsafe:"):
        report = _unsafe_probe(root, mode.split(":", 1)[1])
    elif mode.startswith("clean:"):
        report = _clean_probe(root, mode.split(":", 1)[1])
    elif mode.startswith("temporary:"):
        report = _temporary_probe(root, mode.split(":", 1)[1])
    elif mode.startswith("held:"):
        _, kind, action = mode.split(":")
        report = _held_lock_probe(root, kind, action)
    elif mode == "gc-close":
        report = _gc_close_probe(root)
    elif mode == "failed-close":
        report = _failed_close_probe(root)
    elif mode == "ambiguous-open":
        report = _ambiguous_open_probe(root)
    elif mode == "nested-cantopen":
        report = _nested_cantopen_probe(root)
    elif mode.startswith("construction-gc:"):
        report = _construction_gc_probe(root, mode.split(":", 1)[1])
    elif mode == "child-gc":
        report = _child_gc_probe(root)
    elif mode == "default-schedule":
        report = _default_schedule_probe(root)
    elif mode.startswith("method:"):
        report = _start_method_probe(root, mode.split(":", 1)[1])
    else:
        raise AssertionError(f"unknown probe: {mode}")
    print(json.dumps(report))
