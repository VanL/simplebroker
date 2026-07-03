"""Test fork safety for BrokerDB."""

import multiprocessing
import os
import sys
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB


def _worker_write(db_path: str, worker_id: int, count: int):
    """Worker that writes messages (module-level for pickling)."""
    # Each worker creates its own BrokerDB instance
    with BrokerDB(db_path) as db:
        for i in range(count):
            db.write("test_queue", f"worker{worker_id}_msg{i}")


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_safety_protection(workdir: Path):
    """Test that BrokerDB detects and prevents usage after fork."""
    db_path = workdir / "test.db"

    # Create DB in parent process
    db = BrokerDB(str(db_path))
    db.write("test_queue", "message1")

    # Fork a child process
    pid = os.fork()

    if pid == 0:  # Child process
        # Try to use the parent's DB instance - should fail
        try:
            db.write("test_queue", "message2")
            # If we get here, the test failed
            os._exit(1)
        except RuntimeError as e:
            # Expected error
            assert "forked process" in str(e)
            os._exit(0)
        except Exception:
            # Unexpected error
            os._exit(2)
    else:  # Parent process
        # Wait for child
        _, status = os.waitpid(pid, 0)
        exit_code = os.WEXITSTATUS(status)
        assert exit_code == 0  # Child exited successfully with expected error

        # Parent can still use the DB
        db.write("test_queue", "message3")
        messages = list(db.peek_generator("test_queue", with_timestamps=False))
        assert len(messages) == 2
        assert "message1" in messages
        assert "message3" in messages

        db.close()


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_new_instance_after_fork_works(workdir: Path):
    """Test that creating a new BrokerDB instance after fork works fine."""
    db_path = workdir / "test.db"

    # Create DB and write in parent
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "parent_message")

    pid = os.fork()

    if pid == 0:  # Child process
        # Create new instance in child - should work
        try:
            with BrokerDB(str(db_path)) as child_db:
                child_db.write("test_queue", "child_message")
                messages = list(
                    child_db.peek_generator("test_queue", with_timestamps=False)
                )
                assert len(messages) == 2
                assert "parent_message" in messages
                assert "child_message" in messages
            os._exit(0)
        except Exception:
            os._exit(1)
    else:  # Parent process
        # Wait for child
        _, status = os.waitpid(pid, 0)
        exit_code = os.WEXITSTATUS(status)
        assert exit_code == 0

        # Verify both messages exist
        with BrokerDB(str(db_path)) as db:
            messages = list(db.peek_generator("test_queue", with_timestamps=False))
            assert len(messages) == 2


def test_multiprocessing_with_separate_instances(workdir: Path):
    """Test that multiprocessing works when each process creates its own instance."""
    db_path = workdir / "test.db"

    # Start multiple workers
    processes = []
    try:
        for i in range(3):
            p = multiprocessing.Process(target=_worker_write, args=(str(db_path), i, 5))
            p.start()
            processes.append(p)

        # Wait for all to complete
        for p in processes:
            p.join(timeout=10)  # Add timeout to prevent hanging
    finally:
        # Ensure all processes are terminated even if test fails
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)

    # Verify all messages were written
    with BrokerDB(str(db_path)) as db:
        messages = list(db.peek_generator("test_queue", with_timestamps=False))
        assert len(messages) == 15  # 3 workers * 5 messages each

        # Verify all workers contributed
        for worker_id in range(3):
            worker_messages = [m for m in messages if f"worker{worker_id}_" in m]
            assert len(worker_messages) == 5


# --------------------------------------------------------------------------- #
# Task 4: fork guards on the five previously-unguarded entry points, and the
# abandon-never-close fork fallback in SQLiteRunner.
# --------------------------------------------------------------------------- #


def _invoke_guarded_method(core: BrokerDB, method: str) -> None:
    """Call one guarded method on a (possibly inherited) core."""
    if method == "generate_timestamp":
        core.generate_timestamp()
    elif method == "get_cached_last_timestamp":
        core.get_cached_last_timestamp()
    elif method == "refresh_last_timestamp":
        core.refresh_last_timestamp()
    elif method == "sidecar":
        with core.sidecar():
            pass
    elif method == "get_data_version":
        core.get_data_version()
    else:  # pragma: no cover - guard against typos
        raise AssertionError(f"unknown method {method!r}")


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize(
    "method",
    [
        "generate_timestamp",
        "get_cached_last_timestamp",
        "refresh_last_timestamp",
        "sidecar",
        "get_data_version",
    ],
)
def test_forked_child_guarded_methods_raise(workdir: Path, method: str) -> None:
    """Each previously-unguarded method must raise in a forked child.

    The child calls the method on the inherited core; the guard must raise
    RuntimeError carrying the fork-safety message. FAILS today for these five.
    """
    db_path = workdir / "test.db"
    core = BrokerDB(str(db_path))
    # Bind a real connection pre-fork so the child inherits it.
    core.write("q", "seed")
    _invoke_guarded_method(core, method)  # exercise once pre-fork (parent)

    pid = os.fork()
    if pid == 0:  # child
        try:
            _invoke_guarded_method(core, method)
            os._exit(1)  # no guard fired
        except RuntimeError as exc:
            os._exit(0 if "forked process" in str(exc) else 3)
        except BaseException:
            os._exit(2)  # wrong exception type
    else:
        _, status = os.waitpid(pid, 0)
        assert os.WEXITSTATUS(status) == 0
        core.close()


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_forked_child_queue_generate_timestamp_raises(workdir: Path) -> None:
    """The public Queue path must also hit the guard on an inherited core.

    Create a persistent Queue in the PARENT with a bound connection, exercise
    it once pre-fork, then fork and call generate_timestamp() on the inherited
    object in the child (a default per-call Queue would build a fresh child-side
    connection and never hit the guard). FAILS today.
    """
    from simplebroker.sbqueue import Queue

    db_path = workdir / "test.db"
    queue = Queue("q", persistent=True, db_path=str(db_path))
    # Bind the persistent connection/core pre-fork.
    queue.generate_timestamp()

    read_conn, write_conn = os.pipe()

    pid = os.fork()
    if pid == 0:  # child
        os.close(read_conn)
        try:
            queue.generate_timestamp()
            os.write(write_conn, b"no-guard")
            os._exit(1)
        except RuntimeError as exc:
            os.write(write_conn, b"runtime" if "forked process" in str(exc) else b"x")
            os._exit(0)
        except BaseException:
            os.write(write_conn, b"other")
            os._exit(2)
    else:
        os.close(write_conn)
        payload = os.read(read_conn, 64)
        os.close(read_conn)
        _, status = os.waitpid(pid, 0)
        queue.close()
        assert os.WEXITSTATUS(status) == 0
        assert payload == b"runtime"


def _abandon_fork_child(runner: object, result_w: object) -> None:
    """Child target for the abandon-never-close test (top-level, picklable)."""
    import sqlite3 as _sqlite3

    import simplebroker._runner as _runner_module

    # Snapshot inherited connections BEFORE triggering the fallback.
    inherited = list(runner._all_connections)  # type: ignore[attr-defined]
    try:
        # Trigger the pid-check fallback by using the inherited runner.
        new_conn = runner.get_connection()  # type: ignore[attr-defined]

        abandoned = _runner_module._ABANDONED_FORK_CONNECTIONS
        # All inherited connections abandoned exactly once (identity-deduped).
        all_present = all(any(c is a for a in abandoned) for c in inherited)
        counts_ok = all(sum(1 for a in abandoned if a is c) == 1 for c in inherited)
        # New connection is live and distinct from every inherited one.
        new_is_distinct = all(new_conn is not c for c in inherited)
        # Abandoned (not closed): reading total_changes must NOT raise.
        not_closed = True
        for c in inherited:
            try:
                _ = c.total_changes
            except _sqlite3.ProgrammingError:
                not_closed = False
        result_w.send(  # type: ignore[attr-defined]
            {
                "inherited_count": len(inherited),
                "all_present": all_present,
                "counts_ok": counts_ok,
                "new_is_distinct": new_is_distinct,
                "not_closed": not_closed,
                "new_works": new_conn is not None,
            }
        )
    except BaseException as exc:  # pragma: no cover - unexpected
        result_w.send({"error": repr(exc)})  # type: ignore[attr-defined]


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_fallback_abandons_without_close(workdir: Path) -> None:
    """The fork fallback abandons (never closes) ALL inherited connections.

    Pin the abandon-ALL requirement: before forking, create a SECOND tracked
    connection from another thread so _all_connections holds two inherited
    connections. The child triggers the fallback and asserts BOTH objects are
    present in _ABANDONED_FORK_CONNECTIONS exactly once and neither raises on
    total_changes (i.e., neither was closed).
    """
    import threading as _threading

    from simplebroker._runner import SQLiteRunner

    db_path = workdir / "test.db"
    runner = SQLiteRunner(str(db_path))
    # Bind the current thread's connection.
    runner.get_connection()

    # Bind a SECOND connection from another thread -> two tracked connections.
    def _bind_second() -> None:
        runner.get_connection()

    t = _threading.Thread(target=_bind_second)
    t.start()
    t.join()
    assert len(runner._all_connections) == 2

    read_r, write_w = multiprocessing.Pipe(duplex=False)

    pid = os.fork()
    if pid == 0:  # child
        try:
            _abandon_fork_child(runner, write_w)
        finally:
            os._exit(0)
    else:
        result = read_r.recv()
        os.waitpid(pid, 0)
        runner.close()
        assert "error" not in result, result
        assert result["inherited_count"] == 2, result
        assert result["all_present"] is True, result
        assert result["counts_ok"] is True, result
        assert result["new_is_distinct"] is True, result
        assert result["not_closed"] is True, result
        assert result["new_works"] is True, result


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fork_recovery_does_not_block_on_inherited_locks(workdir: Path) -> None:
    """Post-fork recovery must not acquire locks inherited from parent threads.

    If a parent thread holds a runner-level lock (e.g. _connections_lock) at
    the moment of fork(), the child has no thread that can ever release it —
    so the recovery branch in get_connection() must not acquire inherited
    locks. Parent: a thread acquires runner._connections_lock and holds it
    across the fork point. Child: get_connection() must complete within a
    bounded deadline and exit 0. Pre-fix this HANGS (watchdog kills the child
    and the test fails); post-fix it passes.
    """
    import signal
    import threading
    import time

    from simplebroker._runner import SQLiteRunner

    db_path = workdir / "test.db"
    runner = SQLiteRunner(str(db_path))
    # Bind the parent connection so the child inherits state to recover from.
    runner.get_connection()

    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def _hold_connections_lock() -> None:
        with runner._connections_lock:
            lock_acquired.set()
            release_lock.wait(30.0)

    holder = threading.Thread(target=_hold_connections_lock)
    holder.start()
    assert lock_acquired.wait(5.0), "holder thread never acquired the lock"

    # Fork while the lock is held. POSIX fork keeps only the calling thread,
    # so in the child the lock is held forever by a thread that doesn't exist.
    pid = os.fork()
    if pid == 0:  # child
        try:
            # Routes through the fork-recovery branch; must not block on the
            # inherited (permanently held) lock.
            runner.get_connection()
            os._exit(0)
        except BaseException:
            os._exit(1)
    else:
        try:
            deadline = time.monotonic() + 10.0
            status: int | None = None
            while time.monotonic() < deadline:
                wpid, wstatus = os.waitpid(pid, os.WNOHANG)
                if wpid == pid:
                    status = wstatus
                    break
                time.sleep(0.05)
            if status is None:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                pytest.fail(
                    "child hung in fork recovery: it blocked on a lock "
                    "inherited from a parent thread"
                )
            assert os.WEXITSTATUS(status) == 0
        finally:
            release_lock.set()
            holder.join(timeout=5.0)
            runner.close()
