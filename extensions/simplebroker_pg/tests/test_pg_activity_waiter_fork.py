"""An activity waiter inherited across fork() never locks its parent's listener.

A forked child inherits the waiter and its shared listener object, but not the
listener thread, and any lock a parent thread held at the fork stays held
forever in the child. Inherited state is abandoned, never locked or finalized:
``close()`` must return without touching the inherited listener, and
``wait()`` must report the inheritance as an ``OperationalError`` so the owner
creates a fresh waiter, which the reset registry backs with a new listener.

Plan: 2026-11-10-fork-inherited-activity-waiter-plan.md at 0ac2cfc.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from typing import Any

import pytest
from simplebroker_pg.runner import (
    PostgresActivityWaiter,
    PostgresMultiQueueActivityWaiter,
)

from simplebroker import BrokerTarget, Queue
from simplebroker._backend_plugins import BackendPlugin
from simplebroker._exceptions import OperationalError
from simplebroker.ext import PollingStrategy

pytestmark = [pytest.mark.pg_only]

_CHILD_DEADLINE_SECONDS = 10.0
_WAKE_SECONDS = 5.0
_EXIT_OK, _EXIT_ERROR, _EXIT_NO_ERROR, _EXIT_WRONG_ERROR, _EXIT_NO_WAKE = range(5)

_PARENT_QUEUES = ("alpha", "beta")
# The child's replacement watches other queues, so the child's write cannot
# satisfy the parent's waiter: the parent check must see the parent's write.
_CHILD_QUEUES = ("gamma", "delta")


def _new_waiter(dsn: str, schema: str, *, multi: bool, queues: tuple[str, str]) -> Any:
    if multi:
        return PostgresMultiQueueActivityWaiter(
            dsn, schema=schema, queue_names=queues, stop_event=threading.Event()
        )
    return PostgresActivityWaiter(
        dsn, schema=schema, queue_name=queues[0], stop_event=threading.Event()
    )


def _write(dsn: str, schema: str, queue_name: str) -> None:
    with Queue(
        queue_name, db_path=BrokerTarget("postgres", dsn, {"schema": schema})
    ) as q:
        q.write("activity")


def _child(
    waiter: Any,
    strategy: PollingStrategy,
    operation: str,
    dsn: str,
    schema: str,
    multi: bool,
) -> int:
    if operation == "close":
        waiter.close()
    else:
        # The owner's wait path: the strategy waits on the inherited waiter.
        try:
            strategy.wait_for_activity(0.1)
        except OperationalError as exc:
            if "fork" not in str(exc):
                return _EXIT_WRONG_ERROR
        else:
            return _EXIT_NO_ERROR
    # The owner's recovery: a fresh waiter replaces the inherited one, which
    # the strategy closes (abandons), and the fresh one receives activity.
    fresh = _new_waiter(dsn, schema, multi=multi, queues=_CHILD_QUEUES)
    strategy.start(activity_waiter=fresh)
    _write(dsn, schema, _CHILD_QUEUES[-1] if multi else _CHILD_QUEUES[0])
    woke = fresh.wait(_WAKE_SECONDS)
    strategy.close()
    return _EXIT_OK if woke else _EXIT_NO_WAKE


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("operation", ["close", "wait"])
@pytest.mark.parametrize("multi", [False, True], ids=["single", "multi"])
def test_inherited_waiter_never_takes_the_parent_listener_lock(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    operation: str,
    multi: bool,
) -> None:
    waiter = _new_waiter(pg_dsn, pg_schema, multi=multi, queues=_PARENT_QUEUES)
    strategy = PollingStrategy(threading.Event())
    strategy.start(activity_waiter=waiter)
    listener = waiter._listener
    held = threading.Event()
    release = threading.Event()

    def hold_listener_lock() -> None:
        # Stands in for the listener thread routing a notification at fork.
        with listener._lock:
            held.set()
            release.wait(30.0)

    holder = threading.Thread(target=hold_listener_lock, name="listener-lock-holder")
    holder.start()
    try:
        assert held.wait(5.0)
        pid = os.fork()
        if pid == 0:
            code = _EXIT_ERROR
            try:
                code = _child(waiter, strategy, operation, pg_dsn, pg_schema, multi)
            finally:
                os._exit(code)
        deadline = time.monotonic() + _CHILD_DEADLINE_SECONDS
        status: int | None = None
        while time.monotonic() < deadline:
            waited, child_status = os.waitpid(pid, os.WNOHANG)
            if waited == pid:
                status = child_status
                break
            time.sleep(0.01)
        if status is None:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            pytest.fail("the child blocked on the inherited listener lock")
        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == _EXIT_OK

        # The parent's waiter is still the parent's: it was not signalled by
        # the child and it still receives the parent's own activity.
        release.set()
        holder.join(timeout=5.0)
        assert waiter.wait(0) is False
        _write(pg_dsn, pg_schema, _PARENT_QUEUES[-1] if multi else _PARENT_QUEUES[0])
        assert waiter.wait(_WAKE_SECONDS) is True
    finally:
        release.set()
        holder.join(timeout=5.0)
        strategy.close()
        pg_plugin.cleanup_target(pg_dsn, backend_options={"schema": pg_schema})
