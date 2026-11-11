"""A Redis activity waiter inherited across fork() never locks the parent listener.

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
from collections.abc import Callable
from typing import Any

import pytest
from simplebroker_redis.plugin import (
    RedisActivityWaiter,
    RedisMultiQueueActivityWaiter,
    get_backend_plugin,
)

from simplebroker import BrokerTarget, Queue
from simplebroker._exceptions import OperationalError
from simplebroker.ext import PollingStrategy

pytestmark = [
    pytest.mark.redis_only,
    pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available"),
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]

_CHILD_DEADLINE_SECONDS = 10.0
_WAKE_SECONDS = 5.0
_EXIT_OK, _EXIT_ERROR, _EXIT_NO_ERROR, _EXIT_WRONG_ERROR, _EXIT_NO_WAKE = range(5)

_PARENT_QUEUES = ("alpha", "beta")
# The child's replacement watches other queues, so the child's write cannot
# satisfy the parent's waiter: the parent check must see the parent's write.
_CHILD_QUEUES = ("gamma", "delta")


def _new_waiter(
    url: str, namespace: str, *, multi: bool, queues: tuple[str, str]
) -> Any:
    plugin = get_backend_plugin()
    options = {"namespace": namespace}
    if multi:
        return plugin.create_activity_waiter_for_queues(
            target=url,
            backend_options=options,
            queue_names=queues,
            stop_event=threading.Event(),
        )
    return plugin.create_activity_waiter(
        target=url,
        backend_options=options,
        queue_name=queues[0],
        stop_event=threading.Event(),
    )


def _write(url: str, namespace: str, queue_name: str) -> None:
    target = BrokerTarget("redis", url, {"namespace": namespace})
    with Queue(queue_name, db_path=target) as q:
        q.write("activity")


def _listener_of(waiter: Any) -> Any:
    inner = waiter._waiters[0] if hasattr(waiter, "_waiters") else waiter
    return inner._listener


def _expect_fork_error(wait: Callable[[], object]) -> int | None:
    try:
        wait()
    except OperationalError as exc:
        return None if "fork" in str(exc) else _EXIT_WRONG_ERROR
    return _EXIT_NO_ERROR


def _recover(strategy: PollingStrategy, url: str, namespace: str, multi: bool) -> int:
    # The owner's recovery: a fresh waiter replaces the inherited one, which
    # the strategy closes (abandons), and the fresh one receives activity.
    fresh = _new_waiter(url, namespace, multi=multi, queues=_CHILD_QUEUES)
    strategy.start(activity_waiter=fresh)
    _write(url, namespace, _CHILD_QUEUES[-1] if multi else _CHILD_QUEUES[0])
    woke = fresh.wait(_WAKE_SECONDS)
    strategy.close()
    return _EXIT_OK if woke else _EXIT_NO_WAKE


def _run_with_listener_lock_held(listener: Any, child: Callable[[], int]) -> None:
    """Fork while another thread holds ``listener._lock``; fail if the child blocks."""
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
                code = child()
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
    finally:
        release.set()
        holder.join(timeout=5.0)


def _assert_parent_waiter_still_wakes(
    waiter: Any, url: str, namespace: str, queue_name: str
) -> None:
    # The parent's waiter is still the parent's: it was not signalled by the
    # child and it still receives the parent's own activity.
    assert waiter.wait(0) is False
    _write(url, namespace, queue_name)
    assert waiter.wait(_WAKE_SECONDS) is True


@pytest.mark.parametrize("operation", ["close", "wait"])
@pytest.mark.parametrize("multi", [False, True], ids=["single", "multi"])
def test_inherited_waiter_never_takes_the_parent_listener_lock(
    redis_url: str,
    redis_namespace: str,
    operation: str,
    multi: bool,
) -> None:
    waiter = _new_waiter(redis_url, redis_namespace, multi=multi, queues=_PARENT_QUEUES)
    strategy = PollingStrategy(threading.Event())
    strategy.start(activity_waiter=waiter)

    def child() -> int:
        if operation == "close":
            waiter.close()
        else:
            # The owner's wait path: the strategy waits on the inherited waiter.
            failed = _expect_fork_error(lambda: strategy.wait_for_activity(0.1))
            if failed is not None:
                return failed
        return _recover(strategy, redis_url, redis_namespace, multi)

    try:
        _run_with_listener_lock_held(_listener_of(waiter), child)
        _assert_parent_waiter_still_wakes(
            waiter,
            redis_url,
            redis_namespace,
            _PARENT_QUEUES[-1] if multi else _PARENT_QUEUES[0],
        )
    finally:
        strategy.close()
        get_backend_plugin().cleanup_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )


def test_child_built_composite_refuses_inherited_waiters(
    redis_url: str, redis_namespace: str
) -> None:
    """Ownership follows each registration, not the newest wrapper.

    A composite constructed in the child records the child's process ID, but
    its single waiters, and their listener, belong to the parent. Its wait
    must refuse them rather than take the shared listener's fast path.
    """
    stop_event = threading.Event()
    waiters: list[RedisActivityWaiter] = []
    for name in _PARENT_QUEUES:
        waiter = get_backend_plugin().create_activity_waiter(
            target=redis_url,
            backend_options={"namespace": redis_namespace},
            queue_name=name,
            stop_event=stop_event,
        )
        assert isinstance(waiter, RedisActivityWaiter)
        waiters.append(waiter)
    parent_composite = RedisMultiQueueActivityWaiter(waiters, stop_event)

    def child() -> int:
        composite = RedisMultiQueueActivityWaiter(waiters, stop_event)
        failed = _expect_fork_error(lambda: composite.wait(0.1))
        if failed is not None:
            return failed
        composite.close()
        return _EXIT_OK

    try:
        _run_with_listener_lock_held(_listener_of(waiters[0]), child)
        _assert_parent_waiter_still_wakes(
            parent_composite, redis_url, redis_namespace, _PARENT_QUEUES[-1]
        )
    finally:
        parent_composite.close()
        get_backend_plugin().cleanup_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )
