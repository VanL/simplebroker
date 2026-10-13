"""Binding contracts and real driver/fork proof for [SB-API-14].

Fresh private owners isolate pure state cases; public sticky bindings run in
spawned processes. No reset API or fake connection-creation path is needed.
"""

from __future__ import annotations

import multiprocessing
import os
import signal
import threading
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from types import SimpleNamespace
from typing import Any

import pytest
import simplebroker_pg._connection_owner as owner_module
from psycopg.conninfo import make_conninfo
from simplebroker_pg import PostgresRunner, bind_connection_owner
from simplebroker_pg._connection_owner import _ConnectionOwner, _owner
from simplebroker_pg.validation import connect

from simplebroker.ext import OperationalError
from tests.helper_scripts import scale_timeout_for_ci
from tests.helpers.state_machine_contracts import TransitionCase, fires_transition_table

pytestmark = [pytest.mark.pg_only]


def _same_owner() -> None:
    owner = _ConnectionOwner()
    owner.bind("A")
    owner.bind("A")
    with pytest.raises(RuntimeError):
        owner.bind("B")
    assert owner.capture() == {"application_name": "sb-owner:A"}
    owner.bind("A")
    with pytest.raises(RuntimeError):
        owner.bind("B")
    assert owner.capture() == {"application_name": "sb-owner:A"}


def _unbound_start() -> None:
    owner = _ConnectionOwner()
    assert owner.capture() == {}
    with pytest.raises(RuntimeError):
        owner.bind("A")
    assert owner.capture() == {}


def _ordered_threads(bind_first: bool) -> None:
    """Both legal orders must publish one decision, including across threads."""
    owner = _ConnectionOwner()
    first_done = threading.Event()
    results: list[object] = []

    def first() -> None:
        if bind_first:
            owner.bind("A")
            results.append(None)
        else:
            results.append(owner.capture())
        first_done.set()

    def second() -> None:
        assert first_done.wait(scale_timeout_for_ci(5))
        if bind_first:
            results.append(owner.capture())
        else:
            with pytest.raises(RuntimeError):
                owner.bind("A")
            results.append("rejected")

    threads = [threading.Thread(target=first), threading.Thread(target=second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(scale_timeout_for_ci(5))
        assert not thread.is_alive()
    assert results == (
        [None, {"application_name": "sb-owner:A"}] if bind_first else [{}, "rejected"]
    )


def _racing_threads() -> None:
    owner = _ConnectionOwner()
    start = threading.Barrier(3)
    bound: list[bool] = []
    options: list[dict[str, str]] = []

    def bind() -> None:
        start.wait(timeout=scale_timeout_for_ci(5))
        try:
            owner.bind("A")
        except RuntimeError:
            bound.append(False)
        else:
            bound.append(True)

    def capture() -> None:
        start.wait(timeout=scale_timeout_for_ci(5))
        options.append(owner.capture())

    threads = [threading.Thread(target=bind), threading.Thread(target=capture)]
    for thread in threads:
        thread.start()
    start.wait(timeout=scale_timeout_for_ci(5))
    for thread in threads:
        thread.join(scale_timeout_for_ci(5))
        assert not thread.is_alive()
    assert (bound, options) in (
        ([True], [{"application_name": "sb-owner:A"}]),
        ([False], [{}]),
    )


def _direct_actor(dsn: str, label: str | None, pipe: Connection) -> None:
    if label is not None:
        bind_connection_owner(label)
    with connect(dsn) as connection:
        row = connection.execute("SHOW application_name").fetchone()
        assert row is not None
        pipe.send(row[0])
    if label is not None:
        # Physical closure cannot reopen process ownership.
        bind_connection_owner(label)
        with pytest.raises(RuntimeError):
            bind_connection_owner("different")
    else:
        with pytest.raises(RuntimeError):
            bind_connection_owner("late")
    pipe.close()


def _failed_actor() -> None:
    # Invalid local port deterministically fails without contacting a server.
    with pytest.raises(OperationalError):
        connect("host=127.0.0.1 port=0 connect_timeout=1")
    with pytest.raises(RuntimeError):
        bind_connection_owner("late")


def _spawn(actor: Callable[..., None], *args: object) -> None:
    process = multiprocessing.get_context("spawn").Process(target=actor, args=args)
    process.start()
    try:
        process.join(scale_timeout_for_ci(30))
        assert not process.is_alive(), f"actor {process.pid} did not finish"
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(scale_timeout_for_ci(5))
        process.close()


def _child_first_entry_race() -> None:
    start = threading.Barrier(3)
    capture_reset = threading.Event()
    bind_done = threading.Event()
    bound: list[bool] = []
    options: list[dict[str, str]] = []
    real_lock = threading.Lock

    def reset_lock() -> Any:
        # The binder has observed the old PID before reaching this
        # factory. Force capture to observe it too, then let capture's
        # reset occur only after bind has published its decision.
        if threading.current_thread().name == "owner-bind":
            assert capture_reset.wait(scale_timeout_for_ci(5))
        else:
            capture_reset.set()
            assert bind_done.wait(scale_timeout_for_ci(5))
        return real_lock()

    # Control only allocation scheduling, not the owner or its mutex.
    # This control depends on allocation through owner_module.threading.Lock;
    # keep its lookup aligned when refactoring that dependency.
    # An eager child reset makes this factory unnecessary and both
    # first entries use the already-reset real lock.
    pytest.MonkeyPatch().setattr(
        owner_module, "threading", SimpleNamespace(Lock=reset_lock)
    )

    def bind() -> None:
        start.wait(timeout=scale_timeout_for_ci(5))
        try:
            bind_connection_owner("child-B")
        except RuntimeError:
            bound.append(False)
        else:
            bound.append(True)
        finally:
            bind_done.set()

    def capture() -> None:
        start.wait(timeout=scale_timeout_for_ci(5))
        options.append(_owner.capture())

    threads = [
        threading.Thread(target=bind, name="owner-bind"),
        threading.Thread(target=capture, name="owner-capture"),
    ]
    for thread in threads:
        thread.start()
    start.wait(timeout=scale_timeout_for_ci(5))
    for thread in threads:
        thread.join(scale_timeout_for_ci(5))
        assert not thread.is_alive()
    valid = (bound, options) in (
        ([True], [{"application_name": "sb-owner:child-B"}]),
        ([False], [{}]),
    )
    if not valid:
        os.write(2, f"inconsistent child binding: {bound=}, {options=}\n".encode())
    os._exit(0 if valid else 2)


def _fork_first_entry_actor() -> None:
    """Force the stale-PID race while retaining the production owner and locks."""
    bind_connection_owner("parent-A")
    child = os.fork()
    if child == 0:
        try:
            _child_first_entry_race()
        finally:
            os._exit(1)
    reaped = False
    try:
        deadline = time.monotonic() + scale_timeout_for_ci(15)
        while time.monotonic() < deadline:
            waited, status = os.waitpid(child, os.WNOHANG)
            if waited:
                reaped = True
                assert os.waitstatus_to_exitcode(status) == 0
                return
            time.sleep(0.01)
        raise AssertionError("first child owner entries did not finish")
    finally:
        if not reaped:
            os.kill(child, signal.SIGKILL)
            os.waitpid(child, 0)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available")
def test_first_child_threads_share_one_binding_decision() -> None:
    _spawn(_fork_first_entry_actor)


def _fork_actor(dsn: str, schema: str) -> None:
    bind_connection_owner("parent-A")
    runner = PostgresRunner(dsn, schema=schema)
    held = threading.Event()
    release = threading.Event()

    def hold_lock() -> None:
        with _owner._lock:
            held.set()
            release.wait(scale_timeout_for_ci(30))

    holder = threading.Thread(target=hold_lock)
    child: int | None = None
    reaped = False
    try:
        parent_row = next(
            iter(
                runner.run(
                    "SELECT pg_backend_pid(), current_setting('application_name')",
                    fetch=True,
                )
            )
        )
        assert parent_row[1] == "sb-owner:parent-A"
        holder.start()
        assert held.wait(scale_timeout_for_ci(5))
        child = os.fork()
        if child == 0:
            try:
                # This public call MUST be the child's first owner operation.
                bind_connection_owner("child-B")
                row = next(
                    iter(
                        runner.run(
                            "SELECT pg_backend_pid(), current_setting('application_name')",
                            fetch=True,
                        )
                    )
                )
                assert row[0] != parent_row[0]
                assert row[1] == "sb-owner:child-B"
                runner.shutdown()
                os._exit(0)
            finally:
                os._exit(1)
        deadline = time.monotonic() + scale_timeout_for_ci(15)
        while time.monotonic() < deadline:
            waited, status = os.waitpid(child, os.WNOHANG)
            if waited:
                reaped = True
                assert os.waitstatus_to_exitcode(status) == 0
                break
            time.sleep(0.01)
        assert reaped, "child blocked on inherited foreign-held binding lock"
        assert (
            next(
                iter(
                    runner.run(
                        "SELECT pg_backend_pid(), current_setting('application_name')",
                        fetch=True,
                    )
                )
            )
            == parent_row
        )
    finally:
        if child and not reaped:
            os.kill(child, signal.SIGKILL)
            os.waitpid(child, 0)
        release.set()
        if holder.ident is not None:
            holder.join(scale_timeout_for_ci(5))
            assert not holder.is_alive()
        runner.shutdown()


# Each row fires real lifecycle effects; no enum or field layout is prescribed.
OWNER_TRANSITIONS = tuple(
    TransitionCase(
        transition_id=name,
        start_state=start,
        event=event,
        guard="current process",
        next_state=end,
        effects=effect,
        expected_result=effect,
        payload=name,
    )
    for name, start, event, end, effect in (
        (
            "bind-idempotent-conflict",
            "unbound",
            "bind and start",
            "bound",
            "same label succeeds; conflict preserves A",
        ),
        (
            "creation-seals",
            "unbound",
            "capture creation options",
            "sealed",
            "late binding rejected",
        ),
        (
            "failed-start-seals",
            "unbound",
            "failed real connect",
            "sealed",
            "late binding rejected after failure",
        ),
        (
            "close-stays-sealed",
            "unbound",
            "real connect and close",
            "sealed",
            "late binding rejected after closure",
        ),
        (
            "bind-before-start",
            "unbound",
            "ordered thread bind then creation",
            "bound",
            "creation receives A",
        ),
        (
            "start-before-bind",
            "unbound",
            "ordered thread creation then bind",
            "sealed",
            "binding rejected",
        ),
        (
            "simultaneous-bind-start",
            "unbound",
            "race bind and creation",
            "decided",
            "one consistent creation decision",
        ),
        (
            "pid-reset",
            "parent bound",
            "fork with foreign-held lock",
            "child bound",
            "child runner uses B; parent keeps A",
        ),
    )
)


@fires_transition_table("SM-PG-CONNECTION-OWNER", OWNER_TRANSITIONS)
def test_connection_owner_transition_table(
    transition_case: TransitionCase[str], request: pytest.FixtureRequest
) -> None:
    name = transition_case.payload
    if name == "bind-idempotent-conflict":
        _same_owner()
    elif name == "creation-seals":
        _unbound_start()
    elif name == "failed-start-seals":
        _spawn(_failed_actor)
    elif name == "close-stays-sealed":
        dsn = request.getfixturevalue("pg_dsn")
        context = multiprocessing.get_context("spawn")
        receive, send = context.Pipe(duplex=False)
        try:
            _spawn(_direct_actor, dsn, None, send)
            assert receive.poll(scale_timeout_for_ci(2))
            assert isinstance(receive.recv(), str)
        finally:
            receive.close()
            send.close()
    elif name == "simultaneous-bind-start":
        _racing_threads()
    elif name == "pid-reset":
        if not hasattr(os, "fork"):
            pytest.skip("fork() is not available")
        _spawn(
            _fork_actor,
            request.getfixturevalue("pg_dsn"),
            request.getfixturevalue("pg_schema"),
        )
    else:
        _ordered_threads(name == "bind-before-start")


@pytest.mark.parametrize("label", ["!", "Aa:punctuation-._/", "~" * 54])
def test_owner_accepts_label_grammar(label: str) -> None:
    owner = _ConnectionOwner()
    owner.bind(label)
    assert owner.capture() == {"application_name": "sb-owner:" + label}


@pytest.mark.parametrize(
    "label", [None, 1, b"A", "", "a" * 55, "has space", "\t", "\n", "\x00", "é", "\x7f"]
)
def test_invalid_owner_does_not_change_binding(label: Any) -> None:
    owner = _ConnectionOwner()
    with pytest.raises(ValueError):
        owner.bind(label)
    owner.bind("valid")
    assert owner.capture() == {"application_name": "sb-owner:valid"}


def test_owner_label_round_trips_and_overrides_application_name(pg_dsn: str) -> None:
    label = "A:!~.-_/" + "x" * 46
    assert len(label) == 54
    dsn = make_conninfo(pg_dsn, application_name="configured-original")
    context = multiprocessing.get_context("spawn")
    for binding, expected in (
        (label, "sb-owner:" + label),
        (None, "configured-original"),
    ):
        receive, send = context.Pipe(duplex=False)
        try:
            _spawn(_direct_actor, dsn, binding, send)
            assert receive.poll(scale_timeout_for_ci(2))
            assert receive.recv() == expected
        finally:
            receive.close()
            send.close()


def test_direct_connector_keeps_owner(pg_dsn: str) -> None:
    receive, send = multiprocessing.get_context("spawn").Pipe(duplex=False)
    try:
        _spawn(_direct_actor, pg_dsn, "direct", send)
        assert receive.poll(scale_timeout_for_ci(2))
        assert receive.recv() == "sb-owner:direct"
    finally:
        receive.close()
        send.close()
