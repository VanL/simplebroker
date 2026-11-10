"""SQLiteRunner connections are touched only under the runner's operation lock.

``SQLiteRunner.close()`` closes every tracked connection, from any thread,
under ``_operation_lock`` (SM-SQLITE-RUNNER). A connection the runner has
already published for close must therefore never execute a statement on its
owner thread without that lock: Python's sqlite3 wrapper is not safe against a
concurrent ``close()`` of a connection created with ``check_same_thread=False``
(a reproducer of the unlocked admission read racing ``runner.close()``
segfaults within seconds).

Plan: docs/plans/2026-11-10-backend-thread-ownership-plan.md (F10, D8).
"""

from __future__ import annotations

import sqlite3
import threading
import traceback
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

from simplebroker import _runner as runner_module
from simplebroker import resolve_config
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore


class _OwnershipCheckedConnection(sqlite3.Connection):
    """Records statements issued on a published connection without the lock."""

    runner: ClassVar[SQLiteRunner | None] = None
    violations: ClassVar[list[str]] = []

    def _check(self) -> None:
        runner = type(self).runner
        if runner is None:
            return
        published = self in runner._all_connections
        if published and not cast(Any, runner._operation_lock)._is_owned():
            type(self).violations.append("".join(traceback.format_stack(limit=6)))

    # Every connection touch outside a runner call goes through execute();
    # the runner's own locked calls use cursor().
    def execute(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
        self._check()
        return super().execute(*args, **kwargs)


@pytest.fixture
def checked_connections(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    real_connect = sqlite3.connect

    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        kwargs.setdefault("factory", _OwnershipCheckedConnection)
        return cast(sqlite3.Connection, real_connect(*args, **kwargs))

    monkeypatch.setattr(runner_module.sqlite3, "connect", connect)
    _OwnershipCheckedConnection.violations = []
    try:
        yield
    finally:
        _OwnershipCheckedConnection.runner = None
        _OwnershipCheckedConnection.violations = []


@pytest.mark.usefixtures("checked_connections")
def test_core_construction_touches_published_connections_only_under_lock(
    tmp_path: Path,
) -> None:
    """Construction, setup and first use of a shared runner hold the lock.

    The admission read, connection settings, the setup busy timeout and the
    optimization settings all run on the owner thread's runner connection; each
    must happen before the connection is published for ``close()`` or under
    ``_operation_lock``.
    """
    runner = SQLiteRunner(str(tmp_path / "ownership.db"))
    _OwnershipCheckedConnection.runner = runner
    try:
        core = BrokerCore(runner)
        try:
            core.write("jobs", "one")
            peeked = core.peek_one("jobs")
            assert peeked is not None
            assert peeked[0] == "one"
        finally:
            core.close()
    finally:
        runner.close()
    assert _OwnershipCheckedConnection.violations == []


def test_setup_busy_timeout_does_not_wait_behind_a_foreign_transaction(
    tmp_path: Path,
) -> None:
    """Connection-level setup PRAGMAs need the operation lock, not admission.

    Another thread holds an open transaction on the shared runner. Applying and
    restoring the setup busy timeout on this thread must neither wait for that
    transaction nor leave this thread on the short setup timeout.
    """
    runner = SQLiteRunner(
        str(tmp_path / "setup.db"),
        config=resolve_config(override={"BROKER_BUSY_TIMEOUT": 1000}),
    )
    owner_ready = threading.Event()
    release_owner = threading.Event()

    def transaction_owner() -> None:
        runner.begin_immediate()
        owner_ready.set()
        release_owner.wait(10.0)
        runner.rollback()

    with runner.owned_connection("test warm-up"):
        pass
    owner = threading.Thread(target=transaction_owner, name="transaction-owner")
    owner.start()
    try:
        assert owner_ready.wait(5.0)
        # Inside the foreign transaction window: no admission wait or error.
        with runner._setup_operation_context():
            pass
        assert not getattr(runner._thread_local, "setup_busy_timeout", False)
    finally:
        release_owner.set()
        owner.join(timeout=5.0)
    try:
        with runner.owned_connection("test read-back") as conn:
            assert conn.execute("PRAGMA busy_timeout").fetchone() == (1000,)
    finally:
        runner.close()


def test_busy_timeout_restore_does_not_reopen_connection_closed_during_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = SQLiteRunner(str(tmp_path / "ownership.db"))
    entered = threading.Event()
    resume = threading.Event()
    original = runner._settings_connection

    @contextmanager
    def paused_restore() -> Iterator[sqlite3.Connection | None]:
        entered.set()
        assert resume.wait(5)
        with original() as conn:
            yield conn

    def restore_timeout() -> None:
        runner.get_connection()
        with runner._setup_operation_context():
            monkeypatch.setattr(runner, "_settings_connection", paused_restore)

    try:
        with ThreadPoolExecutor(max_workers=1) as worker:
            restored = worker.submit(restore_timeout)
            try:
                assert entered.wait(5)
                runner.close()
            finally:
                resume.set()
            restored.result(timeout=5)
        assert not runner._all_connections
    finally:
        runner.close()
