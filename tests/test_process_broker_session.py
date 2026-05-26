"""Process-local persistent queue session sharing tests."""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker._backend_plugins import BACKEND_ENTRY_POINT_GROUP
from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._broker_session import (
    _ProcessBrokerSession,
    _session_key,
    close_process_broker_sessions,
)
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import ResolvedTarget
from simplebroker.db import BrokerCore


class CountingSQLiteRunner(SQLiteRunner):
    """SQLite-backed runner that records allocation and close behavior."""

    def __init__(self, db_path: str, plugin: CountingBackendPlugin) -> None:
        self._counting_plugin = plugin
        super().__init__(db_path)

    def lease_thread_connection(self) -> None:
        self._counting_plugin.runner_lease_calls += 1
        lease_depth = int(getattr(self._thread_local, "lease_depth", 0))
        self._thread_local.lease_depth = lease_depth + 1
        self.get_connection()

    def _thread_connection_leased(self) -> bool:
        return int(getattr(self._thread_local, "lease_depth", 0)) > 0

    def _release_after_operation(self) -> None:
        if not self._thread_connection_leased():
            self.release_thread_connection()

    def _finish_transaction(self) -> None:
        if hasattr(self._thread_local, "in_transaction"):
            delattr(self._thread_local, "in_transaction")
        self._release_after_operation()

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Any:
        try:
            return super().run(sql, params, fetch=fetch)
        finally:
            if not bool(getattr(self._thread_local, "in_transaction", False)):
                self._release_after_operation()

    def begin_immediate(self) -> None:
        super().begin_immediate()
        self._thread_local.in_transaction = True

    def commit(self) -> None:
        try:
            super().commit()
        finally:
            self._finish_transaction()

    def rollback(self) -> None:
        try:
            super().rollback()
        finally:
            self._finish_transaction()

    def close(self) -> None:
        self._counting_plugin.runner_close_calls += 1
        super().close()

    def release_thread_connection(self) -> None:
        lease_depth = int(getattr(self._thread_local, "lease_depth", 0))
        if lease_depth > 1:
            self._thread_local.lease_depth = lease_depth - 1
            return
        if lease_depth == 1:
            delattr(self._thread_local, "lease_depth")
        self._counting_plugin.runner_release_calls += 1


class CountingBackendPlugin:
    """Backend plugin facade that delegates behavior to SQLite and counts runners."""

    name = "counting"
    sql = sqlite_backend_plugin.sql
    schema_version = sqlite_backend_plugin.schema_version

    def __init__(self) -> None:
        self.create_runner_calls = 0
        self.runner_close_calls = 0
        self.runner_release_calls = 0
        self.runner_lease_calls = 0

    def create_runner(
        self,
        target: str,
        *,
        backend_options: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
    ) -> CountingSQLiteRunner:
        del backend_options, config
        self.create_runner_calls += 1
        return CountingSQLiteRunner(target, self)

    def __getattr__(self, name: str) -> Any:
        return getattr(sqlite_backend_plugin, name)


_COUNTING_PLUGIN: CountingBackendPlugin | None = None


@pytest.fixture(autouse=True)
def clean_process_broker_sessions() -> Iterator[None]:
    close_process_broker_sessions()
    yield
    close_process_broker_sessions()


def build_counting_backend_plugin() -> CountingBackendPlugin:
    assert _COUNTING_PLUGIN is not None
    return _COUNTING_PLUGIN


class EntryPointsMock(list[EntryPoint]):
    def select(self, *, group: str, name: str) -> EntryPointsMock:
        if group == BACKEND_ENTRY_POINT_GROUP and name == "counting":
            return self
        return EntryPointsMock()


@pytest.fixture
def counting_backend(monkeypatch: pytest.MonkeyPatch) -> CountingBackendPlugin:
    global _COUNTING_PLUGIN
    plugin = CountingBackendPlugin()
    _COUNTING_PLUGIN = plugin

    entry_point = EntryPoint(
        name="counting",
        value="tests.test_process_broker_session:build_counting_backend_plugin",
        group=BACKEND_ENTRY_POINT_GROUP,
    )
    monkeypatch.setattr(
        "simplebroker._backend_plugins.metadata.entry_points",
        lambda: EntryPointsMock([entry_point]),
    )
    return plugin


def counting_target(
    tmp_path: Path, *, suffix: str = "broker.db", **options: Any
) -> ResolvedTarget:
    return ResolvedTarget(
        backend_name="counting",
        target=str(tmp_path / suffix),
        backend_options=dict(options),
        project_root=tmp_path,
    )


def test_persistent_queues_same_resolved_target_share_backend_runner_in_process(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")

    with contextlib.ExitStack() as stack:
        queues = [
            stack.enter_context(Queue(name, db_path=target, persistent=True))
            for name in ("a", "b", "c")
        ]

        for index, queue in enumerate(queues):
            queue.write(f"message-{index}")

    assert counting_backend.create_runner_calls == 1


def test_persistent_queues_different_targets_do_not_share_backend_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target_a = counting_target(tmp_path, suffix="a.db", schema="same")
    target_b = counting_target(tmp_path, suffix="b.db", schema="same")

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(Queue("a", db_path=target_a, persistent=True))
        queue_b = stack.enter_context(Queue("b", db_path=target_b, persistent=True))
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_persistent_queues_different_backend_options_do_not_share_backend_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target_a = counting_target(tmp_path, schema="a")
    target_b = counting_target(tmp_path, schema="b")

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(Queue("a", db_path=target_a, persistent=True))
        queue_b = stack.enter_context(Queue("b", db_path=target_b, persistent=True))
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_persistent_queues_different_config_do_not_share_backend_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(
            Queue(
                "a",
                db_path=target,
                persistent=True,
                config={"BROKER_BUSY_TIMEOUT": 1000},
            )
        )
        queue_b = stack.enter_context(
            Queue(
                "b",
                db_path=target,
                persistent=True,
                config={"BROKER_BUSY_TIMEOUT": 2000},
            )
        )
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_closing_one_queue_does_not_close_shared_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")
    queue_a = Queue("a", db_path=target, persistent=True)
    queue_b = Queue("b", db_path=target, persistent=True)

    try:
        queue_a.write("one")
        queue_b.write("two")
        queue_a.close()
        queue_b.write("three")
    finally:
        queue_a.close()
        queue_b.close()

    assert counting_backend.create_runner_calls == 1


def test_closing_last_queue_releases_shared_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")

    queue_a = Queue("a", db_path=target, persistent=True)
    queue_b = Queue("b", db_path=target, persistent=True)
    queue_a.write("one")
    queue_b.write("two")
    queue_a.close()
    assert counting_backend.runner_close_calls == 0
    queue_b.close()
    assert counting_backend.runner_close_calls >= 1

    queue_c = Queue("c", db_path=target, persistent=True)
    try:
        queue_c.write("three")
    finally:
        queue_c.close()

    assert counting_backend.create_runner_calls == 2


def test_cleanup_connections_does_not_release_shared_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")

    with Queue("a", db_path=target, persistent=True) as queue:
        queue.write("one")
        queue.cleanup_connections()
        assert counting_backend.runner_release_calls >= 1
        assert counting_backend.runner_close_calls == 0
        queue.write("two")

    assert counting_backend.create_runner_calls == 1


def test_persistent_queues_keep_shared_backend_checkout_across_operations(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(Queue("a", db_path=target, persistent=True))
        queue_b = stack.enter_context(Queue("b", db_path=target, persistent=True))

        for _ in range(3):
            queue_a.has_pending()
            queue_b.has_pending()

        assert counting_backend.runner_lease_calls == 1
        assert counting_backend.runner_release_calls == 0

    assert counting_backend.create_runner_calls == 1
    assert counting_backend.runner_release_calls >= 1
    assert counting_backend.runner_close_calls >= 1


def test_ephemeral_queues_do_not_use_process_local_registry(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")

    with Queue("a", db_path=target, persistent=False) as queue:
        queue.write("one")
        queue.write("two")

    assert counting_backend.create_runner_calls == 2


def test_injected_runner_does_not_use_process_local_registry(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")
    runner = SQLiteRunner(str(tmp_path / "injected.db"))

    try:
        with Queue("a", db_path=target, runner=runner, persistent=True) as queue:
            queue.write("one")
    finally:
        runner.close()

    assert counting_backend.create_runner_calls == 0


def test_persistent_sqlite_queues_same_path_share_runner_in_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner_ids: list[int] = []
    original_init = SQLiteRunner.__init__

    def tracked_init(self: SQLiteRunner, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        runner_ids.append(self.instance_id)

    monkeypatch.setattr(SQLiteRunner, "__init__", tracked_init)

    with contextlib.ExitStack() as stack:
        queues = [
            stack.enter_context(
                Queue(f"q{i}", db_path=str(tmp_path / "sqlite.db"), persistent=True)
            )
            for i in range(3)
        ]
        for queue in queues:
            queue.write("message")

    assert len(set(runner_ids)) == 1


def test_persistent_sqlite_queues_normalize_same_file_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner_ids: list[int] = []
    original_init = SQLiteRunner.__init__

    def tracked_init(self: SQLiteRunner, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        runner_ids.append(self.instance_id)

    monkeypatch.setattr(SQLiteRunner, "__init__", tracked_init)
    monkeypatch.chdir(tmp_path)

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(Queue("a", db_path="sqlite.db", persistent=True))
        queue_b = stack.enter_context(
            Queue("b", db_path=str(tmp_path / "sqlite.db"), persistent=True)
        )
        queue_a.write("one")
        queue_b.write("two")

    assert len(set(runner_ids)) == 1


def test_persistent_sqlite_queues_keep_thread_local_connection_isolation(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "sqlite.db")
    main_thread_ids: list[int] = []
    worker_thread_ids: list[int] = []

    with contextlib.ExitStack() as stack:
        queues = [
            stack.enter_context(Queue(f"q{i}", db_path=db_path, persistent=True))
            for i in range(3)
        ]

        for queue in queues:
            with queue.get_connection() as connection:
                main_thread_ids.append(connection._runner.instance_id)

        def touch_queues() -> None:
            for queue in queues:
                with queue.get_connection() as connection:
                    worker_thread_ids.append(connection._runner.instance_id)

        thread = threading.Thread(target=touch_queues)
        thread.start()
        thread.join()

    assert len(set(main_thread_ids)) == 1
    assert len(set(worker_thread_ids)) == 1
    assert set(main_thread_ids) != set(worker_thread_ids)


def test_persistent_sqlite_queue_close_waits_for_in_flight_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue("jobs", db_path=str(tmp_path / "sqlite.db"), persistent=True)
    operation_entered = threading.Event()
    release_operation = threading.Event()
    close_returned = threading.Event()
    operation_errors: list[BaseException] = []
    close_errors: list[BaseException] = []
    original_write = BrokerCore.write

    def delayed_write(self: BrokerCore, queue_name: str, message: str) -> None:
        operation_entered.set()
        assert release_operation.wait(timeout=5.0)
        original_write(self, queue_name, message)

    def write_message() -> None:
        try:
            queue.write("payload")
        except BaseException as exc:  # pragma: no cover - asserted in parent thread
            operation_errors.append(exc)

    def close_queue() -> None:
        try:
            queue.close()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread
            close_errors.append(exc)
        finally:
            close_returned.set()

    monkeypatch.setattr(BrokerCore, "write", delayed_write)

    operation_thread = threading.Thread(target=write_message)
    close_thread = threading.Thread(target=close_queue)
    operation_thread.start()
    assert operation_entered.wait(timeout=5.0)

    close_thread.start()
    try:
        assert not close_returned.wait(timeout=0.25), (
            "Queue.close() returned while a persistent queue operation was still "
            "using the shared broker session"
        )
    finally:
        release_operation.set()
        operation_thread.join(timeout=5.0)
        close_thread.join(timeout=5.0)

    assert not operation_thread.is_alive()
    assert not close_thread.is_alive()
    assert not operation_errors
    assert not close_errors


def test_process_session_close_all_times_out_when_operation_never_releases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "simplebroker._broker_session._CLOSE_ACTIVE_OPERATION_TIMEOUT",
        0.05,
    )
    session = _ProcessBrokerSession(str(tmp_path / "sqlite.db"))
    session._begin_operation()
    close_returned = threading.Event()

    def close_session() -> None:
        session.close_all()
        close_returned.set()

    started_at = time.monotonic()
    close_thread = threading.Thread(target=close_session)
    close_thread.start()
    close_thread.join(timeout=1.0)
    elapsed = time.monotonic() - started_at

    try:
        assert not close_thread.is_alive()
        assert close_returned.is_set()
        assert elapsed < 0.5
        assert session._closed
    finally:
        if session._active_operations > 0:
            session._end_operation()
        close_thread.join(timeout=1.0)


def test_process_session_key_includes_pid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    counting_backend: CountingBackendPlugin,
) -> None:
    del counting_backend
    target = counting_target(tmp_path, schema="same")

    monkeypatch.setattr("simplebroker._broker_session.os.getpid", lambda: 1000)
    parent_key = _session_key(target, {})
    monkeypatch.setattr("simplebroker._broker_session.os.getpid", lambda: 1001)
    child_key = _session_key(target, {})

    assert parent_key != child_key
