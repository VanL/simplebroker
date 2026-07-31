"""Process-local persistent queue session sharing tests."""

from __future__ import annotations

import ast
import concurrent.futures as cf
import contextlib
import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import Any, cast

import pytest

import simplebroker._broker_session as broker_session_module
from simplebroker import Queue
from simplebroker._backend_plugins import BACKEND_ENTRY_POINT_GROUP
from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._broker_session import (
    _ProcessBrokerSession,
    _ProcessBrokerSessionRegistry,
    _session_key,
    _session_spec,
    close_process_broker_sessions,
)
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerCore, _build_process_session_core_factory


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
        super().commit()
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
    backend_api_version = sqlite_backend_plugin.backend_api_version
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


def test_broker_session_has_no_db_import() -> None:
    source_path = Path(broker_session_module.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    db_imports: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            db_imports.extend(
                (node.lineno, alias.name)
                for alias in node.names
                if alias.name in {"db", "simplebroker.db"}
            )
        elif isinstance(node, ast.ImportFrom) and node.module in {
            "db",
            "simplebroker.db",
        }:
            db_imports.append((node.lineno, node.module))
        elif isinstance(node, ast.ImportFrom) and (
            (node.module == "simplebroker" and any(a.name == "db" for a in node.names))
            or (
                node.level > 0
                and node.module is None
                and any(a.name == "db" for a in node.names)
            )
        ):
            db_imports.append((node.lineno, "package db import"))
        elif isinstance(node, ast.Call) and node.args:
            function_name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else (node.func.attr if isinstance(node.func, ast.Attribute) else "")
            )
            first_arg = node.args[0]
            if (
                function_name in {"__import__", "import_module"}
                and isinstance(first_arg, ast.Constant)
                and first_arg.value in {"db", ".db", "simplebroker.db"}
            ):
                db_imports.append((node.lineno, str(first_arg.value)))

    assert db_imports == []


@pytest.mark.parametrize(
    "module_order",
    [
        ("simplebroker.db", "simplebroker._broker_session"),
        ("simplebroker._broker_session", "simplebroker.db"),
    ],
)
def test_process_session_import_orders_exit_cleanly(
    tmp_path: Path,
    module_order: tuple[str, str],
) -> None:
    script = """
import importlib
import sys

for module_name in sys.argv[2].split(","):
    importlib.import_module(module_name)

from simplebroker import Queue

queue = Queue("jobs", db_path=sys.argv[1], persistent=True)
queue.write("payload")
queue._finalizer.detach()
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    # This probe owns interpreter shutdown and must not inherit coverage.py's
    # additional atexit hook. On Windows, automatic subprocess coverage can
    # hang during finalization and changes the boundary being tested.
    env.pop("COVERAGE_PROCESS_START", None)
    env.pop("COVERAGE_PROCESS_CONFIG", None)
    env.pop("COVERAGE_FILE", None)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(tmp_path / "atexit.db"),
            ",".join(module_order),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=10.0,
        check=False,
    )

    assert result.returncode == 0
    assert result.stderr == ""


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
) -> BrokerTarget:
    return BrokerTarget(
        backend_name="counting",
        target=str(tmp_path / suffix),
        backend_options=dict(options),
        project_root=tmp_path,
    )


def build_process_session(
    db_path: str | BrokerTarget,
    *,
    config: dict[str, Any] | None = None,
) -> _ProcessBrokerSession:
    spec = _session_spec(db_path, {} if config is None else config)
    return _ProcessBrokerSession(_build_process_session_core_factory(spec))


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


def test_concurrent_first_use_publishes_one_shared_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="concurrent")
    start_barrier = threading.Barrier(4)

    with contextlib.ExitStack() as stack:
        queues = [
            stack.enter_context(Queue(f"q{index}", db_path=target, persistent=True))
            for index in range(4)
        ]

        def write_once(index: int) -> None:
            start_barrier.wait(timeout=5.0)
            queues[index].write(f"message-{index}")

        with cf.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(write_once, index) for index in range(4)]
            for future in futures:
                future.result(timeout=10.0)

        assert counting_backend.create_runner_calls == 1

    assert counting_backend.runner_close_calls == 1


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
                runner = cast(SQLiteRunner, cast(Any, connection)._runner)
                main_thread_ids.append(runner.instance_id)

        def touch_queues() -> None:
            for queue in queues:
                with queue.get_connection() as connection:
                    runner = cast(SQLiteRunner, cast(Any, connection)._runner)
                    worker_thread_ids.append(runner.instance_id)

        thread = threading.Thread(target=touch_queues)
        thread.start()
        thread.join()

    assert len(set(main_thread_ids)) == 1
    assert len(set(worker_thread_ids)) == 1
    assert set(main_thread_ids) != set(worker_thread_ids)


def test_persistent_sqlite_thread_owners_do_not_reapply_connection_pragmas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thread-owned persistent queues should not churn SQLite setup PRAGMAs."""

    db_path = str(tmp_path / "sqlite.db")
    with Queue("bootstrap", db_path=db_path, persistent=True) as queue:
        queue.write("ready")
        with queue.sidecar(transaction=True) as session:
            session.run(
                "CREATE TABLE IF NOT EXISTS sb_test_thread_events "
                "(thread_index INTEGER, event_index INTEGER, payload TEXT)"
            )
    close_process_broker_sessions()

    apply_calls: list[tuple[int, int]] = []
    apply_lock = threading.Lock()
    original_apply_connection_settings = SQLiteRunner._apply_connection_settings

    def tracked_apply_connection_settings(
        self: SQLiteRunner,
        conn: sqlite3.Connection,
    ) -> None:
        with apply_lock:
            apply_calls.append((threading.get_ident(), self.instance_id))
        original_apply_connection_settings(self, conn)

    monkeypatch.setattr(
        SQLiteRunner,
        "_apply_connection_settings",
        tracked_apply_connection_settings,
    )

    def call_count_for_current_thread() -> int:
        ident = threading.get_ident()
        with apply_lock:
            return sum(
                1 for thread_ident, _runner_id in apply_calls if thread_ident == ident
            )

    start_barrier = threading.Barrier(3)

    def worker(thread_index: int) -> None:
        start_barrier.wait(timeout=5.0)
        queue = Queue(
            f"thread_{thread_index}",
            db_path=db_path,
            persistent=True,
        )
        try:
            queue.write(f"initial-{thread_index}")
            with queue.sidecar(transaction=True) as session:
                session.run(
                    "INSERT INTO sb_test_thread_events "
                    "(thread_index, event_index, payload) VALUES (?, ?, ?)",
                    (thread_index, 0, "initial"),
                )
            first_connection_setup_count = call_count_for_current_thread()
            assert first_connection_setup_count == 1

            for event_index in range(1, 6):
                queue.write(f"message-{thread_index}-{event_index}")
                assert queue.has_pending()
                with queue.sidecar(transaction=True) as session:
                    session.run(
                        "INSERT INTO sb_test_thread_events "
                        "(thread_index, event_index, payload) VALUES (?, ?, ?)",
                        (thread_index, event_index, f"payload-{event_index}"),
                    )

            assert call_count_for_current_thread() == first_connection_setup_count
        finally:
            queue.close()

    with cf.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(worker, index) for index in range(3)]
        for future in futures:
            future.result(timeout=10.0)

    with apply_lock:
        assert len(apply_calls) == 3
        assert len({thread_ident for thread_ident, _runner_id in apply_calls}) == 3

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        sidecar_rows = conn.execute(
            "SELECT COUNT(*) FROM sb_test_thread_events"
        ).fetchone()[0]
    finally:
        conn.close()
    assert sidecar_rows == 18


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
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            operation_errors.append(exc)

    def close_queue() -> None:
        try:
            queue.close()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
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
    session = build_process_session(str(tmp_path / "sqlite.db"))
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


def test_closed_session_rejects_connections_and_extra_releases(tmp_path: Path) -> None:
    session = build_process_session(str(tmp_path / "sqlite.db"))
    session.close_all()
    session.close_all()

    with pytest.raises(RuntimeError, match="Broker session is closed"):
        session.get_connection(None)
    with pytest.raises(RuntimeError, match="Broker session is closed"):
        session.get_connection(None, lease_operation=False)

    session.release_current_thread_connection()


@pytest.mark.parametrize(
    ("branch", "supports_lease", "release_fails"),
    [
        ("direct", True, False),
        ("direct", False, False),
        ("sql", True, False),
        ("sql", False, False),
        ("direct", True, True),
        ("sql", True, True),
    ],
)
def test_failed_core_creation_releases_any_runner_lease(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-030] exception
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    branch: str,
    supports_lease: bool,
    release_fails: bool,
) -> None:
    creation_error = RuntimeError(f"{branch} core creation failed")

    class DirectRunner:
        def __init__(self, plugin: FailingPlugin) -> None:
            self.plugin = plugin
            if supports_lease:
                self.lease_thread_connection = self._lease
                self.release_thread_connection = self._release

        def _lease(self) -> None:
            self.plugin.runner_lease_calls += 1

        def _release(self) -> None:
            self.plugin.runner_release_calls += 1
            if release_fails:
                raise RuntimeError("checkout release failed")

        def close(self) -> None:
            self.plugin.runner_close_calls += 1

    class FailingSQLRunner(CountingSQLiteRunner):
        def __init__(self, target: str, plugin: FailingPlugin) -> None:
            super().__init__(target, cast(CountingBackendPlugin, plugin))
            if not supports_lease:
                self.lease_thread_connection = None  # type: ignore[assignment]
                self.release_thread_connection = None  # type: ignore[assignment]

        def setup_with_stop_event(
            self,
            phase: Any,
            stop_event: threading.Event | None,
        ) -> None:
            del phase, stop_event
            raise creation_error

        def release_thread_connection(self) -> None:
            super().release_thread_connection()
            if release_fails:
                raise RuntimeError("checkout release failed")

    class FailingPlugin:
        name = f"failing-{branch}"
        sql = None if branch == "direct" else sqlite_backend_plugin.sql
        is_direct_backend = branch == "direct"

        def __init__(self) -> None:
            self.runner: DirectRunner | FailingSQLRunner | None = None
            self.runner_lease_calls = 0
            self.runner_release_calls = 0
            self.runner_close_calls = 0

        def create_runner(
            self,
            target: str,
            *args: Any,
            **kwargs: Any,
        ) -> DirectRunner | FailingSQLRunner:
            del args, kwargs
            if branch == "direct":
                self.runner = DirectRunner(self)
            else:
                self.runner = FailingSQLRunner(target, self)
            return self.runner

        def create_core_from_runner(self, *args: Any, **kwargs: Any) -> Any:
            raise creation_error

        def __getattr__(self, name: str) -> Any:
            return getattr(sqlite_backend_plugin, name)

    plugin = FailingPlugin()
    monkeypatch.setattr(
        "simplebroker._broker_session._target_parts",
        lambda db_path: (plugin.name, str(db_path), {}, plugin),
    )
    session = build_process_session(str(tmp_path / f"{branch}.db"))
    try:
        with pytest.raises(RuntimeError, match="core creation failed") as caught:
            session.get_connection(None)

        assert caught.value is creation_error
        assert plugin.runner_lease_calls == int(supports_lease)
        assert plugin.runner_release_calls == int(supports_lease)
        if release_fails:
            assert any(
                "checkout release failed" in note
                for note in getattr(caught.value, "__notes__", ())
            )
        assert session._active_operations == 0
        assert session._active_core_creations == 0
    finally:
        session.close_all()
    assert plugin.runner_close_calls == 1


def test_registry_shutdown_closes_live_sessions_and_tolerates_late_release(
    tmp_path: Path,
) -> None:
    registry = _ProcessBrokerSessionRegistry()
    key, session = registry.acquire(
        str(tmp_path / "registry.db"),
        factory_builder=_build_process_session_core_factory,
    )

    registry.close_all()
    registry.release(key)

    assert session._closed


def test_registry_builds_factory_only_for_new_session_key(tmp_path: Path) -> None:
    registry = _ProcessBrokerSessionRegistry()
    build_calls = 0

    def build_factory(spec: Any) -> Any:
        nonlocal build_calls
        build_calls += 1
        return _build_process_session_core_factory(spec)

    key_a, session_a = registry.acquire(
        str(tmp_path / "registry.db"),
        factory_builder=build_factory,
    )
    key_b, session_b = registry.acquire(
        str(tmp_path / "registry.db"),
        factory_builder=build_factory,
    )

    assert key_a == key_b
    assert session_a is session_b
    assert build_calls == 1

    registry.release(key_a)
    assert not session_a._closed
    registry.release(key_b)
    assert session_a._closed


def test_session_close_wins_race_with_core_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="close-race")
    session = build_process_session(target)
    core_created = threading.Event()
    allow_return = threading.Event()
    close_returned = threading.Event()
    errors: list[BaseException] = []
    original_setup = CountingSQLiteRunner.setup_with_stop_event
    delayed_once = False

    def delayed_setup(
        self: CountingSQLiteRunner,
        phase: Any,
        stop_event: threading.Event | None,
    ) -> None:
        nonlocal delayed_once
        if not delayed_once:
            delayed_once = True
            core_created.set()
            assert allow_return.wait(timeout=5.0)
        original_setup(self, phase, stop_event)

    def get_connection() -> None:
        try:
            session.get_connection(None, lease_operation=False)
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    monkeypatch.setattr(CountingSQLiteRunner, "setup_with_stop_event", delayed_setup)
    worker = threading.Thread(target=get_connection)
    worker.start()
    assert core_created.wait(timeout=5.0)

    def close_session() -> None:
        session.close_all()
        close_returned.set()

    close_thread = threading.Thread(target=close_session)
    close_thread.start()
    try:
        assert not close_returned.wait(timeout=0.1)
    finally:
        allow_return.set()
        worker.join(timeout=5.0)
        close_thread.join(timeout=5.0)

    assert not worker.is_alive()
    assert not close_thread.is_alive()
    assert close_returned.is_set()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert str(errors[0]) == "Broker session is closed"
    assert session._active_core_creations == 0
    assert counting_backend.create_runner_calls == 1
    assert counting_backend.runner_release_calls == 1
    assert counting_backend.runner_close_calls == 1


def test_non_sqlite_core_creation_after_close_does_not_retain_runner(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-030] exception
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creation_admitted = threading.Event()
    allow_creation = threading.Event()
    errors: list[BaseException] = []
    monkeypatch.setattr(
        "simplebroker._broker_session._CLOSE_ACTIVE_OPERATION_TIMEOUT",
        0.05,
    )

    class Runner:
        def __init__(self) -> None:
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    class Core:
        def close(self) -> None:
            return

        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class DirectPlugin:
        name = "race-direct"
        sql = None
        is_direct_backend = True

        def __init__(self, runner: Runner) -> None:
            self.runner = runner
            self.create_runner_calls = 0

        def create_runner(self, *args: Any, **kwargs: Any) -> Runner:
            self.create_runner_calls += 1
            creation_admitted.set()
            assert allow_creation.wait(timeout=5.0)
            return self.runner

        def create_core_from_runner(self, *args: Any, **kwargs: Any) -> Core:
            return Core()

    runner = Runner()
    plugin = DirectPlugin(runner)
    monkeypatch.setattr(
        "simplebroker._broker_session._target_parts",
        lambda db_path: ("race-direct", str(db_path), {}, plugin),
    )
    session = build_process_session("target")

    def get_connection() -> None:
        try:
            session.get_connection(None, lease_operation=False)
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    workers = [threading.Thread(target=get_connection) for _ in range(3)]
    for worker in workers:
        worker.start()
    assert creation_admitted.wait(timeout=5.0)
    with session._operation_condition:
        reached_deadline = time.monotonic() + 5.0
        while session._active_core_creations < 3:
            remaining = reached_deadline - time.monotonic()
            assert remaining > 0
            session._operation_condition.wait(timeout=remaining)

    session.close_all()
    assert session._closed
    allow_creation.set()
    for worker in workers:
        worker.join(timeout=5.0)

    assert all(not worker.is_alive() for worker in workers)
    assert len(errors) == 3
    assert all(isinstance(error, RuntimeError) for error in errors)
    assert {str(error) for error in errors} == {"Broker session is closed"}
    assert plugin.create_runner_calls == 1
    assert runner.close_calls == 1
    assert session._active_core_creations == 0
    session.close_all()
    assert runner.close_calls == 1


def test_factory_close_does_not_cancel_checkout_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_entered = threading.Event()
    allow_release = threading.Event()
    creation_error = RuntimeError("core creation failed")
    errors: list[BaseException] = []
    monkeypatch.setattr(
        "simplebroker._broker_session._CLOSE_ACTIVE_OPERATION_TIMEOUT",
        0.05,
    )

    class Runner:
        def __init__(self) -> None:
            self.lease_calls = 0
            self.release_calls = 0
            self.close_calls = 0

        def lease_thread_connection(self) -> None:
            self.lease_calls += 1

        def release_thread_connection(self) -> None:
            self.release_calls += 1
            release_entered.set()
            assert allow_release.wait(timeout=5.0)

        def close(self) -> None:
            self.close_calls += 1

    class DirectPlugin:
        name = "rollback-race"
        sql = None
        is_direct_backend = True

        def __init__(self, runner: Runner) -> None:
            self.runner = runner

        def create_runner(self, *args: Any, **kwargs: Any) -> Runner:
            return self.runner

        def create_core_from_runner(self, *args: Any, **kwargs: Any) -> Any:
            raise creation_error

    runner = Runner()
    plugin = DirectPlugin(runner)
    monkeypatch.setattr(
        "simplebroker._broker_session._target_parts",
        lambda db_path: ("rollback-race", str(db_path), {}, plugin),
    )
    session = build_process_session("target")

    def get_connection() -> None:
        try:
            session.get_connection(None, lease_operation=False)
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    worker = threading.Thread(target=get_connection)
    worker.start()
    assert release_entered.wait(timeout=5.0)

    session.close_all()
    assert session._closed
    assert runner.close_calls == 1
    assert worker.is_alive()

    allow_release.set()
    worker.join(timeout=5.0)

    assert not worker.is_alive()
    assert errors == [creation_error]
    assert runner.lease_calls == 1
    assert runner.release_calls == 1
    assert runner.close_calls == 1
    assert session._active_core_creations == 0
    assert not session._cores


def test_closed_factory_rejects_runner_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DirectPlugin:
        name = "closed-factory"
        sql = None
        is_direct_backend = True

        def __init__(self) -> None:
            self.create_runner_calls = 0

        def create_runner(self, *args: Any, **kwargs: Any) -> Any:
            self.create_runner_calls += 1
            raise AssertionError("closed factory allocated a runner")

    plugin = DirectPlugin()
    monkeypatch.setattr(
        "simplebroker._broker_session._target_parts",
        lambda db_path: ("closed-factory", str(db_path), {}, plugin),
    )
    factory = _build_process_session_core_factory(_session_spec("target", {}))
    factory.close()
    factory.close()

    with pytest.raises(RuntimeError, match="Broker session is closed"):
        factory.create(None)

    assert plugin.create_runner_calls == 0
