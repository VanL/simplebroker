"""Process-local persistent queue session sharing tests."""

from __future__ import annotations

import ast
import concurrent.futures as cf
import contextlib
import gc
import os
import sqlite3
import subprocess
import sys
import threading
import time
import weakref
from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import Any, cast

import pytest

import simplebroker._broker_session as broker_session_module
from simplebroker import (
    DEFAULT_CONFIG,
    BrokerSession,
    Config,
    ConfigField,
    Queue,
    open_broker,
    resolve_config,
)
from simplebroker._backend_plugins import BACKEND_ENTRY_POINT_GROUP
from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._broker_session import (
    _ProcessBrokerSession,
    _ProcessBrokerSessionRegistry,
    _session_key,
    _session_spec,
    close_process_broker_sessions,
)
from simplebroker._exceptions import StopException
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import BrokerTarget
from simplebroker.db import (
    BrokerCore,
    BrokerDB,
    DBConnection,
    _build_process_session_core_factory,
)
from tests.helper_scripts import drive_until, scale_timeout_for_ci

# External liveness valve for Event waits, joins, and barriers. Deadlock
# insurance only — never applied to injected product durations or
# elapsed-time assertion bounds, which stay exact.
_LIVENESS = scale_timeout_for_ci(30.0)


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
        self.runner_backend_options: list[dict[str, Any]] = []
        self.runner_configs: list[dict[str, Any]] = []

    def create_runner(
        self,
        target: str,
        *,
        backend_options: dict[str, Any] | None = None,
        config: Config | None = None,
    ) -> CountingSQLiteRunner:
        self.create_runner_calls += 1
        self.runner_backend_options.append(dict(backend_options or {}))
        self.runner_configs.append(dict(config or {}))
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
import atexit
import importlib
import sys

def report(stage):
    print(stage, flush=True)

# Registered before SimpleBroker imports, so LIFO atexit order makes this run
# after the process-session registry's close handler.
def report_after_process_session_atexit():
    assert broker_session_module._registry._entries == {}
    assert retained_session._closed is True
    report("process-session-atexit-complete")

atexit.register(report_after_process_session_atexit)

for module_name in sys.argv[2].split(","):
    importlib.import_module(module_name)
report("imports-complete")

from simplebroker import Queue

queue = Queue("jobs", db_path=sys.argv[1], persistent=True)
queue.write("payload")
report("write-complete")
broker_session_module = importlib.import_module("simplebroker._broker_session")
retained_session = queue.conn._shared_session
assert retained_session is not None
assert len(broker_session_module._registry._entries) == 1
queue._finalizer.detach()
report("queue-finalizer-detached")
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    # This probe owns interpreter shutdown and must not inherit coverage.py's
    # additional atexit hook. On Windows, automatic subprocess coverage can
    # hang during finalization and changes the boundary being tested.
    env.pop("COVERAGE_PROCESS_START", None)
    env.pop("COVERAGE_PROCESS_CONFIG", None)
    env.pop("COVERAGE_FILE", None)
    command = [
        sys.executable,
        "-c",
        script,
        str(tmp_path / "atexit.db"),
        ",".join(module_order),
    ]
    try:
        result = subprocess.run(
            command,
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            capture_output=True,
            text=True,
            timeout=10.0,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = (
            exc.stdout.decode(errors="replace")
            if isinstance(exc.stdout, bytes)
            else exc.stdout
        )
        stderr = (
            exc.stderr.decode(errors="replace")
            if isinstance(exc.stderr, bytes)
            else exc.stderr
        )
        pytest.fail(
            "Process-session import-order probe exceeded its deadlock valve; "
            f"stdout={stdout or ''!r}; stderr={stderr or ''!r}"
        )

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "imports-complete",
        "write-complete",
        "queue-finalizer-detached",
        "process-session-atexit-complete",
    ]
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
    config: Config | None = None,
) -> _ProcessBrokerSession:
    spec = _session_spec(
        db_path,
        resolve_config() if config is None else config,
    )
    return _ProcessBrokerSession(_build_process_session_core_factory(spec))


def test_broker_session_handles_and_queues_build_one_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="broker-session")
    first = BrokerSession.connect(target)
    second = BrokerSession.connect(target)
    queues = [first.queue(f"first-{index}") for index in range(3)]
    queues.extend(second.queue(f"second-{index}") for index in range(2))

    for queue in queues:
        queue.write("payload")

    assert counting_backend.create_runner_calls == 1
    first.close()
    assert counting_backend.runner_close_calls == 0
    second.close()
    assert counting_backend.runner_close_calls == 1


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
            start_barrier.wait(timeout=_LIVENESS)
            queues[index].write(f"message-{index}")

        with cf.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(write_once, index) for index in range(4)]
            drive_until(
                lambda: all(future.done() for future in futures),
                timeout=10.0,
                message="concurrent process-session first use did not settle",
                diagnostics=lambda: {
                    "barrier_waiting": start_barrier.n_waiting,
                    "create_runner_calls": counting_backend.create_runner_calls,
                    "future_states": [
                        {
                            "done": future.done(),
                            "running": future.running(),
                            "cancelled": future.cancelled(),
                        }
                        for future in futures
                    ],
                },
            )
            for future in futures:
                future.result()

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


def test_persistent_sqlite_target_does_not_silently_discard_backend_options(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "sqlite-options.db"
    with pytest.raises(
        ValueError,
        match="SQLite backend does not support backend_options",
    ):
        Queue(
            "jobs",
            db_path=BrokerTarget(
                "sqlite",
                str(db_path),
                {"pool": {"size": 2}},
            ),
            persistent=True,
        )

    assert not db_path.exists()


def test_ephemeral_sqlite_target_does_not_silently_discard_backend_options(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ephemeral-sqlite-options.db"
    queue = Queue(
        "jobs",
        db_path=BrokerTarget(
            "sqlite",
            str(db_path),
            {"pool": {"size": 2}},
        ),
    )

    with pytest.raises(
        ValueError,
        match="SQLite backend does not support backend_options",
    ):
        queue.write("payload")

    assert not db_path.exists()


def test_open_broker_sqlite_target_does_not_silently_discard_backend_options(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "open-broker-sqlite-options.db"
    target = BrokerTarget(
        "sqlite",
        str(db_path),
        {"pool": {"size": 2}},
    )

    with (
        pytest.raises(
            ValueError,
            match="SQLite backend does not support backend_options",
        ),
        open_broker(target),
    ):
        pass

    assert not db_path.exists()


@pytest.mark.parametrize(
    ("left_value", "right_value"),
    [(True, 1), (1, 1.0), (True, 1.0)],
)
def test_type_distinct_backend_options_do_not_share_process_session(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
    left_value: object,
    right_value: object,
) -> None:
    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(
            Queue(
                "a",
                db_path=counting_target(tmp_path, mode=left_value),
                persistent=True,
            )
        )
        queue_b = stack.enter_context(
            Queue(
                "b",
                db_path=counting_target(tmp_path, mode=right_value),
                persistent=True,
            )
        )
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_same_repr_opaque_options_do_not_share_process_session(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    class SameRepr:
        def __repr__(self) -> str:
            return "same-repr"

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(
            Queue(
                "a",
                db_path=counting_target(tmp_path, opaque=SameRepr()),
                persistent=True,
            )
        )
        queue_b = stack.enter_context(
            Queue(
                "b",
                db_path=counting_target(tmp_path, opaque=SameRepr()),
                persistent=True,
            )
        )
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_opaque_session_identity_retains_its_object() -> None:
    class Opaque:
        pass

    opaque = Opaque()
    retained = weakref.ref(opaque)
    key = _session_key(
        BrokerTarget("sqlite", "opaque-key.db", {"opaque": opaque}),
        resolve_config(override={}),
    )

    del opaque
    gc.collect()

    assert retained() is not None
    del key
    gc.collect()
    assert retained() is None


def test_list_and_tuple_options_do_not_share_process_session(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(
            Queue(
                "a",
                db_path=counting_target(tmp_path, nested=["value"]),
                persistent=True,
            )
        )
        queue_b = stack.enter_context(
            Queue(
                "b",
                db_path=counting_target(tmp_path, nested=("value",)),
                persistent=True,
            )
        )
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_mapping_and_set_permutations_share_process_session(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target_a = counting_target(
        tmp_path,
        nested={"second": [2], "first": [1]},
        members={"beta", "alpha"},
    )
    target_b = counting_target(
        tmp_path,
        members={"alpha", "beta"},
        nested={"first": [1], "second": [2]},
    )

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(Queue("a", db_path=target_a, persistent=True))
        queue_b = stack.enter_context(Queue("b", db_path=target_b, persistent=True))
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 1


def test_session_key_and_lazy_factory_share_one_recursive_snapshot(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    opaque = object()
    target = counting_target(
        tmp_path,
        pool={"hosts": ["primary"]},
        opaque=opaque,
    )
    metadata = {"labels": ["original"]}
    config = resolve_config(override={"BROKER_EMBEDDER_METADATA": metadata})
    queue_a = Queue(
        "a",
        db_path=target,
        persistent=True,
        config=config,
    )

    target.backend_options["pool"]["hosts"].append("mutated")
    # Config is lightly frozen; callers retain ownership of nested application data.
    # Only target options are captured recursively by session acquisition.

    original_target = counting_target(
        tmp_path,
        pool={"hosts": ["primary"]},
        opaque=opaque,
    )
    original_config = resolve_config(
        override={"BROKER_EMBEDDER_METADATA": {"labels": ["original"]}}
    )
    queue_b = Queue(
        "b",
        db_path=original_target,
        persistent=True,
        config=original_config,
    )

    try:
        queue_a.write("one")
        queue_b.write("two")
    finally:
        queue_a.close()
        queue_b.close()

    assert counting_backend.create_runner_calls == 1
    assert counting_backend.runner_backend_options[0]["pool"] == {"hosts": ["primary"]}
    assert counting_backend.runner_backend_options[0]["opaque"] is opaque
    assert counting_backend.runner_configs[0]["EMBEDDER_METADATA"] == {
        "labels": ["original"]
    }


def test_ephemeral_queue_detaches_target_input_and_reporting(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, pool={"hosts": ["primary"]})
    with Queue("jobs", db_path=target) as queue:
        queue.write("first")
        reported = queue.db_target
        assert isinstance(reported, BrokerTarget)
        reported.backend_options["pool"]["hosts"].append("reported-edit")
        target.backend_options["pool"]["hosts"].append("caller-edit")
        queue.write("second")

        current = queue.db_target
        assert isinstance(current, BrokerTarget)
        assert current.backend_options == {"pool": {"hosts": ["primary"]}}
        assert queue.peek_many(limit=2) == ["first", "second"]

    assert counting_backend.runner_backend_options
    assert all(
        options["pool"] == {"hosts": ["primary"]}
        for options in counting_backend.runner_backend_options
    )


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
                config=resolve_config(override={"BROKER_BUSY_TIMEOUT": 1000}),
            )
        )
        queue_b = stack.enter_context(
            Queue(
                "b",
                db_path=target,
                persistent=True,
                config=resolve_config(override={"BROKER_BUSY_TIMEOUT": 2000}),
            )
        )
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_type_distinct_config_extras_do_not_share_process_session(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")
    config_a = resolve_config(override={"BROKER_EMBEDDER_METADATA": True})
    config_b = resolve_config(override={"BROKER_EMBEDDER_METADATA": 1})

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(
            Queue("a", db_path=target, persistent=True, config=config_a)
        )
        queue_b = stack.enter_context(
            Queue("b", db_path=target, persistent=True, config=config_b)
        )
        queue_a.write("one")
        queue_b.write("two")

    assert counting_backend.create_runner_calls == 2


def test_opaque_extra_participates_in_process_session_identity(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")
    config_a = resolve_config(override={"BROKER_EMBEDDER_METADATA": "a"})
    config_b = resolve_config(override={"BROKER_EMBEDDER_METADATA": "b"})

    with contextlib.ExitStack() as stack:
        queue_a = stack.enter_context(
            Queue("a", db_path=target, persistent=True, config=config_a)
        )
        queue_b = stack.enter_context(
            Queue("b", db_path=target, persistent=True, config=config_b)
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


@pytest.mark.parametrize("explicit_cleanup", [False, True])
def test_worker_queue_close_retains_cache_until_explicit_cleanup_or_session_end(
    tmp_path: Path,
    explicit_cleanup: bool,
) -> None:
    target = str(tmp_path / f"worker-explicit-cleanup-{explicit_cleanup}.db")
    anchor = Queue("anchor", db_path=target, persistent=True)
    assert anchor.conn is not None
    session = anchor.conn._shared_session
    assert session is not None
    raw_connections: list[sqlite3.Connection] = []
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            with Queue(f"jobs-{index}", db_path=target, persistent=True) as queue:
                queue.write("payload")
                assert queue.conn is not None
                core = cast(BrokerDB, queue.conn.get_core())
                raw_connections.append(
                    cast(SQLiteRunner, core._runner).get_connection()
                )
                if explicit_cleanup:
                    queue.cleanup_connections()
        except BaseException as failure:  # pragma: no cover - asserted in parent  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(failure)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_LIVENESS)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert len(session._cores) == (0 if explicit_cleanup else 5)
    for raw_connection in raw_connections:
        if explicit_cleanup:
            with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
                raw_connection.execute("SELECT 1")
        else:
            raw_connection.execute("SELECT 1")

    anchor.close()
    assert session._closed
    for raw_connection in raw_connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            raw_connection.execute("SELECT 1")


def test_retired_shared_sql_core_close_and_finalizer_are_idempotent(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")
    first = Queue("first", db_path=target, persistent=True)
    second = Queue("second", db_path=target, persistent=True)
    first.write("one")
    assert first.conn is not None
    old_core = cast(BrokerCore, first.conn.get_core())

    try:
        first.cleanup_connections()
        assert counting_backend.runner_release_calls == 1

        second.write("two")
        assert counting_backend.runner_lease_calls == 2

        old_core.close()
        assert counting_backend.runner_release_calls == 1
        old_core_ref = weakref.ref(old_core)
        del old_core
        gc.collect()
        assert old_core_ref() is None
        assert counting_backend.runner_release_calls == 1

        second.write("three")
    finally:
        first.close()
        second.close()

    assert counting_backend.runner_release_calls == 2


def test_never_used_persistent_queue_close_does_not_create_runner(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    queue = Queue(
        "unused",
        db_path=counting_target(tmp_path, schema="unused"),
        persistent=True,
    )

    queue.cleanup_connections()
    queue.close()
    queue.close()

    assert counting_backend.create_runner_calls == 0
    assert counting_backend.runner_lease_calls == 0
    assert counting_backend.runner_release_calls == 0


def test_cleanup_then_close_releases_same_thread_core_once(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="cleanup-then-close")
    first = Queue("first", db_path=target, persistent=True)
    sibling = Queue("sibling", db_path=target, persistent=True)
    first.write("one")

    try:
        first.cleanup_connections()
        assert counting_backend.runner_release_calls == 1
        first.close()
        first.close()
        assert counting_backend.runner_release_calls == 1
        sibling.write("two")
        assert counting_backend.runner_lease_calls == 2
    finally:
        first.close()
        sibling.close()

    assert counting_backend.runner_release_calls == 2


def test_queue_finalizer_does_not_release_collector_thread_core(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "foreign-finalizer.db")
    anchor = Queue("anchor", db_path=target, persistent=True)
    assert anchor.conn is not None
    session = anchor.conn._shared_session
    assert session is not None
    queue_refs: list[weakref.ReferenceType[Queue]] = []
    worker_cores: list[BrokerDB] = []
    worker_connections: list[sqlite3.Connection] = []
    gc_was_enabled = gc.isenabled()
    gc.disable()

    def abandon_cyclic_queue() -> None:
        queue = Queue("worker", db_path=target, persistent=True)
        queue.write("one")
        assert queue.conn is not None
        core = cast(BrokerDB, queue.conn.get_core())
        runner = cast(SQLiteRunner, core._runner)
        worker_cores.append(core)
        worker_connections.extend(runner._all_connections)
        queue._test_cycle = queue  # type: ignore[attr-defined]
        queue_refs.append(weakref.ref(queue))

    worker = threading.Thread(target=abandon_cyclic_queue)
    worker.start()
    worker.join(timeout=_LIVENESS)
    assert not worker.is_alive()

    main_core = cast(BrokerDB, anchor.conn.get_core())
    main_runner = cast(SQLiteRunner, main_core._runner)
    main_connection = main_runner.get_connection()
    assert main_core in session._cores

    try:
        gc.collect()
        assert queue_refs[0]() is None
        assert main_core in session._cores
        assert session._thread_local.core is main_core
        main_connection.execute("SELECT 1")
        assert worker_cores[0] in session._cores
        for connection in worker_connections:
            connection.execute("SELECT 1")
    finally:
        if gc_was_enabled:
            gc.enable()
        anchor.close()

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        main_connection.execute("SELECT 1")
    for connection in worker_connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            connection.execute("SELECT 1")


def test_queue_finalizer_on_worker_does_not_release_collector_core(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "worker-collector-finalizer.db")
    anchor = Queue("anchor", db_path=target, persistent=True)
    collector_queue = Queue("collector", db_path=target, persistent=True)
    assert anchor.conn is not None
    session = anchor.conn._shared_session
    assert session is not None
    abandoned_refs: list[weakref.ReferenceType[Queue]] = []
    abandoned_cores: list[BrokerDB] = []
    abandoned_connections: list[sqlite3.Connection] = []
    observations: dict[str, object] = {}
    errors: list[BaseException] = []
    gc_was_enabled = gc.isenabled()
    gc.disable()

    def abandon() -> None:
        queue = Queue("abandoned", db_path=target, persistent=True)
        queue.write("message")
        assert queue.conn is not None
        core = cast(BrokerDB, queue.conn.get_core())
        abandoned_cores.append(core)
        abandoned_connections.extend(cast(SQLiteRunner, core._runner)._all_connections)
        queue._test_cycle = queue  # type: ignore[attr-defined]
        abandoned_refs.append(weakref.ref(queue))

    def collect_on_worker() -> None:
        try:
            collector_queue.write("collector")
            assert collector_queue.conn is not None
            collector_core = cast(BrokerDB, collector_queue.conn.get_core())
            collector_raw = cast(SQLiteRunner, collector_core._runner).get_connection()
            gc.collect()
            observations["abandoned_collected"] = abandoned_refs[0]() is None
            observations["collector_owned"] = collector_core in session._cores
            observations["abandoned_owned"] = abandoned_cores[0] in session._cores
            observations["same_core"] = (
                collector_queue.conn.get_core() is collector_core
            )
            collector_raw.execute("SELECT 1")
            for connection in abandoned_connections:
                connection.execute("SELECT 1")
            collector_queue.close()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    owner = threading.Thread(target=abandon)
    collector = threading.Thread(target=collect_on_worker)
    owner.start()
    owner.join(timeout=_LIVENESS)
    collector.start()
    collector.join(timeout=_LIVENESS)
    try:
        assert not owner.is_alive()
        assert not collector.is_alive()
        assert errors == []
        assert observations == {
            "abandoned_collected": True,
            "abandoned_owned": True,
            "collector_owned": True,
            "same_core": True,
        }
    finally:
        if gc_was_enabled:
            gc.enable()
        collector_queue.close()
        anchor.close()

    for connection in abandoned_connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            connection.execute("SELECT 1")


def test_cleanup_defers_caller_thread_release_until_outer_operation_exits(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")
    first = DBConnection(target, share_in_process=True)
    second = DBConnection(target, share_in_process=True)
    session = first._shared_session
    assert session is not None
    leased_core = first.get_connection()

    try:
        first.cleanup()
        assert session._thread_local.core is leased_core
        assert counting_backend.runner_release_calls == 0

        first.release_connection_after_use()
        assert not hasattr(session._thread_local, "core")
        assert counting_backend.runner_release_calls == 1
    finally:
        first.close()
        second.close()


def test_close_does_not_release_another_threads_core(tmp_path: Path) -> None:
    target = str(tmp_path / "foreign-core.db")
    main_queue = Queue("main", db_path=target, persistent=True)
    assert main_queue.conn is not None
    session = main_queue.conn._shared_session
    assert session is not None
    worker_ready = threading.Event()
    allow_worker = threading.Event()
    worker_cores: list[BrokerCore] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            with Queue("worker", db_path=target, persistent=True) as queue:
                queue.write("one")
                assert queue.conn is not None
                core = cast(BrokerCore, queue.conn.get_core())
                worker_cores.append(core)
                worker_ready.set()
                assert allow_worker.wait(timeout=_LIVENESS)
                queue.write("two")
                assert queue.conn.get_core() is core
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert worker_ready.wait(timeout=_LIVENESS)
    main_queue.write("main")
    main_core = cast(BrokerCore, main_queue.conn.get_core())

    try:
        main_queue.close()
        assert main_core in session._cores
        assert worker_cores[0] in session._cores
    finally:
        allow_worker.set()
        thread.join(timeout=_LIVENESS)
        main_queue.close()

    assert not thread.is_alive()
    assert errors == []
    assert session._closed


def test_anchorless_main_contexts_reuse_core_while_worker_retains_session(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "main-controller-reuse.db")
    worker_ready = threading.Event()
    release_worker = threading.Event()
    worker_session: list[_ProcessBrokerSession] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            retained = Queue("worker", db_path=target, persistent=True)
            assert retained.conn is not None
            session = retained.conn._shared_session
            assert session is not None
            worker_session.append(session)
            worker_ready.set()
            assert release_worker.wait(timeout=_LIVENESS)
            retained.close()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert worker_ready.wait(timeout=_LIVENESS)
    session = worker_session[0]
    cores: list[BrokerDB] = []
    raw_connection: sqlite3.Connection | None = None
    try:
        for index in range(3):
            with Queue("main", db_path=target, persistent=True) as queue:
                queue.write(str(index))
                assert queue.conn is not None
                core = cast(BrokerDB, queue.conn.get_core())
                cores.append(core)
                if raw_connection is None:
                    raw_connection = cast(SQLiteRunner, core._runner).get_connection()
            assert core in session._cores
            assert raw_connection is not None
            raw_connection.execute("SELECT 1")
        assert len({id(core) for core in cores}) == 1
    finally:
        release_worker.set()
        thread.join(timeout=_LIVENESS)

    assert not thread.is_alive()
    assert errors == []
    assert session._closed
    assert raw_connection is not None
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        raw_connection.execute("SELECT 1")


def test_failed_nested_acquisition_does_not_release_outer_operation(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "failed-nested-acquisition.db")
    queue = Queue("jobs", db_path=target, persistent=True)
    stop_event = threading.Event()
    queue.set_stop_event(stop_event)
    observations: dict[str, object] = {}
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            assert queue.conn is not None
            session = queue.conn._shared_session
            assert session is not None
            with queue.get_connection() as outer_core:
                runner = cast(SQLiteRunner, cast(BrokerDB, outer_core)._runner)
                raw_connection = runner.get_connection()
                queue.cleanup_connections()
                stop_event.set()
                with (
                    pytest.raises(StopException, match="interrupted"),
                    queue.get_connection(),
                ):
                    pass
                observations["depth"] = getattr(
                    session._thread_local,
                    "operation_depth",
                    None,
                )
                observations["active"] = session._active_operations
                observations["core_owned"] = outer_core in session._cores
                raw_connection.execute("SELECT 1")
            observations["released_after_outer"] = outer_core not in session._cores
            stop_event.clear()
            queue.close()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=_LIVENESS)

    assert not thread.is_alive()
    assert errors == []
    assert observations == {
        "depth": 1,
        "active": 1,
        "core_owned": True,
        "released_after_outer": True,
    }


def test_terminal_timeout_does_not_close_core_during_active_disposal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disposal_started = threading.Event()
    allow_disposal = threading.Event()

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class Factory:
        def __init__(self) -> None:
            self.close_core_calls = 0
            self.close_calls = 0

        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            return Core()

        def close_core(self, core: Core) -> None:
            del core
            self.close_core_calls += 1
            disposal_started.set()
            assert allow_disposal.wait(timeout=_LIVENESS)

        def close(self) -> None:
            self.close_calls += 1

    factory = Factory()
    session = _ProcessBrokerSession(cast(Any, factory))
    monkeypatch.setattr(broker_session_module, "_CLOSE_ACTIVE_OPERATION_TIMEOUT", 0.0)

    def dispose_on_worker() -> None:
        session.get_connection(None, lease_operation=False)
        session.cleanup_current_thread()

    executor = cf.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(dispose_on_worker)
    try:
        assert disposal_started.wait(timeout=_LIVENESS)
        session.close_all()
        assert factory.close_core_calls == 1
        assert factory.close_calls == 1
    finally:
        allow_disposal.set()
        future.result(timeout=_LIVENESS)
        executor.shutdown()

    assert session._active_operations == 0
    assert factory.close_core_calls == 1


class _InjectedLifecycleAbort(BaseException):
    pass


@pytest.mark.parametrize("deferred", [False, True])
def test_interruption_after_core_claim_keeps_ownership_and_balances_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    deferred: bool,
) -> None:
    session = build_process_session(str(tmp_path / f"claim-abort-{deferred}.db"))
    core = cast(BrokerDB, session.get_connection(None, lease_operation=deferred))
    runner = cast(SQLiteRunner, core._runner)
    raw_connection = runner.get_connection()
    helper_name = (
        "_claim_pending_cleanup_locked"
        if deferred
        else "_request_current_thread_cleanup_locked"
    )
    real_helper = getattr(session, helper_name)

    if deferred:
        session.cleanup_current_thread()

    def interrupt_after_claim(*args: object, **kwargs: object) -> object:
        real_helper(*args, **kwargs)
        raise _InjectedLifecycleAbort("after claim")

    monkeypatch.setattr(session, helper_name, interrupt_after_claim)
    try:
        with pytest.raises(_InjectedLifecycleAbort, match="after claim"):
            if deferred:
                session.release_current_thread_connection()
            else:
                session.cleanup_current_thread()

        assert session._active_operations == 0
        assert core in session._cores
        raw_connection.execute("SELECT 1")
    finally:
        monkeypatch.setattr(session, helper_name, real_helper)
        with session._operation_condition:
            session._active_operations = 0
            session._cores.add(core)
        session.close_all()

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        raw_connection.execute("SELECT 1")


@pytest.mark.parametrize("interrupt_after", ["session-get", "stack-push"])
def test_shared_acquisition_interruption_balances_only_its_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupt_after: str,
) -> None:
    connection = DBConnection(
        str(tmp_path / f"acquisition-abort-{interrupt_after}.db"),
        share_in_process=True,
    )
    session = connection._shared_session
    assert session is not None
    outer_core = cast(BrokerDB, connection.get_connection())
    outer_raw = cast(SQLiteRunner, outer_core._runner).get_connection()
    connection.cleanup()
    assert session.current_thread_operation_depth() == 1
    assert connection._shared_operation_stack_depth() == 1
    real_get = session.get_connection
    real_push = connection._push_shared_operation_session

    if interrupt_after == "session-get":

        def interrupted_get(
            stop_event: threading.Event | None,
            *,
            lease_operation: bool = True,
        ) -> object:
            real_get(stop_event, lease_operation=lease_operation)
            raise _InjectedLifecycleAbort("after session get")

        monkeypatch.setattr(session, "get_connection", interrupted_get)
    else:

        def interrupted_push(pushed_session: _ProcessBrokerSession) -> None:
            real_push(pushed_session)
            raise _InjectedLifecycleAbort("after stack push")

        monkeypatch.setattr(
            connection, "_push_shared_operation_session", interrupted_push
        )

    try:
        with pytest.raises(_InjectedLifecycleAbort, match="after"):
            connection.get_connection()
        assert session._active_operations == 1
        assert session.current_thread_operation_depth() == 1
        assert connection._shared_operation_stack_depth() == 1
        assert getattr(session._thread_local, "cleanup_pending", None) is not None
        assert outer_core in session._cores
        outer_raw.execute("SELECT 1")
    finally:
        monkeypatch.setattr(session, "get_connection", real_get)
        monkeypatch.setattr(connection, "_push_shared_operation_session", real_push)
        while connection._pop_shared_operation_session() is not None:
            session.release_current_thread_connection()
        while getattr(session._thread_local, "operation_depth", 0) > 0:
            session.release_current_thread_connection()
        connection.close()


def test_repeated_pending_cleanup_releases_once_after_nested_operations(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    session = build_process_session(counting_target(tmp_path, schema="nested"))
    first = session.get_connection(None)
    assert session.get_connection(None) is first

    session.cleanup_current_thread()
    session.cleanup_current_thread()
    session.release_current_thread_connection()
    assert session._thread_local.core is first
    assert counting_backend.runner_release_calls == 0

    session.release_current_thread_connection()
    assert not hasattr(session._thread_local, "core")
    assert counting_backend.runner_release_calls == 1
    session.close_all()


def test_deferred_close_failure_keeps_active_exception_primary() -> None:
    body_failure = ValueError("operation failed")

    class CleanupFailure(RuntimeError):
        def __bool__(self) -> bool:
            raise AssertionError("exception truthiness must not be evaluated")

    cleanup_failure = CleanupFailure("deferred cleanup failed")

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class Factory:
        def __init__(self) -> None:
            self.fail_cleanup = True

        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            return Core()

        def close_core(self, core: Core) -> None:
            del core
            if self.fail_cleanup:
                raise cleanup_failure

        def close(self) -> None:
            return

    factory = Factory()
    session = _ProcessBrokerSession(cast(Any, factory))
    core = session.get_connection(None)
    session.cleanup_current_thread()

    with pytest.raises(ValueError, match="operation failed") as caught:
        try:
            raise body_failure
        finally:
            session.release_current_thread_connection(active_failure=body_failure)

    assert caught.value is body_failure
    assert getattr(body_failure, "__notes__", []) == [
        (
            "Additional process-session cleanup failure: "
            f"{type(cleanup_failure).__qualname__}: deferred cleanup failed"
        )
    ]
    assert session._active_operations == 0
    assert session._cores == {core}
    assert not hasattr(session._thread_local, "core")

    factory.fail_cleanup = False
    session.close_all()


def test_deferred_close_failure_propagates_after_successful_operation() -> None:
    cleanup_failure = RuntimeError("deferred cleanup failed")

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class Factory:
        def __init__(self) -> None:
            self.fail_cleanup = True

        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            return Core()

        def close_core(self, core: Core) -> None:
            del core
            if self.fail_cleanup:
                raise cleanup_failure

        def close(self) -> None:
            return

    factory = Factory()
    session = _ProcessBrokerSession(cast(Any, factory))
    core = session.get_connection(None)
    session.cleanup_current_thread()

    with pytest.raises(RuntimeError, match="deferred cleanup failed") as caught:
        session.release_current_thread_connection()

    assert caught.value is cleanup_failure
    assert session._active_operations == 0
    assert session._cores == {core}
    factory.fail_cleanup = False
    session.close_all()


def test_iterator_close_propagates_deferred_core_disposal_failure(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "iterator-disposal-failure.db")
    queue = Queue("jobs", db_path=target, persistent=True)
    sibling = Queue("sibling", db_path=target, persistent=True)
    queue.write("one")
    iterator = queue.read_generator()
    assert next(iterator) == "one"
    assert queue.conn is not None
    session = queue.conn._shared_session
    assert session is not None
    original_close_core = session._factory.close_core
    disposal_failure = RuntimeError("iterator disposal failed")

    def fail_close_core(core: Any) -> None:
        del core
        raise disposal_failure

    session._factory.close_core = fail_close_core  # type: ignore[method-assign]
    queue.cleanup_connections()
    try:
        with pytest.raises(RuntimeError, match="iterator disposal failed") as caught:
            iterator.close()
        assert caught.value is disposal_failure
        assert session._active_operations == 0
        assert len(session._cores) == 1
    finally:
        session._factory.close_core = original_close_core  # type: ignore[method-assign]
        iterator.close()
        queue.close()
        sibling.close()


@pytest.mark.parametrize("deferred", [False, True])
def test_caller_thread_disposal_base_exception_remains_owned_for_retry(
    deferred: bool,
) -> None:
    interruption = KeyboardInterrupt("disposal interrupted")

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class Factory:
        def __init__(self) -> None:
            self.fail = True
            self.close_calls = 0

        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            return Core()

        def close_core(self, core: Core) -> None:
            del core
            self.close_calls += 1
            if self.fail:
                raise interruption

        def close(self) -> None:
            return

    factory = Factory()
    session = _ProcessBrokerSession(cast(Any, factory))
    core = session.get_connection(None, lease_operation=deferred)
    if deferred:
        session.cleanup_current_thread()

    with pytest.raises(KeyboardInterrupt, match="disposal interrupted") as caught:
        if deferred:
            session.release_current_thread_connection()
        else:
            session.cleanup_current_thread()

    assert caught.value is interruption
    assert session._active_operations == 0
    assert core in session._cores
    assert not hasattr(session._thread_local, "core")

    factory.fail = False
    session.close_all()
    assert factory.close_calls == 2


def test_unmatched_release_does_not_consume_another_threads_operation() -> None:
    operation_started = threading.Event()
    allow_release = threading.Event()
    errors: list[BaseException] = []

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class Factory:
        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            return Core()

        def close_core(self, core: Core) -> None:
            del core

        def close(self) -> None:
            return

    session = _ProcessBrokerSession(cast(Any, Factory()))

    def worker() -> None:
        try:
            session.get_connection(None)
            operation_started.set()
            assert allow_release.wait(timeout=_LIVENESS)
            session.release_current_thread_connection()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert operation_started.wait(timeout=_LIVENESS)
    session.release_current_thread_connection()
    assert session._active_operations == 1
    allow_release.set()
    thread.join(timeout=_LIVENESS)
    assert not thread.is_alive()
    assert errors == []
    assert session._active_operations == 0
    session.close_all()


def test_terminal_timeout_does_not_dispose_pending_core_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_started = threading.Event()
    allow_release = threading.Event()
    errors: list[BaseException] = []
    retained_tls_core: list[bool] = []

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class Factory:
        def __init__(self) -> None:
            self.close_calls = 0

        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            return Core()

        def close_core(self, core: Core) -> None:
            del core
            self.close_calls += 1
            if self.close_calls > 1:
                raise RuntimeError("core disposed twice")

        def close(self) -> None:
            return

    factory = Factory()
    session = _ProcessBrokerSession(cast(Any, factory))

    def worker() -> None:
        try:
            session.get_connection(None)
            session.cleanup_current_thread()
            operation_started.set()
            assert allow_release.wait(timeout=_LIVENESS)
            session.release_current_thread_connection()
            retained_tls_core.append(hasattr(session._thread_local, "core"))
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert operation_started.wait(timeout=_LIVENESS)
    monkeypatch.setattr(broker_session_module, "_CLOSE_ACTIVE_OPERATION_TIMEOUT", 0)
    session.close_all()
    assert factory.close_calls == 1
    allow_release.set()
    thread.join(timeout=_LIVENESS)
    assert not thread.is_alive()
    assert errors == []
    assert retained_tls_core == [False]
    assert factory.close_calls == 1
    assert session._active_operations == 0


def test_failure_release_argument_is_reentrant_without_tls_marker(
    tmp_path: Path,
) -> None:
    connection = DBConnection(
        str(tmp_path / "reentrant-release.db"), share_in_process=True
    )
    outer = ValueError("outer")
    inner = RuntimeError("inner")
    observed: list[BaseException | None] = []

    def reentrant_release(*, active_failure: BaseException | None = None) -> None:
        observed.append(active_failure)
        if len(observed) == 1:
            connection.release_connection_after_use(active_failure=inner)

    connection.release_connection_after_use = reentrant_release  # type: ignore[method-assign]
    try:
        connection.release_connection_after_use(active_failure=outer)
    finally:
        connection.close()

    assert observed == [outer, inner]
    assert not hasattr(connection._thread_local, "release_active_failure")


def test_successful_queue_operation_inside_except_propagates_cleanup_failure(
    tmp_path: Path,
) -> None:
    target = str(tmp_path / "handled-exception.db")
    first = Queue("first", db_path=target, persistent=True)
    second = Queue("second", db_path=target, persistent=True)
    first.write("one")
    assert first.conn is not None
    session = first.conn._shared_session
    assert session is not None
    original_close_core = session._factory.close_core

    def fail_close_core(core: Any) -> None:
        del core
        raise RuntimeError("deferred cleanup failed")

    session._factory.close_core = fail_close_core  # type: ignore[method-assign]
    try:
        try:
            raise ValueError("already handled")
        except ValueError:
            with (
                pytest.raises(RuntimeError, match="deferred cleanup failed"),
                first.get_connection(),
            ):
                second.cleanup_connections()
    finally:
        session._factory.close_core = original_close_core  # type: ignore[method-assign]
        first.close()
        second.close()


def test_failed_shared_core_release_is_not_reused_and_retries_at_session_close(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="release-retry")
    first = Queue("first", db_path=target, persistent=True)
    second = Queue("second", db_path=target, persistent=True)
    first.write("one")
    first_conn = first.conn
    second_conn = second.conn
    assert first_conn is not None
    assert second_conn is not None
    old_core = cast(BrokerCore, first_conn.get_core())
    runner = cast(CountingSQLiteRunner, old_core._runner)
    session = first_conn._shared_session
    assert session is not None
    original_release = runner.release_thread_connection
    release_attempts = 0

    def fail_once_before_release() -> None:
        nonlocal release_attempts
        release_attempts += 1
        if release_attempts == 1:
            raise RuntimeError("checkout release failed")
        original_release()

    runner.release_thread_connection = fail_once_before_release  # type: ignore[method-assign]
    try:
        with pytest.raises(RuntimeError, match="checkout release failed"):
            first.cleanup_connections()
        assert old_core in session._cores
        assert not hasattr(session._thread_local, "core")

        new_core = second_conn.get_core()
        assert new_core is not old_core
    finally:
        first.close()
        second.close()

    assert release_attempts == 3
    assert counting_backend.runner_release_calls == 1
    assert session._closed


@pytest.mark.parametrize("fail_first_disposal", [False, True])
def test_final_session_close_waits_for_caller_thread_core_disposal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fail_first_disposal: bool,
) -> None:
    session = build_process_session(str(tmp_path / "disposal-drain.db"))
    disposal_entered = threading.Event()
    allow_disposal = threading.Event()
    cleanup_returned = threading.Event()
    close_waiting = threading.Event()
    close_returned = threading.Event()
    errors: list[BaseException] = []
    connections: list[sqlite3.Connection] = []
    original_shutdown = BrokerDB.shutdown
    shutdown_calls = 0

    def delayed_shutdown(core: BrokerDB) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        disposal_entered.set()
        assert allow_disposal.wait(timeout=_LIVENESS)
        if fail_first_disposal and shutdown_calls == 1:
            raise RuntimeError("caller-thread disposal failed")
        original_shutdown(core)

    monkeypatch.setattr(BrokerDB, "shutdown", delayed_shutdown)

    def cleanup_owner() -> None:
        try:
            core = cast(BrokerDB, session.get_connection(None, lease_operation=False))
            runner = cast(SQLiteRunner, core._runner)
            connections.extend(runner._all_connections)
            session.cleanup_current_thread()
            cleanup_returned.set()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    worker = threading.Thread(target=cleanup_owner)
    worker.start()
    assert disposal_entered.wait(timeout=_LIVENESS)

    original_wait = session._operation_condition.wait

    def observe_wait(timeout: float | None = None) -> bool:
        close_waiting.set()
        return original_wait(timeout)

    session._operation_condition.wait = observe_wait  # type: ignore[method-assign]

    def close_session() -> None:
        try:
            session.close_all()
            close_returned.set()
        except BaseException as exc:  # pragma: no cover - asserted in parent thread  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    closer = threading.Thread(target=close_session)
    closer.start()
    try:
        assert close_waiting.wait(timeout=_LIVENESS)
        assert not cleanup_returned.is_set()
        assert not close_returned.is_set()
    finally:
        allow_disposal.set()
        worker.join(timeout=_LIVENESS)
        closer.join(timeout=_LIVENESS)

    assert not worker.is_alive()
    assert not closer.is_alive()
    assert close_returned.is_set()
    if fail_first_disposal:
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)
        assert str(errors[0]) == "caller-thread disposal failed"
        assert not cleanup_returned.is_set()
        assert shutdown_calls == 2
    else:
        assert errors == []
        assert cleanup_returned.is_set()
        assert shutdown_calls == 1
    assert connections
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            connection.execute("SELECT 1")


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


def test_session_managed_hookless_runner_closes_only_with_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HooklessRunner(SQLiteRunner):
        def __init__(self, target: str) -> None:
            self.close_calls = 0
            super().__init__(target)

        def close(self) -> None:
            self.close_calls += 1
            super().close()

    class HooklessPlugin:
        name = "hookless"
        sql = sqlite_backend_plugin.sql
        backend_api_version = sqlite_backend_plugin.backend_api_version
        schema_version = sqlite_backend_plugin.schema_version

        def __init__(self) -> None:
            self.runner: HooklessRunner | None = None

        def create_runner(
            self,
            target: str,
            *,
            backend_options: dict[str, Any] | None = None,
            config: Config | None = None,
        ) -> HooklessRunner:
            del backend_options, config
            self.runner = HooklessRunner(target)
            return self.runner

        def __getattr__(self, name: str) -> Any:
            return getattr(sqlite_backend_plugin, name)

    plugin = HooklessPlugin()

    def target_parts(
        db_path: str | BrokerTarget,
    ) -> tuple[str, str, dict[str, Any], Any]:
        target = db_path.target if isinstance(db_path, BrokerTarget) else str(db_path)
        return plugin.name, target, {}, plugin

    monkeypatch.setattr(broker_session_module, "_target_parts", target_parts)
    target = str(tmp_path / "hookless.db")
    first = Queue("first", db_path=target, persistent=True)
    sibling = Queue("sibling", db_path=target, persistent=True)
    first.write("one")
    assert plugin.runner is not None

    first.close()
    assert plugin.runner.close_calls == 0
    sibling.write("two")
    assert sibling.has_pending()
    sibling.close()
    assert plugin.runner.close_calls >= 1


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


def test_shared_db_connections_bind_relative_target_before_cwd_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    construction_dir = tmp_path / "construction"
    later_dir = tmp_path / "later"
    construction_dir.mkdir()
    later_dir.mkdir()
    absolute_target = construction_dir / "sqlite.db"

    monkeypatch.chdir(construction_dir)
    relative = DBConnection("sqlite.db", share_in_process=True)
    try:
        monkeypatch.chdir(later_dir)
        absolute = DBConnection(str(absolute_target), share_in_process=True)
        try:
            assert relative._shared_session is absolute._shared_session
        finally:
            absolute.close()
    finally:
        relative.close()


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
        start_barrier.wait(timeout=_LIVENESS)
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
    # Pre-warm before patching so first-use SQLite setup (phaselock, WAL,
    # schema) happens outside the observed windows.
    queue.write("warmup")
    operation_entered = threading.Event()
    release_operation = threading.Event()
    close_returned = threading.Event()
    operation_errors: list[BaseException] = []
    close_errors: list[BaseException] = []
    ordering: list[str] = []
    original_write = BrokerCore.write

    def delayed_write(
        self: BrokerCore,
        queue_name: str,
        message: str,
        *,
        keep_newest: int | None = None,
    ) -> None:
        operation_entered.set()
        assert release_operation.wait(timeout=_LIVENESS)
        original_write(self, queue_name, message, keep_newest=keep_newest)
        ordering.append("write-finished")

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
            ordering.append("close-returned")
            close_returned.set()

    monkeypatch.setattr(BrokerCore, "write", delayed_write)

    operation_thread = threading.Thread(target=write_message)
    close_thread = threading.Thread(target=close_queue)
    operation_thread.start()
    assert operation_entered.wait(timeout=_LIVENESS)

    # Deterministic close-is-waiting observation: close_all blocks on the
    # session's operation condition, whose only .wait caller is the close
    # path, so this fires exactly when close has entered its wait.
    assert queue.conn is not None
    session = queue.conn._shared_session
    assert session is not None
    close_waiting = threading.Event()
    original_condition_wait = session._operation_condition.wait

    def observe_close_wait(timeout: float | None = None) -> bool:
        close_waiting.set()
        return original_condition_wait(timeout)

    session._operation_condition.wait = observe_close_wait  # type: ignore[method-assign]

    close_thread.start()
    try:
        assert close_waiting.wait(timeout=_LIVENESS)
        assert not close_returned.is_set()
        # Positive happens-after proof: if close() failed to block on the
        # in-flight operation, "close-returned" would precede
        # "write-finished" regardless of scheduler timing.
        release_operation.set()
    finally:
        operation_thread.join(timeout=_LIVENESS)
        close_thread.join(timeout=_LIVENESS)

    assert not operation_thread.is_alive()
    assert not close_thread.is_alive()
    assert not operation_errors
    assert not close_errors
    assert ordering == ["write-finished", "close-returned"], (
        "Queue.close() returned while a persistent queue operation was still "
        f"using the shared broker session (observed order: {ordering})"
    )


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

    monkeypatch.setattr("simplebroker._broker_session._getpid", lambda: 1000)
    parent_key = _session_key(target, resolve_config(override={}))
    monkeypatch.setattr("simplebroker._broker_session._getpid", lambda: 1001)
    child_key = _session_key(target, resolve_config(override={}))

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


def test_process_session_close_attempts_every_safe_cleanup_after_exceptions() -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-030] exception
    class Core:
        def __init__(self, label: str) -> None:
            self.label = label

        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class FailingFactory:
        def __init__(self) -> None:
            self._create_lock = threading.Lock()
            self.created = 0
            self.core_close_calls: list[str] = []
            self.close_calls = 0

        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            with self._create_lock:
                label = f"core-{self.created}"
                self.created += 1
            return Core(label)

        def close_core(self, core: Core) -> None:
            self.core_close_calls.append(core.label)
            raise RuntimeError(f"{core.label} close failed")

        def close(self) -> None:
            self.close_calls += 1
            raise RuntimeError("factory close failed")

    factory = FailingFactory()
    session = _ProcessBrokerSession(cast(Any, factory))
    all_created = threading.Barrier(4)

    def create_thread_core() -> None:
        session.get_connection(None, lease_operation=False)
        all_created.wait(timeout=_LIVENESS)

    workers = [threading.Thread(target=create_thread_core) for _ in range(3)]
    for worker in workers:
        worker.start()
    all_created.wait(timeout=_LIVENESS)
    for worker in workers:
        worker.join(timeout=_LIVENESS)
    assert all(not worker.is_alive() for worker in workers)

    with pytest.raises(RuntimeError, match="close failed") as caught:
        session.close_all()

    diagnostics = "\n".join(
        [str(caught.value), *getattr(caught.value, "__notes__", ())]
    )
    assert set(factory.core_close_calls) == {"core-0", "core-1", "core-2"}
    assert factory.close_calls == 1
    for label in ("core-0", "core-1", "core-2", "factory"):
        assert f"{label} close failed" in diagnostics

    session.close_all()
    assert len(factory.core_close_calls) == 3
    assert factory.close_calls == 1


def test_process_session_cleanup_base_exception_keeps_priority() -> None:
    interruption = KeyboardInterrupt("cleanup interrupted")

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class InterruptingFactory:
        def __init__(self) -> None:
            self.core_close_calls = 0
            self.close_calls = 0

        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            return Core()

        def close_core(self, core: Core) -> None:
            del core
            self.core_close_calls += 1
            raise interruption

        def close(self) -> None:
            self.close_calls += 1

    factory = InterruptingFactory()
    session = _ProcessBrokerSession(cast(Any, factory))
    session.get_connection(None, lease_operation=False)

    with pytest.raises(KeyboardInterrupt, match="cleanup interrupted") as caught:
        session.close_all()

    assert caught.value is interruption
    assert factory.core_close_calls == 1
    assert factory.close_calls == 0

    session.close_all()
    assert factory.core_close_calls == 1
    assert factory.close_calls == 0


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
        config=resolve_config(override={}),
        factory_builder=_build_process_session_core_factory,
    )

    registry.close_all()
    registry.release(key)

    assert session._closed


def test_registry_shutdown_attempts_every_session_after_cleanup_exceptions(
    tmp_path: Path,
) -> None:
    close_calls: list[str] = []

    class FailingFactory:
        def __init__(self, label: str) -> None:
            self.label = label

        def create(self, stop_event: threading.Event | None) -> Any:
            raise AssertionError(f"unexpected create for {self.label}: {stop_event}")

        def close_core(self, core: Any) -> None:
            raise AssertionError(f"unexpected core for {self.label}: {core}")

        def close(self) -> None:
            close_calls.append(self.label)
            raise RuntimeError(f"{self.label} session close failed")

    registry = _ProcessBrokerSessionRegistry()
    config = resolve_config(override={})

    def build_factory(spec: Any) -> FailingFactory:
        return FailingFactory(Path(spec.target).name)

    registry.acquire(
        str(tmp_path / "first.db"),
        config=config,
        factory_builder=build_factory,
    )
    registry.acquire(
        str(tmp_path / "second.db"),
        config=config,
        factory_builder=build_factory,
    )

    with pytest.raises(RuntimeError, match="session close failed") as caught:
        registry.close_all()

    diagnostics = "\n".join(
        [str(caught.value), *getattr(caught.value, "__notes__", ())]
    )
    assert set(close_calls) == {"first.db", "second.db"}
    assert "first.db session close failed" in diagnostics
    assert "second.db session close failed" in diagnostics

    registry.close_all()
    assert len(close_calls) == 2


def test_registry_builds_factory_only_for_new_session_key(tmp_path: Path) -> None:
    registry = _ProcessBrokerSessionRegistry()
    build_calls = 0

    def build_factory(spec: Any) -> Any:
        nonlocal build_calls
        build_calls += 1
        return _build_process_session_core_factory(spec)

    key_a, session_a = registry.acquire(
        str(tmp_path / "registry.db"),
        config=resolve_config(override={}),
        factory_builder=build_factory,
    )
    key_b, session_b = registry.acquire(
        str(tmp_path / "registry.db"),
        config=resolve_config(override={}),
        factory_builder=build_factory,
    )

    assert key_a == key_b
    assert session_a is session_b
    assert build_calls == 1

    registry.release(key_a)
    assert not session_a._closed
    registry.release(key_b)
    assert session_a._closed


def test_session_close_timeout_defers_factory_close_until_core_creation_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="close-race")
    session = build_process_session(target)
    monkeypatch.setattr(
        "simplebroker._broker_session._CLOSE_ACTIVE_OPERATION_TIMEOUT",
        0.05,
    )
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
            assert allow_return.wait(timeout=_LIVENESS)
        original_setup(self, phase, stop_event)

    def get_connection() -> None:
        try:
            session.get_connection(None, lease_operation=False)
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
            errors.append(exc)

    monkeypatch.setattr(CountingSQLiteRunner, "setup_with_stop_event", delayed_setup)
    worker = threading.Thread(target=get_connection)
    worker.start()
    assert core_created.wait(timeout=_LIVENESS)

    # Deterministic close-is-waiting observation instead of a negative
    # timing window: wrap the condition close_all() blocks on.
    close_waiting = threading.Event()
    original_condition_wait = session._operation_condition.wait

    def observe_close_wait(timeout: float | None = None) -> bool:
        close_waiting.set()
        return original_condition_wait(timeout)

    session._operation_condition.wait = observe_close_wait  # type: ignore[method-assign]

    def close_session() -> None:
        session.close_all()
        close_returned.set()

    close_thread = threading.Thread(target=close_session)
    close_thread.start()
    try:
        assert close_waiting.wait(timeout=_LIVENESS)
        assert close_returned.wait(timeout=_LIVENESS)
        assert worker.is_alive()
        assert counting_backend.runner_close_calls == 0
    finally:
        allow_return.set()
        worker.join(timeout=_LIVENESS)
        close_thread.join(timeout=_LIVENESS)

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
            assert allow_creation.wait(timeout=_LIVENESS)
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
    assert creation_admitted.wait(timeout=_LIVENESS)
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
        worker.join(timeout=_LIVENESS)

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
    factory_close_error = RuntimeError("factory close failed")
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
            assert allow_release.wait(timeout=_LIVENESS)

        def close(self) -> None:
            self.close_calls += 1
            raise factory_close_error

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
    assert release_entered.wait(timeout=_LIVENESS)

    try:
        session.close_all()
        assert session._closed
        assert runner.close_calls == 0
        assert worker.is_alive()
    finally:
        allow_release.set()
        worker.join(timeout=_LIVENESS)

    assert not worker.is_alive()
    assert errors == [creation_error]
    assert getattr(creation_error, "__notes__", []) == [
        (
            "Additional process-session cleanup failure: "
            "RuntimeError: factory close failed"
        )
    ]
    assert runner.lease_calls == 1
    assert runner.release_calls == 1
    assert runner.close_calls == 1
    assert session._active_core_creations == 0
    assert not session._cores


def test_deferred_factory_close_failure_keeps_closed_session_error_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnformattableCleanupFailure(RuntimeError):
        def __str__(self) -> str:
            raise RuntimeError("cleanup failure was stringified")

    creation_entered = threading.Event()
    allow_creation = threading.Event()
    errors: list[BaseException] = []
    factory_close_error = UnformattableCleanupFailure("deferred factory close failed")
    monkeypatch.setattr(
        "simplebroker._broker_session._CLOSE_ACTIVE_OPERATION_TIMEOUT",
        0.05,
    )

    class Core:
        def set_stop_event(self, stop_event: threading.Event | None) -> None:
            del stop_event

    class Factory:
        def create(self, stop_event: threading.Event | None) -> Core:
            del stop_event
            creation_entered.set()
            assert allow_creation.wait(timeout=_LIVENESS)
            return Core()

        def close_core(self, core: Core) -> None:
            del core

        def close(self) -> None:
            raise factory_close_error

    session = _ProcessBrokerSession(cast(Any, Factory()))

    def get_connection() -> None:
        try:
            session.get_connection(None, lease_operation=False)
        except RuntimeError as exc:
            errors.append(exc)

    worker = threading.Thread(target=get_connection)
    worker.start()
    assert creation_entered.wait(timeout=_LIVENESS)
    try:
        session.close_all()
    finally:
        allow_creation.set()
        worker.join(timeout=_LIVENESS)

    assert not worker.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert str(errors[0]) == "Broker session is closed"
    assert getattr(errors[0], "__notes__", []) == [
        (
            "Additional process-session cleanup failure: "
            f"{type(factory_close_error).__qualname__}: "
            "deferred factory close failed"
        )
    ]


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
    factory = _build_process_session_core_factory(
        _session_spec("target", resolve_config(override={}))
    )
    factory.close()
    factory.close()

    with pytest.raises(RuntimeError, match="Broker session is closed"):
        factory.create(None)

    assert plugin.create_runner_calls == 0


@pytest.mark.parametrize("difference", ["prefix", "validator"])
def test_config_metadata_separates_sessions(
    tmp_path: Path, counting_backend: CountingBackendPlugin, difference: str
) -> None:
    fields = dict(DEFAULT_CONFIG)
    fields["CUSTOM"] = ConfigField(1, "custom", int)
    first = resolve_config("APP", defaults=fields)
    if difference == "prefix":
        second = resolve_config("OTHER", defaults=fields)
    else:
        changed_fields = dict(fields)
        # A behaviorally identical but distinct validator is still different
        # field-declaration metadata, so the sessions stay separate.
        changed_fields["CUSTOM"] = ConfigField(1, "custom", lambda value: int(value))
        second = resolve_config("APP", defaults=changed_fields)
    assert dict(first) == dict(second)
    target = counting_target(tmp_path)
    with (
        Queue("first", db_path=target, persistent=True, config=first) as left,
        Queue("second", db_path=target, persistent=True, config=second) as right,
    ):
        left.write("one")
        right.write("two")
        assert counting_backend.create_runner_calls == 2


def test_derived_config_with_same_metadata_shares_session(
    tmp_path: Path, counting_backend: CountingBackendPlugin
) -> None:
    first = resolve_config("APP")
    second = resolve_config(config=first, override={"APP_CACHE_MB": first["CACHE_MB"]})
    assert second is not first
    target = counting_target(tmp_path)
    with (
        Queue("first", db_path=target, persistent=True, config=first) as left,
        Queue("second", db_path=target, persistent=True, config=second) as right,
    ):
        left.write("one")
        right.write("two")
        assert counting_backend.create_runner_calls == 1
