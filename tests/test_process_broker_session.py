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
from simplebroker import Queue, open_broker, resolve_isolated_config
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
        config: dict[str, Any] | None = None,
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
    config: dict[str, Any] | None = None,
) -> _ProcessBrokerSession:
    spec = _session_spec(
        db_path,
        resolve_isolated_config({} if config is None else config),
    )
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
        resolve_isolated_config({}),
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
    config = resolve_isolated_config(
        {"BROKER_EMBEDDER_METADATA": metadata},
        preserve_unknown=True,
    )
    queue_a = Queue(
        "a",
        db_path=target,
        persistent=True,
        config=config,
    )

    target.backend_options["pool"]["hosts"].append("mutated")
    config["BROKER_EMBEDDER_METADATA"]["labels"].append("mutated")

    original_target = counting_target(
        tmp_path,
        pool={"hosts": ["primary"]},
        opaque=opaque,
    )
    original_config = resolve_isolated_config(
        {"BROKER_EMBEDDER_METADATA": {"labels": ["original"]}},
        preserve_unknown=True,
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
    assert counting_backend.runner_configs[0]["BROKER_EMBEDDER_METADATA"] == {
        "labels": ["original"]
    }


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


def test_type_distinct_config_extras_do_not_share_process_session(
    tmp_path: Path,
    counting_backend: CountingBackendPlugin,
) -> None:
    target = counting_target(tmp_path, schema="same")
    config_a = resolve_isolated_config(
        {"BROKER_EMBEDDER_METADATA": True},
        preserve_unknown=True,
    )
    config_b = resolve_isolated_config(
        {"BROKER_EMBEDDER_METADATA": 1},
        preserve_unknown=True,
    )

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
    config_a = resolve_isolated_config(
        {"BROKER_EMBEDDER_METADATA": "a"},
        preserve_unknown=True,
    )
    config_b = resolve_isolated_config(
        {"BROKER_EMBEDDER_METADATA": "b"},
        preserve_unknown=True,
    )

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
    parent_key = _session_key(target, resolve_isolated_config({}))
    monkeypatch.setattr("simplebroker._broker_session._getpid", lambda: 1001)
    child_key = _session_key(target, resolve_isolated_config({}))

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
        config=resolve_isolated_config({}),
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
    config = resolve_isolated_config({})

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
        config=resolve_isolated_config({}),
        factory_builder=build_factory,
    )
    key_b, session_b = registry.acquire(
        str(tmp_path / "registry.db"),
        config=resolve_isolated_config({}),
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
        assert not close_returned.is_set()
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

    session.close_all()
    assert session._closed
    assert runner.close_calls == 1
    assert worker.is_alive()

    allow_release.set()
    worker.join(timeout=_LIVENESS)

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
    factory = _build_process_session_core_factory(
        _session_spec("target", resolve_isolated_config({}))
    )
    factory.close()
    factory.close()

    with pytest.raises(RuntimeError, match="Broker session is closed"):
        factory.create(None)

    assert plugin.create_runner_calls == 0
