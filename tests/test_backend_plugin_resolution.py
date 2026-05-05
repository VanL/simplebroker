"""Tests for public backend plugin resolution."""

from __future__ import annotations

import threading
from importlib.metadata import EntryPoint
from typing import Any

import pytest

import simplebroker._sql as sqlite_sql
from simplebroker import Queue
from simplebroker._runner import SetupPhase
from simplebroker._targets import ResolvedTarget
from simplebroker.db import DBConnection
from simplebroker.ext import BackendAwareRunner, get_backend_plugin

pytestmark = [pytest.mark.shared]


@pytest.mark.sqlite_only
def test_builtin_sqlite_backend_plugin_resolves() -> None:
    """The built-in sqlite backend should resolve without entry points."""
    from simplebroker._runner import SQLiteRunner

    plugin = get_backend_plugin("sqlite")

    assert plugin.name == "sqlite"
    assert plugin.sql is not None
    assert isinstance(plugin.create_runner(":memory:"), SQLiteRunner)


def test_unknown_backend_plugin_raises_clear_error() -> None:
    """Unknown backends should fail with a readable exception."""
    with pytest.raises(RuntimeError, match="Unknown backend plugin: missing"):
        get_backend_plugin("missing")


def test_external_backend_plugin_resolves_via_entry_point(monkeypatch) -> None:
    """Entry-point plugins should be loaded by name."""

    class DummyPlugin:
        name = "dummy"
        sql = sqlite_sql
        schema_version = 1

        def init_backend(self, config, **kwargs):
            raise NotImplementedError

        def create_runner(self, target: str, **kwargs):  # pragma: no cover - unused
            raise NotImplementedError

        def initialize_target(self, target: str, **kwargs) -> None:
            raise NotImplementedError

        def validate_target(self, target: str, **kwargs) -> None:
            raise NotImplementedError

        def cleanup_target(self, target: str, **kwargs) -> None:
            raise NotImplementedError

        def check_version(self) -> None:
            raise NotImplementedError

        def apply_connection_settings(self, conn, **kwargs) -> None:
            raise NotImplementedError

        def apply_optimization_settings(self, conn, **kwargs) -> None:
            raise NotImplementedError

        def setup_connection_phase(self, target: str, **kwargs) -> None:
            raise NotImplementedError

        def initialize_database(self, runner, **kwargs) -> None:
            raise NotImplementedError

        def meta_table_exists(self, runner) -> bool:
            raise NotImplementedError

        def migrate_schema(self, runner, **kwargs) -> None:
            raise NotImplementedError

        def delete_messages(self, runner, *, queue: str | None) -> int:
            raise NotImplementedError

        def database_size_bytes(self, runner) -> int:
            raise NotImplementedError

        def get_data_version(self, runner) -> int | None:
            raise NotImplementedError

        def prepare_queue_operation(
            self, runner, *, operation: str, queue: str
        ) -> None:
            raise NotImplementedError

        def prepare_broadcast(self, runner) -> None:
            raise NotImplementedError

        def vacuum(self, runner, *, compact: bool, config) -> None:
            raise NotImplementedError

        def create_activity_waiter(
            self,
            *,
            target: str | None,
            backend_options=None,
            runner=None,
            queue_name: str,
            stop_event,
        ):
            raise NotImplementedError

    class EntryPointsMock(list[EntryPoint]):
        def select(self, *, group: str, name: str):
            if group == "simplebroker.backends" and name == "dummy":
                return self
            return EntryPointsMock()

    def build_plugin() -> DummyPlugin:
        return DummyPlugin()

    entry_point = EntryPoint(
        name="dummy",
        value="tests.test_backend_plugin_resolution:build_plugin",
        group="simplebroker.backends",
    )

    monkeypatch.setattr(
        "simplebroker._backend_plugins.metadata.entry_points",
        lambda: EntryPointsMock([entry_point]),
    )
    monkeypatch.setitem(globals(), "build_plugin", build_plugin)

    plugin = get_backend_plugin("dummy")
    assert plugin.name == "dummy"


def test_external_backend_plugin_with_invalid_sql_namespace_is_rejected(
    monkeypatch,
) -> None:
    """Entry-point plugins should fail fast when their SQL namespace is incomplete."""

    class DummyPlugin:
        name = "dummy"
        sql = object()
        schema_version = 1

        def init_backend(self, config, **kwargs):
            raise NotImplementedError

    class EntryPointsMock(list[EntryPoint]):
        def select(self, *, group: str, name: str):
            if group == "simplebroker.backends" and name == "dummy":
                return self
            return EntryPointsMock()

    def build_plugin() -> DummyPlugin:
        return DummyPlugin()

    entry_point = EntryPoint(
        name="dummy",
        value="tests.test_backend_plugin_resolution:build_plugin",
        group="simplebroker.backends",
    )

    monkeypatch.setattr(
        "simplebroker._backend_plugins.metadata.entry_points",
        lambda: EntryPointsMock([entry_point]),
    )
    monkeypatch.setitem(globals(), "build_plugin", build_plugin)

    with pytest.raises(RuntimeError, match="Backend SQL namespace is missing"):
        get_backend_plugin("dummy")


@pytest.mark.sqlite_only
def test_legacy_runner_without_backend_plugin_still_looks_like_sqlite() -> None:
    """Legacy injected runners should remain backend-compatible without metadata."""
    from simplebroker._runner import SQLiteRunner

    class LegacyRunner(SQLiteRunner):
        pass

    runner = LegacyRunner(":memory:")
    assert not isinstance(runner, BackendAwareRunner)


def test_non_aware_runner_with_resolved_target_uses_target_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ResolvedTarget context should identify wrapped runners that are not backend-aware."""

    class LegacyRunner:
        def __init__(self) -> None:
            self.completed_phases: set[SetupPhase] = set()

        def run(
            self,
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> list[tuple[Any, ...]]:
            del sql, params, fetch
            return []

        def begin_immediate(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def close(self) -> None:
            pass

        def setup(self, phase: SetupPhase) -> None:
            self.completed_phases.add(phase)

        def is_setup_complete(self, phase: SetupPhase) -> bool:
            return phase in self.completed_phases

    class DummyWaiter:
        def wait(self, timeout: float) -> bool:
            del timeout
            return False

        def close(self) -> None:
            pass

    class RecordingPlugin:
        sql = sqlite_sql
        schema_version = 1

        def __init__(self, name: str) -> None:
            self.name = name
            self.waiter = DummyWaiter()

        def initialize_database(self, runner: Any, **kwargs: Any) -> None:
            del runner, kwargs

        def meta_table_exists(self, runner: Any) -> bool:
            del runner
            return False

        def read_schema_version(self, runner: Any) -> int:
            del runner
            return 1

        def write_schema_version(self, runner: Any, version: int) -> None:
            del runner, version

        def migrate_schema(self, runner: Any, **kwargs: Any) -> None:
            del runner, kwargs

        def read_last_ts(self, runner: Any) -> int:
            del runner
            return 0

        def create_activity_waiter(
            self,
            *,
            target: str | None,
            backend_options: dict[str, Any] | None = None,
            runner: Any = None,
            queue_name: str,
            stop_event: threading.Event,
        ) -> DummyWaiter:
            del target, backend_options, runner, queue_name, stop_event
            return self.waiter

    default_plugin = RecordingPlugin("sqlite")
    target_plugin = RecordingPlugin("dummy")

    def get_plugin(name: str = "sqlite") -> RecordingPlugin:
        if name == "dummy":
            return target_plugin
        if name == "sqlite":
            return default_plugin
        raise RuntimeError(f"unexpected plugin {name}")

    monkeypatch.setattr("simplebroker._backend_plugins.get_backend_plugin", get_plugin)

    runner = LegacyRunner()
    target = ResolvedTarget("dummy", "dummy-target", {"schema": "test_schema"})

    connection = DBConnection(target, runner)
    try:
        assert connection._backend_plugin is target_plugin
    finally:
        connection.close()

    queue = Queue("jobs", db_path=target, runner=runner, persistent=True)
    try:
        assert queue.create_activity_waiter(stop_event=threading.Event()) is (
            target_plugin.waiter
        )
    finally:
        queue.close()


def test_sqlite_plugin_has_init_backend() -> None:
    """The built-in sqlite plugin should expose init_backend()."""
    from simplebroker._constants import load_config

    plugin = get_backend_plugin("sqlite")
    result = plugin.init_backend(load_config())

    assert "target" in result
    assert "backend_options" in result
