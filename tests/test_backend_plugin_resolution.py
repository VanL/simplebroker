"""Tests for public backend plugin resolution."""

from __future__ import annotations

from importlib.metadata import EntryPoint

import pytest

import simplebroker._sql as sqlite_sql
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
