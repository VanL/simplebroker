"""Tests for public backend plugin resolution."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

import simplebroker._sql as sqlite_sql
from simplebroker import Queue
from simplebroker._backend_plugins import BACKEND_API_VERSION
from simplebroker._constants import __version__ as SIMPLEBROKER_VERSION
from simplebroker._runner import SetupPhase
from simplebroker._targets import ResolvedTarget
from simplebroker.db import DBConnection
from simplebroker.ext import BackendAwareRunner, get_backend_plugin

pytestmark = [pytest.mark.shared]

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class EntryPointStub:
    def __init__(
        self, name: str, loaded: Any = None, *, error: BaseException | None = None
    ):
        self.name = name
        self._loaded = loaded
        self._error = error

    def load(self) -> Any:
        if self._error is not None:
            raise self._error
        return self._loaded


class EntryPointsMock(list[EntryPointStub]):
    def select(self, *, group: str, name: str) -> EntryPointsMock:
        if group != "simplebroker.backends":
            return EntryPointsMock()
        return EntryPointsMock(
            entry_point for entry_point in self if entry_point.name == name
        )


def _install_entry_point(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    loaded: Any = None,
    *,
    error: BaseException | None = None,
) -> None:
    monkeypatch.setattr(
        "simplebroker._backend_plugins.metadata.entry_points",
        lambda: EntryPointsMock([EntryPointStub(name, loaded, error=error)]),
    )


class ValidDummyPlugin:
    name = "dummy"
    sql = sqlite_sql
    backend_api_version = BACKEND_API_VERSION
    schema_version = 1


@pytest.mark.sqlite_only
def test_builtin_sqlite_backend_plugin_resolves() -> None:
    """The built-in sqlite backend should resolve without entry points."""
    from simplebroker._runner import SQLiteRunner

    plugin = get_backend_plugin("sqlite")

    assert plugin.name == "sqlite"
    assert plugin.backend_api_version == BACKEND_API_VERSION
    assert plugin.sql is not None
    assert isinstance(plugin.create_runner(":memory:"), SQLiteRunner)


def test_unknown_backend_plugin_raises_clear_error() -> None:
    """Unknown backends should fail with a readable exception."""
    with pytest.raises(RuntimeError, match="Unknown backend plugin: missing"):
        get_backend_plugin("missing")


def test_external_backend_plugin_resolves_via_entry_point(monkeypatch) -> None:
    """Entry-point plugins should be loaded by name."""

    _install_entry_point(monkeypatch, "dummy", ValidDummyPlugin)

    plugin = get_backend_plugin("dummy")
    assert plugin.name == "dummy"


def test_external_backend_plugin_with_invalid_sql_namespace_is_rejected(
    monkeypatch,
) -> None:
    """Entry-point plugins should fail fast when their SQL namespace is incomplete."""

    class DummyPlugin:
        name = "dummy"
        sql = object()
        backend_api_version = BACKEND_API_VERSION
        schema_version = 1

        def init_backend(self, config, **kwargs):
            raise NotImplementedError

    _install_entry_point(monkeypatch, "dummy", DummyPlugin)

    with pytest.raises(RuntimeError, match="Backend SQL namespace is missing"):
        get_backend_plugin("dummy")


def test_external_backend_plugin_missing_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingVersionPlugin:
        name = "dummy"
        sql = sqlite_sql
        schema_version = 1

    _install_entry_point(monkeypatch, "dummy", MissingVersionPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert "backend_api_version" in message


def test_external_backend_plugin_with_stale_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StaleVersionPlugin(ValidDummyPlugin):
        backend_api_version = 0

    _install_entry_point(monkeypatch, "dummy", StaleVersionPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert "backend API v0" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_external_backend_plugin_with_future_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    future_version = BACKEND_API_VERSION + 1

    class FutureVersionPlugin(ValidDummyPlugin):
        backend_api_version = future_version

    _install_entry_point(monkeypatch, "dummy", FutureVersionPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert f"backend API v{future_version}" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_first_party_backend_api_mismatch_mentions_package_and_core_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StalePostgresPlugin(ValidDummyPlugin):
        name = "postgres"
        backend_api_version = 0

    _install_entry_point(monkeypatch, "postgres", StalePostgresPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("postgres")

    message = str(excinfo.value)
    assert "postgres" in message
    assert "simplebroker-pg" in message
    assert SIMPLEBROKER_VERSION in message
    assert "backend API v0" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_external_backend_plugin_non_integer_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StringVersionPlugin(ValidDummyPlugin):
        backend_api_version = "1"

    _install_entry_point(monkeypatch, "dummy", StringVersionPlugin)

    with pytest.raises(RuntimeError, match="integer backend_api_version"):
        get_backend_plugin("dummy")


def test_external_backend_plugin_bool_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BoolVersionPlugin(ValidDummyPlugin):
        backend_api_version = True

    _install_entry_point(monkeypatch, "dummy", BoolVersionPlugin)

    with pytest.raises(RuntimeError, match="integer backend_api_version"):
        get_backend_plugin("dummy")


def test_entry_point_load_failure_gets_actionable_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_entry_point(
        monkeypatch,
        "dummy",
        error=ImportError("cannot import name 'BackendPlugin'"),
    )

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert "simplebroker" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "cannot import name 'BackendPlugin'" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_first_party_entry_point_load_failure_mentions_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_entry_point(
        monkeypatch,
        "postgres",
        error=ImportError("cannot import name 'BackendPlugin'"),
    )

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("postgres")

    message = str(excinfo.value)
    assert "postgres" in message
    assert "simplebroker-pg" in message
    assert SIMPLEBROKER_VERSION in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "cannot import name 'BackendPlugin'" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


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
        backend_api_version = BACKEND_API_VERSION
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


@pytest.mark.parametrize(
    "relative_path",
    [
        "extensions/simplebroker_pg/simplebroker_pg/plugin.py",
        "extensions/simplebroker_redis/simplebroker_redis/plugin.py",
    ],
)
def test_first_party_extension_plugins_declare_literal_backend_api_version(
    relative_path: str,
) -> None:
    plugin_source = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")

    assert "backend_api_version = 1" in plugin_source
    assert "backend_api_version = BACKEND_API_VERSION" not in plugin_source
