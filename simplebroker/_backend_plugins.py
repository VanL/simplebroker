"""Public backend plugin contracts and resolver support."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from ._exceptions import DatabaseError
from ._sql import BackendSQLNamespace, ensure_backend_sql_namespace

if TYPE_CHECKING:
    from ._runner import SQLRunner

BACKEND_ENTRY_POINT_GROUP = "simplebroker.backends"
DEFAULT_BACKEND_NAME = "sqlite"


class BackendPlugin(Protocol):
    """Public contract for backend plugins."""

    name: str
    sql: BackendSQLNamespace
    schema_version: int

    def init_backend(
        self,
        config: Mapping[str, Any],
        *,
        toml_target: str = "",
        toml_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    def create_runner(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> SQLRunner: ...

    def initialize_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> None: ...

    def validate_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        verify_initialized: bool = True,
        config: Mapping[str, Any] | None = None,
    ) -> None: ...

    def cleanup_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> bool: ...

    def check_version(self) -> None: ...

    def apply_connection_settings(
        self,
        conn: Any,
        *,
        config: Mapping[str, Any],
        optimization_complete: bool = False,
    ) -> None: ...

    def apply_optimization_settings(
        self, conn: Any, *, config: Mapping[str, Any]
    ) -> None: ...

    def setup_connection_phase(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any],
    ) -> None: ...

    def initialize_database(
        self,
        runner: SQLRunner,
        *,
        run_with_retry: Callable[[Callable[[], Any]], Any],
    ) -> None: ...

    def meta_table_exists(self, runner: SQLRunner) -> bool: ...

    def migrate_schema(
        self,
        runner: SQLRunner,
        *,
        current_version: int,
        write_schema_version: Callable[[int], None],
    ) -> None: ...

    def delete_messages(
        self,
        runner: SQLRunner,
        *,
        queue: str | None,
    ) -> int: ...

    def read_magic(self, runner: SQLRunner) -> str | None: ...

    def read_schema_version(self, runner: SQLRunner) -> int: ...

    def write_schema_version(self, runner: SQLRunner, version: int) -> None: ...

    def read_last_ts(self, runner: SQLRunner) -> int: ...

    def advance_last_ts(self, runner: SQLRunner, *, new_ts: int) -> bool: ...

    def write_last_ts(self, runner: SQLRunner, ts: int) -> None: ...

    def read_alias_version(self, runner: SQLRunner) -> int: ...

    def write_alias_version(self, runner: SQLRunner, version: int) -> None: ...

    def select_meta_items(self, runner: SQLRunner) -> list[tuple[str, int | str]]: ...

    def database_size_bytes(self, runner: SQLRunner) -> int: ...

    def get_data_version(self, runner: SQLRunner) -> int | None: ...

    def prepare_queue_operation(
        self,
        runner: SQLRunner,
        *,
        operation: str,
        queue: str,
    ) -> None: ...

    def prepare_broadcast(self, runner: SQLRunner) -> None: ...

    def vacuum(
        self,
        runner: SQLRunner,
        *,
        compact: bool,
        config: Mapping[str, Any],
    ) -> None: ...

    def create_activity_waiter(
        self,
        *,
        target: str | None,
        backend_options: Mapping[str, Any] | None = None,
        runner: SQLRunner | None = None,
        queue_name: str,
        stop_event: Any,
    ) -> ActivityWaiter | None: ...


class ActivityWaiter(Protocol):
    """Optional backend-native waiter used to wake idle watchers."""

    def wait(self, timeout: float) -> bool: ...

    def close(self) -> None: ...


@runtime_checkable
class BackendAwareRunner(Protocol):
    """Additive runner protocol for backend-aware custom runners."""

    @property
    def backend_plugin(self) -> BackendPlugin: ...


def resolve_runner_backend_plugin(
    runner: SQLRunner,
    explicit_plugin: BackendPlugin | None = None,
) -> BackendPlugin:
    """Resolve the backend plugin for a runner instance."""

    plugin = explicit_plugin
    if plugin is None:
        if isinstance(runner, BackendAwareRunner):
            plugin = runner.backend_plugin
        else:
            plugin = get_backend_plugin(DEFAULT_BACKEND_NAME)
    ensure_backend_sql_namespace(plugin.sql)
    return plugin


def _load_entry_point_plugin(name: str) -> BackendPlugin:
    """Load an external backend plugin by entry point name."""
    entry_points_obj: Any = metadata.entry_points()
    if hasattr(entry_points_obj, "select"):
        matches = entry_points_obj.select(group=BACKEND_ENTRY_POINT_GROUP, name=name)
    else:  # pragma: no cover - Python <3.10 compatibility fallback
        matches = [
            entry_point
            for entry_point in entry_points_obj.get(BACKEND_ENTRY_POINT_GROUP, [])
            if entry_point.name == name
        ]

    for entry_point in matches:
        loaded = entry_point.load()
        plugin = loaded() if callable(loaded) else loaded
        if getattr(plugin, "name", None) != name:
            raise RuntimeError(
                f"Backend plugin '{name}' resolved to object with mismatched name "
                f"'{getattr(plugin, 'name', None)}'"
            )
        ensure_backend_sql_namespace(plugin.sql)
        return cast(BackendPlugin, plugin)

    raise RuntimeError(f"Unknown backend plugin: {name}")


def get_backend_plugin(name: str = DEFAULT_BACKEND_NAME) -> BackendPlugin:
    """Resolve a built-in or entry-point backend plugin."""
    if name == DEFAULT_BACKEND_NAME:
        from ._backends.sqlite.plugin import sqlite_backend_plugin

        ensure_backend_sql_namespace(sqlite_backend_plugin.sql)
        return cast(BackendPlugin, sqlite_backend_plugin)
    return _load_entry_point_plugin(name)


def validate_backend_target(
    plugin: BackendPlugin,
    target: str,
    *,
    backend_options: Mapping[str, Any] | None = None,
    verify_initialized: bool = True,
    config: Mapping[str, Any] | None = None,
) -> None:
    """Small wrapper to standardize backend validation exceptions."""
    try:
        plugin.validate_target(
            target,
            backend_options=backend_options,
            verify_initialized=verify_initialized,
            config=config,
        )
    except DatabaseError:
        raise
    except FileNotFoundError as exc:
        raise DatabaseError(str(exc)) from exc


def target_parent_directory(target: str) -> Path:
    """Return the parent directory for filesystem-backed targets."""
    return Path(target).expanduser().resolve().parent


__all__ = [
    "ActivityWaiter",
    "BACKEND_ENTRY_POINT_GROUP",
    "BackendAwareRunner",
    "BackendPlugin",
    "DEFAULT_BACKEND_NAME",
    "get_backend_plugin",
    "resolve_runner_backend_plugin",
    "target_parent_directory",
    "validate_backend_target",
]
