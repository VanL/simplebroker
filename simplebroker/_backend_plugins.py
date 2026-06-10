"""Public backend plugin contracts and resolver support."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, runtime_checkable

from ._exceptions import DatabaseError
from ._sql import BackendSQLNamespace, ensure_backend_sql_namespace

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from ._runner import SQLRunner
    from ._sidecar import SidecarSession
    from .metadata import QueueStats

BACKEND_ENTRY_POINT_GROUP = "simplebroker.backends"
DEFAULT_BACKEND_NAME = "sqlite"


class BackendPlugin(Protocol):
    """Public contract for backend plugins."""

    name: str
    sql: BackendSQLNamespace | None
    schema_version: int
    is_direct_backend: bool

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

    def create_core(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
        stop_event: Any = None,
    ) -> BrokerConnection: ...

    def create_core_from_runner(
        self,
        runner: Any,
        *,
        config: Mapping[str, Any] | None = None,
        stop_event: Any = None,
    ) -> BrokerConnection: ...

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

    def delete_message_ids(
        self,
        runner: SQLRunner,
        *,
        queue: str,
        message_ids: Sequence[int],
    ) -> int: ...

    def delete_from_queues(
        self,
        runner: SQLRunner,
        *,
        queue_names: Sequence[str],
        before_timestamp: int | None = None,
    ) -> int: ...

    def find_message_ids(
        self,
        runner: SQLRunner,
        *,
        queue: str,
        body_contains: str,
        limit: int,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
    ) -> list[int]: ...

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


class BrokerConnection(Protocol):
    """Internal broker core protocol used by Queue, CLI, and watchers."""

    def __enter__(self) -> Any: ...

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any: ...

    def set_stop_event(self, stop_event: Any) -> None: ...

    def generate_timestamp(self) -> int: ...

    def get_cached_last_timestamp(self) -> int: ...

    def refresh_last_timestamp(self) -> int: ...

    def write(self, queue: str, message: str) -> Any: ...

    def insert_messages(
        self,
        records: Iterable[tuple[str, str, int]],
    ) -> None: ...

    def claim_one(
        self,
        queue: str,
        *,
        exact_timestamp: int | None = None,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None: ...

    def claim_many(
        self,
        queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]: ...

    def claim_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: int | None = None,
        config: dict[str, Any] = ...,
    ) -> Iterator[tuple[str, int] | str]: ...

    def peek_one(
        self,
        queue: str,
        *,
        exact_timestamp: int | None = None,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None: ...

    def peek_many(
        self,
        queue: str,
        limit: int = ...,
        *,
        with_timestamps: bool = True,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]: ...

    def peek_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: int | None = None,
    ) -> Iterator[tuple[str, int] | str]: ...

    def move_one(
        self,
        source_queue: str,
        target_queue: str,
        *,
        exact_timestamp: int | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None: ...

    def move_many(
        self,
        source_queue: str,
        target_queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> list[tuple[str, int]] | list[str]: ...

    def move_generator(
        self,
        source_queue: str,
        target_queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: int | None = None,
        config: dict[str, Any] = ...,
    ) -> Iterator[tuple[str, int] | str]: ...

    def delete(self, queue: str | None = None) -> int: ...

    def delete_message_ids(self, queue: str, message_ids: Sequence[int]) -> int: ...

    def delete_from_queues(
        self,
        queue_names: Sequence[str],
        *,
        before_timestamp: int | None = None,
    ) -> int: ...

    def find_message_ids(
        self,
        queue: str,
        *,
        body_contains: str,
        limit: int = ...,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
    ) -> list[int]: ...

    def broadcast(self, message: str, *, pattern: str | None = None) -> int: ...

    def list_queues(
        self,
        *,
        prefix: str | None = None,
        pattern: str | None = None,
    ) -> list[str]: ...

    def get_queue_stats(self) -> list[tuple[str, int, int]]: ...

    def queue_exists(self, queue: str) -> bool: ...

    def queue_exists_and_has_messages(self, queue: str) -> bool: ...

    def get_queue_stat(self, queue: str) -> QueueStats: ...

    def list_queue_stats(
        self,
        *,
        prefix: str | None = None,
        pattern: str | None = None,
    ) -> list[QueueStats]: ...

    def get_overall_stats(self) -> tuple[int, int]: ...

    def count_claimed_messages(self) -> int: ...

    def status(self) -> dict[str, int]: ...

    def has_pending_messages(
        self, queue: str, after_timestamp: int | None = None
    ) -> bool: ...

    def get_data_version(self) -> int | None: ...

    def vacuum(self, *, compact: bool = False) -> None: ...

    def sidecar(
        self, *, transaction: bool = False
    ) -> AbstractContextManager[SidecarSession]: ...

    def get_alias_version(self) -> int: ...

    def resolve_alias(self, name: str) -> str | None: ...

    def canonicalize_queue(self, queue: str) -> str: ...

    def has_alias(self, name: str) -> bool: ...

    def list_aliases(self) -> list[tuple[str, str]]: ...

    def aliases_for_target(self, target: str) -> list[str]: ...

    def add_alias(self, name: str, target: str) -> None: ...

    def remove_alias(self, name: str) -> None: ...

    def get_meta(self) -> dict[str, int | str]: ...

    def close(self) -> None: ...

    def shutdown(self) -> None: ...


class ActivityWaiter(Protocol):
    """Optional backend-native waiter used to wake idle watchers."""

    def wait(self, timeout: float) -> bool: ...

    def close(self) -> None: ...


class MultiQueueActivityWaiterHook(Protocol):
    """Optional backend hook for creating one waiter across many queues."""

    def __call__(
        self,
        *,
        target: str | None,
        backend_options: Mapping[str, Any] | None = None,
        runner: SQLRunner | None = None,
        queue_names: Sequence[str],
        stop_event: Any,
    ) -> ActivityWaiter | None: ...


@runtime_checkable
class BackendAwareRunner(Protocol):
    """Additive runner protocol for backend-aware custom runners."""

    @property
    def backend_plugin(self) -> BackendPlugin: ...


def _ensure_backend_plugin_capabilities(plugin: BackendPlugin) -> None:
    """Validate that a plugin exposes either SQL hooks or a direct core hook."""

    sql_namespace = getattr(plugin, "sql", None)
    if sql_namespace is not None:
        ensure_backend_sql_namespace(sql_namespace)
        return

    if not bool(getattr(plugin, "is_direct_backend", False)):
        raise RuntimeError(
            f"Backend plugin '{getattr(plugin, 'name', '<unknown>')}' has no SQL "
            "namespace and is not marked as a direct backend"
        )

    create_core = getattr(plugin, "create_core", None)
    if not callable(create_core):
        raise RuntimeError(
            f"Backend plugin '{getattr(plugin, 'name', '<unknown>')}' must expose "
            "create_core() when sql is None"
        )


def resolve_runner_backend_plugin(
    runner: SQLRunner,
    explicit_plugin: BackendPlugin | None = None,
    *,
    fallback_plugin: BackendPlugin | None = None,
) -> BackendPlugin:
    """Resolve the backend plugin for a runner instance."""

    plugin = explicit_plugin
    if plugin is None:
        if isinstance(runner, BackendAwareRunner):
            plugin = runner.backend_plugin
        else:
            plugin = fallback_plugin
    if plugin is None:
        plugin = get_backend_plugin(DEFAULT_BACKEND_NAME)
    _ensure_backend_plugin_capabilities(plugin)
    return plugin


def _load_entry_point_plugin(name: str) -> BackendPlugin:
    """Load an external backend plugin by entry point name."""
    matches = metadata.entry_points().select(
        group=BACKEND_ENTRY_POINT_GROUP,
        name=name,
    )

    for entry_point in matches:
        loaded = entry_point.load()
        plugin = loaded() if callable(loaded) else loaded
        if getattr(plugin, "name", None) != name:
            raise RuntimeError(
                f"Backend plugin '{name}' resolved to object with mismatched name "
                f"'{getattr(plugin, 'name', None)}'"
            )
        _ensure_backend_plugin_capabilities(cast(BackendPlugin, plugin))
        return cast(BackendPlugin, plugin)

    raise RuntimeError(f"Unknown backend plugin: {name}")


def get_backend_plugin(name: str = DEFAULT_BACKEND_NAME) -> BackendPlugin:
    """Resolve a built-in or entry-point backend plugin."""
    if name == DEFAULT_BACKEND_NAME:
        from ._backends.sqlite.plugin import sqlite_backend_plugin

        _ensure_backend_plugin_capabilities(cast(BackendPlugin, sqlite_backend_plugin))
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
    "BrokerConnection",
    "BackendPlugin",
    "DEFAULT_BACKEND_NAME",
    "MultiQueueActivityWaiterHook",
    "get_backend_plugin",
    "resolve_runner_backend_plugin",
    "target_parent_directory",
    "validate_backend_target",
]
