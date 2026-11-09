"""Public backend plugin contracts and resolver support."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from importlib import metadata
from typing import TYPE_CHECKING, Any, Final, Protocol, cast, runtime_checkable

from ._constants import Config
from ._constants import __version__ as SIMPLEBROKER_VERSION
from ._delivery import DeliveryGuarantee
from ._exceptions import UnknownBackendPluginError
from ._sql import BackendSQLNamespace, ensure_backend_sql_namespace

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from ._runner import SQLRunner
    from ._sidecar import SidecarSession
    from .metadata import QueueRenameResult, QueueStats

BACKEND_ENTRY_POINT_GROUP = "simplebroker.backends"
BACKEND_API_VERSION: Final[int] = 9
DEFAULT_BACKEND_NAME = "sqlite"
FIRST_PARTY_BACKEND_PACKAGES: Final[dict[str, str]] = {
    "postgres": "simplebroker-pg",
    "redis": "simplebroker-redis",
}
MessageIdInput = int | str


class BackendPlugin(Protocol):
    """Public contract for backend plugins.

    ``schema_version`` versions stored data. ``backend_api_version`` versions the
    Python seam between core and backend plugin packages; it is not persisted in
    backend metadata.

    Optional hooks (probed via getattr, no-op if absent):
    - prepare_alias_mutation(runner) -- called inside the alias-mutation
      transaction before validation (probed in db.py BrokerCore.add_alias
      and the rename path); Postgres uses it to take a stable advisory
      lock ordering.  Note: prepare_broadcast is NOT optional -- it is a
      required protocol member, called directly by BrokerCore.broadcast.
    - write_generated_message(runner, *, queue, message, candidate) -> int | None
      -- probed by BrokerCore for an ordinary write (no keep_newest). In one
      atomic backend operation it advances durable last_ts to
      max(candidate, last_ts + 1), inserts the pending row with that ID,
      publishes the activity notification, and commits, returning the
      committed ID. When candidate <= last_ts and last_ts's logical counter is
      already at its final value it must change nothing and return None. The
      high-water serialization must be held from allocation through commit
      (generated rows commit in ID order), and it must be acquired before any
      lock on the messages storage. Raises IntegrityError, with nothing
      committed, when the chosen ID is already stored. An OperationalError
      raised while acquiring or advancing the high-water serialization must
      set ``_high_water_phase = True``: core then reports it exactly like a
      failed compare-and-advance (contention retried for the timestamp
      budget, then TimestampError; otherwise TimestampError with
      outcome_ambiguous=True). Insert, parameter, and commit failures stay
      unmarked and propagate unchanged. Postgres implements it as one
      pipelined transaction; absent, the shared SQL transaction runs.
    """

    name: str
    sql: BackendSQLNamespace | None
    backend_api_version: int
    schema_version: int
    is_direct_backend: bool

    def init_backend(
        self,
        config: Config,
        *,
        toml_target: str = "",
        toml_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    def create_runner(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Config | None = None,
    ) -> SQLRunner: ...

    def create_core(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Config | None = None,
        stop_event: Any = None,
    ) -> BrokerConnection: ...

    def create_core_from_runner(
        self,
        runner: Any,
        *,
        config: Config | None = None,
        stop_event: Any = None,
    ) -> BrokerConnection: ...

    def initialize_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Config | None = None,
    ) -> None: ...

    def validate_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        verify_initialized: bool = True,
        config: Config | None = None,
    ) -> None: ...

    def cleanup_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Config | None = None,
    ) -> bool: ...

    def check_version(self) -> None: ...

    def apply_connection_settings(
        self,
        conn: Any,
        *,
        config: Config,
        optimization_complete: bool = False,
    ) -> None: ...

    def apply_optimization_settings(self, conn: Any, *, config: Config) -> None: ...

    def setup_connection_phase(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Config,
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

    def rename_queue_messages(
        self,
        runner: SQLRunner,
        *,
        old_queue: str,
        new_queue: str,
    ) -> int: ...

    def retarget_aliases(
        self,
        runner: SQLRunner,
        *,
        old_target: str,
        new_target: str,
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
        config: Config,
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

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any: ...  # noqa: PYI036 approved [DOM-10.1.1] [RUFF-SUP-001] exception

    def set_stop_event(self, stop_event: Any) -> None: ...

    def generate_timestamp(self) -> int: ...

    def get_cached_last_timestamp(self) -> int: ...

    def refresh_last_timestamp(self) -> int: ...

    def advance_last_timestamp(self, timestamp: int) -> int: ...

    def write(
        self,
        queue: str,
        message: str,
        *,
        keep_newest: int | None = None,
    ) -> int: ...

    def insert_messages(
        self,
        records: Iterable[tuple[str, str, MessageIdInput]],
    ) -> None: ...

    def claim_one(
        self,
        queue: str,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool = True,
        order: str = "oldest",
    ) -> tuple[str, int] | str | None: ...

    def claim_many(
        self,
        queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        order: str = "oldest",
    ) -> list[tuple[str, int]] | list[str]: ...

    def claim_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        config: Config | None = None,
    ) -> Iterator[tuple[str, int] | str]: ...

    def peek_one(
        self,
        queue: str,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool = True,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> tuple[str, int] | str | None: ...

    def peek_many(
        self,
        queue: str,
        limit: int = ...,
        *,
        with_timestamps: bool = True,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> list[tuple[str, int]] | list[str]: ...

    def peek_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        include_claimed: bool = False,
    ) -> Iterator[tuple[str, int] | str]: ...

    def move_one(
        self,
        source_queue: str,
        target_queue: str,
        *,
        exact_timestamp: MessageIdInput | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool = True,
        order: str = "oldest",
    ) -> tuple[str, int] | str | None: ...

    def move_many(
        self,
        source_queue: str,
        target_queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
        order: str = "oldest",
    ) -> list[tuple[str, int]] | list[str]: ...

    def move_generator(
        self,
        source_queue: str,
        target_queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        config: Config | None = None,
    ) -> Iterator[tuple[str, int] | str]: ...

    def delete(self, queue: str | None = None) -> int: ...

    def delete_message_ids(
        self, queue: str, message_ids: Sequence[MessageIdInput]
    ) -> int: ...

    def delete_from_queues(
        self,
        queue_names: Sequence[str],
        *,
        before_timestamp: int | None = None,
    ) -> int: ...

    def rename_queue(
        self,
        old_queue: str,
        new_queue: str,
        *,
        retarget_aliases: bool = True,
    ) -> QueueRenameResult: ...

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

    def broadcast(
        self,
        message: str,
        *,
        pattern: str | None = None,
        queue_names: Sequence[str] | None = None,
        create_missing: bool = False,
    ) -> int:
        """Implement ``[SB-BCAST-1]`` through ``[SB-BCAST-6]``.

        See ``docs/specs/12-broadcast.md``.
        """
        ...

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

    def latest_pending_timestamp(self, queue: str) -> int | None: ...

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
    """Optional backend-native waiter used to wake idle watchers.

    ``close()`` is terminal and idempotent. Its first invocation marks the
    waiter closed before backend cleanup and attempts each independently safe
    cleanup action, preserving the first ordinary error and noting later ones.
    Every later invocation is a no-op, even when the first raised; it does not
    retry partial cleanup. The owner serializes ``wait()`` and ``close()``.
    Waiters own registrations, not the runner substrate, and do not expose
    ``shutdown()``. See ``[SB-API-6]`` for the normative contract.
    """

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

    _ensure_backend_api_version(plugin)

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
    if callable(create_core):
        return
    raise RuntimeError(
        f"Backend plugin '{getattr(plugin, 'name', '<unknown>')}' must expose "
        "create_core() when sql is None"
    )


def _backend_package_context(name: str) -> str:
    package_name = FIRST_PARTY_BACKEND_PACKAGES.get(name)
    return f" ({package_name})" if package_name is not None else ""


def _ensure_backend_api_version(plugin: object) -> None:
    plugin_name = str(getattr(plugin, "name", "<unknown>"))
    plugin_version = getattr(plugin, "backend_api_version", None)
    if type(plugin_version) is not int:
        raise RuntimeError(
            f"Backend plugin '{plugin_name}' must declare integer backend_api_version"
        )
    if plugin_version != BACKEND_API_VERSION:
        raise RuntimeError(
            f"Backend plugin '{plugin_name}'{_backend_package_context(plugin_name)} "
            f"was built against backend API v{plugin_version} but simplebroker "
            f"{SIMPLEBROKER_VERSION} provides backend API "
            f"v{BACKEND_API_VERSION}; upgrade the backend extension, or pin "
            "simplebroker to a compatible release"
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


# Successful entry-point resolutions for the life of this process, by backend
# name [SB-API-11]. Only validated plugins are stored: an unknown, ambiguous,
# unloadable or invalid registration is rescanned on the next lookup, so a
# later install or repair is seen. get/setdefault on a plain dict take no lock,
# so a forked child never waits on a resolution in flight in its parent, and
# the fork hook below gives the child an empty memo. Coupling: tests that
# substitute ``metadata.entry_points`` must substitute this mapping too
# (monkeypatch), or their stub plugins see and leak memoized resolutions.
_RESOLVED_ENTRY_POINT_PLUGINS: dict[str, BackendPlugin] = {}

# Memos inherited across fork, kept alive and never read or released. One
# mapping per fork generation, each holding at most the few backend names its
# parent resolved, so the retention is bounded by fork depth.
_INHERITED_RESOLUTIONS: list[dict[str, BackendPlugin]] = []


def _forget_resolutions_after_fork() -> None:
    """Give a fork child no retained resolutions [SB-API-11].

    A plugin object created by the parent's lookup may own locks or resources
    that parent threads hold at fork time; the child resolves afresh instead,
    as it did before resolutions were retained. The inherited mapping is kept
    alive, never released: dropping its last reference here would run a
    plugin finalizer inside the fork hook, where a parent-held lock blocks the
    child before ``os.fork()`` returns.
    """
    global _RESOLVED_ENTRY_POINT_PLUGINS

    _INHERITED_RESOLUTIONS.append(_RESOLVED_ENTRY_POINT_PLUGINS)
    _RESOLVED_ENTRY_POINT_PLUGINS = {}


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_forget_resolutions_after_fork)


def _load_entry_point_plugin(name: str) -> BackendPlugin:
    """Resolve an external backend plugin by entry point name [SB-API-11]."""
    resolved = _RESOLVED_ENTRY_POINT_PLUGINS.get(name)
    if resolved is not None:
        return resolved

    matches = list(
        metadata.entry_points().select(
            group=BACKEND_ENTRY_POINT_GROUP,
            name=name,
        )
    )
    if len(matches) > 1:
        raise RuntimeError(
            f"Ambiguous backend plugin '{name}': {len(matches)} matching entry points"
        )

    for entry_point in matches:
        try:
            loaded = entry_point.load()
            plugin = loaded() if callable(loaded) else loaded
        except Exception as exc:
            raise RuntimeError(
                f"Backend plugin '{name}'{_backend_package_context(name)} could not "
                f"be loaded under simplebroker {SIMPLEBROKER_VERSION} backend API "
                f"v{BACKEND_API_VERSION}: {exc}. Upgrade the backend extension, or "
                "pin simplebroker to a compatible release."
            ) from exc
        if getattr(plugin, "name", None) != name:
            raise RuntimeError(
                f"Backend plugin '{name}' resolved to object with mismatched name "
                f"'{getattr(plugin, 'name', None)}'"
            )
        _ensure_backend_plugin_capabilities(cast(BackendPlugin, plugin))
        # Racing first lookups may each load; the first stored plugin wins, so
        # every caller in the process shares one object.
        return _RESOLVED_ENTRY_POINT_PLUGINS.setdefault(
            name, cast(BackendPlugin, plugin)
        )

    raise UnknownBackendPluginError(f"Unknown backend plugin: {name}")


def get_backend_plugin(name: str = DEFAULT_BACKEND_NAME) -> BackendPlugin:
    """Resolve a built-in or entry-point backend plugin."""
    if name == DEFAULT_BACKEND_NAME:
        from ._backends.sqlite.plugin import sqlite_backend_plugin

        _ensure_backend_plugin_capabilities(cast(BackendPlugin, sqlite_backend_plugin))
        return cast(BackendPlugin, sqlite_backend_plugin)
    return _load_entry_point_plugin(name)


__all__ = [
    "BACKEND_API_VERSION",
    "BACKEND_ENTRY_POINT_GROUP",
    "DEFAULT_BACKEND_NAME",
    "ActivityWaiter",
    "BackendAwareRunner",
    "BackendPlugin",
    "BrokerConnection",
    "MultiQueueActivityWaiterHook",
    "get_backend_plugin",
    "resolve_runner_backend_plugin",
]
