"""Internal built-in backend access for SimpleBroker."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from . import sqlite

if TYPE_CHECKING:
    from .._runner import SQLRunner

DEFAULT_BACKEND = "sqlite"
_INTERNAL_BACKEND_KEY = "_BROKER_INTERNAL_BACKEND"


class BuiltinBackend(Protocol):
    """Typed internal interface for built-in backend modules."""

    def validate_database(self, file_path: Path, verify_magic: bool = True) -> None: ...

    def is_valid_database(self, file_path: Path, verify_magic: bool = True) -> bool: ...

    def check_version(self) -> None: ...

    def apply_connection_settings(
        self,
        conn: sqlite3.Connection,
        *,
        config: dict[str, Any],
        optimization_complete: bool = False,
    ) -> None: ...

    def apply_optimization_settings(
        self, conn: sqlite3.Connection, *, config: dict[str, Any]
    ) -> None: ...

    def setup_connection_phase(
        self, db_path: str, *, config: dict[str, Any]
    ) -> None: ...

    def initialize_database(
        self,
        runner: SQLRunner,
        *,
        run_with_retry: Callable[[Callable[[], Any]], Any],
    ) -> None: ...

    def meta_table_exists(self, runner: SQLRunner) -> bool: ...

    def ensure_schema_v2(
        self,
        runner: SQLRunner,
        *,
        current_version: int,
        write_schema_version: Callable[[int], None],
    ) -> None: ...

    def ensure_schema_v3(
        self,
        runner: SQLRunner,
        *,
        current_version: int,
        write_schema_version: Callable[[int], None],
    ) -> None: ...

    def ensure_schema_v4(
        self,
        runner: SQLRunner,
        *,
        current_version: int,
        write_schema_version: Callable[[int], None],
    ) -> None: ...

    def delete_and_count_changes(
        self, runner: SQLRunner, sql: str, params: tuple[Any, ...] = ()
    ) -> int: ...

    def database_size_bytes(self, db_path: str | Path | None) -> int: ...

    def get_data_version(self, runner: SQLRunner) -> int | None: ...

    def has_claimed_messages(self, runner: SQLRunner) -> bool: ...

    def delete_claimed_batch(self, runner: SQLRunner, *, batch_size: int) -> None: ...

    def compact_database(self, runner: SQLRunner) -> None: ...

    def maybe_run_incremental_vacuum(self, runner: SQLRunner) -> None: ...


_BUILTIN_BACKENDS: dict[str, BuiltinBackend] = {
    "sqlite": cast(BuiltinBackend, sqlite),
}


def get_backend(name: str = DEFAULT_BACKEND) -> BuiltinBackend:
    """Return the named built-in backend."""
    try:
        return _BUILTIN_BACKENDS[name]
    except KeyError as exc:
        raise RuntimeError(f"Unsupported built-in backend: {name}") from exc


def get_configured_backend(config: Mapping[str, object]) -> BuiltinBackend:
    """Return the configured built-in backend, defaulting to SQLite."""
    name = str(config.get(_INTERNAL_BACKEND_KEY, DEFAULT_BACKEND))
    return get_backend(name)


__all__ = [
    "BuiltinBackend",
    "DEFAULT_BACKEND",
    "get_backend",
    "get_configured_backend",
    "sqlite",
]
