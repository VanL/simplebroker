"""Internal built-in backend access for SimpleBroker."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol, cast

from . import sqlite

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
