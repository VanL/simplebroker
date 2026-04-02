"""Internal SQLite backend interface for built-in SimpleBroker code."""

from .maintenance import (
    database_size_bytes,
    delete_messages,
    get_data_version,
    vacuum,
)
from .plugin import SQLiteBackendPlugin, sqlite_backend_plugin
from .runtime import (
    apply_connection_settings,
    apply_optimization_settings,
    check_version,
    setup_connection_phase,
)
from .schema import (
    initialize_database,
    meta_table_exists,
    migrate_schema,
)
from .validation import is_valid_database, validate_database

__all__ = [
    "apply_connection_settings",
    "apply_optimization_settings",
    "check_version",
    "database_size_bytes",
    "delete_messages",
    "get_data_version",
    "initialize_database",
    "is_valid_database",
    "migrate_schema",
    "meta_table_exists",
    "SQLiteBackendPlugin",
    "setup_connection_phase",
    "sqlite_backend_plugin",
    "validate_database",
    "vacuum",
]
