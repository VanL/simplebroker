"""Internal SQLite backend interface for built-in SimpleBroker code."""

from .maintenance import (
    compact_database,
    database_size_bytes,
    delete_and_count_changes,
    delete_claimed_batch,
    get_data_version,
    has_claimed_messages,
    maybe_run_incremental_vacuum,
)
from .runtime import (
    apply_connection_settings,
    apply_optimization_settings,
    check_version,
    setup_connection_phase,
)
from .schema import (
    ensure_schema_v2,
    ensure_schema_v3,
    ensure_schema_v4,
    initialize_database,
    meta_table_exists,
)
from .validation import is_valid_database, validate_database

__all__ = [
    "apply_connection_settings",
    "apply_optimization_settings",
    "check_version",
    "compact_database",
    "database_size_bytes",
    "delete_and_count_changes",
    "delete_claimed_batch",
    "ensure_schema_v2",
    "ensure_schema_v3",
    "ensure_schema_v4",
    "get_data_version",
    "has_claimed_messages",
    "initialize_database",
    "is_valid_database",
    "maybe_run_incremental_vacuum",
    "meta_table_exists",
    "setup_connection_phase",
    "validate_database",
]
