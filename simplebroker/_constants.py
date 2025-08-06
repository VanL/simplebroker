"""Constants and configuration for SimpleBroker.

This module centralizes all constants and environment variable configuration
for SimpleBroker. Constants are immutable values that control various aspects
of the system's behavior, from message size limits to timing parameters.

Environment Variables:
    See the load_config() function for a complete list of supported environment
    variables and their default values.

Usage:
    from simplebroker._constants import MAX_MESSAGE_SIZE, load_config

    # Use constants directly
    if len(message) > MAX_MESSAGE_SIZE:
        raise ValueError("Message too large")

    # Load configuration once at module level
    config = load_config()
    timeout = config["BROKER_BUSY_TIMEOUT"]
"""

import os
from typing import Any, Dict, Final

# ==============================================================================
# VERSION INFORMATION
# ==============================================================================

__version__: Final[str] = "2.1.0"
"""Current version of SimpleBroker."""

# ==============================================================================
# PROGRAM IDENTIFICATION
# ==============================================================================

PROG_NAME: Final[str] = "simplebroker"
"""Program name used in CLI help and error messages."""

# ==============================================================================
# DATABASE CONFIGURATION
# ==============================================================================

DEFAULT_DB_NAME: Final[str] = ".broker.db"
"""Default database filename created in current directory if not specified."""

SIMPLEBROKER_MAGIC: Final[str] = "simplebroker-v1"
"""Magic string stored in database to verify it's a SimpleBroker database."""

SCHEMA_VERSION: Final[int] = 1
"""Current database schema version for migration compatibility."""

# ==============================================================================
# EXIT CODES
# ==============================================================================

EXIT_SUCCESS: Final[int] = 0
"""Exit code for successful operations."""

EXIT_QUEUE_EMPTY: Final[int] = 2
"""Exit code when queue is empty or no messages match criteria."""

# ==============================================================================
# MESSAGE AND QUEUE CONSTRAINTS
# ==============================================================================

MAX_MESSAGE_SIZE: Final[int] = 10 * 1024 * 1024  # 10MB limit
"""Maximum allowed message size in bytes (default: 10MB).

Can be overridden with BROKER_MAX_MESSAGE_SIZE environment variable.
Messages larger than this will be rejected with a ValueError.
"""

MAX_QUEUE_NAME_LENGTH: Final[int] = 512
"""Maximum allowed length for queue names in characters."""

# ==============================================================================
# TIMESTAMP AND ID GENERATION
# ==============================================================================
# SimpleBroker uses hybrid timestamps that combine physical time with a logical
# counter to ensure uniqueness even under extreme concurrency.

TIMESTAMP_EXACT_NUM_DIGITS: Final[int] = 19
"""Exact number of digits required for message ID timestamps in string form."""

PHYSICAL_TIME_BITS: Final[int] = 52
"""Number of bits used for microseconds since epoch (supports until ~2113)."""

LOGICAL_COUNTER_BITS: Final[int] = 12
"""Number of bits used for the monotonic counter to handle sub-microsecond events."""

LOGICAL_COUNTER_MASK: Final[int] = (1 << LOGICAL_COUNTER_BITS) - 1
"""Bitmask for extracting the logical counter from a hybrid timestamp."""

MAX_LOGICAL_COUNTER: Final[int] = 1 << LOGICAL_COUNTER_BITS
"""Maximum value for logical counter (4096) before time must advance."""

UNIX_NATIVE_BOUNDARY: Final[int] = 2**44
"""Boundary for distinguishing Unix timestamps from native format (~17.6 trillion, year 2527)."""

SQLITE_MAX_INT64: Final[int] = 2**63
"""Maximum value for SQLite's signed 64-bit integer - timestamps must be less than this."""

# ==============================================================================
# TIME UNIT CONVERSIONS
# ==============================================================================

MS_PER_SECOND: Final[int] = 1000
"""Milliseconds per second."""

US_PER_SECOND: Final[int] = 1_000_000
"""Microseconds per second."""

MS_PER_US: Final[int] = 1000
"""Microseconds per millisecond."""

NS_PER_US: Final[int] = 1000
"""Nanoseconds per microsecond."""

NS_PER_SECOND: Final[int] = 1_000_000_000
"""Nanoseconds per second."""

WAIT_FOR_NEXT_INCREMENT: Final[float] = 0.000_001
"""Sleep duration in seconds (1μs) when waiting for clock to advance during timestamp collision."""

MAX_ITERATIONS: Final[int] = 100_000
"""Maximum iterations waiting for time to advance before concluding clock is broken."""

# ==============================================================================
# WATCHER SETTINGS
# ==============================================================================

MAX_TOTAL_RETRY_TIME: Final[int] = 300  # 5 minutes max
"""Maximum time in seconds to retry watcher initialization before giving up."""

# ==============================================================================
# DATABASE RUNNER PHASES
# ==============================================================================


class ConnectionPhase:
    """Database setup phases for SQLRunner implementations."""

    CONNECTION = "connection"
    """Basic connectivity and critical settings (e.g., enabling WAL mode)."""

    OPTIMIZATION = "optimization"
    """Performance settings (cache size, synchronous mode, etc.)."""


def load_config() -> Dict[str, Any]:
    """Load configuration from environment variables.

    This function reads all SimpleBroker environment variables and returns
    a configuration dictionary with validated values. It's designed to be
    called once at module initialization to avoid repeated environment lookups.

    Returns:
        dict: Configuration dictionary with the following keys:

        SQLite Performance Settings:
            BROKER_BUSY_TIMEOUT (int): SQLite busy timeout in milliseconds.
                Default: 5000 (5 seconds)
                Controls how long SQLite waits when database is locked.

            BROKER_CACHE_MB (int): SQLite page cache size in megabytes.
                Default: 10
                Larger values improve performance for repeated queries.
                Recommended: 10-50 MB for typical use, 100+ MB for heavy use.

            BROKER_SYNC_MODE (str): SQLite synchronous mode.
                Default: "FULL"
                Options:
                - "FULL": Maximum durability, safe against power loss
                - "NORMAL": ~25% faster writes, small risk on power loss
                - "OFF": Fastest but unsafe - testing only

            BROKER_WAL_AUTOCHECKPOINT (int): WAL checkpoint threshold in pages.
                Default: 1000 (≈1MB with 1KB pages)
                Controls when WAL data is moved to main database.

        Message Processing:
            BROKER_MAX_MESSAGE_SIZE (int): Maximum message size in bytes.
                Default: 10485760 (10MB)
                Messages larger than this are rejected.

            BROKER_READ_COMMIT_INTERVAL (int): Messages per transaction in --all mode.
                Default: 1 (exactly-once delivery)
                Higher values improve performance but risk redelivery on failure.

        Vacuum Settings:
            BROKER_AUTO_VACUUM (int): Enable automatic vacuum of claimed messages.
                Default: 1 (enabled)
                Set to 0 to disable automatic cleanup.

            BROKER_AUTO_VACUUM_INTERVAL (int): Write operations between vacuum checks.
                Default: 100
                Lower values = more frequent cleanup, higher values = better performance.

            BROKER_VACUUM_THRESHOLD (float): Percentage of claimed messages to trigger vacuum.
                Default: 0.1 (10%)
                Vacuum runs when claimed messages exceed this percentage of total.

            BROKER_VACUUM_BATCH_SIZE (int): Messages to delete per vacuum batch.
                Default: 1000
                Larger batches are faster but hold locks longer.

            BROKER_VACUUM_LOCK_TIMEOUT (int): Seconds before vacuum lock is considered stale.
                Default: 300 (5 minutes)
                Prevents stuck vacuum operations from blocking others.

        Watcher Settings:
            BROKER_SKIP_IDLE_CHECK (bool): Skip idle queue optimization check.
                Default: False
                Set to "1" to disable two-phase detection.

            BROKER_JITTER_FACTOR (float): Jitter factor for polling intervals.
                Default: 0.15 (15%)
                Prevents synchronized polling across multiple watchers.

            SIMPLEBROKER_INITIAL_CHECKS (int): Burst mode checks with zero delay.
                Default: 100
                Higher values = faster response to new messages.

            SIMPLEBROKER_MAX_INTERVAL (float): Maximum polling interval in seconds.
                Default: 0.1 (100ms)
                Lower values = more responsive but higher CPU usage.

            SIMPLEBROKER_BURST_SLEEP (float): Sleep between burst mode checks.
                Default: 0.00001 (10μs)
                Tiny delay to prevent CPU spinning.

        Debug:
            BROKER_DEBUG (bool): Enable debug output.
                Default: False
                Shows additional diagnostic information.

        Logging:
            BROKER_LOGGING_ENABLED (bool): Enable logging output.
                Default: False (disabled)
                Set to "1" to enable logging throughout SimpleBroker.
                When enabled, logs will be written using Python's logging module.
                Configure logging levels and handlers in your application as needed.

    """
    config = {
        # SQLite performance settings
        "BROKER_BUSY_TIMEOUT": int(os.environ.get("BROKER_BUSY_TIMEOUT", "5000")),
        "BROKER_CACHE_MB": int(os.environ.get("BROKER_CACHE_MB", "10")),
        "BROKER_SYNC_MODE": os.environ.get("BROKER_SYNC_MODE", "FULL").upper(),
        "BROKER_WAL_AUTOCHECKPOINT": int(
            os.environ.get("BROKER_WAL_AUTOCHECKPOINT", "1000"),
        ),
        # Message processing
        "BROKER_MAX_MESSAGE_SIZE": int(
            os.environ.get("BROKER_MAX_MESSAGE_SIZE", str(MAX_MESSAGE_SIZE)),
        ),
        "BROKER_READ_COMMIT_INTERVAL": int(
            os.environ.get("BROKER_READ_COMMIT_INTERVAL", "1"),
        ),
        # Vacuum settings
        "BROKER_AUTO_VACUUM": int(os.environ.get("BROKER_AUTO_VACUUM", "1")),
        "BROKER_AUTO_VACUUM_INTERVAL": int(
            os.environ.get("BROKER_AUTO_VACUUM_INTERVAL", "100"),
        ),
        "BROKER_VACUUM_THRESHOLD": float(
            os.environ.get("BROKER_VACUUM_THRESHOLD", "10"),
        )
        / 100,
        "BROKER_VACUUM_BATCH_SIZE": int(
            os.environ.get("BROKER_VACUUM_BATCH_SIZE", "1000"),
        ),
        "BROKER_VACUUM_LOCK_TIMEOUT": int(
            os.environ.get("BROKER_VACUUM_LOCK_TIMEOUT", "300"),
        ),
        # Watcher settings
        "BROKER_SKIP_IDLE_CHECK": os.environ.get("BROKER_SKIP_IDLE_CHECK", "0") == "1",
        "BROKER_JITTER_FACTOR": float(os.environ.get("BROKER_JITTER_FACTOR", "0.15")),
        "SIMPLEBROKER_INITIAL_CHECKS": int(
            os.environ.get("SIMPLEBROKER_INITIAL_CHECKS", "100"),
        ),
        "SIMPLEBROKER_MAX_INTERVAL": float(
            os.environ.get("SIMPLEBROKER_MAX_INTERVAL", "0.1"),
        ),
        "SIMPLEBROKER_BURST_SLEEP": float(
            os.environ.get("SIMPLEBROKER_BURST_SLEEP", "0.00001"),
        ),
        # Debug
        "BROKER_DEBUG": bool(os.environ.get("BROKER_DEBUG")),
        # Logging
        "BROKER_LOGGING_ENABLED": os.environ.get("BROKER_LOGGING_ENABLED", "0") == "1",
    }

    # Validate SYNC_MODE
    if config["BROKER_SYNC_MODE"] not in ("FULL", "NORMAL", "OFF"):
        config["BROKER_SYNC_MODE"] = "FULL"

    return config
