"""SQL statement templates for SimpleBroker.

This module contains all SQL statements used by SimpleBroker's database operations.
These templates can be imported by both sync and async implementations.
"""

# ============================================================================
# TABLE CREATION
# ============================================================================

# Messages table - main table for storing messages
CREATE_MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL UNIQUE,
    claimed INTEGER DEFAULT 0
)
"""

# Meta table - stores internal state like last timestamp
CREATE_META_TABLE = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
)
"""

# ============================================================================
# ALIAS TABLE OPERATIONS
# ============================================================================

# IMPORTANT NOTE: alias metadata (alias_version) lives in the meta table; see the meta-table
# section for GET/UPDATE helpers that keep the alias cache in sync.

CREATE_ALIASES_TABLE = """
CREATE TABLE IF NOT EXISTS queue_aliases (
    alias TEXT PRIMARY KEY,
    target TEXT NOT NULL
)
"""

CREATE_ALIAS_TARGET_INDEX = """
CREATE INDEX IF NOT EXISTS idx_queue_aliases_target ON queue_aliases(target)
"""

SELECT_ALIASES = """
SELECT alias, target FROM queue_aliases
"""

SELECT_ALIASES_FOR_TARGET = """
SELECT alias FROM queue_aliases WHERE target = ?
"""

INSERT_ALIAS = """
INSERT INTO queue_aliases (alias, target) VALUES (?, ?)
"""

DELETE_ALIAS = """
DELETE FROM queue_aliases WHERE alias = ?
"""

# ============================================================================
# INDEX CREATION
# ============================================================================

# Composite covering index for efficient queue operations
# This single index serves all our query patterns efficiently:
# - WHERE queue = ? (uses first column)
# - WHERE queue = ? AND ts > ? (uses first two columns)
# - WHERE queue = ? ORDER BY id (uses first column + sorts by id)
# - WHERE queue = ? AND ts > ? ORDER BY id LIMIT ? (uses all three)
CREATE_QUEUE_TS_ID_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_queue_ts_id
ON messages(queue, ts, id)
"""

# Partial index for unclaimed messages - speeds up read operations
CREATE_UNCLAIMED_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_unclaimed
ON messages(queue, claimed, id)
WHERE claimed = 0
"""

# Unique index on timestamp column (for schema v3)
CREATE_TS_UNIQUE_INDEX = """
CREATE UNIQUE INDEX idx_messages_ts_unique
ON messages(ts)
"""

# ============================================================================
# SCHEMA MIGRATION
# ============================================================================

# Add claimed column (schema v2)
ALTER_MESSAGES_ADD_CLAIMED = """
ALTER TABLE messages ADD COLUMN claimed INTEGER DEFAULT 0
"""

# Check for claimed column existence
CHECK_CLAIMED_COLUMN = """
SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name='claimed'
"""

# Check for unique constraint on ts column
CHECK_TS_UNIQUE_CONSTRAINT = """
SELECT sql FROM sqlite_master
WHERE type='table' AND name='messages'
"""

# Check for unique index on ts column
CHECK_TS_UNIQUE_INDEX = """
SELECT COUNT(*) FROM sqlite_master
WHERE type='index' AND name='idx_messages_ts_unique'
"""

# ============================================================================
# MESSAGE OPERATIONS - INSERT
# ============================================================================

# Insert a new message
INSERT_MESSAGE = """
INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)
"""

# ============================================================================
# UNIFIED RETRIEVE OPERATIONS
# These queries support both single and batch operations via the LIMIT parameter
# ============================================================================

# Peek operation - non-destructive read
RETRIEVE_PEEK = """
SELECT body, ts FROM messages
WHERE {where_clause}
ORDER BY id
LIMIT ? OFFSET ?
"""

# Claim operation - mark as claimed and return
RETRIEVE_CLAIM = """
UPDATE messages
SET claimed = 1
WHERE id IN (
    SELECT id FROM messages
    WHERE {where_clause}
    ORDER BY id
    LIMIT ?
)
RETURNING body, ts
"""

# Move operation - change queue
RETRIEVE_MOVE = """
UPDATE messages
SET queue = ?, claimed = 0
WHERE id IN (
    SELECT id FROM messages
    WHERE {where_clause}
    ORDER BY id
    LIMIT ?
)
RETURNING body, ts
"""

# Check for pending messages
CHECK_PENDING_MESSAGES = (
    "SELECT EXISTS(SELECT 1 FROM messages WHERE queue = ? AND claimed = 0 LIMIT 1)"
)

# Check for pending messages since a timestamp
CHECK_PENDING_MESSAGES_SINCE = "SELECT EXISTS(SELECT 1 FROM messages WHERE queue = ? AND claimed = 0 AND ts > ? LIMIT 1)"

# Get data version (SQLite only)
GET_DATA_VERSION = "PRAGMA data_version"


# ============================================================================
# MESSAGE OPERATIONS - DELETE
# ============================================================================

# Delete all messages
DELETE_ALL_MESSAGES = """
DELETE FROM messages
"""

# Delete messages from specific queue
DELETE_QUEUE_MESSAGES = """
DELETE FROM messages WHERE queue = ?
"""

# Delete claimed messages in batches (for vacuum)
DELETE_CLAIMED_BATCH = """
DELETE FROM messages
WHERE id IN (
    SELECT id FROM messages
    WHERE claimed = 1
    LIMIT ?
)
"""

# ============================================================================
# QUEUE OPERATIONS
# ============================================================================

# list queues with unclaimed message counts
LIST_QUEUES_UNCLAIMED = """
SELECT queue, COUNT(*) as count
FROM messages
WHERE claimed = 0
GROUP BY queue
ORDER BY queue
"""

# Get queue statistics (unclaimed and total counts)
GET_QUEUE_STATS = """
SELECT
    queue,
    SUM(CASE WHEN claimed = 0 THEN 1 ELSE 0 END) as unclaimed,
    COUNT(*) as total
FROM messages
GROUP BY queue
ORDER BY queue
"""

# Get distinct queues for broadcast
GET_DISTINCT_QUEUES = """
SELECT DISTINCT queue FROM messages ORDER BY queue
"""

# Check if queue exists and has messages
CHECK_QUEUE_EXISTS = """
SELECT EXISTS(
    SELECT 1 FROM messages
    WHERE queue = ?
    LIMIT 1
)
"""

# ============================================================================
# META TABLE OPERATIONS
# ============================================================================

# Initialize last_ts in meta table
INIT_LAST_TS = """
INSERT OR IGNORE INTO meta (key, value) VALUES ('last_ts', 0)
"""

# Get last timestamp
GET_LAST_TS = """
SELECT value FROM meta WHERE key = 'last_ts'
"""

# Update last timestamp atomically
UPDATE_LAST_TS_ATOMIC = """
UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ?
"""

# Update last timestamp (for resync)
UPDATE_LAST_TS = """
UPDATE meta SET value = ? WHERE key = 'last_ts'
"""

# Get max timestamp from messages (for resync)
GET_MAX_MESSAGE_TS = """
SELECT MAX(ts) FROM messages
"""

# Get all meta key-value pairs for dump
SELECT_META_ALL = """
SELECT key, value FROM meta
"""

# IMPORTANT NOTE: alias information is stored in the alias table; these meta operations

# Alias metadata (alias_version)
INSERT_ALIAS_VERSION_META = """
INSERT OR IGNORE INTO meta (key, value) VALUES ('alias_version', 0)
"""

UPDATE_ALIAS_VERSION = """
UPDATE meta SET value = ? WHERE key = 'alias_version'
"""

GET_ALIAS_VERSION = """
SELECT value FROM meta WHERE key = 'alias_version'
"""


# ============================================================================
# STATUS OPERATIONS
# ============================================================================

# Get total message count for status
GET_TOTAL_MESSAGE_COUNT = """
SELECT COUNT(*) FROM messages
"""

# ============================================================================
# VACUUM OPERATIONS
# ============================================================================

# Get claimed and total message counts for vacuum decision
GET_VACUUM_STATS = """
SELECT
    SUM(CASE WHEN claimed = 1 THEN 1 ELSE 0 END) as claimed,
    COUNT(*) as total
FROM messages
"""

# Count only claimed messages (for pre-vacuum check)
COUNT_CLAIMED_MESSAGES = """
SELECT COUNT(*) FROM messages WHERE claimed = 1
"""

# Get overall stats for list command with --stats
GET_OVERALL_STATS = """
SELECT
    SUM(CASE WHEN claimed = 1 THEN 1 ELSE 0 END),
    COUNT(*)
FROM messages
"""

# SQLite VACUUM command to reclaim disk space
VACUUM = """
VACUUM
"""

# Set auto_vacuum mode to INCREMENTAL (2)
# Mode 0 = NONE, 1 = FULL, 2 = INCREMENTAL
SET_AUTO_VACUUM_INCREMENTAL = """
PRAGMA auto_vacuum=2
"""

# Get current auto_vacuum mode
GET_AUTO_VACUUM = """
PRAGMA auto_vacuum
"""

# Run incremental vacuum to reclaim N pages
# This only works when auto_vacuum mode is set to INCREMENTAL (2)
INCREMENTAL_VACUUM = """
PRAGMA incremental_vacuum(100)
"""

# ============================================================================
# PRAGMA STATEMENTS
# ============================================================================

# SQLite version check
SELECT_SQLITE_VERSION = """
SELECT sqlite_version()
"""

# Transaction control
BEGIN_IMMEDIATE = """
BEGIN IMMEDIATE
"""

# Legacy index cleanup (from older versions)
DROP_OLD_INDEXES = [
    "DROP INDEX IF EXISTS idx_messages_queue_ts",
    "DROP INDEX IF EXISTS idx_queue_id",
    "DROP INDEX IF EXISTS idx_queue_ts",
]

# ============================================================================
# DYNAMIC SQL BUILDERS
# ============================================================================


def build_peek_query(where_conditions: list[str]) -> str:
    """Build SELECT query for peek operations with dynamic WHERE clause."""
    where_clause = " AND ".join(where_conditions)
    return f"""
        SELECT body, ts FROM messages
        WHERE {where_clause}
        ORDER BY id
        LIMIT ? OFFSET ?
        """


def build_claim_single_query(where_conditions: list[str]) -> str:
    """Build UPDATE query for claiming single message."""
    where_clause = " AND ".join(where_conditions)
    return f"""
        UPDATE messages
        SET claimed = 1
        WHERE id IN (
            SELECT id FROM messages
            WHERE {where_clause}
            ORDER BY id
            LIMIT 1
        )
        RETURNING body, ts
        """


def build_claim_batch_query(where_conditions: list[str]) -> str:
    """Build UPDATE query for claiming batch of messages."""
    where_clause = " AND ".join(where_conditions)
    return f"""
        UPDATE messages
        SET claimed = 1
        WHERE id IN (
            SELECT id FROM messages
            WHERE {where_clause}
            ORDER BY id
            LIMIT ?
        )
        RETURNING body, ts
        """


def build_move_by_id_query(where_conditions: list[str]) -> str:
    """Build UPDATE query for moving message by ID."""
    where_clause = " AND ".join(where_conditions)
    return f"""
        UPDATE messages
        SET queue = ?, claimed = 0
        WHERE {where_clause}
        RETURNING id, body, ts
        ORDER BY id
        """


def build_retrieve_query(
    operation: str,
    where_conditions: list[str],
) -> str:
    """Build safe retrieve query for peek, claim, or move operations.

    Args:
        operation: One of "peek", "claim", or "move"
        where_conditions: list of SQL WHERE conditions (pre-validated)

    Returns:
        SQL query string with placeholders
    """
    where_clause = " AND ".join(where_conditions)

    if operation == "peek":
        return RETRIEVE_PEEK.format(where_clause=where_clause)
    elif operation == "claim":
        return RETRIEVE_CLAIM.format(where_clause=where_clause)
    elif operation == "move":
        return RETRIEVE_MOVE.format(where_clause=where_clause)
    else:
        raise ValueError(f"Invalid operation: {operation}")


# ~
