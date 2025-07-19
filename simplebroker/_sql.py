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
# MESSAGE OPERATIONS - SELECT (PEEK/READ)
# ============================================================================

# Select messages for peek operation with dynamic WHERE clause
# Usage: Build WHERE clause dynamically, then use f-string to insert it
SELECT_MESSAGES_PEEK = """
SELECT body, ts FROM messages
WHERE {where_clause}
ORDER BY id
LIMIT ? OFFSET ?
"""

# ============================================================================
# MESSAGE OPERATIONS - UPDATE (CLAIM/MOVE)
# ============================================================================

# Claim messages for reading (with RETURNING for atomicity)
# Usage: Build WHERE clause dynamically, then use f-string to insert it
UPDATE_MESSAGES_CLAIM_SINGLE = """
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

UPDATE_MESSAGES_CLAIM_BATCH = """
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

# Move message by ID with optional unclaimed check
# Usage: Build WHERE clause dynamically based on require_unclaimed
UPDATE_MESSAGE_MOVE_BY_ID = """
UPDATE messages
SET queue = ?, claimed = 0
WHERE {where_clause}
RETURNING id, body, ts
"""

# Move oldest unclaimed message
UPDATE_MESSAGE_MOVE_OLDEST = """
UPDATE messages
SET queue = ?, claimed = 0
WHERE id IN (
    SELECT id FROM messages
    WHERE queue = ? AND claimed = 0
    ORDER BY id
    LIMIT 1
)
RETURNING id, body, ts
"""

# Move specific message by timestamp
UPDATE_MESSAGE_MOVE_BY_TS = """
UPDATE messages
SET queue = ?, claimed = 0
WHERE ts = ? AND queue = ? AND claimed = 0
RETURNING body, ts
"""

# Move all messages with CTE for ordering
UPDATE_MESSAGES_MOVE_ALL = """
WITH to_move AS (
    SELECT id FROM messages
    WHERE queue = ? AND claimed = 0
    ORDER BY id
)
UPDATE messages
SET queue = ?, claimed = 0
WHERE id IN (SELECT id FROM to_move)
RETURNING body, ts, id
"""

# Move all messages newer than timestamp
UPDATE_MESSAGES_MOVE_SINCE = """
WITH to_move AS (
    SELECT id FROM messages
    WHERE queue = ? AND claimed = 0 AND ts > ?
    ORDER BY id
)
UPDATE messages
SET queue = ?, claimed = 0
WHERE id IN (SELECT id FROM to_move)
RETURNING body, ts, id
"""

# Move single message newer than timestamp
UPDATE_MESSAGE_MOVE_SINGLE_SINCE = """
UPDATE messages
SET queue = ?, claimed = 0
WHERE id IN (
    SELECT id FROM messages
    WHERE queue = ? AND claimed = 0 AND ts > ?
    ORDER BY id
    LIMIT 1
)
RETURNING body, ts
"""

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

# List queues with unclaimed message counts
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
        """
