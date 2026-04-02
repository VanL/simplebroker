"""Postgres SQL namespace for SimpleBroker."""

from __future__ import annotations

from simplebroker._sql import RetrieveOperation, RetrieveQuerySpec

CHECK_PENDING_MESSAGES = """
SELECT EXISTS(
    SELECT 1
    FROM messages
    WHERE queue = ? AND claimed = FALSE
    LIMIT 1
)
"""

CHECK_PENDING_MESSAGES_SINCE = """
SELECT EXISTS(
    SELECT 1
    FROM messages
    WHERE queue = ? AND claimed = FALSE AND ts > ?
    LIMIT 1
)
"""

CHECK_QUEUE_EXISTS = """
SELECT EXISTS(
    SELECT 1
    FROM messages
    WHERE queue = ?
    LIMIT 1
)
"""

COUNT_CLAIMED_MESSAGES = "SELECT COUNT(*) FROM messages WHERE claimed = TRUE"
DELETE_ALIAS = "DELETE FROM aliases WHERE alias = ?"
DELETE_ALL_MESSAGES_COUNT = """
WITH deleted AS (
    DELETE FROM messages
    RETURNING 1
)
SELECT COUNT(*) FROM deleted
"""
DELETE_QUEUE_MESSAGES_COUNT = """
WITH deleted AS (
    DELETE FROM messages
    WHERE queue = ?
    RETURNING 1
)
SELECT COUNT(*) FROM deleted
"""
GET_ALIAS_VERSION = "SELECT alias_version FROM meta WHERE singleton = TRUE"
GET_DISTINCT_QUEUES = "SELECT DISTINCT queue FROM messages ORDER BY queue"
GET_LAST_TS = "SELECT last_ts FROM meta WHERE singleton = TRUE"
GET_MAX_MESSAGE_TS = "SELECT COALESCE(MAX(ts), 0) FROM messages"
GET_OVERALL_STATS = """
SELECT
    COALESCE(SUM(CASE WHEN claimed THEN 1 ELSE 0 END), 0),
    COUNT(*)
FROM messages
"""
GET_QUEUE_STATS = """
SELECT
    queue,
    SUM(CASE WHEN NOT claimed THEN 1 ELSE 0 END) AS unclaimed_count,
    COUNT(*) AS total_count
FROM messages
GROUP BY queue
ORDER BY queue
"""
GET_TOTAL_MESSAGE_COUNT = "SELECT COUNT(*) FROM messages"
GET_VACUUM_STATS = """
SELECT
    COALESCE(SUM(CASE WHEN claimed THEN 1 ELSE 0 END), 0),
    COUNT(*)
FROM messages
"""
INSERT_ALIAS = "INSERT INTO aliases (alias, target) VALUES (?, ?)"
INSERT_MESSAGE = """
WITH inserted AS (
    INSERT INTO messages (queue, body, ts)
    VALUES (?, ?, ?)
    RETURNING queue
)
SELECT pg_notify(
    'simplebroker_' || substr(md5(current_schema()), 1, 24),
    queue
)
FROM inserted
"""
LIST_QUEUES_UNCLAIMED = """
SELECT queue, COUNT(*)
FROM messages
WHERE claimed = FALSE
GROUP BY queue
ORDER BY queue
"""
SELECT_ALIASES = "SELECT alias, target FROM aliases ORDER BY alias"
SELECT_ALIASES_FOR_TARGET = """
SELECT alias
FROM aliases
WHERE target = ?
ORDER BY alias
"""
SELECT_META_ALL = """
SELECT key, value
FROM (
    SELECT 'alias_version'::TEXT AS key, alias_version::TEXT AS value
    FROM meta
    WHERE singleton = TRUE
    UNION ALL
    SELECT 'last_ts'::TEXT AS key, last_ts::TEXT AS value
    FROM meta
    WHERE singleton = TRUE
    UNION ALL
    SELECT 'magic'::TEXT AS key, magic AS value
    FROM meta
    WHERE singleton = TRUE
    UNION ALL
    SELECT 'schema_version'::TEXT AS key, schema_version::TEXT AS value
    FROM meta
    WHERE singleton = TRUE
) AS meta_items
ORDER BY key
"""
UPDATE_ALIAS_VERSION = """
UPDATE meta
SET alias_version = ?
WHERE singleton = TRUE
"""
UPDATE_LAST_TS = "UPDATE meta SET last_ts = ? WHERE singleton = TRUE"

DELETE_CLAIMED_BATCH_COUNT = """
WITH deleted AS (
    DELETE FROM messages
    WHERE order_id IN (
        SELECT order_id
        FROM messages
        WHERE claimed = TRUE
        ORDER BY order_id
        LIMIT ?
    )
    RETURNING 1
)
SELECT COUNT(*) FROM deleted
"""

DATABASE_SIZE_BYTES = """
SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
FROM pg_class AS c
JOIN pg_namespace AS n
  ON n.oid = c.relnamespace
WHERE n.nspname = ?
  AND c.relname IN ('messages', 'meta', 'aliases')
"""

LOCK_BROADCAST_SCOPE = "LOCK TABLE messages IN SHARE ROW EXCLUSIVE MODE"

COMPACT_TABLE_MESSAGES = "VACUUM (ANALYZE) messages"
COMPACT_TABLE_META = "VACUUM (ANALYZE) meta"
COMPACT_TABLE_ALIASES = "VACUUM (ANALYZE) aliases"


def _build_where_clause(spec: RetrieveQuerySpec) -> tuple[list[str], list[object]]:
    """Build Postgres WHERE conditions and parameters for retrieve operations."""
    if spec.exact_timestamp is not None:
        where_conditions = ["ts = ?", "queue = ?"]
        params: list[object] = [spec.exact_timestamp, spec.queue]
        if spec.require_unclaimed:
            where_conditions.append("claimed = FALSE")
        return where_conditions, params

    where_conditions = ["queue = ?"]
    params = [spec.queue]
    if spec.require_unclaimed:
        where_conditions.append("claimed = FALSE")
    if spec.since_timestamp is not None:
        where_conditions.append("ts > ?")
        params.append(spec.since_timestamp)
    return where_conditions, params


def build_retrieve_query(
    operation: RetrieveOperation,
    spec: RetrieveQuerySpec,
) -> tuple[str, tuple[object, ...]]:
    """Build a Postgres retrieve query and its parameter tuple."""
    where_conditions, params = _build_where_clause(spec)
    where_clause = " AND ".join(where_conditions)

    if operation == "peek":
        return (
            f"""
            SELECT body, ts
            FROM messages
            WHERE {where_clause}
            ORDER BY order_id
            LIMIT ?
            OFFSET ?
            """,
            tuple(params + [spec.limit, spec.offset]),
        )

    if operation == "claim":
        return (
            f"""
            WITH selected AS (
                SELECT order_id
                FROM messages
                WHERE {where_clause}
                ORDER BY order_id
                LIMIT ?
            ),
            updated AS (
                UPDATE messages
                SET claimed = TRUE
                WHERE order_id IN (SELECT order_id FROM selected)
                RETURNING order_id, body, ts
            )
            SELECT updated.body, updated.ts
            FROM updated
            JOIN selected
              ON selected.order_id = updated.order_id
            ORDER BY selected.order_id
            """,
            tuple(params + [spec.limit]),
        )

    if operation == "move":
        if spec.target_queue is None:
            raise ValueError("Move retrieve query requires target_queue")
        return (
            f"""
            WITH target_queue AS (
                SELECT ? AS queue_name
            ),
            selected AS (
                SELECT order_id
                FROM messages
                WHERE {where_clause}
                ORDER BY order_id
                LIMIT ?
            ),
            updated AS (
                UPDATE messages
                SET queue = (SELECT queue_name FROM target_queue),
                    claimed = FALSE
                WHERE order_id IN (SELECT order_id FROM selected)
                RETURNING order_id, body, ts
            ),
            notified AS (
                SELECT pg_notify(
                    'simplebroker_' || substr(md5(current_schema()), 1, 24),
                    (SELECT queue_name FROM target_queue)
                )
                FROM updated
                LIMIT 1
            )
            SELECT updated.body, updated.ts
            FROM updated
            JOIN selected
              ON selected.order_id = updated.order_id
            LEFT JOIN notified
              ON TRUE
            ORDER BY selected.order_id
            """,
            tuple([spec.target_queue] + params + [spec.limit]),
        )

    raise ValueError(f"Unsupported retrieve operation: {operation}")
