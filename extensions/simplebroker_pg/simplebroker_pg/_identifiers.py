"""Stable identifiers for Postgres advisory locks and notification channels.

Notification channel names are a cross-process wire protocol: writers compute
them in SQL (``_sql.py``) and listeners compute them here, possibly in
different client versions. Change them only with a planned transition.
Plan: ``docs/plans/2026-11-08-pg-notify-fanout-plan.md``.
"""

from __future__ import annotations

import hashlib

QUEUE_ACTIVITY_CHANNEL_PREFIX = "simplebroker_q_"
"""Prefix of per-queue channels; ``_sql.QUEUE_ACTIVITY_CHANNEL_SQL`` mirrors it."""


def stable_lock_key(*parts: str) -> int:
    """Return a stable signed 64-bit advisory lock key."""
    payload = "\x1f".join(parts).encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


def activity_channel_name(schema: str) -> str:
    """Return the schema-wide LISTEN/NOTIFY channel name.

    Current writers no longer notify it. Listeners keep LISTENing it for the
    ``*`` wildcard and for writers from releases before per-queue channels.
    """
    digest = hashlib.md5(schema.encode("utf-8"), usedforsecurity=False).hexdigest()
    return f"simplebroker_{digest[:24]}"


def queue_activity_channel_name(schema: str, queue_name: str) -> str:
    """Return the per-queue LISTEN/NOTIFY channel name (39 bytes).

    Byte layout: md5 over ``schema``, unit separator, canonical queue name.
    Schema and queue names are ASCII by validation, so PostgreSQL ``md5(text)``
    hashes the same bytes in every server encoding.
    """
    payload = f"{schema}\x1f{queue_name}".encode()
    digest = hashlib.md5(payload, usedforsecurity=False).hexdigest()
    return f"{QUEUE_ACTIVITY_CHANNEL_PREFIX}{digest[:24]}"
