"""Stable identifiers for Postgres advisory locks and notification channels."""

from __future__ import annotations

import hashlib


def stable_lock_key(*parts: str) -> int:
    """Return a stable signed 64-bit advisory lock key."""
    payload = "\x1f".join(parts).encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


def activity_channel_name(schema: str) -> str:
    """Return a stable, identifier-safe LISTEN/NOTIFY channel name."""
    digest = hashlib.md5(schema.encode("utf-8"), usedforsecurity=False).hexdigest()
    return f"simplebroker_{digest[:24]}"
