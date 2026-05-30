"""Validation helpers for the Valkey/Redis backend."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import redis

from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError

from ._constants import DEFAULT_NAMESPACE, REDIS_SCHEMA_VERSION
from .responses import response_dict

NAMESPACE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
GLOB_CHARS = frozenset("*?[]{}")
QUEUE_KEY_PART_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")
BATCH_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")
TOP_LEVEL_KEYS = frozenset({"meta", "queues", "aliases", "bodies", "all_ids"})
QUEUE_KEY_STATES = frozenset({"pending", "claimed", "reserved"})
BATCH_KEY_KINDS = frozenset({"ids", "meta"})


class NamespaceState(StrEnum):
    """Ownership state for one Valkey/Redis namespace."""

    ABSENT = "ABSENT"
    OWNED = "OWNED"
    FOREIGN = "FOREIGN"
    PARTIAL_SIMPLEBROKER = "PARTIAL_SIMPLEBROKER"


@dataclass(frozen=True, slots=True)
class NamespaceInspection:
    namespace: str
    state: NamespaceState
    key_count: int
    schema_version: int | None = None


def require_namespace(backend_options: Mapping[str, Any] | None) -> str:
    """Extract and validate the configured namespace."""

    options = backend_options or {}
    raw = options.get("namespace", options.get("schema", DEFAULT_NAMESPACE))
    if not isinstance(raw, str):
        raise DatabaseError("Redis namespace must be a string")
    namespace = raw.strip()
    if not namespace:
        raise DatabaseError("Redis namespace must be non-empty")
    if any(char in GLOB_CHARS or char.isspace() for char in namespace):
        raise DatabaseError(
            "Redis namespace may not contain whitespace, glob characters, or braces"
        )
    if not NAMESPACE_RE.match(namespace):
        raise DatabaseError(
            "Redis namespace must be 1-128 chars of letters, numbers, _, -, or ."
        )
    return namespace


def key_prefix(namespace: str) -> str:
    return f"simplebroker:{namespace}"


def is_namespace_key(prefix: str, key: object) -> bool:
    """Return whether a Redis key belongs to this namespace's known key schema."""

    marker = f"{prefix}:"
    text = str(key)
    if not text.startswith(marker):
        return False
    parts = text[len(marker) :].split(":")

    if len(parts) == 1:
        return parts[0] in TOP_LEVEL_KEYS

    if len(parts) == 2:
        return parts == ["activity", "all"]

    if len(parts) == 3:
        group, name, kind = parts
        if group == "q":
            return kind in QUEUE_KEY_STATES and bool(QUEUE_KEY_PART_RE.match(name))
        if group == "batches":
            return kind in BATCH_KEY_KINDS and bool(BATCH_TOKEN_RE.match(name))
        if group == "activity" and name == "q":
            return bool(QUEUE_KEY_PART_RE.match(kind))

    return False


def connect(target: str) -> redis.Redis:
    try:
        client = redis.Redis.from_url(target, decode_responses=True)
        client.ping()
    except redis.RedisError as exc:
        raise DatabaseError(f"Could not connect to Valkey/Redis target: {exc}") from exc
    return client


def inspect_namespace(
    target: str,
    *,
    backend_options: Mapping[str, Any] | None = None,
) -> NamespaceInspection:
    namespace = require_namespace(backend_options)
    prefix = key_prefix(namespace)
    client = connect(target)
    try:
        meta_key = f"{prefix}:meta"
        meta = response_dict(client.hgetall(meta_key))
        key_count = 0
        for _key in client.scan_iter(f"{prefix}:*"):
            if is_namespace_key(prefix, _key):
                key_count += 1
            if key_count > 1 and meta:
                break

        if not meta:
            if key_count == 0:
                return NamespaceInspection(namespace, NamespaceState.ABSENT, 0)
            return NamespaceInspection(namespace, NamespaceState.FOREIGN, key_count)

        magic = meta.get("magic")
        schema_version_raw = meta.get("schema_version")
        try:
            schema_version = (
                int(schema_version_raw) if schema_version_raw is not None else None
            )
        except ValueError:
            schema_version = None

        required_fields = {"magic", "schema_version", "last_ts", "alias_version"}
        if not required_fields.issubset(meta):
            return NamespaceInspection(
                namespace,
                NamespaceState.PARTIAL_SIMPLEBROKER,
                max(key_count, 1),
                schema_version,
            )
        if magic == SIMPLEBROKER_MAGIC and schema_version == REDIS_SCHEMA_VERSION:
            return NamespaceInspection(
                namespace,
                NamespaceState.OWNED,
                max(key_count, 1),
                schema_version,
            )
        return NamespaceInspection(
            namespace,
            NamespaceState.PARTIAL_SIMPLEBROKER,
            max(key_count, 1),
            schema_version,
        )
    finally:
        client.close()


def validate_target(
    target: str,
    *,
    backend_options: Mapping[str, Any] | None = None,
    verify_initialized: bool = True,
) -> None:
    inspection = inspect_namespace(target, backend_options=backend_options)
    if not verify_initialized:
        if inspection.state in {NamespaceState.ABSENT, NamespaceState.OWNED}:
            return
        raise DatabaseError(
            f"Redis namespace '{inspection.namespace}' is not available for "
            f"SimpleBroker init: {inspection.state.value}"
        )

    if inspection.state is not NamespaceState.OWNED:
        if inspection.state is NamespaceState.ABSENT:
            raise DatabaseError(
                f"Redis namespace '{inspection.namespace}' does not exist; "
                "run 'broker init' first"
            )
        raise DatabaseError(
            f"Redis namespace '{inspection.namespace}' is not SimpleBroker-managed "
            f"({inspection.state.value})"
        )
