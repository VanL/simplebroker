"""Command connection-pool options for the Valkey/Redis backend."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from simplebroker._constants import load_config, resolve_config
from simplebroker._exceptions import DatabaseError

_config = load_config()

POOL_OPTION_KEYS = frozenset({"max_connections", "pool_timeout"})


@dataclass(frozen=True, slots=True)
class RedisPoolOptions:
    max_connections: int = 50
    timeout: float = 5.0


def pool_options_from_config(
    config: Mapping[str, Any] | None = None,
    backend_options: Mapping[str, Any] | None = None,
) -> RedisPoolOptions:
    resolved = resolve_config(config or _config)
    options = backend_options or {}
    max_connections = _parse_max_connections(options.get("max_connections", 50))
    default_timeout = max(0.001, int(resolved["BROKER_BUSY_TIMEOUT"]) / 1000)
    timeout = _parse_timeout(options.get("pool_timeout", default_timeout))
    return RedisPoolOptions(max_connections=max_connections, timeout=timeout)


def _parse_max_connections(value: Any) -> int:
    if isinstance(value, bool):
        raise DatabaseError("Redis max_connections must be an integer >= 1")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise DatabaseError("Redis max_connections must be an integer >= 1") from exc
    if parsed < 1:
        raise DatabaseError("Redis max_connections must be an integer >= 1")
    return parsed


def _parse_timeout(value: Any) -> float:
    if isinstance(value, bool):
        raise DatabaseError("Redis pool_timeout must be a number > 0")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise DatabaseError("Redis pool_timeout must be a number > 0") from exc
    if parsed <= 0 or not math.isfinite(parsed):
        raise DatabaseError("Redis pool_timeout must be a number > 0")
    return parsed
