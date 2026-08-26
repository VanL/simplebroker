"""PostgreSQL-only operational connection inspection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, cast

from simplebroker import Queue

from ._sql import POSTGRES_CONNECTION_STATS_SQL

_FIELD_NAMES = frozenset(
    {
        "numbackends",
        "max_connections",
        "superuser_reserved_connections",
        "reserved_connections",
    }
)


class _BackendProbe(Protocol):
    def _run_backend_probe(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]: ...


def _invalid_result(detail: str) -> ValueError:
    return ValueError(f"Invalid PostgreSQL connection statistics: {detail}")


def _extract_payload(rows: object) -> Mapping[object, object]:
    if type(rows) is not list or len(rows) != 1:
        raise _invalid_result("expected exactly one row with one column")

    row = rows[0]
    if not isinstance(row, tuple) or len(row) != 1:
        raise _invalid_result("expected exactly one row with one column")

    payload = row[0]
    if not isinstance(payload, Mapping):
        raise _invalid_result("expected a keyed JSON object")
    return payload


def _copy_integer_fields(payload: Mapping[object, object]) -> dict[str, int]:
    try:
        fields = set(payload)
    except Exception as exc:
        raise _invalid_result("expected a keyed JSON object") from exc
    if fields != _FIELD_NAMES:
        raise _invalid_result("unexpected field names")

    stats: dict[str, int] = {}
    for field in _FIELD_NAMES:
        try:
            value = payload[field]
        except Exception as exc:
            raise _invalid_result("could not read field values") from exc
        if type(value) is not int:
            raise _invalid_result(f"{field} must be an integer")
        stats[field] = value
    return stats


def _validate_numeric_relationships(stats: dict[str, int]) -> None:
    nonnegative_fields = (
        "numbackends",
        "superuser_reserved_connections",
        "reserved_connections",
    )

    if stats["max_connections"] <= 0:
        raise _invalid_result("max_connections must be positive")
    for field in nonnegative_fields:
        if stats[field] < 0:
            raise _invalid_result(f"{field} must be non-negative")
    if (
        stats["superuser_reserved_connections"] + stats["reserved_connections"]
        >= stats["max_connections"]
    ):
        raise _invalid_result("reserved connections must be below max_connections")


def _parse_connection_stats(rows: object) -> dict[str, int]:
    stats = _copy_integer_fields(_extract_payload(rows))
    _validate_numeric_relationships(stats)
    return stats


def get_connection_stats(queue: Queue) -> dict[str, int]:
    """Return a conservative PostgreSQL server connection-pressure snapshot.

    The result contains ``numbackends``, ``max_connections``,
    ``superuser_reserved_connections``, and ``reserved_connections``. The
    unfiltered ``numbackends`` sum can include database-attached workers that
    do not consume a client slot.

    Raises:
        ValueError: If ``queue`` is not PostgreSQL-backed or the result shape
            is invalid.
        DatabaseError: If PostgreSQL cannot execute the catalog query.
    """

    if queue.backend_name != "postgres":
        raise ValueError("get_connection_stats() requires a PostgreSQL Queue")

    with queue.get_connection() as connection:
        probe = cast(_BackendProbe, connection)
        rows = probe._run_backend_probe(POSTGRES_CONNECTION_STATS_SQL)
    return _parse_connection_stats(rows)
