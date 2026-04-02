"""Typed contract for backend SQL namespaces used by BrokerCore."""

from __future__ import annotations

from typing import Protocol, cast

from ._query_spec import RetrieveOperation, RetrieveQuerySpec


class BackendSQLNamespace(Protocol):
    """Minimum SQL surface BrokerCore expects from a backend."""

    INSERT_MESSAGE: str
    GET_MAX_MESSAGE_TS: str
    LIST_QUEUES_UNCLAIMED: str
    GET_QUEUE_STATS: str
    GET_OVERALL_STATS: str
    COUNT_CLAIMED_MESSAGES: str
    GET_TOTAL_MESSAGE_COUNT: str
    GET_VACUUM_STATS: str
    GET_DISTINCT_QUEUES: str
    CHECK_QUEUE_EXISTS: str
    CHECK_PENDING_MESSAGES: str
    CHECK_PENDING_MESSAGES_SINCE: str
    SELECT_ALIASES: str
    SELECT_ALIASES_FOR_TARGET: str
    INSERT_ALIAS: str
    DELETE_ALIAS: str

    def build_retrieve_query(
        self,
        operation: RetrieveOperation,
        spec: RetrieveQuerySpec,
    ) -> tuple[str, tuple[object, ...]]: ...


_REQUIRED_SQL_ATTRIBUTES = (
    "INSERT_MESSAGE",
    "GET_MAX_MESSAGE_TS",
    "LIST_QUEUES_UNCLAIMED",
    "GET_QUEUE_STATS",
    "GET_OVERALL_STATS",
    "COUNT_CLAIMED_MESSAGES",
    "GET_TOTAL_MESSAGE_COUNT",
    "GET_VACUUM_STATS",
    "GET_DISTINCT_QUEUES",
    "CHECK_QUEUE_EXISTS",
    "CHECK_PENDING_MESSAGES",
    "CHECK_PENDING_MESSAGES_SINCE",
    "SELECT_ALIASES",
    "SELECT_ALIASES_FOR_TARGET",
    "INSERT_ALIAS",
    "DELETE_ALIAS",
    "build_retrieve_query",
)


def ensure_backend_sql_namespace(namespace: object) -> BackendSQLNamespace:
    """Validate that a backend SQL namespace exposes the required members."""

    missing = [
        name for name in _REQUIRED_SQL_ATTRIBUTES if not hasattr(namespace, name)
    ]
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise RuntimeError(
            f"Backend SQL namespace is missing required attributes: {missing_list}"
        )
    return cast(BackendSQLNamespace, namespace)


__all__ = ["BackendSQLNamespace", "ensure_backend_sql_namespace"]
