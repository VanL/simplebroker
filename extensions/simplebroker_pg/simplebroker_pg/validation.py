"""Validation helpers for the Postgres SimpleBroker backend."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

import psycopg

from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError

from ._constants import POSTGRES_SCHEMA_VERSION

SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REQUIRED_TABLES = {"messages", "meta", "aliases"}
TYPED_META_COLUMNS = {
    "singleton",
    "magic",
    "schema_version",
    "last_ts",
    "alias_version",
}


class SchemaState(str, Enum):
    """Ownership state for the configured Postgres schema."""

    ABSENT = "ABSENT"
    EMPTY = "EMPTY"
    OWNED = "OWNED"
    FOREIGN = "FOREIGN"
    PARTIAL_SIMPLEBROKER = "PARTIAL_SIMPLEBROKER"


@dataclass(frozen=True, slots=True)
class SchemaInspection:
    """Structured inspection result for a configured schema."""

    schema: str
    state: SchemaState
    objects: frozenset[str]
    schema_version: int | None = None


def require_schema_name(backend_options: Mapping[str, Any] | None) -> str:
    """Extract and validate the configured schema name."""
    backend_options = backend_options or {}
    schema = backend_options.get("schema")
    if not isinstance(schema, str) or not schema:
        raise DatabaseError(
            "Postgres backend requires backend_options.schema in .simplebroker.toml"
        )
    if schema == "public":
        raise DatabaseError("Postgres backend refuses to use schema 'public'")
    if not SCHEMA_NAME_RE.match(schema):
        raise DatabaseError("Postgres schema must match ^[A-Za-z_][A-Za-z0-9_]*$")
    return schema


def quote_ident(identifier: str) -> str:
    """Quote a validated SQL identifier."""
    if not SCHEMA_NAME_RE.match(identifier):
        raise DatabaseError(f"Invalid identifier: {identifier}")
    return f'"{identifier}"'


def connect(dsn: str) -> psycopg.Connection:
    """Create an autocommit psycopg connection."""
    try:
        return psycopg.connect(dsn, autocommit=True)
    except psycopg.Error as exc:
        raise DatabaseError(f"Could not connect to Postgres target: {exc}") from exc


def inspect_schema(
    dsn: str,
    *,
    backend_options: Mapping[str, Any] | None = None,
) -> SchemaInspection:
    """Inspect schema ownership and initialization state."""
    schema = require_schema_name(backend_options)

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (schema,),
            )
            if cur.fetchone() is None:
                return SchemaInspection(
                    schema=schema,
                    state=SchemaState.ABSENT,
                    objects=frozenset(),
                )

            cur.execute(
                """
                SELECT c.relname
                FROM pg_class AS c
                JOIN pg_namespace AS n
                  ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
                """,
                (schema,),
            )
            objects = frozenset(str(row[0]) for row in cur.fetchall())
            if not objects:
                return SchemaInspection(
                    schema=schema,
                    state=SchemaState.EMPTY,
                    objects=objects,
                )

            if "meta" in objects:
                cur.execute(f"SET search_path TO {quote_ident(schema)}, public")
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = 'meta'
                    """,
                    (schema,),
                )
                meta_columns = {str(row[0]) for row in cur.fetchall()}
                if not TYPED_META_COLUMNS.issubset(meta_columns):
                    return SchemaInspection(
                        schema=schema,
                        state=SchemaState.PARTIAL_SIMPLEBROKER,
                        objects=objects,
                    )

                cur.execute(
                    """
                    SELECT magic, schema_version
                    FROM meta
                    WHERE singleton = TRUE
                    """
                )
                typed_row = cur.fetchone()
                magic_row = (typed_row[0],) if typed_row is not None else None
                version_row = (typed_row[1],) if typed_row is not None else None

                schema_version = (
                    int(version_row[0]) if version_row is not None else None
                )

                if magic_row is not None and magic_row[0] == SIMPLEBROKER_MAGIC:
                    if REQUIRED_TABLES.issubset(objects):
                        return SchemaInspection(
                            schema=schema,
                            state=SchemaState.OWNED,
                            objects=objects,
                            schema_version=schema_version,
                        )
                    return SchemaInspection(
                        schema=schema,
                        state=SchemaState.PARTIAL_SIMPLEBROKER,
                        objects=objects,
                        schema_version=schema_version,
                    )

                if magic_row is not None or version_row is not None:
                    return SchemaInspection(
                        schema=schema,
                        state=SchemaState.PARTIAL_SIMPLEBROKER,
                        objects=objects,
                        schema_version=schema_version,
                    )

            if REQUIRED_TABLES.intersection(objects):
                return SchemaInspection(
                    schema=schema,
                    state=SchemaState.PARTIAL_SIMPLEBROKER,
                    objects=objects,
                )

            return SchemaInspection(
                schema=schema,
                state=SchemaState.FOREIGN,
                objects=objects,
            )


def validate_target(
    dsn: str,
    *,
    backend_options: Mapping[str, Any] | None = None,
    verify_initialized: bool = True,
) -> None:
    """Validate connectivity, schema ownership, and broker metadata."""
    inspection = inspect_schema(dsn, backend_options=backend_options)

    if not verify_initialized:
        if inspection.state in {
            SchemaState.ABSENT,
            SchemaState.EMPTY,
            SchemaState.OWNED,
        }:
            return
        raise DatabaseError(
            f"Schema '{inspection.schema}' is not available for SimpleBroker init: "
            f"{inspection.state.value}"
        )

    if inspection.state is not SchemaState.OWNED:
        if inspection.state is SchemaState.ABSENT:
            raise DatabaseError(
                f"Schema '{inspection.schema}' does not exist; run 'broker init' first"
            )
        raise DatabaseError(
            f"Schema '{inspection.schema}' is not a SimpleBroker-managed schema "
            f"({inspection.state.value})"
        )

    version = inspection.schema_version or 0
    if version > POSTGRES_SCHEMA_VERSION:
        raise DatabaseError(
            f"Schema version {version} is newer than supported version "
            f"{POSTGRES_SCHEMA_VERSION}"
        )
