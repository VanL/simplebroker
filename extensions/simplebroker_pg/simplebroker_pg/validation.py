"""Validation helpers for the Postgres SimpleBroker backend."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import psycopg

from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError

from ._constants import POSTGRES_SCHEMA_VERSION

SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
OLDEST_POSTGRES_SCHEMA_VERSION = 1
REQUIRED_TABLES = {"messages", "meta", "aliases"}
OWNERSHIP_META_COLUMNS = {"singleton", "magic", "schema_version"}
TYPED_META_COLUMNS = {
    "singleton",
    "magic",
    "schema_version",
    "last_ts",
    "alias_version",
}


class SchemaState(StrEnum):
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
    current_shape_ready: bool = False


def require_schema_name(backend_options: Mapping[str, Any] | None) -> str:
    """Extract and validate the configured schema name."""
    backend_options = backend_options or {}
    schema = backend_options.get("schema")
    if not isinstance(schema, str) or not schema:
        raise DatabaseError(
            "Postgres backend requires backend_options.schema or BROKER_BACKEND_SCHEMA"
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


def _schema_has_dependent_objects(cur: psycopg.Cursor[Any], schema: str) -> bool:
    """Detect schema-owned objects omitted by the pg_class relation probe."""

    cur.execute(
        """
            SELECT d.classid::regclass::text
            FROM pg_depend AS d
            JOIN pg_namespace AS n
              ON n.oid = d.refobjid
            WHERE d.refclassid = 'pg_namespace'::regclass
              AND n.nspname = %s
            LIMIT 1
            """,
        (schema,),
    )
    return cur.fetchone() is not None


def _parse_stored_schema_version(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise TypeError("boolean schema version")
    return int(raw)


def inspect_schema(
    dsn: str,
    *,
    backend_options: Mapping[str, Any] | None = None,
) -> SchemaInspection:
    """Inspect schema ownership and initialization state."""
    schema = require_schema_name(backend_options)

    with connect(dsn) as conn, conn.cursor() as cur:
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
            # Routines, types, and other schema-owned objects do not all live in
            # pg_class. One dependency probe prevents bootstrap from treating
            # any such occupied schema as empty without enumerating catalogs.
            if _schema_has_dependent_objects(cur, schema):
                return SchemaInspection(
                    schema=schema,
                    state=SchemaState.FOREIGN,
                    objects=objects,
                )
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
            if not OWNERSHIP_META_COLUMNS.issubset(meta_columns):
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

            raw_version = version_row[0] if version_row is not None else None
            try:
                schema_version = _parse_stored_schema_version(raw_version)
            except (TypeError, ValueError):
                return SchemaInspection(
                    schema=schema,
                    state=SchemaState.PARTIAL_SIMPLEBROKER,
                    objects=objects,
                )

            if magic_row is not None and magic_row[0] == SIMPLEBROKER_MAGIC:
                return SchemaInspection(
                    schema=schema,
                    state=SchemaState.OWNED,
                    objects=objects,
                    schema_version=schema_version,
                    current_shape_ready=(
                        REQUIRED_TABLES.issubset(objects)
                        and TYPED_META_COLUMNS.issubset(meta_columns)
                    ),
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


def validate_schema_inspection(
    inspection: SchemaInspection,
    *,
    verify_initialized: bool,
) -> None:
    """Apply init/open admission after schema ownership inspection."""
    if not verify_initialized and inspection.state in {
        SchemaState.ABSENT,
        SchemaState.EMPTY,
    }:
        return

    if inspection.state is not SchemaState.OWNED:
        if not verify_initialized:
            raise DatabaseError(
                f"Schema '{inspection.schema}' is not available for SimpleBroker init: "
                f"{inspection.state.value}"
            )
        if inspection.state is SchemaState.ABSENT:
            raise DatabaseError(
                f"Schema '{inspection.schema}' does not exist; run 'broker init' first"
            )
        raise DatabaseError(
            f"Schema '{inspection.schema}' is not a SimpleBroker-managed schema "
            f"({inspection.state.value})"
        )

    version = inspection.schema_version
    if version is None:
        raise DatabaseError(
            f"Schema '{inspection.schema}' has no readable schema version"
        )
    if version < OLDEST_POSTGRES_SCHEMA_VERSION:
        raise DatabaseError(
            f"Schema version {version} is older than oldest supported version "
            f"{OLDEST_POSTGRES_SCHEMA_VERSION}"
        )
    if version > POSTGRES_SCHEMA_VERSION:
        raise DatabaseError(
            f"Schema version {version} is newer than supported version "
            f"{POSTGRES_SCHEMA_VERSION}"
        )
    if verify_initialized and version < POSTGRES_SCHEMA_VERSION:
        raise DatabaseError(
            f"Schema version {version} is older than current version "
            f"{POSTGRES_SCHEMA_VERSION}; run 'broker init' to migrate it"
        )
    if version == POSTGRES_SCHEMA_VERSION and not inspection.current_shape_ready:
        raise DatabaseError(
            f"Schema '{inspection.schema}' current schema shape is incomplete"
        )


def validate_target(
    dsn: str,
    *,
    backend_options: Mapping[str, Any] | None = None,
    verify_initialized: bool = True,
) -> None:
    """Validate connectivity, schema ownership, and broker metadata."""
    validate_schema_inspection(
        inspect_schema(dsn, backend_options=backend_options),
        verify_initialized=verify_initialized,
    )
