"""Sidecar-session round trip on the Postgres backend."""

from __future__ import annotations

import pytest
from simplebroker_pg.runner import _adapt_sql

from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT ?", "SELECT %s"),
        ("SELECT ??, ?", "SELECT ?, %s"),
        ("SELECT 'literal ?'::text, ?", "SELECT 'literal ?'::text, %s"),
        ("SELECT 'it''s ?'::text, ?", "SELECT 'it''s ?'::text, %s"),
        (
            r"SELECT E'escaped \' ? still string', ?",
            r"SELECT E'escaped \' ? still string', %s",
        ),
        ('SELECT "identifier?", ?', 'SELECT "identifier?", %s'),
        ("SELECT ? -- literal ?\n, ?", "SELECT %s -- literal ?\n, %s"),
        ("SELECT /* literal ? */ ?", "SELECT /* literal ? */ %s"),
        (
            "SELECT /* outer ? /* inner ? */ outer ? */ ?",
            "SELECT /* outer ? /* inner ? */ outer ? */ %s",
        ),
        (
            "SELECT $$literal ?$$, $tag$also ?$tag$, ?",
            "SELECT $$literal ?$$, $tag$also ?$tag$, %s",
        ),
        ("SELECT ? LIKE '%foo%'", "SELECT %s LIKE '%%foo%%'"),
        ("SELECT 10 % 3, ?", "SELECT 10 %% 3, %s"),
        ("SELECT 'literal %s', ?", "SELECT 'literal %%s', %s"),
        ("SELECT ? /* literal %s */", "SELECT %s /* literal %%s */"),
        ("SELECT $tag$%s$tag$, ?", "SELECT $tag$%%s$tag$, %s"),
    ],
)
def test_adapt_sql_changes_only_parameter_qmarks(sql: str, expected: str) -> None:
    assert _adapt_sql(sql) == expected


def test_sidecar_round_trip_on_postgres(pg_core: BrokerCore) -> None:
    with pg_core.sidecar(transaction=True) as session:
        session.run(
            "CREATE TABLE IF NOT EXISTS app_sidecar_kv (k TEXT PRIMARY KEY, v TEXT)"
        )
        session.run("INSERT INTO app_sidecar_kv (k, v) VALUES (?, ?)", ("a", "1"))
    with pg_core.sidecar() as session:
        rows = list(
            session.run("SELECT v FROM app_sidecar_kv WHERE k = ?", ("a",), fetch=True)
        )
    assert rows == [("1",)]


def test_sidecar_preserves_operator_qmark_while_binding_parameter(
    pg_core: BrokerCore,
) -> None:
    with pg_core.sidecar() as session:
        rows = list(
            session.run(
                "SELECT '{\"key\": 1}'::jsonb ?? ?",
                ("key",),
                fetch=True,
            )
        )
    assert rows == [(True,)]


def test_parameter_free_sidecar_sql_is_unchanged(pg_core: BrokerCore) -> None:
    with pg_core.sidecar() as session:
        rows = list(session.run("SELECT '?'::text", fetch=True))
    assert rows == [("?",)]


def test_parameterized_sidecar_preserves_original_percent_sql(
    pg_core: BrokerCore,
) -> None:
    with pg_core.sidecar() as session:
        rows = list(
            session.run(
                "SELECT ? LIKE '%foo%', 10 % 3, '100%'::text, $tag$%s$tag$",
                ("foobar",),
                fetch=True,
            )
        )
    assert rows == [(True, 1, "100%", "%s")]
