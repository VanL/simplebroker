"""Sidecar-session round trip on the Postgres backend."""

from __future__ import annotations

import pytest

from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


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
