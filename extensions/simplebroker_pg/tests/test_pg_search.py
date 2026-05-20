"""Message-body search behavior for the Postgres backend."""

from __future__ import annotations

import pytest

from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


def _timestamp_by_body(rows: list[tuple[str, int]]) -> dict[str, int]:
    return dict(rows)


def test_find_message_ids_postgres_literal_search_preserves_rows(
    pg_core: BrokerCore,
) -> None:
    pg_core.write("jobs", "tenant:acme one")
    pg_core.write("jobs", "tenant:globex one")
    pg_core.write("jobs", "tenant:acme two")
    timestamps = _timestamp_by_body(pg_core.peek_many("jobs", limit=10))

    ids = pg_core.find_message_ids(
        "jobs",
        body_contains="tenant:acme",
        limit=10,
    )

    assert ids == [
        timestamps["tenant:acme one"],
        timestamps["tenant:acme two"],
    ]
    assert pg_core.peek_many("jobs", limit=10, with_timestamps=False) == [
        "tenant:acme one",
        "tenant:globex one",
        "tenant:acme two",
    ]


def test_find_message_ids_postgres_claimed_visibility(pg_core: BrokerCore) -> None:
    pg_core.write("jobs", "target pending")
    pg_core.write("jobs", "target claimed")
    timestamps = _timestamp_by_body(pg_core.peek_many("jobs", limit=10))

    assert (
        pg_core.claim_one(
            "jobs",
            exact_timestamp=timestamps["target claimed"],
            with_timestamps=False,
        )
        == "target claimed"
    )

    assert pg_core.find_message_ids("jobs", body_contains="target", limit=10) == [
        timestamps["target pending"]
    ]
    assert pg_core.find_message_ids(
        "jobs",
        body_contains="target",
        limit=10,
        include_claimed=True,
    ) == [
        timestamps["target pending"],
        timestamps["target claimed"],
    ]


def test_find_message_ids_postgres_treats_patterns_as_literals(
    pg_core: BrokerCore,
) -> None:
    pg_core.write("jobs", "progress 100% ready")
    pg_core.write("jobs", "progress 100x ready")
    pg_core.write("jobs", "code a_c ready")
    pg_core.write("jobs", "code abc ready")
    timestamps = _timestamp_by_body(pg_core.peek_many("jobs", limit=10))

    assert pg_core.find_message_ids("jobs", body_contains="100%", limit=10) == [
        timestamps["progress 100% ready"]
    ]
    assert pg_core.find_message_ids("jobs", body_contains="a_c", limit=10) == [
        timestamps["code a_c ready"]
    ]
