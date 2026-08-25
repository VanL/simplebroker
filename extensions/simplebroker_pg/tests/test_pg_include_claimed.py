"""include_claimed peek conformance on the Postgres backend."""

from __future__ import annotations

from typing import cast

import pytest

from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


def test_include_claimed_superset_and_exact_id(pg_core: BrokerCore) -> None:
    for i in range(3):
        pg_core.write("jobs", f"m{i}")
    rows = cast(
        list[tuple[str, int]], pg_core.peek_many("jobs", 3, with_timestamps=True)
    )
    ids = [ts for _body, ts in rows]
    assert pg_core.claim_one("jobs", with_timestamps=False) == "m0"

    assert pg_core.peek_many("jobs", 10, with_timestamps=False) == ["m1", "m2"]
    merged = cast(
        list[tuple[str, int]],
        pg_core.peek_many("jobs", 10, with_timestamps=True, include_claimed=True),
    )
    assert [body for body, _ in merged] == ["m0", "m1", "m2"]
    assert [ts for _, ts in merged] == ids

    assert pg_core.peek_one("jobs", exact_timestamp=ids[0]) is None
    assert (
        pg_core.peek_one(
            "jobs", exact_timestamp=ids[0], with_timestamps=False, include_claimed=True
        )
        == "m0"
    )


def test_move_claimed_by_id_preserves_claim_state(pg_core: BrokerCore) -> None:
    message_id = pg_core.write("source", "claimed")
    pending_id = pg_core.write("source", "pending")
    assert pg_core.claim_one("source", with_timestamps=False) == "claimed"

    assert pg_core.move_one(
        "source",
        "dest",
        exact_timestamp=message_id,
        require_unclaimed=False,
        with_timestamps=True,
    ) == ("claimed", message_id)
    assert pg_core.move_one(
        "source",
        "dest",
        exact_timestamp=pending_id,
        require_unclaimed=False,
        with_timestamps=True,
    ) == ("pending", pending_id)

    assert pg_core.peek_many("dest", 10, with_timestamps=False) == ["pending"]
    assert pg_core.peek_many(
        "dest", 10, with_timestamps=True, include_claimed=True
    ) == [("claimed", message_id), ("pending", pending_id)]
    assert pg_core.get_queue_stats() == [("dest", 1, 2)]
