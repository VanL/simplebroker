"""Redis backend tests for latest pending timestamp."""

from __future__ import annotations

import pytest
from simplebroker_redis import get_backend_plugin

pytestmark = [pytest.mark.redis_only]


def test_redis_latest_pending_timestamp_basic_states(
    redis_url: str,
    redis_namespace: str,
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        assert core.latest_pending_timestamp("jobs") is None

        core.insert_messages(
            [
                ("jobs", "old", 10),
                ("jobs", "middle", 20),
                ("jobs", "newest", 30),
            ]
        )
        assert core.latest_pending_timestamp("jobs") == 30

        assert core.claim_one("jobs", exact_timestamp=30, with_timestamps=True) == (
            "newest",
            30,
        )
        assert core.latest_pending_timestamp("jobs") == 20

        assert core.claim_many("jobs", 2, with_timestamps=True) == [
            ("old", 10),
            ("middle", 20),
        ]
        assert core.latest_pending_timestamp("jobs") is None
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_redis_latest_pending_timestamp_skips_reserved_latest(
    redis_url: str,
    redis_namespace: str,
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        core.insert_messages(
            [
                ("jobs", "old", 10),
                ("jobs", "reserved newest", 20),
            ]
        )
        gen = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            with_timestamps=True,
            batch_size=1,
            after_timestamp=10,
        )
        try:
            assert next(gen) == ("reserved newest", 20)
            assert core.latest_pending_timestamp("jobs") == 10
        finally:
            gen.close()
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_redis_latest_pending_timestamp_returns_none_when_all_pending_are_reserved(
    redis_url: str,
    redis_namespace: str,
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        core.insert_messages(
            [
                ("jobs", "old", 10),
                ("jobs", "newest", 20),
            ]
        )
        gen = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            with_timestamps=True,
            batch_size=2,
        )
        try:
            assert next(gen) == ("old", 10)
            assert core.latest_pending_timestamp("jobs") is None
        finally:
            gen.close()
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})
