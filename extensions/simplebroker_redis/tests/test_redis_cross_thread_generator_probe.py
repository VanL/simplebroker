"""Opt-in Redis probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import json
import os

import pytest
from simplebroker_redis import get_backend_plugin

from tests.helper_scripts.cross_thread_generator_probe import (
    run_cross_thread_generator_probe,
)

pytestmark = [pytest.mark.redis_only]


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
def test_redis_cross_thread_generator_probe(
    redis_url: str,
    redis_namespace: str,
) -> None:
    try:
        result = run_cross_thread_generator_probe(
            "redis",
            redis_url,
            redis_namespace,
        )
        print(json.dumps(result, sort_keys=True))
        assert result["parent_timeout"] is False
        assert "probe_error" not in result
    finally:
        get_backend_plugin().cleanup_target(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )
