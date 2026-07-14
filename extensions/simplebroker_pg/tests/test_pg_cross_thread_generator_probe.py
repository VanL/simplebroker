"""Opt-in Postgres probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import json
import os

import pytest
from simplebroker_pg import get_backend_plugin

from tests.helper_scripts.cross_thread_generator_probe import (
    run_cross_thread_generator_probe,
)

pytestmark = [pytest.mark.pg_only]


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
def test_postgres_cross_thread_generator_probe(
    pg_dsn: str,
    pg_schema: str,
) -> None:
    try:
        result = run_cross_thread_generator_probe("postgres", pg_dsn, pg_schema)
        print(json.dumps(result, sort_keys=True))
        assert result["parent_timeout"] is False
        assert "probe_error" not in result
    finally:
        get_backend_plugin().cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )
