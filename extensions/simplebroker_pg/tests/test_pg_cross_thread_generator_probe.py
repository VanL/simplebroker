"""Opt-in Postgres probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import json
import os

import pytest
from simplebroker_pg import get_backend_plugin

from tests.helper_scripts.cross_thread_generator_probe import (
    run_cross_thread_generator_probe,
    run_cross_thread_sidecar_probe,
    run_queue_close_mode_probe,
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
        assert result["process_exitcode"] == 0
        assert "probe_error" not in result
        assert result["foreign_close_blocked"] is False
        assert result["foreign_close_error"] is None
        assert result["foreign_warning_count"] == 1
        assert result["poisoned_after_foreign_close"] is True
        assert result["same_core_waiter_blocked_before_close"] is True
        assert result["same_core_waiter_blocked_after_close"] is False
        assert result["same_core_waiter_error"].startswith(
            "OperationalError: cross-thread finalization"
        )
        assert result["owner_error_after_poison"].startswith(
            "OperationalError: cross-thread finalization"
        )
        assert result["owner_mutation_error_after_poison"].startswith(
            "OperationalError: cross-thread finalization"
        )
        assert result["owner_close_error"].startswith(
            "OperationalError: cross-thread finalization"
        )
        assert result["owner_shutdown_error"].startswith(
            "OperationalError: cross-thread finalization"
        )
        assert result["active_batch_after_foreign_close"] == "claim"
        assert result["core_lock_available_after_close"] is False

        for transaction in (False, True):
            for action in ("clean_exit", "throw"):
                sidecar_result = run_cross_thread_sidecar_probe(
                    "postgres",
                    pg_dsn,
                    pg_schema,
                    transaction=transaction,
                    action=action,
                )
                print(json.dumps(sidecar_result, sort_keys=True))
                assert sidecar_result["parent_timeout"] is False
                assert sidecar_result["process_exitcode"] == 0
                assert "probe_error" not in sidecar_result
                if action == "clean_exit":
                    assert sidecar_result["foreign_close_error"] is None
                    assert sidecar_result["foreign_close_result"] is False
                else:
                    assert sidecar_result["foreign_close_error"].startswith(
                        "RuntimeError: foreign sidecar throw"
                    )
                assert sidecar_result["foreign_warning_count"] == 1
                assert sidecar_result["poisoned_after_foreign_close"] is True
                assert sidecar_result["retained_session_error"].startswith(
                    "RuntimeError: sidecar session is closed"
                )
                assert sidecar_result["owner_close_error"].startswith(
                    "OperationalError: cross-thread finalization"
                )
    finally:
        get_backend_plugin().cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
@pytest.mark.parametrize(
    ("mode", "expected_close_errors"),
    [
        ("shared_non_last", [None, "OperationalError"]),
        ("shared_last", ["OperationalError"]),
        ("private_persistent", [None]),
        ("ephemeral", [None]),
    ],
)
def test_postgres_public_queue_close_modes(
    pg_dsn: str,
    pg_schema: str,
    mode: str,
    expected_close_errors: list[str | None],
) -> None:
    try:
        result = run_queue_close_mode_probe(
            "postgres",
            pg_dsn,
            pg_schema,
            mode,
        )
        assert result["parent_timeout"] is False
        assert result["process_exitcode"] == 0
        assert "probe_error" not in result
        actual = [
            "OperationalError"
            if value and value.startswith("OperationalError")
            else value
            for value in result["close_errors"]
        ]
        assert actual == expected_close_errors
        assert result["repeated_close_error"] is None
        assert result["warning_count"] == 1
    finally:
        get_backend_plugin().cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )
