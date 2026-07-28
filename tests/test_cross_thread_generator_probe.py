"""Opt-in SQLite probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from simplebroker import Queue

from .helper_scripts.cross_thread_generator_probe import (
    run_cross_thread_generator_probe,
    run_cross_thread_sidecar_probe,
)
from .helper_scripts.timing import scale_timeout_for_ci

pytestmark = [pytest.mark.sqlite_only]


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
def test_sqlite_cross_thread_generator_probe(tmp_path: Path) -> None:
    db_path = str(tmp_path / "probe.db")
    result = run_cross_thread_generator_probe(
        "sqlite",
        db_path,
    )
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

    recovered = Queue("jobs", db_path=db_path)
    messages = list(
        recovered.read_generator(
            with_timestamps=False,
            delivery_guarantee="at_least_once",
        )
    )
    assert messages == ["one", "two"]
    assert recovered.peek_many(10, with_timestamps=False) == []


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
def test_sqlite_cross_thread_move_generator_restart_restores_source(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "move-probe.db")
    result = run_cross_thread_generator_probe(
        "sqlite",
        db_path,
        operation="move",
    )
    print(json.dumps(result, sort_keys=True))
    assert result["parent_timeout"] is False
    assert result["process_exitcode"] == 0
    assert "probe_error" not in result
    assert result["poisoned_after_foreign_close"] is True
    assert result["active_batch_after_foreign_close"] == "move"

    recovered = Queue("jobs", db_path=db_path)
    messages = list(
        recovered.read_generator(
            with_timestamps=False,
            delivery_guarantee="at_least_once",
        )
    )
    assert messages == ["one", "two"]
    assert Queue("done", db_path=db_path).peek_many(10, with_timestamps=False) == []


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
def test_sqlite_cross_thread_transactional_sidecar_probe(tmp_path: Path) -> None:
    result = run_cross_thread_sidecar_probe(
        "sqlite",
        str(tmp_path / "sidecar-probe.db"),
    )
    print(json.dumps(result, sort_keys=True))
    assert result["parent_timeout"] is False
    assert result["process_exitcode"] == 0
    assert "probe_error" not in result
    assert result["foreign_close_error"] is None
    assert result["foreign_close_result"] is False
    assert result["foreign_warning_count"] == 1
    assert result["poisoned_after_foreign_close"] is True
    assert result["retained_session_error"].startswith(
        "RuntimeError: sidecar session is closed"
    )
    assert result["owner_close_error"].startswith(
        "OperationalError: cross-thread finalization"
    )


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
def test_sqlite_other_core_default_contention_is_bounded(tmp_path: Path) -> None:
    bound = scale_timeout_for_ci(45.0)
    result = run_cross_thread_generator_probe(
        "sqlite",
        str(tmp_path / "bounded-default.db"),
        sqlite_default_config=True,
        second_writer_timeout=bound,
        timeout=bound + scale_timeout_for_ci(10.0),
    )
    print(json.dumps(result, sort_keys=True))
    assert result["parent_timeout"] is False
    assert result["process_exitcode"] == 0
    assert "probe_error" not in result
    assert result["poisoned_after_foreign_close"] is True
    assert result["second_writer_blocked"] is False
    assert result["second_writer_error"] is not None
    assert any(
        marker in result["second_writer_error"].lower()
        for marker in ("locked", "busy", "retry")
    )
    assert result["second_writer_elapsed"] <= bound
