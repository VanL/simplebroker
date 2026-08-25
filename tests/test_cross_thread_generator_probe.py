"""Opt-in SQLite probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from .helper_scripts.cross_thread_generator_probe import (
    run_cross_thread_generator_probe,
)
from .helper_scripts.timing import scale_timeout_for_ci

pytestmark = [pytest.mark.sqlite_only]


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
