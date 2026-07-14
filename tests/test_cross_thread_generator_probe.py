"""Opt-in SQLite probe for cross-thread transactional generator cleanup."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from .helper_scripts.cross_thread_generator_probe import (
    run_cross_thread_generator_probe,
)

pytestmark = [pytest.mark.sqlite_only]


@pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_FINALIZATION_PROBE") != "1",
    reason="diagnostic probe is opt-in",
)
def test_sqlite_cross_thread_generator_probe(tmp_path: Path) -> None:
    result = run_cross_thread_generator_probe(
        "sqlite",
        str(tmp_path / "probe.db"),
    )
    print(json.dumps(result, sort_keys=True))
    assert result["parent_timeout"] is False
    assert "probe_error" not in result
