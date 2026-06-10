"""Behavior tests for the public sidecar-table session API.

These tests run against real SQLite databases under tmp_path. Do not add
mocks: assert observable database state, not internal calls. This module is
auto-marked sqlite_only by conftest (Python-API tests with no run_cli usage);
Postgres and Redis coverage lives in the extension test directories.
"""

from __future__ import annotations

from simplebroker.ext import SidecarUnavailableError


def test_sidecar_unavailable_error_is_broker_error() -> None:
    from simplebroker.ext import BrokerError

    assert issubclass(SidecarUnavailableError, BrokerError)
