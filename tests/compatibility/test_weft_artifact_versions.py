"""Prove that Weft's inner test process possesses the intended artifacts."""

from __future__ import annotations

import importlib.metadata
import os
from collections.abc import Mapping
from pathlib import Path

import pytest

pytestmark = pytest.mark.shared

_EXPECTED_ENVIRONMENT = (
    "SIMPLEBROKER_EXPECTED_CORE_VERSION",
    "SIMPLEBROKER_EXPECTED_PG_VERSION",
    "SIMPLEBROKER_EXPECTED_REDIS_VERSION",
    "SIMPLEBROKER_EXPECTED_SOURCE_ROOT",
)


def _read_expectations(environment: Mapping[str, str]) -> dict[str, str] | None:
    """Return complete artifact expectations or reject a partial contract."""

    present = {key: environment[key] for key in _EXPECTED_ENVIRONMENT if key in environment}
    if not present:
        return None

    missing = [key for key in _EXPECTED_ENVIRONMENT if key not in present]
    assert not missing, (
        "artifact possession probe requires all SIMPLEBROKER_EXPECTED_* values; "
        f"missing {', '.join(missing)}"
    )
    return present


def test_weft_process_uses_expected_simplebroker_artifacts() -> None:
    """Check versions and reject imports from the SimpleBroker source checkout."""

    expected = _read_expectations(os.environ)
    if expected is None:
        pytest.skip("artifact expectations are not active in the ordinary source suite")

    assert importlib.metadata.version("simplebroker") == expected[
        "SIMPLEBROKER_EXPECTED_CORE_VERSION"
    ]
    assert importlib.metadata.version("simplebroker-pg") == expected[
        "SIMPLEBROKER_EXPECTED_PG_VERSION"
    ]
    assert importlib.metadata.version("simplebroker-redis") == expected[
        "SIMPLEBROKER_EXPECTED_REDIS_VERSION"
    ]

    import simplebroker_pg
    import simplebroker_redis

    import simplebroker

    source_root = Path(expected["SIMPLEBROKER_EXPECTED_SOURCE_ROOT"]).resolve()
    for module in (simplebroker, simplebroker_pg, simplebroker_redis):
        assert module.__file__ is not None
        module_path = Path(module.__file__).resolve()
        assert not module_path.is_relative_to(source_root), (
            f"{module.__name__} leaked from source checkout: {module_path}"
        )


def test_artifact_probe_rejects_partial_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partially configured possession probe must fail instead of skip."""

    for key in _EXPECTED_ENVIRONMENT:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SIMPLEBROKER_EXPECTED_CORE_VERSION", "8.0.0")

    with pytest.raises(AssertionError, match="requires all"):
        _read_expectations(os.environ)


def test_artifact_probe_is_optional_only_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The source suite may omit the complete artifact possession contract."""

    for key in _EXPECTED_ENVIRONMENT:
        monkeypatch.delenv(key, raising=False)

    assert _read_expectations(os.environ) is None
