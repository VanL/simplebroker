"""Test ext.py imports to increase coverage."""

from pathlib import Path

import pytest

pytestmark = [pytest.mark.shared]

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_ext_all_exports():
    """Test that __all__ contains expected exports."""
    from simplebroker import ext

    expected = [
        "SQLRunner",
        "SQLiteRunner",
        "SetupPhase",
        "BACKEND_API_VERSION",
        "BackendPlugin",
        "BrokerConnection",
        "ActivityWaiter",
        "BackendAwareRunner",
        "MultiQueueActivityWaiterHook",
        "find_project_config",
        "get_backend_plugin",
        "project_config_path_for_directory",
        "resolve_project_target",
        "TimestampGenerator",
        "DeliveryGuarantee",
        "validate_delivery_guarantee",
        "MaintenanceSchedule",
        "vacuum_is_eligible",
        "BrokerError",
        "DatabaseError",
        "OperationalError",
        "IntegrityError",
        "DataError",
        "TimestampError",
        "QueueNameError",
        "MessageError",
        "RESERVED_TABLE_NAMES",
        "SidecarSession",
        "SidecarUnavailableError",
        "BaseWatcher",
        "PollingStrategy",
        "StopWatching",
        "default_error_handler",
    ]

    assert set(ext.__all__) == set(expected)


def test_watcher_contract_exports():
    """The watcher subclassing contract is part of the ext surface."""
    from simplebroker.ext import (
        BaseWatcher,
        PollingStrategy,
        StopWatching,
        default_error_handler,
    )
    from simplebroker.watcher import _StopLoop

    assert _StopLoop is StopWatching  # backwards-compatible private alias
    assert BaseWatcher is not None
    assert PollingStrategy is not None
    assert hasattr(PollingStrategy, "replace_activity_waiter")
    assert callable(default_error_handler)


def test_first_party_extensions_use_public_shared_backend_contracts() -> None:
    """First-party extensions must not import the private contract modules."""
    forbidden = ("simplebroker._delivery", "simplebroker._maintenance")
    extension_roots = (
        PROJECT_ROOT / "extensions" / "simplebroker_pg" / "simplebroker_pg",
        PROJECT_ROOT / "extensions" / "simplebroker_redis" / "simplebroker_redis",
    )

    offenders = [
        path.relative_to(PROJECT_ROOT).as_posix()
        for root in extension_roots
        for path in root.rglob("*.py")
        if any(module in path.read_text(encoding="utf-8") for module in forbidden)
    ]

    assert offenders == []
