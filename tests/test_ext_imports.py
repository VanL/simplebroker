"""Test ext.py imports to increase coverage."""

from pathlib import Path

import pytest

pytestmark = [pytest.mark.shared]

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_ext_imports():
    """Test that all exports from ext.py can be imported."""
    from simplebroker.ext import (
        BACKEND_API_VERSION,
        RESERVED_TABLE_NAMES,
        ActivityWaiter,
        BackendAwareRunner,
        BackendPlugin,
        BaseWatcher,
        BrokerConnection,
        BrokerError,
        DataError,
        DeliveryGuarantee,
        IntegrityError,
        MaintenanceSchedule,
        MessageError,
        MultiQueueActivityWaiterHook,
        OperationalError,
        PollingStrategy,
        QueueNameError,
        SetupPhase,
        SidecarSession,
        SidecarUnavailableError,
        SQLiteRunner,
        SQLRunner,
        StopWatching,
        TimestampError,
        TimestampGenerator,
        default_error_handler,
        find_project_config,
        get_backend_plugin,
        project_config_path_for_directory,
        resolve_project_target,
        vacuum_is_eligible,
        validate_delivery_guarantee,
    )

    # Verify they're all importable
    assert RESERVED_TABLE_NAMES is not None
    assert BACKEND_API_VERSION == 5
    assert ActivityWaiter is not None
    assert BaseWatcher is not None
    assert PollingStrategy is not None
    assert StopWatching is not None
    assert default_error_handler is not None
    assert BackendAwareRunner is not None
    assert BackendPlugin is not None
    assert BrokerConnection is not None
    assert BrokerError is not None
    assert DataError is not None
    assert DeliveryGuarantee is not None
    assert IntegrityError is not None
    assert MessageError is not None
    assert MaintenanceSchedule is not None
    assert MultiQueueActivityWaiterHook is not None
    assert OperationalError is not None
    assert QueueNameError is not None
    assert SetupPhase is not None
    assert SidecarSession is not None
    assert SidecarUnavailableError is not None
    assert SQLiteRunner is not None
    assert SQLRunner is not None
    assert TimestampError is not None
    assert TimestampGenerator is not None
    assert get_backend_plugin is not None
    assert callable(find_project_config)
    assert callable(project_config_path_for_directory)
    assert callable(resolve_project_target)
    assert callable(validate_delivery_guarantee)
    assert callable(vacuum_is_eligible)


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


def test_project_config_helpers_are_stable_across_public_modules() -> None:
    """Project-config discovery is public on both ``ext`` and ``project``."""
    from simplebroker import ext, project

    assert ext.find_project_config is project.find_project_config
    assert (
        ext.project_config_path_for_directory
        is project.project_config_path_for_directory
    )
    assert ext.resolve_project_target is project.resolve_project_target
    for name in (
        "find_project_config",
        "project_config_path_for_directory",
        "resolve_project_target",
    ):
        assert name in project.__all__
        assert name in ext.__all__


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
