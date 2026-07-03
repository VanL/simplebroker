"""Test ext.py imports to increase coverage."""

import pytest

pytestmark = [pytest.mark.shared]


def test_ext_imports():
    """Test that all exports from ext.py can be imported."""
    from simplebroker.ext import (
        RESERVED_TABLE_NAMES,
        ActivityWaiter,
        BackendAwareRunner,
        BackendPlugin,
        BaseWatcher,
        BrokerConnection,
        BrokerError,
        DataError,
        IntegrityError,
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
        get_backend_plugin,
    )

    # Verify they're all importable
    assert RESERVED_TABLE_NAMES is not None
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
    assert IntegrityError is not None
    assert MessageError is not None
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


def test_ext_all_exports():
    """Test that __all__ contains expected exports."""
    from simplebroker import ext

    expected = [
        "SQLRunner",
        "SQLiteRunner",
        "SetupPhase",
        "BackendPlugin",
        "BrokerConnection",
        "ActivityWaiter",
        "BackendAwareRunner",
        "MultiQueueActivityWaiterHook",
        "get_backend_plugin",
        "TimestampGenerator",
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
    assert callable(default_error_handler)
