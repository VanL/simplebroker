"""Test ext.py imports to increase coverage."""

import pytest

pytestmark = [pytest.mark.shared]


def test_ext_imports():
    """Test that all exports from ext.py can be imported."""
    from simplebroker.ext import (
        ActivityWaiter,
        BackendAwareRunner,
        BackendPlugin,
        BrokerError,
        DataError,
        IntegrityError,
        MessageError,
        OperationalError,
        QueueNameError,
        SetupPhase,
        SQLiteRunner,
        SQLRunner,
        TimestampError,
        TimestampGenerator,
        get_backend_plugin,
    )

    # Verify they're all importable
    assert ActivityWaiter is not None
    assert BackendAwareRunner is not None
    assert BackendPlugin is not None
    assert BrokerError is not None
    assert DataError is not None
    assert IntegrityError is not None
    assert MessageError is not None
    assert OperationalError is not None
    assert QueueNameError is not None
    assert SetupPhase is not None
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
        "ActivityWaiter",
        "BackendAwareRunner",
        "get_backend_plugin",
        "TimestampGenerator",
        "BrokerError",
        "OperationalError",
        "IntegrityError",
        "DataError",
        "TimestampError",
        "QueueNameError",
        "MessageError",
    ]

    assert set(ext.__all__) == set(expected)
