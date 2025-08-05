"""Test ext.py imports to increase coverage."""


def test_ext_imports():
    """Test that all exports from ext.py can be imported."""
    from simplebroker.ext import (
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
    )

    # Verify they're all importable
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


def test_ext_all_exports():
    """Test that __all__ contains expected exports."""
    from simplebroker import ext

    expected = [
        "SQLRunner",
        "SQLiteRunner",
        "SetupPhase",
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
