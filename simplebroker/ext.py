"""Public extension points for SimpleBroker.

This module provides the public API for extending SimpleBroker with custom
runners and accessing core components like timestamp generation.
"""

from ._exceptions import (
    BrokerError,
    DataError,
    IntegrityError,
    MessageError,
    OperationalError,
    QueueNameError,
    TimestampError,
)
from ._runner import SQLiteRunner, SQLRunner
from ._timestamp import TimestampGenerator

__all__ = [
    # Protocols and implementations
    "SQLRunner",
    "SQLiteRunner",
    "TimestampGenerator",
    # Exceptions
    "BrokerError",
    "OperationalError",
    "IntegrityError",
    "DataError",
    "TimestampError",
    "QueueNameError",
    "MessageError",
]
