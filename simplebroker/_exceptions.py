"""Database-agnostic exceptions for SimpleBroker.

These exceptions allow runners to use non-SQLite databases while maintaining
compatible error handling throughout the system.

For backward compatibility, database-related exceptions inherit from both
BrokerError and the corresponding sqlite3 exception.
"""

import sqlite3


class BrokerError(Exception):
    """Base exception for all SimpleBroker errors."""

    pass


class DatabaseError(BrokerError, sqlite3.DatabaseError):
    """Base class for package-defined database failures.

    This deliberately does not inherit :class:`OSError`.  Storage failures
    are database errors, while OS errors remain reserved for filesystem and
    process operations.
    """

    pass


class OperationalError(DatabaseError, sqlite3.OperationalError):
    """Database is locked, busy, or temporarily unavailable.

    Runners should raise this for retryable conditions.
    Inherits from sqlite3.OperationalError for compatibility.

    The optional ``retryable`` attribute lets non-SQLite backends classify
    contention explicitly: ``True`` forces the retry machinery to retry,
    ``False`` forbids retrying, and ``None`` (the default) falls back to
    matching SQLite lock/busy phrases in the message.
    """

    retryable: bool | None = None


class StopException(OperationalError):
    """Control flow raised when an operation is interrupted by a stop signal.

    User callbacks that catch :class:`Exception` must re-raise this exception
    so they do not swallow shutdown.
    """

    # A user-initiated stop must never be retried.
    retryable = False


class IntegrityError(DatabaseError, sqlite3.IntegrityError):
    """Database integrity constraint violated.

    Raised for unique constraints, foreign keys, etc.
    Inherits from sqlite3.IntegrityError for compatibility.
    """

    pass


class DataError(DatabaseError, sqlite3.DataError, ValueError):
    """Invalid data format or type.

    Inherits from sqlite3.DataError for compatibility.
    """

    pass


class TimestampError(BrokerError):
    """Timestamp validation or generation error.

    This is SimpleBroker-specific and doesn't map to sqlite3.
    """

    pass


class QueueNameError(BrokerError, ValueError):
    """Invalid queue name.

    This is SimpleBroker-specific and doesn't map to sqlite3. Subclasses
    ValueError for backward compatibility: queue-name rejection raised a
    plain ValueError before this class was put into use, so existing
    ``except ValueError`` handlers keep working.
    """

    pass


class MessageError(BrokerError, ValueError):
    """Invalid message content.

    This is SimpleBroker-specific and doesn't map to sqlite3. Subclasses
    ValueError for backward compatibility: message rejection raised a plain
    ValueError (or UnicodeEncodeError, itself a ValueError subclass) before
    this class was put into use, so existing ``except ValueError`` handlers
    keep working.
    """

    pass


class SidecarUnavailableError(BrokerError):
    """The active backend has no SQL storage for sidecar tables.

    Raised by ``sidecar()`` on backends (for example Redis/Valkey) that do
    not store queues in a SQL database. Catch this to detect the capability:
    there is deliberately no separate ``supports_sidecar`` flag.
    """

    pass


# ~
