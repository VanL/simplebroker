"""Executable contract for SimpleBroker's public exception hierarchy."""

import sqlite3

from simplebroker._exceptions import StopException
from simplebroker.ext import (
    BrokerError,
    DatabaseError,
    DataError,
    IntegrityError,
    OperationalError,
)


def test_database_error_catches_all_public_database_failures() -> None:
    database_failures = (
        OperationalError,
        IntegrityError,
        DataError,
        StopException,
    )

    assert all(issubclass(error, DatabaseError) for error in database_failures)
    assert all(issubclass(error, BrokerError) for error in database_failures)
    assert issubclass(OperationalError, sqlite3.OperationalError)
    assert issubclass(IntegrityError, sqlite3.IntegrityError)
    assert issubclass(DataError, sqlite3.DataError)


def test_database_failures_are_not_os_errors() -> None:
    assert not issubclass(DatabaseError, OSError)
    assert not issubclass(OperationalError, OSError)
    assert not issubclass(StopException, OSError)
