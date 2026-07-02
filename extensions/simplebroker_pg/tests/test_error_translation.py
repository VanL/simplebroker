"""_translate_error must mark Postgres contention SQLSTATEs retryable."""

import psycopg
import pytest

from simplebroker._exceptions import DataError, IntegrityError, OperationalError
from simplebroker_pg.runner import _translate_error

pytestmark = pytest.mark.pg_only


class _FakePgError(psycopg.Error):
    """Carries a sqlstate without needing a live server."""

    def __init__(self, message: str, sqlstate: str) -> None:
        # psycopg.Error.__init__ reads self.sqlstate, so the backing
        # attribute must exist before super().__init__ runs.
        self._fake_sqlstate = sqlstate
        super().__init__(message)

    @property
    def sqlstate(self) -> str:  # type: ignore[override]
        return self._fake_sqlstate


@pytest.mark.parametrize("state", ["55P03", "40001", "40P01"])
def test_contention_sqlstates_are_marked_retryable(state):
    err = _translate_error(_FakePgError("contention", state))
    assert isinstance(err, OperationalError)
    assert err.retryable is True


def test_other_operational_sqlstates_keep_marker_fallback():
    err = _translate_error(_FakePgError("some failure", "57014"))
    assert isinstance(err, OperationalError)
    assert err.retryable is None


def test_integrity_and_data_translation_unchanged():
    assert isinstance(_translate_error(_FakePgError("dup", "23505")), IntegrityError)
    assert isinstance(_translate_error(_FakePgError("bad", "22001")), DataError)
