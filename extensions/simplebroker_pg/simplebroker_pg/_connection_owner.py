"""Process-wide creation metadata, per [SB-API-14].

SM-PG-CONNECTION-OWNER owns binding and startup sealing. Both entry points
reset inherited state before locking. No driver I/O runs under this lock.
"""

from __future__ import annotations

import os
import re
import threading
from typing import Any

OWNER_PREFIX = "sb-owner:"
LABEL_PATTERN = re.compile(r"[!-~]{1,54}")


class _ConnectionOwner:
    """One binding decision shared by all first-party connection creators."""

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._label: str | None = None
        self._started = False
        self._lock = threading.Lock()

    def _check_pid(self) -> None:
        if self._pid != os.getpid():
            # Inherited locks may belong to threads absent in the child.
            self._lock = threading.Lock()
            self._label = None
            self._started = False
            self._pid = os.getpid()

    def bind(self, label: str) -> None:
        if not isinstance(label, str) or LABEL_PATTERN.fullmatch(label) is None:
            raise ValueError(
                "Connection owner must be 1-54 non-space printable ASCII characters; "
                "choose an opaque label without secrets"
            )
        self._check_pid()
        with self._lock:
            if self._label == label:
                return
            if self._label is not None or self._started:
                raise RuntimeError(
                    "Bind the connection owner before PostgreSQL resource creation "
                    "in a new dedicated process; an existing binding cannot change"
                )
            self._label = label

    def capture(self) -> dict[str, Any]:
        self._check_pid()
        with self._lock:
            self._started = True
            if self._label is None:
                return {}
            return {"application_name": OWNER_PREFIX + self._label}


_owner = _ConnectionOwner()
if hasattr(os, "register_at_fork"):
    # Reset while the child is still single-threaded: lazy first-entry resets
    # alone can race if the child starts several threads before using us.
    os.register_at_fork(after_in_child=_owner._check_pid)


def bind_connection_owner(label: str) -> None:
    """Bind first-party PostgreSQL connections in this process to an owner.

    Call before any PostgreSQL pool, listener or direct connection starts.
    Labels contain 1-54 non-space printable ASCII characters and no secrets.
    The same label is idempotent; another label or late binding requires a
    new dedicated process. Forked and spawned children must bind explicitly.
    See [SB-API-14] in docs/specs/16-python-library-api.md.
    """
    _owner.bind(label)
