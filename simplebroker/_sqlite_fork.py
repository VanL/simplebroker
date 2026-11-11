"""Managed SQLite lifetime admission across Python fork [SB-API-11].

SM-SQLITE-FORK-ADMISSION counts potentially live driver lifetimes. A child
inherits a sticky refusal if any were active. The hook neither waits for parent
locks nor calls SQLite. This detects managed resources, not arbitrary raw
connections, native/reentrant forks, or interpreter-shutdown finalization.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import weakref
from typing import Any, Self, cast

_active_lifetimes = 0
_lock = threading.RLock()
_unsafe_fork = False

# Child-only resurrection prevents the base C deallocator from closing an
# inherited connection, including on CPython 3.11. Never clear this list in a
# nested child: it may own the last reference to an earlier ancestor's handle.
_ABANDONED_CONNECTIONS: list[sqlite3.Connection] = []

FORK_ADVICE = (
    "Close all SQLite resources before forking, or use spawn or exec. "
    "Creating a new Queue or SQLiteRunner in this child does not make SQLite safe."
)


def _after_fork() -> None:
    global _active_lifetimes, _lock, _unsafe_fork
    _unsafe_fork = _unsafe_fork or _active_lifetimes > 0
    _active_lifetimes = 0
    _lock = threading.RLock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def ensure_sqlite_allowed() -> None:
    """Refuse unsafe child access before driver calls or inherited locks."""
    if _unsafe_fork:
        raise RuntimeError(
            "SQLite access is unsafe after fork because the parent had an open "
            "or opening SimpleBroker SQLite connection. " + FORK_ADVICE
        )


class _Lifetime:
    """One counted lifetime, without an owning reference to its connection."""

    __slots__ = ("accounted", "pid")

    def __init__(self) -> None:
        global _active_lifetimes
        ensure_sqlite_allowed()
        self.pid = os.getpid()
        self.accounted = False
        # No allocations of GC-tracked containers or driver calls in these
        # critical sections. RLock permits creator-process GC reentrancy.
        with _lock:
            _active_lifetimes += 1
            self.accounted = True

    def retire(self) -> None:
        global _active_lifetimes
        if self.pid != os.getpid():
            return
        with _lock:
            if self.accounted:
                _active_lifetimes -= 1
                self.accounted = False


class _ManagedConnection(sqlite3.Connection):
    """Retire physical ownership after explicit close or creator-process GC."""

    _lifetime: _Lifetime
    _abandoned_pid: int | None
    _construction: _ConnectionFactory | None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        self = super().__new__(cls)
        self._lifetime = _Lifetime()
        self._abandoned_pid = None
        factory = kwargs.get("factory")
        self._construction = (
            factory if isinstance(factory, _ConnectionFactory) else None
        )
        if self._construction is not None:
            self._construction.lifetime = self._lifetime
            self._construction.reference = weakref.ref(self)
        return self

    # Inherit the native __init__. A Python wrapper's exception traceback would
    # itself retain the failed object, preventing proof that it was destroyed.

    def close(self) -> None:
        lifetime = self._lifetime
        current_pid = os.getpid()
        if lifetime.pid != current_pid:
            if self._abandoned_pid != current_pid:
                _ABANDONED_CONNECTIONS.append(self)
                self._abandoned_pid = current_pid
            return
        if lifetime.accounted:
            super().close()
            lifetime.retire()

    def __del__(self) -> None:
        # Construction can fail before _Lifetime was published. Otherwise this
        # same path closes before retirement, or resurrects an inherited handle.
        # Failed finalization retains the count; it is never reported as safe.
        if hasattr(self, "_lifetime"):
            with contextlib.suppress(BaseException):
                self.close()
            construction = getattr(self, "_construction", None)
            if construction is not None:
                # Cyclic GC clears old weakrefs before running finalizers. A
                # fresh weakref still observes resurrection by another object.
                construction.reference = weakref.ref(self)
                construction.finalizer_thread = threading.get_ident()


class _ConnectionFactory:
    """Non-owning witness for one native constructor's complete unwind."""

    def __init__(self) -> None:
        self.lifetime: _Lifetime | None = None
        self.reference: weakref.ReferenceType[_ManagedConnection] | None = None
        self.finalizer_thread: int | None = None

    def __call__(self, *args: Any, **kwargs: Any) -> _ManagedConnection:
        return _ManagedConnection(*args, **kwargs)

    def retire_failed_construction(self) -> None:
        # A nested audit callback can raise a native-looking SQLITE_CANTOPEN
        # while retaining the outer object. Error codes do not prove ownership.
        # Only this thread's fully unwound native construction can prove that
        # the C deallocator completed. A foreign finalizer may have cleared its
        # weakref but still be inside native close, so remain conservative there.
        if (
            self.finalizer_thread == threading.get_ident()
            and self.reference is not None
            and self.reference() is None
            and self.lifetime is not None
        ):
            self.lifetime.retire()


def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
    """Open an internally owned, tracked connection with real sqlite semantics."""
    ensure_sqlite_allowed()
    # Temporary handles may also be finalized by a foreign creator-process
    # thread. Their operation owner still governs all use and explicit close.
    kwargs["check_same_thread"] = False
    factory = _ConnectionFactory()
    kwargs["factory"] = factory
    try:
        connection = cast(_ManagedConnection, sqlite3.connect(*args, **kwargs))
    except BaseException:
        factory.retire_failed_construction()
        raise
    connection._construction = None
    return connection
