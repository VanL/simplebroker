"""Small phase setup coordinator with advisory lock files and xattr hints.

This module is intentionally standalone: it depends only on the Python standard
library and can be copied into another project without SimpleBroker imports.

The design treats extended attributes as durable completion hints when they are
available. When xattrs are unavailable, it records each completed phase in an
atomically replaced status sidecar. A separate stable sidecar file and
process-local mutex protect setup. File existence is never considered lock
ownership; only the acquired locks are authoritative.
"""

from __future__ import annotations

import contextlib
import errno
import os
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from urllib.parse import quote

try:  # Unix/Linux/macOS
    import fcntl
except ImportError:  # pragma: no cover - unavailable on Windows
    fcntl = None  # type: ignore[assignment]

try:  # Windows
    import msvcrt
except ImportError:  # pragma: no cover - unavailable on Unix
    msvcrt = None  # type: ignore[assignment]


_MISSING_XATTR_ERRNOS = {
    value
    for value in (
        getattr(errno, "ENODATA", None),
        getattr(errno, "ENOATTR", None),
    )
    if value is not None
}
_UNSUPPORTED_XATTR_ERRNOS = {
    value
    for value in (
        getattr(errno, "ENOTSUP", None),
        getattr(errno, "EOPNOTSUPP", None),
        getattr(errno, "ENOSYS", None),
        getattr(errno, "EPERM", None),
        getattr(errno, "EACCES", None),
    )
    if value is not None
}

_PROCESS_LOCKS_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[Path, threading.RLock] = {}


class PhaseLockTimeout(TimeoutError):
    """Raised when the phase lock cannot be acquired before the timeout."""


class PhaseLockUnavailable(RuntimeError):
    """Raised when this platform has no supported advisory file lock primitive."""


def _process_lock_key(path: Path) -> Path:
    with contextlib.suppress(OSError, RuntimeError):
        return path.resolve(strict=False)
    return path.absolute()


def _process_lock_for(path: Path) -> threading.RLock:
    key = _process_lock_key(path)
    with _PROCESS_LOCKS_GUARD:
        lock = _PROCESS_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _PROCESS_LOCKS[key] = lock
        return lock


@dataclass(frozen=True)
class Phase:
    """One ordered setup phase.

    The action must be idempotent. The xattr is written only after the action
    returns successfully.
    """

    name: str
    action: Callable[[], None]
    attr_name: str | None = None
    attr_value: bytes = b"1"


@dataclass(frozen=True)
class PhaseRunResult:
    """Result returned by :meth:`PhaseLockService.run_phases`."""

    completed: tuple[str, ...]
    skipped: tuple[str, ...]
    xattrs_available: bool
    lock_path: Path
    status_paths: tuple[Path, ...] = ()


class _AdvisoryLock:
    """Cross-platform non-blocking advisory lock wrapper."""

    def __init__(self, path: Path, *, timeout: float, retry_delay: float) -> None:
        self.path = path
        self.timeout = timeout
        self.retry_delay = retry_delay
        self._file: BinaryIO | None = None
        self._locked = False
        self._process_lock: threading.RLock | None = None
        self._process_locked = False

    def acquire(
        self,
        *,
        diagnostics: Callable[[], str] | None = None,
    ) -> None:
        if fcntl is None and msvcrt is None:
            raise PhaseLockUnavailable(
                "No supported advisory file lock primitive is available"
            )

        self.path.parent.mkdir(parents=True, exist_ok=True)
        start = time.monotonic()
        last_error: OSError | None = None
        self._acquire_process_lock(
            start,
            diagnostics=diagnostics,
        )

        while True:
            try:
                lock_file = self.path.open("a+b")
            except OSError as exc:
                last_error = exc
                if time.monotonic() - start >= self.timeout:
                    self._release_process_lock()
                    raise PhaseLockTimeout(
                        self._timeout_message(
                            start,
                            last_error=last_error,
                            diagnostics=diagnostics,
                        )
                    ) from None
                time.sleep(self.retry_delay)
                continue

            try:
                with contextlib.suppress(OSError):
                    os.chmod(self.path, 0o600)
                self._prepare_lock_file(lock_file)
                self._try_lock(lock_file)
            except OSError as exc:
                last_error = exc
                lock_file.close()
                if time.monotonic() - start >= self.timeout:
                    self._release_process_lock()
                    raise PhaseLockTimeout(
                        self._timeout_message(
                            start,
                            last_error=last_error,
                            diagnostics=diagnostics,
                        )
                    ) from None
                time.sleep(self.retry_delay)
                continue

            self._file = lock_file
            self._locked = True
            return

    def _prepare_lock_file(self, lock_file: BinaryIO) -> None:
        """Ensure byte-range locking has at least one byte to lock."""

        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
            with contextlib.suppress(OSError):
                os.fsync(lock_file.fileno())
        lock_file.seek(0)

    def _acquire_process_lock(
        self,
        start: float,
        *,
        diagnostics: Callable[[], str] | None,
    ) -> None:
        process_lock = _process_lock_for(self.path)
        while True:
            if process_lock.acquire(blocking=False):
                self._process_lock = process_lock
                self._process_locked = True
                return
            if time.monotonic() - start >= self.timeout:
                raise PhaseLockTimeout(
                    self._timeout_message(
                        start,
                        last_error=None,
                        diagnostics=diagnostics,
                    )
                ) from None
            time.sleep(self.retry_delay)

    def _timeout_message(
        self,
        start: float,
        *,
        last_error: OSError | None,
        diagnostics: Callable[[], str] | None,
    ) -> str:
        elapsed = time.monotonic() - start
        parts = [
            f"Timeout waiting for phase lock: {self.path}",
            f"lock_path={self.path}",
            f"timeout={self.timeout:.3f}s",
            f"elapsed={elapsed:.3f}s",
        ]
        try:
            stat = self.path.stat()
        except OSError as exc:
            parts.append(f"lock_stat_error={exc!r}")
        else:
            parts.extend(
                [
                    "lock_exists=True",
                    f"lock_age={max(0.0, time.time() - stat.st_mtime):.3f}s",
                    f"lock_size={stat.st_size}",
                ]
            )
        if last_error is not None:
            parts.append(f"last_error={last_error!r}")
        if diagnostics is not None:
            with contextlib.suppress(Exception):
                detail = diagnostics()
                if detail:
                    parts.append(detail)
        return "; ".join(parts)

    def _try_lock(self, lock_file: BinaryIO) -> None:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        if msvcrt is not None:
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            return
        raise PhaseLockUnavailable(
            "No supported advisory file lock primitive is available"
        )

    def release(self) -> None:
        lock_file = self._file
        if lock_file is None:
            self._release_process_lock()
            return

        try:
            if self._locked and fcntl is not None:
                with contextlib.suppress(OSError):
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            if self._locked and msvcrt is not None:
                with contextlib.suppress(OSError):
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
        finally:
            self._locked = False
            self._file = None
            with contextlib.suppress(Exception):
                lock_file.close()
            self._release_process_lock()

    def _release_process_lock(self) -> None:
        process_lock = self._process_lock
        if not self._process_locked or process_lock is None:
            return
        try:
            process_lock.release()
        finally:
            self._process_locked = False
            self._process_lock = None


class PhaseLockService:
    """Coordinate ordered setup phases with a lock file and xattr completion hints."""

    def __init__(
        self,
        target: str | os.PathLike[str],
        *,
        namespace: str = "user.simplebroker",
        lock_suffix: str = ".setup.lock",
        status_suffix: str = ".setup.status",
        timeout: float = 10.0,
        retry_delay: float = 0.05,
        use_xattrs: bool | None = None,
    ) -> None:
        self.target = Path(target)
        self.namespace = namespace.strip(".")
        self.lock_path = self.target.with_suffix(lock_suffix)
        self.status_base_path = self.target.with_suffix(status_suffix)
        self.timeout = timeout
        self.retry_delay = retry_delay
        self._use_xattrs = use_xattrs
        self._xattrs_available: bool | None = None

    @property
    def xattrs_available(self) -> bool:
        """Return whether xattrs have appeared usable for this service."""

        if self._use_xattrs is False:
            return False
        if self._xattrs_available is not None:
            return self._xattrs_available
        return all(
            callable(getattr(os, name, None)) for name in ("getxattr", "setxattr")
        )

    def attr_key(self, phase_name: str, attr_name: str | None = None) -> str:
        """Return the xattr key for a phase."""

        raw_name = attr_name or phase_name
        if not raw_name or "\x00" in raw_name:
            raise ValueError(
                "phase attribute names must be non-empty and contain no NUL"
            )
        if self.namespace:
            return f"{self.namespace}.{raw_name}"
        return raw_name

    def has_phase(self, phase_name: str, *, attr_name: str | None = None) -> bool:
        """Return True when the phase completion marker is present."""

        if not self._should_use_xattrs():
            return self._status_path_for_phase(phase_name).exists()
        key = self.attr_key(phase_name, attr_name)
        return self._get_xattr(key) is not None

    def mark_phase(
        self,
        phase_name: str,
        *,
        attr_name: str | None = None,
        value: bytes = b"1",
    ) -> bool:
        """Set the phase completion xattr.

        Returns False when xattrs are unavailable or rejected by the filesystem.
        Since xattrs are a cache, callers should keep phase actions idempotent.
        """

        key = self.attr_key(phase_name, attr_name)
        return self._set_xattr(key, value)

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        """Hold the advisory setup lock."""

        lock = _AdvisoryLock(
            self.lock_path,
            timeout=self.timeout,
            retry_delay=self.retry_delay,
        )
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def run_phases(self, phases: Iterable[Phase]) -> PhaseRunResult:
        """Run missing phases in order under the setup lock.

        The lock is always acquired before completion markers are trusted. This
        makes marker observation a happens-after edge for the prior owner:
        waiters do not proceed until any process that wrote the marker has also
        released and closed the advisory lock file.
        """

        phase_list = tuple(phases)
        completed: list[str] = []
        skipped: list[str] = []
        lock = _AdvisoryLock(
            self.lock_path,
            timeout=self.timeout,
            retry_delay=self.retry_delay,
        )
        lock.acquire(
            diagnostics=lambda: self._phase_diagnostics(phase_list),
        )

        try:
            using_xattrs = self._should_use_xattrs()
            if using_xattrs:
                self._run_xattr_phases(phase_list, completed, skipped)
            else:
                self._run_status_phases(phase_list, completed, skipped)
        finally:
            lock.release()

        using_xattrs = self._should_use_xattrs()

        return PhaseRunResult(
            completed=tuple(completed),
            skipped=tuple(skipped),
            xattrs_available=self.xattrs_available,
            lock_path=self.lock_path,
            status_paths=() if using_xattrs else self._status_paths_for(phase_list),
        )

    def _run_xattr_phases(
        self,
        phases: tuple[Phase, ...],
        completed: list[str],
        skipped: list[str],
    ) -> None:
        for phase in phases:
            if self._has_xattr_phase(phase.name, attr_name=phase.attr_name):
                skipped.append(phase.name)
                continue
            phase.action()
            marked = self.mark_phase(
                phase.name,
                attr_name=phase.attr_name,
                value=phase.attr_value,
            )
            if not marked:
                self._mark_status_phase(phase.name)
            completed.append(phase.name)

    def _run_status_phases(
        self,
        phases: tuple[Phase, ...],
        completed: list[str],
        skipped: list[str],
    ) -> None:
        for phase in phases:
            if self._has_status_phase(phase.name):
                skipped.append(phase.name)
                continue
            phase.action()
            self._mark_status_phase(phase.name)
            completed.append(phase.name)

    def _should_use_xattrs(self) -> bool:
        if self._use_xattrs is False:
            self._xattrs_available = False
            return False
        if not self.xattrs_available:
            return False
        # Probe the target. Missing attr means xattrs work; unsupported means
        # use the per-phase status sidecar fallback.
        self._get_xattr(self.attr_key("__phaselock_probe__"))
        return self.xattrs_available

    def _has_xattr_phase(
        self,
        phase_name: str,
        *,
        attr_name: str | None = None,
    ) -> bool:
        key = self.attr_key(phase_name, attr_name)
        return self._get_xattr(key) is not None

    def _get_xattr(self, key: str) -> bytes | None:
        if self._use_xattrs is False:
            self._xattrs_available = False
            return None
        getter = getattr(os, "getxattr", None)
        if not callable(getter):
            self._xattrs_available = False
            return None
        try:
            value = getter(self.target, key)
        except FileNotFoundError:
            return None
        except OSError as exc:
            if exc.errno in _MISSING_XATTR_ERRNOS:
                self._xattrs_available = True
                return None
            if exc.errno in _UNSUPPORTED_XATTR_ERRNOS:
                self._xattrs_available = False
                return None
            self._xattrs_available = False
            return None
        self._xattrs_available = True
        return bytes(value)

    def _set_xattr(self, key: str, value: bytes) -> bool:
        if self._use_xattrs is False:
            self._xattrs_available = False
            return False
        setter = getattr(os, "setxattr", None)
        if not callable(setter):
            self._xattrs_available = False
            return False
        try:
            setter(self.target, key, bytes(value))
        except (FileNotFoundError, OSError):
            self._xattrs_available = False
            return False
        self._xattrs_available = True
        return True

    def _status_path_for_phase(self, phase_name: str) -> Path:
        encoded = quote(phase_name, safe="-._~")
        return self.status_base_path.with_name(
            f"{self.status_base_path.name}.{encoded}"
        )

    def status_path_for_phase(self, phase_name: str) -> Path:
        """Return the fallback status sidecar path for a phase."""
        return self._status_path_for_phase(phase_name)

    def _has_status_phase(self, phase_name: str) -> bool:
        return self._status_path_for_phase(phase_name).exists()

    def _status_paths_for(self, phases: tuple[Phase, ...]) -> tuple[Path, ...]:
        return tuple(
            self._status_path_for_phase(phase.name)
            for phase in phases
            if self._has_status_phase(phase.name)
        )

    def _status_paths(self) -> list[Path]:
        return sorted(
            self.status_base_path.parent.glob(f"{self.status_base_path.name}.*")
        )

    def _mark_status_phase(self, phase_name: str) -> None:
        destination = self._status_path_for_phase(phase_name)
        tmp_path = self.status_base_path.with_name(
            f"{self.status_base_path.name}.tmp.{os.getpid()}.{time.time_ns()}"
        )
        tmp_path.touch(mode=0o600)
        os.replace(tmp_path, destination)

    def discard_status_markers(self) -> None:
        """Remove fallback completion markers.

        Use this only when the target itself is known to be stale or absent.
        Advisory lock files are deliberately left alone because existence is not
        ownership, and another thread or process may currently hold the lock.
        """

        for path in self._status_paths():
            with contextlib.suppress(OSError):
                path.unlink()

    def _phase_diagnostics(self, phases: tuple[Phase, ...]) -> str:
        using_xattrs = self._should_use_xattrs()
        marked: list[str] = []
        missing: list[str] = []
        for phase in phases:
            is_marked = (
                self._has_xattr_phase(phase.name, attr_name=phase.attr_name)
                if using_xattrs
                else self._has_status_phase(phase.name)
            )
            (marked if is_marked else missing).append(phase.name)

        status_files = [path.name for path in self._status_paths()]
        return (
            f"target={self.target}; target_exists={self.target.exists()}; "
            f"xattrs_available={self.xattrs_available}; "
            f"marked={marked}; missing={missing}; status_files={status_files}"
        )


__all__ = [
    "Phase",
    "PhaseLockService",
    "PhaseLockTimeout",
    "PhaseLockUnavailable",
    "PhaseRunResult",
]
