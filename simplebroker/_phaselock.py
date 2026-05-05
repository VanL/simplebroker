"""Small phase setup coordinator with advisory lock files and xattr hints.

This module is intentionally standalone: it depends only on the Python standard
library and can be copied into another project without SimpleBroker imports.

The design treats extended attributes as durable completion hints when they are
available. When xattrs are unavailable, it records the last completed phase in
one atomically replaced status sidecar. A separate stable sidecar file is used
only as a mutex. File existence is never considered lock ownership; only the
advisory OS lock is authoritative.
"""

from __future__ import annotations

import contextlib
import errno
import os
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from urllib.parse import quote, unquote

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


class PhaseLockTimeout(TimeoutError):
    """Raised when the phase lock cannot be acquired before the timeout."""


class PhaseLockUnavailable(RuntimeError):
    """Raised when this platform has no supported advisory file lock primitive."""


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
    status_path: Path | None = None


class _AdvisoryLock:
    """Cross-platform non-blocking advisory lock wrapper."""

    def __init__(self, path: Path, *, timeout: float, retry_delay: float) -> None:
        self.path = path
        self.timeout = timeout
        self.retry_delay = retry_delay
        self._file: BinaryIO | None = None
        self._locked = False

    def acquire(self) -> None:
        if fcntl is None and msvcrt is None:
            raise PhaseLockUnavailable(
                "No supported advisory file lock primitive is available"
            )

        self.path.parent.mkdir(parents=True, exist_ok=True)
        start = time.monotonic()
        while True:
            lock_file = self.path.open("a+b")
            try:
                with contextlib.suppress(OSError):
                    os.chmod(self.path, 0o600)
                self._try_lock(lock_file)
            except OSError:
                lock_file.close()
                if time.monotonic() - start >= self.timeout:
                    raise PhaseLockTimeout(
                        f"Timeout waiting for phase lock: {self.path}"
                    ) from None
                time.sleep(self.retry_delay)
                continue

            self._file = lock_file
            self._locked = True
            return

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

        If all phase xattrs are already present, no lock is acquired. Otherwise
        the lock is acquired, xattrs are re-read, and only still-missing phases
        run. The lock file is unlinked only after all requested phases are
        durably marked, avoiding split-lock races when xattrs are unavailable.
        """

        phase_list = tuple(phases)
        using_xattrs = self._should_use_xattrs()
        if self._all_marked(phase_list, using_xattrs=using_xattrs):
            return PhaseRunResult(
                completed=(),
                skipped=tuple(phase.name for phase in phase_list),
                xattrs_available=self.xattrs_available,
                lock_path=self.lock_path,
                status_path=(
                    None if using_xattrs else self._latest_status_path(phase_list)
                ),
            )

        completed: list[str] = []
        skipped: list[str] = []
        with self.locked():
            using_xattrs = self._should_use_xattrs()
            if using_xattrs:
                self._run_xattr_phases(phase_list, completed, skipped)
            else:
                self._run_status_phases(phase_list, completed, skipped)

        if self._all_marked(phase_list, using_xattrs=using_xattrs):
            self.cleanup_lockfile()

        return PhaseRunResult(
            completed=tuple(completed),
            skipped=tuple(skipped),
            xattrs_available=self.xattrs_available,
            lock_path=self.lock_path,
            status_path=None if using_xattrs else self._latest_status_path(phase_list),
        )

    def cleanup_lockfile(self) -> None:
        """Best-effort removal of the sidecar lock file."""

        with contextlib.suppress(OSError):
            self.lock_path.unlink()

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
            self.mark_phase(
                phase.name,
                attr_name=phase.attr_name,
                value=phase.attr_value,
            )
            completed.append(phase.name)

    def _run_status_phases(
        self,
        phases: tuple[Phase, ...],
        completed: list[str],
        skipped: list[str],
    ) -> None:
        completed_index = self._latest_status_index(phases)
        for index, phase in enumerate(phases):
            if index <= completed_index:
                skipped.append(phase.name)
                continue
            phase.action()
            self._mark_status_phase(phase.name)
            completed.append(phase.name)

    def _all_marked(
        self,
        phases: tuple[Phase, ...],
        *,
        using_xattrs: bool,
    ) -> bool:
        if using_xattrs:
            return all(
                self._has_xattr_phase(phase.name, attr_name=phase.attr_name)
                for phase in phases
            )
        return self._latest_status_index(phases) >= len(phases) - 1

    def _should_use_xattrs(self) -> bool:
        if self._use_xattrs is False:
            self._xattrs_available = False
            return False
        if not self.xattrs_available:
            return False
        # Probe the target. Missing attr means xattrs work; unsupported means
        # use the single status sidecar fallback.
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

    def _status_paths(self) -> list[Path]:
        return sorted(
            self.status_base_path.parent.glob(f"{self.status_base_path.name}.*")
        )

    def _phase_from_status_path(self, path: Path) -> str | None:
        prefix = f"{self.status_base_path.name}."
        if not path.name.startswith(prefix):
            return None
        encoded = path.name[len(prefix) :]
        if encoded.startswith("tmp."):
            return None
        return unquote(encoded)

    def _latest_status_index(self, phases: tuple[Phase, ...]) -> int:
        phase_indexes = {phase.name: index for index, phase in enumerate(phases)}
        latest = -1
        for path in self._status_paths():
            phase_name = self._phase_from_status_path(path)
            if phase_name is None:
                continue
            index = phase_indexes.get(phase_name)
            if index is not None and index > latest:
                latest = index
        return latest

    def _latest_status_path(self, phases: tuple[Phase, ...]) -> Path | None:
        index = self._latest_status_index(phases)
        if index < 0:
            return None
        return self._status_path_for_phase(phases[index].name)

    def _mark_status_phase(self, phase_name: str) -> None:
        destination = self._status_path_for_phase(phase_name)
        tmp_path = self.status_base_path.with_name(
            f"{self.status_base_path.name}.tmp.{os.getpid()}.{time.time_ns()}"
        )
        tmp_path.touch(mode=0o600)
        os.replace(tmp_path, destination)
        self._cleanup_status_paths(keep=destination)

    def _cleanup_status_paths(self, *, keep: Path) -> None:
        for path in self._status_paths():
            if path == keep:
                continue
            with contextlib.suppress(OSError):
                path.unlink()


__all__ = [
    "Phase",
    "PhaseLockService",
    "PhaseLockTimeout",
    "PhaseLockUnavailable",
    "PhaseRunResult",
]
