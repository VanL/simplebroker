# Copyright (c) 2025 Van Lindberg
# SPDX-License-Identifier: MIT

"""Small phase setup coordinator with advisory lock files and xattr hints.

This module is intentionally standalone: it depends only on the Python standard
library and can be copied into another project.

The design treats extended attributes as durable completion hints when they are
available. When xattrs are unavailable, it records completed phases in an
atomically replaced status file. A separate stable sidecar file and
process-local mutex protect setup. File existence is never considered lock
ownership; only the acquired locks are authoritative.
"""

from __future__ import annotations

import contextlib
import errno
import os
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, cast

__version__ = "1.0"

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
_MAX_STATUS_BYTES = 64 * 1024
PHASELOCK_ENABLE_XATTRS = "PHASELOCK_ENABLE_XATTRS"
_XATTR_ENV_ENABLED = {"1", "true", "yes", "on", "xattr", "xattrs"}
_XATTR_ENV_DISABLED = {"0", "false", "no", "off", "fallback", "status", "none"}


class PhaseLockTimeout(TimeoutError):
    """Raised when the phase lock cannot be acquired before the timeout."""


class PhaseLockUnavailable(RuntimeError):
    """Raised when this platform has no supported advisory file lock primitive."""


@dataclass(frozen=True)
class _XattrProvider:
    get_value: Callable[[Path, str], bytes]
    set_value: Callable[[Path, str, bytes], None]


_DARWIN_XATTR_PROVIDER_UNSET = object()
_DARWIN_XATTR_PROVIDER: _XattrProvider | None | object = _DARWIN_XATTR_PROVIDER_UNSET


def _xattr_provider() -> _XattrProvider | None:
    getter = getattr(os, "getxattr", None)
    setter = getattr(os, "setxattr", None)
    if callable(getter) and callable(setter):

        def get_value(path: Path, key: str) -> bytes:
            return bytes(getter(path, key))

        def set_value(path: Path, key: str, value: bytes) -> None:
            setter(path, key, bytes(value))

        return _XattrProvider(get_value=get_value, set_value=set_value)

    return _darwin_xattr_provider()


def _xattr_env_mode() -> bool | None:
    value = os.environ.get(PHASELOCK_ENABLE_XATTRS, "").strip().lower()
    if not value or value == "auto":
        return True
    if value in _XATTR_ENV_ENABLED:
        return True
    if value in _XATTR_ENV_DISABLED:
        return False
    return None


def _darwin_xattr_provider() -> _XattrProvider | None:
    global _DARWIN_XATTR_PROVIDER

    if sys.platform != "darwin":
        return None
    if _DARWIN_XATTR_PROVIDER is not _DARWIN_XATTR_PROVIDER_UNSET:
        return cast("_XattrProvider | None", _DARWIN_XATTR_PROVIDER)

    try:
        import ctypes
        import ctypes.util

        libc_path = ctypes.util.find_library("c")
        libc = ctypes.CDLL(libc_path or None, use_errno=True)
        getxattr = libc.getxattr
        getxattr.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_int,
        ]
        getxattr.restype = ctypes.c_ssize_t
        setxattr = libc.setxattr
        setxattr.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_int,
        ]
        setxattr.restype = ctypes.c_int

        def raise_oserror(path: Path) -> None:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), os.fspath(path))

        def get_value(path: Path, key: str) -> bytes:
            path_bytes = os.fsencode(path)
            key_bytes = os.fsencode(key)
            size = getxattr(path_bytes, key_bytes, None, 0, 0, 0)
            if size < 0:
                raise_oserror(path)
            if size == 0:
                return b""

            while True:
                buffer = ctypes.create_string_buffer(size)
                read_size = getxattr(
                    path_bytes,
                    key_bytes,
                    buffer,
                    size,
                    0,
                    0,
                )
                if read_size >= 0:
                    return bytes(buffer.raw[:read_size])
                if ctypes.get_errno() != getattr(errno, "ERANGE", 34):
                    raise_oserror(path)
                size = getxattr(path_bytes, key_bytes, None, 0, 0, 0)
                if size < 0:
                    raise_oserror(path)

        def set_value(path: Path, key: str, value: bytes) -> None:
            path_bytes = os.fsencode(path)
            key_bytes = os.fsencode(key)
            value_bytes = bytes(value)
            buffer = ctypes.create_string_buffer(value_bytes)
            if setxattr(path_bytes, key_bytes, buffer, len(value_bytes), 0, 0) != 0:
                raise_oserror(path)

        _DARWIN_XATTR_PROVIDER = _XattrProvider(
            get_value=get_value,
            set_value=set_value,
        )
    except Exception:
        _DARWIN_XATTR_PROVIDER = None
    return _DARWIN_XATTR_PROVIDER


def _validate_phase_name(phase_name: str) -> None:
    if (
        not phase_name
        or "\x00" in phase_name
        or "\n" in phase_name
        or "\r" in phase_name
    ):
        raise ValueError("phase names must be non-empty and contain no NUL or newlines")


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
        should_stop_waiting: Callable[[], bool] | None = None,
        diagnostics: Callable[[], str] | None = None,
    ) -> bool:
        if fcntl is None and msvcrt is None:
            raise PhaseLockUnavailable(
                "No supported advisory file lock primitive is available"
            )

        self.path.parent.mkdir(parents=True, exist_ok=True)
        start = time.monotonic()
        last_error: OSError | None = None
        if not self._acquire_process_lock(
            start,
            should_stop_waiting=should_stop_waiting,
            diagnostics=diagnostics,
        ):
            return False

        while True:
            if should_stop_waiting is not None and should_stop_waiting():
                self._release_process_lock()
                return False

            try:
                lock_file = self.path.open("a+b")
            except OSError as exc:
                last_error = exc
                if should_stop_waiting is not None and should_stop_waiting():
                    self._release_process_lock()
                    return False
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
                if should_stop_waiting is not None and should_stop_waiting():
                    self._release_process_lock()
                    return False
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
            return True

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
        should_stop_waiting: Callable[[], bool] | None,
        diagnostics: Callable[[], str] | None,
    ) -> bool:
        process_lock = _process_lock_for(self.path)
        while True:
            if should_stop_waiting is not None and should_stop_waiting():
                return False
            if process_lock.acquire(blocking=False):
                self._process_lock = process_lock
                self._process_locked = True
                return True
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


class AdvisoryFileLock(_AdvisoryLock):
    """Public exact-path advisory lock for already-built lock sidecar paths.

    The lock is advisory. All cooperating processes must take the same lock
    path before touching the protected resource.
    """

    @property
    def locked(self) -> bool:
        """Return whether this object currently holds the advisory lock."""

        return self._locked

    def __enter__(self) -> AdvisoryFileLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.release()


class PhaseLockService:
    """Coordinate ordered setup phases with a lock file and xattr completion hints."""

    def __init__(
        self,
        target: str | os.PathLike[str],
        *,
        namespace: str = "user.phaselock",
        lock_suffix: str = ".lock",
        status_suffix: str = ".status",
        timeout: float = 10.0,
        retry_delay: float = 0.05,
        use_xattrs: bool | None = None,
        strict_marker_locking: bool | None = None,
    ) -> None:
        self.target = Path(target)
        self.namespace = namespace.strip(".")
        self.lock_path = self.target.with_suffix(lock_suffix)
        self.status_base_path = self.target.with_suffix(status_suffix)
        self.timeout = timeout
        self.retry_delay = retry_delay
        self._use_xattrs = use_xattrs
        self._strict_marker_locking = (
            os.name == "nt" if strict_marker_locking is None else strict_marker_locking
        )
        self._xattrs_available: bool | None = None

    @property
    def strict_marker_locking(self) -> bool:
        """Return whether markers are trusted only after acquiring the lock."""

        return self._strict_marker_locking

    @property
    def xattrs_available(self) -> bool:
        """Return whether xattrs have appeared usable for this service."""

        if self._xattrs_disabled():
            return False
        if self._xattrs_available is not None:
            return self._xattrs_available
        return _xattr_provider() is not None

    def attr_key(self, phase_name: str, attr_name: str | None = None) -> str:
        """Return the xattr key for a phase."""

        raw_name = attr_name or phase_name
        _validate_phase_name(raw_name)
        if self.namespace:
            return f"{self.namespace}.{raw_name}"
        return raw_name

    def has_phase(self, phase_name: str, *, attr_name: str | None = None) -> bool:
        """Return True when the phase completion marker is present."""

        if not self._should_use_xattrs():
            return self._has_status_phase(phase_name)
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

        On Windows, the lock is always acquired before completion markers are
        trusted. This makes marker observation a happens-after edge for the
        prior owner: waiters do not proceed until any process that wrote the
        marker has also released and closed the advisory lock file.

        On POSIX, markers remain completion hints that can bypass lock
        acquisition. POSIX file-handle semantics do not need the Windows barrier,
        and preserving the fast path avoids changing non-Windows setup behavior.
        """

        phase_list = tuple(phases)
        if not phase_list:
            return PhaseRunResult(
                completed=(),
                skipped=(),
                xattrs_available=self.xattrs_available,
                lock_path=self.lock_path,
                status_paths=(),
            )

        using_xattrs = self._should_use_xattrs()
        if not self.strict_marker_locking and self._all_marked(
            phase_list, using_xattrs=using_xattrs
        ):
            return PhaseRunResult(
                completed=(),
                skipped=tuple(phase.name for phase in phase_list),
                xattrs_available=self.xattrs_available,
                lock_path=self.lock_path,
                status_paths=() if using_xattrs else self._status_paths_for(phase_list),
            )

        completed: list[str] = []
        skipped: list[str] = []
        lock = _AdvisoryLock(
            self.lock_path,
            timeout=self.timeout,
            retry_delay=self.retry_delay,
        )
        acquired = lock.acquire(
            should_stop_waiting=(
                None
                if self.strict_marker_locking
                else lambda: self._all_marked(
                    phase_list, using_xattrs=self._should_use_xattrs()
                )
            ),
            diagnostics=lambda: self._phase_diagnostics(phase_list),
        )
        if not acquired:
            using_xattrs = self._should_use_xattrs()
            return PhaseRunResult(
                completed=(),
                skipped=tuple(phase.name for phase in phase_list),
                xattrs_available=self.xattrs_available,
                lock_path=self.lock_path,
                status_paths=() if using_xattrs else self._status_paths_for(phase_list),
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
        status_order, _diagnostic = self._read_status_entries()
        status_phases = set(status_order)
        for phase in phases:
            _validate_phase_name(phase.name)
            if phase.name in status_phases:
                skipped.append(phase.name)
                continue
            phase.action()
            status_phases.add(phase.name)
            status_order.append(phase.name)
            self._write_status_phases(status_order)
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
        status_phases, _diagnostic = self._read_status_phases()
        return all(phase.name in status_phases for phase in phases)

    def _xattrs_disabled(self) -> bool:
        if self._use_xattrs is not None:
            return self._use_xattrs is False
        return _xattr_env_mode() is False

    def _should_use_xattrs(self) -> bool:
        if self._xattrs_disabled():
            self._xattrs_available = False
            return False
        if not self.xattrs_available:
            return False
        # Probe the target. Missing attr means xattrs work; unsupported means
        # use the status file fallback.
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
        if self._xattrs_disabled():
            self._xattrs_available = False
            return None
        provider = _xattr_provider()
        if provider is None:
            self._xattrs_available = False
            return None
        try:
            value = provider.get_value(self.target, key)
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
        if self._xattrs_disabled():
            self._xattrs_available = False
            return False
        provider = _xattr_provider()
        if provider is None:
            self._xattrs_available = False
            return False
        try:
            provider.set_value(self.target, key, bytes(value))
        except (FileNotFoundError, OSError):
            self._xattrs_available = False
            return False
        self._xattrs_available = True
        return True

    def _has_status_phase(self, phase_name: str) -> bool:
        status_phases, _diagnostic = self._read_status_phases()
        return phase_name in status_phases

    def _status_paths_for(self, phases: tuple[Phase, ...]) -> tuple[Path, ...]:
        if self.status_base_path.exists():
            return (self.status_base_path,)
        return ()

    def _status_paths(self) -> list[Path]:
        paths: list[Path] = []
        if self.status_base_path.exists():
            paths.append(self.status_base_path)
        paths.extend(
            self.status_base_path.parent.glob(f"{self.status_base_path.name}.tmp.*")
        )
        return sorted(set(paths))

    def _mark_status_phase(self, phase_name: str) -> None:
        _validate_phase_name(phase_name)
        status_order, _diagnostic = self._read_status_entries()
        if phase_name not in set(status_order):
            status_order.append(phase_name)
        self._write_status_phases(status_order)

    def _read_status_phases(self) -> tuple[set[str], str | None]:
        entries, diagnostic = self._read_status_entries()
        return set(entries), diagnostic

    def _read_status_entries(self) -> tuple[list[str], str | None]:
        try:
            stat = self.status_base_path.stat()
        except FileNotFoundError:
            return [], None
        except OSError as exc:
            return [], f"status_read_error={exc!r}"

        if stat.st_size > _MAX_STATUS_BYTES:
            return [], f"status_read_error=status file too large: {stat.st_size}"

        try:
            raw = self.status_base_path.read_bytes()
            text = raw.decode("utf-8")
        except OSError as exc:
            return [], f"status_read_error={exc!r}"
        except UnicodeDecodeError as exc:
            return [], f"status_parse_error={exc!r}"

        entries: list[str] = []
        seen: set[str] = set()
        for line in text.splitlines():
            if not line:
                continue
            try:
                _validate_phase_name(line)
            except ValueError as exc:
                return [], f"status_parse_error={exc!r}"
            if line in seen:
                continue
            seen.add(line)
            entries.append(line)
        return entries, None

    def _write_status_phases(self, phases: Iterable[str]) -> None:
        status_order: list[str] = []
        seen: set[str] = set()
        for phase_name in phases:
            _validate_phase_name(phase_name)
            if phase_name in seen:
                continue
            seen.add(phase_name)
            status_order.append(phase_name)

        self.status_base_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.status_base_path.with_name(
            f"{self.status_base_path.name}.tmp.{os.getpid()}.{time.time_ns()}"
        )
        data = "".join(f"{phase_name}\n" for phase_name in status_order).encode("utf-8")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        fd: int | None = None
        try:
            fd = os.open(tmp_path, flags, 0o600)
            with os.fdopen(fd, "wb") as status_file:
                fd = None
                status_file.write(data)
                status_file.flush()
                with contextlib.suppress(OSError):
                    os.fsync(status_file.fileno())
            os.replace(tmp_path, self.status_base_path)
            self._fsync_parent_directory()
        except Exception:
            if fd is not None:
                with contextlib.suppress(OSError):
                    os.close(fd)
            with contextlib.suppress(OSError):
                tmp_path.unlink()
            raise

    def _fsync_parent_directory(self) -> None:
        flags = getattr(os, "O_RDONLY", 0)
        with contextlib.suppress(OSError, AttributeError):
            fd = os.open(self.status_base_path.parent, flags)
            try:
                with contextlib.suppress(OSError):
                    os.fsync(fd)
            finally:
                os.close(fd)

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

        status_entries, status_diagnostic = self._read_status_entries()
        status_parts = [
            f"target={self.target}",
            f"target_exists={self.target.exists()}",
            f"xattrs_available={self.xattrs_available}",
            f"status_path={self.status_base_path}",
            f"status_exists={self.status_base_path.exists()}",
            f"status_phases={status_entries}",
            f"marked={marked}",
            f"missing={missing}",
        ]
        if status_diagnostic is not None:
            status_parts.append(status_diagnostic)
        return "; ".join(status_parts)


__all__ = [
    "AdvisoryFileLock",
    "PHASELOCK_ENABLE_XATTRS",
    "Phase",
    "PhaseLockService",
    "PhaseLockTimeout",
    "PhaseLockUnavailable",
    "PhaseRunResult",
    "__version__",
]
