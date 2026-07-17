#!/usr/bin/env python3
"""Combine main and deferred subprocess coverage data."""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

from coverage import Coverage, CoverageData
from coverage.exceptions import CoverageException
from coverage.sqldata import SCHEMA_VERSION

_SUBPROCESS_COVERAGE_SUFFIX = "-subprocess"
_DEFAULT_RETRY_TIMEOUT_SECONDS = 10.0
_DEFAULT_SETTLE_SECONDS = 0.5
_COVERAGE_DATA_TABLES = ("meta", "file", "context", "line_bits", "arc", "tracer")
_COVERAGE_SCHEMA_COLUMNS = {
    "coverage_schema": ("version",),
    "meta": ("key", "value"),
    "file": ("id", "path"),
    "context": ("id", "context"),
    "line_bits": ("file_id", "context_id", "numbits"),
    "arc": ("file_id", "context_id", "fromno", "tono"),
    "tracer": ("file_id", "tracer"),
}


def _source_files(data_file: Path) -> list[Path]:
    subprocess_file = data_file.with_name(
        f"{data_file.name}{_SUBPROCESS_COVERAGE_SUFFIX}"
    )
    candidates = [
        *data_file.parent.glob(f"{data_file.name}.*"),
        subprocess_file,
        *data_file.parent.glob(f"{subprocess_file.name}.*"),
    ]
    return sorted(
        {path.resolve() for path in candidates if path.is_file() and path != data_file}
    )


def _readable_coverage_file(path: Path) -> None:
    data = CoverageData(basename=str(path))
    try:
        data.read()
    finally:
        data.close(force=True)


def _has_no_recoverable_coverage_data(path: Path) -> bool:
    """Recognize a data file interrupted before its first measurement write."""

    if path.stat().st_size == 0:
        return True

    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(path)
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "coverage_schema" not in tables:
            return False
        if (
            connection.execute("SELECT version FROM coverage_schema").fetchone()
            is not None
        ):
            return False
        return all(
            table not in tables
            or connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is None
            for table in _COVERAGE_DATA_TABLES
        )
    except sqlite3.Error:
        return False
    finally:
        if connection is not None:
            connection.close()


def _repair_missing_schema_version(path: Path) -> bool:
    """Restore coverage.py's schema marker after interrupted initialization."""

    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(path, timeout=1.0)
        connection.execute("BEGIN IMMEDIATE")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if not set(_COVERAGE_SCHEMA_COLUMNS).issubset(tables):
            return False
        for table, expected_columns in _COVERAGE_SCHEMA_COLUMNS.items():
            columns = tuple(
                str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
            )
            if columns != expected_columns:
                return False
        if (
            connection.execute("SELECT version FROM coverage_schema").fetchone()
            is not None
        ):
            return False

        connection.execute(
            "INSERT INTO coverage_schema (version) VALUES (?)",
            (SCHEMA_VERSION,),
        )
        connection.commit()
        return True
    except sqlite3.Error:
        return False
    finally:
        if connection is not None:
            connection.close()


def _file_signature(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return str(path), stat.st_size, stat.st_mtime_ns


def _wait_for_stable_sources(
    data_file: Path,
) -> tuple[list[Path], list[Path], list[Path]]:
    """Wait for late coverage writers without accepting permanent corruption."""

    retry_timeout = float(
        os.environ.get(
            "COVERAGE_COMBINE_RETRY_TIMEOUT",
            str(_DEFAULT_RETRY_TIMEOUT_SECONDS),
        )
    )
    settle_seconds = float(
        os.environ.get(
            "COVERAGE_COMBINE_SETTLE_SECONDS",
            str(_DEFAULT_SETTLE_SECONDS),
        )
    )
    deadline = time.monotonic() + retry_timeout
    last_snapshot: tuple[tuple[str, int, int], ...] | None = None
    stable_since: float | None = None
    last_error: CoverageException | OSError | None = None
    empty_sources: list[Path] = []

    while True:
        now = time.monotonic()
        sources = _source_files(data_file)
        monitored = ([data_file] if data_file.is_file() else []) + sources

        empty_sources = []
        snapshot: tuple[tuple[str, int, int], ...] | None = None
        try:
            snapshot = tuple(_file_signature(path) for path in monitored)
            for path in monitored:
                if _has_no_recoverable_coverage_data(path):
                    empty_sources.append(path)
                    continue
                _readable_coverage_file(path)
        except (CoverageException, OSError) as exc:
            last_error = exc
            if snapshot is None:
                stable_since = None
            elif snapshot != last_snapshot or stable_since is None:
                last_snapshot = snapshot
                stable_since = now
        else:
            last_error = None
            if snapshot != last_snapshot or stable_since is None:
                last_snapshot = snapshot
                stable_since = now
            elif (
                not empty_sources
                and stable_since is not None
                and now - stable_since >= settle_seconds
            ):
                return sources, [], []

            if settle_seconds == 0 and not empty_sources:
                return sources, [], []

        if now >= deadline:
            if last_error is not None:
                repaired_sources = []
                if stable_since is not None and now - stable_since >= settle_seconds:
                    repaired_sources = [
                        path
                        for path in monitored
                        if _repair_missing_schema_version(path)
                    ]
                if repaired_sources:
                    empty_sources = []
                    try:
                        for path in monitored:
                            if _has_no_recoverable_coverage_data(path):
                                empty_sources.append(path)
                                continue
                            _readable_coverage_file(path)
                    except (CoverageException, OSError) as repair_error:
                        raise repair_error from last_error
                    empty_source_set = set(empty_sources)
                    return (
                        [
                            source
                            for source in sources
                            if source not in empty_source_set
                        ],
                        empty_sources,
                        repaired_sources,
                    )
                raise last_error
            if (
                empty_sources
                and stable_since is not None
                and now - stable_since >= settle_seconds
            ):
                empty_source_set = set(empty_sources)
                return (
                    [source for source in sources if source not in empty_source_set],
                    empty_sources,
                    [],
                )
            raise CoverageException(
                "coverage data files did not become stable before the merge deadline"
            )
        time.sleep(min(0.1, max(0.0, deadline - now)))


def _normalize_stored_paths(data_file: Path) -> None:
    """Store repository-relative paths with POSIX separators on every OS."""

    source = CoverageData(basename=str(data_file))
    try:
        source.read()
        if all("\\" not in path for path in source.measured_files()):
            return

        with tempfile.TemporaryDirectory(
            prefix="coverage-normalize-",
            dir=data_file.parent,
        ) as temp_dir:
            normalized_file = Path(temp_dir) / data_file.name
            normalized = CoverageData(basename=str(normalized_file))
            try:
                normalized.update(source, map_path=lambda path: path.replace("\\", "/"))
                normalized.write()
            finally:
                normalized.close(force=True)
            source.close(force=True)
            os.replace(normalized_file, data_file)
    finally:
        source.close(force=True)


def main() -> int:
    data_file = Path(os.environ.get("COVERAGE_FILE", ".coverage")).resolve()
    sources = _source_files(data_file)
    if not sources:
        print(f"No parallel coverage files found; keeping {data_file}")
        return 0

    try:
        sources, empty_sources, repaired_sources = _wait_for_stable_sources(data_file)
    except (CoverageException, OSError) as exc:
        print(f"Could not combine coverage data: {exc}", file=sys.stderr)
        return 1

    if not sources and (not data_file.is_file() or data_file in empty_sources):
        print(
            "Could not combine coverage data: no recoverable coverage data files",
            file=sys.stderr,
        )
        return 1

    try:
        for empty_source in empty_sources:
            empty_source.unlink(missing_ok=True)
        coverage = Coverage(data_file=str(data_file))
        try:
            coverage.load()
            if sources:
                coverage.combine(
                    data_paths=[str(source) for source in sources],
                    strict=True,
                )
            coverage.save()
        finally:
            coverage.get_data().close(force=True)
        _normalize_stored_paths(data_file)
    except (CoverageException, OSError) as exc:
        print(f"Could not combine coverage data: {exc}", file=sys.stderr)
        return 1

    print(f"Combined {len(sources)} coverage data files into {data_file}")
    if repaired_sources:
        print(
            f"Repaired {len(repaired_sources)} coverage data file(s) with a "
            "missing schema version"
        )
    if empty_sources:
        print(
            f"Excluded {len(empty_sources)} interrupted empty coverage data "
            "file(s) with no measurements"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
