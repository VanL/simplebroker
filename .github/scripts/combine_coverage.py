#!/usr/bin/env python3
"""Combine main and deferred subprocess coverage data."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from coverage import CoverageData
from coverage.exceptions import CoverageException

_SUBPROCESS_COVERAGE_SUFFIX = "-subprocess"
_DEFAULT_RETRY_TIMEOUT_SECONDS = 10.0
_DEFAULT_SETTLE_SECONDS = 0.5


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
        data.close()


def _file_signature(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return str(path), stat.st_size, stat.st_mtime_ns


def _wait_for_stable_sources(data_file: Path) -> list[Path]:
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

    while True:
        now = time.monotonic()
        sources = _source_files(data_file)
        monitored = ([data_file] if data_file.is_file() else []) + sources

        try:
            snapshot = tuple(_file_signature(path) for path in monitored)
            for path in monitored:
                _readable_coverage_file(path)
        except (CoverageException, OSError) as exc:
            last_error = exc
            stable_since = None
        else:
            last_error = None
            if snapshot != last_snapshot or stable_since is None:
                last_snapshot = snapshot
                stable_since = now
            elif stable_since is not None and now - stable_since >= settle_seconds:
                return sources

            if settle_seconds == 0:
                return sources

        if now >= deadline:
            if last_error is not None:
                raise last_error
            raise CoverageException(
                "coverage data files did not become stable before the merge deadline"
            )
        time.sleep(min(0.1, max(0.0, deadline - now)))


def main() -> int:
    data_file = Path(os.environ.get("COVERAGE_FILE", ".coverage")).resolve()
    sources = _source_files(data_file)
    if not sources:
        print(f"No parallel coverage files found; keeping {data_file}")
        return 0

    try:
        sources = _wait_for_stable_sources(data_file)
    except (CoverageException, OSError) as exc:
        print(f"Could not combine coverage data: {exc}", file=sys.stderr)
        return 1

    combined = CoverageData(basename=str(data_file))
    source_data: list[CoverageData] = []
    try:
        if data_file.is_file():
            combined.read()

        for source in sources:
            partial = CoverageData(basename=str(source))
            source_data.append(partial)
            partial.read()

        for partial in source_data:
            combined.update(partial)
        combined.write()
    except (CoverageException, OSError) as exc:
        print(f"Could not combine coverage data: {exc}", file=sys.stderr)
        return 1
    finally:
        for partial in source_data:
            partial.close()
        combined.close()

    for source in sources:
        try:
            source.unlink()
        except OSError as exc:
            print(
                f"Could not remove combined coverage data {source}: {exc}",
                file=sys.stderr,
            )
            return 1

    print(f"Combined {len(sources)} coverage data files into {data_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
