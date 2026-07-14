#!/usr/bin/env python3
"""Combine main and deferred subprocess coverage data."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from coverage import CoverageData
from coverage.exceptions import CoverageException

_SUBPROCESS_COVERAGE_SUFFIX = "-subprocess"


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


def main() -> int:
    data_file = Path(os.environ.get("COVERAGE_FILE", ".coverage")).resolve()
    sources = _source_files(data_file)
    if not sources:
        print(f"No parallel coverage files found; keeping {data_file}")
        return 0

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
