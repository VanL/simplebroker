#!/usr/bin/env python3
"""Combine parallel coverage data without discarding an existing base file."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    data_file = Path(os.environ.get("COVERAGE_FILE", ".coverage")).resolve()
    shards = sorted(
        path for path in data_file.parent.glob(f"{data_file.name}.*") if path.is_file()
    )
    if not shards:
        print(f"No parallel coverage files found; keeping {data_file}")
        return 0

    result = subprocess.run(
        [sys.executable, "-m", "coverage", "combine", "--append"],
        check=False,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
