"""Run the repository-owned coverage combiner from any working directory."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

if __name__ == "__main__":
    runpy.run_module("bin.coverage_combine", run_name="__main__")
