"""Atheris harness: coverage-guided fuzzing of CLI argv normalization.

The harness drives
``tests/test_property_cli_args.py::test_cli_args_totality_property`` through
Hypothesis's external-fuzzer hook. It exercises only ``ArgumentProcessor`` and
``argparse``; no broker target is resolved and no command is dispatched.

Run on supported Linux:

    uv sync --frozen --extra dev --group fuzz
    mkdir -p fuzz/corpus/cli_args
    uv run --frozen --no-sync python fuzz/fuzz_cli_args.py \
        fuzz/corpus/cli_args -runs=1000 -print_final_stats=1
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast

import atheris  # type: ignore[import-untyped]


class _HypothesisFuzzer(Protocol):
    fuzz_one_input: Callable[[bytes], None]


class _HypothesisTest(Protocol):
    hypothesis: _HypothesisFuzzer


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

with atheris.instrument_imports():
    import_module("simplebroker.cli")

test_cli_args_totality_property = import_module(
    "tests.test_property_cli_args"
).test_cli_args_totality_property


def main() -> None:
    fuzz_one_input = cast(
        _HypothesisTest, test_cli_args_totality_property
    ).hypothesis.fuzz_one_input
    atheris.Setup(sys.argv, fuzz_one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
