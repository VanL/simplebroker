"""Atheris harness: coverage-guided fuzzing of TimestampGenerator.validate().

Rather than maintaining a parallel fuzz target, this drives the existing
Hypothesis totality property
(tests/test_property_timestamp_validate.py::test_validate_is_total) through
Hypothesis's external-fuzzer hook: Atheris supplies the byte buffer,
Hypothesis decodes it into a generated input for the property, and coverage
feedback from the instrumented parser guides the search. A crash is therefore
a real property violation, saved to the Hypothesis example database for
replay with plain pytest.

Run (Linux only — Atheris does not build on macOS arm64):

    pip install atheris hypothesis pytest && pip install -e .
    python fuzz/fuzz_timestamp_validate.py fuzz/corpus/timestamp_validate

Any libFuzzer flags (-max_total_time, -runs, ...) pass through. See
.github/workflows/fuzz.yml for the scheduled run.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

import atheris  # type: ignore[import-untyped]


class _HypothesisFuzzer(Protocol):
    fuzz_one_input: Callable[[bytes], None]


class _HypothesisTest(Protocol):
    hypothesis: _HypothesisFuzzer


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Instrument only the code under test; Hypothesis/pytest imports stay
# uninstrumented so coverage feedback tracks the parser, not the test tools.
with atheris.instrument_imports():
    import simplebroker._timestamp  # noqa: F401

from tests.test_property_timestamp_validate import test_validate_is_total  # noqa: E402


def main() -> None:
    fuzz_one_input = cast(
        _HypothesisTest, test_validate_is_total
    ).hypothesis.fuzz_one_input
    atheris.Setup(sys.argv, fuzz_one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
