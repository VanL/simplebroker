"""Atheris harness: coverage-guided fuzzing of the dump/load parser.

``load_lines`` consumes untrusted stdin (ndjson dump format), which makes it
the most fuzz-shaped surface in SimpleBroker. This harness drives the
existing Hypothesis parser-totality property
(tests/test_property_dump_load.py::test_parser_totality_property) through
Hypothesis's external-fuzzer hook: Atheris supplies the byte buffer,
Hypothesis decodes it into generated input lines, and coverage feedback from
the instrumented parser guides the search toward unexplored validation
branches. A crash is a real contract violation (an exception other than the
documented ValueError), saved to the Hypothesis example database for replay
with plain pytest.

Run (Linux only — Atheris does not build on macOS arm64):

    pip install atheris hypothesis pytest && pip install -e .
    python fuzz/fuzz_dump_load.py fuzz/corpus/dump_load

Any libFuzzer flags (-max_total_time, -runs, ...) pass through. See
.github/workflows/fuzz.yml for the scheduled run.
"""

from __future__ import annotations

import sys
from pathlib import Path

import atheris

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Instrument only the code under test; Hypothesis/pytest imports stay
# uninstrumented so coverage feedback tracks the parser, not the test tools.
with atheris.instrument_imports():
    import simplebroker._dump  # noqa: F401

from tests.test_property_dump_load import test_parser_totality_property  # noqa: E402


def main() -> None:
    atheris.Setup(sys.argv, test_parser_totality_property.hypothesis.fuzz_one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
