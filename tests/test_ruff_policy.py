"""Firing tests for the repository-owned Ruff policy."""

from __future__ import annotations

import json
import re
import subprocess
import tokenize
import tomllib
from collections import Counter
from pathlib import Path, PurePath, PureWindowsPath

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
RULE_FIXTURE = ROOT / "tests" / "fixtures" / "ruff-enabled-rules.txt"
STATIC_ANALYSIS_SPEC = (
    ROOT / "docs" / "specs" / "01-development-documentation-operating-model.md"
)

REVIEWED_FAMILIES = ["E", "W", "F", "I", "B", "C901", "C4", "UP"]
GLOBAL_IGNORES = ["E501", "B008"]
EXTENSIONLESS_PYTHON = ["bin/*"]
MCCABE_MAX_COMPLEXITY = 10
APPROVED_DIAGNOSTIC_COUNTS = {
    "BLE001": 100,
    "C901": 53,
    "PYI034": 6,
    "PYI036": 19,
    "SIM115": 1,
}
APPROVED_DIRECTIVE_COUNTS = {
    "BLE001": 98,
    "C901": 53,
    "PYI034": 6,
    "PYI036": 7,
    "SIM115": 1,
}
SUPPRESSION_REASON = "approved [DOM-10.1.1] exception"


def _ruff(
    *args: str, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["ruff", *args],
        cwd=ROOT,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )


def _ruff_config() -> tuple[dict[str, object], dict[str, object]]:
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    ruff = project["tool"]["ruff"]
    return ruff, ruff["lint"]


def _repository_path(path: PurePath) -> str:
    return path.as_posix()


def _enabled_rules() -> set[str]:
    result = _ruff("check", "--show-settings", "simplebroker/__init__.py")
    assert result.returncode == 0, result.stderr

    match = re.search(
        r"linter\.rules\.enabled = \[\n(?P<rules>.*?)\n\]",
        result.stdout,
        re.DOTALL,
    )
    assert match is not None, result.stdout
    return set(re.findall(r"\(([A-Z]+\d+)\)", match.group("rules")))


def _tracked_python_files() -> set[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    paths: set[Path] = set()
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        relative = Path(raw_path.decode())
        path = ROOT / relative
        if not path.is_file():
            continue
        if relative.suffix in {".py", ".pyi"}:
            paths.add(path.resolve())
            continue
        try:
            with path.open("rb") as handle:
                first_line = handle.readline()
        except (FileNotFoundError, IsADirectoryError):
            continue
        if first_line.startswith(b"#!") and b"python" in first_line.lower():
            paths.add(path.resolve())
    return paths


def _ruff_discovered_files() -> set[Path]:
    result = _ruff("check", "--show-files", ".")
    assert result.returncode == 0, result.stderr
    return {Path(line).resolve() for line in result.stdout.splitlines() if line}


def _lint_job() -> str:
    workflow = (ROOT / ".github" / "workflows" / "test.yml").read_text(encoding="utf-8")
    return workflow.split("  lint:", 1)[1].split("  packaging:", 1)[0]


def test_ruff_extends_defaults_without_losing_legacy_families() -> None:
    ruff, lint = _ruff_config()

    assert ruff["extend-include"] == EXTENSIONLESS_PYTHON
    assert "select" not in lint
    assert lint["extend-select"] == REVIEWED_FAMILIES
    assert lint["ignore"] == GLOBAL_IGNORES
    assert lint["mccabe"] == {"max-complexity": MCCABE_MAX_COMPLEXITY}
    assert lint.get("preview", False) is False
    assert lint.get("per-file-ignores", {}) == {}


def test_effective_ruff_rules_match_reviewed_inventory() -> None:
    expected = set(RULE_FIXTURE.read_text(encoding="utf-8").splitlines())
    assert expected
    assert _enabled_rules() == expected


def test_repository_path_uses_forward_slashes() -> None:
    assert _repository_path(PureWindowsPath("tests", "test_example.py")) == (
        "tests/test_example.py"
    )


def test_approved_suppressions_match_the_spec_registry() -> None:
    spec = STATIC_ANALYSIS_SPEC.read_text(encoding="utf-8")
    heading = "#### Approved Ruff Suppression Registry [DOM-10.1.1]"
    assert heading in spec
    registry = spec.split(heading, 1)[1].split("\n## ", 1)[0]
    registry_rows = [row for row in registry.splitlines() if row.startswith("| `")]
    assert registry_rows
    assert all("user approved 2026-07-29." in row for row in registry_rows)

    registered_locations: set[tuple[str, int]] = set()
    registered_c901_locations: set[tuple[str, int]] = set()
    for row in registry_rows:
        location_cell = row.split("|", 2)[1]
        rule_cell = row.split("|", 3)[2]
        row_locations: set[tuple[str, int]] = set()
        for path_and_lines in re.findall(r"`([^`]+:\d+(?:,\d+)*)`", location_cell):
            path_text, lines_text = path_and_lines.rsplit(":", 1)
            row_locations.update(
                (path_text, int(line_text)) for line_text in lines_text.split(",")
            )
        registered_locations.update(row_locations)
        if "C901" in re.findall(r"`([A-Z]+\d+)`", rule_cell):
            count_match = re.fullmatch(r"\s*`C901`\s+\((\d+)\)\s*", rule_cell)
            assert count_match is not None, (
                f"Malformed C901 rule/count cell: {rule_cell}"
            )
            assert len(row_locations) == int(count_match.group(1)), (
                f"C901 row count mismatch: declared {count_match.group(1)}, "
                f"listed {len(row_locations)} in {location_cell}"
            )
            duplicate_locations = registered_c901_locations & row_locations
            assert not duplicate_locations, (
                f"Duplicate C901 registry locations: {sorted(duplicate_locations)}"
            )
            registered_c901_locations.update(row_locations)

    directive_counts: Counter[str] = Counter()
    directive_locations: set[tuple[str, int]] = set()
    c901_directive_locations: set[tuple[str, int]] = set()
    for path in _ruff_discovered_files():
        with tokenize.open(path) as source:
            tokens = tokenize.generate_tokens(source.readline)
            comments = (token for token in tokens if token.type == tokenize.COMMENT)
            for comment in comments:
                if SUPPRESSION_REASON not in comment.string:
                    continue
                match = re.search(r"# noqa: ([A-Z0-9, ]+) approved", comment.string)
                assert match is not None, (
                    f"Malformed approved suppression: {path}:{comment.start[0]}: "
                    f"{comment.string}"
                )
                directive_counts.update(
                    code.strip() for code in match.group(1).split(",")
                )
                location = (
                    _repository_path(path.relative_to(ROOT)),
                    comment.start[0],
                )
                directive_locations.add(location)
                if "C901" in {code.strip() for code in match.group(1).split(",")}:
                    c901_directive_locations.add(location)
    assert dict(directive_counts) == APPROVED_DIRECTIVE_COUNTS
    assert directive_locations == registered_locations

    normal_result = _ruff("check", ".")
    assert normal_result.returncode == 0, normal_result.stdout + normal_result.stderr

    result = _ruff(
        "check",
        "--ignore-noqa",
        "--output-format",
        "json",
        ".",
    )
    assert result.returncode == 1, result.stderr
    diagnostics = json.loads(result.stdout)
    diagnostic_counts = Counter(
        diagnostic["code"]
        for diagnostic in diagnostics
        if diagnostic["code"] in APPROVED_DIAGNOSTIC_COUNTS
    )
    assert dict(diagnostic_counts) == APPROVED_DIAGNOSTIC_COUNTS
    raw_c901_locations = {
        (
            _repository_path(Path(diagnostic["filename"]).resolve().relative_to(ROOT)),
            diagnostic["noqa_row"],
        )
        for diagnostic in diagnostics
        if diagnostic["code"] == "C901"
    }
    assert raw_c901_locations == c901_directive_locations
    assert raw_c901_locations == registered_c901_locations


def test_configured_complexity_boundary_fires_at_eleven() -> None:
    def probe(complexity: int) -> str:
        branches = "\n".join(
            f"    if value == {branch}:\n        return {branch}"
            for branch in range(1, complexity)
        )
        return f"def complexity_{complexity}(value: int) -> int:\n{branches}\n    return 0\n"

    result = _ruff(
        "check",
        "--config",
        str(PYPROJECT),
        "--select",
        "C901",
        "--output-format",
        "json",
        "--stdin-filename",
        "complexity_probe.py",
        "-",
        input_text=probe(10) + "\n" + probe(11),
    )
    assert result.returncode == 1, result.stderr
    diagnostics = json.loads(result.stdout)
    assert len(diagnostics) == 1
    diagnostic = diagnostics[0]
    assert diagnostic["code"] == "C901"
    assert "`complexity_11` is too complex (11 > 10)" in diagnostic["message"]


def test_real_ruff_fires_default_and_retained_legacy_rules() -> None:
    probe = """\
def probe() -> None:
    try:
        raise ValueError
    except Exception:
        raise RuntimeError("probe")
"""
    result = _ruff(
        "check",
        "--config",
        str(PYPROJECT),
        "--stdin-filename",
        "probe.py",
        "--output-format",
        "json",
        "-",
        input_text=probe,
    )
    assert result.returncode == 1, result.stderr
    codes = {diagnostic["code"] for diagnostic in json.loads(result.stdout)}
    assert {"BLE001", "B904"} <= codes


def test_public_context_manager_annotations_remain_override_compatible(
    tmp_path: Path,
) -> None:
    """Keep the pre-policy typing contract for downstream subclasses."""
    probe = tmp_path / "context_manager_compatibility.py"
    probe.write_text(
        """\
from typing import Any, Literal

from extensions.simplebroker_redis.simplebroker_redis.core import RedisBrokerCore
from simplebroker import Queue
from simplebroker._backend_plugins import BrokerConnection
from simplebroker._phaselock import AdvisoryFileLock
from simplebroker.db import BrokerCore, BrokerDB, DBConnection
from simplebroker.ext import SQLiteRunner


class CustomLock(AdvisoryFileLock):
    def __enter__(self) -> AdvisoryFileLock:
        return super().__enter__()


class CustomQueue(Queue):
    def __enter__(self) -> Queue:
        return super().__enter__()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: str) -> None:
        return super().__exit__(exc_type, exc_val, exc_tb)


class CustomConnection(DBConnection):
    def __enter__(self) -> DBConnection:
        return super().__enter__()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: str) -> None:
        return super().__exit__(exc_type, exc_val, exc_tb)


class CustomCore(BrokerCore):
    def __enter__(self) -> BrokerCore:
        return super().__enter__()

    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_tb: str
    ) -> Literal[False]:
        return super().__exit__(exc_type, exc_val, exc_tb)


class CustomDB(BrokerDB):
    def __enter__(self) -> BrokerDB:
        return super().__enter__()


class CustomSQLiteRunner(SQLiteRunner):
    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_tb: str
    ) -> Literal[False]:
        return super().__exit__(exc_type, exc_val, exc_tb)


class CustomRedisCore(RedisBrokerCore):
    def __enter__(self) -> RedisBrokerCore:
        return super().__enter__()

    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_tb: str
    ) -> Literal[False]:
        return super().__exit__(exc_type, exc_val, exc_tb)


class CustomBrokerConnection(BrokerConnection):
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: str) -> Any:
        return None
""",
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "mypy",
            "--config-file",
            str(PYPROJECT),
            "--no-incremental",
            str(probe),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_public_exit_annotations_keep_any_typed_parameters() -> None:
    """Pin the permissive annotations that exported consumers may inspect."""
    from typing import Any, get_type_hints

    from extensions.simplebroker_redis.simplebroker_redis.core import RedisBrokerCore
    from simplebroker import Queue
    from simplebroker._backend_plugins import BrokerConnection
    from simplebroker._runner import SQLiteRunner
    from simplebroker.db import BrokerCore, DBConnection
    from simplebroker.watcher import BaseWatcher

    methods = (
        BrokerConnection.__exit__,
        SQLiteRunner.__exit__,
        DBConnection.__exit__,
        BrokerCore.__exit__,
        Queue.__exit__,
        BaseWatcher.__exit__,
        RedisBrokerCore.__exit__,
    )
    for method in methods:
        hints = get_type_hints(method)
        assert hints["exc_tb"] is Any
        if method is not BaseWatcher.__exit__:
            assert hints["exc_type"] is Any
            assert hints["exc_val"] is Any


def test_ruff_discovers_every_tracked_python_file() -> None:
    expected = _tracked_python_files()
    discovered = _ruff_discovered_files()

    assert expected <= discovered, sorted(
        str(path.relative_to(ROOT)) for path in expected - discovered
    )


def test_ci_uses_comprehensive_lint_and_explicit_formatter_paths() -> None:
    lint_job = _lint_job()
    expected_formatter = (
        "uv run --frozen --no-sync ruff format --check "
        "simplebroker tests bin .github/scripts "
        "extensions/simplebroker_pg/simplebroker_pg "
        "extensions/simplebroker_pg/tests "
        "extensions/simplebroker_redis/simplebroker_redis "
        "extensions/simplebroker_redis/tests"
    )

    assert "uv run --frozen --no-sync ruff check ." in lint_job
    assert "--preview" not in lint_job
    assert expected_formatter in " ".join(lint_job.split())
    assert "ruff format --check ." not in lint_job
