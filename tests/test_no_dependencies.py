"""Test that SimpleBroker has no external dependencies.

SimpleBroker advertises "no external dependencies" as a core feature.
These tests ensure this promise is maintained.
"""

import ast
import sys
import tomllib
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = [pytest.mark.shared]
PROJECT_ROOT = Path(__file__).parent.parent
SIMPLEBROKER_DIR = PROJECT_ROOT / "simplebroker"


def _parsed_source_files() -> Iterator[tuple[Path, ast.AST]]:
    """Yield each syntactically valid package source with its parsed tree."""

    for path in SIMPLEBROKER_DIR.rglob("*.py"):
        try:
            yield path, ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue


def _absolute_imports(tree: ast.AST) -> Iterator[tuple[str, int]]:
    """Yield absolute imported module names and their source lines."""

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from ((alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            yield node.module, node.lineno


def test_pyproject_has_no_dependencies() -> None:
    """Verify pyproject.toml declares no runtime dependencies."""
    pyproject_path = PROJECT_ROOT / "pyproject.toml"

    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    dependencies = pyproject["project"]["dependencies"]
    assert dependencies == [], (
        f"SimpleBroker must have no dependencies, but found: {dependencies}"
    )


def test_no_external_imports() -> None:
    """Verify that no external packages are imported.

    This test parses all Python files in simplebroker/ and ensures
    only standard library modules are imported.
    """
    stdlib_modules = set(sys.stdlib_module_names)
    external_imports = [
        (path.name, module)
        for path, tree in _parsed_source_files()
        for module, _line in _absolute_imports(tree)
        if module.split(".")[0] not in stdlib_modules
    ]

    assert not external_imports, (
        "Found imports from non-stdlib packages:\n"
        + "\n".join(f"  {file}: {module}" for file, module in external_imports)
        + "\n\nSimpleBroker must have NO external dependencies."
    )


def test_typing_extensions_not_imported() -> None:
    """Specifically verify typing_extensions is never imported.

    This was a bug that snuck in - ensure it doesn't happen again.
    typing_extensions should never be imported; use stdlib typing instead.
    """
    violations = [
        (path.relative_to(PROJECT_ROOT), line)
        for path, tree in _parsed_source_files()
        for module, line in _absolute_imports(tree)
        if module == "typing_extensions"
    ]

    assert not violations, (
        "Found typing_extensions imports:\n"
        + "\n".join(f"  {file}:{line}" for file, line in violations)
        + "\n\nUse stdlib typing imports directly. "
        + "This project requires Python 3.11+."
    )
