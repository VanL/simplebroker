"""Test that SimpleBroker has no external dependencies.

SimpleBroker advertises "no external dependencies" as a core feature.
These tests ensure this promise is maintained.
"""

import ast
import sys
from pathlib import Path
import pytest

tomllib = None
try:
    import tomllib
except ImportError:
    # Python < 3.11
    pass

@pytest.mark.skipif(tomllib is None, reason="tomllib not available in Python < 3.11")
def test_pyproject_has_no_dependencies():
    """Verify pyproject.toml declares no runtime dependencies."""
    project_root = Path(__file__).parent.parent
    pyproject_path = project_root / "pyproject.toml"

    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    dependencies = pyproject["project"]["dependencies"]
    assert dependencies == [], (
        f"SimpleBroker must have no dependencies, but found: {dependencies}"
    )


def test_no_external_imports():
    """Verify that no external packages are imported.

    This test parses all Python files in simplebroker/ and ensures
    only standard library modules are imported.
    """
    project_root = Path(__file__).parent.parent
    simplebroker_dir = project_root / "simplebroker"

    # Standard library modules (not exhaustive, but covers common ones)
    # This is a list of modules known to be in stdlib
    stdlib_modules = set(sys.stdlib_module_names)

    # Also allow relative imports (starting with .)
    # and special typing imports that fall back to stdlib
    external_imports = []

    for py_file in simplebroker_dir.rglob("*.py"):
        if py_file.name.startswith("_") and py_file.name != "__init__.py":
            # Still check private modules
            pass

        with open(py_file, encoding="utf-8") as f:
            try:
                tree = ast.parse(f.read(), filename=str(py_file))
            except SyntaxError:
                continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module_name = alias.name.split(".")[0]
                    if module_name not in stdlib_modules:
                        external_imports.append((py_file.name, alias.name))

            elif isinstance(node, ast.ImportFrom):
                if node.module is None:
                    continue  # relative import like "from . import foo"

                module_name = node.module.split(".")[0]

                # Allow relative imports (start with .)
                if node.level > 0:
                    continue

                # Check if it's a stdlib module
                if module_name not in stdlib_modules:
                    # typing_extensions is explicitly forbidden
                    if module_name == "typing_extensions":
                        external_imports.append((py_file.name, node.module))
                    else:
                        external_imports.append((py_file.name, node.module))

    assert not external_imports, (
        "Found imports from non-stdlib packages:\n"
        + "\n".join(f"  {file}: {module}" for file, module in external_imports)
        + "\n\nSimpleBroker must have NO external dependencies."
    )


def test_typing_extensions_not_imported():
    """Specifically verify typing_extensions is never imported.

    This was a bug that snuck in - ensure it doesn't happen again.
    typing_extensions should never be imported; use stdlib typing instead.
    """
    project_root = Path(__file__).parent.parent
    simplebroker_dir = project_root / "simplebroker"

    violations = []

    for py_file in simplebroker_dir.rglob("*.py"):
        with open(py_file, encoding="utf-8") as f:
            content = f.read()

        # Check for typing_extensions in imports
        if "typing_extensions" in content:
            # Parse to find the actual import lines
            try:
                tree = ast.parse(content, filename=str(py_file))
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom):
                        if node.module == "typing_extensions":
                            violations.append(
                                (py_file.relative_to(project_root), node.lineno)
                            )
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            if alias.name == "typing_extensions":
                                violations.append(
                                    (py_file.relative_to(project_root), node.lineno)
                                )
            except SyntaxError:
                pass

    assert not violations, (
        "Found typing_extensions imports:\n"
        + "\n".join(f"  {file}:{line}" for file, line in violations)
        + "\n\nUse 'from typing import ...' with version checks instead."
        + "\n\nExample:"
        + "\n  if sys.version_info >= (3, 11):"
        + "\n      from typing import Self"
        + "\n  else:"
        + "\n      # fallback for Python 3.10"
    )
