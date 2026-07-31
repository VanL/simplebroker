"""Structural and light behavioral binds for ``[SB-API-*]``."""

from __future__ import annotations

import re
from pathlib import Path

import simplebroker
import simplebroker.commands as commands
import simplebroker.ext as ext
import simplebroker.project as project
from simplebroker import Queue, dump_lines, load_lines, open_broker

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "16-python-library-api.md"
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
SPEC_INDEX = ROOT / "docs" / "specs" / "00-specs-index.md"
README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"


def _section(code: str) -> str:
    text = SPEC.read_text(encoding="utf-8")
    match = re.search(
        rf"^## .+ \[{re.escape(code)}\]\n(?P<body>.*?)(?=^## |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"missing section {code}"
    return match.group("body")


def test_api_clause_inventory_and_authority() -> None:
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-API-(\d+)\]$", text, re.MULTILINE)
    assert codes == [str(i) for i in range(1, 13)]
    for number in codes:
        assert f"| [SB-API-{number}] |" in text

    registry = REGISTRY.read_text(encoding="utf-8")
    assert "16-python-library-api.md" in registry
    assert "[SB-API-1]" in registry
    row = next(
        line
        for line in registry.splitlines()
        if "Python library" in line or "embedding API" in line.lower()
    )
    assert "`canonical-spec`" in row
    assert "16-python-library-api.md" in row

    assert "16-python-library-api.md" in SPEC_INDEX.read_text(encoding="utf-8")
    for path in (README, KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/16-python-library-api.md" in surface


def test_api_public_surfaces_language() -> None:
    body = _section("SB-API-1")
    assert "simplebroker" in body
    assert "simplebroker.ext" in body
    assert "simplebroker.commands" in body
    assert "not" in body.lower() and ("private" in body.lower() or "`_`" in body)


def test_api_root_ext_commands_all_are_importable() -> None:
    for name in simplebroker.__all__:
        assert hasattr(simplebroker, name), name
    for name in ext.__all__:
        assert hasattr(ext, name), name
    for name in commands.__all__:
        assert hasattr(commands, name), name


def test_api_project_config_helpers_on_ext_and_project() -> None:
    body = _section("SB-API-2")
    assert "find_project_config" in body
    assert "project_config_path_for_directory" in body
    assert "resolve_project_target" in body
    assert ext.find_project_config is project.find_project_config
    assert (
        ext.project_config_path_for_directory
        is project.project_config_path_for_directory
    )
    assert ext.resolve_project_target is project.resolve_project_target
    for name in (
        "find_project_config",
        "project_config_path_for_directory",
        "resolve_project_target",
    ):
        assert name in ext.__all__
        assert name in project.__all__


def test_api_queue_lifecycle_and_library_shape_language() -> None:
    lifecycle = _section("SB-API-3")
    assert "Queue" in lifecycle
    assert "close" in lifecycle.lower() or "context" in lifecycle.lower()

    shape = _section("SB-API-4")
    assert "return" in shape.lower()
    assert "raise" in shape.lower() or "exception" in shape.lower()
    assert "exit" in shape.lower()
    assert "SB-DELIVERY" in shape or "[SB-DELIVERY" in shape


def test_api_generators_watchers_sidecar_io_errors_language() -> None:
    assert "generator" in _section("SB-API-5").lower()
    watch = _section("SB-API-6")
    assert "QueueWatcher" in watch
    assert "BaseWatcher" in watch
    assert "PollingStrategy" in watch
    sidecar = _section("SB-API-7")
    assert "SidecarSession" in sidecar
    assert "RESERVED_TABLE_NAMES" in sidecar
    io = _section("SB-API-8")
    assert "dump_lines" in io
    assert "load_lines" in io
    assert "SB-IO" in io
    errors = _section("SB-API-9")
    assert "exception" in errors.lower()
    assert "message text" in errors.lower() or "not a frozen" in errors.lower()


def test_api_command_layer_and_advanced_language() -> None:
    commands_section = _section("SB-API-10")
    assert "simplebroker.commands" in commands_section
    assert "cmd_" in commands_section
    assert "exit" in commands_section.lower()
    advanced = _section("SB-API-11")
    assert "BACKEND_API_VERSION" in advanced or "backend" in advanced.lower()
    assert "SDK" in advanced or "sdk" in advanced.lower()


def test_api_cross_surface_matrix_present() -> None:
    matrix = _section("SB-API-12")
    for needle in (
        "Queue.write",
        "cmd_write",
        "dump_lines",
        "SB-DELIVERY",
        "SB-IO",
        "SB-CLI",
    ):
        assert needle in matrix


def test_api_queue_write_returns_id_not_exit_code(tmp_path: Path) -> None:
    """[SB-API-4] Library write returns a message id."""
    db = tmp_path / "api.db"
    with Queue("q", db_path=str(db)) as q:
        message_id = q.write("hello")
    assert isinstance(message_id, int)
    assert message_id > 0


def test_api_dump_load_library_entrypoints(tmp_path: Path) -> None:
    """[SB-API-8] dump_lines/load_lines are the library I/O entry points."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    with Queue("q", db_path=str(src)) as q:
        q.write("payload")
    with open_broker(str(src)) as broker:
        lines = list(dump_lines(broker))
    assert lines
    with open_broker(str(dst)) as broker:
        load_lines(broker, lines)
    with Queue("q", db_path=str(dst)) as q:
        assert q.peek_one() == "payload"


def test_api_commands_exit_code_shape(tmp_path: Path) -> None:
    """[SB-API-10] cmd_* returns CLI-style exit codes."""
    db = str(tmp_path / "cmd.db")
    assert commands.cmd_write(db, "jobs", "one") == 0
    assert commands.cmd_exists(db, "jobs") == 0
    assert commands.cmd_exists(db, "missing-queue-xyz") == 2
