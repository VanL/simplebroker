"""Structural and light behavioral binds for ``[SB-API-*]``."""

from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import get_type_hints

import pytest

import simplebroker
import simplebroker.sbqueue as sbqueue_module
from simplebroker import (
    DumpClockSkewWarning,
    Queue,
    ResolvedConfig,
    commands,
    dump_lines,
    ext,
    format_message_id,
    load_lines,
    open_broker,
    project,
    snapshot_config,
)
from simplebroker._exceptions import QueueNameError

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
    assert codes == [str(i) for i in range(1, 14)]
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


def test_api_public_message_id_formatter_contract() -> None:
    """[SB-API-1] exposes one root formatter and delegates identity semantics."""
    body = _section("SB-API-1")
    assert "simplebroker.format_message_id" in body
    assert "[SB-ID-1]" in body
    assert "[SB-ID-4]" in body
    assert "format_message_id" in simplebroker.__all__
    assert "format_message_id" not in ext.__all__


def test_api_snapshot_factory_is_package_root_public() -> None:
    assert "snapshot_config" in simplebroker.__all__
    assert simplebroker.snapshot_config is snapshot_config
    assert "snapshot_config" not in ext.__all__


def test_api_moved_message_is_package_root_public() -> None:
    assert "MovedMessage" in simplebroker.__all__
    assert simplebroker.MovedMessage.__required_keys__ == {"message", "timestamp"}
    assert simplebroker.format_message_id is format_message_id
    assert get_type_hints(format_message_id) == {
        "value": int | str,
        "return": str,
    }


def test_api_closeable_peek_iterator_contract() -> None:
    """[SB-API-1/4/5] expose one close-only Queue iterator surface."""
    public = _section("SB-API-1")
    shape = _section("SB-API-4")
    generators = _section("SB-API-5")
    spec = SPEC.read_text(encoding="utf-8")

    assert "CloseableIterator[T]" in public
    assert "structural protocol" in public
    assert "send()" in public and "throw()" in public
    assert "all_messages=True" in shape
    assert "CloseableIterator" in shape
    assert "Backend-facing `BrokerConnection` generator methods" in generators
    assert "`Iterator[...]` seams" in generators
    assert "`tests/test_dev_scripts.py`" in spec

    assert "CloseableIterator" in simplebroker.__all__
    assert simplebroker.CloseableIterator is sbqueue_module.CloseableIterator
    protocol_members = vars(simplebroker.CloseableIterator)
    assert {"__iter__", "__next__", "close"} <= protocol_members.keys()
    assert "send" not in protocol_members
    assert "throw" not in protocol_members

    # Docstring-wording pins removed (audit Task 6.2); the lazy
    # first-iteration and same-thread-close behaviors are owned by the
    # generator lifecycle suites.


def test_api_queue_rejects_alias_sigil_before_config_or_target_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def config_setup_must_not_run(_config: object) -> object:
        raise AssertionError("invalid queue reached config/target setup")

    monkeypatch.setattr(sbqueue_module, "snapshot_config", config_setup_must_not_run)
    with pytest.raises(QueueNameError):
        Queue("@alias", persistent=True)


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


def test_api_isolated_config_surface_is_root_importable() -> None:
    body = _section("SB-API-2")
    assert "resolve_isolated_config" in body
    assert "ResolvedConfig" in body
    assert "resolve_isolated_config" in simplebroker.__all__
    assert "ResolvedConfig" in simplebroker.__all__
    assert simplebroker.ResolvedConfig is ResolvedConfig


def test_api_queue_lifecycle_and_library_shape_language() -> None:
    lifecycle = _section("SB-API-3")
    assert "Queue" in lifecycle
    assert "close" in lifecycle.lower() or "context" in lifecycle.lower()
    assert "Queue.backend_name" in lifecycle
    assert '"sqlite"' in lifecycle
    assert '"redis"' in lifecycle
    assert '"postgres"' in lifecycle
    assert '`"pg"` is not an alias' in lifecycle

    shape = _section("SB-API-4")
    assert "return" in shape.lower()
    assert "raise" in shape.lower() or "exception" in shape.lower()
    assert "exit" in shape.lower()
    assert "SB-DELIVERY" in shape or "[SB-DELIVERY" in shape


def test_api_generators_watchers_sidecar_io_errors_language() -> None:
    generators = _section("SB-API-5")
    assert "generator" in generators.lower()
    assert "first iterated" in generators.lower()
    watch = _section("SB-API-6")
    assert "QueueWatcher" in watch
    assert "BaseWatcher" in watch
    assert "PollingStrategy" in watch
    assert "existing `Queue`" in watch
    sidecar = _section("SB-API-7")
    assert "SidecarSession" in sidecar
    assert "RESERVED_TABLE_NAMES" in sidecar
    assert "**SQLite:**" in sidecar
    sqlite_migration = sidecar.split("**SQLite:**", 1)[1].split("**PostgreSQL:**", 1)[0]
    assert "does not block" in sqlite_migration
    assert "whole-file backup" in sqlite_migration
    postgres_migration = sidecar.split("**PostgreSQL:**", 1)[1]
    assert "`RESTRICT`" in postgres_migration
    assert "fails without mutation" in postgres_migration
    io = _section("SB-API-8")
    assert "dump_lines" in io
    assert "load_lines" in io
    assert "SB-IO" in io
    errors = _section("SB-API-9")
    assert "exception" in errors.lower()
    assert "InvalidConfigError" in errors
    assert "ValueError" in errors


def test_api_v6_cutover_contract_names_the_legacy_pg_exception() -> None:
    body = " ".join(_section("SB-API-11").split()).lower()
    assert "normal target-backed cold-admission path" in body
    assert "`simplebroker-pg` 3.10.0" in body
    assert "missing `order_id` diagnostic" in body
    assert "every transaction that accesses caller-owned sidecars" in body
    assert "core and its backend extension as one coherent set" in body
    assert "no old client may open or retain the target" in body
    assert "not a rollback or mixed-version mechanism" in body
    assert "restoring the whole pre-v6 target" in body


def test_api_polling_strategy_defaults_match_canonical_config() -> None:
    """[SB-API-6] binds all public constructor defaults to canonical config."""
    watch = _section("SB-API-6")
    parameter_keys = {
        "initial_checks": "BROKER_INITIAL_CHECKS",
        "max_interval": "BROKER_MAX_INTERVAL",
        "burst_sleep": "BROKER_BURST_SLEEP",
        "jitter_factor": "BROKER_JITTER_FACTOR",
    }
    config = simplebroker.resolve_isolated_config({})
    parameters = inspect.signature(ext.PollingStrategy).parameters

    assert list(parameters) == ["stop_event", *parameter_keys]
    for parameter, key in parameter_keys.items():
        assert key in watch
        default = parameters[parameter].default
        assert default == config[key]
        assert type(default) is type(config[key])


def test_api_activity_waiter_terminal_close_contract() -> None:
    body = _section("SB-API-6")
    normalized_body = " ".join(body.split()).lower()
    # Identifier tokens only; behaviors owned by the terminal-close
    # runtime tests cited in the SB-API-6 verification row.
    assert "baseexception.add_note()" in normalized_body
    assert not hasattr(simplebroker.ActivityWaiter, "shutdown")


def test_api_watcher_start_stop_cleanup_ownership_contract() -> None:
    """[SB-API-6] names the ownership contract; behavior owners are the
    test_watcher_stop_contract nodes in the verification row (audit
    Task 6.2 removed the sentence pins)."""
    body = " ".join(_section("SB-API-6").split()).lower()
    assert "cleanup" in body
    assert "run_forever()" in body


def test_api_command_layer_and_advanced_language() -> None:
    commands_section = _section("SB-API-10")
    assert "simplebroker.commands" in commands_section
    assert "cmd_" in commands_section
    assert "exit" in commands_section.lower()
    assert "explicit target" in commands_section.lower()
    assert "InvalidConfigError" in commands_section
    advanced = _section("SB-API-11")
    assert "BACKEND_API_VERSION" in advanced or "backend" in advanced.lower()
    assert "SDK" in advanced or "sdk" in advanced.lower()


def test_api_owned_runner_lifecycle_and_backend_v9_contract() -> None:
    """[SB-API-11] identifier and version tokens; behaviors owned by the
    runner-lifecycle and timestamp-advance suites in the verification
    row."""
    advanced = " ".join(_section("SB-API-11").split()).lower()
    assert "backend api v6" in advanced
    assert "backend api v7" in advanced
    assert "backend api v8" in advanced
    assert "backend api v9" in advanced
    assert "keep_newest" in advanced
    assert "selection order" in advanced
    assert "advance_last_timestamp(timestamp)" in advanced
    assert "linearization point" in advanced
    assert "runner remains reusable" in advanced
    assert "owning process session or factory" in advanced


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


def test_api_postgres_connection_inspection_contract() -> None:
    postgres = _section("SB-API-13")
    normalized = " ".join(postgres.split())
    for needle in (
        "simplebroker_pg.get_connection_stats(queue) -> dict[str, int]",
        "numbackends",
        "max_connections",
        "superuser_reserved_connections",
        "reserved_connections",
        "pg_catalog.pg_stat_database",
        "autovacuum",
        "ValueError",
        "DatabaseError",
        "safety margin",
    ):
        assert needle in normalized
    assert "monitoring-role grant" in normalized
    assert "does not use sidecar" in normalized

    registry = REGISTRY.read_text(encoding="utf-8")
    row = next(line for line in registry.splitlines() if "Python library" in line)
    assert "[SB-API-13]" in row
    assert "extensions/simplebroker_pg/tests/test_connection_stats.py" in row


def test_api_queue_write_returns_id_not_exit_code(tmp_path: Path) -> None:
    """[SB-API-4] Library write returns a message id."""
    db = tmp_path / "api.db"
    with Queue("q", db_path=str(db)) as q:
        message_id = q.write("hello")
    assert isinstance(message_id, int)
    assert message_id > 0


def test_api_write_keep_newest_signatures_and_public_validator() -> None:
    from simplebroker import ext

    queue_parameter = inspect.signature(Queue.write).parameters["keep_newest"]
    command_parameter = inspect.signature(commands.cmd_write).parameters["keep_newest"]
    for parameter in (queue_parameter, command_parameter):
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is None

    assert ext.validate_keep_newest(1) == 1
    assert ext.validate_keep_newest(9999) == 9999
    with pytest.raises(TypeError):
        ext.validate_keep_newest(True)
    with pytest.raises(ValueError):
        ext.validate_keep_newest(10_000)


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


def test_api_load_future_skew_surface_is_root_importable_and_keyword_only() -> None:
    assert simplebroker.DumpClockSkewWarning is DumpClockSkewWarning
    assert issubclass(DumpClockSkewWarning, UserWarning)
    parameters = inspect.signature(load_lines).parameters
    assert parameters["force"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["force"].default is False
    assert parameters["config"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["config"].default is None
    command_parameters = inspect.signature(commands.cmd_load).parameters
    for name, default in (("force", False), ("quiet", False), ("config", None)):
        assert command_parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert command_parameters[name].default is default


def test_api_commands_exit_code_shape(tmp_path: Path) -> None:
    """[SB-API-10] cmd_* returns CLI-style exit codes."""
    db = str(tmp_path / "cmd.db")
    assert commands.cmd_write(db, "jobs", "one") == 0
    assert commands.cmd_exists(db, "jobs") == 0
    assert commands.cmd_exists(db, "missing-queue-xyz") == 2
