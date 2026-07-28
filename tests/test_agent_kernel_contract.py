"""Keep agent-facing kernel docs synchronized with enumerable CLI contracts."""

from __future__ import annotations

import re
from pathlib import Path

from simplebroker import __all__ as package_all
from simplebroker._constants import EXIT_ERROR, EXIT_QUEUE_EMPTY, EXIT_SUCCESS

ROOT = Path(__file__).resolve().parents[1]
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"


def test_llms_txt_points_at_agent_kernel() -> None:
    text = LLMS.read_text(encoding="utf-8")
    assert "docs/agent-kernel.md" in text
    # llmstxt.org-style: H1 + blockquote + H2 link sections
    assert text.lstrip().startswith("# ")
    assert re.search(r"^> ", text, re.M)
    assert re.search(r"^- \[.+\]\(docs/agent-kernel\.md\)", text, re.M)


def test_agent_kernel_exit_codes_match_cli_constants() -> None:
    text = KERNEL.read_text(encoding="utf-8")
    section = text.split("## Exit codes and I/O (CLI)", 1)[1].split("## ", 1)[0]
    documented = {int(code) for code in re.findall(r"^\| `(\d+)` \|", section, re.M)}
    assert documented == {EXIT_SUCCESS, EXIT_ERROR, EXIT_QUEUE_EMPTY}


def test_agent_kernel_cites_cli_contract() -> None:
    text = KERNEL.read_text(encoding="utf-8")
    section = text.split("## Exit codes and I/O (CLI)", 1)[1].split("## ", 1)[0]
    assert "docs/specs/10-cli-contract.md" in section or "[SB-CLI-1]" in section


def test_agent_kernel_cites_delivery_contract() -> None:
    text = KERNEL.read_text(encoding="utf-8")
    section = text.split("## Delivery (use-level)", 1)[1].split("## ", 1)[0]
    assert "docs/specs/11-delivery-contract.md" in section
    assert "[SB-DELIVERY-1]" in section
    assert "[SB-DELIVERY-7]" in section


def test_agent_kernel_forbids_delete_while_peek_stream() -> None:
    text = KERNEL.read_text(encoding="utf-8")
    assert "Do not delete while draining a peek stream" in text
    assert "peek_generator" in text
    # Preferred reservation path named
    assert "move" in text.lower() and "inflight" in text


def test_agent_kernel_does_not_claim_identical_cli_python_packaging() -> None:
    text = KERNEL.read_text(encoding="utf-8")
    assert "not CLI exit codes" in text or "not mean identical packaging" in text
    assert "Does **not** print the message id" in text or "does **not** print" in text
    assert "CLI-only" in text or "aliases" in text.lower()


def test_agent_kernel_public_surface_symbols_exist() -> None:
    # Kernel names package-root exports agents should prefer — hard gate
    # against renames (no "mentioned in doc" fallback).
    required = (
        "Queue",
        "QueueWatcher",
        "open_broker",
        "dump_lines",
        "load_lines",
        "create_activity_waiter_for_queues",
    )
    exported = set(package_all)
    for name in required:
        assert name in exported, f"{name!r} missing from simplebroker.__all__"
    text = KERNEL.read_text(encoding="utf-8")
    for name in required:
        assert name in text, f"{name!r} not mentioned in agent-kernel.md"
