"""Structural contract for program theory and durable negative knowledge."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
THEORY = ROOT / "docs" / "program-theory.md"
CONTEXT_INDEX = ROOT / "docs" / "agent-context" / "context.index.yaml"
PRODUCT_REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
PLAN_INDEX = ROOT / "docs" / "plans" / "README.md"
WRITING_PLANS = ROOT / "docs" / "agent-context" / "runbooks" / "writing-plans.md"

INITIAL_README_SHA = "f1bd821640d2f51006eec321b21d5341b0175cdc"
THEORY_SECTIONS = tuple(f"[THEORY-{number}]" for number in range(9))
ALT_FIELDS = (
    "Disposition",
    "Owner",
    "Governs",
    "Source record",
    "Candidate",
    "Why plausible",
    "Evidence",
    "Reason",
    "Current consequence",
    "Reconsider when",
    "Promoted to",
)
REV_FIELDS = ("Current account", "Supersedes", "Pressure", "Evidence")
PROVENANCE = {"contemporaneous", "owner-recalled", "inferred", "unknown"}
DISPOSITIONS = {"adopted", "rejected", "deferred", "superseded", "invalidated"}
QUOTE_ALLOWLIST = {
    (
        "simple enough to understand in an afternoon, yet powerful enough for real work",
        "# SimpleBroker, introductory paragraph",
        "12",
    ),
    ("do one thing well", "## Design Philosophy", "166"),
    (
        "It's not trying to replace RabbitMQ or Redis",
        "## Design Philosophy",
        "166",
    ),
    (
        "the entire codebase should stay under 1000 lines",
        "## Contributing, item 1",
        "239",
    ),
}
EXPECTED_READ_ORDER = [
    "docs/program-theory.md",
    "docs/agent-context/README.md",
    "docs/agent-context/decision-hierarchy.md",
    "docs/agent-context/principles.md",
    "docs/agent-context/engineering-principles.md",
    "docs/agent-context/runbooks/",
    "docs/agent-context/lessons.md",
    "docs/lessons.md",
]
CONCEPT_OWNERS = {
    "Broker target": (
        "Python library / embedding API surfaces",
        "canonical-spec",
        "specs/16-python-library-api.md",
        "16-python-library-api.md",
    ),
    "Queue": (
        "Queue and broker residual operations",
        "canonical-spec",
        "specs/17-ops.md",
        "17-ops.md",
    ),
    "Message identity": (
        "Message identity, allocation, exact-ID handling, and preservation",
        "canonical-spec",
        "specs/13-message-identity.md",
        "13-message-identity.md",
    ),
    "Claim": (
        "Delivery guarantees, claim/peek/watch safety",
        "canonical-spec",
        "specs/11-delivery.md",
        "11-delivery.md",
    ),
    "Move": (
        "Delivery guarantees, claim/peek/watch safety",
        "canonical-spec",
        "specs/11-delivery.md",
        "11-delivery.md",
    ),
    "Watcher/waiter": (
        "Delivery guarantees, claim/peek/watch safety",
        "canonical-spec",
        "specs/11-delivery.md",
        "11-delivery.md",
    ),
    "Process session": (
        "Python library / embedding API surfaces",
        "canonical-spec",
        "specs/16-python-library-api.md",
        "16-python-library-api.md",
    ),
    "Broker core": (
        "Queue and broker residual operations",
        "canonical-spec",
        "specs/17-ops.md",
        "17-ops.md",
    ),
    "Backend adapter/runner": (
        "Python library / embedding API surfaces",
        "canonical-spec",
        "specs/16-python-library-api.md",
        "16-python-library-api.md",
    ),
}
SPECIALIZED_CONTRACTS = {
    "Queue": (
        "Broadcast selection, creation, and atomicity",
        "canonical-spec",
        "specs/12-broadcast.md",
        "[SB-BCAST-*]",
    ),
    "Broker core": (
        "Broadcast selection, creation, and atomicity",
        "canonical-spec",
        "specs/12-broadcast.md",
        "[SB-BCAST-*]",
    ),
    "Message identity": (
        "Message identity, allocation, exact-ID handling, and preservation",
        "canonical-spec",
        "specs/13-message-identity.md",
        "[SB-ID-*]",
    ),
    "Move": (
        "Message identity, allocation, exact-ID handling, and preservation",
        "canonical-spec",
        "specs/13-message-identity.md",
        "[SB-ID-5]",
    ),
}
ORDERED_SELECTION_ROUTES = {
    "Message identity": "specs/14-timestamp-selection.md",
    "Move": "specs/14-timestamp-selection.md",
}
RECORD_HEADING = re.compile(
    r"^### \[(?P<kind>ALT|REV)-(?P<scope>[A-Z][A-Z0-9]*)-"
    r"(?P<number>\d{3})\] (?P<title>\S.*)$"
)
FIELD_LINE = re.compile(r"^(?P<name>[A-Z][A-Za-z ]+):")
LIVE_SOURCE = re.compile(
    r"^\[(?P<record>ALT-[A-Z][A-Z0-9]*-\d{3})\] in "
    r"(?P<path>docs/plans/[^ ]+\.md)$"
)
RETIRED_SOURCE = re.compile(
    r"^(?P<plan>[^ /]+\.md) at (?P<sha>[0-9a-f]{7,40}) "
    r"\[(?P<record>ALT-[A-Z][A-Z0-9]*-\d{3})\]$"
)


@dataclass(frozen=True)
class Record:
    record_id: str
    kind: str
    path: Path
    body: str
    fields: dict[str, str]


def _without_fenced_blocks(text: str) -> str:
    kept: list[str] = []
    fence: str | None = None
    for line in text.splitlines():
        marker = re.match(r"^\s*(```+|~~~+)", line)
        if marker:
            token = marker.group(1)[0]
            fence = None if fence == token else token
            continue
        if fence is None:
            kept.append(line)
    return "\n".join(kept)


def _parse_fields(body: str, expected: tuple[str, ...]) -> dict[str, str]:
    found: list[tuple[str, int]] = []
    lines = body.splitlines()
    for index, line in enumerate(lines):
        match = FIELD_LINE.match(line)
        if not match:
            continue
        name = match.group("name")
        if name not in expected:
            raise ValueError(f"unknown field {name!r}")
        found.append((name, index))
    names = tuple(name for name, _ in found)
    if names != expected:
        raise ValueError(f"field order {names!r}, expected {expected!r}")

    fields: dict[str, str] = {}
    for position, (name, line_index) in enumerate(found):
        end = found[position + 1][1] if position + 1 < len(found) else len(lines)
        first = lines[line_index].split(":", 1)[1].strip()
        continuation = [line.strip() for line in lines[line_index + 1 : end]]
        fields[name] = "\n".join([first, *continuation]).strip()
        if not fields[name]:
            raise ValueError(f"empty field {name!r}")
    return fields


def _parse_records(path: Path, text: str) -> list[Record]:
    lines = _without_fenced_blocks(text).splitlines()
    starts: list[tuple[int, re.Match[str]]] = []
    for index, line in enumerate(lines):
        match = RECORD_HEADING.match(line)
        if line.startswith(("### [ALT-", "### [REV-")) and not match:
            raise ValueError(f"malformed record heading: {line}")
        if match:
            starts.append((index, match))

    records: list[Record] = []
    for start, match in starts:
        end = next(
            (
                index
                for index in range(start + 1, len(lines))
                if re.match(r"^#{1,3} ", lines[index])
            ),
            len(lines),
        )
        body = "\n".join(lines[start + 1 : end]).strip()
        kind = match.group("kind")
        record_id = f"{kind}-{match.group('scope')}-{match.group('number')}"
        fields = _parse_fields(body, ALT_FIELDS if kind == "ALT" else REV_FIELDS)
        records.append(Record(record_id, kind, path, body, fields))
    return records


def _corpus_records() -> dict[str, Record]:
    paths = [ROOT / "README.md"]
    paths.extend(sorted((ROOT / "docs").rglob("*.md")))
    paths.extend(sorted((ROOT / "skills").rglob("*.md")))
    records: dict[str, Record] = {}
    for path in paths:
        for record in _parse_records(path, path.read_text(encoding="utf-8")):
            assert record.record_id not in records, (
                f"duplicate definition {record.record_id}: "
                f"{records[record.record_id].path} and {record.path}"
            )
            records[record.record_id] = record
    return records


def _validate_record_vocabulary(record: Record) -> None:
    evidence_lines: list[str] = []
    for line in record.fields["Evidence"].splitlines():
        if not line.startswith("- "):
            assert evidence_lines, (
                f"{record.record_id} has evidence text before its first "
                f"provenance row: {line}"
            )
            continue
        match = re.fullmatch(
            r"- (contemporaneous|owner-recalled|inferred|unknown): (.+)",
            line,
        )
        assert match, f"{record.record_id} has malformed evidence row: {line}"
        evidence_lines.append(match.group(1))
    assert evidence_lines, f"{record.record_id} has no evidence rows"
    assert set(evidence_lines) <= PROVENANCE
    if record.kind == "ALT":
        assert record.fields["Disposition"] in DISPOSITIONS
        assert record.fields["Reconsider when"]


def _retired_plan_sources(text: str) -> dict[str, str]:
    sources: dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith("| ") or "`" not in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) == 5 and re.fullmatch(r"`[0-9a-f]{7,40}`", cells[4]):
            sources[cells[0]] = cells[4].strip("`")
    return sources


def _validate_source_record(
    source: str,
    retired: dict[str, str],
    *,
    live_ids: set[str] | None = None,
) -> str:
    if source == "none":
        return "none"
    live = LIVE_SOURCE.fullmatch(source)
    if live:
        if live_ids is not None and live.group("record") not in live_ids:
            raise ValueError("live source record is not defined in the corpus")
        return "live"
    match = RETIRED_SOURCE.fullmatch(source)
    if match:
        if live_ids is not None and match.group("record") in live_ids:
            raise ValueError("retired source cites a live record")
        if retired.get(match.group("plan")) != match.group("sha"):
            raise ValueError("retired source does not match Retired Plans ledger")
        return "retired"
    raise ValueError("malformed Source record")


def _section(text: str, code: str) -> str:
    match = re.search(
        rf"^## .+ {re.escape(code)}\n(?P<body>.*?)(?=^## |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"missing {code}"
    return match.group("body")


def _context_read_order(text: str) -> list[str]:
    section = text.split("read_order:\n", 1)[1].split("\ndocuments:", 1)[0]
    return [
        line.removeprefix("  - ").split("  #", 1)[0]
        for line in section.splitlines()
        if line.startswith("  - ")
    ]


def _hub_read_order(text: str) -> list[str]:
    section = text.split("## Read Order\n", 1)[1].split("\n## ", 1)[0]
    paths = re.findall(r"^\d+\. `([^`]+)`", section, re.MULTILINE)
    resolved: list[str] = []
    for path in paths:
        if path.startswith("../"):
            resolved.append(f"docs/{path.removeprefix('../')}")
        elif path == "README.md":
            resolved.append("docs/agent-context/README.md")
        else:
            resolved.append(f"docs/agent-context/{path}")
    return resolved


def _github_anchor(heading: str) -> str:
    slug = re.sub(r"[`*_~]", "", heading.strip().lower())
    slug = re.sub(r"[^\w\- ]", "", slug)
    return re.sub(r"-+", "-", slug.replace(" ", "-")).strip("-")


def _anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    text = _without_fenced_blocks(path.read_text(encoding="utf-8"))
    for heading in re.findall(r"^#{1,6} +(.+?)\s*$", text, re.MULTILINE):
        base = _github_anchor(heading)
        count = counts.get(base, 0)
        anchors.add(base if count == 0 else f"{base}-{count}")
        counts[base] = count + 1
    return anchors


def _assert_local_links_resolve(path: Path, text: str) -> None:
    for raw_target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
        if "://" in raw_target:
            continue
        target, separator, anchor = raw_target.partition("#")
        resolved = path if not target else (path.parent / target).resolve()
        assert resolved.exists(), f"{path}: link target does not resolve: {raw_target}"
        if separator:
            assert anchor in _anchors(resolved), (
                f"{path}: link anchor does not resolve: {raw_target}"
            )


def _registry_rows(text: str) -> dict[str, tuple[str, str, str]]:
    rows: dict[str, tuple[str, str, str]] = {}
    for line in text.splitlines():
        if not line.startswith("| ") or "`" not in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) == 5 and cells[1] in {
            "`readme-only`",
            "`draft-spec`",
            "`canonical-spec`",
        }:
            rows[cells[0]] = (cells[1].strip("`"), cells[2], cells[3])
    return rows


def _concept_rows(text: str) -> dict[str, str]:
    rows: dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith("| ") or "Registry `" not in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) == 4:
            rows[cells[0]] = cells[3]
    return rows


def _assert_stable_references_resolve(text: str) -> None:
    definitions: set[str] = set()
    paths = [ROOT / "README.md"]
    paths.extend(sorted((ROOT / "docs").rglob("*.md")))
    paths.extend(sorted((ROOT / "skills").rglob("*.md")))
    for path in paths:
        corpus = _without_fenced_blocks(path.read_text(encoding="utf-8"))
        for heading in re.findall(r"^#{1,6} +(.+)$", corpus, re.MULTILINE):
            definitions.update(
                re.findall(
                    r"\[((?:DOM|SB|THEORY|ALT|REV)-[A-Z0-9.*-]+)\]",
                    heading,
                )
            )

    references = set(
        re.findall(
            r"\[((?:DOM|SB|THEORY|ALT|REV)-[A-Z0-9.*-]+)\]",
            _without_fenced_blocks(text),
        )
    )
    for reference in references:
        if reference.endswith("*"):
            prefix = reference[:-1]
            assert any(item.startswith(prefix) for item in definitions), (
                f"stable reference family has no definition: {reference}"
            )
        else:
            assert reference in definitions, (
                f"stable reference has no definition: {reference}"
            )


def _negative_knowledge_routes(text: str) -> dict[str, str]:
    routes: dict[str, str] = {}
    for fixture, owner in re.findall(
        r"^\| `(NK-[A-Z-]+)` \| [^|]+ \| `([^`]+)` \|$",
        text,
        re.MULTILINE,
    ):
        if fixture in routes:
            raise ValueError(f"multiple owners for {fixture}")
        routes[fixture] = owner
    return routes


def test_record_parser_rejects_wrong_field_order() -> None:
    malformed = """\
### [REV-TEST-001] Wrong order

Supersedes: old
Current account: current
Pressure: evidence
Evidence:
- contemporaneous: source
"""
    with pytest.raises(ValueError, match="field order"):
        _parse_records(Path("fixture.md"), malformed)


@pytest.mark.parametrize(
    ("malformed", "message"),
    [
        ("### [ALT-theory-1] Bad ID", "malformed record heading"),
        (
            """\
### [REV-TEST-001] Unknown field

Current account: current
Supersedes: old
Pressure: pressure
Unexpected field: value
Evidence:
- contemporaneous: source
""",
            "unknown field",
        ),
        (
            """\
### [REV-TEST-001] Empty field

Current account:
Supersedes: old
Pressure: pressure
Evidence:
- contemporaneous: source
""",
            "empty field",
        ),
    ],
)
def test_record_parser_rejects_malformed_records(
    malformed: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _parse_records(Path("fixture.md"), malformed)


def test_record_vocabulary_rejects_empty_evidence_source() -> None:
    malformed = """\
### [REV-TEST-001] Empty evidence

Current account: current
Supersedes: old
Pressure: pressure
Evidence:
- contemporaneous:
"""
    record = _parse_records(Path("fixture.md"), malformed)[0]
    with pytest.raises(AssertionError, match="malformed evidence"):
        _validate_record_vocabulary(record)


def test_record_vocabulary_rejects_text_before_evidence_rows() -> None:
    malformed = """\
### [REV-TEST-001] Leading evidence text

Current account: current
Supersedes: old
Pressure: pressure
Evidence:
not a provenance row
- contemporaneous: source
"""
    record = _parse_records(Path("fixture.md"), malformed)[0]
    with pytest.raises(AssertionError, match="before its first provenance"):
        _validate_record_vocabulary(record)


def test_record_parser_ignores_fenced_examples() -> None:
    example = """\
```markdown
### [ALT-EXAMPLE-001] Not a definition

Disposition: rejected
```
"""
    assert _parse_records(Path("fixture.md"), example) == []


def test_retired_source_form_matches_the_ledger_exactly() -> None:
    retired = _retired_plan_sources(PLAN_INDEX.read_text(encoding="utf-8"))
    plan = "2026-04-02-env-var-backend-selection.md"
    sha = retired[plan]
    source = f"{plan} at {sha} [ALT-TEST-001]"
    assert _validate_source_record(source, retired) == "retired"

    with pytest.raises(ValueError, match="ledger"):
        _validate_source_record(source.replace(plan, f"x-{plan}"), retired)
    with pytest.raises(ValueError, match="ledger"):
        _validate_source_record(source.replace(sha, "0000000"), retired)
    with pytest.raises(ValueError, match="malformed"):
        _validate_source_record(
            f"[ALT-TEST-001] in {plan}",
            retired,
        )
    with pytest.raises(ValueError, match="malformed"):
        _validate_source_record(
            f"docs/plans/{plan} at {sha} [ALT-TEST-001]",
            retired,
        )
    with pytest.raises(ValueError, match="retired source cites a live record"):
        _validate_source_record(
            source,
            retired,
            live_ids={"ALT-TEST-001"},
        )


def test_negative_knowledge_examples_route_to_one_owner() -> None:
    expected = {
        "NK-DURABLE-NONGOAL": "docs/program-theory.md",
        "NK-CURRENT-LIMITATION": ("product-section registry winning README/spec owner"),
        "NK-ARCHITECTURE-REJECTION": "governing implementation document",
        "NK-PLAN-SCOPE": "closed plan and git history only",
    }
    routes = _negative_knowledge_routes(WRITING_PLANS.read_text(encoding="utf-8"))
    assert routes == expected

    duplicate = (
        "| `NK-DURABLE-NONGOAL` | first | `docs/program-theory.md` |\n"
        "| `NK-DURABLE-NONGOAL` | second | `another owner` |"
    )
    with pytest.raises(ValueError, match="multiple owners"):
        _negative_knowledge_routes(duplicate)
    assert _negative_knowledge_routes("| no fixture |") == {}


@pytest.fixture(scope="module")
def theory_text() -> str:
    assert THEORY.exists(), (
        "docs/program-theory.md must be created after DOM-16 promotion"
    )
    return THEORY.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def corpus_records() -> dict[str, Record]:
    return _corpus_records()


def test_program_theory_metadata(theory_text: str) -> None:
    theory = theory_text
    assert theory.startswith("# SimpleBroker Program Theory\n")
    for metadata in (
        "Status: Active",
        "Owner: SimpleBroker product owner",
        "Boundary:",
        "Verification:",
        "Required action:",
        "Governing process: [DOM-16]",
    ):
        assert metadata in theory
    for code in THEORY_SECTIONS:
        _section(theory, code)


def test_repository_and_product_entry_orders() -> None:
    index = CONTEXT_INDEX.read_text(encoding="utf-8")
    assert _context_read_order(index) == EXPECTED_READ_ORDER
    assert re.search(
        r"  - path: docs/program-theory\.md\n    role: program_theory(?:\n|$)",
        index,
    )

    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    assert agents.index("docs/agent-context/context.index.yaml") < agents.index(
        "winning product contract"
    )
    shared_agent_context = agents.split("## Shared Agent Context", 1)[1].split(
        "## Project Conventions", 1
    )[0]
    assert "docs/program-theory.md" in shared_agent_context
    assert "load-bearing for product-scope *judgment*" in shared_agent_context
    assert shared_agent_context.index(
        "docs/agent-context/context.index.yaml"
    ) < shared_agent_context.index("docs/program-theory.md")
    assert "docs/program-theory.md" in (ROOT / "docs" / "README.md").read_text(
        encoding="utf-8"
    )
    assert "docs/program-theory.md" in (ROOT / "llms.txt").read_text(encoding="utf-8")
    hub = (ROOT / "docs" / "agent-context" / "README.md").read_text(encoding="utf-8")
    assert _hub_read_order(hub) == EXPECTED_READ_ORDER
    assert agents.index("docs/agent-kernel.md") < agents.index(
        "docs/specs/product-section-registry.md"
    )
    startup = "\n".join(
        (
            agents,
            index,
            hub,
            (ROOT / "docs" / "agent-kernel.md").read_text(encoding="utf-8"),
            (ROOT / "llms.txt").read_text(encoding="utf-8"),
        )
    )
    assert INITIAL_README_SHA not in startup
    assert "initial README" not in startup


def test_record_corpus_uses_exact_grammar(
    corpus_records: dict[str, Record],
) -> None:
    for record in corpus_records.values():
        _validate_record_vocabulary(record)


def test_promoted_sources_are_reciprocal(
    corpus_records: dict[str, Record],
) -> None:
    records = corpus_records
    retired = _retired_plan_sources(PLAN_INDEX.read_text(encoding="utf-8"))
    live_ids = set(records)
    for record in records.values():
        if record.kind != "ALT":
            continue
        source_form = _validate_source_record(
            record.fields["Source record"],
            retired,
            live_ids=live_ids,
        )
        promoted = record.fields["Promoted to"]
        if promoted != "none":
            target_id = promoted.strip("[]")
            assert target_id in records
            target = records[target_id]
            assert target.fields["Source record"].startswith(
                f"[{record.record_id}] in "
            )
        if source_form == "live":
            source_match = LIVE_SOURCE.fullmatch(record.fields["Source record"])
            assert source_match is not None
            source_id = source_match.group("record")
            assert source_id in records
            source = records[source_id]
            assert source.path.relative_to(ROOT).as_posix() == source_match.group(
                "path"
            )
            assert source.fields["Promoted to"] == f"[{record.record_id}]"


def test_core_concepts_resolve_to_registry_owners(theory_text: str) -> None:
    registry = PRODUCT_REGISTRY.read_text(encoding="utf-8")
    theory_three = _section(theory_text, "[THEORY-3]")
    registry_rows = _registry_rows(registry)
    concept_rows = _concept_rows(theory_three)
    assert set(concept_rows) == set(CONCEPT_OWNERS)
    for concept, (concern, state, target, registry_owner) in CONCEPT_OWNERS.items():
        owner = concept_rows[concept]
        assert f"Registry `{concern}`" in owner
        assert f"]({target})" in owner
        registry_state, registry_spec, registry_locus = registry_rows[concern]
        assert registry_state == state
        if state == "canonical-spec":
            assert target.removeprefix("specs/") in registry_spec
        else:
            assert registry_locus == registry_owner


def test_core_concepts_route_specialized_contracts(theory_text: str) -> None:
    registry = PRODUCT_REGISTRY.read_text(encoding="utf-8")
    theory_three = _section(theory_text, "[THEORY-3]")
    registry_rows = _registry_rows(registry)
    concept_rows = _concept_rows(theory_three)
    for concept, (concern, state, target, code_family) in SPECIALIZED_CONTRACTS.items():
        owner = concept_rows[concept]
        assert f"Registry `{concern}`" in owner
        assert f"]({target})" in owner
        assert code_family in owner
        registry_state, registry_spec, _ = registry_rows[concern]
        assert registry_state == state
        assert target.removeprefix("specs/") in registry_spec


def test_identity_concepts_route_ordered_selection_contract(
    theory_text: str,
) -> None:
    registry = PRODUCT_REGISTRY.read_text(encoding="utf-8")
    theory_three = _section(theory_text, "[THEORY-3]")
    registry_rows = _registry_rows(registry)
    concept_rows = _concept_rows(theory_three)
    concern = "Ordered timestamp selection and filter consequences"
    state, spec, _locus = registry_rows[concern]
    assert state == "canonical-spec"
    assert "14-timestamp-selection.md" in spec

    for concept, target in ORDERED_SELECTION_ROUTES.items():
        owner = concept_rows[concept]
        assert f"Registry `{concern}`" in owner
        assert f"]({target})" in owner
        assert "[SB-SELECT-*]" in owner


def test_theory_links_and_stable_references_resolve(theory_text: str) -> None:
    _assert_local_links_resolve(THEORY, theory_text)
    _assert_stable_references_resolve(theory_text)


def test_revisions_put_current_account_first(
    corpus_records: dict[str, Record],
) -> None:
    revisions = [record for record in corpus_records.values() if record.kind == "REV"]
    assert revisions
    for revision in revisions:
        assert revision.body.index("Current account:") < revision.body.index(
            "Supersedes:"
        )


def test_lineage_is_bounded_and_current_first(theory_text: str) -> None:
    theory_seven = _section(theory_text, "[THEORY-7]")
    quote_rows = [
        (quote, locus, line)
        for quote, locus, line in re.findall(
            r"^\| [^|]+ \| “([^”]+)” \| `([^`]+)` \| (\d+) \| [^|]+ \| [^|]+ \|$",
            theory_seven,
            re.MULTILINE,
        )
    ]
    assert len(quote_rows) == len(QUOTE_ALLOWLIST)
    assert set(quote_rows) == QUOTE_ALLOWLIST
    rendered_quotes = re.findall(r"“([^”]+)”", theory_seven)
    assert rendered_quotes == [quote for quote, _, _ in quote_rows]
    for line in theory_seven.splitlines():
        if "“" not in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        assert len(cells) == 6
        assert cells[0] and cells[4] and cells[5]
    quotes = [quote for quote, _, _ in quote_rows]
    assert len(quotes) <= 4
    assert all(len(quote.split()) <= 20 for quote in quotes)
    assert sum(len(quote.split()) for quote in quotes) <= 60
    assert INITIAL_README_SHA in theory_seven

    outside_lineage = theory_text.replace(theory_seven, "")
    assert INITIAL_README_SHA not in outside_lineage
    assert "[ALT-PT20260729-004]" not in theory_text
    assert "origin revision" not in theory_text.lower()
