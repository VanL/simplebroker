# Agent-Guidance Bootstrap Plan

Date: 2026-07-16

Status: in progress

Owner: SimpleBroker

Primary downstream: Weft (pins simplebroker; any CLI-contract implications
of adopted review practices are cross-repo questions, not local edits)

Class: 5+P — normative spec text lands in this repo (the operating-model
spec `[DOM-*]` arrives whole) and the change is process-material.
Hardening: N/A — docs/guidance only; no risky trigger (no async work, no
runtime contract change, no persistence lifecycle, no one-way door — the
scaffold is create-only and reversible by revert).

## 1. Goal

First install of the agent-guidance operating model (source: agent-guidance
@ `fc23eae`, 2026-07-16) via `bin/bootstrap-agent-guidance` — the
fresh-install path; `propagate-guidance` waves apply from the *next*
delta onward. Create-only: 38 files created, 0 overwritten, 1 skipped
(`docs/README.md`, which stays project-owned).

## 2. Payload Checklist (grep-verified at landing)

Copied @ `fc23eae`: AGENTS.md; bin/check-dom15-fixtures; agent-context
(README, context.index.yaml, decision-hierarchy, engineering-principles,
lessons pointer, principles) + 11 runbooks (adversarial-acceptance-probes,
designing-agent-facing-interfaces, external-skill-suites, hardening-plans,
maintaining-traceability, review-loops-and-agent-bootstrap,
skills-lifecycle, testing-patterns, writing-implementation-docs,
writing-plans, writing-specs); docs/implementation/README;
docs/plans/README; specs (00-index, 01-operating-model, README);
skills (README, _template, brainstorming-to-plan, call-agent, coalescing,
debugging, interface-review). Generated: docs/coalescing.md;
implementation 00/01/02/03; docs/lessons.md. Alias: CLAUDE.md → AGENTS.md
symlink.

## 3. Survey Findings and Adaptations

| # | Divergence | Adaptation |
|---|------------|-----------|
| 1 | `docs/README.md` declares **the root README is the source of truth for current behavior** — load-bearing local doctrine, correct for a published tool | Keep verbatim. Adapt the incoming `docs/specs/README.md` + `00-specs-index.md` with an explicit scope note: the spec tree governs the **development process** ([DOM-*]); **product behavior remains specified by the root README**. Promotion of README content into coded/spec-gated form is future, separately planned work — out of scope here |
| 2 | 51 existing plans (44 dated, 7 undated; corrected at review — the plan originally said 23 from a truncated listing, review F2). ~23 of 51 carry a `Status:` header (most of those also `Date:`/`Owner:`) with varied vocabulary (`proposed`, `draft`, `implemented`, …) | Plans index is **forward-only from 2026-07-16** with the declared boundary ("absence here is never a status claim"). `Status:` headers are the derivable source for that minority; the remainder need file-body/history judgment at backfill. Legacy plans are not rewritten; vocabulary maps at backfill time |
| 3 | gstack suite ACTIVE (`.claude/skills/`, ~57 skill dirs, real dirs) | `external-skill-suites.md` crosswalk verified against the real inventory and marked **active** so rows read as live routing; precedence note in AGENTS.md |
| 4 | Plan-header format differs from house format | This plan uses their header block plus house sections; `writing-plans.md` arrives canonical — new plans may adopt house format; legacy format is never retro-converted |
| 5 | No reference-code scheme in repo | Canonical [DOM-N] arrives with the spec; no collision; README section codes are future work (see #1) |
| 6 | Test harness: pytest via uv, 148 test files; no doc-gates | Gates for this landing: `bin/check-dom15-fixtures` exit 0 + a fast pytest smoke (imports/CLI basics) proving the scaffold broke nothing. Full-suite runs are unaffected (docs-only) |
| 7 | Generated implementation docs (01–03) and engineering-principles are project-neutral boilerplate | Repository-map (02) and agent-inventory (03) refreshed with real entry points (`simplebroker/` package, `extensions/`, `fuzz/`, `bin/`, `tests/`); engineering-principles kept as starter with a header noting adaptation happens as real invariants surface — no speculative content invented |
| 8 | AGENTS.md is generic | Adapted: uv/pytest commands, the README-is-product-truth doctrine, Weft-downstream coupling note, SECURITY.md pointer, release discipline (published package) |

## 4. Deviation Log

| Spec ref | Planned | Actual | Rationale |
|----------|---------|--------|-----------|

## 5. Tasks

1. Run bootstrap (create-only) from source @ `fc23eae`. Verify payload
   checklist by grep.
2. Apply adaptations #1–#8.
3. Gates: `python3 bin/check-dom15-fixtures` exit 0; pytest smoke green.
4. Scoped independent review (different family, grok) — adaptation only,
   source content already reviewed in the hub. Dispositions in §6.
5. Land: explicit file-list staging (tree is clean — verified at survey);
   wave commit, then state-file pin commit.
6. First coalescing sweep in the same unit (standing rule): derive counts
   honestly; lessons ledger is new (0 entries); plans tier reports the
   51-plan backfill as declared debt with its reconsideration condition —
   an honest checked-deferred is the expected outcome.

## 6. Review Findings and Dispositions

Scoped adaptation review (grok, read-only, 2026-07-16; adaptation only —
source content canonical in the hub). Round 1 verdict: **FAIL**, correctly —
two P1s were the orchestrator's own errors. All findings fixed in the same
pass; round 2 below.

| ID | Sev | Finding (gist) | Disposition |
|----|-----|----------------|-------------|
| F1 | P1 | The copied plans README retained the HUB's own 13-row status index below the new forward-only section — phantom plans presented as local | **Fixed:** hub residue deleted; only the local index remains. Root cause: boundary added without reading the copied file's tail. Hub back-port owed: the bootstrap should generate a neutral plans README, not copy the hub's (carries hub state by construction) |
| F2 | P1 | Legacy-plan census wrong: plan said 23 (16 dated + 7 undated); actual 51 (44 dated + 7 undated). Source: an early `head`-truncated listing trusted as a count | **Fixed** in all three sites (plan §3, plans README boundary, coalescing.md); correction noted inline in §3 |
| F3 | P2 | "Most legacy plans carry `Status:` headers" false — ~23 of 51 (~45%); vocabulary wider than `proposed` | **Fixed** in the same three sites: minority stated, vocabulary enumerated, remainder needs file-body judgment |
| F4 | P2 | Scope note claimed on both specs README and 00-index; only README had it | **Fixed:** 00-specs-index now opens with the process-only scope + product-README carve-out |
| F5 | P2 | `implementation/01` still said "`docs/specs/` is the source of truth for intended behavior" — fights the doctrine for readers landing there first | **Fixed:** boundary bullet scoped to the development process with the product-README carve-out |
| F6 | P2 | Agent-inventory note said the shared probe record "applies as-is" while the table below sat scaffold-blank | **Fixed:** table populated from the call-agent probe record with dates; note reworded to point at the exact-invocation source |
| F7 | P3 | Repository-map skills table omitted `skills/interface-review/` | **Fixed:** row added. Hub back-port owed: the generator's skills-table template is missing the newly promoted skill |
| F8 | nit | Soft counts (148 test files → ~141 modules; 59 gstack dirs → ~57) | **Fixed:** softened to "~140+" in AGENTS.md; "~57" in §3 |
| F9–F13 | nit/pass | Provenance form, coalescing derivations, AGENTS facts, root-README untouched, payload arithmetic — all verified clean by the reviewer | No change |
