# Agent-Theory Delta Wave Propagation Plan (2026-10-14)

- **Date:** 2026-10-14 (revised 2026-10-15)
- **Status:** completed — see index row in `docs/plans/README.md`
- **Class:** 5+P (effective Class 5 — normative process spec `[DOM-*]` / `[AT-REF-*]`, shared `docs/agent-context/` guidance, reusable skills, and `bin/` verification gates; zero SimpleBroker product-contract or runtime code changes)
- **Hub Pin:** `agent-theory` @ `296a7cd` (advancing from `8f41a9d` / `0423923`)
- **Reviewed baseline:** `e04c27a`

## Goal

Propagate the 2026-10-14 / 2026-10-15 `agent-theory` delta wave (`0423923..296a7cd`, including the `STE100` / `ba4dd7a` fold-up, `8f41a9d` ecosystem fold-up wave, and `296a7cd` whole-plan review lenses, `skills/test-audit/` promotion, and taste-up-front boundary) into `simplebroker` while preserving all SimpleBroker-specific product ownership, layered SoT rules, Ruff static-analysis/McCabe complexity gates (`[DOM-10.1]`), suppression registry invariants (`[DOM-10.1.1]`), and program-theory contract checks.

## Source Documents

- Theory: N/A — process-governance and shared agent-context propagation wave from `agent-theory` @ `296a7cd`; SimpleBroker product theory in `docs/program-theory.md` (`[THEORY-1]`..`[THEORY-5]`) is unchanged.
- `AGENTS.md` — repository agent entry point, subagent delegation contract, and module-theory reading rule
- `docs/README.md` — product vs. process documentation ownership and entry routing
- `docs/specs/00-specs-index.md` — process and product specification index
- `docs/specs/01-development-documentation-operating-model.md` — `[DOM-2]`, `[DOM-5]`, `[DOM-10]`, `[DOM-11]`, `[DOM-14]`, `[DOM-15]`
- `docs/agent-context/context.index.yaml` — machine-readable shared agent context index
- `docs/agent-context/decision-hierarchy.md` — completion gate and mechanical claim verification
- `docs/agent-context/principles.md` — collaboration standards (report surprise) and change hygiene (negative lifecycle labels)
- `docs/agent-context/engineering-principles.md` — principles §2, §4, §8, §10, §11, §12, §14, and warning signs
- `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md` — 3-tier review hierarchy, standing invariants, causal premise check, `RECONSIDERATION REQUESTED`
- `docs/agent-context/runbooks/writing-plans.md` — planning standard, §4c clause-level hazard audit, §8 review protocol, and plan lifecycle
- `docs/agent-context/runbooks/testing-patterns.md` — test design rules 7–10, pattern 9, and verification pattern
- `docs/agent-context/runbooks/hardening-plans.md` — §1 simplification register, §13 cleanup authority, §15 long-suite stale-remote guard
- `docs/agent-context/runbooks/writing-specs.md` — §9 inline traceability directives and clause-by-clause hazard audit
- `docs/agent-context/runbooks/maintaining-traceability.md` — full CI root verification and retired-plan citation grep floor
- `docs/implementation/01-documentation-system.md` — documentation system map
- `docs/implementation/02-repository-map.md` — canonical file and skill inventory
- `docs/implementation/10-ruff-suppression-registry.md` — governed `noqa` suppression inventory (`[DOM-10.1.1]`)
- `skills/README.md`, `skills/call-agent/SKILL.md`, `skills/coalescing/SKILL.md`, `skills/debugging/SKILL.md`, `skills/test-audit/SKILL.md`, `skills/test-audit/SWEEP.md` — reusable operational skills and index
- `bin/check-doc-paths`, `bin/check-dom15-fixtures`, `bin/coalesce-check`, `tests/test_doc_gates.py` — mechanical governance gates and test suite

## Invariants and Boundaries

1. **Product contracts untouched:** No file under `simplebroker/`, `extensions/`, `examples/`, or `docs/specs/1*.md` is modified. `docs/README.md` retains its layered product SoT table and `docs/specs/product-section-registry.md` rules.
2. **No foreign backticked plan paths:** All references to hub-only plans in `docs/specs/` or elsewhere use quoted plan names with repository attribution (e.g., `"2026-10-14-ecosystem-fold-up-wave-plan"` in `agent-theory`) so `bin/check-doc-paths` passes without false positives.
3. **Static analysis and McCabe complexity (`[DOM-10.1]` / `[DOM-10.1.1]`):**
   - `ruff check .` checks `bin/*` with `max-complexity = 10` (`C901`). Every helper in `bin/check-doc-paths`, `bin/check-dom15-fixtures`, and `bin/coalesce-check` must stay at or below McCabe complexity 10.
   - `docs/implementation/10-ruff-suppression-registry.md` and `bin/ruff_suppression_index.py --check` enforce exact `noqa` cardinalities and locations. The single `# noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception` in `bin/check-dom15-fixtures` must be preserved verbatim, and no new `noqa` directives may be introduced.
4. **Program-theory and plan-context test contracts:**
   - `tests/test_program_theory_contract.py` requires `docs/agent-context/context.index.yaml` `read_order` to stay aligned with `docs/agent-context/README.md`, `writing-plans.md` to retain the `NK-001`..`NK-005` routing table, and any template `### [ALT-...]` / `### [REV-...]` headings in `skills/crystallize-program-theory/SKILL.md` to remain inside fenced blocks.
   - `bin/check-plan-context` block in `docs/agent-context/runbooks/writing-plans.md` §8 must be preserved alongside the upstream §8 additions.

## Scope and Slice Breakdown

### Slice 1 — Entry Point, Process Specs, and New Skill (`AGENTS.md`, `docs/README.md`, `docs/specs/`, `skills/crystallize-program-theory/`)

- **`AGENTS.md`:** Add the delegated RED transcript verification requirement (`including verifying the exact failure assertion in the subagent transcript when RED test writing is delegated`) to `SUBAGENT SCOPE FOR THE AUTHORIZATION ABOVE`, and add the **Module theory** progressive-disclosure note under `## Shared Agent Context` (pointing to `[DOM-2]`, `[AT-REF-4]`, and `skills/crystallize-program-theory/SKILL.md`).
- **`docs/specs/01-development-documentation-operating-model.md`:**
  - `[DOM-2]`: Add `docs/program-theory.md` layer description, `Status` lifecycle (`Stub`, `Draft`, `Active`), `docs/specs/02-agent-theory-and-program-theory.md` pointer, and **Module theory** progressive-disclosure contract.
  - `[DOM-5]`: Clarify routine coalescing sweep Class 2 precondition (`verified pre-fold source_sha reachable from a retained Git ref`).
  - `[DOM-10]`: Add full CI test/lint root verification and delegated RED transcript verification before `[DOM-10.1]`.
  - `[DOM-11]`: Add standing invariants/refused alternatives (`[ALT-*]`), causal premise & falsifiable proof verification, `fixed in plan` body-sync rule, `RECONSIDERATION REQUESTED` escalation, P1 one-line mutation proof, and the explicit 3-tier review hierarchy (Tier 1 cross-family CLI, Tier 2 native subagent fallback after two bounded logged Tier-1 failures, Tier 3 explicit `Review: unavailable`).
  - `[DOM-14]`: Add date-watermarked reclassification cue, bounded `docs/coalescing.md` surface rule, graduated 3-tier fold verification (`text fidelity`, `symbol liveness`, `behavioral parity` + doc-only floor), and `- YYYY-MM-DD: (revised ...)` prefix ordering.
- **`docs/specs/02-agent-theory-and-program-theory.md` (new):** Adopt reference primer `[AT-REF-1]`..`[AT-REF-6]` from `agent-theory` @ `8f41a9d` with local plan citation and quoted hub plan attribution.
- **`skills/crystallize-program-theory/SKILL.md` (new):** Adopt skill from `agent-theory` @ `8f41a9d` with `Status:` header and consumer-scoped pointers to `docs/program-theory.md` (`[PT-*]`) and `docs/specs/02-agent-theory-and-program-theory.md`.
- **Indexes and maps:** Register `02-agent-theory-and-program-theory.md` and `skills/crystallize-program-theory/SKILL.md` in `docs/specs/00-specs-index.md`, `docs/README.md`, `docs/implementation/01-documentation-system.md`, `docs/implementation/02-repository-map.md`, and `skills/README.md`.

### Slice 2 — Shared Agent Context, Runbooks, and Existing Skills (`docs/agent-context/`, `skills/`)

- **`docs/agent-context/context.index.yaml`:** Bump `updated_at: 2026-10-15`.
- **`docs/agent-context/decision-hierarchy.md`:** Expand `## Completion Gate` with the three rules on status documents as claims, stale open markers, and mechanical verification of count/completeness words (`all`, `every`, `full`, `zero`, `none`).
- **`docs/agent-context/principles.md`:** Add **Report surprise; do not absorb it** under `## Collaboration Standards` and full-tree grep evidence for negative lifecycle labels under `## Change Hygiene`.
- **`docs/agent-context/engineering-principles.md`:** Apply hub `8f41a9d` additions across §2 (non-default resolved config), §4 (contention theory stop-condition and module-owned clock/platform seams), §8 (1/2/3 empirical check, `fixed in plan` sync, `RECONSIDERATION REQUESTED`, status-doc rerun), §10 (prior-release migration fixtures and independent wire literals), §11 (dead-code grep across `examples/`/scripts/consumers and `tests/` backlink grep before spec retirement), §12 (mechanical completeness verification), §14 (static complexity as visibility signal, governed suppression registry, zero self-suppression in governance tools with `[RUFF-SUP-003]` registry qualification, real-config `RUF100` probe), and `## Warning Signs`.
- **Runbooks (`docs/agent-context/runbooks/`):**
  - `review-loops-and-agent-bootstrap.md`: §2 3-tier hierarchy, §3 standing invariants and numbered 1/2/3 checks, §4/§4a `RECONSIDERATION REQUESTED` and causal premise check, §5 `fixed in plan` body sync.
  - `writing-plans.md`: Planning Standard bullets, §4c clause-by-clause hazard audit, §8 review protocol updates (preserving SimpleBroker's `bin/check-plan-context` paragraph), and Plan Lifecycle status-column and graduated revision-log rules.
  - `testing-patterns.md`: Rules 7–10, Pattern 9 (preserving SimpleBroker's Patterns 7 and 8), and Verification Pattern steps 1 and 3.
  - `hardening-plans.md`: §1 standing invariants and simplification difference register, §13 cleanup authority and liveness preconditions, §15 long-suite stale-remote guard and failure-path verification.
  - `writing-specs.md`: §9 inline traceability directives (`_Traceability: meta_` / `_Traceability: ignore <reason>_`) and Spec Maintenance clause-by-clause hazard audit.
  - `maintaining-traceability.md`: Completion Gate full CI test/lint roots and Retired-plan citation `tests/` grep floor.
- **Skills (`skills/call-agent/SKILL.md`, `skills/coalescing/SKILL.md`, `skills/debugging/SKILL.md`):** Propagate `8f41a9d` updates and refresh `Status:` headers.

### Slice 3 — Verification Gate Scripts and Tests (`bin/`, `tests/test_doc_gates.py`)

- **`bin/check-doc-paths`:** Add `DECLARED_FUTURE_ARTIFACTS`, `_prose_lines()` fenced-code stripping, `_stale_allowlist_failures()`, `check_tree(..., declared_future=...)`, and `--self-test` (`self_test()`), preserving SimpleBroker's `SCAN_FILES` (`docs/agent-kernel.md`, `llms.txt`) and keeping all functions $\le 10$ McCabe complexity.
- **`bin/check-dom15-fixtures`:** Add `TRIGGER_FIRE_RE`, `CLAUSE_SPLIT_RE`, `states_firing_trigger()`, negated-clause handling in `has_negative_fact()`, and expanded `self_test()` probes while preserving `# noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception` and keeping McCabe complexity $\le 10$.
- **`bin/coalesce-check`:** Add word-bounded `FOREIGN_MARKER_RE`, `resolve_remote_ref()`, and `retired_plan_cues()` (including retired-plan cues in `cues` and summary output) while preserving SimpleBroker's modular helper structure and `r"^- 20[0-9]{2}-[0-9]{2}-[0-9]{2}:"` lesson entry regex.
- **`tests/test_doc_gates.py`:** Add `check-doc-paths --self-test` coverage alongside `check-dom15-fixtures --self-test`.

### Slice 4 — 2026-10-15 Follow-Up Hub Delta (`8f41a9d..296a7cd`)

- **Whole-plan review lenses (`[DOM-11]`, `engineering-principles.md` §8, `review-loops-and-agent-bootstrap.md` §3–§4, `writing-plans.md` §8 & §10, `skills/call-agent/SKILL.md` §2):** Add the two explicit whole-plan review lenses ((1) expert-developer craft/elegance across the assembled plan including all amendments, and (2) known algorithm or computer science principle vs. ad-hoc casework).
- **Promoted `skills/test-audit/` (`SKILL.md` and `SWEEP.md`):** Adopt `skills/test-audit/SKILL.md` and `skills/test-audit/SWEEP.md` from `agent-theory` @ `296a7cd` and register in `skills/README.md`, `docs/implementation/02-repository-map.md`, and `docs/agent-context/runbooks/external-skill-suites.md`.
- **Taste-up-front vs. BDUF (`docs/specs/02-agent-theory-and-program-theory.md` `[AT-REF-2]`–`[AT-REF-4]`, `skills/crystallize-program-theory/SKILL.md` Q2, Q5, §4) and `skills/coalescing/SKILL.md` step 3.4:** Propagate `[REV-AT-005]` taste-anchor guidance and `git cat-file -e` vs. `git merge-base --is-ancestor` clarification.

## Revision Log

| Date | Baseline SHA | Sections changed | Reason | Re-review status |
|------|--------------|------------------|--------|------------------|
| 2026-10-15 | `e04c27a` | Hub Pin (`8f41a9d` → `296a7cd`), Slice 4 | Propagate same-session hub follow-up commit `296a7cd` (whole-plan review lenses, `skills/test-audit/` promotion, taste-up-front boundary) | Verified by all repository gates + diff check against reviewed hub commit `296a7cd` |

## Independent Review Loop

- **Author family:** `claude`
- **Reviewer family:** `codex` (Tier 1 cross-family CLI review, scoped adaptation review per `skills/propagate-guidance/SKILL.md` step 6)
- **Round 1 findings & dispositions (`REVISE` → all 4 fixed in tree):**
  1. **F1 (`skills/README.md:22`, Blocking):** `skills/crystallize-program-theory/SKILL.md` was registered in `docs/implementation/02-repository-map.md` and `docs/README.md` but omitted from `skills/README.md`. **Disposition:** Fixed — registered `skills/crystallize-program-theory/SKILL.md` in `skills/README.md`.
  2. **F2 (`AGENTS.md:73`, Blocking):** Transplanted hub `AGENTS.md` module-theory pointer cited `the "module theory" / progressive-disclosure section of docs/program-theory.md`, which exists in the hub's meta-theory (`[AT-THEORY-2.1]`) but not in SimpleBroker's product `docs/program-theory.md`. **Disposition:** Fixed — retargeted `AGENTS.md` to `docs/specs/01-development-documentation-operating-model.md` (`[DOM-2]`) and `docs/specs/02-agent-theory-and-program-theory.md` (`[AT-REF-4]`). *(Queued for Step 8 back-port to `agent-theory` hub).*
  3. **F3 (`skills/crystallize-program-theory/SKILL.md:14,35,43,49`, Blocking):** Hub-relative references (`See hub docs/program-theory.md [AT-THEORY-2.1]`, `edit docs/program-theory.md in this repository`, `Hub meta-theory: docs/program-theory.md [AT-THEORY-0]–[AT-THEORY-8]`) treated local `docs/program-theory.md` as the hub meta-theory rather than SimpleBroker's active `[PT-*]` product theory. **Disposition:** Fixed — attributed `[AT-THEORY-*]` to the foreign `agent-theory` hub and updated local references to SimpleBroker's `docs/program-theory.md` (`Status: Active`, `[PT-*]`) and `docs/specs/02-agent-theory-and-program-theory.md`. *(Queued for Step 8 back-port to `agent-theory` hub).*
  4. **F4 (`docs/agent-context/engineering-principles.md:275`, Non-blocking):** `Governance tools carry zero self-suppression` could be misread as contradicting `bin/check-dom15-fixtures`'s registered `# noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception`. **Disposition:** Fixed — clarified that a governance tool may never exempt or suppress its own policy check on itself, while registered `[RUFF-SUP-003]` lint exceptions in `docs/implementation/10-ruff-suppression-registry.md` remain governed by the suppression-registry gate.

## Verification Gates

1. `python3 bin/check-dom15-fixtures && python3 bin/check-dom15-fixtures --self-test`
2. `python3 bin/check-doc-paths && python3 bin/check-doc-paths --self-test`
3. `python3 bin/check-plan-context && python3 bin/check-plan-context --self-test`
4. `python3 bin/coalesce-check`
5. `uv run ruff check .`
6. `python3 bin/ruff_suppression_index.py --check`
7. `uv run pytest tests/test_doc_gates.py tests/test_plan_context_gate.py tests/test_program_theory_contract.py tests/test_ruff_policy.py`
8. `git diff --check`
