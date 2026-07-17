# Propagate Guidance Delta Wave Plan

Date: 2026-07-17

Status: active — delta wave from agent-guidance @ `b248e1c` (prior pin
`fc23eae`)

Owner: SimpleBroker

Primary downstream: Weft (pins simplebroker; adopted review-practice
text carries no CLI-contract implications, so nothing propagates further
from this wave)

Class: 3+P — no normative spec text lands here (the [DOM-*] operating
model is unchanged by this wave); the payload is runbook and skill guidance
plus process material. Hardening: N/A — docs/guidance only; no risky
trigger (no async work, no runtime contract change, no persistence
lifecycle, no one-way door; every edit is heading-anchored and reversible
by revert).

## 1. Goal

Land the agent-guidance delta between `fc23eae` (this repo's 2026-07-16
bootstrap pin) and `b248e1c` — the review-practice hardening wave from
the hub's `2026-07-17` process work. The wave touches three already-present
files with heading-anchored inserts; no payload-target files are
created or deleted (this plan and its index row are the wave's only new
content).

Source hub commits carried by this wave (three of the seven in the range
apply here):

- `ea5314b` — scoped-change review prompt template (review-loops §4a)
- `3ffb807` — call-agent: the review brief is a required-shape artifact
- `cd74fcd` + `6052289` — owner's two-question PASS/BLOCKED plan-review
  prompt, unified verdict vocabulary (take the `b248e1c` state, not the
  intermediate `cd74fcd`)
- `b248e1c` — planning standard: plans record evidence, never transient
  git state; verdict-vocabulary block
- `fafd874` — mm revision-gate slate + the "Approval attaches to the text
  that was reviewed" Plan Lifecycle bullet

Out of range for this repo: hub `8c504fd` (bootstrap generator), the
`docs/coalescing.md` mm-slate ledger row, and `bin/bootstrap-agent-guidance`
edits — all hub-only mechanics; this repo's coalescing skill, interface-review
skill, and DOM spec are already current and are not touched.

## 2. Payload Checklist (grep-verified at landing)

Source @ `b248e1c`. Three payload files, six transplant units (review
F2 — the original count said four):

1. `docs/agent-context/runbooks/writing-plans.md`
   - (1a) Plan Lifecycle bullet **"Approval attaches to the text that was
     reviewed, not to the file name."** — inserted after the immutable-at-closure
     bullet, before the harvest-candidates bullet. Adapted (see §3 row 1).
   - (1b) Planning Standard bullet **"Plans record completed work and
     evidence, never transient repository state."** — inserted after the
     red-green TDD bullet. Verbatim from `b248e1c`.
2. `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`
   - (2a) §4 Recommended Plan Review Prompt replaced with the owner's
     two-question PASS/BLOCKED version plus the block-must-trace note
     (`b248e1c` state). Verbatim.
   - (2b) New §4a Scoped Change Review Prompt template plus round-2
     variant. Verbatim from `b248e1c`.
   - (2c) §6 verdict-vocabulary block appended after the Review Output
     Standard closing paragraph. Verbatim.
3. `skills/call-agent/SKILL.md`
   - (3) Step 2 additions: the two-gate-question verdict phrasing, the
     brief-standard required-shape paragraph, and the §4a template
     pointer. Verbatim from `b248e1c` (step 2 base was byte-identical to
     the hub; only this file's provenance header carries a local
     adaptation, which is left untouched).

## 3. Survey Findings and Adaptations

Concurrent work shared the tree during this wave (the code-review
findings remediation, landed by its own session at `9369b1c` and
neighbors) — disjoint from this wave's file list; none of its files
touched. Landing instruction: stage by explicit file list against the
§2 checklist, never by tree state (review F1: this section originally
froze a `git status` census — the exact transient-state claim the
wave's own planning-standard bullet forbids; reworded to durable
evidence).

Base-blob check: `writing-plans.md` and
`review-loops-and-agent-bootstrap.md` are byte-identical to hub `fc23eae`,
so their `fc23eae..b248e1c` additions transplant verbatim.
`skills/call-agent/SKILL.md` diverges only in its provenance header
(lines 4–7, a local bootstrap adaptation); its step 2 is byte-identical
to the hub base, so the step-2 insert splices cleanly.

Section numbering (review-loops §3–§4, §4a, §6) matches the local file,
so section-number references in the transplanted text are correct as-is.

| # | Divergence | Adaptation |
|---|------------|-----------|
| 1 | The hub's "Approval attaches…" bullet cites `docs/coalescing.md` fold-up candidates, which in the hub carry the mm revision-gate slate. This repo's `docs/coalescing.md` does not carry that slate and is out of scope for this wave | Replace the local-pointing `see docs/coalescing.md` clause with a by-name citation of mm's foreign plan `2026-07-17-process-hardening-after-lifecycle-rollback.md`, explicitly marked "foreign to this repository", so the evidence resolves to its true home rather than a local ledger that lacks it |
| 2 | `call-agent/SKILL.md` provenance header was locally adapted at bootstrap (points at this repo's `2026-07-16-agent-guidance-bootstrap-plan.md`) | Leave the header untouched; edit only step 2, whose base matched the hub exactly |

## 4. Deviation Log

| Spec ref | Planned | Actual | Rationale |
|----------|---------|--------|-----------|

## 5. Tasks

1. Transplant the four payload units by heading-anchored insert with
   unique-match confirmation. Apply adaptation #1 to unit (1a).
2. Grep-verify every payload-checklist line landed.
3. Gate: `python3 bin/check-dom15-fixtures` exit 0 (docs-only wave; no
   product surface touched).
4. Scoped independent review (different family) — adaptation only; source
   content already reviewed in the hub. Dispositions in §6.
5. Land: explicit file-list staging of the three payload files plus this
   plan and the plans README row; wave commit. Foreign WIP stays out of
   the commit.

## 6. Review Findings and Dispositions

Scoped adaptation review (grok, read-only, 2026-07-17; §4a-form brief;
adaptation only — source content canonical in the hub @ `b248e1c`).
Verdict: **no blocker**. Transplant fidelity, phantom-path hygiene,
internal references, and doctrine checks all passed. Landing note from
review observations: the two runbook payload files landed via the
concurrent session's `bc7de9e` with correct adapted text; this wave's
closing commit carries the remaining unit (call-agent, index row, this
plan).

| ID | Sev | Finding (gist) | Disposition |
|----|-----|----------------|-------------|
| F1 | P2 | Plan §3 froze a `git status` census — the exact transient-state claim this wave's own planning-standard bullet forbids, and it was already stale by review time | **Accepted, fixed:** §3 reworded to durable evidence + explicit-list landing instruction, with the self-referential catch noted |
| F2 | P3 | Unit count said four; the checklist enumerates six | **Accepted, fixed** |
| F3 | nit | "no files are created or deleted" readable as covering the whole wave | **Accepted, one clause added** ("payload-target files") |
|----|-----|----------------|-------------|
