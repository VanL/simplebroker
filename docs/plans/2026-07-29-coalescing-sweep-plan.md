# Coalescing Sweep Plan

**Date:** 2026-07-29
**Status:** completed
**Class:** 3 — the sweep changes multiple normative documentation indexes and
traceability links under the existing [DOM-14] process. It does not change the
process itself or product behavior. Hardening: N/A — the sweep performs only
two-step soft retirement and does not delete files or cross a runtime boundary.
**Owner:** repository agents under the user's authorization for this
maintenance run.

## 1. Goal

Run the tripped coalescing tiers against the current tree: audit all plan
harvest candidates, soft-retire only candidates that pass the four-part harvest
gate, assess the lessons and promotion tiers, update the coalescing state, and
leave the repository with reproducible counts and traceability evidence.

## 2. Source Documents

- `docs/specs/01-development-documentation-operating-model.md` [DOM-8],
  [DOM-9], [DOM-12], [DOM-14], [DOM-15]
- `skills/coalescing/SKILL.md`
- `docs/coalescing.md`
- `docs/lessons.md`
- `docs/plans/README.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`

Source spec baseline: `36e2f3568a8e97943b9fe7b06e35d2b2bc688406`.
This plan applies the existing process and proposes no spec delta.

## 3. Context and Key Files

- `docs/plans/README.md` owns the authoritative Status Index and Retired Plans
  ledger. The derived plans count is 11 at the source baseline.
- The 11 `completed` or `superseded` plan files are the harvest corpus.
- Specs and other documentation may contain live backlinks that must become
  retired citations before a candidate can be soft-retired.
- `docs/coalescing.md` owns thresholds, deferral state, watermarks, and the run
  log. `bin/coalesce-check` verifies cues but is not the count recipe.
- `docs/lessons.md` has 10 dated entries, all younger than the 30-day age
  floor. They must remain verbatim.

Comprehension checks before editing:

1. Does each source SHA resolve to the candidate's final plan text both locally
   and in published history?
2. Has each candidate passed all four harvest gates, including backlink
   conversion, before its index status changes?

## 4. Invariants and Constraints

1. Do not change SimpleBroker runtime, CLI, API, or product contracts.
2. Do not delete plan files. Physical deletion is a separate, independently
   re-verified change.
3. Do not advance the lessons watermark or remove lesson entries because the
   age floor is not met.
4. Do not soft-retire a plan with an open deviation, unabsorbed durable
   rationale, unextracted lesson, or live spec backlink.
5. For the superseded roadmap, confirm its successors explicitly inherit its
   decided scope before retirement.
6. Use `36e2f356` as the source cue only where `git show` proves that commit
   contains the candidate's final state.
7. Preserve unrelated concurrent work and stop if the files in this sweep
   acquire overlapping edits.
8. A non-landing session remains additive-only. Status flips and retirement
   ledger updates require explicit commit authorization and a durable
   checkpoint.

Frozen candidate inventory at `36e2f356`:

1. `2026-07-27-cross-thread-generator-orphan-healing-plan.md`
2. `2026-07-27-information-architecture-improvement-plan.md`
3. `2026-07-27-product-docs-source-ownership-decision.md`
4. `2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`
5. `2026-07-28-delivery-contract-spec-promotion-plan.md`
6. `2026-07-28-broadcast-create-missing-plan.md`
7. `2026-07-28-explicit-broadcast-targets-plan.md`
8. `2026-07-28-propagate-guidance-delta-wave-plan.md`
9. `2026-07-29-code-quality-cleanup-plan.md`
10. `2026-07-29-development-toolchain-refresh-plan.md`
11. `2026-07-29-process-session-core-factory-plan.md`

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## 5. Tasks

1. Audit the coalescing surfaces and candidate corpus.
   - Files: `docs/coalescing.md`, `docs/lessons.md`, `docs/plans/README.md`,
     the 11 candidate plans, and all references found by exact-name search.
   - Verify the structured index, source cue, deviation closure, rationale and
     lesson disposition, successor inheritance, and backlink inventory.
   - Stop if a source cue does not resolve or a candidate's final state is not
     present at the baseline.
2. Obtain independent plan review before retirement edits.
   - Give the reviewer [DOM-14], this plan, the skill, and the baseline counts.
   - Resolve every finding before continuing.
3. Stop for explicit commit authorization and record the intended durable
   checkpoint.
   - The user's sweep request authorizes the audit, not a commit.
   - Without explicit commit authorization, finish additive-only: do not flip
     statuses or append retirement ledger rows.
4. Apply the authorized soft-retirement slice.
   - Files: `docs/plans/README.md`, exact backlink owners, and
     `docs/coalescing.md`.
   - Flip only passing candidates to `retired-pending`; append ledger rows and
     retired citations with verified source cues.
   - Record blocked candidates and real reconsideration conditions.
5. Assess lessons and promotion tiers.
   - Keep all hot lesson entries intact and record checked-deferred state.
   - Promote no workflow unless three independent, same-shape citations exist.
6. Re-derive counts, run traceability gates, and close the plan.
   - Update this plan's index row to `completed` only after review and current
     verification succeed.

## 6. Testing Plan

This is documentation maintenance. Proof uses the real repository tree and git
history. Do not mock path inventories, backlinks, or source-cue resolution.

- Run the authoritative Python derivation recipe from `docs/coalescing.md`.
- Run `bin/coalesce-check`.
- Run `python3 bin/check-dom15-fixtures`.
- Run `bin/check-doc-paths`.
- Run this exact inventory search and inspect every hit outside
  `docs/plans/README.md`, this sweep plan, and candidate plan bodies:

  ```bash
  rg -n "2026-07-27-cross-thread-generator-orphan-healing-plan|2026-07-27-information-architecture-improvement-plan|2026-07-27-product-docs-source-ownership-decision|2026-07-27-product-spec-doctrine-and-cli-vertical-plan|2026-07-28-delivery-contract-spec-promotion-plan|2026-07-28-broadcast-create-missing-plan|2026-07-28-explicit-broadcast-targets-plan|2026-07-28-propagate-guidance-delta-wave-plan|2026-07-29-code-quality-cleanup-plan|2026-07-29-development-toolchain-refresh-plan|2026-07-29-process-session-core-factory-plan" .
  ```

  Success means no surviving spec backlink uses a live candidate path; every
  surviving historical reference is a source-pinned retired citation or is
  explicitly deferred with an owner and reconsideration condition.

## 7. Verification and Gates

Success requires:

- every candidate has a written four-gate disposition;
- every soft-retired row has a matching Retired Plans ledger row;
- source cues resolve with `git show`;
- unindexed plans remain zero;
- the post-sweep harvest count is below threshold, excluding this active plan,
  or every remaining candidate has a specific blocker, owner, and
  reconsideration condition;
- lessons remain unchanged because they are hot;
- independent completed-work review has no unresolved blocker;
- `git diff --check`, DOM-15 fixtures, coalescing checks, and traceability gates
  pass from the current tree.

Rollback before commit is reverting only this sweep's hunks. After an
authorized commit, rollback is a normal revert of that commit; no file deletion
or product-state migration is involved.

## 8. Independent Review Loop

Use a read-only independent agent to review the plan before retirement edits
and the completed diff before closure. The reviewer receives [DOM-14], the
coalescing skill, this plan, the source baseline, touched files, and verification
evidence. Findings are accepted and fixed, or rejected with a recorded reason.

Plan-review disposition, 2026-07-29:

| Finding | Disposition |
|---------|-------------|
| P1: landing authorization was not an executable stop gate | Accepted: Task 3 now stops for explicit commit authorization and defines additive-only fallback. |
| P1: below-threshold completion conflicted with valid deferral | Accepted: the gate now permits specific blocker/owner/reconsideration records. |
| P1: verification and candidate scope were not exact | Accepted: the 11 candidates are frozen above; `bin/check-doc-paths` and the exact-name inventory command are named. |

Round-2 verdict: **PASS**. The reviewer verified only the three accepted
findings and found each resolved.

## Harvest Audit

Read-only audit at published source pin `36e2f356`:

| Candidate | Gate | Durable rationale / lesson disposition | Backlink action |
|-----------|------|-----------------------------------------|-----------------|
| `2026-07-27-cross-thread-generator-orphan-healing-plan.md` | PASS | Absorbed by `docs/implementation/04-cross-thread-finalization-poisoning.md` and [SB-DELIVERY-5/6]; cross-thread lesson already in the 2026-07-27 ledger | No spec backlink; convert the implementation-doc reference to retired form |
| `2026-07-27-information-architecture-improvement-plan.md` | PASS | Successors explicitly supersede the executable roadmap and separate deferred programs into future plans; no implementation lesson applies | Successor plan-body references need no soft-retirement edit |
| `2026-07-27-product-docs-source-ownership-decision.md` | PASS | Ownership doctrine lives in `docs/README.md` and the product-section registry; no separate lesson | Convert registry Related Plans entry and replace live authority pointers in surviving docs |
| `2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md` | PASS | Closed deviations; doctrine lives in `docs/README.md`, registry, CLI spec, and invariant inventory | Convert CLI/registry Related Plans entries and surviving implementation references |
| `2026-07-28-delivery-contract-spec-promotion-plan.md` | PASS | Closed process deviation; contract lives in [SB-DELIVERY-1..7]; relevant delivery lessons already exist | Convert delivery/registry Related Plans entries and surviving implementation references |
| `2026-07-28-broadcast-create-missing-plan.md` | PASS | Empty deviation table; contract is absorbed by README, agent kernel, code, extension docs, and changelog; allocation lesson already extracted | No external exact-name backlink |
| `2026-07-28-explicit-broadcast-targets-plan.md` | PASS | Closed deviation; contract is absorbed by README [BCAST-1..6], agent kernel, and Redis docs; allocation lesson remains hot | No external exact-name backlink |
| `2026-07-28-propagate-guidance-delta-wave-plan.md` | PASS | Closure drift was repaired from `42049aa`, the run log, and current green gates, then checkpointed in `54fa706` | Convert surviving references to retired form and use `54fa706` as the local-only retirement source pin |
| `2026-07-29-code-quality-cleanup-plan.md` | PASS | Local cleanup rationale is non-durable; lasting session/core rationale lives in the ownership implementation doc | Convert the implementation-doc Related Plans entry |
| `2026-07-29-development-toolchain-refresh-plan.md` | PASS | uv policy lives in README, updater, tests, and locks; release inventory facts are version-bound, not durable lessons | Active successor plan reference remains until its own reconciliation |
| `2026-07-29-process-session-core-factory-plan.md` | PASS | Rationale is fully absorbed by `docs/implementation/06-process-session-core-ownership.md`; no separate lesson | Convert the implementation-doc Related Plans entry |

Every candidate has a closed deviation disposition and a plan body at its
declared source pin. Ten use published `36e2f356`; the repaired guidance-wave
plan uses local-only checkpoint `54fa706`.

## Completed-Work Review

Independent review on 2026-07-29 verified all 11 status/ledger pairs, source
pins, backlink conversions, lessons and promotion dispositions, and the
no-deletion boundary. One blocker was accepted:

| Finding | Disposition |
|---------|-------------|
| F1: Deferral State `checked_through` cells used provisional `post sweep` labels rather than DOM-14's required date + SHA | Accepted. Close this plan and commit the fully derived sweep tree first; then make a state-anchor commit that replaces every provisional label with that sweep commit SHA. |

## Completion Evidence

- Authoritative derivation before closure: harvest=0, retired-pending=67,
  unindexed=0. Closing this plan makes harvest=1, still below threshold.
- `bin/check-doc-paths`: PASS.
- `bin/coalesce-check`: PASS; `54fa706`, `7409242`, and `f133ce7` are
  accurately reported as local-only pins.
- `python3 bin/check-dom15-fixtures`: PASS.
- `git diff --check`: PASS.
- Exact-name inventory: every surviving reference is a source-pinned retired
  citation; candidate plan bodies remain in place for two-step retirement.
- Lessons: 10 entries unchanged and deferred under the 30-day age floor.
- Promotion: no coherent uncaptured workflow theme reached three independent
  citations.

## 9. Out of Scope

- Physical deletion of any `retired-pending` plan.
- Folding hot lessons or advancing their watermark.
- Changing thresholds, the coalescing process, or product documentation
  ownership.
- Cleaning unrelated documentation or runtime code.

## 10. Fresh-Eyes Review

Before execution, confirm the plan names the source pin, all four harvest
gates, the no-delete boundary, the hot-lessons deferral, the landing boundary,
the exact verification commands, and both independent review points.
