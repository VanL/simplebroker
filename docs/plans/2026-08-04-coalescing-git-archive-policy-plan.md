# Coalescing Git-Archive Policy Plan

**Date:** 2026-08-04
**Status:** completed
**Class:** 5+P — the owner-directed correction edits normative process-spec
text and materially changes how future coalescing work is classified, planned,
and committed. It changes no product behavior and adds no runtime risk.
**Hardening:** N/A — no risky trigger. Git-backed coalescing is expressly
excluded from the destructive-edge trigger, and no other [DOM-5] risky trigger
fires.
**Owner:** SimpleBroker product owner.

## Goal

Make the coalescing policy reflect its actual safety boundary: removal is
reversible archive maintenance when a verified Git source cue retains every
item. Routine sweeps are Class 2 and need no dated task plan or
coalescing-specific commit authorization. Promotion or material revision of
durable guidance still follows ordinary planning and review rules.

## Sources and Baseline

- Owner instruction in this session, 2026-08-04.
- `[DOM-5]`, `[DOM-14]`, `[DOM-15]` in
  `docs/specs/01-development-documentation-operating-model.md`.
- `skills/coalescing/SKILL.md`.
- `docs/agent-context/runbooks/writing-plans.md`.
- `docs/coalescing.md` and `docs/plans/README.md`.

Spec baseline: `4ee262a3`. Promotion strategy: D, direct normative spec
revision. Promotion baseline: worktree against `4ee262a3`; this plan records
the owner correction after the first draft exposed the old policy conflict.

## Proposed Spec Delta

Promotion strategy D applies these exact insertions/replacements:

```markdown
Git-backed coalescing is not a destructive edge for classification purposes
when every removed item has a verified pre-fold source SHA reachable from a
retained Git ref and the repository's traceability gate passes. An ordinary
authorized sweep does not require a task plan merely because it soft-retires or
physically removes plans, removes already-distilled or expired raw ledger
entries, advances watermarks, or updates the run log. A plan is required when
the sweep promotes or materially changes durable guidance (for example a
golden rule, principle, runbook, skill, or cross-repository rule), or when some
other [DOM-5] trigger independently fires. The routine sweep is Class 2:
explicit authorization supplies intent, Git makes it reversible, and this
paragraph excludes the coalescing removals themselves from [DOM-5]'s triggers.
```

```markdown
- coalescing removals are Git-backed archive maintenance, not permanently
  destructive, when a verified pre-fold source SHA reachable from a retained
  ref contains every removed item. The authorized sweep may delete
  already-distilled, expired, or otherwise nonnormative raw material, advance
  watermarks, and retire plans without a separate task plan or
  coalescing-specific commit authorization; an item that exists only in the
  worktree remains ineligible because it has no archive cue
```

```markdown
- routine coalescing maintenance is plan-exempt. Promotion or material revision
  of durable guidance (golden rules, principles, runbooks, skills, or
  cross-repository rules) follows the ordinary [DOM-5]/[DOM-15] planning and
  review requirements before that promotion is written
```

```markdown
| Authorized coalescing run that only removes already-distilled, expired, or nonnormative source-pinned raw entries, retires or deletes source-pinned plans, advances watermarks, and updates its run log — explicit user intent, reversible through a retained Git ref, and no [DOM-5] trigger fires because this section excludes those archive removals | 2 |
| Coalescing run that promotes a lesson into a golden rule or materially changes a runbook/skill — durable guidance changes | Class 3+P (effective 5) |
```

All existing two-step retirement, independent harvest verification, historical
alternative retrieval, age-floor, and authorized-sweep text remains in force.

## Invariants and Boundaries

- Git must already contain the exact removed material; worktree-only material
  cannot be folded or retired.
- This correction does not authorize mid-task tidy-up or waive user
  authorization for a sweep.
- It does not weaken harvest, backlink, alternative, cue, or traceability
  verification.
- General repository/session commit policy still applies. Coalescing adds no
  second commit-authorization gate.
- No runtime, CLI, API, product contract, threshold, or age-floor change.

## Tasks and Verification

1. Align `[DOM-5]`, `[DOM-14]`, `[DOM-15]`, the coalescing skill, planning
   runbook, state file, and plans-ledger explanation.
2. Obtain independent policy review; reproduce and resolve every finding.
3. Run `python3 bin/check-dom15-fixtures`, `bin/check-doc-paths`,
   `python3 bin/coalesce-check`, and `git diff --check`.
4. Close this plan only after an independent completed-diff review passes.

Rule 5 evidence: before the correction, the old text classified verified
Git-backed retirement as destructive and blocked it on separate landing
authorization. The post-change exact-text review and structural gates prove
those stale requirements are gone while source-cue safeguards remain.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Review Log

| Date | Finding | Disposition |
|------|---------|-------------|
| 2026-08-04 | P1: the policy revision itself is durable guidance and needs a plan under its own new boundary. | Accepted; this Class-5+P plan records the revision. |
| 2026-08-04 | P1: the skill retained a contradictory landing-authorization sentence. | Accepted; remove the stale requirement. |
| 2026-08-04 | P2: the close step could imply a second commit-authorization test. | Accepted in substance; state that general session policy applies and coalescing adds no separate gate. |
| 2026-08-04 | P2: a resolvable loose Git object can be pruned. | Accepted; require every archive SHA to be reachable from a retained ref. |
| 2026-08-04 | P2: “fold raw entries” blurred routine removal with durable-guidance promotion. | Accepted; Class 2 is narrowed to already-distilled, expired, or nonnormative removals. |
| 2026-08-04 | P2: in-sweep repair could be read as authority for a material gate/runbook change. | Accepted; add an explicit stop-and-classify boundary. |
| 2026-08-04 | P1: normative spec edits make the plan Class 5+P, not 3+P. | Accepted; classification corrected. |
| 2026-08-04 | P1: the proposed delta summarized rather than freezing exact Markdown. | Accepted; exact strategy-D text is recorded above. |
| 2026-08-04 | P2: strategy D lacked the reciprocal spec backlink. | Accepted; add this plan to `[DOM]` Related Plans. |

## Fresh-Eyes Review

Before closure, verify the exact delta matches the promoted spec text; routine
Class-2 scope cannot include new durable guidance; every removed item requires
a retained-ref source cue; and general session commit policy is not replaced by
a coalescing-specific gate.

## Out of Scope

- The 67-plan physical-deletion execution, which is the routine Class-2 sweep
  governed by this policy.
- Lesson, golden-rule, runbook, skill, or cross-repository promotion.
- Unrelated worker-example changes already present in the worktree.

## Completion Evidence

- Exact strategy-D spec delta, skill, planning runbook, state file, and ledger
  wording are aligned around retained-ref Git archive safety.
- Independent policy review passed after three correction rounds: class and
  exact-delta shape, reciprocal spec backlink, retained-ref reachability,
  narrow Class-2 scope, escalation boundary, and commit-policy wording all
  passed.
- `python3 bin/check-dom15-fixtures` and its self-test, `bin/check-doc-paths`,
  `python3 bin/coalesce-check`, and `git diff --check` passed from the changed
  tree before closure.
