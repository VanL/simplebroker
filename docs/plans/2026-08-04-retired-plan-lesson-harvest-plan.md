# Retired Plan Lesson Harvest Plan

**Date:** 2026-08-04
**Status:** completed
**Class:** 3 — independent harvest review found 15 reusable corrections in 13
retired-pending plans that are not represented in the lessons ledger. Extracting
them changes a shared memory surface before the routine deletion pass.
**Owner:** repository agents under the user's authorized coalescing run.

## Goal

Extract the 15 missing reusable lessons from source-pinned retired plans into
the dated moment tier of `docs/lessons.md`, then allow the 67-plan physical
deletion gate to pass. This plan does not promote a Golden Rule, principle,
runbook, skill, or cross-repository rule.

## Sources and Baseline

- `docs/specs/01-development-documentation-operating-model.md` [DOM-8],
  [DOM-9], [DOM-14], [DOM-15]
- `skills/coalescing/SKILL.md`
- `docs/lessons.md`
- the 67 `retired-pending` plans and Retired Plans ledger
- independent semantic harvest audit performed 2026-08-04

Source cue: published mainline `197629e2`, which contains byte-identical final
bodies for all 67 candidates and is reachable from `origin/main`.
Source spec: none. This extracts observed implementation/process lessons and
does not change intended product behavior or the coalescing policy.

## Invariants and Boundaries

- Preserve the concrete failure shape and correction; do not generalize past
  the source evidence.
- Deduplicate against every existing Golden Rule and dated ledger entry.
- Keep the 15 themes distinct; shared words such as “state,” “cleanup,” or
  “test” do not make them one lesson.
- Add dated ledger entries only. Do not revise Golden Rules or other durable
  guidance in this plan.
- Every entry cites its source plan context and is recoverable at `197629e2`.
- Preserve unrelated worker-example work and the policy-change diff.

## Frozen Lesson Inventory

Every range is retrieved from `197629e2:docs/plans/<file>`.

1. Activity notifications are wake hints, not authoritative queue state.
   Source: `2026-05-05-pg-watcher-followup-review-remediation-plan.md:327-357`.
2. Failed best-effort cleanup remains tracked and observable.
   Source: `2026-05-11-sqlite-cross-thread-close-hardening-plan.md:856-868`.
3. Destructive pre-parser flag hoisting needs side-effect-free help and a
   complete subcommand/combination guard.
   Source: `2026-07-02-evaluation-fixes-plan.md:54-62`.
4. An ordering token and the row it orders become visible in one commit.
   Source: `2026-07-02-evaluation-fixes-plan.md:322-345`.
5. Unused-code searches include examples; executable examples need a gated
   behavioral proxy.
   Source: `2026-07-02-evaluation-fixes-plan.md:678-708`.
6. Tests sanitize ambient configuration while preserving explicit harness
   channels and per-test overrides.
   Source: `2026-07-02-evaluation-fixes-plan.md:1053-1117`.
7. Deadlock correction fixes lock order and proves the real lock manager; retry
   is not a substitute.
   Source: `2026-07-02-watch-after-and-pg-rename-lock-plan.md:667-681`.
8. Compatibility-handshake peers declare independent literals, and protocol
   version state stays distinct from storage-schema state.
   Source: `2026-07-03-backend-api-version-handshake-plan.md:1013-1028`.
9. Post-fork recovery replaces inherited locks before every possible lock
   acquisition.
   Source: `2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan.md:809-851`.
10. Check `WIFEXITED` before interpreting `WEXITSTATUS`.
    Source: `2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan.md:851-855`.
11. Secret-output boundaries redact structurally, fail closed, and keep secrets
    out of argv and ordinary output. Sources:
    `2026-07-05-independent-review-fixes-plan.md:183-203` and
    `2026-07-12-code-scanning-alert-triage-plan.md:88-95`.
12. Irreversible release tags follow exact-SHA green validation. Source:
    `2026-07-12-release-reproducibility-and-publication-hardening-plan.md:62-64,111-135`.
13. Cleanup authority derives from resource ownership/state, never path-name
    heuristics. Source:
    `2026-07-13-project-assessment-remediation-plan.md:29,42-44`.
14. Resolved runtime configuration flows through every validation boundary;
    default constants are not resolved instance state. Sources:
    `2026-05-05-review-findings-remediation-plan.md:182-192` and
    `2026-07-09-core-reliability-issues-1-5-plan.md:48-51`.
15. Global mutation invariants are checked from authoritative live state inside
    the atomic boundary, never from a cache or client snapshot. Sources:
    `2026-05-05-review-findings-remediation-plan.md:155-171` and
    `2026-07-16-code-review-findings-remediation-plan.md:61,72,546-603`.

Two narrower findings are not promoted. Wrapper backend-context precedence is
an adapter-specific ownership rule already absorbed by the backend/session
implementation owners. Test-wrapper option/value routing is a local parser
defect whose lasting behavior is covered by its wrapper tests; neither has a
second independent recurrence or a cross-surface correction shape.

## Tasks and Verification

1. Draft one dated ledger entry per frozen theme using exact source passages.
2. Grep both directions against existing lessons and current symbols; merge or
   drop only on demonstrated semantic duplication.
3. Obtain independent review for fidelity, overclaiming, and completeness.
4. Run `python3 bin/coalesce-check`, `bin/check-doc-paths`,
   `python3 bin/check-dom15-fixtures`, and `git diff --check`.
5. Close this plan only when all 15 themes have an accepted entry or a recorded
   evidence-based disposition.

Rule 5 evidence: the independent pre-change audit enumerated 15 source themes
with no semantic match in the current ledger. Post-change review must map every
theme one-to-one to a dated entry and still recover the source passage at
`197629e2`.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Review Log

| Date | Finding | Disposition |
|------|---------|-------------|
| 2026-08-04 | P1: the inventory lacked durable source mappings. | Accepted; every theme now names its source plan and exact passage at `197629e2`. |
| 2026-08-04 | P1: resolved-config and authoritative-live-state findings were recurring, not plan-local. | Accepted; promoted as themes 14 and 15 with both recurrences named. |
| 2026-08-04 | P2: remaining plan-local dispositions lacked evidence. | Accepted; each now names its narrow owner and lack of independent recurrence. |
| 2026-08-04 | P2: changed-file boundary and fresh-eyes record were absent. | Accepted; both are explicit below. |

Expected edit set: `docs/lessons.md`, this plan, `docs/plans/README.md`, and
the coalescing run log. Any promotion into a Golden Rule, principle, runbook,
skill, or additional file re-enters review before the expansion.

## Fresh-Eyes Review

Before extraction, verify all 15 source passages resolve at `197629e2`, each
draft says no more than its source evidence, the two local dispositions remain
narrow, and no proposed entry duplicates an existing Golden Rule or ledger
entry. Before closure, verify a one-to-one source-to-entry map and rerun the
repository gates from the current tree.

## Out of Scope

- Golden Rule, principle, runbook, skill, or cross-repository promotion.
- Product behavior or contract changes.
- The mechanics of deleting the 67 plan files after this harvest passes.
- The two explicitly plan-local/product-local findings above.

## Completion Evidence

- Fifteen dated entries map one-to-one to the frozen themes and published
  source passages at `197629e2`.
- Independent fidelity review passed after narrowing native notifications,
  session-start subprocess configuration, and current-set mutation semantics
  to preserve their source exceptions.
- No Golden Rule, runbook, skill, or other durable-guidance promotion occurred;
  the two plan-local dispositions remained independently accepted.
