# Agent-Theory Delta Wave Plan (2026-08-07)

Date: 2026-08-07
Status: completed — landed `6595df5` (wave) and `230d59d` (sweep: nine
retired plans deleted under the adopted rule; 13 harvest candidates
checked-deferred); all review findings applied
Class: 5+P — normative spec text lands ([DOM-14], [DOM-15], [DOM-16]
drafting note), and runbooks, skills, and gate scripts are materially
changed. Effective requirements: class 5 plus pre-landing
different-family review, scoped per the hub's propagate-guidance
skill step 6 (source content is already hub-reviewed; review covers
adaptation only).
Theory: N/A — process-guidance wave; no SimpleBroker product concept,
principle, or non-goal changes.

## Goal

Land the agent-theory delta wave: hub source pin `0423923`
(2026-08-07), consumer's last pin agent-guidance @ `e42762c`
(2026-07-28, landed `42049aa`). Most hub changes in this range
originated in this repository (the 2026-08-06 testbed-feedback wave
and the 2026-08-07 backport wave) and are already native; the payload
below is the hub-originated remainder plus three reverse-debt fixes
the hub recorded against this repo, plus two owner decisions of
2026-08-07 that postdate the backport.

## Source Documents

- `docs/specs/01-development-documentation-operating-model.md`
  [DOM-14], [DOM-15], [DOM-16] (sections this wave amends — [DOM-16]
  gains the theory-citation tests, payload item 14)
- `docs/agent-context/decision-hierarchy.md` (gains the Trusted Base
  section)
- Hub source, pinned: agent-theory @ `0423923` — extract with
  `git -C ../agent-theory show 0423923:<path>`. Hub records referenced
  by name (foreign): the hub plan
  "2026-08-07-simplebroker-backport-wave-plan" and hub theory record
  [REV-AT-003] (its program-theory file; cited by name, not path).

## Payload checklist (one line each; grep-verified at completion)

1. Trusted Base for Normative Guidance section →
   `docs/agent-context/decision-hierarchy.md` (hub-originated; never
   propagated here; security-relevant).
2. Reverse-debt fix: `bin/check-dom15-fixtures` adopter comment says
   "agent-guidance" → "agent-theory" (rename staleness).
3. Reverse-debt fix: `bin/check-doc-paths` docstring truncated
   mid-sentence ("…and path claims into the") → completed.
4. Owner decision 2026-08-07: second-agent verification before
   physical plan deletion optional, not required →
   `docs/agent-context/runbooks/writing-plans.md` Plan Lifecycle and
   `skills/coalescing/SKILL.md` step 3.4 (retained: recorded-gate
   re-check, verified retrieval, ref reachability, two-step
   sequencing).
5. Owner decision 2026-08-07: ordinary-maintenance-is-Class-2 general
   rule + promotions gate on the human owner → [DOM-15] Rules bullet,
   [DOM-14] promotion sentence, coalescing-skill ceiling.
6. Demotion-in-place of superseded plan text, now normative →
   `writing-plans.md` Plan Lifecycle (this repo's own pre-release
   practice, promoted hub-side by owner direction).
7. Existence-check-first duties → `writing-plans.md` (author) and
   `review-loops-and-agent-bootstrap.md` (reviewer), with promotion
   provenance (owner direction; corroborated by the hub backport
   plan's citation audit).
8. Gate-wiring rule (writing a gate is not wiring it; shallow-clone
   honesty) → `runbooks/writing-specs.md` after the enumeration-gate
   rule.
9. Comprehension-gate teeth (expected answers in plan; answers in
   execution log; wrong answer blocks) → `writing-plans.md` §3 with a
   pointer from `hardening-plans.md` §14.
10. Release stop-gates → `hardening-plans.md` new §15, adapted to this
    repo's concrete release identity (tag-push workflows,
    `bin/release.py` as the executable driver).
11. Review-loops additions: guidance surfaces reviewable and
    blockable + deferred-units register; conditional audit-response
    protocol (§5a); review-timeout calibration clause.
12. Closure task-diff rule → `maintaining-traceability.md` Completion
    Gate (a checked box, passing default suite, or gated-off test is
    not evidence).
13. Gate-script upgrades: `bin/coalesce-check` shallow-clone loud-skip
    with cue-syntax inventory (scaffold contract change: exit 0 with
    printed skip on shallow clones, was exit 1 false-BROKEN) and
    origin/main-limitation comment; `bin/check-dom15-fixtures`
    rigorous fence parser + expanded self-test probes routed through
    the real extraction path.
14. Citation tests for theory drafting (accurately represented +
    load-bearing; register effects are drafting hypotheses, never
    evidence) → [DOM-16] anti-anchoring area, citing hub record
    [REV-AT-003] by name.

Excluded as already native (verified by grep at HEAD `8415919`):
archive rule and Class-2 sweep fixtures, Unindexed tier, judgment
paragraph, read-order contract test, Pattern 7 multiprocess,
reachability clause, two-tier chain, status-index bindings,
admission threshold, evidence-trail posture.

## Divergences and adaptations

- This repo's runbooks/spec diverge textually from the hub's; every
  insert is heading-anchored against THIS repo's text with unique-match
  assertions, never hub line numbers.
- Hub plan and theory records are foreign: quoted names with repo
  attribution, never backticked paths (check-doc-paths form rule).
- §-numbers cited by name where portable ("engineering principle §12
  (Enumerable Contracts Get Executable Gates)" style).
- Release stop-gates: hub text is general-with-conditional-examples;
  here the examples are this repo's real mechanisms (release-gate
  workflows, `bin/release.py`).
- The shallow-clone in-tool skip complements the existing
  `tests/test_doc_gates.py` shallow pytest-skip; the test is updated
  to assert the new loud-skip behavior instead of skipping.
- Dirty-tree invariant: the tree is clean at `8415919`; the only new
  files are this plan and the wave edits. Their WIP: none.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Tasks

1. Transplant payload items 1–14 per the adaptations above.
2. Gates: `python3 bin/check-doc-paths`, `python3 bin/check-dom15-fixtures`
   (+ `--self-test`), `python3 bin/coalesce-check`,
   `python3 bin/check-plan-context`; targeted pytest for
   `tests/test_doc_gates.py` and `tests/test_plan_context_gate.py`.
3. Scoped independent review (different family): adaptation only.
4. Land by explicit file-list staging; wave commit, then pin commit
   updating the coalescing run log's checked-through source SHA.
5. Coalescing sweep in the same unit: derive counts; under the newly
   landed deletion rule, physically delete `retired-pending` plans
   whose recorded five-gate harvest review passes mechanical re-check
   (retrieval from recorded source SHA, ref-reachable, no live-path
   backlinks); honest checked-deferred for anything else.

## Out of Scope

- Any product code, CLI, backend, or release behavior.
- The hub's [DOM-16]-package/ownership-triad constitutional items.
- Backfilling or re-reviewing this repo's own native rules.

## Review Log

| Date | Stage | Reviewer / result | Findings and disposition |
|------|-------|-------------------|--------------------------|
| 2026-08-07 | Scoped adaptation review, attempt 1 | codex CLI (`codex exec -s read-only -C <repo> …`, 900 s bound, completed in bound) — **blocker: F1,F2,F5,F6,F7**; P3s F3,F4,F8; no-clobber, placement, release-mechanics accuracy, and native-exclusion checks all passed | All eight accepted and applied: F1 [DOM-14] deletion bullet reconciled to the pinned semantics (the stale "independently verified" clause was an adaptation omission); F2 quoted foreign plan name; F3 owner-decision attribution added to [DOM-14] and the skill ceiling; F4 §3 cited by name (Context and Key Files); F5 Source Documents gains [DOM-16]; F6 new pytest for `check-dom15-fixtures --self-test`; F7 shallow test asserts the cue-syntax inventory; F8 docstring gains the shallow contract paragraph without touching native evidence-trail text. Round-2 re-review waived per the hub propagate-guidance skill step 6 (disclosed): every fix is mechanically verified — gates rerun green (`check-doc-paths`, live+self-test DOM-15, `coalesce-check`, `check-plan-context`), 8/8 pytest, and `grep -c "independently verified"` = 0. |

## Execution Log

(append-only)

- 2026-08-07: Transplant complete (14 payload items, 3 anchor
  corrections caught by unique-match assertions); completeness gate:
  19/19 checklist greps OK. Scoped review round 1: blocker
  F1,F2,F5,F6,F7 — all eight findings applied; round-2 waived with
  mechanical verification disclosed in the Review Log. Gates at
  landing: all five scripts exit 0; pytest test_doc_gates (4) +
  test_plan_context_gate (4) pass.
