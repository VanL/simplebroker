# Windows Auto-xdist Restoration Plan

Class: 3+P — this changes intended Windows CI verification behavior and its
structural policy gate. The owner explicitly requires reversal of the fixed
two-worker cap. No product contract, CLI, compatibility, storage, persistence,
cleanup, async work, or one-way door changes. Hardening: N/A — no risky trigger.

Status: completed

## 1. Goal

Restore both Windows full-suite commands from fixed `-n 2` to the repository's
default `-n auto`. Keep `--dist loadgroup`, the 180-second thread-dump timeout,
`--max-worker-restart=0`, the 45-minute outer bound, coverage, and all test
behavior unchanged.

## Source Documents

- Source spec: None — owner-directed CI verification-policy correction.
- Theory: N/A — no product-theory claim changes.
- `docs/agent-context/runbooks/testing-patterns.md`.
- `docs/lessons.md`, especially the 2026-08-13 correction that load
  sensitivity does not establish incompatibility.
- Introducing commit `fb4b4a25` and current workflow commit `e755dd7`.
- `.github/workflows/test.yml` and `tests/test_release_workflow.py`.

## 3. Context and Key Files

- Commit `fb4b4a25` changed both Windows commands from `-n auto` to `-n 2`
  after process-heavy timeouts. It recorded oversubscription and load
  sensitivity, not an xdist incompatibility or a reason that automatic worker
  discovery was invalid.
- `.github/workflows/test.yml` currently uses fixed `-n 2` for Windows while
  non-Windows uses `-n auto`.
- `tests/test_release_workflow.py::test_windows_tests_keep_default_xdist_contention`
  currently requires `-n 2` and forbids `-n auto`, making the load-shedding cap
  a durable gate.
- The prior completed plan remains historical and is not edited. This plan
  owns the follow-up correction.

Comprehension gate: implementation proceeds only if these answers hold:

1. What did the old evidence prove? Expected: subprocess-heavy tests were
   sensitive to concurrent load on some Windows runners.
2. What did it not prove? Expected: that automatic xdist worker selection was
   invalid or that the tests had cross-worker interference.
3. What remains load-bearing? Expected: owned synchronization and diagnostics,
   `loadgroup`, thread-dump timeouts, fail-closed worker loss, coverage, and the
   outer job bound.

## 4. Invariants and Constraints

1. Change only the worker-count argument and its structural assertions.
2. Preserve all selectors, timeouts, distribution mode, coverage arguments,
   fail-closed handling, and test selection.
3. Do not serialize, skip, shrink, rerun, or increase deadlines.
4. Both Windows full-suite commands must use `-n auto` exactly once and must
   not contain a fixed `-n 2` argument.

## 4a. Deviation Log

| Baseline | Planned behavior | Actual behavior | Rationale | Follow-up |
|----------|------------------|-----------------|-----------|-----------|

## 4b. Spec Baseline

- Source spec: None — verification-policy correction only.

## 4c. Exact Process Delta and Promotion Strategy

Promotion strategy: D — direct owner-ratified clarification in the existing
2026-08-13 lesson. Append this sentence to that entry before workflow edits:

> Default contention includes the workflow's automatic xdist worker selection;
> a fixed worker cap is load shedding and needs independent evidence beyond an
> ambiguous timeout cluster.

The user's direction supplies owner ratification. The historical failure
records remain intact.

## 4d. Consulted Surfaces

- `docs/program-theory.md`
- `docs/agent-context/decision-hierarchy.md`
- `docs/agent-context/engineering-principles.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/lessons.md`
- `.github/workflows/test.yml`
- `tests/test_release_workflow.py`

## 5. Tasks

1. Promote §4c's exact clarification.
2. Change the structural gate first and observe it fail against fixed `-n 2`.
   For each Windows command, assert `step.count("-n auto") == 1` and absence
   of `-n 2`; retain the existing exact selector and control assertions.
3. Restore both Windows commands to `-n auto` without changing any other
   argument.
4. Run the workflow-policy suite, Ruff, format, documentation, plan-context,
   and diff gates.
5. Obtain independent completed-work review, disposition findings, and close
   the plan/index only after current-state verification passes.

## 6. Testing Plan

```bash
uv run pytest -n0 -q tests/test_release_workflow.py
uv run ruff check tests/test_release_workflow.py
uv run ruff format --check tests/test_release_workflow.py
python3 bin/check-dom15-fixtures
bin/check-plan-context
git diff --check
```

The next exact-SHA Windows CI run remains the platform acceptance signal.

## 7. Verification and Gates

- Both Windows commands use `-n auto` and preserve every other load-bearing
  option.
- Workflow policy tests pass.
- Static and documentation gates pass.
- Independent review confirms the diff changes no other CI dimension.

## 8. Independent Review Loop

- Plan review inputs: this plan, DOM-5/DOM-15, testing patterns, current
  workflow/gate, and `fb4b4a25`. Required verdict: ready or blocked.
- Completed-work review inputs: final diff and all §6 evidence. Required check:
  only worker selection changed; selectors, timeouts, distribution, coverage,
  fail-closed handling, and outer bounds remain intact.
- Independent plan review: PASS after adding the explicit fresh-eyes record and
  exactly-once `-n auto` structural assertion.
- Independent completed-work review (`gpt-5.6-terra`): PASS with no findings.
  It confirmed the workflow diff contains exactly two `-n 2` → `-n auto`
  substitutions and preserves every named control.

## 9. Out of Scope

- Changing product tests, xdist grouping, deadlines, or CI matrix versions.
- Diagnosing a future concrete Windows failure before it occurs under restored
  default contention.

## 10. Fresh-Eyes Review

Before implementation, reread the plan from the viewpoint of a zero-context
engineer and verify: the current and target worker settings are unambiguous;
the exact structural assertion is stated; no instruction permits changing a
timeout, selector, distribution mode, coverage option, fail-closed setting, or
job bound; the next Windows run is disclosed as residual evidence. Outcome:
PASS after independent review required an explicit exactly-once `-n auto`
assertion and this fresh-eyes record. No implementation ambiguity remains.

## 11. Execution Log

- 2026-08-13: Classified Class 3+P. Historical audit found that `fb4b4a25`
  changed `-n auto` to `-n 2` after load-sensitive timeouts without proving
  automatic worker selection invalid.
- 2026-08-13: Comprehension answers: the evidence proved load sensitivity; it
  did not prove cross-worker incompatibility; synchronization, diagnostics,
  loadgroup, fail-closed handling, coverage, and outer bounds remain
  load-bearing.
- 2026-08-13: The red structural witness failed because both Windows commands
  still contained fixed `-n 2`. Changed only those two tokens to `-n auto`.
  Workflow-policy suite: 42 passed. Ruff check/format, DOM-15 fixtures,
  plan-context, and diff check passed. Independent completed-work review PASS
  with no findings. Exact-SHA Windows CI remains the external acceptance
  signal.
