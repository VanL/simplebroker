# Windows xdist Contention Restoration Plan

Class: 3+P — this changes intended CI verification behavior across the pytest
marker registry, three test modules, the Windows workflow, its structural gate,
and durable testing guidance. The user explicitly requires removal of the
serial marker unless a test has a demonstrated incompatibility with xdist.
No public product contract, CLI shape, compatibility surface, storage format,
persistence, cleanup, async work, or one-way door changes, so no risky trigger
fires. Hardening: N/A — no risky trigger.

Status: completed

## 1. Goal

Restore the streaming, process-session, and cross-thread transition proofs to
the normal Windows xdist run. Remove the `windows_serial` marker and dedicated
serial workflow phases because the recorded failures demonstrate load
sensitivity, not cross-worker incompatibility. Keep the existing causal
synchronization, aggregate deadlines, timeout diagnostics, Windows worker
budget, and fail-closed worker-loss behavior.

## Source Documents

- Source spec: None — CI verification-policy correction requested explicitly
  by the repository owner; no product contract changes.
- Theory: N/A — the change does not alter product purpose, concepts,
  ownership, principles, or non-goals.
- `docs/agent-context/runbooks/testing-patterns.md`, especially Pattern 7 and
  Pattern 8.
- `docs/lessons.md`, including the 2026-08-07 and 2026-08-11 Windows timeout,
  xdist, process-session, and spawned-child entries.
- `.github/workflows/test.yml` and `tests/test_release_workflow.py`.
- Introducing commits `4946dfd7`, `0d158715`, and `9e249389`.

## 3. Context and Key Files

- `pyproject.toml` registers `windows_serial` solely for the dedicated Windows
  phase.
- `tests/test_streaming.py` has two function markers. The first marked test was
  rewritten after isolation was introduced, so its original high-volume
  rationale no longer describes the current proof.
- `tests/test_process_broker_session.py` applies the marker to the entire
  module, including tests with no subprocess or thread-lifecycle sensitivity.
- `tests/test_cross_thread_probe_transitions.py` applies the marker to the
  entire module. Its child readiness, aggregate deadline, stage diagnostics,
  and cleanup are test-owned.
- `.github/workflows/test.yml` excludes the marker from both Windows xdist
  invocations and repeats the selected modules in two `-n0` steps. Windows
  still deliberately uses two xdist workers and fails closed on worker loss.
- `tests/test_release_workflow.py` makes that isolation load-bearing and also
  counts the Windows 3.14 workflow condition.

Comprehension gate before editing: the implementer records answers in the
execution log. Any answer that disagrees with the expected answers below
blocks implementation until the cited workflow, tests, history, and testing
runbook are reread.

1. A real xdist incompatibility would require cross-worker interference such
   as a shared fixed target, port, suite ordering, or process-global state
   expected to span worker processes. The marked tests use worker-local
   processes and isolated targets; no such collision is recorded.
2. Serial success after a contended timeout proves that reducing load changes
   the outcome. It does not prove the test is incompatible with xdist.
3. Determinism must continue to come from test-owned barriers, readiness
   markers, state assertions, aggregate deadlines, and diagnostics. Removing
   contention is not a substitute for those controls.

## 4. Invariants and Constraints

1. The normal Windows matrix remains bounded at `-n 2`, keeps
   `--max-worker-restart=0`, and retains the existing job and per-test timeout
   diagnostics.
2. All formerly marked tests run exactly once in each applicable Windows
   matrix job. Coverage remains collected in the normal Python 3.14 Windows
   pass and uploaded through the existing Windows artifact path.
3. No test-owned child process, thread, barrier, readiness marker, aggregate
   deadline, cleanup, or assertion is weakened.
4. No new rerun, timeout increase, workload reduction, skip, or platform
   exclusion is introduced.
5. No product code or public documentation changes.
6. A future serial exception requires evidence of actual cross-worker
   interference and a companion proof for any supported contention it removes.

## 4a. Deviation Log

| Baseline | Planned behavior | Actual behavior | Rationale | Follow-up |
|----------|------------------|-----------------|-----------|-----------|

## 4b. Spec Baseline

- Source spec: None — verification-policy correction only.

## 4c. Exact Proposed Process Delta and Promotion Strategy

Promotion strategy: **D — direct owner-ratified guidance correction**. This is
process-authoring rather than a product-spec change: promote the following
dated entry directly into `docs/lessons.md` before changing the workflow. It
needs no implementation-link claim. The user's instructions in this session
supply the required owner ratification.

Exact text:

> 2026-08-13: Determinism in concurrency tests comes from controlling and
> observing the actors the test owns, not from removing default runner
> contention. A timeout under xdist proves load sensitivity, not xdist
> incompatibility. Keep a proof in the default contended phase unless evidence
> identifies actual cross-worker interference, such as a shared fixed target,
> port, suite-order dependency, or state intended to span worker processes. A
> serial diagnostic companion may help isolate a failure, but must not replace
> the contended gate or make the test easier to pass. This corrects the
> 2026-08-07 and 2026-08-11 conclusions that ambiguous Windows timeouts alone
> justified a dedicated serial platform phase; their failure observations and
> causal synchronization guidance remain valid.

The correction expressly supersedes only the serial-phase conclusions in the
2026-08-07 `node down`, 2026-08-11 process-session, and 2026-08-11
spawned-child entries. It preserves their observed run evidence, deadline and
diagnostic requirements, and test-owned concurrency guidance.

## 4d. Consulted Surfaces

- `docs/program-theory.md`
- `docs/agent-context/decision-hierarchy.md`
- `docs/agent-context/principles.md`
- `docs/agent-context/engineering-principles.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`
- `docs/lessons.md`
- `.github/workflows/test.yml`
- `pyproject.toml`
- `tests/test_release_workflow.py`
- `tests/test_streaming.py`
- `tests/test_process_broker_session.py`
- `tests/test_cross_thread_probe_transitions.py`

## 5. Tasks

1. Promote §4c's exact dated durable correction to `docs/lessons.md` after
   independent plan approval and before changing verification behavior.
2. Replace the isolation-enforcing workflow test with a red structural gate
   that forbids registration or assignment of `windows_serial`, marker
   exclusions, and dedicated serial steps; requires both Windows commands to
   select exactly `not benchmark`; and retains `-n 2 --dist loadgroup
   --max-worker-restart=0` plus Python 3.14 coverage.
3. Remove all four test marker assignments and the pytest marker registration.
4. Remove the two serial workflow steps and marker exclusions. Update the
   Windows 3.14 condition count while preserving normal coverage production.
5. Run focused workflow tests, the three affected suites under xdist, static
   checks, documentation gates, and an independent completed-work review.
6. Close this plan and its Status Index row only after all evidence is current.

Stop and re-evaluate if any affected test reveals a concrete shared resource,
suite-order dependency, or cross-worker collision. Do not reintroduce blanket
isolation from a timeout alone.

## 6. Testing Plan

Red-first: change the structural workflow contract first and confirm it fails
against the existing isolation. Then implement the workflow and marker removal.

Real proofs:

```bash
uv run pytest -n0 -q tests/test_release_workflow.py
uv run pytest -n 2 --dist loadgroup -q tests/test_streaming.py tests/test_process_broker_session.py tests/test_cross_thread_probe_transitions.py
uv run ruff check tests/test_streaming.py tests/test_process_broker_session.py tests/test_cross_thread_probe_transitions.py tests/test_release_workflow.py
uv run ruff format --check tests/test_streaming.py tests/test_process_broker_session.py tests/test_cross_thread_probe_transitions.py tests/test_release_workflow.py
python3 bin/check-dom15-fixtures
bin/check-plan-context
git diff --check
```

The local xdist run does not substitute for Windows CI. Residual acceptance is
the next exact-SHA Windows matrix run under `-n 2`.

## 7. Verification and Gates

- The repository contains no `windows_serial` marker or dedicated serial step.
- Workflow tests prove both Windows pytest commands use `-n 2`, select
  `not benchmark`, and retain fail-closed worker handling.
- The affected suites pass together under xdist without weakening their owned
  concurrency.
- Documentation and plan-context gates pass.
- Independent review finds no unsupported serial exception or lost coverage.

## 8. Independent Review Loop

- Plan review before implementation: independent review `/root/plan_review`
  returned BLOCKED on the missing exact process delta. The reviewer also ran
  the unchanged affected suites under `-n 2`: 46 passed in 6.97 seconds, and
  found no demonstrated xdist incompatibility. The blocker is accepted and
  resolved by §4c's exact owner-ratified text and strategy D.
- Plan re-review input: this revised plan, DOM-5/DOM-15, testing patterns,
  current workflow and marker assignments, and introducing commits
  `4946dfd7`, `0d158715`, and `9e249389`. Required verdict:
  implementation-ready or blocked, with every process-delta concern named.
- Completed-work and pre-landing review inputs: final diff; this plan and
  deviation log; focused workflow test; combined `-n 2` affected-suite run;
  Ruff, format, DOM-15, plan-context, and diff gates. Review must check that no
  assertion, workload, timeout, diagnostic, or coverage path was weakened and
  disposition every finding before closure. A different model family is
  preferred for the pre-landing pass; if unavailable, disclose the limitation.
- Completed-work and pre-landing review: independent `gpt-5.6-terra` review
  initially BLOCKED on loss of the non-coverage Windows thread-timeout guard
  and an under-broad structural scan. Both findings were accepted and fixed;
  re-review PASS confirmed no assertion, workload, owned concurrency,
  diagnostic, fail-closed behavior, or Windows coverage path was weakened.

## 9. Out of Scope

- Increasing Windows xdist above two workers.
- Redesigning subprocess or thread helpers.
- Adding a new stress workflow or changing product behavior in response to a
  failure. A concrete failure may justify a separate follow-up.

## 10. Execution Log

- 2026-08-13: Classified Class 3+P. Historical audit found timeout and load
  sensitivity evidence, but no shared resource or cross-worker incompatibility.
- 2026-08-13: Comprehension gate answers: (1) incompatibility requires concrete
  cross-worker interference, and none is recorded; (2) serial success proves
  load sensitivity only; (3) barriers, readiness markers, state assertions,
  aggregate deadlines, and diagnostics own determinism, while default runner
  contention remains part of the gate. All answers match §3; implementation
  may proceed after revised-plan review.
- 2026-08-13: Independent plan re-review PASS after the exact strategy-D
  guidance delta, comprehension gate, structural contract, review inputs, and
  task order were corrected. The reviewer found no demonstrated xdist
  incompatibility.
- 2026-08-13: Red workflow witness failed on the still-registered
  `windows_serial` marker. Removed the four assignments, marker registration,
  both selector exclusions, and both dedicated serial steps without changing
  workloads, timeouts, owned concurrency, diagnostics, or the Windows `-n 2`
  worker budget. Focused workflow contract: 3 passed. Full workflow-policy
  suite: 42 passed. Affected suites together under `-n 2 --dist loadgroup`: 46
  passed. Ruff check and format, DOM-15 fixtures, plan-context, and diff check
  passed. Exact-SHA Windows acceptance remains a post-landing residual because
  it cannot be run locally.
- 2026-08-13: Independent completed-work review BLOCKED on two accepted
  findings. P1 found that the ordinary Windows 3.11–3.13 command lacked the
  removed serial phase's `--timeout=180 --timeout-method=thread` diagnostic;
  those options were restored without changing contention. P2 found that the
  structural gate scanned only the three formerly marked modules and matched
  the selector by substring; it now scans every Python file under `tests/`
  except its own negative assertion and requires the Windows commands' sole
  `-m` expression to equal `not benchmark`.
- 2026-08-13: Post-fix verification repeated from current state: affected
  suites 46 passed under `-n 2 --dist loadgroup`; workflow policy 42 passed;
  Ruff check/format, DOM-15 fixtures, plan-context, and diff check passed.
  Independent re-review PASS. Exact-SHA Windows CI remains the only residual
  acceptance signal that requires publication of the commit.
- 2026-08-13: Owner directed closure and commit. Two shared tests discovered
  during follow-up audit were ported from explicit SQLite paths to resolved
  `BrokerTarget` values; both passed through SQLite, `pytest-pg`, and
  `pytest-redis`. Final closure gates passed from the closing tree. Exact-SHA
  Windows CI remains a post-commit/push acceptance signal and was not run
  because this task does not authorize a push.
