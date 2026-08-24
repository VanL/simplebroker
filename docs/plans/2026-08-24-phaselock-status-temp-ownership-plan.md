# Phase-lock Status Temp Ownership Plan

Status: completed
Class: 3 — a release-gate failure exposed a cross-process temp-file cleanup
race in SQLite setup coordination. The task escalated from ordinary release
execution before the corrective commit or tag boundary.

## Goal

Prevent a fresh-database setup contender from deleting the fallback status
temporary file owned by the process currently holding the phase lock, while
preserving stale-marker recovery and the existing public setup behavior.

## Source Documents

Source spec: None — internal setup-coordination correctness bug; no public
contract or storage format changes.

- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/agent-context/runbooks/hardening-plans.md`

## Context and Key Files

- `simplebroker/_phaselock.py`: `PhaseLockService.run_phases()` owns advisory
  lock acquisition, fallback marker reads, phase execution, and atomic status
  publication. `discard_status_markers()` removes the stable status file and
  matching temporary files.
- `simplebroker/_runner.py`: `SQLiteRunner._run_exclusive_setup()` decides when
  a missing or undersized SQLite target invalidates stale completion markers.
- `tests/test_runner_error_handling.py`: owns real-thread and real-process
  setup-serialization proofs.
- `tests/test_runner_validation.py`: proves stale markers do not make empty or
  invalid targets bypass setup validation.

Comprehension gate recorded during execution:

1. Who owns the temp file between exclusive creation and `os.replace()`?
   The phase-lock holder performing status publication.
2. Why did the existing advisory lock not prevent deletion? The contender ran
   `_discard_stale_completion_markers()` before acquiring that lock, and the
   cleanup glob included every `.status.tmp.*` file.

An incorrect answer blocks implementation until `_run_exclusive_setup()`,
`run_phases()`, and `discard_status_markers()` are reread together.

## Invariants and Constraints

- Fresh or undersized SQLite targets must still reject stale completion
  markers and run the required connection, schema, and optimization phases.
- A process must not remove ordinary status artifacts while another ordinary
  setup process holds the phase lock. Explicit global `--cleanup` remains the
  documented destructive exception.
- The freshness predicate must be re-evaluated after lock acquisition. A
  waiter must not erase markers published by an owner that created the target
  while the waiter was blocked.
- Status publication remains an exclusive temp create, flush, atomic replace,
  and optional directory fsync. Do not weaken or retry around a missing source.
- Keep the real filesystem, advisory lock, threads, and process pool in the
  verification path. Mock only the cleanup observation needed to make ordering
  deterministic.
- No public API, CLI, schema, marker format, dependency, or timeout change.
- Stop and re-evaluate if the fix requires a second lock, a new marker format,
  a best-effort suppression of `FileNotFoundError`, or weaker serialization.

## Rollback, Rollout, and Observation

The code and test change is independently revertible before publication. No
data migration or one-way door exists. After release, success is the absence
of `.status.tmp.* -> .status` `FileNotFoundError` in the normal CI setup gate,
plus green macOS, Windows, and fallback-path concurrency jobs on the exact
release SHA. A PyPI publication itself is not rollbackable.

## Out of Scope

- Redesigning phase-lock storage or advisory-lock portability.
- Changing the undefined overlap between explicit global cleanup and live use.
- Increasing timeouts, reducing xdist workers, or weakening the concurrency
  proof.

## Tasks and Evidence

1. Reproduce the ordering defect with a deterministic contender that attempts
   stale-marker cleanup while a setup owner holds the phase lock.
2. Move the freshness recheck and status discard into the acquired-lock region;
   make read-only completion checks return false without deleting markers.
3. Run the deterministic regression, the real multiprocess schema setup test,
   the full fallback-path gate, repeated multiprocess stress, Ruff, mypy, and
   the complete release driver.
4. Update the implementation rationale, changelog, and durable lesson; obtain
   an independent diff review before closing this plan.

Observed evidence:

- The deterministic lock-ordering regression failed before the production
  change because contender cleanup ran while the owner held the phase lock.
- The focused invalidation regressions pass for ordinary fallback cleanup,
  suppressed unlink failure, xattr cleanup, and xattr cleanup followed by a
  fallback reopen.
- The fallback-path CI-equivalent gate passes with 121 tests passed and 5
  platform skips. The original multiprocess schema-serialization test passed
  50 consecutive fallback-mode iterations.
- Ruff, format, source and affected-test mypy, `check-dom15-fixtures`,
  `check-plan-context`, and `git diff --check` pass.
- Independent review found and drove fixes for failed-unlink invalidation, a
  scheduling handshake in the ordering test, dead unlocked cleanup code, and
  both xattr/fallback transition cases. The final blocker-only review reported
  no remaining code, concurrency, API, test, or documentation blocker.

## Anti-mocking and Acceptance

The regression must use two real `SQLiteRunner` instances and the real
advisory lock. It may observe `discard_status_markers()` to expose ordering,
but must not replace lock acquisition, marker IO, or status publication. The
pre-fix test must fail because contender cleanup is observed before owner
release; the post-fix test must pass and both threads must terminate without
errors.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

No deviations recorded.

## Completion Gate

- The implementation, focused regressions, fallback gate, stress evidence,
  static checks, documentation checks, and independent review are complete in
  the corrective commit.
- Exact-SHA GitHub workflows remain the downstream release gate. No tag may be
  created until those workflows pass.
- `docs/plans/README.md` is `completed` in the corrective commit.
