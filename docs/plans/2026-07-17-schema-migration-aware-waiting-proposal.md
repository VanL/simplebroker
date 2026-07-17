# Schema Migration-Aware Waiting Proposal

**Date:** 2026-07-17

**Status:** draft — threshold-triggered follow-up; design decision and
independent review required before implementation

**Class:** 4 — this changes concurrent database-open behavior and failure
timing on a published library surface. It crosses process, persistence, and
recovery boundaries, so [DOM-5] and the hardening-plan gates apply.

## Goal

Prevent a concurrent opener from timing out while a healthy first opener is
still migrating a large legacy SQLite database, without making ordinary lock
contention take minutes to fail. The measured trigger is concrete: the 5.4.0
F21 investigation observed migration medians of 29.622 seconds at 10 million
rows and 273.111 seconds at 50 million rows against a fixed 20-second wait.

This artifact records the required follow-up proposal. It does not authorize
or implement a timeout change.

## Source Documents

- `docs/plans/2026-07-16-code-review-findings-remediation-plan.md` — F21 and
  Unit H threshold rule.
- `docs/plans/2026-07-16-code-review-findings-remediation-plan-f21-memo.md` —
  production-path fixture protocol, raw measurements, and decision.
- `README.md` — published SQLite behavior and safety contract.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15].

No product spec defines migration waiting today. The README remains the
product contract if this proposal becomes an implementation plan.

## Spec Baseline

Planning baseline: the SimpleBroker 5.4.0 remediation worktree based on
`21183714c696c5f60e9f9ba7aff0c08fa0c30bf4`. Before implementation, replace
this with the landed 5.4.0 commit and re-check every cited edit point.

## Context and Key Files

Read first:

- `simplebroker/_phaselock.py`: lock acquisition, fixed timeout, status file,
  owner diagnostics, and platform-specific advisory-lock behavior.
- `simplebroker/_backends/sqlite/plugin.py`: schema phase ownership and calls
  into migration.
- `simplebroker/_backends/sqlite/schema.py`: the v1-to-v5 migration chain.
- `simplebroker/_runner.py`: connection setup and fork recovery.
- `tests/test_sqlite_setup_contention.py`, `tests/test_phaselock.py`, and
  `tests/test_schema_migrations.py`: real-process and real-SQLite proof seams.

Current structure: one opener owns the schema phase advisory lock and performs
the migration. Other openers wait using the same fixed phase-lock budget used
for ordinary setup contention. The status sidecar contains ownership and
phase information, but it is not a migration-progress heartbeat. A waiter
therefore cannot distinguish a healthy 273-second migration from a stalled
holder before its 20-second budget expires.

Comprehension gates before editing:

1. Which lock/status state is kernel-released on process death, and which
   sidecar bytes may remain stale?
2. At what point can a waiter prove the holder is still alive without treating
   PID reuse or a stale status file as progress?
3. Which exact phase is migration-only, so a larger budget cannot leak into
   normal write contention or opportunistic maintenance?

## Invariants and Constraints

1. Ordinary PhaseLock contention and SQLite busy retry budgets do not inherit
   the migration budget.
2. A dead or wedged migration holder still produces a bounded failure. No
   unbounded polling and no “wait while PID exists” rule without PID-reuse
   protection.
3. Cancellation and watcher stop checks remain responsive while waiting.
4. Schema version, database layout, lock filename, and compatibility with
   5.4.x processes do not change unless a later reviewed plan explicitly
   approves that expansion.
5. The first migration holder remains the only writer. A waiter never attempts
   repair or a second migration while ownership is live.
6. Tests use real SQLite files and real processes. Do not mock the advisory
   lock, migration transaction, process death, or elapsed wait.
7. Weft usage and representative database sizes are checked before selecting
   a default. A 50M-row synthetic tier is evidence, not proof of Weft's current
   production distribution.

## Candidate Designs and Decision Gate

### A. Migration-specific total budget (leading simple option)

Give only the schema-migration phase a larger bounded wait. The measured worst
run was 372.106 seconds, so any proposed default needs explicit headroom and a
documented operator override. This is simple and backward-compatible, but a
fixed budget still scales poorly for larger databases and delays detection of
a genuinely wedged holder.

### B. Holder-liveness plus progress-aware budget

Wait while the same live holder reports monotonic migration progress, with a
short no-progress deadline and a hard total ceiling. This best matches the
failure model, but requires a trustworthy progress signal and a carefully
crash-safe sidecar lifecycle. It is the strongest behavior and the riskiest
implementation.

### C. Operator-serialized migration runbook

Keep runtime behavior unchanged and require operators to start one process,
wait for migration completion, then scale out. This has the smallest code
risk, but keeps default concurrent startup failure-prone and is easy for
automation to violate.

Decision gate: measure Weft's largest current broker and prototype whether an
existing status update can safely express progress. Choose B only if progress
can be made crash-safe without a storage-format or cross-version ambiguity;
otherwise choose A with an explicit bounded override and retain C as the
rollback/runbook.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Dependency-Ordered Tasks

1. Record Weft's current row-count/file-size distribution and first-open
   orchestration. Do not copy private production data into the repository.
2. Prototype the decision gate against the existing PhaseLock status model;
   document PID reuse, stale-status, crash, cancellation, and Windows behavior.
3. Amend this proposal into an executable implementation plan: select A, B, or
   C; name the exact config/constant surface; update the README contract delta;
   and obtain user approval plus independent review.
4. Add failing real-process acceptance tests before production edits:
   - a healthy migration exceeding 20 seconds lets a concurrent opener succeed;
   - a killed holder releases ownership and the waiter recovers or fails within
     the chosen bound;
   - a live holder with no progress hits the no-progress/total bound;
   - stop/cancellation interrupts the wait promptly;
   - normal PhaseLock contention retains its existing timeout.
5. Implement only the selected design, update README/CHANGELOG/implementation
   notes, and run the full SQLite, Postgres, and Redis gates.

## Rollback and Rollout

The rollout must be additive and readable by 5.4.x processes. If the new wait
policy regresses startup, operators fall back to the serialized-migration
runbook and the policy can be reverted without touching the migrated database.
No schema downgrade is required. Post-release signals: migration duration,
waiter timeout rate, cancellation latency, and reports of concurrent first-open
failure.

## Verification and Gates

- Firing tests named in task 4 on POSIX and Windows CI.
- Re-run the F21 10M tier at minimum; retain raw timing evidence.
- `uv run pytest`, `uv run bin/pytest-pg`, `uv run bin/pytest-redis`.
- Ruff, mypy, DOM-15 fixtures, runnable examples, packaging smoke.
- Independent completed-work review before any completion claim.

## Independent Review Loop

This draft requires a cross-family plan review after the design gate is
resolved. Every finding is dispositioned in this file. Implementation remains
blocked until the reviewer can answer “yes” to whether a zero-context engineer
can implement the selected design confidently and correctly.

## Out of Scope

- Changing ordinary SQLite busy retry behavior.
- Reworking schema migrations themselves for speed.
- A new schema or lock-sidecar format without a separately approved expansion.
- Distributed coordination for network filesystems; SQLite WAL on NFS/SMB
  remains unsupported.

## Fresh-Eyes Review

Pending after the Weft-size and progress-signal decision gate. The main risk to
challenge is mistaking holder liveness for useful progress; the main
overengineering risk is adding a heartbeat protocol when a bounded
migration-only budget plus operator runbook is sufficient.
