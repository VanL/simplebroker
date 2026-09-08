# Process-Session Deferred Close Release

Class: 4. A patch-release CI failure exposed a cross-thread cleanup race in
process-session shutdown; correcting deferred resource ownership crosses a
concurrency and cleanup lifecycle boundary.
Plan type: implementation and coordinated patch release; no intended public API
shape change.
Owner: repository owner; execution by the assigned implementer.
Status: see the Status Index in `docs/plans/README.md`.

## Goal

Prevent bounded process-session shutdown from closing a shared backend runner
while an already-admitted core constructor or checkout rollback is still using
it. Preserve the five-second return bound, primary-error ordering, and the
existing three-package release process. Isolate the unrelated dump/load warning
test from first-use schema bootstrap without widening its timing allowance.

## Source Documents

- `docs/specs/16-python-library-api.md` [SB-API-11]: process-session resource
  ownership and cleanup failure order.
- `docs/implementation/06-process-session-core-ownership.md`: current session,
  factory, admitted-construction, and bounded-close state machines.
- `docs/implementation/10-ruff-suppression-registry.md`: generated evidence
  owner for broad-exception test probes.
- `docs/agent-context/runbooks/writing-plans.md` and
  `docs/agent-context/runbooks/hardening-plans.md`: class-4 lifecycle and release
  gates.

## Escalation Record

The work began as a mechanical patch release. Exact-SHA Windows 3.13 CI run
`34178863069` then produced a `ProgrammingError` from a runner closed during an
admitted constructor. That production-lifecycle finding escalated the task from
class 2 release preparation to this class 4 plan. The paired warning-routing
failure never reached its injected clock seam within five seconds, identifying
first-use bootstrap as unrelated setup in that test.

## Context and Key Files

- `simplebroker/_broker_session.py`: `_ProcessBrokerSession` admits core
  creation under its condition, waits up to
  `_CLOSE_ACTIVE_OPERATION_TIMEOUT`, and owns final factory cleanup.
- `simplebroker/db.py`: `_ProcessSessionCoreFactory` owns the shared runner and
  creates or closes backend cores.
- `tests/test_process_broker_session.py`: real-thread transition proofs for
  close, construction, checkout rollback, cleanup count, and failure priority.
- `tests/test_dump_load.py`: warning-sink isolation proof whose target can be
  initialized before the tested cross-thread warning phase.
- `bin/release.py` and the three release-gate workflows: exact-SHA validation
  precedes immutable tags and publication.

Comprehension gate, answered from the cited owners before the lifecycle edit:

1. Who owns the runner after the session drain deadline expires? The session
   still owns it; timeout bounds waiting but does not transfer ownership or
   authorize closing it underneath admitted work.
2. Which failure stays primary if admitted creation and deferred cleanup both
   fail? The creation or closed-session failure; ordinary cleanup failure is
   attached as diagnostic evidence. A cleanup `BaseException` retains its
   established interrupt priority.

An incorrect answer blocks implementation and requires rereading the two
ownership documents.

## Invariants and Constraints

- Do not increase `_CLOSE_ACTIVE_OPERATION_TIMEOUT` or test timing allowances.
- `close_all()` remains bounded when an admitted operation or constructor does
  not finish.
- No shared runner closes until every admitted core creation has unwound.
- Deferred cleanup runs exactly once; repeated close stays idempotent.
- Primary operation, construction, or closed-session failures remain primary;
  ordinary cleanup errors are notes, not replacements.
- Keep real threads, real SQLite bootstrap, and backend-style runner leases in
  the proofs. Event seams may control ordering; do not mock the session state
  machine.
- Do not alter backend API versions, storage formats, release authentication,
  worker counts, tag immutability, or publication environments.
- Stop and re-plan if safety requires an unbounded close, background cleanup
  worker, second factory state machine, or public contract change.

## Rollout and Rollback

Roll out as patch versions of core, PostgreSQL, and Redis from one exact tested
SHA. Tags are the only one-way door and are created only after the three
required workflows pass that SHA. Before tags, rollback is a normal revert of
the lifecycle commit. After any PyPI publication there is no rollback; a defect
requires a higher patch version and new immutable tags.

## Deviation Log

None.

## Tasks

1. Reproduce and classify both Windows failures from exact job logs.
   - Distinguish product ownership failure from unrelated test setup.
   - Done signal: causal timelines identify the closed runner and the warning
     test's pre-seam bootstrap.
2. Defer factory cleanup through the session's existing admitted-creation
   counter.
   - Files: `simplebroker/_broker_session.py`, ownership implementation doc.
   - Preserve the bounded return and one session state machine.
   - Done signal: forced-timeout construction and rollback retain a live runner
     until unwind, then close it once.
3. Preserve failure priority and isolate warning-test setup.
   - Files: both named test modules and the generated Ruff registry.
   - Force constructor plus cleanup failure and closed-session plus cleanup
     failure; pre-bootstrap the warning target before the five-second phase.
   - Done signal: primary exceptions and notes match [SB-API-11], with no timing
     allowance increase.
4. Review, verify, and release.
   - Require independent concurrency/cleanup review and incorporate findings.
   - Push one replacement SHA; require exact-SHA Test, PostgreSQL, and Redis
     success before `uv run --locked python bin/release.py all`.
   - Monitor tag workflows through PyPI and immutable GitHub Releases, verify
     artifact versions, then close this plan in a targeted commit.

## Testing Plan

- Run the two formerly failing tests repeatedly with real threads and SQLite.
- Run the complete process-session and dump/load modules plus transition,
  cross-thread finalization, and CLI dump/load suites.
- Run Ruff, formatting, mypy, generated suppression-index checks, DOM-15, and
  plan-context gates.
- Do not mock `_ProcessBrokerSession`, the factory ownership transition, or
  SQLite bootstrap. Purpose-built in-test factories may inject runner-close
  failure and block exact transition points.
- Final acceptance is the full local `release.py all` gate, exact-SHA CI, three
  tag-triggered release gates, and published artifact inspection.

## Independent Review Loop

An independent agent reviews the uncommitted lifecycle diff for races,
cleanup/error propagation, test quality, and documentation alignment. Any
severity finding blocks the replacement SHA until fixed and reverified. A final
review pass must cover the reviewer-driven correction as well as the original
deferred-close change.

## Out of Scope

- Redesigning active-operation timeout semantics.
- Changing phaselock, backend bootstrap, xdist topology, or Windows allowances.
- Weft and Taut changes.
- Any dependency update beyond the already merged build 1.6.0 Dependabot PR.

## Execution Log

- Exact-SHA run `34178863069`, Windows 3.13: 3200 passed, 230 skipped, and two
  failures. The process-session failure returned SQLite `ProgrammingError`
  after the drain deadline closed its runner; the warning test did not enter
  its injected clock seam within five seconds.
- The first implementation passed the affected modules and 50 repeated race
  pairs. Independent review then found that deferred factory-close failure
  could replace the active primary error. The correction retains the primary
  error and attaches ordinary cleanup failure as a note; the generated Ruff
  evidence is updated with the renamed firing test.

## Review Log

- Independent lifecycle review: one high finding (deferred cleanup could mask
  the primary error) and one low finding (stale generated test name). Both were
  accepted and corrected before the replacement SHA. The dump/load
  pre-bootstrap change was judged sound isolation, with fresh-target behavior
  already covered separately.
- Corrected-diff review: one medium finding showed that formatting an
  unformattable cleanup exception could still mask the primary error. Cleanup
  notes now render only literal string arguments, and a deferred-close
  regression makes `__str__` raise while proving that the primary error and
  diagnostic note survive.
- Final independent review: PASS after both review rounds were incorporated;
  no remaining code, race, exception-priority, test, documentation, or plan
  finding.
