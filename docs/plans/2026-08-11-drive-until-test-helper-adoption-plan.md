# Deterministic Test Wait Helper and Targeted Adoption Plan

Date: 2026-08-11
Status: completed — implementation, full verification, final review, and
owner-authorized landing complete
Class: 3 — the change introduces a reusable test workflow, crosses the shared
timing-helper, core watcher, extension-transition, and reference-example test
boundaries, and would leave a zero-context implementer guessing without an
explicit migration fence. No [DOM-5] risky trigger fires: the helper itself is
synchronous test infrastructure; it introduces no runtime async/background
work, public or compatibility contract, storage change, persistence lifecycle,
destructive edge, or one-way door.
Plan type: test-infrastructure implementation against existing repository
testing guidance; no product-spec or program-theory revision
Hardening: N/A — no [DOM-5] risky trigger. The plan still records callback,
deadline, anti-mocking, rollback, and review boundaries because the adopters
exercise concurrent behavior.

## Goal

Put eventual-condition waiting and manually driven reactor waiting behind one
tested test-only primitive, including the deadline-edge final observation,
one readiness-gated final drive turn, and useful timeout diagnostics. Preserve
the current Boolean waiter for callers that intentionally branch on timeout,
then adopt the raising helper only where it removes a duplicated loop or turns
an ignored timeout into a firing failure. The change must improve test
determinism and failure locality without imposing one abstraction on typed
multiprocess protocols or adding a repository policy gate.

## Source Documents

- Source spec: None — this is internal test infrastructure and test-suite
  migration; it does not change SimpleBroker product behavior.
- User-approved direction in the 2026-08-11 task: plan a deterministic test
  wait helper and adopt it where the repository evidence justifies it.
- `docs/program-theory.md` [THEORY-2], [THEORY-4], [THEORY-5]: SimpleBroker
  owns queue semantics; consumers own application completion and ledgers; new
  concepts require concrete pressure.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-8], [DOM-10], [DOM-11], [DOM-15].
- `docs/agent-context/engineering-principles.md` sections 4, 6, 7, 8, 10,
  13, and 14: real behavior, reuse, YAGNI, independent review, failing-first
  proof, deficiency gates, and named state-machine contracts.
- `docs/agent-context/runbooks/testing-patterns.md` rules 1–6 and patterns 4
  and 7: observable behavior, bounded polling, aggregate multiprocess
  deadlines, and diagnostics.
- `docs/agent-context/runbooks/writing-plans.md` and
  `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`.
- `docs/plans/2026-08-10-test-suite-signal-remediation-plan.md`: existing
  deterministic-concurrency posture and the constraint against a speculative
  general-purpose testing framework.
- `docs/implementation/07-complexity-and-state-machine-map.md` entries
  [SM-POLLING], [SM-REACTOR], [SM-REACTOR-OUTPUT], [SM-PG-LISTENER], and
  [SM-REDIS-ACTIVITY-LISTENER].
- `docs/lessons.md` 2026-08-07 concurrency and readiness-marker entries:
  assert owned synchronization state rather than scheduler speed; publish
  readiness from the exact lifecycle boundary under test.

## Consulted Surfaces Declaration

Plan authoring consulted the program theory; context index, decision
hierarchy, principles, engineering principles, testing-patterns,
writing-plans, hardening-plans, and review-loop runbooks; DOM-5/8/10/11/15;
the specs and implementation indexes; repository map and agent inventory;
the plan index and coalescing state; the completed test-suite signal plan; the
timing, watcher, extension listener, managed-subprocess, multiprocess watcher,
and reference-reactor code and tests named below.

## Repository Baseline and Spec Impact

- Repository baseline: `7610c73b1a3ee5b72389273e5235a28fc69a3bb3`.
- No product spec is revised. No `[SB-*]` clause, public Python/CLI surface,
  runtime timeout, backend protocol, or release note changes.
- The helper is test-only. Its interface is owned by
  `tests/helper_scripts/timing.py` and its direct contract tests, not by a
  product spec or README statement.
- No spec backlink is required. This plan and its Status Index row are the
  traceability owners for the internal change.

## Decisions and Explicit Rejections

1. **Extend the existing timing path.** Add `drive_until` beside
   `wait_for_condition` in `tests/helper_scripts/timing.py` and export it from
   `tests/helper_scripts/__init__.py`. Do not create a third timing module or
   move the state-machine helpers in `tests/helpers/`.
2. **Keep a compatibility path.** Existing callers that intentionally inspect
   a Boolean timeout may continue using `wait_for_condition`; its polling must
   share the new primitive's tested loop and gain the final predicate check.
   Preserve its current signature defaults (`timeout=5.0`, `interval=0.1`,
   `message=None`) and explicitly forward its interval into the shared loop;
   do not inherit `drive_until`'s `interval=0.01` default. Do not silently turn
   every Boolean timeout into an exception.
3. **Raise in new and migrated assertion-style use.** `drive_until` returns
   `None` on success and raises an `AssertionError` subtype on timeout. This
   makes an ignored timeout impossible at adopted call sites.
4. **One hard deadline only.** Do not add `stall_timeout` or `progress` in this
   change. SimpleBroker tests do not share one causally relevant progress
   token; unrelated queue or database activity could reset a stall clock while
   the asserted condition remains stuck.
5. **No generic safety DSL.** Do not add `assert_order`. SimpleBroker queue
   rows are mutable current state and message IDs are not a general event
   chronology. Short test-owned traces should keep direct sequence assertions;
   broker mutations should use exact final-state or conservation assertions.
6. **No policy gate or runbook mandate.** Adoption evidence is not mature
   enough to classify every valid wait loop mechanically. Reconsider only
   after the helper is stable and a new semantically equivalent local loop or
   repeated flake shows that review alone is insufficient.

These are plan-local scope decisions. The underlying product boundaries
already live in program theory and `[SB-ID-*]` / `[SB-SELECT-*]`; this plan
does not create a competing durable alternative record.

## Context and Key Files

### Current owners

- `tests/helper_scripts/timing.py` owns shared polling and CI timeout scaling.
  `wait_for_condition` currently checks only while `now < deadline`, does not
  make a final boundary observation, returns `False` without diagnostics, and
  accepts a `message` that it does not surface. `wait_for_value` and
  `wait_for_count` delegate to it.
- `tests/helper_scripts/__init__.py` is the existing package-level export
  surface for timing helpers.
- `tests/test_performance_harness.py` tests performance calibration and timeout
  scaling. It is not the owner for an eventual-wait state machine. Add the new
  direct contract module as `tests/test_timing_helpers.py` rather than mixing
  state-machine tests into the performance harness.
- At the baseline, `wait_for_condition` has 64 external calls across 10 core
  test modules. Seventeen calls discard its Boolean result: sixteen in
  `tests/test_watcher_burst_mode.py` and one in
  `tests/test_watcher_race_conditions.py`.
- `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py` and
  `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py`
  each define the same local `_wait_until` loop for listener-error
  publication.
- `examples/reference_reactor.py::BaseReactor.process_once` owns a reactor
  turn; `wait_for_activity` owns its bounded wait;
  `Reactor._has_pending_reactor_results` and
  `Reactor._has_pending_reactor_backlog` expose the two readiness facts that
  justify a final deadline turn.
- `examples/tests/test_reference_reactor.py` contains both pure-observation
  waits and one success-predicate drive loop for transient output publication.

### Explicit migration set

| File / node | Current shape | Planned use |
|-------------|---------------|-------------|
| `tests/test_watcher_burst_mode.py` — the 16 expression-statement `wait_for_condition` calls in `test_burst_mode_resets_on_activity`, `test_burst_mode_no_reset_on_empty_wake`, `test_burst_mode_with_errors_single_message`, `test_burst_mode_with_errors_batch_processing`, `test_burst_mode_with_peek_mode`, and `test_burst_mode_state_transitions` | Timeout Boolean is discarded | Replace with raising `drive_until`; preserve each predicate, timeout, interval, message, and subsequent exact assertions |
| `tests/test_watcher_race_conditions.py::test_multiple_queues_concurrent_activity` | Initial-drain wait result is discarded | Replace with raising pure-observation `drive_until`; diagnose per-watcher drain counts |
| `tests/test_watcher_transition_tables.py::test_watcher_lifecycle_fires_transition_table` | Two raw deadline/sleep loops wait for `is_running()` and one delivered payload | Replace with pure-observation `drive_until`; preserve the transition-table assertions and real watcher thread |
| PostgreSQL and Redis listener transition modules | Duplicated `_wait_until` helpers; three total calls | Delete the locals and import `drive_until`; diagnose ready/error/closed state without replacing the scripted transport seam |
| `examples/tests/test_reference_reactor.py::_wait_for_outputs` | File-local deadline loop | Delegate to pure-observation `drive_until`; retain this domain wrapper because it names the output queue/count and supplies queue diagnostics |
| `test_reactor_turns_have_single_thread_owner` and `test_manual_drive_thread_self_closes_after_external_stop_join_false` | Duplicated owner-publication loops | Use pure-observation `drive_until` for `_drive_owner_ident`; retain the subsequent ownership and shutdown assertions |
| `test_pending_output_retries_in_process_after_transient_publish_failure` | Manual `process_once` / predicate / `wait_for_activity` loop | Use driven mode with `process_once`, `wait_for_activity`, and readiness probes for worker results and backlog; diagnose publish attempts, result/seen status counts, pending output count, stop flags, and owned thread identity |
| `test_output_backlog_blocks_new_input_but_not_control_lane` | Pure sidecar-status deadline loop | Use pure-observation `drive_until`; preserve the background reactor and later control-lane/final-state proof |

### Deliberate non-migrations

- `tests/helper_scripts/managed_subprocess.py::ManagedProcess.wait_for_output`
  keeps ownership of stream decoding, process exit, and output diagnostics.
- `tests/test_watcher_multiprocess.py` keeps its typed, consumptive child
  protocol loops, aggregate phase deadlines, early child-error handling, and
  PID/exit/liveness diagnostics. `multiprocessing.Queue.empty()` must not be
  introduced as a readiness oracle.
- Negative blocking proofs that use an owned entry/release event plus a short
  timed non-completion assertion remain specialized. `drive_until` proves
  eventual liveness; it does not prove absence.
- Expected-exception loops, including the simulated reactor crash, remain
  explicit unless a positive success predicate exists. Do not call
  `drive_until(predicate=lambda: False, ...)` merely to reuse syntax.
- Performance thresholds, jitter/backoff measurement, timestamp-boundary
  waits, lock-contention induction, process reaping/escalation, and cleanup
  deadlines retain their owning helpers.
- `_wait_for_control_reply_at_timestamp` returns a selected payload and remains
  domain-specific. Do not add a generic value-capture protocol to
  `drive_until` for one caller.
- The two 100 ms reference-reactor duplicate-absence assertions are not
  converted into ordering claims by this plan. A snapshot after the first
  output is not a causal quiescence marker; any later repair must first name
  the producer-closing/fencing evidence.

### Required reading comprehension gate

Before implementation, record answers in the execution log. A wrong answer
blocks editing until the cited owner is reread.

1. **When may the helper perform a final deadline `step`?**
   Expected answer: only when at least one declared readiness probe reports
   ready; the helper performs at most one shared final step and then rechecks
   the predicate. It never blindly drives after expiry.
2. **What does the hard timeout actually bound?**
   Expected answer: helper-controlled polling and waits. Predicate, step,
   readiness, and wait callbacks must themselves be bounded; the helper does
   not spawn a watchdog thread or forcibly cancel a blocking callback.
3. **Why are the multiprocess watcher loops excluded?**
   Expected answer: they consume typed protocol messages and update phase
   state while surfacing child errors and process diagnostics. Hiding those
   transitions in closures would relocate rather than remove complexity and
   could make queue readiness unsafe.

## Proposed Test-Only Interface and Semantics

The exact spelling may be formatted to repository style, but implementation
must preserve this behavior:

```python
def drive_until(
    predicate: Callable[[], bool],
    *,
    step: Callable[[], None] | None = None,
    wait: Callable[[float], None] = time.sleep,
    drains: Sequence[Callable[[], bool]] = (),
    timeout: float = 5.0,
    interval: float = 0.01,
    message: str = "condition did not become true",
    diagnostics: Callable[[], object] | None = None,
) -> None: ...
```

Contract:

1. Use `time.monotonic()` and one aggregate deadline. Do not scale timeouts
   internally; callers retain ownership of `scale_timeout_for_ci`.
2. Check `predicate` before the first step so an already-satisfied condition
   causes no side effect.
3. Before the deadline, call `step` at most once per turn when supplied,
   recheck the predicate, then call `wait(min(interval, remaining))` only when
   more waiting is needed. In observation mode, omit the step but retain the
   same predicate/deadline/wait loop.
4. At or after the deadline, make a final predicate check. If it is false and
   any readiness callback in `drains` reports true, call `step` exactly once
   (not once per ready callback) and recheck exactly once. Do not wait again.
5. Reject `drains` with `step=None` at entry. Do not add other speculative
   validation unless a firing test demonstrates a current misuse.
6. On remaining failure, raise a dedicated internal `AssertionError` subtype
   containing the caller message, elapsed time, predicate-check count,
   step-call count, and `repr()` of diagnostics. Invoke diagnostics only on
   failure. If diagnostics itself fails, include that diagnostic error in the
   timeout assertion instead of masking the primary timeout.
7. Predicate, step, wait, and readiness exceptions propagate unchanged.
   Only the helper's own timeout has the dedicated subtype.
8. `wait_for_condition` delegates to the same loop, catches only that dedicated
   timeout, and preserves its existing `True`/`False` result. It gains the
   initial/final predicate semantics without swallowing callback exceptions.
   This intentionally means `timeout=0` now performs the initial predicate
   observation and may return `True`; no current caller supplies zero. Retain
   the existing `timeout=5.0`, `interval=0.1`, and `message=None` signature
   defaults, pass the effective interval explicitly, and keep `message`
   intentionally unobservable on the Boolean timeout path. `wait_for_value`
   and `wait_for_count` remain compatibility wrappers.

## Invariants and Constraints

- Product code under `simplebroker/` and extension packages must not change.
- No public CLI, library, backend, message-order, delivery, or timeout contract
  changes. Test timeout values remain owned by existing call sites.
- One implementation owns the polling/deadline state machine. Do not leave a
  second loop inside `wait_for_condition`.
- Deadline draining is readiness-gated and bounded to one final step. It must
  not claim unrelated work merely to make a test pass.
- The helper remains synchronous and test-only. No threads, signals, async
  loop, dependency, plugin, marker, registry, or persistent artifact is added.
- Diagnostics are observational and failure-only. They must not mutate broker
  or reactor state, consume queues, or downgrade callback failures.
- Existing exact state, transition-table, process-exit, and cleanup assertions
  remain the correctness oracles. `drive_until` supplies liveness only.
- Real watcher threads, reactor turns, SQLite sidecar state, and extension
  listener state remain real. Fake time is allowed only in the helper's direct
  unit contract tests.
- A migrated test must keep its original predicate and downstream assertions.
  Migration is not permission to raise timeouts or weaken expected state.
- Preserve specialized helpers whose interface carries more semantics than a
  Boolean predicate. The deletion test must show actual complexity removal.
- No policy gate is introduced. The final raw-loop scan is evidence for the
  migration boundary, not a zero-count acceptance criterion.

Fatal failures:

- missed final predicate or readiness-gated result at the deadline;
- blind or repeated post-deadline steps;
- timeout or callback exceptions being swallowed;
- a migrated ignored wait remaining non-firing;
- loss of real state-machine, process, or backend behavior;
- a new flake or hang in repeated targeted runs.

Best-effort only:

- diagnostic rendering beyond the required stable fields; a diagnostic
  callback failure is reported inside the timeout assertion;
- exact human wording after the caller message and stable diagnostic labels.

## Rollback, Sequencing, and Stop Gates

Rollback is file-local and fully reversible. Land the helper and its direct
tests before adopters. The compatibility wrapper keeps untouched callers
working, so any adoption slice can be reverted without restoring a second
polling implementation. No data, release, or one-way-door rollback exists.

Stop and revise the plan if:

- implementation needs product-code changes or a new dependency;
- one helper interface must consume typed process messages, return arbitrary
  captured values, own process cleanup, or create an async variant;
- a migrated test needs a longer timeout rather than the same evidence
  condition;
- a readiness probe is advisory in a way that makes one final step unsafe;
- callback duration must be forcibly bounded, which would require a watchdog
  execution context;
- a proposed AST/policy gate, runbook mandate, progress clock, or order DSL
  enters the change;
- PostgreSQL/Redis imports cannot use the existing `tests.helper_scripts`
  package under their actual wrapper commands;
- the direct red tests cannot distinguish current behavior from the proposed
  boundary behavior.

## Dependency-Ordered Tasks

### 1. Add failing contract tests for the helper state machine

- Files to add: `tests/test_timing_helpers.py`.
- Read first: `tests/helper_scripts/timing.py`,
  `tests/test_performance_harness.py`, and this plan's interface section.
- Use a deterministic fake monotonic clock and wait callback in the test
  module; do not sleep on the wall clock.
- Record the pre-change red result for tests covering:
  1. initially true predicate performs no step or wait;
  2. observation and driven success before deadline;
  3. predicate becoming true exactly as the final deadline observation occurs;
  4. ready-at-deadline causes one final step and succeeds;
  5. multiple ready drains still cause only one final step;
  6. no ready drain causes no post-deadline step;
  7. ready drain plus unsatisfied predicate performs one final step, then raises
     with message, elapsed/check/step counts, and diagnostics;
  8. diagnostic callback failure does not replace the timeout;
  9. predicate, step, wait, and drain exceptions propagate unchanged;
  10. drains without a step are rejected;
  11. Boolean `wait_for_condition` preserves `True`/`False`, shares the
      initial/final predicate behavior (including `timeout=0`), does not
      swallow predicate assertions, and retains the effective `interval=0.1`
      default plus a caller-supplied interval. Prove interval forwarding with
      the fake monotonic/sleep seam so the wrapper cannot silently inherit
      `drive_until`'s `interval=0.01` default.
- Done signal: the new nodes fail because `drive_until` or its required
  behavior is absent, and the red evidence is recorded in the execution log.

### 2. Implement and export the shared primitive

- Files to modify: `tests/helper_scripts/timing.py`,
  `tests/helper_scripts/__init__.py`.
- Keep one internal loop; a private timeout subtype may distinguish helper
  expiry from assertions raised by callbacks.
- Do not modify performance calibration or retry helpers except for import or
  formatting adjustments forced by the new code.
- Verify Task 1 green, then rerun `tests/test_performance_harness.py` to prove
  timeout-scaling helpers remain unchanged.
- Stop if the function grows process cleanup, arbitrary result capture,
  progress/stall state, or more than the declared callback roles.
- Done signal: all direct contract rows fire and the package export imports.

### 3. Convert ignored waits and exact pure-wait duplicates

- Files to modify:
  - `tests/test_watcher_burst_mode.py`
  - `tests/test_watcher_race_conditions.py`
  - `tests/test_watcher_transition_tables.py`
  - `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py`
  - `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py`
- Convert only the nodes listed in the migration table. Preserve predicates,
  timeout values, CI scaling, intervals, real actors, and exact assertions.
- Remove the two extension-local `_wait_until` definitions only after their
  three callers use the shared helper.
- Add failure diagnostics from already-owned state: watcher delay/drain
  histories, listener ready/error/closed state, and collected payloads. Do not
  read or consume a process queue for diagnostics.
- Red proof for ignored waits: temporarily make one selected predicate remain
  false and record that the migrated call now fails at its own wait boundary;
  revert the mutation before the green run. The direct timeout tests from Task
  1 are the permanent regression gate.
- Done signal: no expression-statement `wait_for_condition` call remains in
  the two named core modules; the two raw watcher transition waits and both
  extension `_wait_until` definitions are gone; targeted core/PG/Redis tests
  pass.

### 4. Pilot readiness-aware driven mode in the reference reactor

- File to modify: `examples/tests/test_reference_reactor.py`.
- Import the existing shared helper; do not move reactor behavior into test
  infrastructure.
- Apply only the migrations listed in the table. The transient-publish test is
  the sole driven pilot: `predicate` observes the output, `step` is
  `reactor.process_once`, `wait` is `reactor.wait_for_activity`, and `drains`
  contains the reactor-result and backlog readiness probes.
- `_wait_for_outputs` remains a small domain wrapper around observation mode
  because it centralizes queue-count diagnostics for background-reactor tests.
- Directly test the deadline-drain mechanism in Task 1; do not make the real
  reactor test scheduler-dependent by trying to force an exact wall-clock
  collision.
- Do not convert the expected-crash loop, control-reply value loop, negative
  blocking assertions, or 100 ms absence assertions.
- Done signal: the selected local loops are deleted, all existing downstream
  state assertions remain, and both reactor test modules pass repeatedly.

### 5. Reconcile evidence, review, and close only after implementation lands

- Run the final raw-loop and helper-call scan. Record remaining loops by
  semantic family; do not treat a nonzero count as failure.
- Evaluate whether this work exposed a new reusable correction. Update
  `docs/lessons.md` or `docs/agent-context/runbooks/testing-patterns.md` only
  if implementation produces evidence beyond the already-recorded readiness
  and synchronization rules. Such a material process change requires explicit
  reclassification to `Class 3+P` before editing guidance.
- Run independent review after the helper-core slice and again over completed
  work. Reviewers receive this plan, the helper contract tests, all changed
  callers, the state-machine map, and current verification evidence.
- Reconcile every finding in the Review Log. Rerun accepted-finding tests after
  revisions.
- Stage/commit only the explicit plan delta and implementation files when the
  owner authorizes landing. Verify the completion commit with `git log`.
- Flip this plan and its Status Index row to `completed` in the same change as
  the completion claim. Do not call uncommitted implementation complete.
- Done signal: final gates pass from the completion candidate, review is PASS,
  the plan/index close together, and `git log` identifies the landed work.

## Testing Plan

### Direct helper contract

- Harness: plain pytest against `tests/test_timing_helpers.py` with deterministic
  fake time.
- Mock only `tests.helper_scripts.timing.time.monotonic` or provide a test-owned
  wait callback that advances the fake clock. Do not mock adopted watcher,
  reactor, Queue, sidecar, or listener state machines.
- Every callback role and deadline branch in the proposed interface has a
  firing row in Task 1. Diagnostic prose is checked by stable fields/substrings,
  not one full frozen message.

### Adoption proof

- Core watcher tests run real watcher threads and retain transition/final-state
  assertions.
- Reference reactor tests use real `process_once`, worker result queues, Queue
  operations, and SQLite sidecar state. Readiness probes are real and advisory;
  the worker-result queue remains the source of truth inside reactor code.
- PostgreSQL and Redis transition tests keep their existing scripted transport
  adapters because those tests exercise listener state transitions, not live
  service integration. Their actual extension wrapper commands still run so
  import/marker/package topology is real.
- No test claims absence from elapsed time or ordering from broker timestamps
  as part of this plan.

### Failing-first record

- Task 1 records the missing-helper and deadline-edge red failures before code.
- Task 3 records one temporary false-predicate mutation proving an ignored wait
  becomes a failure. Revert it before green verification.
- Task 4 relies on the direct deterministic deadline-drain red test plus the
  real reactor integration test; forcing a scheduler collision in the real
  test would reintroduce the timing dependence being removed.

## Verification and Gates

Per-task commands:

```bash
uv run pytest -n0 -q tests/test_timing_helpers.py
uv run pytest -n0 -q tests/test_performance_harness.py
uv run pytest -n0 -q \
  tests/test_watcher_burst_mode.py \
  tests/test_watcher_race_conditions.py \
  tests/test_watcher_transition_tables.py
uv run ./bin/pytest-pg --fast -n0 -q \
  extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py
uv run ./bin/pytest-redis --fast -n0 -q \
  extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py
uv run pytest -n0 -q \
  examples/tests/test_reference_reactor.py \
  examples/tests/test_reference_reactor_transitions.py
```

Repeat the combined changed concurrency/example selection three times in
serial. Any timeout, hang, or divergent diagnostic is a failure to investigate,
not a reason to raise the timeout.

Final required commands:

```bash
uv run pytest
uv run pytest -n auto examples
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
uv run ruff check .
uv run ruff format --check simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
mapfile -t core_test_files < <(find tests -type f -name '*.py' \
  -not -path '*/__pycache__/*' | sort)
mapfile -t pg_test_files < <(find extensions/simplebroker_pg/tests -type f \
  -name '*.py' -not -path '*/__pycache__/*' | sort)
mapfile -t redis_test_files < <(find extensions/simplebroker_redis/tests \
  -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages \
  --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs \
  "${core_test_files[@]}"
uv run mypy extensions/simplebroker_pg/simplebroker_pg \
  "${pg_test_files[@]}" --config-file pyproject.toml
uv run mypy extensions/simplebroker_redis/simplebroker_redis \
  "${redis_test_files[@]}" --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Success means:

- helper contract tests fire every enumerated callback/deadline behavior;
- selected waits fail at their own boundary with actionable diagnostics;
- all retained state-machine and final-state assertions still pass;
- no specialized protocol was flattened into the generic helper;
- three serial targeted runs and all final suites are green;
- no product files, specs, README, CHANGELOG, workflow, or policy gates changed;
- final independent review is PASS and current-state verification is recorded.

Post-merge signal: core, examples, PostgreSQL, and Redis CI remain green with
no recurrence of locally patched deadline-loop flakes. There is no product
deployment signal because runtime behavior does not change.

## Independent Review Loop

Plan review before implementation:

- Preferred reviewer: Claude via `skills/call-agent/SKILL.md`, a different
  agent family from the author, using read-only review posture.
- Reviewer inputs: this plan verbatim; `docs/program-theory.md` [THEORY-2/4/5];
  DOM-5/10/11/15; testing-patterns; the completed test-suite signal plan;
  `tests/helper_scripts/timing.py`; both extension transition modules;
  `examples/reference_reactor.py`; `examples/tests/test_reference_reactor.py`;
  `tests/test_watcher_multiprocess.py` as the explicit non-migration control.
- Stance: existence-check every named file, node, command, callback order, and
  migration count first; then look for deadline races, swallowed exceptions,
  unsafe readiness assumptions, untestable diagnostics, missing red proofs,
  omitted rollback, performative generality, and migration overreach.
- Verdict: PASS/BLOCKED under the two DOM-11 questions. Every finding is
  reproduced and dispositioned below; accepted fixes receive scoped round-2
  verification.

Implementation reviews:

1. After Tasks 1–2, review only the helper contract, direct tests, and
   compatibility behavior before broad adoption.
2. Before completion, review the full diff, current verification evidence,
   migration/exclusion boundaries, and documentation impact.

## Review Log

Append-only after review begins.

| Date | Unit / reviewer | Verdict | Findings and disposition |
|------|-----------------|---------|--------------------------|
| 2026-08-11 | Plan draft / pending different-family review | pending | Dispatch after the draft and Status Index row pass local document gates. |
| 2026-08-11 | Full plan and Status Index diff / Claude Opus 4.8, read-only different-family review | PASS with one IMPORTANT correction | Existence checks confirmed every named file, symbol, count, migration node, and gate. IMPORTANT: the compatibility wrapper could accidentally inherit `drive_until`'s 10x faster interval default. Accepted: Decisions #2, contract #8, and Task 1 row 11 now pin the existing `interval=0.1` default and require a fake-clock forwarding test. OPTIONAL: accepted the explicit `timeout=0` semantic note and the fact that `message` remains cosmetic on the Boolean path; no change for site-specific diagnostics because failure-only, observational diagnostics are already bounded as best-effort. Reviewer answered both DOM-11 questions yes after the interval correction. Scoped round-2 review remains required for that correction. |
| 2026-08-11 | Interval-default correction / Claude Opus 4.8, scoped read-only round 2 | PASS | Confirmed Decision #2, contract #8, and Task 1 row 11 preserve and test the legacy `interval=0.1` default and caller-supplied intervals without contradicting the `drive_until` default. Confirmed the `timeout=0` and cosmetic-`message` notes match the initial/final predicate and dedicated-timeout contracts. No remaining findings. |
| 2026-08-11 | Helper-core implementation slice / Claude Opus 4.8, read-only different-family review | no blocker; no P1-P3 | Reproduced contract behavior and 18 direct tests. F1 nit accepted: remove the single-use `_validate_drive_args` wrapper and inline its two-line check; scoped round-2 verification required. F2 nit accepted as intentional final-check semantics: a step that crosses the deadline may be followed by the mandatory final predicate observation, with honest counts and no intervening side effect. F3 nit accepted as the reviewed compatibility contract: `message` remains intentionally unobservable on Boolean timeout. Observations about non-positive intervals and short-circuit drain probes remain out of scope; callers must supply positive intervals and observational readiness probes. |
| 2026-08-11 | Helper-core F1 correction / Claude Opus 4.8, scoped read-only round 2 | PASS | Verified the single-use validation wrapper is gone, the inlined guard still raises before predicate/time side effects, complexity remains below the Ruff limit, the deadline stages are unchanged, and no new defect was introduced. |
| 2026-08-11 | Completed implementation / Claude Opus 4.8, read-only different-family review | no blocker; no P1-P3 | Reproduced all 18 direct contracts, 17 interval-preserving migrations, extension import/loop removal, the sole reactor driven pilot, native/fallback polling-oracle correction, suppression-registry reconciliation, plan hygiene, and verification claims. F1 nit accepted: expand the public `drive_until` docstring with the initial check, aggregate deadline, final readiness-gated step/recheck, no-final-wait, and timeout-diagnostics summary; scoped round-2 verification required. Observation that a normal step may precede the one final readiness-gated step remains non-actionable because it is the explicit reviewed contract and reactor turns are idempotent. Reviewer answered yes to zero-context maintainability and no impairment to robustness/test signal. |
| 2026-08-11 | Completed-work F1 docstring correction / Claude Opus 4.8, scoped read-only round 2 | PASS | Verified the 4-line public contract summary accurately matches predicate-before-side-effects, the single monotonic deadline, optional step/check/wait turns, one readiness-gated final step/recheck with no wait, and the timeout subtype's counts/failure-only diagnostics. No defect introduced. |
| 2026-08-11 | Post-landing CI repair / independent same-family scoped review | blocker: F1 | The implementation correctly uses the locked stable history copy and matches `PollingStrategy`'s zero-delay burst / positive-delay backoff semantics. F1 accepted: the pure regression's single positive case would not reject `bool(delays)`, `any(delay > 0)`, an empty trace, an all-zero trace, or a stale positive followed by zero. Added negative empty, all-zero, and stale-positive cases plus the five-zero-to-positive transition. Scoped round-2 verification required before landing. |
| 2026-08-11 | Post-landing CI repair F1 / independent same-family scoped round 2 | PASS | Verified the strengthened regression proves empty false, all-zero false, a six-sample zero-to-positive transition true without the former 120-turn gate, and stale-positive-then-zero false. Targeted regression and live integration, Ruff, format, and diff checks passed; no new defect introduced. |

## Execution Log

Append-only during implementation.

| Date | Slice | Evidence | Result |
|------|-------|----------|--------|
| 2026-08-11 | Plan authoring | Existence-checked named helper, extension, watcher, reactor, wrapper, and gate paths at repository baseline | PASS; implementation not started |
| 2026-08-11 | Plan verification | `python3 bin/check-dom15-fixtures`; `bin/check-plan-context`; `bin/check-doc-paths`; `git diff --check`; trailing-whitespace scan; working-tree scope check | PASS; two intended uncommitted documentation paths only |
| 2026-08-11 | Owner activation and comprehension gate | User explicitly activated the reviewed plan. Final deadline step: only once when at least one declared drain reports ready, followed by one predicate recheck. Hard timeout: bounds helper polling/waits, not a blocking callback. Multiprocess watcher exclusion: those loops own typed consumptive protocols, phase state, child errors, and process diagnostics that a Boolean helper would hide. | PASS; implementation authorized and required reading answers match the plan |
| 2026-08-11 | Helper-core red/green slice | RED: `uv run pytest -n0 -q tests/test_timing_helpers.py` failed collection because `drive_until` was absent; subsequent focused reds proved observation, driven, deadline, drain, diagnostics, validation, and Boolean final-boundary behavior. GREEN: `uv run pytest -n0 -q tests/test_timing_helpers.py tests/test_performance_harness.py`; scoped Ruff, format, MyPy, and `git diff --check`. | PASS; 25 tests green, Ruff/format clean, MyPy clean for the three helper-core paths; independent slice review dispatched before adoption |
| 2026-08-11 | Core and extension adoption | Converted the 17 ignored Boolean waits, two raw watcher lifecycle loops, and three extension listener waits. Firing proof temporarily replaced the first burst-mode predicate with `lambda: False`; the exact test failed at `drive_until` after 2.000132s with predicate/step counts and polling diagnostics, then the mutation was reverted. The proof exposed an unbounded delay-history dump, which was corrected to count plus a 20-entry tail before green. Targeted core tests: 42 passed. PG transition wrapper: 28 passed. Redis transition wrapper: 74 passed. Scoped Ruff, format, and MyPy passed. | PASS; no expression-statement Boolean wait or extension `_wait_until` remains in the declared migration set; original timeouts/intervals and downstream state assertions are preserved |
| 2026-08-11 | Reference-reactor adoption and repetition | Converted `_wait_for_outputs`, both owner-publication loops, the sole transient-publication driven pilot, and the output-backlog observation loop. Kept the value-returning control reply, expected-crash, blocking, and absence loops explicit. `uv run pytest -n0 -q examples/tests/test_reference_reactor.py examples/tests/test_reference_reactor_transitions.py`: 60 passed. The combined helper/core-watcher/reactor selection ran three times serially: 120 passed on each run. Scoped Ruff and format passed. | PASS; real reactor turns, queues, worker results, sidecar state, and all downstream assertions remain active |
| 2026-08-11 | Remaining-loop reconciliation | Scanned expression-statement Boolean waits, local wait/drive helpers, and monotonic deadline loops across core, examples, PostgreSQL, and Redis tests. The only expression-form `wait_for_condition` match is the direct callback-exception propagation test. Remaining loops are typed multiprocess protocols; process/stream/exit/cleanup owners; value-returning control waits; expected-crash loops; or performance, contention, lock, and timestamp timing. | PASS; the nonzero remainder matches the plan's explicit semantic exclusions, so no policy or zero-count gate was added |
| 2026-08-11 | Full verification candidate | First `uv run pytest`: 2,559 passed / 17 skipped / one suppression-inventory failure; repaired by registering the required diagnostic catch under existing `[RUFF-SUP-008]` and updating its exact count test. First full PostgreSQL run: 1,091 passed / 5 skipped / one latent burst-oracle failure; diagnostics proved native waiters start on a positive idle schedule until notification, so the oracle was corrected without extending the timeout and passed on SQLite/PostgreSQL/Redis. Final: core 2,560 passed / 17 skipped; examples 119 passed; PostgreSQL core 1,092 passed / 5 skipped and extension 175 passed / 5 skipped; Redis core 1,085 passed / 12 skipped and extension 246 passed / 1 skipped. Ruff: clean; format: 316 files clean; MyPy: core 200, PostgreSQL 30, Redis 27 source files clean; suppression index, DOM-15 fixtures, plan context, doc paths, and diff hygiene: clean. | PASS; final independent review still required before handoff; implementation remains uncommitted |
| 2026-08-11 | Guidance and skill evaluation | The implementation reinforced existing readiness/synchronization guidance. The only new local correction was bounding watcher diagnostic history to a count plus head/tail; it is recorded here and embodied in the diagnostic helper, but does not justify a material runbook/skill rule or Class 3+P expansion. TDD vertical slices and call-agent review mechanics worked as documented. | PASS; no lessons, runbook, or skill edit proposed |
| 2026-08-11 | Final review correction and handoff candidate | Accepted the sole final-review nit by expanding the helper docstring. Current-state `tests/test_timing_helpers.py` plus `tests/test_performance_harness.py`: 25 passed; scoped Ruff, format, MyPy, and diff hygiene passed; different-family round 2 passed. | PASS; no review finding remains. Per owner-controlled landing rule, the plan/index stay in progress and the implementation stays uncommitted rather than making a false completion claim. |
| 2026-08-11 | Owner-authorized closure and landing | User directed “Close and commit” after the verified, independently reviewed handoff candidate. Closure gates rerun from the current tree; this plan and its Status Index row close in the implementation commit. | PASS; verify the landing commit with `git log` immediately after commit creation |
| 2026-08-11 | Post-landing macOS CI flake repair | `gh` job logs for jobs `93860421794` (Python 3.14) and `93860422397` (Python 3.13) showed the same `test_burst_mode_state_transitions` timeout: the watcher had left zero-delay burst polling and reached positive idle delays, but the migrated predicate still required 120 scheduler turns within two seconds. A pure trace regression failed under the count-based predicate, then passed when the predicate was reduced to the owned transition evidence: the latest stable delay copy is positive. The live integration test passed, the complete burst-mode module passed, and 200 serial repetitions passed without extending a timeout or changing product code. | PASS; the raising helper exposed a latent ignored-Boolean oracle defect. Release-wide verification and exact-SHA CI remain the release driver's responsibility. |

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| *(none; Source spec is explicitly N/A)* | | | | |
| `[DOM-10.1.1]` existing suppression inventory | No new policy gate or suppression machinery | Registered `_render_diagnostics` under existing `[RUFF-SUP-008]`, raised its approved/global `BLE001` cardinality by one, regenerated the derived index, and updated the exact repository directive-count assertion | Catching arbitrary ordinary diagnostic callback failures is required so diagnostics cannot mask the primary timeout. The full core suite fired the existing inventory gate; enumerating exception subclasses is unsound. This maintains an existing approved group and gate rather than adding or weakening policy. | None; existing `[RUFF-SUP-008]` already owns arbitrary timing-helper diagnostic containment |
| Migration invariant: preserve existing predicates | Keep every migrated evidence predicate unchanged | `test_burst_mode_state_transitions` now distinguishes the native waiter's positive initial idle schedule from fallback polling's initial zero-delay checks | The raising helper exposed a latent PostgreSQL silent pass: the discarded Boolean wait always timed out because a native waiter has zero activity-burst budget until `notify_activity()`. Diagnostics showed a positive bounded delay head, and `SM-POLLING::BURST_LIFECYCLE` already proves native burst begins after notification. The timeout was not raised; SQLite, PostgreSQL, and Redis now fire the backend-owned initial-schedule branch. | None; this corrects the test oracle to existing watcher behavior and keeps later real-activity burst assertions unchanged |
| Migration invariant: preserve existing predicates | Keep every migrated evidence predicate unchanged | `test_burst_mode_state_transitions` now recognizes idle backoff from a positive latest delay instead of requiring 120 recorded polls | The former Boolean result was discarded, so its scheduler-throughput gate could time out silently. Once activated by `drive_until`, two macOS jobs proved the watcher had entered the intended positive-delay state after only 102 or 110 turns. Increasing the timeout would retain the wrong contract; the test now asserts the synchronization state it owns from a locked history copy. | None; this is a test-oracle correction with no product or helper-contract change |

## Assumptions and Reopen Conditions

- Assumption: extension wrapper commands can import `tests.helper_scripts` as
  their transition modules already import `tests.helpers`. If a wrapper proves
  otherwise, stop; do not copy the helper back into each extension.
- Assumption: `process_once` and `wait_for_activity` are individually bounded
  enough for a synchronous test deadline. Reopen the interface design if a
  real callback can block indefinitely.
- Reconsider `progress` / `stall_timeout` only after a concrete SB test shows a
  hard timeout fails despite causally relevant monotone progress that can be
  named without observing unrelated work.
- Reconsider a policy gate only after a new equivalent local loop or repeated
  helper-bypass flake appears, and only if the prohibited deficiency can be
  recognized without banning barriers, specialized waiters, or typed process
  protocols.
- Reconsider a trace-order helper only after at least three tests share one
  append-only, causally frozen trace shape. Broker queue state and public
  message IDs do not satisfy that condition.

## Out of Scope

- Product code or behavior changes.
- Public API/CLI, backend, storage, message identity/order, delivery, README,
  CHANGELOG, release, or workflow changes.
- A generic event ledger, `assert_order`, current-state conservation DSL, or
  trace assertion framework.
- Stall/progress clocks, async helpers, watchdog threads, callback
  cancellation, or automatic CI timeout scaling.
- Repository-wide migration of every wall-clock loop.
- Replacement of managed subprocess, multiprocess watcher, queue-drain,
  process-exit, cleanup, performance, contention, or negative-blocking helpers.
- A marker, manifest, AST rule, allowlist, policy test, runbook mandate, or new
  dependency.
- Fixing the reference reactor's two time-based duplicate-absence claims
  without a separate causal-quiescence design.

## Fresh-Eyes Review Checklist

- Every named file, symbol, command, and migration node exists at the baseline.
- The interface defines callback order before, during, and after the deadline.
- Compatibility callers cannot have callback assertions swallowed.
- The final deadline turn is readiness-gated and occurs at most once.
- Callback blocking is named as outside the helper's enforcement capability.
- Direct tests, not scheduler luck, prove the deadline-edge behavior.
- Migration converts only liveness waits; safety and process protocols retain
  their stronger owners.
- No task smuggles in `assert_order`, progress clocks, a policy gate, or broad
  cleanup.
- Per-slice and final commands cover core, examples, PostgreSQL, Redis, lint,
  format, type checking, documentation gates, and current diff hygiene.
- Plan review and completed-work review have exact inputs and disposition
  rules.
