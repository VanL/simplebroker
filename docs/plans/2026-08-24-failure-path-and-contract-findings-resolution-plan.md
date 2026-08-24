# Failure-Path and Contract Findings Resolution Plan

Status: completed
Class: 5 — the plan changes the public watcher callback-failure contract,
clarifies public watcher context-manager and CLI parsing contracts, and crosses
the SQL-runner transaction boundary. The remaining items are contract tests,
bounded reliability fixes, implementation rationale, or private cleanup.
Plan type: implementation with spec revision

## Goal

Resolve the reviewed findings according to their evidence rather than their
original common-theme framing: make an error-handler crash terminate visibly,
retain intentional best-effort and CLI compatibility behavior while specifying
it precisely, add the missing `last_ts` and peek retry proofs, make SQL delete
transaction ownership explicit, clean only proven private vestiges, and record
why a claimed-row index/schema migration is not justified by the million-row
vacuum probe.

## Source Documents

Source specs:

- `docs/specs/10-cli.md` [SB-CLI-3], [SB-CLI-4]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-2],
  [SB-DELIVERY-4]
- `docs/specs/13-message-identity.md` [SB-ID-3]
- `docs/specs/16-python-library-api.md` [SB-API-4], [SB-API-6], [SB-API-11]
- `docs/specs/17-ops.md` [SB-OPS-3], [SB-OPS-6]

Theory:

- `docs/program-theory.md` [THEORY-1] requires a small, predictable surface
  that is explicit under failure.
- `docs/program-theory.md` [THEORY-4] requires truthful Unix composition,
  narrow safety claims, and concrete pressure before adding concepts or
  storage machinery.

Implementation and guidance:

- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/implementation/10-ruff-suppression-registry.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/writing-specs.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`

No active plan owns these changes. The status-review runner/reactor plan at
`docs/plans/2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`
records already-shipped work and does not authorize or overlap this delete
transaction correction.

## Resolution Inventory

| Finding | Resolution in this plan |
|---------|-------------------------|
| 4. `Queue.last_ts` suppresses lazy-fetch failure | Preserve behavior and [SB-ID-3]. Add a firing test proving lazy access returns `None` while explicit refresh propagates. |
| 5. watcher context exit suppresses stop failure | Preserve behavior. Clarify [SB-API-6] and strengthen primary-exception and retryable-cleanup tests. |
| 6. error-handler exception permits continued dispatch | Change behavior and specs: stop before another dispatch in consume, peek, and move mode, and propagate the error-handler exception independently of logging. |
| 7. claimed index / vacuum scans | Add no schema, index, or query change. Promote the deferred alternative below into implementation doc 09 with an objective reconsideration trigger. |
| 8. peek bypasses core retry | Route the existing peek operation through the existing bounded retry owner; add retryable and non-retryable wiring tests. |
| 9. `UPDATE_LAST_TS_ATOMIC` | Do not pretend the legacy equality-CAS string is the live monotone advance. Retain the historical private export, but label it as legacy and point maintainers to `SQLiteBackendPlugin.advance_last_ts()`. |
| 10. `commit_before_yield=False` | Remove the unused private argument and branch while preserving commit-before-return behavior. |
| 11. dash-leading write operands | Preserve behavior. Clarify [SB-CLI-3] and bind every ambiguous recognized token in the CLI contract suite. |
| `BrokerDB._conn` snapshot | Replace the captured connection with a delegating private property so compatibility access follows the runner's current thread/generation. |
| queue-existence duplication | No change. The report did not establish one owner with identical semantic and backend obligations. |
| queue/all delete transaction | Begin explicitly and roll back on ordinary failure, matching the neighboring exact-id and multi-queue SQL paths. |

Weft is a known downstream user of `BaseWatcher` and `peek_one()`. Its source
was inspected for blast radius. Per owner direction, this plan adds no Weft
compatibility shim, edits no Weft files, and does not make Weft adaptation a
SimpleBroker completion gate; the owner will handle that downstream work.

## Context and Key Files

### Public watcher failure and cleanup

- `simplebroker/watcher.py::_handle_handler_error()` currently treats
  `True`/`None` as continue and `False` as clean stop, propagates
  `StopWatching`/`StopException`, but catches any other error-handler
  `Exception`, optionally logs it, and returns. `_safe_call_handler()` then
  reports an ordinary handler failure, allowing the drain loop to continue.
- `BaseWatcher._run_with_retries()` retries generic watcher failures but treats
  `StopWatching` as a clean stop. A new terminal callback path must bypass both
  the clean-stop case and the generic retry loop without exposing a new public
  exception type.
- Consume and peek reach `_dispatch()` through `_try_dispatch_message()`. Move
  mode is a load-bearing exception: `QueueMoveWatcher._move_all_messages()`
  moves the row, increments `move_count`, and calls `_dispatch()` directly.
  Keep that direct path because `_try_dispatch_message()` also updates the
  watched source queue's timestamp cache. Create the private terminal signal
  below this split in `_handle_handler_error()`, and intercept it above the
  split in `_run_with_retries()` / `run_forever()` so all three modes stop once.
- `run_forever()` owns runtime cleanup in `finally`. Synchronous terminal
  failures can propagate after that cleanup. `run_in_thread()` uses a normal
  Python `threading.Thread`, so an uncaught terminal exception reaches
  `threading.excepthook` without depending on SimpleBroker logging.
- `BaseWatcher.__exit__()` calls `stop()` and suppresses ordinary cleanup
  exceptions. The lifecycle owner keeps failed cleanup retryable and leaves the
  finalizer attached; [SB-API-6] already governs that stop ownership.
- `tests/test_watcher_stop_contract.py` owns clean `StopWatching` behavior and
  stop/cleanup ownership. `tests/test_watcher_transition_tables.py` owns
  `SM-WATCHER-LIFECYCLE`; its existing `TERMINAL_ERROR` row means exhaustion of
  the generic retry budget. `SM-CLI-WATCH` separately has
  `CALLBACK_ERROR_CONTINUES`, where the CLI output callback fails and the
  configured error handler elects to continue. Neither row means that the
  error handler itself crashed. Add that distinct transition plus focused
  callback-failure coverage in `tests/test_watcher_error_handler_contract.py`
  rather than expanding the broad historical watcher file.

### Queue and SQL operations

- `simplebroker/sbqueue.py::Queue.last_ts` owns lazy-cache suppression;
  `refresh_last_ts()` is the strict fetch path.
- `simplebroker/db.py::_execute_peek_operation()` currently holds the core lock
  and calls the runner directly. Comparable core reads define an inner locked
  operation and pass it to `_run_with_retry()`.
- `BrokerCore.delete()` calls the backend plugin and commits, but unlike
  `delete_message_ids()` and `delete_from_queues()` it does not explicitly
  begin or roll back. The fix must reuse that neighboring transaction shape.
- `_execute_transactional_operation()` has only `commit_before_yield=True`
  callers. Its `finally` executes before return reaches the caller, so the dead
  false branch never implemented post-return commit.
- `simplebroker/_sql/sqlite.py::UPDATE_LAST_TS_ATOMIC` is a legacy exact CAS.
  The live monotone operation is
  `simplebroker/_backends/sqlite/plugin.py::SQLiteBackendPlugin.advance_last_ts()`.
- `BrokerDB.__init__()` snapshots `self._runner._conn`, while the runner's own
  `_conn` compatibility property resolves the current thread and connection
  generation. A `BrokerDB` property should delegate rather than snapshot.

### CLI and vacuum rationale

- `simplebroker/cli.py::ArgumentProcessor` derives its option sets from parser
  registration. `_protect_write_operands()` protects the first dash-leading
  message, preserves help, recognizes output flags in the documented regions,
  and honors an explicit `--` marker.
- `README.md` already teaches the write-output placement and literal-message
  escape. The canonical [SB-CLI-3] section does not yet own that rule.
- `BrokerCore._should_vacuum()` runs the claimed/total aggregate for automatic
  eligibility. The default long-lived schedule checks every 100 committed
  message mutations; eligibility fires at the configured ratio or above
  10,000 claimed rows. Explicit `broker --vacuum` is a one-shot administrative
  process. A partial claimed index does not remove the full claimed/total
  aggregate and would require migration/repair work on existing databases.

### Comprehension gates

Before editing, the implementer records answers in the execution log. An
incorrect answer blocks work until the cited paths are reread.

1. **Why must an error-handler crash use a distinct private control path?**
   Expected answer: raising `StopWatching` would misclassify failure as clean,
   while raising an ordinary exception directly would enter the generic watcher
   retry loop. The private carrier is unwrapped before generic retry so callers
   observe the original error-handler exception after cleanup.
2. **What broker state remains after the failing dispatch in each mode?**
   Expected answer: consume remains claimed, peek remains pending without
   progress past that id, and move remains in the destination with
   `move_count == 1`. Stopping the watcher cannot roll back those
   already-defined mode transitions.
3. **Why does the million-row scan not authorize an index migration?**
   Expected answer: healthy automatic maintenance normally fires much earlier
   for claimed rows; explicit CLI vacuum makes a one-time roughly 22 ms probe
   operationally insignificant; a partial claimed index does not optimize the
   main claimed/total aggregate; and migration cost on legacy databases is real.
4. **When is a write output option after the queue still an option?**
   Expected answer: after a non-dash literal message, after stdin marker `-`, or
   before an explicit `--` that introduces the literal message. Otherwise the
   first dash-leading token after the queue is message data, except unescaped
   `-h`/`--help`, which remains help.
5. **How is error-handler failure different from the existing watcher-machine
   terminal and CLI callback rows?** Expected answer: lifecycle
   `TERMINAL_ERROR` is generic retry exhaustion;
   `SM-CLI-WATCH.CALLBACK_ERROR_CONTINUES` is a failed message/output callback
   followed by an error handler that returns continue. The new row is only for
   the error handler itself raising, bypasses retry, and fails that watcher run.

## Invariants and Constraints

- A generic error-handler exception is fatal to that watcher run. No later
  message may be dispatched. In the ordinary one-message paths no next claim
  or move occurs; any state already materialized by an existing batch boundary
  remains governed by [SB-DELIVERY-1]/[SB-DELIVERY-2] and is not restored.
- Synchronous `run()`/`run_forever()` re-raise the original error-handler
  exception after runtime cleanup. Background execution leaves that same
  exception uncaught for Python's standard `threading.excepthook`. Logging may
  add diagnostics but is never the only signal.
- Preserve explicit clean stops: error-handler `False`, `StopWatching`, and
  internal `StopException` retain their current clean-stop meanings. Preserve
  `True` and `None` as continue.
- Preserve the original message-handler exception as the cause of the
  error-handler exception. Do not add a public exception class, terminal-error
  property, callback registry, or second background supervision mechanism.
- An ordinary runtime-cleanup failure while a terminal callback failure is
  unwinding must not replace the callback failure; retain it as ordered PEP 678
  note evidence and leave cleanup retryable. A `BaseException` interruption
  outside `Exception` keeps its existing propagation priority.
- Preserve [SB-DELIVERY-2] broker state for consume, peek, and move failures.
  In move mode the completed move remains counted: terminal callback failure
  after the first move leaves `move_count == 1` and the second source message
  unmoved.
- Context-manager exit remains best effort for ordinary stop/cleanup
  exceptions and never replaces an exception from the `with` body. It does not
  replay a background thread failure into the exiting thread.
- `Queue.last_ts` remains forgiving on first lazy fetch; explicit refresh
  remains strict. `None` must never mean an empty broker.
- Peek retries only exceptions already classified retryable by the shared
  policy. Non-retryable failure remains single-attempt; retry budgets and stop
  cancellation remain unchanged.
- SQL delete remains immediate and atomic per queue. Explicit transaction
  ownership must not add cross-queue atomicity to Redis or alter return values.
- Claim and move materialization still commit before results are handed to
  their callers. Removing the dead parameter must not alter generator delivery
  paths.
- CLI parsing does not change. In particular, `write q --cleanup` stores
  `--cleanup`; unescaped `write q --help` shows help; `--` remains the literal
  escape; and root actions after a command are never hoisted.
- Add no SQLite schema version, index, persistent counter, migration, or vacuum
  query change under this plan.
- Add no dependency and no parallel retry, watcher, parser, or transaction
  abstraction. Reuse the current owners.
- Do not edit Weft or weaken the SimpleBroker result solely to preserve Weft
  behavior. Do still preserve all first-party backend contracts.

## Rollback, Rollout, and Observation

There is no storage migration or one-way door. Keep the watcher contract/code
slice atomic so reverting it also reverts its normative text and tests. The
CLI clarification and vacuum alternative record are documentation of existing
behavior and can land independently. Peek retry, delete transaction ownership,
and private cleanup are separately revertible.

The watcher change is user-visible and must be recorded in `CHANGELOG.md`.
After release, success is: error-handler crashes stop without later dispatch,
the exception is visible even with logging disabled, ordinary clean-stop paths
remain quiet, and no new watcher cleanup leak appears. Peek success is a drop
in surfaced transient lock failures without increased retry exhaustion time.
No new telemetry is warranted; executable tests and ordinary exception/thread
reporting are the observation surfaces.

If implementing the watcher result requires a new public exception/property,
if cleanup masks the callback failure, or if a first-party backend cannot
honor the same failure order, stop and revise the spec delta. If delete needs
a backend API version bump or retrying commit becomes outcome-ambiguous, stop
and split that work into a separate plan.

## Spec Baseline

- `0901c7cd96e5` — `docs/specs/10-cli.md`, `docs/specs/11-delivery.md`,
  `docs/specs/13-message-identity.md`, `docs/specs/16-python-library-api.md`,
  and `docs/specs/17-ops.md` at plan authoring time.
- Execution rebased to `1b8ecfa0558b` after the independently reviewed CLI
  output/error-contract slice landed. That commit does not change [SB-CLI-3],
  [SB-API-6], [SB-DELIVERY-2], or [SB-ID-3]; its adjacent spec and test edits
  are the implementation baseline and must be preserved.
- Promotion baseline: `1b8ecfa0558b` plus the current worktree diffs in
  `docs/specs/10-cli.md`, `docs/specs/11-delivery.md`, and
  `docs/specs/16-python-library-api.md`; verification/backlink-only diffs in
  `docs/specs/13-message-identity.md` and `docs/specs/17-ops.md` accompany the
  same implementation. [SB-CLI-3] uses strategy D. The [SB-API-6] strategy-D
  context clarification and strategy-B callback-failure delta co-promote with
  [SB-DELIVERY-2] in one watcher slice.

## Proposed Spec Delta

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/10-cli.md` | D — clarification only | [SB-CLI-3], verification |
| `docs/specs/16-python-library-api.md` | D — clarification only, co-promoted with the watcher slice | [SB-API-6] context exit |
| `docs/specs/16-python-library-api.md` | B — atomic spec, code, tests, and backlinks | [SB-API-6] callback failure |
| `docs/specs/11-delivery.md` | B — atomic with the same watcher slice | [SB-DELIVERY-2], verification |

The two [SB-API-6] rows use different promotion strategies, but task 3 applies
them in one spec edit and one verification/backlink reconciliation. This avoids
promoting the same clause twice while keeping the existing-behavior
clarification distinct from the behavior change.

### [SB-CLI-3] — insert after the global-position rule

> `write` has a free-form message operand and therefore preserves a narrower
> option-position compatibility rule. Its output options (`-t`,
> `--timestamps`, and `--json`) are recognized before the queue name, after a
> non-dash literal message, after the stdin marker `-`, or before an explicit
> `--` whose following token supplies the literal message. Otherwise, the
> first dash-leading token after the queue name is message content even when it
> spells a recognized write or root option. Unescaped `-h` / `--help` remains
> the help request; place it after `--` to write it literally.
>
> `--` ends option interpretation. For example,
> `broker write -t tasks -- "-literal"` requests timestamp output and writes
> `-literal`. Root options and root actions appearing after a subcommand are
> never hoisted back to the root parser. These rules preserve dash-leading
> data and prevent a message such as `--cleanup` from becoming a destructive
> action.

Update [SB-CLI-3] verification to name the executable token table in
`tests/test_cli_contract_sb_cli.py` and the behavioral cases in
`tests/test_cli_write_output.py` / `tests/test_cli_rearrange_args.py`.

### [SB-API-6] — insert after `BaseWatcher.stop()` lifecycle ownership

> Error-handler outcomes have four meanings. Returning `True` or `None`
> continues watching. Returning `False`, raising `StopWatching`, or raising the
> internal `StopException` ends the watcher run cleanly. If the error handler
> raises any other ordinary `Exception`, that callback failure is terminal:
> the watcher dispatches no later message in that run, retains the original
> message-handler exception as its explicit cause, and re-raises the
> error-handler exception after runtime cleanup. Synchronous `run()` and
> `run_forever()` expose it to their caller; `run_in_thread()` leaves it
> uncaught for Python's standard `threading.excepthook`. This terminal signal
> does not depend on `BROKER_LOGGING_ENABLED`. An ordinary runtime-cleanup
> exception during that terminal unwind does not replace the callback failure;
> it is retained as an ordered PEP 678 exception note and cleanup remains
> retryable. A `BaseException` outside `Exception` may interrupt cleanup and
> propagates with its existing priority.
>
> `BaseWatcher.__exit__()` requests stop and join. An ordinary stop or cleanup
> exception during exit is best effort: it is suppressed, never replaces an
> exception from the `with` body, and leaves failed cleanup retryable under the
> lifecycle rules above. A `BaseException` outside `Exception` propagates.
> Context exit does not replay a background-thread failure into the exiting
> thread; background failures use `threading.excepthook` as described above.

Update [SB-API-6] verification with the new callback contract file and the
context-exit cases in `tests/test_watcher_stop_contract.py`.

### [SB-DELIVERY-2] — insert after the handler-failure state table

> The table describes broker state after message-handler failure. The
> error-handler continuation and terminal-failure rules are [SB-API-6]. If the
> error handler itself raises an ordinary exception, the watcher stops before
> another dispatch in every mode; it does not undo the already-committed
> consume claim or move, any state already materialized by an existing batch
> boundary, and it does not advance peek progress past the failed id.

Update [SB-DELIVERY-2] verification to cite the consume/peek/move terminal
callback matrix in `tests/test_watcher_error_handler_contract.py`.

## Durable Alternative Record

### [ALT-RF20260824-001] Add a claimed-row index from the million-row vacuum probe

Disposition: deferred
Owner: SimpleBroker product owner
Governs: `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
Source record: none
Candidate: Add a SQLite partial index on claimed messages, a schema migration or
current-version repair, and a no-claimed fast path for vacuum eligibility.
Why plausible: Claimed existence and batch deletion currently scan without a
claimed-row index, and a synthetic table with one million pending rows produced
roughly 15–22 ms scans while core maintenance holds its process-local lock.
Evidence:
- contemporaneous: `simplebroker/db.py::_record_maintenance_activity()` and
  `_should_vacuum()` run synchronous eligibility checks after scheduled
  committed activity.
- contemporaneous: `simplebroker/_maintenance.py::vacuum_is_eligible()` and
  `docs/specs/17-ops.md` [SB-OPS-6] fire at the ratio threshold or above 10,000
  claimed rows.
- contemporaneous: `simplebroker/_sql/sqlite.py::GET_VACUUM_STATS` must still
  aggregate total rows, so the proposed partial index does not remove its main
  O(N) decision scan.
- owner-recalled: the 2026-08-24 findings review judged a one-time roughly
  22 ms explicit CLI-vacuum scan insignificant beside process setup and an
  operator-requested maintenance action.
Reason: The probe models a tail-sized mostly-pending table, not ordinary
maintained work-queue state. Healthy automatic vacuum fires far earlier for
claimed rows; explicit CLI vacuum is not a hot path; and a new index imposes
schema migration, repair, write, and legacy-database costs without realistic
end-to-end evidence of user harm.
Current consequence: Add no index, schema version, counter, fast path, or vacuum
query change. Promote this rationale to implementation doc 09 during the first
documentation slice.
Reconsider when: A production trace or reproducible end-to-end benchmark under
the default automatic-vacuum policy attributes a user-visible latency or
throughput regression in a documented supported workload to these scans; the
product contract is expanded to recommend million-row steady-state tables; or
explicit CLI vacuum misses a documented operator latency objective because of
these scans. The isolated million-row microbenchmark alone is not a trigger.
If one of these conditions fires, reopen the alternative and then measure index
creation, repair, and write costs on representative legacy database sizes
before adoption. A fired trigger reopens evaluation; it does not adopt the
index.
Promoted to: [ALT-IMPL09-001]

## Tasks

1. **Independently review and approve the exact plan and spec delta before
   implementation.**
   - Read first: this plan, [THEORY-1], [THEORY-4], all source specs above,
     `simplebroker/watcher.py`, `simplebroker/db.py`, `simplebroker/cli.py`, and
     the named tests.
   - Reviewer stance: challenge whether generic error-handler failure should be
     terminal, whether exception chaining and background observability are
     implementable without a new public API, whether the CLI wording exactly
     matches the parser, and whether the plan retains speculative cleanup.
   - Done signal: every finding is appended to the Review Log below and either
     changes the plan or has a written rejection. A reviewer who cannot
     implement confidently blocks the next slice.

2. **Promote the CLI clarification and vacuum decision.**
   - Files to touch: `docs/specs/10-cli.md`,
     `docs/implementation/09-storage-schema-and-claim-lifecycle.md`,
     `tests/test_cli_contract_sb_cli.py`, `tests/test_cli_write_output.py`,
     `tests/test_cli_rearrange_args.py`, this plan, and related-plan backlinks.
   - Apply the strategy-D CLI text exactly unless a recorded deviation is
     approved.
   - Add a CLI token table that fires at least `-t`, `--timestamps`, `--json`,
     `--cleanup`, `-h`, `--help`, and `--`, proving option versus literal
     meaning in each documented position. Do not duplicate parser option lists
     in production; the test is the enumerable contract gate.
   - Promote [ALT-RF20260824-001] to the implementation document under the next
     available `ALT-IMPL09-*` identifier with the same disposition, evidence,
     current consequence, and reconsideration trigger. Add reciprocal
     `Source record` / `Promoted to` cues before closing this plan.
   - Record the strategy-D promotion baseline and run the documentation and
     focused CLI tests.
   - Stop if the token table contradicts current code; that is a behavior
     decision, not a clarification to rewrite silently.

3. **Make generic error-handler failure terminal and observable.**
   - Files to touch: `simplebroker/watcher.py`, `docs/specs/16-python-library-api.md`,
     `docs/specs/11-delivery.md`, `docs/guides/python.md`, `CHANGELOG.md`,
     `tests/test_python_library_api_contract_sb_api.py`,
     `tests/test_watcher_error_handler_contract.py`,
     `tests/test_watcher_stop_contract.py`,
     `tests/test_watcher_transition_tables.py`,
     `docs/implementation/07-complexity-and-state-machine-map.md`, and spec
     verification/backlink sections.
   - Red first: add real-SQLite consume, peek, and move tests with two source
     messages through the ordinary one-message boundaries. The first message
     handler and its error handler fail. Assert one dispatch only, the exact
     [SB-DELIVERY-2] residual state, synchronous propagation of the
     error-handler exception, explicit chaining to the message-handler
     exception, and identical behavior with logging disabled. In move mode,
     explicitly assert that the first row is in the destination,
     `move_count == 1`, and the second row remains in the source. Add a
     batch-mode case if inspection shows state can be materialized before
     dispatch; it must prove no later dispatch without promising restoration
     of that state.
   - Add a real background-thread case that captures the standard
     `threading.excepthook`, proves the terminal exception is reported once,
     and proves the thread and runtime resources terminate. Mock only the host
     hook used to collect the standard signal.
   - Add an `ERROR_HANDLER_FAILURE` row to `SM-WATCHER-LIFECYCLE`: from a
     running dispatch, an error handler raises; the next state is failed; the
     action is to bypass generic retry, clean runtime resources, and propagate
     once. Keep the existing `TERMINAL_ERROR` row unchanged as generic retry
     exhaustion. Keep `SM-CLI-WATCH.CALLBACK_ERROR_CONTINUES` unchanged because
     its error handler returns continue; add a cross-table assertion or comment
     that prevents later conflation of the two callback paths.
   - Implement one private terminal carrier created only around a generic
     error-handler exception in `_handle_handler_error()`. Both
     `_try_dispatch_message()` and move mode's direct `_dispatch()` call must
     propagate it unchanged. Catch and re-raise the carrier before the generic
     exception branch in `_run_with_retries()`; do not reroute move through
     `_try_dispatch_message()`. Set terminal stop state, retain the carrier
     through `run_forever()` cleanup, and then re-raise the original
     error-handler exception from the original message-handler exception. If
     ordinary runtime cleanup also fails, add that failure as ordered PEP 678
     note evidence and leave the lifecycle retryable. Do not expose the carrier
     from `simplebroker` or `simplebroker.ext`.
   - Add context-exit tests for a clean body plus stop failure, a body exception
     plus stop failure, preservation of the body exception, later cleanup
     retry, and `BaseException` propagation. Fault injection may target
     `stop()`/cleanup; do not mock lifecycle ownership or the background thread
     for the retry proof.
   - Preserve and rerun clean `False`, `StopWatching`, `StopException`,
     `True`, and `None` tests.
   - Co-promote the strategy-D [SB-API-6] context-exit clarification with the
     strategy-B [SB-API-6]/[SB-DELIVERY-2] text, code, tests, mappings, guide,
     changelog, and reciprocal links atomically; record one promotion baseline.
   - Stop if the exception is retried, cleanup masks it, background reporting
     requires a new supervisor, or any mode dispatches a second message. Add a
     double-failure test proving an ordinary cleanup exception is secondary to
     the callback failure.

4. **Complete the missing `Queue.last_ts` failure proof without changing
   behavior.**
   - Files to touch: `tests/test_queue_api_comprehensive.py` and the [SB-ID-3]
     verification row in `docs/specs/13-message-identity.md`.
   - Inject failure at the narrow connection/high-water fetch boundary. Assert
     a fresh handle's lazy `last_ts` returns `None`; assert `refresh_last_ts()`
     propagates the same failure; assert an empty real target still returns
     `0`.
   - A protocol stub is allowed for the injected backend failure. Keep a real
     SQLite target for the empty-origin distinction; do not mock the property
     itself.
   - Done signal: [SB-ID-3]'s `None` versus `0` distinction has direct firing
     evidence and production code is unchanged.

5. **Route peek through the existing bounded retry policy.**
   - Files to touch: `simplebroker/db.py` and
     `tests/test_retry_policy_coverage.py`.
   - Red first with a runner wrapper around real SQLite: inject one explicitly
     retryable `OperationalError` at the peek query and then delegate to the
     real runner; public `peek_one()` and `peek_many()` must succeed. Inject an
     explicitly non-retryable error and prove one attempt and propagation.
   - Define a local locked peek operation and pass it to `_run_with_retry()`.
     Do not add a peek-specific retry loop or broaden which failures retry.
   - Preserve exact-id, bounds, `include_claimed`, result materialization, and
     stop-event behavior through existing tests.
   - Run the shared SQL-backed tests through SQLite and PostgreSQL. Redis uses
     its direct core and must not be forced through this SQL helper.

6. **Make single-queue SQL delete transaction ownership explicit.**
   - Files to touch: `simplebroker/db.py`,
     `tests/test_custom_runner_integration.py`, and
     `docs/implementation/09-storage-schema-and-claim-lifecycle.md`.
   - Red first with a recording non-autocommit SQL runner/plugin seam: prove
     `begin_immediate()` precedes plugin deletion, success commits once,
     statement/plugin failure rolls back once, and the original failure
     propagates. Add a real SQLite durable-state assertion.
   - Reuse the `delete_message_ids()` / `delete_from_queues()` transaction
     shape. Do not change Redis delete orchestration or claim stronger
     cross-queue atomicity.
   - Treat commit failure through the runner's existing transaction/error
     policy. Stop if the change would retry an outcome-ambiguous commit or
     require a backend API version bump.

7. **Remove or repair only the proven private vestiges.**
   - Files to touch: `simplebroker/db.py`, `simplebroker/_sql/sqlite.py`,
     `tests/test_sql_internals.py`, and
     `tests/test_custom_runner_integration.py` for `BrokerDB._conn`.
   - Remove `commit_before_yield` from `_retrieve()` and
     `_execute_transactional_operation()`, delete the unreachable branch, and
     commit every non-empty materialized claim/move result before return.
     Preserve no-result rollback and exception rollback.
   - Keep `UPDATE_LAST_TS_ATOMIC` and its historical re-export, but correct its
     comment to call it the legacy exact-CAS string and point at the live
     monotone plugin operation. Do not add a test for unused SQL text.
   - Replace the `BrokerDB.__init__()` `_conn` snapshot with a private property
     delegating to `SQLiteRunner._conn`. Prove it follows thread-local access
     and returns a live replacement after runner close/reopen.
   - Do not unify queue-existence methods. Do not introduce a generic
     transaction helper solely to remove repeated `try/commit/rollback` code.
   - Stop if any removal reaches a package-root or `simplebroker.ext` export.

8. **Reconcile traceability and run final gates.**
   - Update every touched spec verification row and `## Related Plans` section,
     implementation docs 07/09, `docs/guides/python.md`, and `CHANGELOG.md`.
   - Inspect the final diff against every resolution-inventory row. An item is
     complete only with its named code/doc/test evidence or explicit no-change
     disposition.
   - Run the final commands below, obtain an independent implementation review,
     answer every finding in the Review Log, record any deviation, and change
     the status-index row to `completed` only when all gates and requested
     implementation are complete.

## Testing Plan

The core proofs use real SQLite storage, real watcher threads, and real runner
transactions. Narrow fault-injection wrappers may introduce callback,
retryable runner, commit, or cleanup failures, but must delegate all unaffected
work to the real owner. Do not mock `Queue`, watcher drain mode, SQLite durable
state, transaction ordering, lifecycle cleanup ownership, or CLI parsing.

Targeted task commands:

```bash
uv run pytest tests/test_cli_contract_sb_cli.py tests/test_cli_write_output.py tests/test_cli_rearrange_args.py -q
uv run pytest tests/test_python_library_api_contract_sb_api.py tests/test_watcher_stop_contract.py tests/test_watcher_error_handler_contract.py tests/test_watcher_transition_tables.py -q
uv run pytest tests/test_queue_api_comprehensive.py tests/test_message_identity_contract_sb_id.py -q
uv run pytest tests/test_retry_policy_coverage.py tests/test_sql_internals.py tests/test_custom_runner_integration.py tests/test_operations_contract_sb_ops.py -q
```

Cross-backend and final gates:

```bash
uv run pytest
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
uv run ruff check .
uv run ruff format --check .
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
mapfile -t sb_core_test_files < <(rg --files tests -g '*.py' -g '!tests/typecheck_fixtures/**' | sort)
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs "${sb_core_test_files[@]}"
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
bin/coalesce-check
git diff --check
```

If full extension services are unavailable, that is residual verification risk,
not a silent pass. Record the exact skipped command and require the normal CI
backend jobs before release. Per owner direction, no Weft test command is part
of this plan.

## Interface Review

Scope and baseline: CLI write argument normalization and the Python watcher
callback/lifecycle surface at `0901c7cd96e5`.

| Principle | Disposition and evidence |
|-----------|--------------------------|
| 1. Context is the scarcest resource | Met: CLI success output remains compact; watcher failure reuses the raised exception rather than adding a new state payload (`simplebroker/watcher.py:531-575`, `996-1070`). |
| 2. Progressive disclosure | Met: parser help remains first-line teaching and the Python guide owns usage detail (`simplebroker/cli.py:231-510`; `docs/guides/python.md` watcher section). |
| 3. Self-explanatory names | Met: `StopWatching`, `error_handler`, `run_forever`, and write output option names are direct; the plan adds no lookup vocabulary. |
| 4. One identity per thing | Not applicable: neither delta creates or changes object identity. |
| 5. Derive what is derivable | Met: CLI preparse metadata remains derived from parser registration (`simplebroker/cli.py:146-228`); no duplicate production flag table is added. |
| 6. No hidden session setup | Met: CLI interpretation is determined by the full argv; watcher behavior is determined by the watcher and callbacks supplied to that run. |
| 7. Teach, don't reject | Met with deliberate boundary: dash-leading data is protected and `--` teaches the escape; help remains help (`simplebroker/cli.py:754-840`; `README.md:301-310`). |
| 8. Every message carries its action | Met by task 3: a logging-disabled error-handler crash propagates after cleanup, including move mode's direct `_dispatch()` seam, and a background run reaches `threading.excepthook`. |
| 9. Atomic writes with recovery | Met for this scope: message/delivery state transitions remain atomic and are not redesigned; delete transaction ownership is made explicit under [SB-OPS-3]. |
| 10. Draw the trust boundary | Met: root actions remain root-position only; user callbacks own application failure, while SimpleBroker owns stopping and surfacing watcher failure. |
| 11. Wire format matches the mental model | Met: free-form message tokens remain data unless the documented position makes them options; watcher callers receive their callback exception, not a storage-shaped wrapper. |

Enumerable interface elements covered by executable gates: write output flags
`-t`, `--timestamps`, `--json`; help tokens `-h`, `--help`; the `--` marker;
error-handler outcomes `True`, `None`, `False`, clean-stop exceptions, and
generic terminal exception.

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-F1 | P1 | `simplebroker/watcher.py:552-563`, `1962-1970` | Generic error-handler failure is swallowed and may permit later dispatch; move mode uses the direct dispatch seam, and logging can be disabled. | Promote [SB-API-6]/[SB-DELIVERY-2] and implement the common carrier plus mode matrix in task 3 before completion. |
| IR-F2 | P2 | `docs/specs/10-cli.md:104`; `README.md:301-310` | Canonical CLI contract omits the free-form write operand rule already taught and implemented. | Strategy-D clarification plus token-table gate in task 2. |
| IR-F3 | P2 | `simplebroker/watcher.py:1105-1116`; `docs/specs/16-python-library-api.md:240` | Public context-manager cleanup suppression exists but its primary-exception and retry semantics are not normative. | Co-promote the strategy-D [SB-API-6] clarification and lifecycle tests with the task 3 watcher slice. |

Ratified judgments (challenged, upheld): keep dash-leading write operands
literal; keep context exit best effort; keep clean `StopWatching` distinct from
terminal callback failure; expose no new public watcher error API; add no
vacuum index or schema migration; add no Weft compatibility shim.

Preimplementation verdict: IR-F1 blocked until the atomic watcher slice landed;
IR-F2 and IR-F3 blocked contract-complete status. Round 3 verified all three
resolutions in the implemented diff and reported no remaining blocker.

Runbook feedback: no new agent-interface principle candidate. This review
applies the existing explicit-failure and enumerable-contract gates.

## Independent Review Loop

Use a different agent family from the plan author where available. The reviewer
must first existence-check every named path, symbol, spec section, test owner,
and command, then read this plan's `## Proposed Spec Delta`, interface review,
and durable alternative record against the current code.

Review prompt:

> Review the current worktree plan and its exact proposed spec delta against
> baseline code and specs at `0901c7cd96e5`. Check every named surface against
> code. Challenge the terminal watcher
> failure design, exception order, mode-state assertions, context-exit policy,
> CLI grammar wording, retry and delete transaction seams, private cleanup, and
> the no-index reconsideration trigger. Look for bad ideas, missing rollback,
> tests that mock away the contract, and performative overengineering. Do not
> implement. Could a zero-context engineer implement this correctly and
> confidently? Return blockers first and recommend removal as readily as
> additions.

The author records each finding below with one of: incorporated, rejected with
reason, or out of scope with reason. Any change to invariants, authority,
public behavior, or blast radius requires review of that delta again. Run a
second independent diff review after implementation and before completion.

## Review Log

| Round | Finding | Disposition | Plan change or evidence |
|-------|---------|-------------|-------------------------|
| 1 | P1: move mode bypasses `_try_dispatch_message()` and leaves `move_count` unspecified. | incorporated | The context, invariants, task 3, delivery matrix, and interface review now name the direct `_dispatch()` seam, retain it deliberately, and require `move_count == 1` with the second row unmoved. |
| 1 | P1: `ERROR_HANDLER_FAILURE` overlaps existing transition vocabulary. | incorporated | Task 3 now defines the new row precisely, preserves lifecycle `TERMINAL_ERROR` as retry exhaustion, and preserves CLI `CALLBACK_ERROR_CONTINUES` for an error handler that elects to continue. |
| 1 | P2: the review prompt implied that this worktree-only plan existed in the baseline commit. | incorporated | The prompt now asks reviewers to compare the current plan against baseline code and specs. |
| 1 | P2: two [SB-API-6] promotion slices create needless churn. | incorporated | The strategy-D context clarification now co-promotes with the atomic strategy-B watcher slice in task 3. |
| 1 | P2: `tests/test_runner_lifecycle.py` did not own either planned assertion. | incorporated | The task and targeted gate now use `tests/test_custom_runner_integration.py`; the unrelated lifecycle test was removed from the command. |
| 1 | P2: the vacuum trigger used invented performance thresholds and fixed legacy sizes. | incorporated | The trigger now requires a documented supported-workload regression, an expanded million-row use contract, or a missed documented CLI objective. Migration and write costs are measured only after reopening, on representative databases. |
| 1 | P3: remove the `UPDATE_LAST_TS_ATOMIC` annotation cleanup from scope. | rejected | The original finding specifically identifies the private constant's current “atomic” comment as misleading. Correcting that single annotation, while retaining the symbol and adding no test or behavior, is the bounded documentation resolution; dropping it would leave the reviewed defect in place. |
| 1 | P3: task 6 did not choose the delete transaction test owner. | incorporated | Task 6 now names `tests/test_custom_runner_integration.py` directly. |
| 1 | P3: IR-F1 evidence omitted move mode's direct dispatch. | incorporated | IR-F1 and principle 8 now cite both the swallowing seam and `QueueMoveWatcher._move_all_messages()`. |
| 2 | Prior P1 blockers and the revised spec/test/vacuum structure. | approved | The reviewer found no blocker and judged the revised plan implementable by a zero-context engineer. The move seam and state-machine vocabulary are now explicit. |
| 2 | P3: task 5 retained a discretionary retry-test owner. | incorporated | Task 5 now names `tests/test_retry_policy_coverage.py` without an alternative owner. |
| 3 | Implemented diff, including all watcher modes, CLI grammar, `last_ts`, retry/delete transaction proofs, private cleanup, spec promotion, and the durable vacuum decision. | approved | The independent implementation reviewer found no blocker and answered yes to the zero-context readiness question. |
| 3 | P3: only the consume-mode watcher test explicitly disabled broker logging. | incorporated | The peek and move matrix cases now also set `BROKER_LOGGING_ENABLED` false, proving exception propagation rather than log visibility in every delivery mode. |
| 3 | P3: there is no direct synthetic commit-failure test for physical delete. | rejected | This slice does not add commit retry semantics. The real-runner tests prove the new begin/delete/commit order and statement-failure rollback with durable state preserved. Injecting an indeterminate commit result would not establish a portable safe retry contract and is outside [SB-OPS-3]. |
| 3 | P3: cleanup-secondary proof does not inject signal-context restoration failure. | rejected | Retry-bookkept resource cleanup is owned by `_cleanup_runtime_resources()` and is covered directly. Signal restoration remains a separate existing context boundary with no retained resource owner or retry surface; changing it is outside the watcher callback-failure contract. |

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Execution Evidence

- RED/GREEN proofs were observed for the newly promoted CLI, watcher,
  `last_ts`, peek retry, delete transaction, live `_conn`, and dead-branch
  contracts. Targeted suites passed after implementation.
- `uv run pytest` passed with `2919 passed, 17 skipped` on the final code and
  documentation state before this execution-log-only edit.
- `uv run ./bin/pytest-pg --fast` reached `1273 passed, 5 skipped` and
  `uv run ./bin/pytest-redis --fast` reached `1266 passed, 12 skipped`. Each
  stopped on the same execution-baseline test,
  `test_sb_cli_4_caller_path_failures_are_invalid_arguments`, because its
  SQLite-specific `-f`/`-d` path-failure cases return success under the network
  backend fixtures. That test and behavior came from baseline `1b8ecfa0558b`;
  this plan does not alter [SB-CLI-4] target selection. The failure is therefore
  recorded for its owning active CLI remediation plan rather than expanded
  into this implementation. Both scoped reruns passed through their shared and
  extension stages with that exact test deselected:
  `uv run ./bin/pytest-pg --fast -k 'not
  test_sb_cli_4_caller_path_failures_are_invalid_arguments' -q` and the
  equivalent `pytest-redis` command.
- Production and core-test mypy gates passed (`63` and `206` source files,
  respectively). `ruff check`, `ruff format --check`, DOM-15 fixtures, plan
  context, documentation paths, coalescing cues, and `git diff --check` also
  passed. The coalescing check reported `22` SHA claims (`4` foreign), one
  retrieval cue, and no unresolved local cue.
- The owner authorized a targeted closing commit on 2026-08-24. This plan,
  its promoted contracts, implementation, and firing tests land atomically in
  that commit; Weft remains outside the authorized scope.

## Assumptions and Open Questions

- Assumption: standard `threading.excepthook` is sufficient background failure
  observability; no new join-and-reraise API is needed. Owner: SimpleBroker.
  Reopen if independent review shows context-manager use can still lose the
  only terminal signal or Python's hook cannot receive the original exception.
- Assumption: the delete fix stays within existing SQL runner transaction
  semantics and backend API v7. Owner: implementation reviewer. Reopen on any
  first-party SQL backend that cannot begin, commit, and roll back this single
  plugin call through the existing runner contract.
- Downstream ownership: the repository owner will handle any Weft adaptation to
  the watcher behavior change. It is outside this plan and does not block
  SimpleBroker by explicit direction.

## Out of Scope

- Any claimed-message index, SQLite schema version, maintenance counter,
  vacuum threshold, batch size, or automatic-vacuum redesign.
- Queue-existence helper consolidation.
- A new public watcher terminal-error object, future/promise, supervisor, or
  thread subclass.
- Changing handler-failure delivery semantics or restoring claimed/moved
  messages.
- Changing CLI parsing or making recognized output flags after the queue win
  over literal data.
- Weft edits, compatibility adapters, or downstream verification.
- General SQL-runner or transaction-helper refactoring.
- Removing the historical `simplebroker._sql` package or its remaining private
  compatibility exports.
- Release publication or version selection.

## Completion Gate

- Every resolution-inventory row has the named implementation, documentation,
  test evidence, or explicit no-change result.
- The spec delta was independently reviewed, promoted with recorded baseline
  identifiers, and reconciled with code and reciprocal links.
- [ALT-RF20260824-001] is promoted to implementation doc 09 with reciprocal
  source cues and the exact reconsideration trigger; no vacuum/schema code
  changed.
- The watcher error-handler matrix proves terminal, observable failure for
  consume, peek, and move while every clean outcome remains intact.
- `last_ts`, peek retry, delete transaction, dead branch, and live `_conn`
  proofs pass at their real owners.
- Final core, PostgreSQL, Redis, static, documentation, traceability, and diff
  gates pass or any unavailable external service is explicitly left to the
  normal CI release gate.
- Independent implementation review findings are incorporated or answered.
- Documentation and `CHANGELOG.md` match the delivered behavior; no durable
  lesson or runbook improvement remains unassessed.
- The completed implementation is committed when the owner authorizes a
  commit, verified by `git log`; otherwise report the exact uncommitted files
  and do not call the work complete.
- The Status Index row changes from `active` to `completed` in the same change
  as the completion claim.
