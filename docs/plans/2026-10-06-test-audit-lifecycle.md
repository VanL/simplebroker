# Test audit: lifecycle, concurrency and examples

Date: 2026-10-06. Status: audit evidence complete; remediation not authorized.
Parent: [whole test-surface audit](2026-10-06-whole-test-surface-audit-plan.md).
Baseline and checked HEAD: `d4d2634a9587409b06ece9ce593eb4c780f5da1b`.

## Source Documents

- `docs/program-theory.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/specs/11-delivery.md`
- `docs/specs/16-python-library-api.md`
- `docs/implementation/06-process-session-core-ownership.md`

The test-audit skill was read in full. Exact test/support owners and focused
verification boundaries are recorded below; citations do not replace reading.

## Scope and method

All 50 assigned files were read in full, including parameter inputs, helper
bodies, and every transition-table row: 33,221 lines and 733 static test
declarations. Counts below are declarations, not collected parametrized cases.
The test-audit skill was used in audit mode. No test, product, dependency,
runner-concurrency, or timing allowance was changed. No deletion is authorized
by this report. Findings distinguish independent behavioral evidence from
setup scripts, descriptive table metadata, and pass-through instrumentation.

Production tracing included `DBConnection.release_connection_after_use`, its
operation stack and `_ProcessBrokerSession` release/cache/disposal ownership;
`BrokerCore.claim_generator`; watcher startup, waiter handoff, stop, polling,
pending precheck, drain and retry-timeout owners; advisory-lock acquisition and
phase publication; SQLite v3 migration; and reference-reactor dispatch,
single-owner drive, wait, signal setup, stop and durable control/output paths.
The config snapshot rule is [SB-API-2] in
`docs/specs/16-python-library-api.md` (transactional-generator snapshot event);
iterator and SQL poison rules are [SB-DELIVERY-4/5/6]. Ownership rationale is
`docs/implementation/06-process-session-core-ownership.md`, particularly
explicit caller-thread release and operation acquisition/disposal balance.
`docs/program-theory.md` was used for product-scope judgment: a small queue model
does not make native persistence or resource-lifetime proofs redundant.

### Verification performed

The following six focused baseline tests ran with `uv run --locked pytest ...
--no-cov -n 2`: reentrant release, generator snapshot, both v3 index-before-begin
tests, watcher metadata distinction, and phaselock literal version. Result:
**6 passed in 0.34s**. This is not a full-suite or cross-platform result.

Two isolated, process-local negative controls ran through `uv run --locked
python -c`, imported the existing test functions, and used temporary database
directories. They changed no files:

1. Replaced the production `DBConnection.release_connection_after_use` method
   with an unconditional `AssertionError`, then invoked the reentrant-release
   test. The test still passed because it overwrites the method itself.
2. Wrapped `BrokerCore.claim_generator` to discard the supplied `config`, then
   invoked the generator snapshot test. The test still passed.

Both controls exited zero and printed their false-green result. No timing,
signal, Windows, PostgreSQL, Redis, or weft failure reproduction was attempted.
Source-supported failure modes below are not claims that a production defect
or a particular historical CI crash has been reproduced.

## Per-file coverage accounting

Every row is a full-file read. A finding does not invalidate the remaining
contracts in that file.

| File | Declarations | Retained independent contracts / audit disposition |
| --- | ---: | --- |
| `examples/tests/test_multi_queue_pattern_transitions.py` | 2 | Real example priority rounds, 3:1 order, short high lane, temporary-handler restoration, stop-before-low; monitoring success/error accounting, current-queue clearing, report threshold and controlled slow duration. Subclasses call real nested drain/dispatch owners. |
| `examples/tests/test_recommended_python_examples.py` | 3 | Async wrapper oldest/newest public-ID selection, explicit early stream close leaving later rows pending, runnable Python example output plus persisted queue state. |
| `examples/tests/test_reference_reactor.py` | 34 | Queue-role rejection before files, owned-ID JSON formatting, single drive owner, worker wake, retained control/checkpoint restart, late lower IDs, terminal seen ledger, paging, crash/replay, exact-ID collision including claimed occupants, route drift, bounded backlog, error outputs, per-source order. L10/L11 and O4. |
| `examples/tests/test_reference_reactor_transitions.py` | 3 | Real scheduling/control/output tables; durable output failure vs sidecar failure; backlog-before-input, control bypass, absorbing terminal state, allocated ID outside transaction. Real signal row must remain isolated: L9. |
| `tests/test_activity_waiter_api.py` | 14 | Public waiter API exports/types, backend factory forwarding and cache grouping, stop/config ownership, cache close/replacement and failure handling. Factory mocks are outside Queue responsibility. |
| `tests/test_activity_waiter_replacement.py` | 1 | Native waiter renewal and displaced-resource ownership through Queue/watcher replacement. Distinct replacement topology. |
| `tests/test_broker_session.py` | 34 | Public handle queue/connection reuse, scope ownership, close admission, active-operation rejection, body-vs-cleanup exception priority, interrupted close, lease-only foreign finalizer, real fork rejection under held lock. Retain lock/counter observations where they expose resource balance. |
| `tests/test_connection_config.py` | 23 | Retained Config across lazy connection use, per-call overrides, limits/logging, actual batch commit/rollback footprints for claim/move, ambient exclusion. One false-green snapshot test: L2. |
| `tests/test_connection_transition_tables.py` | 3 | Real DBConnection, process-session and transactional delivery transition rows, including creation failure and deferred release. Worker rows fire identical topology: L3. |
| `tests/test_cross_thread_finalization_poisoning.py` | 22 | Real SQLite foreign next/throw/close/GC, no foreign runner mutation, monotonic first poison, owner/post-publication/preblocked diagnosis, nonretryability, nested sidecar/generator exception priority. Process isolation cleanup hole: L7. |
| `tests/test_cross_thread_generator_probe.py` | 1 | Bounded subprocess reproduction of cross-thread finalization with native integrity and probe-result validation. Diagnostic stress is not equivalent to the deterministic poison tests. |
| `tests/test_cross_thread_probe_transitions.py` | 3 | Probe report and lifecycle protocol transitions plus opt-in diagnostic failure distinctions. Helper protocol is a legitimate explicit subject, not an additional product proof. |
| `tests/test_db_connection_lifecycle.py` | 9 | Real runner ownership, injected vs owned close, persistent/ephemeral acquisition, concurrent creation and shutdown/resource cleanup. |
| `tests/test_default_handlers.py` | 13 | Print/JSON output and exact newline framing, logged handler errors and continue result, integration values. Ambient-dependency claim lacks an adverse input: O1. |
| `tests/test_fork_safety.py` | 10 | Real inherited core/Queue rejection, new child operation, registry abandonment, all inherited connections abandoned without close, held runner/session locks and pre-lock recovery. Several older child waits unbounded: L8. |
| `tests/test_phaselock.py` | 64 | Native advisory locking, phase durability/failure/cancel, concurrent first setup, marker observation, fallback/xattr failure, Windows byte locking, Darwin ABI, permissions, source independence and no descriptor/process-lock leaks. Literal metadata freeze L13; ordinal failure seam L6. |
| `tests/test_phaselock_transition_tables.py` | 2 | Real phase state rows, marker-while-waiting native contention, fallback publication, Darwin cached discovery/concurrent first init/ERANGE and real fork lock reset. Native variants are not generic table duplicates. |
| `tests/test_process_broker_session.py` | 75 | Session key isolation, real same-target sharing, runner lease/factory lifetime, per-thread cache, explicit recycle, bounded close, pending core creation, operation/disposal BaseException balance and late timeout behavior. Fake-only reentrancy L1. |
| `tests/test_queue_connection_manager.py` | 12 | Public get_connection handle identities plus committed rows, close raw-handle unusability, worker resource cleanup, body-error recovery and mixed modes. Thread/future watchdog issues included in L8. |
| `tests/test_retry.py` | 29 | Real retry engine attempts/backoff, stop, bounds, jitter, callback errors, overflow protection, stdlib-only vendoring, inherited hot-loop guard. Controlled operation/sleep seams preserve retry owner. |
| `tests/test_retry_policy_coverage.py` | 38 | Public peek reaches bounded retry; explicit permanent/retryable classification, setup progress refresh/expiry with controlled clock, elapsed/stop policies; capacity budget, mixed failures, clipped sleep/deadline, jitter floor/cap and >1024 attempts. |
| `tests/test_runner_error_handling.py` | 32 | Real SQLite exception translation, cursor finalization, close timeout preparation, owner commit/rollback admission, failed rollback invalidation, foreign close, setup budget/restoration/marker barriers and native process serialization. Minimal core only isolates setup-budget owner from unrelated validation. |
| `tests/test_runner_lifecycle.py` | 6 | Unopened/lazy and repeated shutdown, connection generation cleanup and usable replacement. |
| `tests/test_runner_validation.py` | 16 | Stop before connection/schema mutation, invalid/empty/new files, stale status rejection without overwrite, failed bootstrap rollback before phase mark, transient write retries/progress/bounds and passive completion check. |
| `tests/test_sqlite_admission.py` | 18 | Foreign magic before writes including live WAL, stored version/malformed/newer rejection, legacy migration, atomic cookie/proof read under external DDL, one repair owner, read-only scalar fast path and replacement invalidation. Real schema/byte oracles retained. |
| `tests/test_sqlite_connect_example.py` | 1 | Copyable connection example on native SQLite, WAL/sidecar-safe behavior distinct from product validator ownership. |
| `tests/test_sqlite_lifecycle.py` | 1 | Native opened-file lifecycle/integrity and resource warning behavior, not merely connection mock state. |
| `tests/test_sqlite_message_id_returning_order.py` | 5 | Real SQLite returning rows deliberately reordered by pass-through dependency; claim/move batches and generators normalize by public ID. Strong external-order fault seam. |
| `tests/test_sqlite_schema.py` | 41 | Literal old-layout fixtures, real migrations and atomic failure rollback, durable versions, caller sidecar preservation, unsupported attachments, pragma restoration, canonical shape, unique-index equivalence/conflicts, native query plans and write-free reopen. One exact duplicate L14; common-builder inspection O5. |
| `tests/test_sqlite_setup_contention.py` | 3 | Native concurrent first writers, temporary locked startup, mixed old/new lock paths idempotence and exact conservation. Temporary-lock retry claim lacks retry witness: L12. |
| `tests/test_thread_safety.py` | 4 | Thread-affine per-thread raw connections and real concurrent broker operations; shared runner native distinction retained. |
| `tests/test_transaction_error_propagation.py` | 2 | Transaction context propagates original body error and rolls back; real owner seam rather than swallowing exception. |
| `tests/test_vacuum_compact.py` | 15 | Claimed-row vacuum counts, pending preservation, native compaction file-size outcome, thresholds and maintenance behavior. |
| `tests/test_vacuum_lock.py` | 4 | Native advisory-lock contention/release and POSIX ownership semantics; OS skips represent unavailable substrate, not weakened application contract. |
| `tests/test_validation_lock_safety.py` | 3 | Validator on active WAL, excludes unintended checkpoint/lock mutation, preserves messages and sidecar state. Different owner from connection example. |
| `tests/test_watcher.py` | 66 | Public watcher start/run/stop/context, peek/consume/move/after, dispatch errors, native/fallback strategy ownership, local latch and bounded native deadline, replacement, data-version core renewal. L4, O2/O3. |
| `tests/test_watcher_burst_mode.py` | 9 | Owner cadence/burst bookkeeping, real write wakes, gradual backoff and native-vs-fallback hints. Idle no-reset proof can pass without postwrite observation: L5. |
| `tests/test_watcher_cleanup.py` | 15 | Runtime and TLS resource cleanup, owned/supplied Queue difference, idle stop and live-run cleanup owner, repeated/concurrent stop and failure paths. |
| `tests/test_watcher_concurrency.py` | 9 | Shared queue rival consumers, exact once/no loss, peek/consume mix, concurrent writes, real startup stop skips initial drain. Runtime protection is not just timing throughput. |
| `tests/test_watcher_edge_cases.py` | 26 | Input types, real configured size/logging, handler stop/control behavior, bounded retries and cleanup exception priority, signal/non-main thread, move failures. Accelerated real-clock timeout O3. |
| `tests/test_watcher_error_handler_contract.py` | 7 | Handler-vs-error-handler failure/control semantics, original exception/cause identity, no generic retry after terminal callback, stop before another claim and message-state preservation. |
| `tests/test_watcher_multiprocess.py` | 5 | Real process watcher delivery/load/shutdown and exact child results. Targeted write vs idle children lacks complete postwrite decision witness: L5. |
| `tests/test_watcher_multiprocess_transitions.py` | 1 | Child protocol readiness/work/error/stop/stats across process boundaries. Do not delete as if it were a second watcher implementation. |
| `tests/test_watcher_race_conditions.py` | 11 | Stale positive precheck plus rival drain/later work, native hints still query, real concurrent writers/queues and contention. Concurrent query overlap measurement mislabels request barrier: O4. |
| `tests/test_watcher_sigint_probe_transitions.py` | 1 | Actual CLI child interrupt lifecycle and helper protocol transitions. Bootstrap retries belong to shared helper review; they are not automatically redundant signal coverage. |
| `tests/test_watcher_stop_contract.py` | 21 | Stop-before-start/drain/read, batch advance boundaries, generator close and waiter ownership, retry interruption and startup failure handoff. Ordinal stop seam L6. |
| `tests/test_watcher_thundering_herd.py` | 5 | Real multi-watcher same database/queue targeting, per-queue cheap idle check, native vs fallback behavior and delivery instrumentation. Thread idle witness shares L5 issue. |
| `tests/test_watcher_transition_tables.py` | 7 | Polling latch coalescence, waiter ownership, real watcher lifecycle retry/control failure, real CLI newline/interrupt/broken pipe plus construction/output-only seams. Prose-only metadata assertion L15; failure cleanup L4. |
| `tests/test_weft_sqlite_stop_corruption_regression.py` | 1 | Optional real downstream task startup/cancel/process exit with repeated native integrity checks. Keep real cross-repo topology; cleanup gap L7. Dependency availability is explicit skip, not proof of passing. |
| `tests/test_write_visibility.py` | 3 | Real shared-runner and multiprocess writer/checkpoint-reader visibility and independent conservation, plus transaction settlement ordering. Stress and ordering proof protect different regressions. |

## Actionable findings

Priority is remediation order, not measured production impact. F = fix proof;
C = consolidate while preserving its witness; D = delete only the identified
redundant/process-only assertion after independent preservation review.

### L1. Reentrant release test bypasses the complete production subject (P2, F)

`tests/test_process_broker_session.py:1918`,
`test_failure_release_argument_is_reentrant_without_tls_marker`, assigns a
test function directly to `connection.release_connection_after_use` and then
recursively calls that function. It proves Python argument passing in its own
fake, not production reentrant release or session-depth balance. Its no-TLS
marker assertion also passes when the real method never runs. The isolated
negative control disabled the production method and the test passed.

Owner: `simplebroker/db.py:1209` pops the exact operation-session stack entry
then calls `_ProcessBrokerSession.release_current_thread_connection`;
`simplebroker/_broker_session.py:443` delegates to real `_end_operation`.
Fix by acquiring real nested operations and observing/injecting at the session
release dependency, delegating the real release. Assert original failure
identity, stack/depth balance, no leaked lease and a usable subsequent
operation. Validate this test fails when failure forwarding or one stack pop
is disabled. Retain existing surrounding exception-priority/deferred-cleanup
tests: they exercise distinct ordinary/handled failure states.

### L2. Generator snapshot test has no sensitive snapshot oracle (P2, F/C)

`tests/test_connection_config.py:149`,
`test_generator_retains_explicit_config_on_first_iteration`, mutates `supplied`
after that dict was copied into a resolved Config. The generator only ever
receives the Config. One seeded row and one `next()` return the same result for
every positive batch size. The isolated control discarded `config` entirely
and the test still passed.

Owner: `simplebroker/db.py:2779` resolves the per-call Config on first
at-least-once advancement and chooses a batch limit. The spec requires retaining
that Config, not resnapshotting the already-detached caller dict. Existing
keepers at `:346`, `:379`, and `:417` assert committed/pending claim/move batch
footprints and explicit override. Consolidate any distinct first-iteration
timing claim into a proof that observes config consumption before/after first
advance and tests enough rows plus close/rollback to distinguish batch sizes.
Do not delete the timing requirement merely because override coverage exists.
Focused validation: run these four tests and verify a drop-config or
re-resolve-on-next-batch fault fails the keeper chosen for the relevant claim.

### L3. Two process-session table rows fire the same topology (P3, C/F)

`tests/test_connection_transition_tables.py:303` and `:311` declare
`WORKER_RETAIN_NON_LAST_USER` and `WORKER_CLOSE_RETAINS_CACHE`. Both call
`_assert_worker_user_transition` at `:365`: two worker managers sharing a core,
close first, then close second, with one anchor retained. The different table
labels do not create different starting configurations.

Owner: process-session close drops a registry lease; explicit thread cleanup
owns cache disposal. Merge into one row that explicitly witnesses both closes,
or make the second row actually use one worker manager. The existing
`tests/test_process_broker_session.py:847`,
`test_worker_queue_close_retains_cache_until_explicit_cleanup_or_session_end`,
uses single worker Queue leases and a live anchor, checks raw handles remain
usable without explicit cleanup, then verifies terminal close. That is a
substantive keeper for single-worker cache retention, not proof that the two
table descriptions already differ. Validate both table IDs still have a real
witness or intentionally become one ID with contract registry updated.

### L4. Actor cleanup is not guaranteed after assertion failure (P2, F)

`tests/test_watcher.py:902`, `TestQueueWatcher.test_run_forever_blocking`,
starts a non-daemon thread. Its finally only joins; stop happens only on the
success path. A startup-timeout or earlier assertion can leave an indefinitely
live run. `tests/test_watcher_transition_tables.py:565`,
`test_watcher_lifecycle_fires_transition_table` general START/delivery/repeated
stop branch likewise starts then waits/asserts without a finally stop. Its
`_assert_stop_races_start` at `:519` waits for cleanup readiness before entering
the finally that releases the gated stop thread.

Owner: `BaseWatcher.stop` deliberately requires an explicit stop and may return
after a bounded join without claiming another run's cleanup. These tests must
own release/stop/join, even when their assertions fail. Put finally immediately
after actor start; release any gate, request stop, boundedly join and assert
termination. Capture worker exceptions as primary evidence rather than leaving
later teardown warnings. Validation: force readiness assertion failure and
prove no test-created actor survives; then run affected tests under xdist.
Do not serialize watcher tests or enlarge deadlines as the fix.

### L5. Idle watcher assertions can pass before the relevant decision (P2, F)

`tests/test_watcher_burst_mode.py:199`,
`test_burst_mode_no_reset_on_empty_wake`, sleeps 0.2s after a targeted write and
only asserts idle cadence when its new history is nonempty. No idle observation
means no idle assertion. `tests/test_watcher_multiprocess.py:521`,
`test_multiprocess_unrelated_write_does_not_drain_idle_watchers`, stops all idle
children as soon as the active child delivers. Each ready message is based on
`delivery_calls` incremented *before* `super().read_many` (`:36`), not completed
startup drain (`watcher_process`, `:114`). No proof any idle child sees and
finishes a postwrite precheck. A broken wake-to-drain rule can remain unexecuted.
The threaded `tests/test_watcher_thundering_herd.py:121`,
`test_unrelated_write_does_not_drain_idle_watchers`, also observes pending-query
entry; that entry alone is not a completed no-drain decision.

Owner: `simplebroker/watcher.py:873` waits, checks pending, skips drain when
false; `:1818` has hint/precheck consumption; `:1843` notifies/resets bursts only
after useful work. Require a completed initial drain and an acknowledged
postwrite idle *decision*, then inspect no-drain and cadence. A pass-through
observer at the decision boundary is appropriate; a replacement loop is not.
Keep native, threaded and process topologies. Validate a fault that always
drains/reset bursts after unrelated wake fails, including when idle scheduling
is deliberately delayed. No longer sleeps or permissive conditional assertion.

### L6. Stop/failure injection depends on callback ordinal (P2, F)

`tests/test_watcher_stop_contract.py:769`,
`test_stop_during_waiter_handoff_leaves_queue_as_owner`, raises at the fourth
`_check_stop` call. `tests/test_phaselock.py:711`,
`test_acquisition_callback_failure_releases_process_lock`, raises on callback
two or three; `:1886`, `test_advisory_lock_stops_after_lock_attempt_failure`,
returns true on call three. Adding a valid earlier check can move the injection
to a different stage, fail on harmless refactoring, or avoid the advertised
handoff/failed-flock condition.

Owners: watcher startup creates waiter, checks stop, starts strategy, detaches
Queue ownership; advisory acquire takes process lock, prepares file, tries
native lock, then retries. Trigger at a witnessed semantic seam: created waiter
still Queue-owned, acquired process lock, or recorded failed `_try_lock`.
Preserve real cleanup and dependent native-lock behavior. Validate added earlier
stop checks do not break tests, but premature ownership transfer / leaked
process lock does. Start-failure waiter test and file-preparation failure test
remain separate regression proofs.

### L7. Process cleanup falls outside the failure finally (P2, F)

`tests/test_cross_thread_finalization_poisoning.py:210`,
`_run_queue_close_mode_probe`, closes its Pipe in finally, but joins/terminates
the spawned child only after that finally. A failed poll/recv assertion skips
child cleanup. Its consumers are
`test_foreign_wrapper_queue_close_return_modes_are_process_isolated` (`:410`).
`tests/test_weft_sqlite_stop_corruption_regression.py:145` launches a consumer,
but only kills after the happy stop/cancel path reaches `_wait_for_exit`; its
finally only closes inboxes. An earlier startup/cancel/integrity assertion can
leave the downstream task process running.

Production ownership is deliberately terminal/process-isolated in the first
test and downstream task-owned in the second. Register launched actors
immediately and always boundedly join, then terminate/kill/reap on failure,
before temporary-directory cleanup. Keep integrity checks and skip semantics.
Validate missing readiness/report and failed cancel path leave no child. Weft
validation requires an available compatible downstream; it was not run here.

### L8. Older concurrency probes can hang the worker on the bug they detect (P2, F)

`tests/test_fork_safety.py:108`, `test_fork_safety_protection`, `:151`,
`test_new_instance_after_fork_works`, and `:256`,
`test_forked_child_guarded_methods_raise`, use blocking `waitpid` without a
parent deadline/kill finally. The public inherited Queue timestamp probe at
`:286` has blocking pipe read then waitpid. At `:368`,
`test_fork_fallback_abandons_without_close`, the second connection thread join
and child Pipe recv/wait are unbounded. A fork-lock regression is precisely a
credible hang, not an unlikely unrelated dependency failure.

The same containment issue exists in
`tests/test_queue_connection_manager.py:140` (unbounded joins) and `:177`
(unbounded barrier; ThreadPoolExecutor context shutdown can wait forever even
after future timeout). Existing real held-lock fork tests in the same fork file
show how to bound and reap without replacing the actual fork topology.
Use those cleanup patterns, explicit bounded readiness, abortable barriers and
owned actor finally blocks. A process boundary is required when a deliberately
deadlocked thread cannot be stopped. Validate deliberate nonresponding child
or failed participant terminates the probe and gives a useful assertion.

### L9. Real SIGTERM test can kill the enclosing pytest worker (P1, F)

`examples/tests/test_reference_reactor_transitions.py:677`,
`_fire_deferred_signal_transition`, records readiness wait as a boolean, then
unconditionally sends `os.kill(os.getpid(), SIGTERM)` even if readiness is false.
It joins its sender only after `reactor.run_forever` returns normally. If startup
raises and the reactor restores handlers, or readiness times out before the
owned handler is installed, a still-live sender can send SIGTERM to pytest with
its ordinary disposition. This turns a targeted regression into a worker crash
or contaminates a later test. That failure path was source-traced, not executed.

Owner: `examples/reference_reactor.py:353` installs signal context before
running and restores it in finally; shutdown itself remains owner-thread work.
Retain a real OS-signal proof but isolate it in a bounded managed subprocess.
Readiness failure must never send the signal; cancellation/join/reap belong in
finally. Validate success plus injected pre-ready startup failure, proving the
outer pytest worker survives and the child reports its actual error. Do not
replace this native signal proof entirely with direct handler invocation.

### L10. Reactor stop tests infer blocking from sleep (P2, F)

`examples/tests/test_reference_reactor.py:530`,
`test_stop_during_startup_waits_for_drive_thread_before_closing`, and `:561`,
`test_stop_waits_for_manual_drive_thread_before_closing_queues`, start a stopper,
sleep 0.05s, and assert it is alive. Being alive can mean it was never scheduled,
not that stop reached join without closing queues. The owner is already gated
in real strategy start/publish, which is valuable. Both tests also release gates
only on success, outside a finally.

Owner: `BaseReactor.stop` joins another drive owner before closing resources.
Observe the stopper entering the real join/ownership boundary and assert raw
queue/resources remain usable while that join is held, then release, await
completion and verify close. Always release gates and join both actors in
finally. Validate an eager-close-before-join fault fails, even when stopper
scheduling is delayed. Keep startup and mid-publish topologies distinct.

### L11. Reactor cross-queue concurrency relies on a 20ms overlap (P2, F)

`examples/tests/test_reference_reactor.py:1550`,
`test_per_queue_single_inflight_preserves_source_order`, uses `sleep(0.02)` in
each processor to hope two distinct sources overlap. Healthy worker scheduling
can run them sequentially and fail `cross_queue_overlap.is_set()`. The
single-source overlap guard and source-ID order remain useful.

Owner: reference reactor excludes another live item from the same queue
(`_queue_ready_for_dispatch`, `:942`) but submits different queue work to its
worker pool. Gate the first two distinct-source processors with an explicit
bounded rendezvous, preserving the active-source guard and finally release.
Validate deterministic cross-source overlap under CPU contention and a
same-source concurrent-admission fault. Do not raise the processor sleep.

### L12. Temporary startup lock test does not witness a retry (P2, F)

`tests/test_sqlite_setup_contention.py:138`,
`test_first_write_retries_during_temporary_setup_lock`, waits for the writer's
file touched *before* Queue creation, sleeps one second, then releases the lock.
A delayed child can start its actual open after release and pass. Success
therefore proves eventual write after temporary lock, not that setup attempted
or retried while contended.

Owner: real SQLite setup admission plus converted-error retry/phase owner.
Report a pass-through observation of an actual contention failure/retry before
parent release, then let the real write complete and verify exact persisted
message. Keep subprocess/native file lock topology. Validate retry disabled
fails this test and a delayed startup cannot make it pass without the witness.

### L13. Phaselock version literal is a routine-update tripwire (P3, D)

`tests/test_phaselock.py:101`, `test_version_is_1_0`, requires the module's
metadata string to remain exactly `1.0`. The module is intentionally copyable
and exports its version, but no traced product contract or caller consumes
that exact number. Native lock and phase behavior do not change merely because
the standalone version increments. This is the same class of brittle process
assertion as the motivating build dependency check.

Owner: `_phaselock.py:30` metadata; behavior lives in its locking/phase code.
Recommend deletion of this exact-value assertion, subject to preservation
review. No runtime behavior needs a replacement. If standalone metadata format
is actually a declared distribution contract, test that format/availability,
not a historical literal. Focused validation: native phase/acquisition tests
still cover behavior after a process-local metadata-only version change.
Uncertainty: an external consumer requiring `1.0` was not exhaustively searched;
no such rule was found in the repository.

### L14. Exact duplicate v3 index-before-begin proof (P3, D/C)

Delete/consolidate `tests/test_sqlite_schema.py:1255`,
`test_ensure_schema_v3_repair_handles_index_created_before_write_lock`, only
after keeping `:1192`, `test_ensure_schema_v3_handles_index_created_after_preflight`.
Both create v1 layout, ensure v2, use the same
`_CreateTsIndexBeforeFirstBegin` real-competitor wrapper, invoke v3 at version 2,
and check version callback `[3]` plus unique index. The keeper additionally
checks transaction exit. Same owner, substrate, precondition, injected event and
oracle; only path/name differ. There is no second repair starting state.

Owner: `schema.py:284` acquires the transaction before inspecting index state.
Both focused tests passed. Validate keeper against a fault that ignores or
misclassifies the competitor-created index, then retain keeper and remove only
the redundant declaration. Do not remove equivalent-name/conflicting-name or
failed-commit migration tests: they start from different schema states.

### L15. Test locks descriptive table prose instead of behavior (P3, D)

`tests/test_watcher_transition_tables.py:690`,
`test_error_handler_failure_and_cli_continue_rows_remain_distinct`, asserts
four authored `event`/`next_state` strings in local table metadata. Neither
production watcher nor command runs. Editing truthful prose breaks it; changing
runtime behavior does not. The table firing function is the correct proof.

Keep `ERROR_HANDLER_FAILURE` in
`test_watcher_lifecycle_fires_transition_table` (`:565`, helper `:438`) and
`CALLBACK_ERROR_CONTINUES` in `test_cli_watch_fires_transition_table` (`:705`).
They exercise distinct owners: watcher callback-failure exception identity,
cause and no retry; real command child output callback failure then broken pipe
with both attempts and exit zero. Those protect the actual distinction. Delete
only the metadata prose assertion after independently checking the firing
rows. Focused validation: both rows fail under their respective callback policy
fault; harmless metadata prose edits do not need a product test.

## Observations and uncertainty kept out of deletion recommendations

O1: `tests/test_default_handlers.py:151` claims config independence but only
supplies ordinary ValueError and asserts True/log output, also covered by the
normal log test. The real `default_error_handler` (`watcher.py:192`) currently
does not resolve config. Poison the config resolver/ambient input if that
independence claim is important; otherwise fold this ordinary example into the
existing output cases. Without a sensitivity control, do not treat its name as
an extra proof or claim a product dependency bug.

O2: `tests/test_watcher.py:1165` calls its after-filter proof database filtering,
but only verifies delivered messages. Filtering in Python would also pass.
Likewise `:2028` verifies real write/delivery with a data-version provider but
does not alone prove provider-based wake rather than fallback poll. Narrow
names/claims or add an independent query/wake witness only where that mechanism
is a promised performance requirement. Existing data-version callback/replaced
core tests are substantive; preserve real after/batch message coverage.

O3: `tests/test_watcher.py:1620` fixes the exact config resolver call list
`[{"env": {}}]` on reload. Ambient-free constructor defaults and their types are
a legitimate contract, covered by poisoned-env subprocess test `:1558`; exact
one-call/module resolution is narrower implementation coupling. Both child
process runs lack a timeout. `tests/test_watcher_edge_cases.py:619` multiplies
real elapsed time by 100 to test retry timeout, retains real sleeps, and freezes
the default `300s` text. Use a controlled clock/sleep seam for budget crossing,
and isolate default-value catalog checks from policy behavior. No measured
flake of these probes was reproduced here.

O4: `tests/test_watcher_race_conditions.py:164` increments the concurrency
counter before its entry barrier and before the real `_has_pending_messages`.
`test_concurrent_pre_checks` (`:727`) therefore proves 20 simultaneous requests,
not 20 overlapping database queries. That is a useful contention topology;
rename/clarify the oracle rather than requiring database query overlap where
serialization can be correct. Concurrent writer futures at `:348` are waited
but not individually resolved, so primary writer errors become late count
failures; resolve them and compare exact expected message identities for clearer
diagnosis. `examples/tests/test_reference_reactor.py:436` writes after a startup
sleep, so its wake timing is not proof the long wait was actually entered. The
native-waiter worker-result test at `:336` supplies a genuine entered-wait witness
for its *different* wake source. Do not silently substitute that for input wake.

O5: `tests/test_sqlite_schema.py:1503` compares canonical/rebuild SQL-builder
bodies. This is an authored-source consistency test, not a real migration
oracle. The real fresh-vs-migrated native shape/sidecar test at `:266` is stronger
behavioral evidence. If one authored DDL source is a declared maintainability
policy, the cheap direct check can remain explicitly labeled as such; it must
not stand in for native migration. Query plan tests also intentionally pin an
index performance obligation, not incidental returned SQL rows. Reassess only
if that obligation changes.

The optional weft file mutates global `sys.path` at collection to import a
sibling checkout, potentially adding its site-packages before an eventual skip.
This is a nonhermetic integration choice, not evidence of an SB runtime bug.
Its owner may deliberately want current sibling compatibility. Prefer a
process-isolated, explicit integration environment in later harness work; do
not silently disable the corruption regression.

## Preservation and handoff

No additional bulk-pruning recommendation follows from line count or private
attributes. Session identity/refcount/counter checks detect ownership leaks
that public delivery alone would miss. Real SQLite, POSIX fork, Windows lock,
Darwin xattr ABI, optional downstream and subprocess signal tests represent
different substrates. Pass-through fault injection and controlled time are
appropriate when the real owner still runs. Protocol tables have a legitimate
test-support subject; authored table prose alone does not prove runtime state.

The most useful test-audit refinement exposed by this lane is a recurring rule:
an entered helper is not a completed decision. State explicitly which owner
decision the readiness event witnesses, arrange that it happens before the
assertion, and give every started actor a failure-path cleanup owner. Existing
skill guidance already supports this; no skill edit was needed in this
report-only task.

Root review should independently verify L1/L2 false-green controls, L9 signal
containment, and proposed L13/L14/L15 deletions before authorizing remediation.

## Subsequent remediation

Authorized changes, controls, native runs and residual findings are recorded in
[`2026-10-06-test-audit-remediation-plan.md`](2026-10-06-test-audit-remediation-plan.md).
This appendix retains the original report-only audit findings and limits.
