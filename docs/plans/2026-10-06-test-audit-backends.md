# Backend test-audit appendix

Date: 2026-10-06. Parent: [whole test-surface audit](2026-10-06-whole-test-surface-audit-plan.md).
Baseline: `d4d2634`. Mode: report-only audit, not pruning or remediation.

## Source Documents

- `docs/program-theory.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/specs/16-python-library-api.md`
- `docs/specs/11-delivery.md`
- `docs/specs/17-ops.md`

The test-audit skill was read in full. Exact test/support owners and focused
verification boundaries are recorded below; citations do not replace reading.

## Result and boundary

All 47 assigned Python files were read in full, including helper bodies,
parameter matrices and transition payloads: 20,770 lines and 455 static test
declarations. The table below accounts for every file. A declaration count is
not a runtime-case count or a claim that each case was executed during this
audit.

The most important defect is a fork regression test whose parent can wait
forever precisely when the protected fork behavior regresses. Other findings
are unnecessary version/export pins, source-text-based scheduling, concurrency
proofs that do not establish the contender reached its operation, and
failure-path thread cleanup. No application defect is established by these
findings. Passing tests are not evidence that their negative assertions have
had the required scheduling opportunity.

No tests, production code, runner settings, time budgets, or dependencies were
changed. No deletion is recommended without the additional preservation proof
described below. `F` means fix; `C` means consolidate only after proof. Priority
P2 denotes a material test/harness defect; P3 denotes a lower-cost maintenance
candidate.

Judgment used the test-audit skill, program theory, testing-patterns, the
product-section registry, relevant SB-API/DELIVERY/BCAST/SELECT/OPS contracts,
and process-session ownership rationale. Owner inspection for findings covered
`session.py`, `_broker_session.py`, `db.py`, `sbqueue.py`, PostgreSQL runner,
plugin, schema and validation paths, and Redis runner and activity-listener
paths. Installed redis-py's `ConnectionPool._checkpid` was also inspected:
its inherited-lock failure is real dependency behavior, not a fictitious test
scenario. Its five-second fork-lock guard does not bound a child blocked on a
SimpleBroker mutex before reaching redis-py.

## Actionable findings

### BE-1: fork failure can hang the parent; the pipe descriptor also leaks (P2, F)

`extensions/simplebroker_redis/tests/test_redis_pool.py:222`,
`test_inherited_broker_session_rejects_while_queue_recovers`.

The parent performs blocking `os.read(read_fd, 32)` and then blocking
`os.waitpid(child, 0)`. Neither has an aggregate deadline. A child that waits
on an inherited SimpleBroker mutex never writes its result. The parent then
never reaches its `finally`, which releases the parent holder. Even a passing
run leaves the parent's `read_fd` unclosed. Setup also starts the holder before
the cleanup guard begins, so an early admission/pipe/fork failure can strand
the holder or owned session.

The protected contract is important: an inherited explicit BrokerSession must
reject active methods before acquiring its handle lock, inherited close is a
no-op, and its Queue can recover through a child-owned session. The test was
added in `64c31ee` with the public BrokerSession surface; do not replace it
with a fake-PID test or delete it as a duplicate of Queue-only recovery.

Fix: establish resource ownership in one outer `try/finally`; poll the pipe and
child using one monotonic aggregate deadline; report child PID/liveness/exit
status; kill and reap an unfinished child on failure; close both pipe ends;
release and join the holder before closing the session. Preserve the real
fork while the parent lock is held. The same file's
`test_public_persistent_queue_recovers_child_owned_session:635` already has a
bounded `select` and kill/reap cleanup pattern, but tests a distinct recovery
path and is not an equivalent replacement.

Focused validation: run the explicit-session case normally and in an isolated
control whose child intentionally blocks before writing. The control must
fail within the coordination deadline with diagnostics, leave no child or
holder alive, and keep the restored production fork behavior passing. No
deadlock sensitivity control was run during this read-only audit.

### BE-2: real fork scheduling depends on exact private source text (P2, F)

`extensions/simplebroker_redis/tests/test_redis_pool.py:563`,
`test_public_persistent_queue_recovers_child_owned_session`, all `held` cases.

The test searches `inspect.getsourcelines()` for the exact lines
`if self._project_setup_complete:` and `if self._closed or self._closing:`
and uses `sys.settrace` to stop at that line. Extracting a private predicate,
renaming a private field, or rewriting an equivalent condition produces
`StopIteration` before the fork. The `held="none"` case does this lookup too,
although it never installs the trace. That failure does not indicate broken
child session recovery.

The protected behavior is child recovery without acquiring/finalizing locks
owned by vanished parent threads. `_ProcessBrokerSession._begin_operation`
and `DBConnection._ensure_project_target_initialized` actually own those
locks. The scheduling mechanism was introduced in `07bd8b0`, a correctness
repair, so the hostile inherited-lock interleaving must survive remediation.

Fix: coordinate through the owned lock boundary using a test-side delegating
lock/condition wrapper that holds the actual lock and signals entry. Do not
export a production-only test seam or merely pause before the lock. Keep the
real fork, admission/project/no-lock cases, child-owned identity and payload
assertions, plus existing kill/reap cleanup. A wrapper must preserve Condition
and fork behavior; that implementation needs careful review rather than a
mechanical replacement.

Focused validation: show that the test survives a behavior-preserving private
predicate refactor and still fails when child recovery attempts the inherited
locked path. Retain this test until both controls establish equivalence.

### BE-3: the PostgreSQL stats test pins unrelated release metadata (P2, F)

`extensions/simplebroker_pg/tests/test_connection_stats.py:124`,
`test_connection_stats_is_postgres_only_without_widening_core_protocol`.

`BACKEND_API_VERSION == 9` is an unrelated copied value. A coordinated,
compatible backend API bump fails it without changing stats behavior. Exact
ordered equality of the three-element `simplebroker_pg.__all__` also rejects
an additive export or an export-order change. SB-API-13 requires the stats
helper's import, PostgreSQL-only boundary and absence from the portable
BrokerConnection operation set, not this frozen API number or export order.

Keep the portable-boundary negative assertions and public annotation/import
coverage. Replace the export pin with the promised-name presence/import proof;
use the already-existing relational API handshake checks rather than another
literal. Exact plugin/core version matching remains necessary. The first-party
plugin's declaration must remain independent of the core constant so an
unupdated extension cannot falsely claim compatibility.

Keeper evidence: PG
`test_pg_init_backend.py::test_backend_plugin_declares_backend_api_version`
and Redis `test_redis_validation.py`'s identically named test compare the real
plugin declaration with the current core version. They cover matching but do
not replace the stats-specific PostgreSQL-only proof. Root's backend resolution
lane owns stale/future rejection. History: export boundary introduced in
`01148d5`; API literal changed to nine in `3418079`. That history shows the
copied value required maintenance for a separate feature.

Focused validation: public helper import and annotations; portable-boundary
checks; relational first-party matches; stale/future rejection. An additive
export/reorder and coordinated API bump should not fail the stats test. The
current stats-pin and both relational checks ran successfully in this audit;
that pass does not prove pin quality.

### BE-4: PostgreSQL lock tests do not prove the contender reached the lock (P2, F)

Affected exact test locations:

| Test | Location of negative wait |
| --- | --- |
| `test_prepare_broadcast_excludes_concurrent_new_queue` | `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py:52` |
| `test_exact_broadcast_does_not_resurrect_queue_deleted_before_selection` | same file, `:118` |
| `test_exact_broadcast_create_missing_resurrects_queue_deleted_before_atomic_point` | same file, `:176` |
| `test_same_queue_claim_waits_instead_of_skipping` | `extensions/simplebroker_pg/tests/test_pg_fifo_semantics.py:69` |
| `test_postgres_rename_waits_for_write_like_table_lock` | `extensions/simplebroker_pg/tests/test_pg_queue_rename.py:218` |

These start a contender and infer serialization from `finished.wait(0.2)
being false. No event establishes entry into the contender's production
operation. A delayed contender can run only after the held transaction commits;
the negative assertion and final result then pass even if the intended lock
was removed. A thread-start event alone would not resolve this gap.

Actual PostgreSQL locks must stay exercised. `prepare_broadcast` takes the
metadata row before the message-table lock; rename orders aliases, metadata,
then table; claim/move lock rows through the retrieve query rather than an
additional advisory call. These are backend-specific exclusion/deadlock risks,
not redundant copies of generic operation results.

Fix: a delegating runner probe should signal at the relevant actual lock/query
entry, or observe the specific contender waiting through PostgreSQL lock state.
Only then make the bounded noncompletion assertion and release the blocker.
The backend's write-keep test already wraps real SQL and coordinates the
locked producer/contender path; it is a useful pattern, not a replacement.
Keep final state/result/error assertions and do not expand the 0.2-second wait
to hide scheduling ambiguity.

Focused validation: controlled delayed contender, then an isolated mutation
of each protected lock/claim serialization path. Establish intended failure
at the contract assertion rather than setup failure. These sensitivity controls
were not run here; the missing opportunity is established by source inspection.

### BE-5: early failures can escape worker cleanup or mask the original error (P2, F)

`extensions/simplebroker_pg/tests/test_pg_queue_rename.py:340`,
`test_alias_mutation_finishes_before_rename_takes_metadata_lock`.

If `mutation_paused.wait(5)` fails, `rename_thread.start()` has not run. The
unconditional `rename_thread.join()` in `finally` raises
`RuntimeError: cannot join thread before it is started`. It masks the assertion
and skips the remaining core/runner cleanup. Track started threads, release all
gates, join only started workers, and retain the first failure while completing
independent resource cleanup.

Related failure ownership: `test_pg_fifo_semantics.py:77` and
`test_pg_broadcast_semantics.py:69` close cores/clean schemas without a
failure-path release and join of the daemon contender. The exact-broadcast
cases roll back their blocker, but do not join the contender in `finally`
before closing its core. FIFO and the first broadcast worker also do not
capture worker exceptions or set completion in `finally`. A primary assertion
failure can thus race teardown against live work or degrade into a thread
warning rather than the original worker error. Successful-path joins do not
provide failure-path ownership.

Fix: release/rollback blocker first, join every started worker, capture and
surface its errors, then close owned cores and drop the schema. Cleanup must
handle a worker that failed before signaling. Retain all lock-order and final
payload/alias assertions; no equivalent keeper justifies deleting these cases.

Focused validation: inject an ordinary error before the first pause and before
the contender start; require the original failure, no unhandled thread warning,
no join-before-start error and all runner cleanup. This is a harness defect,
not evidence of a product deadlock.

### BE-6: historical v5 migration input is expressed as moving current-minus-one (P2, F)

`extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py:154`,
`test_migrate_schema_from_previous_version_rebuilds_v6_layout`.

Both recorded live version and `current_version` use
`PostgresBackendPlugin.schema_version - 1`, while the assertions demand the
specific v5-to-v6 removal of `order_id`. At schema seven the input becomes six,
yet the test still demands the old migration step. This couples an explicit
historical storage transition to an evolving current-version expression.

Related maintenance pins: the older-owned readiness case at `:336` matches
`older than current version 6`; the fresh-layout test at
`test_pg_message_id_order.py:84` requires `POSTGRES_SCHEMA_VERSION == 6` and
exact current DDL spelling. A later compatible schema evolution should not
fail simply because the current version is no longer six. Historical versions
must remain literal: do not change all literal v5/v6 fixtures to current-minus-one.

Fix: name and feed literal historical v5 for that migration proof, retain
literal independent historical schema construction, assert version publication
for each actual supported step, and derive the current diagnostic value where
that is the claim. Use catalog results for current shape where practical.
The real v5 migration cases in `test_pg_message_id_order.py` prove rows,
sidecars, rollback, dependency refusal, and concurrent startup. They are
stronger dependency evidence but are not shown equivalent to every recorded
transaction/order assertion; deletion is not approved.

History: the previous-version case was repurposed for public-ID layout in
`2bc9ea`; literal v5 builder and real migrations preserve the independent
historic input. Focused validation: literal v5-to-current ladder and rollback,
older/current/newer classification, and a harmless later-version declaration
control that does not demand re-running v5 removal on v6 input.

### BE-7: stop/close wait tests may stop before the wait starts (P3, F)

`test_pg_notify.py:546`'s
`test_multi_queue_activity_waiter_listener_close_wakes_waiters` uses a
0.1-second sleep. PG transition payload `_listener_close_stops_wait` at
`test_pg_state_machine_transitions.py:315` and Redis payload
`_redis_listener_stop_wakes_wait` at `test_redis_state_machine_transitions.py:262`
close immediately after starting the thread. Redis integration tests
`test_activity_waiter_stop_event_breaks_wait_promptly` and
`test_multi_queue_activity_waiter_stop_event_breaks_wait_promptly`
(`test_redis_integration.py:1116`, `:1144`) use the same sleep.

A delayed thread sees stop/closed state on entry and returns false without
testing interruption of an already active wait. These are internal listener
shutdown/stop guarantees, not permission to introduce concurrent public
ActivityWaiter.wait/close. SB-API-6 explicitly requires the public owner to
serialize wait and close. Redis listener waiting also uses short condition
polls, so one must distinguish a prompt stop check from a notify-specific claim.

Fix: signal at the actual condition-wait (or composite polling wait) seam,
then stop/close and retain the bounded join plus result/error/transport-close
assertions. Do not claim removal of notify must fail a poll-bounded stop test;
choose the control that breaks the claim each test actually makes. Preserve
real pubsub/LISTEN integration alongside transition-fake controls.

Focused validation: force a delayed worker to show it cannot pass without the
entry handshake; break the listener's relevant stop-observation/wakeup path
in isolation. No such controls ran during this audit.

## Consolidation candidate, not a deletion decision

Redis activity-waiter close transition rows (`CLOSE_SUCCESS`,
`CLOSE_ORDINARY_FAILURE`, `CLOSE_NESTED_FAILURES`, `CLOSE_INTERRUPTED`,
`CLOSE_AGAIN`, `test_redis_activity_waiter_lifecycle.py:72`) overlap some
individual cases below them. This is a P3, C candidate only. The matrix's
ordinary-failure row checks a message and the final two cleanup events, whereas
individual cases also prove first-error identity, complete cleanup order and
nested child notes. They are not currently interchangeable keepers. Extend one
representation to retain every distinct assertion/input/path, then run the
specific failure-priority/idempotency controls before removing exact overlap.
Single-waiter and public namespace-wiring cases are distinct and stay.

## Retained coverage and false positives

SQL text and resource internals are not blanket grounds for removal. Historical
storage fixtures, lock order, SQL bind grammar, real query-plan boundedness,
pool lease balance and inherited mutexes represent genuine substrate contracts.
The default PG command-pool value three is explicitly promised in SB-API-3;
`test_default_pool_bound_is_three_command_connections` is therefore not an
arbitrary magic-value test. A declared-size assertion alone is weaker than a
real exhaustion/progress proof, but nearby pool tests retain that real risk.

Redis tests that inject Lua replies while invoking the real Python owner prove
retry/status handling, not Lua atomicity. Actual Lua-backed corruption,
reservation-token, stale-recovery and keep-window tests provide separate real
dependency evidence. Do not remove them as duplicate generic core behavior.
PostgreSQL fake-pool/listener tests similarly exercise the real runner/listener
state machine, while real wire and lock tests establish dependency semantics.
No actionable test-local replacement algorithm was established in this lane.

Queue-owned single activity waiters are cached and closed by Queue.close
(`sbqueue.py:1854`, `:2096`). Apparent missing explicit waiter.close calls in
those tests are not leaks. Composite waiters are separate ownership and their
tests close them explicitly. Opt-in cross-thread generator probes intentionally
test unsupported foreign-thread cleanup and poisoning in an isolated process;
they do not supersede same-thread public iterator guarantees.

The root reviewer owns the separate `_reset_pg_tables` catch-all OperationalError
isolation finding in `tests/conftest.py`. Owner inspection confirmed it should
not assume every translated PG failure means missing tables. It is not counted
again here.

## Complete file accounting

Paths use prefix `extensions/simplebroker_pg/tests/` for PG rows and
`extensions/simplebroker_redis/tests/` for Redis rows. All declarations and
all helper/parameter bodies in each row were reviewed. The retained contract
column names concrete protection, not an execution claim.

| Backend/file | Lines | Declarations | Concrete retained contract; audit disposition |
| --- | ---: | ---: | --- |
| PG `conftest.py` | 133 | 0 | Unique real schemas and cleanup; independent literal v5 storage fixture. Retain. |
| PG `test_connection_stats.py` | 668 | 16 | Exact keyed/type/count relations; ordinary-role cross-database count; Queue lease/pool behavior. BE-3. |
| PG `test_error_translation.py` | 43 | 3 | Real SQLSTATE translation and retryable marking. Exception fakes are outside owner. Retain. |
| PG `test_pg_activity_waiter_lifecycle.py` | 188 | 4 | Actual waiter cleanup identity/notes, idempotency and terminal BaseException behavior. Retain. |
| PG `test_pg_broadcast_semantics.py` | 192 | 3 | Actual PostgreSQL exclusion and committed-delete selection with/without creation. BE-4/5. |
| PG `test_pg_cross_thread_generator_probe.py` | 135 | 2 | Isolated unsupported cross-thread generator/sidecar poisoning and public close modes. Retain opt-in probe. |
| PG `test_pg_dump_load_pipe.py` | 169 | 3 | Real CLI SQLite/PG interoperability and header-only ID high-water preservation. Retain. |
| PG `test_pg_example_multi_queue_watcher.py` | 109 | 2 | Real native activity for every/dynamic queue; startup-stop waiter cleanup. Retain wiring proof. |
| PG `test_pg_fifo_semantics.py` | 83 | 1 | A contender cannot skip a locked pending head. BE-4/5, retain real locks. |
| PG `test_pg_include_claimed.py` | 64 | 2 | Pending/claimed merge and exact selection; move retains claim state. Retain. |
| PG `test_pg_init_backend.py` | 513 | 17 | Config/DSN precedence and escaping; backend match; capacity-error classification. Retain. |
| PG `test_pg_integration.py` | 667 | 14 | Real target routing, schema quoting, bootstrap fast path, shared session/iterator pool and CLI errors. Retain. |
| PG `test_pg_latest_pending_timestamp.py` | 78 | 2 | Claimed-newest exclusion and absent-index recreation. Retain. |
| PG `test_pg_maintenance.py` | 602 | 18 | Actual vacuum lock/discard/error priority and real claimed deletion/storage. Retain. |
| PG `test_pg_message_id_order.py` | 618 | 10 | Public-ID layout/order; real literal v5 migration rows, sidecars, dependencies, rollback and concurrent startup. BE-6 subset. |
| PG `test_pg_notify.py` | 656 | 14 | Real notification routing, move/fan-in/coexistence, registry cleanup and native deadlines. BE-7 subset. |
| PG `test_pg_ownership.py` | 524 | 8 | Foreign-object refusal, historical owned migration, restricted-role capacity refusal. Retain real wire ownership. |
| PG `test_pg_plugin_contract_edges.py` | 460 | 12 | Config snapshot forwarding, schema readiness and plugin error/cleanup seams. Retain. |
| PG `test_pg_queue_metadata.py` | 155 | 5 | Actual metadata includes claimed rows; count and list selection. Retain. |
| PG `test_pg_queue_rename.py` | 345 | 5 | Old/new activity; alias/meta/table order and real lock conflict/deadlock avoidance. BE-4/5. |
| PG `test_pg_runner_lifecycle.py` | 1,523 | 52 | Actual fake-pool runner lease state machine, interruption balance, real pool/listener/fork/locks. Retain. |
| PG `test_pg_schema_validation_paths.py` | 802 | 27 | Ownership/readiness states, migration fault/rollback and real lock-free current startup/renamed index migration. BE-6 subset. |
| PG `test_pg_search.py` | 85 | 3 | Actual literal search including percent/underscore and claimed visibility. Retain SQL semantics. |
| PG `test_pg_sidecar.py` | 90 | 5 | Independent binding lexer corpus plus real SQL percent, qmark and dollar-quote execution. Retain. |
| PG `test_pg_state_machine_transitions.py` | 1,346 | 2 | Full listener table and vacuum failure matrix; real owner and lock/discard semantics. BE-7 subset. |
| PG `test_pg_timestamp_resilience.py` | 131 | 2 | Missing floor error and stale repair versus concurrent real high-water advancement. Retain deterministic delegate. |
| PG `test_pg_write_keep.py` | 250 | 2 | Real row/table lock serialization of pending producers and mutators. Retain backend-specific proof. |
| Redis `conftest.py` | 42 | 0 | Unique real namespaces, runner shutdown and namespace cleanup. Retain. |
| Redis `test_redis_activity_waiter_lifecycle.py` | 481 | 10 | Real waiter close order/notes/interruptions plus public config-derived namespace identity. C candidate only. |
| Redis `test_redis_atomicity.py` | 1,808 | 45 | Actual Lua corruption preflight, stale candidates, reservation/keep/delete/alias/broadcast atomic phases. Retain. |
| Redis `test_redis_batches.py` | 540 | 12 | Real reserved claim/move batches, stale admission/token recheck, race and completion isolation. Retain. |
| Redis `test_redis_core_behaviors.py` | 708 | 20 | Actual core ambiguity/no unsafe replay, keep commit outcome, descriptor identity, validation and alias semantics. Retain. |
| Redis `test_redis_cross_thread_generator_probe.py` | 52 | 1 | Isolated cross-thread close/recovery diagnostic on real backend. Retain opt-in probe. |
| Redis `test_redis_dump_load_pipe.py` | 184 | 3 | Real CLI SQLite/Redis transfer and header high-water. Retain. |
| Redis `test_redis_include_claimed.py` | 69 | 3 | Actual pending/claimed merge order/bounds/exact/generator visibility. Retain. |
| Redis `test_redis_integration.py` | 1,169 | 34 | Actual Queue/plugin/session routing, atomic broadcast conflicts, durable indexes/search/delete, native activity. BE-7 subset. |
| Redis `test_redis_keys.py` | 25 | 1 | Fixed-width ID lexical encoding and range rejection. Retain independent vectors. |
| Redis `test_redis_latest_pending_timestamp.py` | 110 | 3 | Missing/pending/claimed/reserved latest-ID semantics. Retain. |
| Redis `test_redis_message_id_order.py` | 288 | 8 | Actual ordered merged selection; reservation windows and concurrent newest claims. Retain. |
| Redis `test_redis_plugin_contract_edges.py` | 582 | 17 | Consumer compile compatibility, pool normalization/config once, init/cleanup failures, registration gap, no-I/O binding. Retain. |
| Redis `test_redis_plugin_validation_paths.py` | 307 | 13 | Actual namespace foreign-prefix/old-version refusal without mutation; config/error seams. Retain. |
| Redis `test_redis_pool.py` | 656 | 16 | Real bounded pool reuse and listener slot; inherited mutex/session recovery. BE-1/2. |
| Redis `test_redis_queue_rename.py` | 202 | 6 | Pending/claimed preservation, reservation refusal, alias retarget, old/new activity and missing no-op. Retain. |
| Redis `test_redis_retry_policy.py` | 295 | 9 | Actual retry parser with controlled time: conflicts, stale candidates, cancellation and transport no-replay. Retain. |
| Redis `test_redis_sidecar.py` | 25 | 1 | Explicit unavailable sidecar in both transaction modes. Retain negative capability. |
| Redis `test_redis_state_machine_transitions.py` | 2,347 | 7 | Complete listener/runner/write/broadcast transition payloads; actual Python owners and real companion semantics. BE-7 subset. |
| Redis `test_redis_validation.py` | 251 | 12 | Namespace grammar/ownership/version; stale API rejected before I/O; real staged project init. Retain. |
| Total | 20,770 | 455 | 47 files, full-file review. |

## Verification and residual risk

Executed, unchanged source:

```text
uv run --locked pytest -q \
  extensions/simplebroker_pg/tests/test_connection_stats.py::test_connection_stats_is_postgres_only_without_widening_core_protocol \
  extensions/simplebroker_pg/tests/test_pg_init_backend.py::test_backend_plugin_declares_backend_api_version \
  extensions/simplebroker_redis/tests/test_redis_validation.py::test_backend_plugin_declares_backend_api_version
```

Result: all three selected cases reached 100%, command exit zero, under the
repository's configured xdist settings. Installed redis-py source inspection
succeeded; installed psycopg_pool reports 3.3.3. No live-service suites,
Windows reproduction, pre-fix replay, or mutation sensitivity controls were
run in this lane. Their absence is an explicit limit, not grounds for calling
the tests equivalent or the product defective. Later remediation should keep
the real PostgreSQL/Valkey boundaries and required parallel runner settings.

The skill's value and ownership rules directly affected this audit: apparently
redundant real substrate proofs were retained, literal historic storage inputs
were distinguished from brittle current-version mirrors, and no pruning was
performed. No skill change is proposed from this lane; the existing rules
already cover these recurring failure patterns.
