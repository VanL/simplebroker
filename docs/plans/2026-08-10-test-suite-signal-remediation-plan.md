# Test Suite Signal Remediation Plan

Date: 2026-08-10
Status: completed
Class: 4 — the test remediation crosses the core, CLI, watcher, SQLite,
PostgreSQL, Redis, documentation-gate, and test-infrastructure boundaries.
During implementation, stronger filesystem tests exposed two production
violations of the existing operator-owned permission contract. The owner
authorized narrow database and phase-lock creation-mode corrections. That
externally visible compatibility surface triggers [DOM-5] hardening even though
the normative product contract, CLI shape, and storage format remain unchanged.
Plan type: implementation against existing specs; no normative spec revision
Hardening: applied using
`docs/agent-context/runbooks/hardening-plans.md`; the plan records explicit
invariants, anti-mocking boundaries, fault witnesses, rollback scope,
cross-backend/Windows acceptance, and independent production-slice review.

## Goal

Remove tests that provide no distinct signal, replace false or circular oracles
with observable behavior proofs, and retain implementation-adjacent tests only
where the internal seam is itself a documented safety, portability, migration,
or performance boundary. The result should make a green suite mean more without
changing SimpleBroker's normative product contract. Two narrow production
corrections restore the already-specified operator-owned permission behavior.

This is not a coverage-maximization exercise. Case count and line coverage may
fall when duplicate or test-owned code is removed. Success is stronger defect
detection, clear ownership, and less refactor brittleness.

## Source Documents

- User-requested all-tests audit in the 2026-08-10 session, covering 218 pytest
  modules, 80,238 physical lines, 3,101 collected cases, and two fuzz harnesses
- `docs/program-theory.md` [THEORY-1], [THEORY-2], [THEORY-3], [THEORY-4]
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-3], [SB-CLI-4],
  [SB-CLI-5]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-2],
  [SB-DELIVERY-3], [SB-DELIVERY-4], [SB-DELIVERY-5], [SB-DELIVERY-8]
- `docs/specs/12-broadcast.md` [SB-BCAST-1], [SB-BCAST-2], [SB-BCAST-4]
- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-4], [SB-ID-5]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-1], [SB-SELECT-2],
  [SB-SELECT-4]
- `docs/specs/15-persistence-io.md` [SB-IO-1], [SB-IO-2], [SB-IO-3],
  [SB-IO-4], [SB-IO-5]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-2], [SB-API-3],
  [SB-API-4], [SB-API-6], [SB-API-11]
- `docs/specs/17-ops.md` [SB-OPS-1], [SB-OPS-2], [SB-OPS-3], [SB-OPS-4],
  [SB-OPS-5], [SB-OPS-6], [SB-OPS-7]
- `docs/implementation/07-complexity-and-state-machine-map.md`
  [SM-REDIS-BROADCAST], [SM-RELEASE], [SM-SIGINT-PROBE]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15]
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`
- `docs/lessons.md`, especially the hostile-default, owner-seam fault
  injection, independently declared protocol literal, lock-order, and
  behavioral example-gate lessons

## Consulted Surfaces Declaration

Plan authoring consulted the program theory; decision hierarchy; principles;
engineering principles; testing-patterns, writing-plans, hardening-plans, and
review-loop runbooks; canonical product specs 10 through 17; implementation
index, repository map, and agent inventory; the plan index; `docs/coalescing.md`;
and the production/test files cited below.

## Spec Baseline

- `b58ef6619927812adfb6d03d2d1838ab421449f1` —
  `docs/specs/10-cli.md` through `docs/specs/17-ops.md` at plan authoring time.
- No normative spec delta is proposed. The plan strengthens or removes evidence
  for behavior already governed by the existing specs.
- Non-normative verification tables, evidence manifests, implementation notes,
  or Related Plans sections may change only to replace deleted node IDs with
  surviving firing evidence. Such edits must not change requirement text.

## Context and Key Files

### Current suite shape

- `tests/`: 181 collected pytest modules and 2,674 cases at the audit baseline.
- `extensions/simplebroker_pg/tests/` and
  `extensions/simplebroker_redis/tests/`: 37 modules and 427 collected cases.
- `fuzz/fuzz_dump_load.py` and `fuzz/fuzz_timestamp_validate.py`: useful
  totality/property harnesses; not deletion candidates.
- `tests/conftest.py`: owns backend matrices, Queue factories, CLI helpers,
  managed subprocesses, and test cleanup. Its `queue_factory` defaults to
  `persistent=True`, unlike the public `Queue` default.
- `tests/helper_scripts/`: subprocess and backend helpers. Assertions in child
  work must return through a parent-visible result, exit code, event, or queue.

### Production owners that replacements must keep real

- `simplebroker/watcher.py`: watcher main loop, dispatch, stop, polling, burst,
  activity, and signal behavior.
- `simplebroker/sbqueue.py`: public Queue lifecycle, configuration, and
  read/move/materialization surfaces.
- `simplebroker/db.py`: broker operations, transactions, move semantics, and
  connection ownership.
- `simplebroker/cli.py` and `simplebroker/commands.py`: public parsing, exit
  codes, JSON diagnostics, watch, streaming, init, cleanup, and move output.
- `simplebroker/_dump.py`: dump selection and load behavior. Production
  `_selected` must never be the expected-value oracle for its own tests.
- `simplebroker/_backends/sqlite/`: schema, cleanup, vacuum, and data-version
  owners.
- `extensions/simplebroker_pg/simplebroker_pg/` and
  `extensions/simplebroker_redis/simplebroker_redis/`: backend-specific
  atomicity, ownership, listener, rename, and storage realization.

### Required reading and comprehension gates

Before editing, the implementer records answers in the execution log. A wrong
answer blocks the slice until the cited source is reread.

1. When may a private-state assertion remain the primary oracle?
   Expected answer: only when that state or ordering is itself the documented
   safety, FFI, migration, storage-compatibility, or performance boundary; it
   should otherwise support a public or durable-state assertion.
2. What must happen when a stronger test exposes a real production defect?
   Expected answer: stop that slice, record the failing behavior and owning
   spec, and reclassify/open a separate product remediation. Do not weaken the
   test or silently change production under this test-only plan.
3. What must happen before deleting a cited test node?
   Expected answer: find every spec, implementation, workflow, manifest, and
   documentation citation; select stronger successor evidence or record the
   behavior as intentionally obsolete; then update the citations atomically.
4. How are concurrent watcher/process tests made deterministic?
   Expected answer: readiness events, barriers, bounded joins, parent-visible
   results, exact final state, and explicit exit codes. Sleeps and permissive
   signal escalation are not readiness or correctness proofs.
5. Which backend matrix is required for a shared behavior change?
   Expected answer: SQLite locally plus the PostgreSQL and Redis/Valkey runners;
   backend-specific storage tests stay in their owning extension suite.

## Invariants and Constraints

- The governing `[SB-*]` contracts and public API/CLI shapes remain the same.
  Production edits are limited to removing forced-private file modes so actual
  database and phase-lock artifacts follow the existing operator/umask policy.
- Never delete a test merely because it is private, slow, or redundant-looking.
  Deletion requires a named stronger successor or a documented obsolete claim.
- Every enumerated contract element touched by a deleted node retains firing
  evidence. Verification tables and exact evidence manifests stay truthful.
- A replacement must fail under the defect its predecessor missed. Record a
  narrow mutation witness or controlled-fault red proof before accepting it.
- Expected values must be independent of the production code under test. In
  particular, do not call production selectors, validators, serializers, or
  formatters to compute their own oracle.
- Public behavior and durable state are the primary oracles: exact bodies/IDs,
  queue state, source/destination conservation, process exit, output stream,
  resource usability, schema visibility, and backend-visible ownership.
- Do not freeze incidental cross-backend ordering where the specs do not own
  order. Use exact sets or multisets while retaining all required identity and
  conservation checks.
- For structured diagnostics, pin stable fields exactly and human prose by
  semantic substring. Preserve exact prose only when the contract declares it
  stable.
- Do not replace sleeps with longer sleeps. Use events, barriers, readiness
  files, bounded polling, or explicit subprocess output.
- Filesystem failures are injected at the owning backend/plugin seam against an
  isolated explicit target. Do not globally patch `Path.exists`, `Path.unlink`,
  or an unrelated predicate.
- Keep real SQLite transactions, real filesystem behavior, real Queue/Broker
  operations, and real subprocess/signal handling where those are the risk.
  PostgreSQL and Redis behavior must run against the actual test services.
- Narrow doubles are allowed only at adapter boundaries: deterministic clock,
  owner-seam fault, recording polling strategy, or external command
  collaborator. They must not replace the core behavior being proved.
- Do not introduce a new test framework, general-purpose abstraction, snapshot
  system, or mutation-testing dependency. Reuse current pytest, Hypothesis,
  transition-table, managed-subprocess, and backend-runner patterns.
- Preserve the existing test markers and xdist serialization groups unless a
  real ownership or isolation bug requires a reviewed change.
- Coverage percentage and test count are diagnostics, not acceptance criteria.
  A justified deletion may lower either.

Hidden couplings:

- Canonical specs and contract-test modules cite exact node IDs. Renaming or
  deleting a node can create false evidence even when runtime tests are green.
- Windows runs streaming tests separately and signal/process behavior differs
  by platform. Local POSIX success is not enough for those slices.
- `queue_factory` changes connection defaults relative to public Queue. Tests
  of defaults must construct the public object without the fixture override.
- PostgreSQL and Redis extension tests may inspect storage internals for
  adapter integrity. Replacing those indiscriminately with core-only behavior
  would lose backend-specific evidence.
- The test-owned watcher subclasses currently duplicate production control
  flow. Removing them may reveal timing assumptions in several modules at once.

Fatal versus best-effort failures:

- Message loss, duplication, wrong destination state, failure to stop, wrong
  exit code, missing selected broadcast writes, partial transaction state, and
  stale contract evidence are fatal test failures.
- Diagnostic timing counters and benchmark observations are best-effort unless
  a documented budget owns them. They must not substitute for correctness.

Stop and re-plan if implementation requires further production-code changes, normative
spec changes, new dependencies, new product-persistent artifacts, new public
timing budgets, or a second implementation path created solely for testing.
The provenance artifact named in S4 is repository evidence, not product state;
it is the only new durable artifact authorized by this plan.

## Rollback, Rollout, and One-Way Doors

Each slice is independently reversible by restoring its test files and exact
evidence citations. Land replacements before dependent deletions. A deletion
slice must not depend on an unlanded successor test.

There is no data migration or one-way door. Existing file modes are preserved;
rollback affects only future database, lock, or status creation. The rollout is
CI sequencing:

1. land stronger oracles and replacement tests;
2. observe core, Windows, PostgreSQL, and Redis matrices;
3. delete superseded tests and update evidence references;
4. rerun the same matrices from the final identifier.

If a replacement test exposes a production defect, rollback the incomplete
test-cleanup slice or leave the failing test isolated on its branch while a
separately classified product fix is planned. Do not merge a knowingly failing
main branch and do not hide the defect by restoring a permissive assertion.

Post-merge success is all required CI matrices green without the known false
oracles, no stale evidence node IDs, and no repeated flake from the new
concurrency/signal tests over three current-state runs. Permission success is
fresh POSIX artifacts matching ordinary requested modes filtered by umask,
existing modes remaining unchanged, and Windows retaining inherited ACL
behavior in its CI matrix.

## Principle-Level Diagnosis

| Principle | Failure pattern | Remediation shape |
|-----------|-----------------|-------------------|
| Observable behavior over implementation | Concrete classes, SQL text, cache hits, private fields, source AST | Assert public output, durable state, resource lifetime, or explicit adapter invariant |
| Independent oracle | Production selector/validator/formatter computes expected value | Test-owned model derived from the canonical spec |
| Production path stays real | Watcher subclasses override drain/check/dispatch | Real watcher with instrumentation only at the strategy or owner seam |
| Exact conservation | Length-only or `<=` assertions | Exact IDs/bodies, source plus destination, no loss/duplication |
| Deterministic concurrency | Sleeps, ignored readiness, permissive signal codes | Events, barriers, bounded joins, parent-visible results, exact exit |
| Enumerable evidence | Deleted or renamed node remains in a spec/manifest | Atomic citation update and collect-only gate |
| Delete unsupported concepts | Test-owned metrics, obsolete checkpoint guidance, unreachable defensive inputs | Delete rather than invent a replacement contract |

## Audit Remediation Ledger

This is the durable, executable output of the all-tests audit. Existing node
IDs were checked against pytest collection; `New:` marks a successor that the
implementation slice must create. Whole-module deletions appear once. Every
disposition is firm: implementation does not reopen delete-versus-replace
judgment. The exact node name is also the current behavioral claim.

| ID | Current claim / exact node or module | Defect | Disposition | Exact successor or new owner | Observable oracle | Contract / evidence owner | Slice |
|----|--------------------------------------|--------|-------------|------------------------------|-------------------|---------------------------|-------|
| L001 | `tests/test_thread_safety.py::test_shared_broker_thread_safety` | The final `len(read) <= len(written)` oracle explicitly permits message loss. | KEEP+STRENGTHEN | same node | Final drained bodies equal written bodies exactly; no loss or duplicate | shared-core safety | S2 |
| L002 | `tests/test_thread_safety.py::test_database_lock_timeout` | The test exercises cross-instance visibility, not a database lock timeout, so its name misstates the behavior it protects. | KEEP+RENAME | New name: `tests/test_thread_safety.py::test_cross_instance_shared_target_visibility` | Both broker instances observe the exact two-body set | cross-instance visibility | S2 |
| L003 | `tests/test_queue_api_comprehensive.py::TestQueueReadMethods::test_read_many_delivery_guarantees` | Length-only assertions pass with wrong, duplicated, or missing bodies. | MERGE | `tests/test_delivery_contract_sb_delivery.py::test_materialized_batches_commit_before_return[read]` | Exact returned body and observer-visible remainder | `[SB-DELIVERY-5]` | S2 |
| L004 | `tests/test_queue_api_comprehensive.py::TestQueueLastTimestampCaching::test_move_many_delivery_guarantees` | Length-only results do not prove source/destination conservation or correct bodies. | MERGE | `tests/test_delivery_contract_sb_delivery.py::test_materialized_batches_commit_before_return[move]` | Exact returned body, destination body, and source remainder | `[SB-DELIVERY-5]` | S2 |
| L005 | `tests/test_queue_api_comprehensive.py::TestQueueLastTimestampCaching::test_move_one_require_unclaimed` | Its `require_unclaimed=False` branch is conditional and has no assertion, while the useful default branch duplicates stronger coverage. | DELETE | `tests/test_move.py::test_move_only_unclaimed` | Claimed row stays at source; only pending body moves | `[SB-DELIVERY-3]` | S2 |
| L006 | `tests/test_queue_api_comprehensive.py::TestQueueHelperMethods::test_get_data_version` | It checks only integer type and expressly allows the version not to change after the purported change. | REPLACE | New: same module `tests/test_queue_api_comprehensive.py::TestQueueHelperMethods::test_get_data_version_advances_after_external_write` | Separate writer changes target; observer’s SQLite data version strictly advances | SQLite change-counter behavior | S5 |
| L007 | `tests/test_queue_api_comprehensive.py::TestQueueConnectionModes::test_ephemeral_mode_default` | It explicitly passes `persistent=False`, then inspects private fields, so it proves neither the public default nor operation-scoped lifetime. | REPLACE | New: `tests/test_db_connection_lifecycle.py::test_queue_default_is_ephemeral_and_operation_scoped` | Construct `Queue` without `persistent`; operations work and no retained connection survives | `[SB-API-3]` | S5 |
| L008 | `tests/test_queue_api_comprehensive.py::TestQueueConnectionModes::test_persistent_mode` | Private connection presence and ordinary read/write do not prove reuse or closure semantics. | MERGE | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_connection_lifetime` | Supported operations reuse a live connection until explicit close | `[SB-API-3]` | S5 |
| L009 | `tests/test_queue_api_comprehensive.py::TestQueueConnectionModes::test_context_manager` | It proves data persistence through a second queue but never proves the first queue’s owned resource closed. | MERGE | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_mode_uses_cached_connection` | Operations work in context; owned resource closes on exit; data remains readable by a new queue | `[SB-API-3]` | S5 |
| L010 | `tests/test_sql_internals.py::TestSQLBuilders::test_build_retrieve_query_peek` | Exact SQL-shape assertions make equivalent query refactors fail without adding behavior signal. | DELETE | `tests/test_sql_internals.py::TestRetrieveMethod::test_retrieve_peek_operation` | Real SQLite peek returns exact bodies without mutation | executable behavior | S6 |
| L011 | `tests/test_sql_internals.py::TestSQLBuilders::test_build_retrieve_query_claim` | It pins private SQL construction instead of proving claim state through an executed transaction. | DELETE | `tests/test_sql_internals.py::TestRetrieveMethod::test_retrieve_claim_operation` | Real claim returns exact body and removes it from pending state | delivery behavior | S6 |
| L012 | `tests/test_sql_internals.py::TestSQLBuilders::test_build_retrieve_query_move` | It pins private SQL text instead of proving atomic source/destination behavior. | DELETE | `tests/test_sql_internals.py::TestRetrieveMethod::test_retrieve_move_operation` | Real move conserves exact source/destination state | `[SB-DELIVERY-3]` | S6 |
| L013 | `tests/test_sql_internals.py::TestRetrieveMethod::test_retrieve_with_require_unclaimed` | The claimed-inclusive half has no oracle, and the asserted unclaimed half duplicates public move coverage. | DELETE | `tests/test_move.py::test_move_only_unclaimed` | Claimed row is excluded and pending row moves | `[SB-DELIVERY-3]` | S6 |
| L014 | `tests/test_sql_internals.py::TestRetrieveMethod::test_retrieve_commit_before_yield_difference` | Both alleged delivery modes assert the same post-return state, so the test demonstrates no difference. | DELETE | `tests/test_delivery_contract_sb_delivery.py::test_materialized_batches_commit_before_return` | Observer sees committed exact state when materialized result returns | `[SB-DELIVERY-5]` | S6 |
| L015 | `tests/test_sql_internals.py::TestBuildRetrieveSpec::test_build_retrieve_spec_basic` | Exact private dataclass equality freezes argument plumbing without proving selection behavior. | DELETE | `tests/test_queue_api_comprehensive.py::TestQueueReadMethods::test_read_many_after_timestamp`; `tests/test_queue_api_comprehensive.py::TestQueueLastTimestampCaching::test_move_many_before_timestamp` | Exact selected bodies and durable state, not private dataclass equality | `[SB-SELECT-1]`, `[SB-DELIVERY-3]` | S6 |
| L016 | `tests/test_message_claim.py::test_vacuum_lock_prevents_concurrent_vacuum` | It does not hold a competing lock or require a skipped vacuum, so all vacuum calls can run and the test still passes. | DELETE | `tests/test_vacuum_lock.py::test_concurrent_vacuum_skips_while_lock_held` | Held real lock prevents competing vacuum; later vacuum succeeds | `[SB-OPS-6]` | S6 |
| L017 | `tests/test_runner_error_handling.py::TestSQLiteRunnerErrorHandling::test_corrupted_database_detection` | Empty output and an ordinary `("ok",)` result are accepted, while the remaining assertion is effectively tautological. | DELETE | `tests/test_runner_validation.py::TestSQLiteRunnerValidation::test_validation_with_corrupted_sqlite_header`; `tests/test_runner_error_handling.py::TestSQLiteRunnerErrorHandling::test_run_database_error_real` | Deterministic corrupt header/data raises translated database error | error translation | S6 |
| L018 | `tests/test_default_handlers.py::TestHandlerIntegration::test_default_handlers_with_queuewatcher` | It checks private constructor assignments and callability without driving a handler through a watcher. | DELETE | `tests/test_default_handlers.py::TestDefaultHandlers::test_simple_print_handler`, `tests/test_default_handlers.py::TestDefaultHandlers::test_logger_handler`, `tests/test_default_handlers.py::TestDefaultErrorHandler::test_default_error_handler_logs_error`; live watcher coverage in `tests/test_watcher.py::TestQueueWatcher::test_basic_consuming_mode` | Actual output/logging and live delivery, not constructor field equality | handler behavior | S6 |
| L019 | `tests/test_custom_runner_integration.py::test_injected_runner_handles_actual_queue_writes_in_both_modes` | A private SQL-log substring is used as a proxy for runner-target ownership. | MERGE | New: same module `tests/test_custom_runner_integration.py::test_injected_runner_target_wins_over_decoy_queue_target_in_both_modes` | Queue is given path A and runner backed by B; write appears only in B | `[SB-API-3]` | S5 |
| L020 | `tests/test_custom_runner_integration.py::test_injected_runner_handles_actual_queue_reads_in_both_modes` | SQL-marker inspection couples the test to implementation spelling instead of proving which target changed. | MERGE | New: `tests/test_custom_runner_integration.py::test_injected_runner_target_wins_over_decoy_queue_target_in_both_modes` | Public read consumes B; A remains absent or unchanged | `[SB-API-3]` | S5 |
| L021 | `tests/test_custom_runner_integration.py::test_ephemeral_queue_with_injected_runner_reuses_runner_backed_core` | Concrete `BrokerCore`/`BrokerDB` type assertions freeze the current adapter implementation. | MERGE | New: `tests/test_custom_runner_integration.py::test_injected_runner_target_wins_over_decoy_queue_target_in_both_modes` | Repeated supported operations remain on B in both modes; no concrete-core assertion | `[SB-API-3]` | S5 |
| L022 | `tests/test_custom_runner_integration.py::test_injected_runner_is_caller_owned_across_close_and_finalizer` | Calling `_finalizer()` directly does not prove actual garbage collection preserves caller ownership. | REPLACE | Same node, using real dereference and `gc.collect()` | Queue close and actual GC leave caller-owned runner usable; runner closes exactly when caller closes it | `[SB-API-3]` | S5 |
| L023 | `tests/test_retry.py::test_version_is_non_empty_string` | It is a weaker duplicate of the version-consistency test against `pyproject.toml`. | DELETE | `tests/test_constants.py::TestConstants::test_version` | Package and project versions agree | package contract | S6 |
| L024 | `tests/test_retry.py::test_bounded_jitter_delegates_to_apply_jitter` | It locks down helper delegation rather than the returned delay’s behavioral bounds. | DELETE | `tests/test_retry.py::test_apply_jitter_enforces_floor`; `tests/test_retry.py::test_apply_jitter_spans_up_to_base` | Returned delay stays within the owned behavioral bounds | retry behavior | S6 |
| L025 | `tests/test_json_output.py::test_read_json_invalid_timestamp_error_is_json` | Exact whole-sentence equality makes harmless diagnostic rewording fail despite stable structured behavior. | KEEP+STRENGTHEN | same node | `rc=1`, stdout empty, stderr JSON has exact code/retryability and semantic timestamp guidance | `[SB-CLI-4]` | S3 |
| L026 | `tests/test_json_output.py::test_peek_json_invalid_message_id_error_is_json` | It pins complete human prose instead of the stable error code, retryability, stream, and meaning. | KEEP+STRENGTHEN | same node | Exact `INVALID_MESSAGE_ID`, `retryable=false`, stderr-only JSON; prose checked by semantic fragments | `[SB-CLI-4]` | S3 |
| L027 | `tests/test_json_output.py::test_move_json_invalid_message_id_error_is_json` | It duplicates the same brittle full-prose message-ID oracle on another command. | KEEP+STRENGTHEN | same node | Same stable fields and stream, without whole-sentence pin | `[SB-CLI-4]` | S3 |
| L028 | `tests/test_json_output.py::test_move_json_argument_error_is_json` | The whole human sentence is treated as wire contract although only structured fields and semantics are owned. | KEEP+STRENGTHEN | same node | Exact `INVALID_ARGUMENT`, retryability, stream, and source/destination semantic fragments | `[SB-CLI-4]` | S3 |
| L029 | `tests/test_json_output.py::test_json_error_reports_explicit_retryable_classification` | Full payload equality unnecessarily freezes injected human prose while testing the retryability marker. | KEEP+STRENGTHEN | same node | Explicit exception marker alone yields `retryable=true`; stable fields exact, prose semantic | `[SB-CLI-4]` | S3 |
| L030 | `tests/test_cli_rename.py::test_rename_json_collision_is_error_without_mutation` | Exact diagnostic prose is brittle even though the meaningful signals are structured fields and unchanged queues. | KEEP+STRENGTHEN | same node | Stable JSON fields plus “target exists” meaning; exact old/new queue bodies unchanged | `[SB-CLI-4]`, `[SB-OPS-4]` | S3 |
| L031 | `tests/test_message_by_timestamp.py::test_malformed_message_id_reports_error_but_absent_id_is_silent` | It freezes the complete plain-text diagnostic rather than its exit, stream, and semantic content. | KEEP+STRENGTHEN | same node | Malformed ID: `rc=1`, stderr semantic fragments; absent valid ID: `rc=2`, both streams empty | `[SB-CLI-1]`, `[SB-CLI-4]` | S3 |
| L032 | `tests/test_agent_kernel_contract.py::test_agent_kernel_forbids_delete_while_peek_stream` | Global keyword searches can pass when the required rule appears in an unrelated or contradictory section. | REPLACE | Same node with section-scoped parsing | Peek-stream section, specifically, states offset/skip-safe deletion rule and distinguishes move reservation | kernel ownership evidence | S4 |
| L033 | `tests/test_sqlite_schema.py::test_initialize_database_uses_one_explicit_transaction` | Tracing exact `BEGIN IMMEDIATE` and `COMMIT` statements freezes transaction spelling without proving atomic visibility. | REPLACE | New: same module `tests/test_sqlite_schema.py::test_initialize_database_bootstrap_is_atomic_at_each_failure_point` | Early/middle/late injected bootstrap failures expose no partial schema or metadata; retry succeeds | schema atomicity | S5 |
| L034 | `tests/test_worker_examples.py::test_resilient_worker_trap_makes_checkpoint_failure_explicit` | Shell-source wording is used as a proxy for the worker’s actual signal-time exit and diagnostic behavior. | DELETE | `tests/test_worker_examples.py::test_resilient_worker_signal_checkpoint_failure_exits_nonzero` | Real worker subprocess exits nonzero and reports checkpoint failure | worker transparency | S6 |
| L035 | `tests/test_project_scoping.py::TestEnvironmentVariableParsing::test_parse_bool_true_values` | It duplicates the canonical true-value parser table. | DELETE | `tests/test_constants.py::TestParseBool::test_true_values` | All owned true spellings parse true | configuration behavior | S6 |
| L036 | `tests/test_project_scoping.py::TestEnvironmentVariableParsing::test_parse_bool_false_values` | It duplicates the canonical false-value parser table. | DELETE | `tests/test_constants.py::TestParseBool::test_false_values` | All owned false spellings parse false | configuration behavior | S6 |
| L037 | `tests/test_timestamp_edge_cases.py::TestTimestampEdgeCases::test_fork_reinitialization` | Mocked PID and private-field changes do not exercise a real fork boundary. | DELETE | `tests/test_fork_safety.py::test_fork_safety_protection`; `tests/test_fork_safety.py::test_new_instance_after_fork_works` | Real child rejects inherited handle and a child-created instance works | fork safety | S6 |
| L038 | `tests/test_cleanup.py::test_cleanup_permission_error` | Making the database file read-only does not prevent POSIX unlink, and accepting success or failure leaves no oracle. | DELETE | `tests/test_cleanup.py::test_cleanup_attempts_every_later_path_after_each_unlink_failure`; `tests/test_cleanup.py::test_cleanup_aggregates_multiple_cli_failures_and_json_error` | Deterministic owner-seam unlink refusal, exact remaining target, clean CLI diagnostic | `[SB-OPS-7]` | S6 |
| L039 | `tests/test_cli_edge_cases.py::TestCLIEdgeCases::test_cleanup_permission_error` | Global `Path` patches fire outside the cleanup owner seam and do not reliably prove target preservation. | DELETE | `tests/test_cleanup.py::test_cleanup_attempts_every_later_path_after_each_unlink_failure`; `tests/test_cleanup.py::test_cleanup_aggregates_multiple_cli_failures_and_json_error` | Deterministic refusal at SQLite cleanup seam; no global `Path` patch | `[SB-OPS-7]` | S6 |
| L040 | `tests/test_queue_api_additions.py::test_queue_string_representations_in_context` | It repeats `str`/`repr` behavior already asserted more precisely by dedicated representation tests. | DELETE | `tests/test_queue_api_additions.py::test_queue_str_representation`; `tests/test_queue_api_additions.py::test_queue_repr_representation`; `tests/test_queue_api_additions.py::test_queue_repr_redacts_resolved_targets_and_uses_python_quoting` | Exact public `str`/`repr` semantics and target redaction | API representation | S6 |
| L041 | `tests/test_queue_api_additions.py::test_queue_string_consistency` | It is another duplicate of the dedicated public string and redaction contracts. | DELETE | `tests/test_queue_api_additions.py::test_queue_str_representation`; `tests/test_queue_api_additions.py::test_queue_repr_representation`; `tests/test_queue_api_additions.py::test_queue_repr_redacts_resolved_targets_and_uses_python_quoting` | Stable public representation across equivalent construction | API representation | S6 |
| L042 | `tests/test_performance.py::test_queue_validation_performance` | Private cache hits, misses, and cached-versus-uncached timing lock down one optimization rather than a public latency budget. | REPLACE | New name: `tests/test_performance.py::test_public_queue_name_validation_throughput_budget` | Repeated public Queue construction/validation stays within existing `validation_cached` budget; valid/invalid outcomes remain correct | existing performance budget | S4 |
| L043 | `tests/test_resilient_worker.py` | The whole module treats `--after` as a durable worker offset, contrary to the documented incomplete-filter semantics. | DELETE | `tests/test_worker_examples.py::test_resilient_worker_preserves_delete_failure_and_exact_id`; `tests/test_worker_examples.py::test_resilient_worker_does_not_skip_id_behind_checkpoint` | Published worker never narrows selection with `--after`; failures do not skip pending work | `[SB-SELECT-2]` | S6 |
| L044 | `tests/test_watcher_burst_mode.py::test_burst_mode_resets_on_activity` | The test-owned watcher overrides pending checks and draining, so a broken production activity-reset path can remain green. | REPLACE | same node, real `QueueWatcher` plus recording `PollingStrategy` | Real write/dispatch resets the strategy to burst state | watcher polling behavior | S1 |
| L045 | `tests/test_watcher_burst_mode.py::test_burst_mode_no_reset_on_empty_wake` | Its empty-wake result comes from overridden test logic rather than the shipped watcher loop. | REPLACE | same node, real watcher | Empty wake produces no handler call and does not falsely reset activity | watcher polling behavior | S1 |
| L046 | `tests/test_watcher_burst_mode.py::test_burst_mode_with_batch_processing` | Test-owned draining can satisfy batch assertions while production batching or activity handling is broken. | REPLACE | same node, real watcher | Exact batch bodies delivered; recorded strategy transitions reflect real drain | delivery behavior | S1 |
| L047 | `tests/test_watcher_burst_mode.py::test_burst_mode_with_errors_single_message` | The overridden dispatch path can mask production retry, pending-state, and error-progress regressions. | REPLACE | same node, real watcher | Real dispatch failure reaches error handler and leaves documented queue/progress state | watcher error behavior | S1 |
| L048 | `tests/test_watcher_burst_mode.py::test_burst_mode_with_errors_batch_processing` | Test-owned batch draining can pass despite incorrect production error and batch-progress behavior. | REPLACE | same node, real watcher | Exact successful/failed bodies and real batch progress state | watcher error behavior | S1 |
| L049 | `tests/test_watcher_burst_mode.py::test_polling_jitter` | Jitter observations are collected through a watcher that bypasses the production pending/drain path. | REPLACE | same node, recording strategy only | Recorded intervals stay inside configured jitter bounds while real watcher loop runs | polling behavior | S1 |
| L050 | `tests/test_watcher_burst_mode.py::test_burst_mode_with_peek_mode` | The overridden drain path does not prove real peek-watch delivery or checkpoint advancement. | REPLACE | same node, real watcher | Exact handler bodies; source remains pending; real activity state advances correctly | `[SB-DELIVERY-4]` | S1 |
| L051 | `tests/test_watcher_burst_mode.py::test_burst_mode_state_transitions` | The asserted state sequence is substantially generated by the test subclass’s duplicate control flow. | REPLACE | same node, real watcher | Recorded strategy transition sequence follows real empty/activity/error events | watcher polling behavior | S1 |
| L052 | `tests/test_after_flag.py::test_after_iso_date_precise_boundary` | Both date spellings are far before every row, only exit codes are compared, and parsed JSON is discarded. | REPLACE | same node | Exact-insert IDs at midnight−1, midnight, midnight+1; date-only `--after` returns only the strict successor | `[SB-CLI-5]`, `[SB-SELECT-1]` | S3 |
| L053 | `tests/test_after_flag.py::test_after_timestamp_heuristic` | It copies the private `2**44` classification seam even though that threshold is not a public contract. | REPLACE | same node | Documented representative bare values, explicit `s/ms/ns`, and generated native ID select the expected rows; no `2**44` assertion | `[SB-CLI-5]` | S3 |
| L054 | `tests/test_after_flag.py::test_after_error_propagation` | `chmod(0444)` fails database preflight before timestamp selection, and `rc != 0` accepts wrong exits or tracebacks. | MERGE | New: `tests/test_cli_validation.py::TestDatabaseTargetValidation::test_invalid_database_is_clean_cli_error` | `rc=1`, stdout empty, actionable stderr, no traceback | `[SB-CLI-1]` | S3 |
| L055 | `tests/test_property_timestamp_validate.py::test_native_ids_round_trip_in_default_mode` | Importing `UNIX_NATIVE_BOUNDARY` turns a private heuristic threshold into a property contract. | REPLACE | same node | Actual generated IDs and documented native examples round-trip; no private-boundary import | `[SB-CLI-5]`, `[SB-ID-1]` | S3 |
| L056 | `tests/test_property_timestamp_validate.py::test_unit_suffixes_and_bare_seconds_agree` | It asserts a broad undeclared bare-number heuristic and compensates with a copied special-case escape. | REPLACE | New name: `tests/test_property_timestamp_validate.py::test_explicit_unit_suffixes_agree` | Explicit `s/ms/ns` forms normalize equivalently over valid ranges; only representative documented bare examples remain | `[SB-CLI-5]` | S3 |
| L057 | `tests/test_property_timestamp_validate.py::test_known_quirk_non_ascii_digits_accepted` | It is a weaker single-example duplicate of the stronger Unicode script-invariance property. | DELETE | `tests/test_property_timestamp_validate.py::test_non_ascii_digits_are_script_invariant` | All Unicode digit scripts receive the same accept/reject behavior | timestamp grammar | S6 |
| L058 | `tests/test_broadcast_integration.py::test_broadcast_empty_queue_behavior` | It accepts exit `2` even though the claimed-only queue still exists and must receive the broadcast. | KEEP+STRENGTHEN | same node | Claimed-only queue still exists; broadcast returns `0`, emits exact body, stats show one claimed plus one pending | `[SB-BCAST-1]`, `[SB-OPS-1]` | S2 |
| L059 | `tests/test_broadcast_integration.py::test_broadcast_race_condition_documentation` | All operations are sequential, so the named resolution-to-insertion race never occurs. | DELETE | `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py::test_prepare_broadcast_excludes_concurrent_new_queue`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_broadcast_script_selects_queues_at_atomic_insertion_point` | Real concurrent selection/insertion boundary has exact target state | `[SB-BCAST-4]` | S2 |
| L060 | `tests/test_edge_cases.py::test_timestamp_overflow_protection` | The maximum-safe no-match branch accepts either success or empty, masking the documented exit-code distinction. | KEEP+STRENGTHEN | same node | Overflow is clean error; maximum-safe no-match is exactly `rc=2` with both streams empty | `[SB-CLI-1]` | S3 |
| L061 | `tests/test_move.py::test_move_atomic` | Two sequential moves and a deterministic XOR prove ordinary removal, not atomicity or concurrency. | DELETE | `tests/test_move_claim_patterns.py::test_concurrent_moves_no_duplicate_move` | Concurrent movers yield one winner per ID with no loss/duplication | `[SB-DELIVERY-3]` | S2 |
| L062 | `tests/test_move_integration.py::test_move_with_concurrent_operations` | Despite its name, it performs no concurrent operations and duplicates basic move behavior. | DELETE | `tests/test_move_claim_patterns.py::test_concurrent_moves_no_duplicate_move` | Actual concurrency, exact moved IDs, exact final queues | `[SB-DELIVERY-3]` | S2 |
| L063 | `tests/test_move_integration.py::test_move_claimed_message_workflow` | It repeats pending-only move behavior and adds no effective claimed-row assertion. | DELETE | `tests/test_move.py::test_move_only_unclaimed` | Claimed row stays; pending row moves | `[SB-DELIVERY-3]` | S2 |
| L064 | `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_handler_execution_verification` | The callback fabricates `"queue": "dest"` and the test merely asserts that fabricated value. | DELETE | `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_basic_move_functionality` | Real callback receives exact body and durable move state is correct | `[SB-DELIVERY-3]` | S2 |
| L065 | `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_order_preservation_global_id` | The test claims to verify global IDs but records and compares only message bodies. | DELETE | Strengthened `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_message_preservation` | Destination contains original exact ID/body; source lacks it | `[SB-ID-5]` | S2 |
| L066 | `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_message_preservation` | Its final queue assertion trusts callback-fabricated data instead of persisted destination identity and source absence. | KEEP+STRENGTHEN | same node | Persisted destination tuple equals original body/ID and source is absent; no fabricated callback queue field | `[SB-ID-5]` | S2 |
| L067 | `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_redis_core_internal_state_edges_are_safe` | This kitchen-sink node pins key layout and private state while several helper calls have no observable oracle. | REPLACE | New: same module `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_redis_namespaces_isolate_public_queue_state`, `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_redis_reserved_batch_rejects_unknown_token_without_mutation`; retain `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_fork_safety_replaces_inherited_write_lock` | Cross-core namespace isolation; realistic invalid reservation leaves state unchanged; real fork behavior | backend integrity | S5 |
| L068 | `tests/test_timestamp_resilience.py::test_transient_conflict_recovery` | No conflict is injected, so it is merely a weaker duplicate of normal-operation coverage. | DELETE | `tests/test_timestamp_resilience.py::test_normal_operation_no_conflicts`; `tests/test_timestamp_resilience.py::test_forced_conflict_handled` | Normal path and an actually injected conflict are separately observable | timestamp resilience | S6 |
| L069 | `tests/test_timestamp_resilience.py::test_truly_unresolvable_conflict_fails_safely` | It duplicates the later unresolvable-conflict test, which also checks conflict and resync metrics. | MERGE | `tests/test_timestamp_resilience.py::test_unresolvable_conflict` | Forced conflict exhausts retries, raises cleanly, and records conflict/resync metrics | timestamp resilience | S6 |
| L070 | `tests/test_message_by_timestamp.py::test_mutual_exclusivity_with_all` | The symmetric parameterized selector-conflict test fully subsumes this case. | DELETE | `tests/test_message_by_timestamp.py::test_message_selector_conflicts_are_symmetric` | Read/peek reject message selector with `--all` before mutation | `[SB-SELECT-1]` | S6 |
| L071 | `tests/test_message_by_timestamp.py::test_mutual_exclusivity_with_after` | The symmetric parameterized selector-conflict test fully subsumes this case. | DELETE | `tests/test_message_by_timestamp.py::test_message_selector_conflicts_are_symmetric` | Read/peek reject message selector with `--after` before mutation | `[SB-SELECT-1]` | S6 |
| L072 | `tests/test_message_by_timestamp.py::test_workflow_peek_then_use_timestamp` | It is a strict subset of the stronger exact read-by-timestamp workflow. | DELETE | `tests/test_message_by_timestamp.py::test_read_message_by_timestamp` | Peek obtains ID; exact read returns intended body and updates queue | `[SB-ID-5]` | S6 |
| L073 | `tests/test_message_by_timestamp.py::test_list_command_reflects_operations` | It runs `list --stats` twice without intervening state change, making the first assertion redundant. | MERGE | `tests/test_message_by_timestamp.py::test_list_command_reflects_operations` (remove unchanged intermediate stats call) | One exact post-operation stats assertion proves pending/claimed counts | `[SB-OPS-1]` | S6 |
| L074 | `tests/test_message_by_timestamp.py::test_timestamp_boundary_values` | The maximum-value setup and assertion are repeated verbatim inside the same node. | MERGE | `tests/test_message_by_timestamp.py::test_timestamp_boundary_values` (remove duplicated maximum-ID block) | Min/max valid IDs and invalid neighbors each have one exact assertion | `[SB-ID-1]` | S6 |
| L075 | `tests/test_release_script.py::test_plan_tag_action_for_new_or_matching_tags` | Its create, push-local, and reuse-remote states duplicate registered `SM-RELEASE` transitions. | MERGE | `tests/test_release_script.py::test_release_fires_transition_table[SM-RELEASE::TAG-CREATE]`; `tests/test_release_script.py::test_release_fires_transition_table[SM-RELEASE::TAG-PUSH-LOCAL]`; `tests/test_release_script.py::test_release_fires_transition_table[SM-RELEASE::TAG-REUSE-REMOTE]` | Exact tag action for each repository state | `SM-RELEASE` | S6 |
| L076 | `tests/test_release_script.py::test_plan_tag_action_rejects_remote_tag_at_different_commit` | It duplicates the transition-table remote-tag-move rejection case. | MERGE | `tests/test_release_script.py::test_release_fires_transition_table[SM-RELEASE::TAG-REJECT-REMOTE-MOVE]` | Remote tag mismatch fails closed | `SM-RELEASE` | S6 |
| L077 | `tests/test_release_script.py::test_plan_tag_action_replaces_local_tag_when_new_release_commit_is_expected` | It duplicates the transition-table local-tag replacement case. | MERGE | `tests/test_release_script.py::test_release_fires_transition_table[SM-RELEASE::TAG-REPLACE-LOCAL]` | Only expected new release commit permits local replacement | `SM-RELEASE` | S6 |
| L078 | `tests/test_release_script.py::test_tag_creation_happens_after_push_and_exact_sha_ci` | It is weaker than the CI-success transition, which also verifies command kwargs and publication ordering. | MERGE | `tests/test_release_script.py::test_release_fires_transition_table[SM-RELEASE::CI-SUCCESS-TAG]` | Push, exact-SHA CI, ancestry check, then tag publication in exact order and kwargs | `SM-RELEASE` | S6 |
| L079 | `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py::test_public_exit_annotations_keep_any_typed_parameters` | Exact `get_type_hints(...) is Any` assertions pin annotation representation while the mypy probe tests the real compatibility risk. | DELETE | `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py::test_public_context_manager_annotations_remain_override_compatible` | Mypy subprocess proves downstream override compatibility | `[SB-API-11]` | S6 |
| L080 | `tests/test_program_theory_contract.py::test_revisions_put_current_account_first` | The corpus parser already rejects wrong field order before this test’s body can add signal. | DELETE | `tests/test_program_theory_contract.py::test_record_corpus_uses_exact_grammar` | Parser rejects any revision field-order violation | `[DOM-5]` evidence grammar | S6 |
| L081 | `tests/test_pragma_settings.py::test_write_with_normal_sync_works` | It performs an ordinary write through code with no NORMAL-specific branch after the actual pragma value is already tested. | DELETE | `tests/test_pragma_settings.py::test_custom_sync_mode_normal` plus general write/read coverage | Configured synchronous mode is observably `NORMAL`; ordinary writes already work | SQLite configuration | S6 |
| L082 | `tests/test_no_dependencies.py::test_typing_extensions_not_imported` | The same AST walk’s broader no-external-import test already rejects `typing_extensions`. | DELETE | `tests/test_no_dependencies.py::test_no_external_imports` | Parsed source rejects every non-stdlib import, including `typing_extensions` | dependency policy | S6 |
| L083 | `tests/test_cli_main.py::test_main_status_json_flag` | It is a weaker direct-`main()` duplicate of the black-box status JSON test. | DELETE | `tests/test_status_command.py::TestStatusCommand::test_status_json_output` | Black-box status JSON has exact structure and values | `[SB-CLI-4]` | S6 |
| L084 | `tests/test_move_after_exclusion.py::test_watch_with_after_without_move_works` | Missing output ends in `pass`; no assertion. | DELETE | `tests/test_move_checkpoint_semantics.py::test_peek_watcher_skips_message_moved_in_behind_checkpoint`; `tests/test_delivery_contract_sb_delivery.py::test_two_peekers_observe_same_id_without_mutation` | Eligible post-bound message is dispatched; peek leaves state unchanged. | SB-SELECT-4, SB-DELIVERY-4 | S6 |
| L085 | `tests/test_cli_move.py::TestIntegrationScenarios::test_move_with_destination_already_populated` | Count-only destination check permits lost pre-existing bodies. | KEEP+STRENGTHEN | Same node | Exact destination body multiset; source empty. | SB-DELIVERY-3 | S2 |
| L086 | `tests/test_property_dump_load.py::test_filter_algebra_property` | Expected set calls production `_selected`; circular oracle. | KEEP+STRENGTHEN | Same node, independent `fnmatchcase` model | Exact selected records and preserved relative order. | SB-IO-3 | S3 |
| L087 | `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_polling_strategy_pragma_failures` | Real corruption branch may assert nothing. | KEEP+STRENGTHEN | Same node with deterministic failing provider | Calls 1–9 return false; call 10 raises the threshold error. | SB-API-6 | S1 |
| L088 | `tests/test_cli_edge_cases.py::TestCLIEdgeCases::test_path_traversal_with_parent_refs` | Foreign `/etc/passwd` failure does not prove traversal rejection. | MERGE | New `tests/test_security_fixes.py::test_parent_traversal_rejects_valid_outside_broker` | Valid `../outside.db` is rejected with traversal diagnostic and unchanged state. | SB-API-2 | S3 |
| L089 | `tests/test_move_claim_patterns.py::test_move_atomicity` | Successful 5→4+1 move does not test failure atomicity. | REPLACE | New `tests/test_move_claim_patterns.py::test_move_failure_is_atomic` | Injected transaction failure leaves both queues and IDs unchanged. | SB-DELIVERY-3 | S2 |
| L090 | `tests/test_persistence_io_contract_sb_io.py::test_io_pending_only_and_fresh_load_language` | Keyword presence can pass contradictory prose. | DELETE | `tests/test_dump_load.py::test_dump_format_header_aliases_messages_in_order`; `tests/test_dump_load.py::test_reloading_same_dump_fails_loudly`; `tests/test_peek_include_claimed.py::test_peeking_claimed_rows_mutates_nothing` | Pending-only dump, duplicate-load rejection, and claimed inspection fire behaviorally. | SB-IO-2, SB-IO-4, SB-IO-5 | S6 |
| L091 | `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_message_size_limit_exceeded` | Calls private `_dispatch`; main-loop validation can be broken while green. | MERGE | New `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_live_watcher_enforces_instance_size_limit` | Oversized seeded row bypasses normal handler; error handler fires once; queue state matches mode. | SB-DELIVERY-8, SB-API-6 | S1 |
| L092 | `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_oversized_message_without_error_handler_is_logged_and_rejected` | Private dispatch and manually nulled handler bypass real watcher flow. | REPLACE | New `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_live_watcher_logs_and_discards_oversized_message` | Live watcher rejects dispatch, logs the semantic size error, does not call the message handler, and leaves the queue empty as documented for watcher-side rejection. | SB-DELIVERY-8, SB-API-6 | S1 |
| L093 | `tests/test_queue_config_defaults.py::test_watcher_dispatch_uses_configured_message_size_limit` | Private dispatch proves config storage, not live use. | MERGE | New `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_live_watcher_enforces_instance_size_limit` | Writer with larger limit seeds row; smaller-limit watcher enforces its snapshot. | SB-API-3, SB-DELIVERY-8 | S1 |
| L094 | `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_environment_variable_parsing` | Sets no environment variables and inspects private defaults. | REPLACE | New `tests/test_connection_config.py::test_watcher_environment_config_controls_live_polling` | Environment values resolve through public config and change recorded live polling behavior. | SB-API-3, SB-API-6 | S1 |
| L095 | `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_error_handler_itself_fails` | Private dispatch, global config patch, and exact logger call duplicate live coverage. | DELETE | `tests/test_watcher.py::TestErrorScenarios::test_error_handler_exception` | Seeded message drives real handler and error-handler failures; both errors are logged. | SB-DELIVERY-2, SB-API-6 | S6 |
| L096 | `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_check_stop_centralization` | Freezes private call count and neutralizes the real stop condition. | DELETE | `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_interruptible_sleep_responsiveness`; `tests/test_watcher_edge_cases.py::TestWatcherEdgeCases::test_concurrent_stop_safety` | Real stop interrupts work promptly and concurrent stops leave no live thread. | SB-API-6 | S6 |
| L097 | `tests/test_queue_config_defaults.py::test_watcher_omitted_db_uses_configured_default` | Calling private `_queue_obj` does not prove watcher execution uses the target. | KEEP+STRENGTHEN | Same node | Public write reaches live handler through configured default; fallback DB is absent. | SB-API-3 | S1 |
| L098 | `tests/test_multi_queue_watcher_example.py::test_missing_error_handler_uses_default_after_an_override` | Reads private `_queues`; dispatch can ignore the mapping. | KEEP+STRENGTHEN | Same node | Fail handlers on both queues; first invokes override and second invokes default. | SB-API-6 | S1 |
| L099 | `tests/test_portability.py::test_chmod_called_on_new_database` | Pins exact `chmod(path, 0600)` contrary to operator-owned policy. | REPLACE | New `tests/test_portability.py::test_database_creation_respects_operator_umask` | Created artifact follows operator umask/policy and remains usable. | Configuration guide, file-permission policy | S5 |
| L100 | `tests/test_portability.py::test_chmod_not_called_on_existing_database` | Pins an internal call absence rather than preserved policy. | REPLACE | New `tests/test_portability.py::test_reopen_preserves_existing_database_mode` | Reopening leaves an existing operator-selected mode unchanged. | Configuration guide, file-permission policy | S5 |
| L101 | `tests/test_cli_edge_cases.py::TestCLIEdgeCases::test_system_exit_with_string_code` | Fabricates parser output impossible from the real parser. | DELETE | `tests/test_cli_contract_sb_cli.py::test_sb_cli_3_global_options_after_subcommand_fail` | Reachable invalid invocation exits 1 with visible parser error. | SB-CLI-1, SB-CLI-3 | S6 |
| L102 | `tests/test_cli_edge_cases.py::TestCLIEdgeCases::test_system_exit_with_none_code` | Fabricates parser output impossible from the real parser. | DELETE | `tests/test_cli_contract_sb_cli.py::test_sb_cli_3_global_options_after_subcommand_fail` | Reachable invalid invocation exits 1 with visible parser error. | SB-CLI-1, SB-CLI-3 | S6 |
| L103 | `tests/test_runner_validation.py::TestSQLiteRunnerValidation::test_validation_with_empty_file` | “Setup did not raise” passes if setup is a no-op. | KEEP+STRENGTHEN | Same node | Query succeeds and target is a valid initialized SQLite database. | SQLite adapter initialization | S5 |
| L104 | `tests/test_runner_validation.py::TestSQLiteRunnerValidation::test_validation_with_nonexistent_file` | “Setup did not raise” passes if no file is created. | KEEP+STRENGTHEN | Same node | Query succeeds and target becomes a valid SQLite database. | SQLite adapter initialization | S5 |
| L105 | `tests/test_performance.py::test_concurrent_mixed_operations_performance` | Operations are serial; name claims concurrency. | KEEP+RENAME | New name: `tests/test_performance.py::test_sequential_mixed_cli_throughput` | Serial workload meets its existing budget; results use allowed exits. | README Performance & Tuning | S4 |
| L106 | `tests/test_benchmark.py::test_published_m4_benchmark_catalog_matches_owner_results` | “Owner results” are hardcoded copies of README values. | REPLACE | New `tests/test_benchmark.py::test_published_benchmark_catalog_matches_result_artifact` | Attributable artifact schema is valid and renders the README table exactly. | README Performance & Tuning; THEORY-4 | S4 |
| L107 | `tests/test_persistence_io_contract_sb_io.py::test_io_cross_backend_evidence_labels_routine_and_opt_in_suites_truthfully` | Source-token checks do not prove runner collection. | KEEP+STRENGTHEN | Same node using runner `--collect-only` | PG/Redis routine runners collect named pipe nodes; direct PG↔Redis node is excluded. | SB-IO-2, DOM-10 | S4 |
| L108 | `tests/test_persistence_io_contract_sb_io.py::test_dump_omits_claimed_messages` | Duplicates stronger canonical dump behavior. | MERGE | `tests/test_dump_load.py::test_dump_format_header_aliases_messages_in_order` | Exact dump records exclude the seeded claimed body. | SB-IO-2 | S6 |
| L109 | `tests/test_persistence_io_contract_sb_io.py::test_load_rejects_duplicate_ids_on_reload` | Duplicates stronger canonical reload behavior. | MERGE | `tests/test_dump_load.py::test_reloading_same_dump_fails_loudly` | Second load raises `IntegrityError` and does not duplicate state. | SB-IO-4 | S6 |
| L110 | `tests/test_product_section_registry_final_cutover.py::test_readme_toc_ownership_audit_section_present` | Heading/keyword checks are wording brittle and contradiction-blind. | MERGE | New `tests/test_product_section_registry_final_cutover.py::test_registered_product_owners_and_entry_links_resolve` | Every registry owner/range/link parses and resolves. | Product section registry, DOM-10 | S4 |
| L111 | `tests/test_product_section_registry_final_cutover.py::test_docs_readme_declares_specs_own_exact_behavior` | Global words can appear in contradictory prose. | MERGE | New `tests/test_product_section_registry_final_cutover.py::test_registered_product_owners_and_entry_links_resolve` | Docs index links each family to its canonical owner. | Product section registry, DOM-10 | S4 |
| L112 | `tests/test_product_section_registry_final_cutover.py::test_root_readme_points_at_canonical_specs` | Token presence does not validate link targets or ownership. | MERGE | New `tests/test_product_section_registry_final_cutover.py::test_registered_product_owners_and_entry_links_resolve` | Root entry links resolve to registered owners. | Product section registry, DOM-10 | S4 |
| L113 | `tests/test_product_section_registry_final_cutover.py::test_kernel_cites_primary_code_families` | Family-name presence does not prove usable routing. | MERGE | New `tests/test_product_section_registry_final_cutover.py::test_registered_product_owners_and_entry_links_resolve` | Kernel and `llms.txt` links resolve to every canonical family owner. | Product section registry, DOM-10 | S4 |
| L114 | `tests/test_move_claim_patterns.py::test_move_updates_claimed_status` | Direct table/column assertions freeze schema for public behavior. | KEEP+STRENGTHEN | Same node | Public source/destination bodies, IDs, and stats prove unclaimed move state. | SB-DELIVERY-3 | S2 |
| L115 | `tests/test_move_claim_patterns.py::test_move_with_vacuum_interaction` | Raw SQL counts freeze schema although behavior is public. | KEEP+STRENGTHEN | Same node | Public stats and claimed-inclusive inspection prove source cleanup and unchanged destination. | SB-DELIVERY-3, SB-OPS-6 | S2 |
| L116 | `tests/test_move_claim_patterns.py::test_move_schema_verification` | Duplicates schema/index tests and does not inspect a move query plan. | DELETE | `tests/test_message_claim.py::test_partial_index_on_claimed_column`; `tests/test_sqlite_schema.py::test_ensure_schema_v2_adds_claimed_column_and_partial_index` | Migration creates the claimed column/index; adapter test proves index exists. | SQLite migration boundary | S6 |
| L117 | `tests/test_move_by_id.py::test_move_claimed_message_with_require_unclaimed` | `str or tuple[0]` permits wrong return type. | KEEP+STRENGTHEN | Same node | `with_timestamps=False` returns exact string or `None`. | SB-DELIVERY-3, SB-API-4 | S2 |
| L118 | `tests/test_move_by_id.py::test_move_claimed_message_without_require_unclaimed` | `str or tuple[0]` permits wrong return type. | KEEP+STRENGTHEN | Same node | Exact string plus exact public source/destination state. | SB-DELIVERY-3, SB-API-4 | S2 |
| L119 | `tests/test_move_by_id.py::test_move_mixed_mode` | Three permissive union assertions admit tuple regressions. | KEEP+STRENGTHEN | Same node | Each non-timestamp move returns its exact string; final queues are exact. | SB-DELIVERY-3, SB-API-4 | S2 |
| L120 | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_mode_uses_cached_connection` | Concrete `BrokerDB` assertion freezes implementation. | KEEP+STRENGTHEN | Same node | Repeated handles are identical and perform documented operations. | SB-API-3 | S5 |
| L121 | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_ephemeral_mode_creates_new_connections` | Concrete class and private runner ID freeze realization. | KEEP+STRENGTHEN | Same node | Handles have distinct lifetimes and each supports documented operations. | SB-API-3 | S5 |
| L122 | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_connection_type_consistency` | Only asserts concrete inheritance, not protocol behavior. | MERGE | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_mode_uses_cached_connection`; `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_ephemeral_mode_creates_new_connections` | Both modes yield usable `BrokerConnection` behavior. | SB-API-3, SB-API-4 | S5 |
| L123 | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_avoids_reconnection_overhead` | Patches setup and pins call counts without a public budget. | MERGE | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_mode_uses_cached_connection` | Persistent handle identity and continued usability establish reuse. | SB-API-3 | S6 |
| L124 | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_connection_reuse` | Constructor-count spy duplicates the reuse signal. | MERGE | `tests/test_queue_connection_manager.py::TestQueueConnectionManager::test_persistent_mode_uses_cached_connection` | Same handle is reused and remains operational. | SB-API-3 | S6 |
| L125 | `tests/test_connection_config.py::test_broker_core_merges_partial_config_with_defaults` | Private `_config` can be correct while operations ignore it. | KEEP+STRENGTHEN | Same node | Partial max-size override accepts boundary body and rejects oversized body. | SB-API-3, SB-DELIVERY-8 | S5 |
| L126 | `tests/test_connection_config.py::test_dbconnection_non_sqlite_target_accepts_partial_config` | Concrete core/private config assertions do not prove retained behavior. | KEEP+STRENGTHEN | Same node | Partial config changes a real operation through the injected connection. | SB-API-3 | S5 |
| L127 | `tests/test_connection_config.py::test_watcher_default_strategy_uses_instance_config` | Inspects private strategy fields; run loop can ignore them. | REPLACE | New `tests/test_connection_config.py::test_watcher_instance_config_controls_live_polling` | Recording strategy observes configured schedule during real watcher execution. | SB-API-3, SB-API-6 | S1 |
| L128 | `tests/test_commands_helpers.py::TestResolveTimestampFilters::test_invalid_message_id_returns_exit_error` | Private helper and exact prose freeze implementation/text. | MERGE | `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_error_inventory_and_public_paths` | Public JSON CLI returns exit 1, empty stdout, `INVALID_MESSAGE_ID`, `retryable=false`, semantic fragment. | SB-CLI-4, SB-ID-4 | S3 |
| L129 | `tests/test_watcher.py::test_context_manager_usage` | Weakref `_thread` layout is private. | KEEP+STRENGTHEN | Same node | Public `is_running()` is true inside, false after; exact messages consumed. | SB-API-3, SB-API-6 | S1 |
| L130 | `tests/test_watcher.py::test_context_manager_with_exception` | Private weakref plus `try/except: pass` weakens failure signal. | KEEP+STRENGTHEN | Same node | `pytest.raises(ValueError)` and public `is_running()` proves cleanup. | SB-API-3, SB-API-6 | S1 |
| L131 | `tests/test_insert_messages.py::test_insert_normalization_rejects_missing_high_water_from_validator` | Monkeypatch returns impossible `None`; unreachable defensive branch. | DELETE | `tests/test_insert_messages.py::test_broker_insert_messages_rejects_unadvanceable_high_water` | Real invalid high-water rejects before mutation. | SB-ID-4 | S6 |
| L132 | `tests/test_message_id_validation.py::test_parse_exact_message_id_delegates_to_canonical_validator` | Does not prove delegation and duplicates boundary cases. | DELETE | `tests/test_parse_exact_message_id.py::test_valid_19_digit_timestamps`; `tests/test_parse_exact_message_id.py::test_overflow_values` | Canonical valid and overflow inputs produce exact results. | SB-ID-4 | S6 |
| L133 | `tests/test_watcher.py::TestQueueWatcher::test_mixed_peek_and_read_watchers` | `len(peek_messages) >= 0` is tautological. | KEEP+STRENGTHEN | Same node | Exact consumed subset uniqueness and pending state; peek output is only bounded/subset-constrained. | SB-DELIVERY-1, SB-DELIVERY-4 | S1 |
| L134 | `tests/test_cli_metadata.py::test_stats_json_output` | Sum assertion is implied by already exact payload. | KEEP+STRENGTHEN | Same node | Exact JSON object remains the sole oracle. | SB-OPS-2, SB-CLI-4 | S3 |
| L135 | `tests/test_activity_waiter_api.py::test_top_level_exports_multi_queue_waiter_api` | `ActivityWaiter is not None` adds nothing after import/export success. | KEEP+STRENGTHEN | Same node | Exact `__all__` membership and callable identity. | SB-API-1, SB-API-6 | S5 |
| L136 | `tests/test_cli_watch.py::TestWatchCommand::test_watch_sigint_handling` | Ignores readiness and accepts SIGTERM/SIGKILL escalation. | DELETE | `tests/test_watcher_sigint_probe_transitions.py::test_watcher_sigint_probe_fires_transition_table[SM-SIGINT-PROBE::interrupt-cleans-and-exits]` | Ready real process receives SIGINT, cleans up, exits exactly 0. | SB-CLI-1 | S6 |
| L137 | `tests/test_watcher_cleanup.py::TestWatcherCleanup::test_watcher_auto_cleanup` | Only proves startup; cleanup occurs after the test and is not asserted. | MERGE | New `tests/test_watcher_cleanup.py::TestWatcherCleanup::test_tracker_stop_all_stops_registered_watchers` | `stop_all()`, bounded joins, and no surviving watcher threads. | SB-API-6 | S1 |
| L138 | `tests/test_watcher_cleanup.py::TestWatcherCleanup::test_multiple_watchers_cleanup` | Only proves three threads start; tracker failure stays green. | MERGE | New `tests/test_watcher_cleanup.py::TestWatcherCleanup::test_tracker_stop_all_stops_registered_watchers` | All registered watchers stop and all threads terminate. | SB-API-6 | S1 |
| L139 | `tests/test_watcher_cleanup.py::TestWatcherCleanup::test_watcher_stops_quickly` | Sleep does not prove handler started; bound passes with no dispatch. | KEEP+STRENGTHEN | Same node using `handler_started`/`handler_release` events | Handler starts; stop waits for active handler; thread then exits. | SB-API-6 | S1 |
| L140 | `tests/test_watcher_metrics.py` | Entire metrics product exists only in the test module. | DELETE | None; optional measurements belong in a benchmark/diagnostic harness. | No correctness oracle is lost. | No product contract | S6 |
| L141 | `tests/test_watcher_thundering_herd.py::test_thundering_herd_mitigation` | Handler isolation does not bound irrelevant drains; test-owned watcher logic. | REPLACE | New `tests/test_watcher_thundering_herd.py::test_real_watcher_queue_isolation`; new `tests/test_watcher_thundering_herd.py::test_unrelated_write_does_not_drain_idle_watchers` | Exact target delivery; idle handlers remain empty; unrelated drain counters do not advance. | SB-API-6; Python guide low-overhead claim | S1 |
| L142 | `tests/test_watcher_thundering_herd.py::test_thundering_herd_with_multiple_active_queues` | Proves only handler routing, not herd mitigation. | MERGE | New `tests/test_watcher_thundering_herd.py::test_real_watcher_queue_isolation` | Exact bodies reach active queues; inactive queues receive none. | SB-API-6 | S1 |
| L143 | `tests/test_watcher_thundering_herd.py::test_pre_check_correctness` | Direct private helper duplicates public pending behavior. | DELETE | `tests/test_queue_api_comprehensive.py::TestQueueHelperMethods::test_has_pending`; `tests/test_queue_api_comprehensive.py::TestQueueHelperMethods::test_has_pending_after_timestamp` | Public API reports pending state and bound behavior. | SB-API-4 | S6 |
| L144 | `tests/test_watcher_thundering_herd.py::test_pre_check_with_timestamp_filtering` | Mutates private `_last_seen_ts` and calls private pre-check. | KEEP+STRENGTHEN | Same node using public `after_timestamp` and real loop | Only post-bound bodies are dispatched. | SB-SELECT-4, SB-API-6 | S1 |
| L145 | `tests/test_watcher_thundering_herd.py::test_disable_pre_check_via_env` | Uses direct config, not environment, and never runs the main loop. | REPLACE | New `tests/test_watcher_thundering_herd.py::test_skip_idle_check_environment_controls_main_loop` | Under each env config, a recording seam observes whether live pre-check fires. | SB-API-3, SB-API-6 | S1 |
| L146 | `tests/test_watcher_thundering_herd.py::test_metrics_collection` | Asserts counters implemented only by the test subclass. | DELETE | None; useful overhead measurement moves to the new idle-drain test. | Real watcher idle-drain count, not test-owned metrics. | Python guide low-overhead claim | S6 |
| L147 | `tests/test_watcher_multiprocess.py::test_multiprocess_thundering_herd` | Reports processed counts only; toggled pre-check has no work oracle. | MERGE | New `tests/test_watcher_multiprocess.py::test_multiprocess_unrelated_write_does_not_drain_idle_watchers` | Each child reports drain/pre-check counters; unrelated children do no extra drain work. | SB-API-6; Python guide low-overhead claim | S1 |
| L148 | `tests/test_watcher_multiprocess.py::test_multiprocess_database_locking` | Lock-error ratio allows complete message loss and is scheduler-sensitive. | REPLACE | New `tests/test_watcher_multiprocess.py::test_multiprocess_contention_preserves_exact_delivery` | Exact 100 IDs delivered once, queue drained, children exit cleanly, no terminal errors. | SB-DELIVERY-1 | S1 |
| L149 | `tests/test_watcher_multiprocess.py::test_deadline_queue_type_hint_is_runtime_valid` | Introspects a test helper production never uses. | DELETE | None; obsolete test-only claim. | No product oracle is lost. | No product contract | S6 |
| L150 | `tests/test_json_message_id_contract.py::test_core_identity_dict_fields_are_exhaustively_classified` | AST inventory pins files, functions, dict counts, and formatter spelling. | REPLACE | New `tests/test_json_message_id_contract.py::test_public_json_identity_producers_preserve_message_ids` | Every public JSON producer emits 19 ASCII digits preserving integer identity. | SB-ID-1, SB-CLI-4, SB-IO-1 | S3 |
| L151 | `tests/test_security_fixes.py::test_path_traversal_protection` | Absolute portion has no deterministic acceptance oracle and may write `/tmp`. | REPLACE | New `tests/test_security_fixes.py::test_parent_traversal_rejects_valid_outside_broker`; new `tests/test_security_fixes.py::test_absolute_file_target_round_trips` | Valid outside relative target is rejected; absolute `tmp_path` target writes and reads back. | SB-API-2 | S3 |
| L152 | `tests/test_security_fixes.py::test_message_size_validation_non_stdin` | Named non-stdin but exactly duplicates stdin test. | REPLACE | New `tests/test_security_fixes.py::test_direct_argv_message_size_limit` | In-process `main()` oversized argv exits 1 and stores nothing. | SB-DELIVERY-8 | S3 |
| L153 | `tests/test_commands_init.py::TestInitCommand::test_init_existing_valid_database` | Private lock/raw SQL setup and raw SQL oracle freeze schema. | KEEP+STRENGTHEN | Same node using public Queue write/read | Second init succeeds and preserves exact public message. | SB-CLI-3, SB-API-2 | S3 |
| L154 | `tests/test_commands_init.py::TestInitCommand::test_init_database_file_permissions` | `mode & 0600` accepts 0644/0666 and asserts no owned permission contract. | DELETE | New in L099: `tests/test_portability.py::test_database_creation_respects_operator_umask` | Creation follows operator policy and remains usable. | Configuration guide file-permission policy | S6 |
| L155 | `tests/test_commands_init.py::TestInitCommand::test_init_concurrent_access_safety` | Two sequential calls are not concurrency. | DELETE | `tests/test_runner_error_handling.py::TestSQLiteRunnerErrorHandling::test_schema_setup_is_serialized_across_processes` | Eight processes serialize schema setup and leave valid state. | SQLite bootstrap safety | S6 |
| L156 | `tests/test_commands_init.py::TestInitCommand::test_init_cleanup_on_error` | Patched constructor makes artifact branch unreachable; state oracle is conditional. | DELETE | `tests/test_commands_init.py::TestInitCommand::test_init_permission_error_database_creation` | Reachable owner-seam failure returns error with no valid partial DB. | SB-CLI-1, init behavior | S6 |
| L157 | `tests/test_watcher_concurrency.py::TestWorkerPool::test_worker_joins_late` | Late worker may never start while early workers satisfy all assertions. | KEEP+STRENGTHEN | Same node with readiness event before phase two | Late watcher reaches running state; exact 100-message conservation holds. | SB-DELIVERY-1, SB-API-6 | S1 |
| L158 | `tests/test_watcher_concurrency.py::TestMixedMode::test_mixed_peek_read_basic` | `len(peek_messages) >= 0` is tautological. | KEEP+STRENGTHEN | Same node | Exact consumed set and empty pending state; peek output is only subset/bounded. | SB-DELIVERY-1, SB-DELIVERY-4 | S1 |
| L159 | `tests/test_watcher_concurrency.py::TestEdgeCases::test_queue_name_validation` | Construction plus `is not None` is tautological; invalid half is absent. | DELETE | `tests/test_property_queue_names.py::test_grammar_valid_names_work_end_to_end`; `tests/test_property_queue_names.py::test_grammar_invalid_names_are_rejected_at_construction` | Independent valid/invalid grammar fires end to end. | SB-DELIVERY-8 | S6 |
| L160 | `tests/test_streaming.py::test_streaming_read_all` | 1,000-row correctness does not prove incremental emission or memory behavior. | REPLACE | New `tests/test_streaming.py::test_read_all_emits_before_source_exhaustion` | First item reaches output sink while controlled iterator blocks before item two. | SB-CLI-2, SB-DELIVERY-1, SB-DELIVERY-5 | S3 |
| L161 | `tests/test_streaming.py::test_streaming_peek_all` | Exactly one page does not cross pagination boundary or prove streaming. | REPLACE | New `tests/test_streaming.py::test_peek_all_crosses_page_boundary_without_mutation` | `PEEK_BATCH_SIZE + 1` exact bodies emit and remain pending. | SB-DELIVERY-4 | S3 |
| L162 | `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_broadcast_fires_transition_table[SM-REDIS-BROADCAST::EXACT-FILTER-MISSING]` | Correct count plus missing-queue absence does not prove selected insert. | KEEP+STRENGTHEN | Same node | `jobs` contains exactly seed plus one announcement; missing queue absent. | SB-BCAST-1, SB-BCAST-2, SB-BCAST-4 | S2 |
| L163 | `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_broadcast_fires_transition_table[SM-REDIS-BROADCAST::PATTERN-SUCCESS]` | Correct count plus unmatched state does not prove selected inserts. | KEEP+STRENGTHEN | Same node | Both matching queues contain one announcement; unmatched queue unchanged. | SB-BCAST-1, SB-BCAST-4 | S2 |
| L164 | `tests/test_performance_optimizations.py` | Entire module pins cache identity, hit counts, LRU size, and parser object. | DELETE | `tests/test_property_queue_names.py::test_grammar_valid_names_work_end_to_end`; `tests/test_cli_argument_parsing.py::test_complex_argument_combinations` | Public validation and parser behavior remain covered. | SB-DELIVERY-8, SB-CLI-3 | S6 |
| L165 | `tests/test_queue_coverage.py::test_ensure_core_lazy_initialization` | Does not assert pre-access state and duplicates lifecycle owner. | DELETE | `tests/test_db_connection_lifecycle.py::test_get_core_lazily_creates_and_reuses_sqlite_core` | Core is created on demand, reused, and usable. | SB-API-3 | S6 |
| L166 | `tests/test_queue_coverage.py::test_cleanup_finalizer_function` | Calls `_finalizer()` directly and mocks close; GC path is untested. | REPLACE | New `tests/test_db_connection_lifecycle.py::test_queue_gc_finalizer_closes_owned_connection_once` | Weakref clears after GC and real owned cleanup fires once. | SB-API-3 | S5 |
| L167 | `tests/test_queue_coverage.py::test_cleanup_finalizer_with_exception` | Direct private call and patched logger bypass real GC path. | REPLACE | New `tests/test_db_connection_lifecycle.py::test_queue_gc_finalizer_logs_cleanup_failure` | Actual GC path logs semantic cleanup failure and does not escape. | SB-API-3 | S5 |
| L168 | `tests/test_queue_coverage.py::test_queue_persistent_with_custom_runner_no_finalizer` | Asserts only private finalizer presence; no ownership behavior. | MERGE | L022 strengthens `tests/test_custom_runner_integration.py::test_injected_runner_is_caller_owned_across_close_and_finalizer` with real GC first | Queue GC/close never closes caller-owned runner; runner remains usable. | SB-API-3 | S6 |
| L169 | `tests/test_vacuum_compact.py::test_vacuum_with_compact_flag` | SQL spy freezes exact command and duplicates disk behavior. | DELETE | `tests/test_vacuum_compact.py::test_vacuum_compact_database_size_reduction` | Compact materially reduces real database size. | SB-OPS-6 | S6 |
| L170 | `tests/test_vacuum_compact.py::test_vacuum_without_compact_flag` | SQL-call absence is implementation detail and duplicates disk behavior. | DELETE | `tests/test_vacuum_compact.py::test_vacuum_compact_database_size_reduction` | Non-compact removes claimed rows without equivalent disk reclamation. | SB-OPS-6 | S6 |
| L171 | `tests/test_vacuum_compact.py::test_compact_with_no_claimed_messages` | Exact `VACUUM` spy duplicates real compaction owner. | DELETE | `tests/test_vacuum_compact.py::test_vacuum_compact_database_size_reduction` | Real compaction effect is observable on disk. | SB-OPS-6 | S6 |
| L172 | `tests/test_vacuum_compact.py::test_automatic_vacuum_runs_incremental_vacuum` | Private method and exact pragma pin scheduling policy explicitly left internal. | DELETE | `tests/test_message_claim.py::test_automatic_vacuum_trigger`; `tests/test_maintenance_policy.py::test_maintenance_schedule_becomes_due_at_interval_and_keeps_remainder` | Eligible maintenance removes claimed rows; scheduling due-state remains correct. | SB-OPS-6 | S6 |
| L173 | `tests/test_vacuum_compact.py::test_vacuum_claimed_messages_holds_core_lock` | Mock backend plus private `RLock._is_owned()` does not prove contention safety. | DELETE | `tests/test_vacuum_lock.py::test_concurrent_vacuum_skips_while_lock_held` | Real held lock prevents overlapping vacuum without corruption. | SB-OPS-6 | S6 |
| L174 | `extensions/simplebroker_redis/tests/test_redis_queue_rename.py::test_redis_rename_cleans_old_keys_and_queue_set` | Pins Redis key/hash/zset layout rather than rename behavior. | KEEP+RENAME | New name: `extensions/simplebroker_redis/tests/test_redis_queue_rename.py::test_redis_rename_preserves_pending_claimed_ids_and_removes_old_queue` | Claimed-inclusive public read shows exact bodies/IDs at new name; pending-only read proves the claimed row remains claimed; old name absent. | SB-OPS-4, SB-ID-5 | S5 |
| L175 | `extensions/simplebroker_redis/tests/test_redis_queue_rename.py::test_redis_rename_rejects_reserved_target_collision` | Manufactures private reserved key instead of real reservation. | KEEP+STRENGTHEN | Same node using live claim generator | Active target reservation rejects rename; source and target remain exact. | SB-OPS-4 | S5 |
| L176 | `extensions/simplebroker_redis/tests/test_redis_queue_rename.py::test_redis_rename_retargets_aliases_and_bumps_version` | Reads private meta hash/version field. | KEEP+STRENGTHEN | Same node using public alias-version accessor | Alias resolves to new name and public version advances. | SB-OPS-4, SB-OPS-5 | S5 |
| L177 | `extensions/simplebroker_redis/tests/test_redis_queue_rename.py::test_redis_rename_missing_source_does_not_create_new_keys` | Pins absence of private Redis keys/registry entry. | KEEP+STRENGTHEN | Same node | Missing rename returns zero; subsequent write to destination yields only that new row. | SB-OPS-4 | S5 |
| L178 | `tests/test_sqlite_maintenance_helpers.py` | Mock-only module duplicates real deletes and mixes unrelated no-backing defaults. | DELETE | `tests/test_batch_delete.py::test_delete_message_ids_empty_batch_is_noop`; `tests/test_delete_from_queues.py::test_delete_from_queues_empty_input_is_noop`; `tests/test_vacuum_compact.py::test_vacuum_compact_database_size_reduction` | Empty deletes preserve real state; vacuum effect is proven on disk. | SB-OPS-3, SB-OPS-6 | S6 |
| L179 | `tests/test_ext_imports.py::test_ext_imports` | Successful imports followed by `is not None` add no oracle. | MERGE | `tests/test_python_library_api_contract_sb_api.py::test_api_root_ext_commands_all_are_importable` | Every declared public export resolves. | SB-API-1, SB-API-11 | S6 |
| L180 | `tests/test_ext_imports.py::test_project_config_helpers_are_stable_across_public_modules` | Exact duplicate of canonical API-contract node. | MERGE | `tests/test_python_library_api_contract_sb_api.py::test_api_project_config_helpers_on_ext_and_project` | Public helper identities and memberships match. | SB-API-2, SB-API-11 | S6 |
| L181 | `tests/test_broadcast.py::test_broadcast_return_values` | Exit-only cases duplicate stronger state tests. | DELETE | `tests/test_broadcast.py::test_broadcast`; `tests/test_broadcast.py::test_broadcast_with_pattern`; `tests/test_broadcast.py::test_broadcast_with_pattern_no_matches`; `tests/test_broadcast.py::test_broadcast_no_queues` | Exit codes are paired with exact selected/nonselected queue state. | SB-BCAST-1, SB-BCAST-4, SB-CLI-1 | S6 |
| L182 | `tests/test_broadcast.py::test_broadcast_visible_to_new_connections` | Every CLI call already uses a new process/connection; duplicates `test_broadcast`. | DELETE | `tests/test_broadcast.py::test_broadcast` | Both queues contain exact original plus broadcast body via fresh CLI calls. | SB-BCAST-4 | S6 |
| L183 | `tests/test_batch_operations.py::TestBatchOperations::test_move_many_exactly_once` | Separate setup duplicates the same materialized contract. | MERGE | New parameterized `tests/test_batch_operations.py::TestBatchOperations::test_move_many_materialized_semantics[exactly_once]` | Exact returned, source, and destination sequences. | SB-DELIVERY-5 | S2 |
| L184 | `tests/test_batch_operations.py::TestBatchOperations::test_move_many_at_least_once` | Length-only source/destination checks permit wrong bodies. | MERGE | New parameterized `tests/test_batch_operations.py::TestBatchOperations::test_move_many_materialized_semantics[at_least_once]` | Exact returned, source, and destination sequences. | SB-DELIVERY-5 | S2 |
| L185 | `tests/test_project_scoping.py::TestCLIIntegration::test_parser_includes_init_command` | Private argparse traversal duplicates the useful parse assertion. | KEEP+STRENGTHEN | Same node without `_subparsers` inspection | `parse_args(["init"])` yields command `init`. | SB-CLI-3, SB-API-2 | S3 |
| L186 | `tests/test_project_scoping.py::TestCrossPlatformCompatibility::test_windows_drive_root_detection` | Duplicates the canonical boundary-detection node. | DELETE | `tests/test_project_scoping.py::TestFilesystemBoundaryDetection::test_filesystem_root_detection_windows` | Windows drive roots are recognized. | SB-API-2 | S6 |
| L187 | `tests/test_project_scoping.py::TestCrossPlatformCompatibility::test_unix_root_detection` | Duplicates the canonical boundary-detection node. | DELETE | `tests/test_project_scoping.py::TestFilesystemBoundaryDetection::test_filesystem_root_detection_unix` | Unix root is recognized. | SB-API-2 | S6 |
| L188 | `tests/test_project_scoping.py::TestPerformanceAndLimits::test_deep_directory_performance` | One-second wall-clock limit is unowned and duplicates deeper behavior. | DELETE | `tests/test_project_scoping.py::TestProjectDatabaseSearch::test_max_depth_limit` | 50-level search obeys explicit depth and finds target when allowed. | SB-API-2 | S6 |
| L189 | `tests/test_backend_plugin_resolution.py::test_legacy_runner_without_backend_plugin_still_looks_like_sqlite` | `not isinstance` is true by construction; no plugin fallback or operation runs. | DELETE | `tests/test_backend_plugin_resolution.py::test_non_aware_runner_with_resolved_target_uses_target_plugin` | Resolved target selects plugin and performs the supported path. | SB-API-11 | S6 |
| L190 | `tests/test_cli_rearrange_args.py::TestCLIMissingValues::test_complex_scenario_from_review` | Exact duplicate of literal `--cleanup` message regression. | DELETE | `tests/test_cli_rearrange_args.py::TestHelpHasNoSideEffects::test_dash_messages_are_still_protected` | Literal `--cleanup` is written and read back, never hoisted. | SB-CLI-3 | S6 |
| L191 | `tests/test_cli_watch.py::TestWatchCommand::test_watch_json_output` | Weaker duplicate of JSON watch test covering initial and later output. | DELETE | `tests/test_cli_watch.py::TestWatchCommand::test_watch_json_includes_timestamps` | Both exact bodies arrive with valid timestamp strings. | SB-CLI-4, SB-ID-1 | S6 |
| L192 | `tests/test_portability.py::test_chmod_windows_compatibility` | Mocks the forced-`chmod` warning path removed by the owner-authorized operator-permission correction; it no longer represents product behavior. | DELETE | `tests/test_portability.py::test_database_creation_respects_operator_umask`; existing Windows CI matrix | Real POSIX creation follows requested mode filtered by umask; Windows retains inherited ACL behavior without a chmod compatibility path. | Configuration guide, file-permission policy | S5 |
| L193 | `tests/test_product_section_registry_final_cutover.py::test_kernel_and_llms_list_every_canonical_product_spec` | Exact link-presence assertions are subsumed by the registry-derived owner/entry-link test. | DELETE | `tests/test_product_section_registry_final_cutover.py::test_registered_product_owners_and_entry_links_resolve` | Every registered spec exists and is linked from the root README, kernel, llms index, and spec index with its code family. | DOM-5 evidence ownership | S4 |

## No-Action Register

The audit considered but retains these shapes:

| Surface | Reason to keep |
|---------|----------------|
| `tests/test_phaselock.py` | OS/FFI error ordering and lock state are the adapter contract |
| transition-table firing tests | Payloads own assertions; the manifest binding is a documented repository contract |
| fork and cross-thread finalization probes | Real process/thread behavior proves safety that public readback alone cannot |
| release-workflow and irreversible-operation source gates | Negative source structure is the safety boundary |
| schema migration, partial-index, and query-plan tests | Storage compatibility or named performance behavior is explicitly owned |
| broadcast lock-order AST gate | Lock acquisition order is a deadlock invariant and is paired with real behavior |
| executable SQL-builder tests | They execute emitted SQL and guard a historically ungated example path |
| PG/Redis pool, ownership, atomicity, and narrow storage tests | Backend realization cannot always be inferred from core return values |
| both fuzz harnesses | They exercise parser/round-trip totality rather than duplicating examples |

## Dependency-Ordered Tasks

### S0. Freeze the evidence graph and execution baseline

- Files to touch: this plan and its execution log only.
- Record current collected node IDs and counts for core, PostgreSQL, and Redis.
- For every L001–L193 ledger row, run `rg` across all first-party surfaces:
  `docs/`, `.github/`, `README.md`, `CHANGELOG.md`, `pyproject.toml`, `llms.txt`,
  `tests/`, `fuzz/`, `bin/`, `simplebroker/`, `examples/`, and `extensions/`.
  Classify each match as firing evidence, a deliberate fixture literal, or a
  stale citation; update stale citations and append the exact successor mapping
  to the execution log.
- Record baseline targeted runtimes and three repeated runs for the current
  signal/process/concurrency groups so later flake claims are comparative.
- Stop if a deletion candidate is the only firing evidence for a contract and
  no stronger replacement is specified in this plan.
- Done signal: every deletion/rename has a successor or an explicit obsolete
  disposition; no citation is left ownerless.

### S1. Repair watcher, signal, lifecycle, and concurrency evidence

- Primary files:
  `tests/test_cli_watch.py`, `tests/test_move_after_exclusion.py`,
  `tests/test_watcher_burst_mode.py`, `tests/test_watcher_cleanup.py`,
  `tests/test_watcher_thundering_herd.py`,
  `tests/test_watcher_multiprocess.py`,
  `tests/test_watcher_concurrency.py`, `tests/test_watcher_edge_cases.py`,
  `tests/test_queue_config_defaults.py`, `tests/test_connection_config.py`, and
  `tests/test_multi_queue_watcher_example.py`.
- S1 owns the watcher-specific configuration replacements L094 and L127;
  S5 owns the non-watcher connection/configuration rows in the same file.
- Reuse `managed_subprocess`, readiness files/events, existing timing scaling,
  real QueueWatcher, and injected `PollingStrategy` seams.
- First record mutation witnesses: a subprocess that ignores SIGINT, an idle
  watcher whose drain counter advances on unrelated activity, a missing late
  worker, and a dropped processed ID must each fail the new oracle.
- Remove production-control-flow overrides. Instrument strategy calls or
  handler output, not `_drain_queue`, `_has_pending_messages`, `_dispatch`, or
  `_check_stop`.
- Replace sleeps used as readiness with events or bounded observable polling.
- Keep a small queue-isolation stress test distinct from the optimization
  claim. Herd mitigation requires a bounded irrelevant-drain oracle.
- Do not delete test-owned metrics in this slice. Land any needed real-watcher
  drain counter first; D6.1 then removes the obsolete metrics module.
- Stop if reliable evidence requires a production testing hook or new watcher
  API. That is outside this plan.
- Per-slice verification: targeted watcher files under SQLite, then shared
  PostgreSQL and Redis runs; repeat signal/herd/multiprocess cases three times.
- Done signal: no watcher test accepts forced termination, lost messages,
  absent dispatch, or unbounded irrelevant drain as success.

### S2. Repair delivery, move, broadcast, and conservation oracles

- Primary files:
  `tests/test_thread_safety.py`, `tests/test_queue_api_comprehensive.py`,
  `tests/test_batch_operations.py`, `tests/test_move.py`,
  `tests/test_move_integration.py`, `tests/test_move_claim_patterns.py`,
  `tests/test_move_by_id.py`, `tests/test_cli_move.py`,
  `tests/test_queue_move_watcher.py`, `tests/test_broadcast.py`,
  `tests/test_broadcast_integration.py`, and Redis broadcast transition tests.
- Reuse the canonical materialized-batch test and Queue state machine rather
  than creating a third delivery harness.
- Add exact conservation oracles: source plus destination IDs/bodies, no loss,
  no duplication, claimed state when relevant, and exact empty/nonempty exit.
- Drive atomic failure with an owner-level transaction/trigger fault while the
  real database transaction remains active. Do not mock the move operation.
- Tighten `with_timestamps=False` to require strings, not `str or tuple`.
- For broadcast, verify every selected queue and every nonselected queue, not
  only the returned affected count.
- Delete length-only, dead-branch, and sequential fake-concurrency cases after
  the stronger owners pass on all backends.
- Stop if a stronger assertion reveals a backend divergence not permitted by
  the specs. Record it as a separate product defect.
- Done signal: controlled message loss, selected-target omission, or partial
  move state fails a test in the owning suite.

### S3. Repair independent properties and CLI transparency

- Primary files:
  `tests/test_property_dump_load.py`, `tests/test_property_queue_names.py`,
  `tests/test_property_timestamp_validate.py`, `tests/test_after_flag.py`,
  `tests/test_security_fixes.py`, `tests/test_cli_edge_cases.py`,
  `tests/test_cli_validation.py`, `tests/test_commands_init.py`,
  `tests/test_streaming.py`,
  `tests/test_json_message_id_contract.py`, `tests/test_json_output.py`,
  `tests/test_message_by_timestamp.py`, `tests/test_commands_helpers.py`, and
  `tests/test_cli_rename.py`.
- Define reference models from spec literals or standard-library primitives,
  never from SimpleBroker's private selector/regex/formatter under test.
- Add exact timestamp boundary rows around midnight rather than comparing two
  exit codes far from all messages. Keep documented bare examples and explicit
  suffix equivalence; remove private threshold claims.
- Prove streaming with a controlled blocking iterator and a real page-boundary
  case. Keep actual stdout/output serialization real.
- Replace source AST JSON inventory with an exhaustive public-producer table.
- Separate traversal rejection, absolute-path acceptance, and message-size
  argv/stdin behavior. Use targets wholly inside `tmp_path`.
- Standardize diagnostics on exact stable fields plus semantic prose fragments.
- Stop if the canonical spec does not decide an expected value. Do not turn an
  observed quirk into a new test contract under this plan.
- Done signal: selector/formatter mutants and buffered emission fail; harmless
  prose rewording and equivalent internal refactors do not.

### S4. Repair benchmark and documentation evidence gates

- Primary files:
  `tests/test_benchmark.py`, `tests/test_performance.py`,
  `tests/test_persistence_io_contract_sb_io.py`,
  `tests/test_product_section_registry_final_cutover.py`,
  `tests/test_agent_kernel_contract.py`, and their machine-readable owners.
- Replace copied benchmark values with a committed, machine-readable result
  artifact under `benchmarks/results/`. It records the exact command, package
  version, host/CPU, date, operations, message size, vacuum setting, trials,
  and raw per-case results. Render or compare the README table from that
  artifact so the test checks one attributable source rather than two copied
  claims.
- Rename the serial mixed-operation benchmark to
  `test_sequential_mixed_cli_throughput` and preserve its existing budget. Real
  concurrency already has dedicated correctness owners; this benchmark should
  state what it measures instead of adding a second concurrency harness.
- Replace the queue-validation cache-hit oracle with
  `test_public_queue_name_validation_throughput_budget`, a budget at the public
  validation surface. Do not assert cache type, size, hits, or misses.
- Replace global keyword checks with parsed section, registry, link, and actual
  `--collect-only` integrity. Preserve exact enumerable manifest checks.
- Stop if the change would make a benchmark result artifact or new performance
  budget normative. That requires owner direction and possibly reclassification.
- Done signal: fabricated prose/numbers or a runner that omits a claimed suite
  fails, while harmless wording changes do not.

### S5. Replace adapter, lifecycle, configuration, and schema implementation pins

- Primary files:
  `tests/test_custom_runner_integration.py`,
  `tests/test_queue_connection_manager.py`, `tests/test_connection_config.py`,
  `tests/test_queue_coverage.py`, `tests/test_sqlite_schema.py`,
  `tests/test_portability.py`, `tests/test_default_handlers.py`,
  Redis rename/core behavior tests, and relevant existing lifecycle owners.
- Prove injected-runner ownership with decoy target A and runner target B;
  exercise public operations and continued runner usability.
- Use real dereference/GC only where automatic finalization remains a desired
  lifecycle proof. Explicit close remains the preferred public path.
- Preserve connection reuse/lifetime evidence without concrete core classes or
  setup-count assumptions.
- Prove partial configuration through actual size/timing/target behavior, not
  private snapshots alone.
- Replace exact bootstrap transaction text with early/middle/late fault points
  and no visible partial schema/meta state.
- Replace Redis layout assertions with claimed-inclusive rows, IDs, aliases,
  collisions, missing-source behavior, and cross-core visibility. Keep a
  narrow storage assertion only when the layout is an explicit migration or
  cleanup invariant.
- Stop if the replacement cannot distinguish ownership without a new public
  inspection surface. Record the residual and keep the narrow internal test.
- Done signal: equivalent internal classes, SQL construction, or key layout can
  change without breaking tests, while ownership/state defects still fail.

### S6. Delete and merge only after successors are live

The ledger is the deletion authority. Do not add a deletion based only on
similarity or coverage count. Execute these exact dependency batches:

- D6.1 obsolete-claim deletions with no successor: `L140`, `L146`, `L149`
  These rows own no product contract. S0 must still prove that no repository
  citation treats the node as firing evidence before deletion.
- D6.2 consolidation behind an existing successor: `L003`, `L004`, `L005`, `L008`, `L009`, `L010`, `L011`, `L012`, `L013`, `L014`, `L015`, `L016`
  `L017`, `L018`, `L023`, `L024`, `L034`, `L035`, `L036`, `L037`, `L038`, `L039`, `L040`, `L041`
  `L043`, `L057`, `L059`, `L061`, `L062`, `L063`, `L064`, `L065`, `L068`, `L069`, `L070`, `L071`
  `L072`, `L073`, `L074`, `L075`, `L076`, `L077`, `L078`, `L079`, `L080`, `L081`, `L082`, `L083`
  `L084`, `L090`, `L095`, `L096`, `L101`, `L102`, `L108`, `L109`, `L116`, `L122`, `L123`, `L124`
  `L128`, `L131`, `L132`, `L136`, `L143`, `L155`, `L156`, `L159`, `L164`, `L165`, `L168`, `L169`
  `L170`, `L171`, `L172`, `L173`, `L178`, `L179`, `L180`, `L181`, `L182`, `L186`, `L187`, `L188`
  `L189`, `L190`, `L191`, `L192`, `L193`
  For each row, run the named successor first, update every exact citation in
  the same change, delete or merge the predecessor, then rerun the successor
  and collect-only. Failure of any named successor blocks that row, not the
  rest of the batch.
- D6.3 consolidation behind a new successor: `L019`, `L020`, `L021`, `L054`, `L088`, `L091`, `L093`, `L110`, `L111`, `L112`, `L113`, `L137`
  `L138`, `L142`, `L147`, `L154`, `L183`, `L184`
  Each exact new owner must collect and pass in its owning S1-S5 slice before
  the predecessor is edited. Land the successor and deletion together only
  when the row's targeted backend/platform matrix is green. L154 specifically
  waits for L099's S5 portability successor.
- D6.4 evidence reconciliation covers every D6.1-D6.3 row. Update canonical
  spec verification tables, state-machine manifests, workflow selectors, and
  documentation node citations atomically. A dangling old ID or an uncollected
  new ID blocks the batch.
- If S0 finds that a listed predecessor is the only firing evidence and the
  ledger names no valid successor, stop and amend this reviewed plan. Do not
  improvise a deletion.
- Done signal: every D6 row has a passing successor or an explicitly obsolete
  claim; all repository evidence pointers resolve after collect-only.

### S7. Final cross-backend and traceability reconciliation

- Re-run targeted slices from current state, then the full core, PostgreSQL,
  Redis, Windows-relevant CI, static, typing, and document gates below.
- Compare collected inventory with the S0 ledger. Explain every removed or
  renamed node; do not treat a lower count as a regression by itself.
- Run the high-risk signal/concurrency subset three times from the final state.
- Perform independent review after S1, after S2/S3, and before completion.
- Update this plan's execution evidence and deviation/review logs. Update the
  Status Index to `completed` only when every required matrix and review passes.
- Done signal: all gates pass, all review findings are disposed, and no
  evidence citation or plan row is stale.

## Testing Plan

### Red proof for test-only remediation

Most strengthened tests target behavior that currently works, so a new test may
pass immediately. Failing-test-first uses a mutation witness rather than a
production bug requirement:

| Risk | Required defect witness |
|------|-------------------------|
| signal shutdown | child ignores SIGINT or requires helper escalation; new test fails |
| message conservation | omit or duplicate one returned ID; new exact-set oracle fails |
| dump selector | mutate include/exclude outcome while returned count remains plausible; independent model fails |
| broadcast | omit one selected insertion while returning the expected count; target-state oracle fails |
| streaming | buffer until iterator exhaustion; first-item-before-release oracle fails |
| move atomicity | abort during the real transaction; source/destination remain unchanged |
| watcher herd | wake/drain an unrelated watcher; idle-drain counter oracle fails |
| schema bootstrap | fail at each named setup phase; no partial schema/meta becomes visible |

Mutation witnesses may be temporary local edits reverted before the green run,
or narrow test doubles at the owner seam. Record the command and observed red
result in the execution log. Deletion-only and exact-duplicate slices use the
named-exit form: no red test is required when a stronger already-firing node is
identified and rerun before deletion.

### Anti-mocking posture

Must stay real:

- Queue/Broker operations and durable queue state
- SQLite transactions, files, cleanup enumeration, schema, and data version
- subprocess launch, readiness, signals, stdout/stderr, and exit codes
- watcher main loop and drain/dispatch path
- PostgreSQL and Redis services for shared/backend-specific claims
- dump/load serialization and public JSON producers

Allowed narrow seams:

- backend plugin method that owns a filesystem fault
- deterministic clock or random source
- recording `PollingStrategy` that does not replace watcher control flow
- external command collaborator used by a shipped script test
- temporary SQLite trigger/fault that leaves the transaction path real

Forbidden patterns include global filesystem predicate patches, overriding
production watcher drain/check methods, calling `_finalizer()` as a substitute
for GC, computing expected results with the production helper, fabricating
callback metadata and asserting it, and accepting a set of outcomes when the
spec owns one.

## Verification and Gates

Per-slice commands use exact node/file selections and disable unrelated xdist
when signal/process ordering requires it. Final required commands:

```bash
uv run pytest
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
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis \
  --config-file pyproject.toml
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

The default pytest command excludes `benchmark` tests. Run every changed
benchmark serially and explicitly:

```bash
uv run pytest -o addopts='' -q -m benchmark \
  tests/test_performance.py::test_public_queue_name_validation_throughput_budget \
  tests/test_performance.py::test_sequential_mixed_cli_throughput
```

Collection/evidence gates:

```bash
uv run pytest -o addopts='' --collect-only -q tests
uv run pytest -o addopts='' --collect-only -q \
  extensions/simplebroker_pg/tests extensions/simplebroker_redis/tests
rg -n 'tests/test_.*::test_' docs .github README.md CHANGELOG.md \
  pyproject.toml llms.txt tests fuzz bin simplebroker examples extensions
```

Windows acceptance uses the existing `.github/workflows/test.yml` matrix,
including its separate `windows_serial` streaming phase. Signal assertions may
remain platform-specific only where the governing CLI contract permits it;
helper escalation is never a passing correctness result.

The implementation is not completion-ready if PostgreSQL/Redis service tests or
required Windows jobs are skipped without an explicit external blocker. A
blocker is recorded, not converted into a completion claim.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Configuration guide file-permission policy; L099/L154 | Replace exact-`0600` implementation pins with a real umask/operator-policy firing test, then remove the duplicate init-mode test | A real `BrokerDB` creation under umask `0002` became `0600` because `simplebroker/db.py` explicitly called `chmod(0600)` | Owner authorized production alignment on 2026-08-10: remove only the post-create chmod/warning path; keep SQLite creation, existing-file modes, directory policy, and all non-permission behavior unchanged. New files follow SQLite's requested mode masked by umask; existing files are not migrated. Rollback affects future creations only because modes already created under either version persist | None; the winning configuration and implementation docs already assign mode/ACL/umask policy to the operator. Verify real creation under controlled umask, reopen preservation, and Windows behavior through the existing matrix |
| Configuration guide file-permission policy; phase-lock companions | Keep the test-remediation change limited to database creation | The same forced-private policy existed in `_phaselock.py`: lock acquisition rewrote the stable lock to `0600`, and atomic fallback status publication requested `0600` for every temporary generation | Owner explicitly extended the operator-owned policy to phase-lock companion files on 2026-08-10. Remove the lock-file chmod and request the ordinary `0666` creation mode for the existing `O_EXCL` status temporary file, filtered by umask. Preserve advisory-lock mechanics, atomic replace, fsync, cleanup, and all names. Existing stable lock modes are preserved; each new status generation follows the then-current umask. Rollback affects future lock/status creation or publication only | None; add real subprocess umask tests for fresh lock/status artifacts and existing stable-lock mode preservation. Windows continues to rely on inherited ACLs and the existing matrix |

## Independent Review Loop

Review stages:

1. plan review before implementation;
2. scoped review after S1 watcher/signal work;
3. scoped review after S2/S3 integrity and transparency work;
4. final completion review over the full diff and current gate evidence.

Different-family review is preferred. If only a same-family reviewer is
available, use a separate review context and disclose that limitation. The
reviewer receives this plan, the governing specs, the audit remediation ledger,
the touched files, and current verification output. The stance is:

> Existence-check every named file, test, seam, citation, and command first.
> Then look for false deletions, circular replacement oracles, mock-heavy tests,
> missing backend/platform gates, and process that does not improve defect
> detection. Answer PASS or BLOCKED: could a zero-context engineer implement
> this confidently without changing product behavior or reducing reliability?

| Date | Stage | Reviewer | Result | Disposition |
|------|-------|----------|--------|-------------|
| 2026-08-10 | Plan review round 1 | same-family isolated subagent | BLOCKED | F1 malformed node ID and F3 incomplete benchmark/type/format gates corrected; F2 exact durable ledger and F4 different-family review remain open |
| 2026-08-10 | Plan review round 2 | same-family isolated subagent | BLOCKED | Fourteen new-successor merge/delete rows were misclassified in D6.2; moved to D6.3 and L154 now depends explicitly on L099 |
| 2026-08-10 | Plan review round 3 | Claude CLI, fresh tool-less process | BLOCKED | B1 matched round 2; B2 gave L054/L094/L127 explicit file/slice ownership; B3 enumerated all ledger contract and state-machine codes; dual-slice and final evidence-scan findings also corrected. The skill preflight reported a false auth miss, but the actual CLI review succeeded as the owner predicted |
| 2026-08-10 | Plan review round 4 | same-family isolated subagent | BLOCKED | Final citation scan omitted test/fuzz/code surfaces and missed a stale watcher-metrics literal in `tests/test_dev_scripts.py`; S0 and S7 now scan every first-party surface and classify every match |
| 2026-08-10 | Plan review round 5 | Claude CLI, fresh tool-less process | PASS | Full-plan review found no remaining behavioral, sequencing, gate, or product-scope blocker after round-3 remediation; it noted only non-blocking clarity residuals |
| 2026-08-10 | Plan review round 6 | same-family isolated subagent | PASS AT REVIEWED SCOPE | Reviewed the then-declared 191-row inventory; the owner later found two unauthorized deletions outside that inventory, now recorded as L192/L193 |
| 2026-08-10 | Plan review round 7 | Claude CLI, fresh tool-less incremental process | PASS | Confirmed the final S0/S7 first-party scan expansion preserves the full-plan PASS and closes the stale fixture-literal gap |
| 2026-08-10 | Final implementation review | same-family isolated subagent | PASS | Class-4 permission scope, behavior-first oracles, deterministic watcher negatives, benchmark provenance, evidence citations, and plan truth reconciled; Windows and the broken editable Weft checkout remain explicit external acceptance boundaries |
| 2026-08-10 | Owner pre-landing review | repository owner | BLOCKED | Redis rename did not distinguish claimed from pending; move-all omitted source emptiness; L192/L193 deletions were absent from the ledger; required untracked dependencies were not staged |
| 2026-08-10 | Owner pre-landing remediation | primary agent | PASS | Required assertions and ledger rows added; optional watcher-state gap strengthened without encoding the observed false exact-once log claim; artifact and plan dependency paths staged |

Every finding is accepted and incorporated, rejected with evidence, or marked
out of scope with a concrete reason before implementation or closure proceeds.

## Out of Scope

- Fixing production defects beyond the two owner-authorized permission-policy
  corrections recorded in the deviation log.
- Changing CLI flags, exit codes, JSON schemas, Queue APIs, delivery semantics,
  broadcast semantics, timestamp grammar, storage format, or backend behavior.
- Establishing new performance budgets, metrics APIs, mutation infrastructure,
  or public testing hooks.
- Rewriting all tests into one style or eliminating every private assertion.
- Increasing coverage percentage or test count as an end in itself.
- Coalescing completed plans or revising durable testing guidance. Any reusable
  lesson discovered during implementation is evaluated separately at closeout.
- Releasing or publishing packages.

## Fresh-Eyes Review

- [x] The plan distinguishes false assurance, brittleness, duplication, and
  justified internal contracts.
- [x] Every audit ledger row L001–L193 has a definitive disposition and owning
  slice.
- [x] Replacements precede dependent deletions.
- [x] The plan states what must remain real and what may be mocked.
- [x] Concurrency and signal tests use readiness and parent-visible outcomes.
- [x] Evidence citations are treated as a dependency graph, not prose cleanup.
- [x] A production defect stops and reclassifies the work.
- [x] Core, Windows, PostgreSQL, and Redis verification is explicit.
- [x] No normative spec or production behavior change is hidden in the plan.
- [x] No new framework, abstraction, or performance contract is proposed.

## Execution Log

Implementation is complete. On 2026-08-10 the owner directed closure and
accepted two explicitly recorded external residuals: Windows inherited-ACL
coverage remains owned by the existing CI matrix, and the editable sibling
Weft checkout must repair its own broken `taskspec` exports before its
regression module can collect locally. Neither residual weakens the locally
verified SimpleBroker behavior or changes the published contract.

| Date | Slice | Evidence | Result |
|------|-------|----------|--------|
| 2026-08-10 | Audit baseline | 218 modules; 80,238 lines; 3,101 collected cases; two fuzz harnesses; 36 representative weak tests passed; independent audit adjudication completed | Baseline recorded; plan drafting began |
| 2026-08-10 | Plan structure | 193 sequential unique ledger rows after owner review added L192/L193; closed dispositions: 73 DELETE, 37 MERGE, 36 REPLACE, 44 KEEP+STRENGTHEN, 3 KEEP+RENAME; all 110 DELETE/MERGE rows appear exactly once across D6.1-D6.3; no dual-slice rows | PASS after pre-landing correction |
| 2026-08-10 | Selector and reference validation | 187 candidate selectors were originally reviewed; owner review added the omitted L192/L193 selectors for 189 total plus four whole-module targets. Both added predecessors exist in the baseline diff, and both named successors collect and pass. Independent review collected the original 101 unique successor selectors into 112 cases; full baseline core/extension collection returned 3,101 cases; eight spec backlinks resolve | PASS after pre-landing correction |
| 2026-08-10 | Repository gates | `check-dom15-fixtures`, `check-plan-context`, `check-doc-paths`, tracked and untracked `git diff --check` | PASS |
| 2026-08-10 | Independent plan review | Same-family repository review and fresh tool-less Claude full/incremental reviews; all blocked findings incorporated before final passes | PASS; implementation authorized after review |
| 2026-08-10 | S0 collection baseline | Core: 2,674 nodes (2,660 selected, 14 deselected by default); PostgreSQL: 180; Redis: 247; combined inventory remains 3,101 | PASS at `b58ef6619927812adfb6d03d2d1838ab421449f1` |
| 2026-08-10 | S0 repeated signal/process/concurrency baseline | Three serial runs over CLI watch, worker signal, watcher SIGINT transitions, thread safety, watcher concurrency, multiprocess, and herd suites: 92 passed in 33.36s, 33.14s, and 32.83s | PASS; zero failures across three runs |
| 2026-08-10 | S0 exact candidate citation scan | Historical references to L043 are deliberate records; L049, L051, L090, and L108 keep the same firing node; live successor updates are required for L140 dev-script fixture literals, L142/L147/L148 Ruff registry entries, L150 `[SB-ID-1]`, L169 `[SB-OPS-6]`, and L191 `[SB-CLI-4]` | S6 obligations frozen before deletion/rename |
| 2026-08-10 | S1 watcher and concurrency remediation | Replaced sleep/permissive oracles with readiness, barriers, real watchers, exact delivery, and parent-visible counters; SQLite and service-backed focused suites plus repeated high-risk runs | PASS; obsolete watcher metrics module removed |
| 2026-08-10 | S2 delivery and integrity remediation | Exact body/ID conservation, order-neutral multisets, claimed-inclusive state, deterministic concurrent moves, and real post-mutation rollback fault | PASS after independent review blockers were corrected |
| 2026-08-10 | S3 CLI/property remediation | Independent dump selector model, semantic JSON/error fields, incremental streaming proof, and durable CLI state | PASS after independent review blockers were corrected |
| 2026-08-10 | S4 benchmark and evidence remediation | Full 36-case best-of-three matrix rerun with exact command; raw attributed artifact added under `benchmarks/results/`; README table and highlighted SQLite claims derived from it | PASS locally; benchmark artifact contract test green |
| 2026-08-10 | S5 adapters and production permission correction | Real lifecycle/GC/backend tests; database forced `0600` removed; subprocess umask cases prove fresh `0644`/restrictive `0600` and existing-mode preservation | PASS; independent production review incorporated |
| 2026-08-10 | S6 deletion and evidence reconciliation | All ledger deletions/merges applied successor-first; obsolete modules removed; canonical spec/manifests, Ruff registry, complexity map, and fixture literals reconciled | PASS in focused collection and static gates |
| 2026-08-10 | Phase-lock permission correction | Removed stable-lock chmod; status temp requests ordinary `0666` filtered by umask; real subprocess mode/preservation tests and ten repeated contention rounds | PASS; independent review confirmed locking, replace, fsync, and cleanup unchanged |
| 2026-08-10 | S7 local full-matrix attempt | Core reached 2,540 passes, PostgreSQL 1,092, and Redis 1,085 before collection failed only in the editable sibling Weft checkout, whose `taskspec` package cannot export symbols required by its own modules | External sibling blocker recorded; clean reruns exclude only `tests/test_weft_sqlite_stop_corruption_regression.py`; Windows remains CI-only |
| 2026-08-10 | S7 clean local matrix | With only the externally broken Weft regression module excluded: core 2,541 passed; PostgreSQL shared 1,092 plus extension 175 passed; Redis shared 1,085 plus extension 246 passed. Core collection is 2,572 and extension collection is 427. Ruff, format, suppression registry, production/core/PG/Redis mypy, DOM-15, plan-context, doc-path, benchmark, and diff gates pass | PASS locally; three repeated final watcher runs passed; active status retained for Windows CI, external Weft repair, and uncommitted worktree |
| 2026-08-10 | Owner pre-landing review remediation | Added pending-only Redis rename state, move-all source emptiness, oversized watcher discarded-state proof, and ledger rows L192/L193. An attempted exact-once log oracle fired red because the configured default handler emits a second semantic error; the false ledger promise was corrected without pinning log internals | SQLite/PG/Redis focused behavior passed; Ruff, format, targeted mypy, DOM-15, plan-context, doc-path, ledger sequence, and staged/unstaged diff checks passed; artifact and plan paths staged |
| 2026-08-10 | Owner-directed closure | Owner requested close and commit with a clean worktree after the pre-landing review passed. Local core, PostgreSQL, Redis, benchmark, typing, lint, formatting, documentation, collection, and repeated-concurrency evidence is recorded above | COMPLETED; Windows CI and the sibling Weft import repair remain disclosed external follow-ups, not hidden green claims |
