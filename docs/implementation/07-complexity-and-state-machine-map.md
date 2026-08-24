# Complexity and State-Machine Map

## Purpose and Scope

This document is the durable implementation inventory for complexity findings
and named state machines. It records ownership, the reason for the planned
disposition, and the proof boundary. It does not define product behavior and it
is not a suppression registry.

The inventory baseline is the 2026-07-29 audit against
`1324a1f647b7c8fc9b14a6aed6bae696aefe62ea`:

```bash
uv run --frozen --no-sync ruff check --select C901 --output-format json .
```

The audit produced exactly 76 findings: 16 P1, 35 P2, and 25 P3. Line numbers
below identify that historical baseline. The final reconciliation later in
this document records current owners and scores. Stable suppression-group IDs
join local directives to [DOM-10.1.1]'s human-owned reasons and approved
cardinalities. `bin/ruff_suppression_index.py` derives the symbol-keyed
location index from Ruff's reported `noqa_row` after C901 activation.

## Governing Contracts

- `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1]
  owns C901 visibility and auditability.
- The same spec's [DOM-10.1.1] owns every adopted suppression and its durable
  reason.
- The same spec's [DOM-10.2] owns the transition-table requirement.
- `docs/specs/10-cli.md` and `docs/specs/11-delivery.md`
  continue to own affected product behavior.
- `docs/specs/13-message-identity.md` `[SB-ID-1]` through
  `[SB-ID-5]` owns message identity and allocation behavior.

## Change Rules

P1 means a near-term defect fix or a clear ownership-seam refactor. P2 means an
owner-local improvement in the relevant slice. P3 means retain the cohesive
function and register a reasoned exception; it does not waive missing tests or
known defects. A P1 or P2 function that remains above 10 after sound work must
use the same reviewed exception path as P3. No function should be split merely
to improve its score.

This inventory must change with a classification, owner, or proof boundary.
It must not be edited as a substitute for updating the exact suppression
registry.

## C901 Disposition Inventory

### Production, extensions, tools, and examples

| Baseline location | Function and score | Disposition | Planned outcome |
|-------------------|--------------------|-------------|-----------------|
| `.github/scripts/combine_coverage.py:132` | `_wait_for_stable_sources` (19) | P1 | Name the inspection result while retaining one polling and deadline owner. |
| `bin/coalesce-check:100` | `main` (14) | P2 | Extract owner-local reporting or decision helpers only where they clarify the command flow. |
| `bin/release.py:1403` | `repository_settings_issues` (23) | P3 | Retain the cohesive repository checklist and register its proof and rejected splits. |
| `bin/release.py:2182` | `_run_batch_release` (20) | P2 | Clarify batch phase ownership while preserving synchronized release ordering. |
| `bin/release.py:2308` | `main` (25) | P1 | Extract the single-target workflow and share only genuine planning with batch release. |
| `examples/async_pooled_broker.py:232` | `PooledAsyncSQLiteRunner.run` (12) | P2 | Improve the runner-local operation boundary without separating coupled transaction handling. |
| `examples/async_pooled_broker.py:717` | `stream_read` (19) | P1 | Characterize delivery transitions, then split fixed modes into named private generators. |
| `examples/multi_queue_patterns.py:86` | `pattern_2_priority_simulation` (12) | P2 | Name coupled scheduling operations inside the priority example. |
| `examples/multi_queue_patterns.py:309` | `pattern_5_monitoring` (16) | P2 | Name coupled monitoring-dispatch operations inside the example. |
| `examples/multi_queue_watcher.py:63` | `MultiQueueWatcher.__init__` (15) | P1 | Fix handler carry-over, then extract validation and entry construction. |
| `examples/reference_reactor.py:889` | `BaseReactor._drain_queue` (11) | P3 | Retain scheduling and lease decisions together; add explicit transition coverage. |
| `examples/sqlite_connect.py:314` | `validate_safe_path_components` (16) | P3 | Retain the standalone copyable validation checklist. |
| `examples/sqlite_connect.py:418` | `validate_database_path` (15) | P1 | Use SQLite-managed read-only validation and prove live-WAL lock behavior. |
| `extensions/simplebroker_pg/simplebroker_pg/plugin.py:741` | `vacuum` (11) | P3 | Retain the coupled lease, lock, delete, maintenance, and cleanup protocol. |
| `extensions/simplebroker_pg/simplebroker_pg/runner.py:142` | `_SharedActivityListener._run` (14) | P2 | Clarify listener states inside the listener owner; keep routing and failure publication local. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py:1333` | `find_message_ids` (13) | P3 | Retain the cohesive bounded Redis scan and selector semantics. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py:1409` | `broadcast` (36) | P1 | Name selector and Lua-result states; retain one owner for atomic retry behavior. |
| `extensions/simplebroker_redis/simplebroker_redis/plugin.py:438` | `cleanup_target` (11) | P2 | Clarify target-cleanup phases without creating a second cleanup path. |
| `simplebroker/_backends/sqlite/schema.py:179` | `ensure_schema_v3` (12) | P3 | Retain transaction, repair, rollback, and version publication together. |
| `simplebroker/_backends/sqlite/validation.py:14` | `validate_database` (19) | P3 | Retain the ordered database-validation checklist. |
| `simplebroker/_constants.py:279` | `_validate_safe_path_components` (17) | P1 | Extract same-file component and dangerous-character helpers; preserve error order. |
| `simplebroker/_constants.py:532` | `load_config` (19) | P1 | Use one private field schema for environment and override coercion. |
| `simplebroker/_dump.py:155` | `load_lines` (19) | P3 | Retain streaming parse order and partial durable mutation semantics; add transition coverage. |
| `simplebroker/_phaselock.py:137` | `_darwin_xattr_provider` (13) | P3 | Retain provider discovery, ERANGE handling, and process cache initialization together. |
| `simplebroker/_phaselock.py:282` | `_AdvisoryLock.acquire` (12) | P2 | Name local lock-wait decisions while preserving cancellation and diagnostics ordering. |
| `simplebroker/_phaselock.py:597` | `PhaseLockService.run_phases` (11) | P3 | Retain acquisition, marker selection, cancellation, and durable completion together. |
| `simplebroker/_retry.py:258` | `execute_retry` (11) | P3 | Retain the cohesive retry policy and callback ordering. |
| `simplebroker/_scripts.py:411` | `_extract_pytest_runner_overrides` (12) | P3 | Retain the ordered runner-override parser. |
| `simplebroker/_scripts.py:712` | `packaging_smoke_main` (12) | P2 | Clarify command phases without weakening the black-box packaging proof. |
| `simplebroker/_timestamp.py:298` | `TimestampGenerator.validate` (11) | P3 | Retain public timestamp precedence in one readable parser. |
| `simplebroker/_timestamp.py:390` | `_parse_with_unit_suffix` (13) | P2 | Consolidate repeated unit conversion inside the parser owner. |
| `simplebroker/_timestamp.py:525` | `_parse_numeric_timestamp` (11) | P3 | Retain numeric precedence and range diagnostics together. |
| `simplebroker/cli.py:905` | `main` (69) | P1 | Separate parsing, global actions, SQLite preparation, and dispatch behind one public entry. |
| `simplebroker/commands.py:421` | `_process_queue_fetch` (13) | P3 | Retain ordered fetch, output, and closed-pipe handling together. |
| `simplebroker/commands.py:896` | `cmd_move` (12) | P2 | Close the bounded generator explicitly and clarify command-local result handling. |
| `simplebroker/commands.py:1167` | `cmd_watch` (17) | P2 | Name watch callbacks and shutdown operations without separating their shared lifecycle. |
| `simplebroker/commands.py:1293` | `cmd_init` (11) | P2 | Clarify initialization steps while preserving exact diagnostic precedence. |
| `simplebroker/db.py:568` | `DBConnection.get_connection` (11) | P2 | Name owner-local connection acquisition decisions; retain retry and registry coupling. |
| `simplebroker/db.py:770` | `DBConnection.cleanup` (17) | P1 | Name ownership snapshot, drain, and best-effort close operations inside `DBConnection`. |
| `simplebroker/db.py:1324` | `BrokerCore.sidecar` (17) | P3 | Retain transaction arbitration and failure precedence in one ownership boundary. |
| `simplebroker/db.py:1780` | `BrokerCore._yield_transactional_batches` (23) | P3 | Retain generator ownership, poison publication, and transaction outcomes together. |
| `simplebroker/db.py:3035` | `BrokerCore.broadcast` (15) | P2 | Clarify selector and insertion operations inside the SQL broadcast owner. |
| `simplebroker/sbqueue.py:764` | `Queue.move` (11) | P3 | Retain public move validation, bounded consumption, and result semantics together. |
| `simplebroker/sbqueue.py:1271` | `Queue.stream_messages` (14) | P2 | Name delivery-mode operations without creating parallel stream implementations. |
| `simplebroker/watcher.py:1179` | `PollingStrategy.wait_for_activity` (12) | P3 | Retain native waiter, hints, burst/backoff, replacement, and stop logic together. |

The Queue return-shaping branches remain one cohesive runtime path per method.
`@overload` declarations describe literal flag selections to static checkers;
they add no dispatch registry or alternate implementation. High-level
`Queue.move` keeps validation, bounded generator closure, and conversion to
the existing ordinary `{message, timestamp}` dictionary in the same owner.
The public `MovedMessage` `TypedDict` names that shape without changing the
runtime value. The registered `Queue.move` and `Queue.stream_messages`
complexity dispositions therefore remain unchanged.

### Tests and reusable test infrastructure

| Baseline location | Function and score | Disposition | Planned outcome |
|-------------------|--------------------|-------------|-----------------|
| `extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py:189` | `test_vacuum_compacts_after_deleting_claimed_batches` (11) | P3 | Retain the end-to-end vacuum contract scenario. |
| `tests/backend_benchmark.py:78` | `BenchmarkSettings.validate` (13) | P3 | Retain the cohesive benchmark-settings checklist. |
| `tests/conftest.py:880` | `run_cli` (15) | P2 | Clarify CLI coverage staging and subprocess result handling without mocking publication. |
| `tests/helper_scripts/cross_thread_generator_probe.py:84` | `_execute_probe` (21) | P2 | Name actor phases while keeping owner-thread and foreign-thread coordination explicit. |
| `tests/helper_scripts/cross_thread_generator_probe.py:107` | `owner` (11) | P2 | Name owner-thread result publication while retaining exact exception capture. |
| `tests/helper_scripts/cross_thread_generator_probe.py:289` | `_execute_sidecar_probe` (12) | P3 | Retain the complete sidecar actor protocol in one debuggable probe. |
| `tests/helper_scripts/cross_thread_generator_probe.py:500` | `_queue_close_probe_child` (11) | P2 | Clarify child protocol phases without hiding process-boundary diagnostics. |
| `tests/helper_scripts/managed_subprocess.py:222` | `managed_subprocess` (24) | P1 | Give `ManagedProcess` one idempotent escalation and reader-cleanup owner. |
| `tests/helper_scripts/watcher_sigint_script_improved.py:15` | `main` (13) | P1 | Replace implicit readiness with a deterministic parent-child protocol. |
| `tests/test_backend_plugin_resolution.py:344` | `test_non_aware_runner_with_resolved_target_uses_target_plugin` (22) | P3 | Retain the full target-resolution integration scenario. |
| `tests/test_no_dependencies.py:31` | `test_no_external_imports` (13) | P2 | Name AST-scan classifications while retaining one dependency-policy assertion. |
| `tests/test_process_broker_session.py:811` | `test_failed_core_creation_releases_any_runner_lease` (18) | P2 | Extract fixture operations only where failure and lease ownership remain visible. |
| `tests/test_process_broker_session.py:1016` | `test_non_sqlite_core_creation_after_close_does_not_retain_runner` (13) | P2 | Clarify post-close setup while retaining the real session and runner. |
| `tests/test_race_condition_analysis.py:87` | `test_database_creation_timing` (19) | P1 | Rewrite as an exact protocol assertion or delete when stronger coverage subsumes it. |
| `tests/test_race_condition_analysis.py:214` | `test_concurrent_database_access` (11) | P1 | Require every intended worker and clean shutdown, or delete the diagnostic test. |
| `tests/test_runner_error_handling.py:498` | `test_schema_setup_refreshes_idle_budget_after_forward_progress` (11) | P2 | Reduce setup noise while retaining real progress-budget behavior. |
| `tests/test_watcher.py:454` | `test_graceful_shutdown_sigint` (14) | P1 | Treat SIGTERM or SIGKILL fallback as failure. |
| `tests/test_watcher_burst_mode.py:494` | `test_polling_jitter` (13) | P2 | Separate repeated sampling assertions while retaining the real strategy. |
| `tests/test_watcher_burst_mode.py:708` | `test_burst_mode_state_transitions` (15) | P3 | Retain the coupled burst-mode transition scenario and add an explicit table. |
| `tests/test_watcher_concurrency.py:471` | `test_multiple_peek_watchers` (11) | P1 | Require every watcher to observe the exact expected sequence. |
| `tests/test_watcher_concurrency.py:553` | `test_concurrent_writes_during_watch` (16) | P2 | Name actor setup and result checks while preserving the real concurrent path. |
| `tests/test_watcher_multiprocess.py:32` | `watcher_process` (14) | P2 | Name worker phases while retaining process-local diagnostics and cleanup. |
| `tests/test_watcher_multiprocess.py:162` | `lock_test_process` (13) | P2 | Name lock-worker phases while retaining process-boundary result capture. |
| `tests/test_watcher_multiprocess.py:266` | `test_multiprocess_single_queue` (19) | P2 | Share only genuine process orchestration; preserve the full single-queue proof. |
| `tests/test_watcher_multiprocess.py:404` | `test_multiprocess_separate_queues` (21) | P2 | Share only genuine process orchestration; preserve queue-isolation proof. |
| `tests/test_watcher_multiprocess.py:702` | `test_multiprocess_unrelated_write_does_not_drain_idle_watchers` (20) | P2 | Share only genuine process orchestration; preserve the real unrelated-wakeup and idle-drain proof. |
| `tests/test_watcher_multiprocess.py:661` | `test_multiprocess_graceful_shutdown` (19) | P2 | Share only genuine process orchestration; preserve exact shutdown outcomes. |
| `tests/test_watcher_multiprocess.py:917` | `test_multiprocess_contention_preserves_exact_delivery` (17) | P2 | Share only genuine process orchestration; preserve real contention and exact delivery conservation. |
| `tests/test_watcher_race_conditions.py:472` | `test_multiple_queues_concurrent_activity` (14) | P2 | Name actor setup and assertions while preserving concurrent queue behavior. |
| `tests/test_watcher_race_conditions.py:745` | `test_pre_check_database_contention` (12) | P2 | Clarify contention setup while retaining the real database boundary. |

## Final C901 Reconciliation

The implementation pass reduced the raw inventory from 76 findings to 53.
The 2026-08-07 integral-only timestamp grammar then removed the obsolete float
branches from both retained parser owners, reducing the live inventory to 51.
The 2026-08-10 test-signal remediation then replaced a test-owned herd
scenario with smaller real-watcher behavior and reduced the live inventory to
50.
All 26 removed findings crossed a real same-owner phase, validation, cleanup,
dispatch, or contract-narrowing seam. The remaining 50 are reviewed P3 exceptions in
[DOM-10.1.1], including former P2 candidates whose executable transition
contracts showed that further splitting would separate live state from its
failure or cleanup order.

| Baseline owner and score | Final owner and score | Outcome |
|--------------------------|-----------------------|---------|
| coverage settlement (19) | `bin.coverage_combine._wait_for_stable_sources` (9) | Moved from the CI entry script to an importable tooling owner; named inspection and deadline adjudication. |
| `coalesce-check.main` (14) | `main` (2) | Extracted SHA classification, cue validation, lesson derivation, and reporting. |
| `release.main` (25) | `main` (5); `_run_single_release` (21) | Public dispatch is simple; the single workflow remains a registered cohesive safety-order owner. |
| pooled runner `run` (12) | `run` (2) | Shared one connection-local SQL execution and exception-translation path. |
| async `stream_read` (19) | `stream_read` (7) | Split peek, exactly-once, at-least-once, and single-message generators; public close propagates to the selected generator. |
| `MultiQueueWatcher.__init__` (15) | `__init__` (4) | Fixed handler carry-over; named validation and queue-entry construction. |
| safe path validation (17) | `_validate_safe_path_components` (6) | Named dangerous-character and component checks without changing error order. |
| `load_config` (19) | `load_config` (2) | One 32-field schema plus named default-path and project-config validation phases. |
| advisory `acquire` (12) | `acquire` (10) | Named the shared lock-retry decision while keeping acquisition ownership local. |
| `packaging_smoke_main` (12) | `packaging_smoke_main` (5) | Named build, artifact inspection, install, and smoke phases. |
| `TimestampGenerator.validate` (11) | `validate` (9) | Removed float fallback and made each integral grammar's rejection/precedence explicit. |
| unit-suffix parser (13) | `_parse_with_unit_suffix` (7) | Consolidated suffix conversion and made the decimal-only suffix boundary explicit without changing parser precedence. |
| `_parse_numeric_timestamp` (11) | `_parse_numeric_timestamp` (5) | Removed fractional numeric branches; the owner now performs only decimal-integer unit conversion and range checks. |
| `cli.main` (69) | `main` (10) | Named parse, global action, target preparation, and explicit command-family dispatch phases. |
| `cmd_move` (12) | `cmd_move` (5) | Named all/next modes and explicit bounded-generator closure. |
| `cmd_watch` (17) | `cmd_watch` (10) | Kept input resolution and the stateful callback as named subproblems; recomposed banner selection, watcher construction, and ordered flush/stop cleanup into the lifecycle owner after locality review. |
| `cmd_init` (11) | `cmd_init` (2) | Named SQLite and non-SQL target initialization. |
| `DBConnection.get_connection` (11) | `get_connection` (6) | One retry owner for normal and shared connection acquisition. |
| `DBConnection.cleanup` (17) | `cleanup` (10) | Atomic ownership drain plus one best-effort close path. |
| SQL `BrokerCore.broadcast` (15) | `broadcast` (6) | Named selector normalization and in-transaction target selection. |
| `Queue.stream_messages` (14) | `stream_messages` (10) | One shared iterator-close path across every delivery mode. |
| dependency import scan (13) | `test_no_external_imports` (1) | Both dependency gates share parsed-source and absolute-import traversal. |
| setup idle-budget test (11) | test (6) | Shared setup runner, plugin, and minimal-core fixtures without mocking the budget owner. |
| managed subprocess context (24) | `managed_subprocess` (7) | `ManagedProcess.close` is the one idempotent escalation and reader-cleanup owner. |

Configuration has one deep resolution seam in `_constants.py`.
`load_config()` is the strict fresh environment parser; `resolve_config()` is
the compatible environment-base resolver for ordinary mappings. Public
ownership boundaries convert those results to the sole lower-layer carrier,
`ResolvedConfig`, through `snapshot_config()`. There is no import-time config
object or cached exception. Each invalid fresh sample therefore raises a new
`InvalidConfigError`, while import remains safe and `cli.main()` remains the
sole process-level translator to the one-line exit-1 diagnostic.

`ResolvedConfig` guarantees every canonical key with the existing
normalization and validation. It also preserves additional keys as opaque
extension data. Its top-level bindings are copied and read-only; nested opaque
values remain extension-owned. Exact marker receipts preserve identity.
`resolve_isolated_config()` uses the same canonical schema without ambient
input and rejects extras by default for fail-closed embedders; its explicit
`preserve_unknown=True` mode opts into opaque pass-through. `_overlay_config()`
is the ambient-free operation-local overlay for a handle that already owns a
marker.

Queue, discovery, command, CLI, watcher, load, broker-context, and direct
runner seams sample once at their published ownership event. They pass the
same marker through target selection, `DBConnection`, process-session keys and
factories, `BrokerCore`, first-party backend plugins, runners, and cleanup.
Lazy resource acquisition is not a second configuration time. A watcher given
an existing Queue adopts the Queue marker unless explicit watcher config
overlays or replaces watcher-local policy; the Queue still operates under its
own marker. Transactional generator overrides are frozen when the
configuration-consuming generator body first runs. `_paths.py` resolves its
fixed built-in backend at validation time; opaque extras cannot select a core
backend. Process sessions include the complete marker in identity, so opaque
extras can separate resource sessions without becoming canonical core options.
| two diagnostic race tests (19, 11) | deleted | Stronger production-path transition and concurrency tests made the diagnostic-only assertions redundant. |

Redis broadcast also improved from 36 to 28 through named selector, patterned
broadcast, reservation, and result-code seams. Its atomic Lua retry loop
remains registered because moving target selection or retry state out of that
owner would weaken the protocol. The release batch and single workflows remain
separate registered owners for the same reason: their commit and baseline
semantics are not interchangeable.

## State-Machine Inventory

All 30 reviewed seed entries are confirmed machines. `SM-REDIS-WRITE` was
added by the reserved-zero and Redis write-atomicity plan after the seed
inventory, bringing that inventory to 31. `SM-ACTIVITY-WAITER` was added by
the terminal activity-waiter lifecycle plan, bringing the current inventory
to 32. None is merged or
reclassified as a non-machine. The detailed inventory preserves the baseline
classification evidence and original table candidates for audit history; its
candidate column is not the current ownership source. The executable ownership
table and manifest below are authoritative for the completed implementation.

### Executable transition-table ownership

| Machine IDs | Executable table module |
|-------------|-------------------------|
| `SM-SQLITE-SCHEMA`, `SM-DUMP-LOAD`, `SM-TIMESTAMP-GENERATOR`, `SM-SQLITE-RUNNER` | `tests/test_core_persistence_transition_tables.py` |
| `SM-DARWIN-XATTR`, `SM-PHASE-LOCK` | `tests/test_phaselock_transition_tables.py` |
| `SM-CONNECTION`, `SM-PROCESS-SESSION`, `SM-DELIVERY-POISON` | `tests/test_connection_transition_tables.py` |
| `SM-SETUP-BUDGET` | `tests/test_retry_policy_coverage.py` |
| `SM-POLLING`, `SM-WATCHER-LIFECYCLE`, `SM-CLI-WATCH` | `tests/test_watcher_transition_tables.py` |
| `SM-ACTIVITY-WAITER` | `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py` |
| `SM-PG-LISTENER`, `SM-PG-VACUUM` | `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py` |
| `SM-REDIS-BROADCAST`, `SM-REDIS-WRITE`, `SM-REDIS-ACTIVITY-LISTENER`, `SM-REDIS-RUNNER` | `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py` |
| `SM-COVERAGE-SETTLEMENT`, `SM-CLI-COVERAGE` | `tests/test_dev_scripts.py` |
| `SM-RELEASE` | `tests/test_release_script.py` |
| `SM-ASYNC-STREAM` | `tests/test_example_async_stream_transitions.py` |
| `SM-REACTOR`, `SM-REACTOR-OUTPUT` | `examples/tests/test_reference_reactor_transitions.py` |
| `SM-PRIORITY-WATCHER`, `SM-MONITORING-WATCHER` | `examples/tests/test_multi_queue_pattern_transitions.py` |
| `SM-SUBPROCESS` | `tests/test_managed_subprocess_transitions.py` |
| `SM-CROSS-THREAD-PROBE` | `tests/test_cross_thread_probe_transitions.py` |
| `SM-MULTIPROCESS-WATCHER` | `tests/test_watcher_multiprocess_transitions.py` |
| `SM-SIGINT-PROBE` | `tests/test_watcher_sigint_probe_transitions.py` |

`tests/state_machine_manifest.py` marks coverage `COMPLETE`.
`tests/test_state_machine_policy.py` requires exact equality among these 32
inventory IDs, the 32 manifest entries, and the implementation-map inventory.
The example and reusable-protocol slice contains 74 firing rows; this includes
15 `SM-REACTOR` rows and 7 `SM-REACTOR-OUTPUT` rows. Failure rows inject below
the real persistent-state owner so the owner's rollback and replay behavior
remain observable. Core timestamp/runner and watcher lifecycle rows are outside
that seven-module slice, so adding rows to those tables does not change 74.

| ID and status | Persistent-state owner | Baseline table candidate | Existing integration proof | Classification evidence |
|---------------|------------------------|---------------------------------|----------------------------|-------------------------|
| `SM-SQLITE-SCHEMA` (confirmed) | `simplebroker/_backends/sqlite/schema.py` | `tests/test_sqlite_schema.py` | `tests/test_sqlite_schema.py`; runner setup/error suites | Durable schema version and objects decide which migration, repair, commit, or rollback event is legal on later setup calls. |
| `SM-DUMP-LOAD` (confirmed) | `simplebroker/_dump.py::load_lines` plus destination broker state | `tests/test_dump_load.py` | `tests/test_dump_load.py`; `tests/test_property_dump_load.py`; cross-backend dump/load suites | Header, batch, and already-applied durable records change the legal next input and retry outcome across iterator callbacks and broker writes. |
| `SM-TIMESTAMP-GENERATOR` (confirmed) | `simplebroker/_timestamp.py::TimestampGenerator` plus backend high-water state; realizes `[SB-ID-1]` through `[SB-ID-3]` | timestamp edge/resilience table beside `tests/test_timestamp_resilience.py` | `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`, including `SHARED_INSTANCE_SERIALIZATION`; timestamp edge, property, resilience, and released-backend latest-pending suites | Local counter, backend high-water mark, CAS result, physical clock, fork identity, and shared-instance lock ownership govern the next allocation action across calls and processes. One generator lock covers candidate selection, durable compare-and-advance or conflict refresh, and cache publication; CAS alone cannot prevent a stale local publication. |
| `SM-DARWIN-XATTR` (confirmed) | `simplebroker/_phaselock.py` Darwin-provider cache | `tests/test_phaselock.py` | `tests/test_phaselock.py` | Process-cached discovery success or failure controls later xattr reads; ERANGE changes the probe/read transition. |
| `SM-PHASE-LOCK` (confirmed) | `simplebroker/_phaselock.py::PhaseLockService` | `tests/test_phaselock.py` | `tests/test_phaselock.py` | Advisory ownership and durable markers determine whether later processes wait, run, skip, cancel, or fail. |
| `SM-CONNECTION` (confirmed) | `simplebroker/db.py::DBConnection` | `tests/test_db_connection_lifecycle.py` | connection lifecycle, fork, and process-session suites | Registry, thread-local handle, runner/core ownership, and closed state govern reuse and cleanup across threads and calls. |
| `SM-PROCESS-SESSION` (confirmed) | `simplebroker/_broker_session.py::_ProcessBrokerSession` | `tests/test_process_broker_session.py` | `tests/test_process_broker_session.py` | Session lifecycle and runner leases constrain connection creation, reuse, release, and post-close calls across threads. |
| `SM-SETUP-BUDGET` (confirmed) | `simplebroker/_retry_policy.py::SetupProgressBudget` | `tests/test_retry_policy_coverage.py` | retry-policy coverage and runner setup/error suites | Last-progress time and idle budget persist across setup operations and choose wait, refresh, timeout, or cancellation. |
| `SM-DELIVERY-POISON` (confirmed) | `simplebroker/db.py` sidecar and transactional-generator ownership | `tests/test_cross_thread_finalization_poisoning.py` | cross-thread poisoning, generator, and released-backend probe suites | Owner identity, suspended transaction, poison, and first cause govern legal `next`, `throw`, `close`, commit, and rollback effects across threads and yields. |
| `SM-POLLING` (confirmed) | `simplebroker/watcher.py::PollingStrategy` | `tests/test_watcher.py` | watcher, burst-mode, edge-case, stop, and race suites | Waiter identity, burst/backoff phase, activity hints, and stop state persist across waits and callbacks. As required by `[SB-API-6]`, the direct `burst_sleep` default comes from the ambient-free canonical config schema; `BaseWatcher` passes its retained resolved value explicitly. |
| `SM-ACTIVITY-WAITER` (confirmed) | First-party concrete waiter `_closed` state; manifest representative `simplebroker_redis.plugin.RedisMultiQueueActivityWaiter` | Redis real-waiter lifecycle transition table | PostgreSQL and Redis real-waiter lifecycle suites; PostgreSQL notify and Redis integration replacement tests | Resource-local open/closed state governs whether cleanup may run. The first close transitions to terminal before cleanup; ordinary failures preserve all independently safe cleanup attempts and ordered failure evidence, while interruptions stop the current attempt. Every later close is a no-op. |
| `SM-WATCHER-LIFECYCLE` (confirmed) | `simplebroker/watcher.py::BaseWatcher` | `WATCHER_LIFECYCLE_TRANSITIONS` in `tests/test_watcher_transition_tables.py` | watcher lifecycle, cleanup, edge-case, concurrency, stop, and race suites, including `STOP_RACES_START` | Thread state, waiter attachment, retry state, terminal exception propagation, stop state, and the lock-protected cleanup owner govern legal start, run, stop, join, and cleanup calls. Run or stop-before-run claims cleanup before blocking work; join timeout does not transfer ownership, and cleanup failure reopens a retry/finalizer path. The state is encoded in the established `_run_thread` ownership slot so pinned downstream subclass snapshots do not gain a new shared instance field. |
| `SM-CLI-WATCH` (confirmed) | `simplebroker/commands.py::cmd_watch` callback and output lifecycle | `tests/test_cli_watch.py` | `tests/test_cli_watch.py`; watcher subprocess and command-helper suites | Callback results, one-time warning, output health, interrupt state, and watcher cleanup persist across callbacks and shutdown. |
| `SM-PG-LISTENER` (confirmed) | `extensions/simplebroker_pg/simplebroker_pg/runner.py::_SharedActivityListener` | PostgreSQL notify/lifecycle table | PostgreSQL notify and runner-lifecycle suites | Readiness, registrations, listener-thread failure, notification routing, and closed state persist across callbacks and threads. |
| `SM-PG-VACUUM` (confirmed) | `extensions/simplebroker_pg/simplebroker_pg/plugin.py::vacuum` plus maintenance lease | PostgreSQL maintenance table | PostgreSQL maintenance and plugin contract-edge suites | Durable lease, advisory lock, transaction result, and maintenance result govern later delete, compact, unlock, and release actions. |
| `SM-REDIS-BROADCAST` (confirmed) | Redis `core.py::broadcast` plus Lua script and Redis allocation state | Redis broadcast table beside atomicity tests | Redis atomicity, broadcast, and integration suites | Atomic target selection, persisted high-water state, capacity, Lua status, and retry count govern the next Python and server action. |
| `SM-REDIS-WRITE` (confirmed) | Redis `core.py::_write_message` plus the process-local target write-lock registry, `scripts.py::WRITE_MESSAGE`, persisted high-water, and row indexes; realizes `[SB-ID-2]` | `REDIS_WRITE_TRANSITIONS` | `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_write_fires_transition_table`; real-Valkey same-target serialization, stale-fence, visibility, monotone-resync, and command-count tests in `test_redis_atomicity.py` | Process and target identity, the shared write lock, locally reserved candidate, server high-water, Lua result, and shared conflict count govern serialization, fork reset, refresh, monotone resync, retry, commit, or terminal failure. High-water and row publication share the successful Lua visibility point; Pub/Sub and maintenance remain post-commit. |
| `SM-REDIS-ACTIVITY-LISTENER` (confirmed) | Redis `plugin.py::_SharedRedisActivityListener` | Redis listener lifecycle table | Redis plugin contract-edge and pool suites | Readiness, registrations/refcounts, read failure, notification routing, stop, and closed state persist across callbacks and threads. |
| `SM-SQLITE-RUNNER` (confirmed) | `simplebroker/_runner.py::SQLiteRunner` | `tests/test_core_persistence_transition_tables.py` | runner ownership/error, poison, process-session, fork-safety, schema, setup, and `CLOSE_REOPEN` suites | Per-thread connection, connection generation, transaction owner, admitted-call count, unusable state, process identity, setup marker, and tracked resources govern admission, transaction settlement, reusable close/reopen, fork reset, and cleanup. Runner close releases its tracked snapshot; the process session/factory owns terminal admission. The transaction-ownership change is traced by `docs/plans/2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`. |
| `SM-REDIS-RUNNER` (confirmed) | Redis `runner.py::RedisRunner` | Redis runner lifecycle table | Redis pool, fork, and plugin contract suites | Lazy client/pool, ownership, process identity, and closed state govern reuse, reset, and cleanup across calls and forks. |
| `SM-COVERAGE-SETTLEMENT` (confirmed) | `bin/coverage_combine.py::_wait_for_stable_sources` | `tests/test_dev_scripts.py` | `tests/test_dev_scripts.py`; arbitrary-cwd thin-entry subprocess proof | Per-source snapshots, stable-since time, deadline, validation, repair, and exclusion state govern later polling actions. |
| `SM-CLI-COVERAGE` (confirmed) | `tests/conftest.py` CLI coverage staging/publication helpers | `tests/test_dev_scripts.py` | development-script and CLI subprocess suites | Private staging, child completion, validation, promotion, and cleanup cross the subprocess and filesystem boundary. |
| `SM-RELEASE` (confirmed) | `bin/release.py` release workflow | `tests/test_release_script.py` | release script, publication, workflow, and workflow-gate suites | Working tree, generated files, commit identity, CI result, tag state, and rerun state persist across commands and invocations. |
| `SM-ASYNC-STREAM` (confirmed) | `examples/async_pooled_broker.py::stream_read` and runner transaction state | new async example runtime table | No direct runtime suite at baseline; T7 must add one. | Generator delivery mode, transaction state, yielded batch, early close, and failure govern commit or rollback across async yields. |
| `SM-REACTOR` (confirmed) | `examples/reference_reactor.py::BaseReactor` and `Reactor` | `examples/tests/test_reference_reactor.py` | `examples/tests/test_reference_reactor.py` | Queue scheduling, lease, route, stop, and work state persist across driven turns and restarts. |
| `SM-REACTOR-OUTPUT` (confirmed) | `examples/reference_reactor.py` pending-output storage and publisher | `examples/tests/test_reference_reactor_transitions.py` | pending-output transaction-boundary, replay, retry, route-drift, backlog, and crash-restart tests | A candidate output ID is allocated outside the sidecar transaction, then durable pending output governs race adoption, publication, retry, replay, success, and terminal route-error actions across restarts. The boundary correction is traced by `docs/plans/2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`. |
| `SM-PRIORITY-WATCHER` (confirmed) | `examples/multi_queue_patterns.py::pattern_2_priority_simulation` nested watcher | new multi-queue example table | No direct test at baseline; T7 must add one. | Queue priority, pending work, callback result, and watcher lifecycle persist across notifications and dispatch turns. |
| `SM-MONITORING-WATCHER` (confirmed) | `examples/multi_queue_patterns.py::pattern_5_monitoring` nested watcher | new multi-queue example table | No direct test at baseline; T7 must add one. | Monitoring phase, queue event, callback result, and watcher lifecycle persist across callbacks and dispatch turns. |
| `SM-SUBPROCESS` (confirmed) | `tests/helper_scripts/managed_subprocess.py::ManagedProcess` and context manager | new direct managed-subprocess table | Existing helper consumers exercise parts; no complete direct table at baseline. | Child status, interrupt, termination escalation, reader ownership, stdin state, and cleanup persist across parent calls and process events. |
| `SM-CROSS-THREAD-PROBE` (confirmed) | `tests/helper_scripts/cross_thread_generator_probe.py` actor protocol | `tests/test_cross_thread_generator_probe.py` | direct probe tests plus PostgreSQL and Redis backend probes | Owner and foreign actor phases, publication, timeout, poison, and completion cross threads and processes. |
| `SM-MULTIPROCESS-WATCHER` (confirmed) | `tests/test_watcher_multiprocess.py` worker protocols | `tests/test_watcher_multiprocess.py` | the five multiprocess scenarios in the same file | Worker readiness, queue activity, stop, lock, result, and failure states cross process boundaries. |
| `SM-SIGINT-PROBE` (confirmed) | `tests/helper_scripts/watcher_sigint_script_improved.py` parent-child protocol | watcher subprocess table beside `tests/test_watcher.py` | Existing SIGINT subprocess test is partial; T7 and T8 must make fallback and readiness exact. | Readiness, signal receipt, watcher shutdown, fallback escalation, and terminal exit cross the process and signal boundary. |

## Explicit Non-Machines

The Redis body scan, configuration and path validators, timestamp parsers,
generic retry helper, CLI dispatcher, and repository-settings checklist are
not state machines under [DOM-10.2]. They are scans, ordered validation or
precedence chains, fixed-mode dispatch, or ordinary retry/checklist control
flow. Their C901 dispositions and normal behavioral tests still apply.

## C901 Activation Record

C901 is active at complexity 10. The initial activation registered all 76
findings; T9 through T13 reduced that inventory to 53 reviewed retained
findings in [DOM-10.1.1]. Each source directive uses the approved local pointer,
and every former P1/P2 finding is either removed at a named ownership seam or
reclassified with executable evidence and a durable reason.

`tests/test_ruff_policy.py` proves:

1. normal repository Ruff is clean;
2. raw C901 with `--ignore-noqa` still reports all retained findings;
3. raw `noqa_row` locations, local C901 directives, and registry locations
   match exactly;
4. every C901 row's declared count matches its unique listed locations; and
5. complexity 10 passes while complexity 11 fails under the normal
   configuration.

This is a reviewed exception registry, not a baseline allowlist. New or moved
findings must update source, registry, and policy evidence atomically.

## Related Plan

- `docs/plans/2026-08-23-correctness-and-concurrency-review-remediation-plan.md`
- retired: 2026-08-11-activity-waiter-terminal-close-contract-plan — source
  `27f9ae4`; see the ledger in `docs/plans/README.md`
- retired: 2026-08-04-cmd-watch-locality-plan — source `5023710`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-29-complexity-and-state-machine-hardening-plan — source
  `5023710`; see the ledger in `docs/plans/README.md`
