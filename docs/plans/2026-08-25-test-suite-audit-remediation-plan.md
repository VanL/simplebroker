# Test Suite Audit Remediation Plan

Status: active
Class: 4 — the plan deletes and rewrites tests across the whole suite,
changes CI workflow timeout wiring, and adds small private clock, rng,
and pid alias seams to production modules (no file-operation
wrappers). No public product contract
changes; the risk triggers are CI-gate changes and the possibility of
deleting a test that was load-bearing, so the [DOM-5] hardening
checklist items (invariants, anti-mocking guidance, rollback) are
included.
Plan type: test-suite and CI remediation from a completed audit.

## Goal

Make every test in `tests/` valuable and correct-by-purpose: remove
tests that cannot fail, replace procedure-reifying tests with
behavioral ones, eliminate the three mechanisms actually causing CI
flakiness (concurrently-observable shared-stdlib patches — synchronous
single-threaded fault patches are explicitly permitted — unscaled
timing valves with
negative-window assertions, Windows subprocess volume), and collapse
redundant accretion onto each contract's strongest owner. The suite
must end strictly stronger: every deletion is justified as
cannot-fail, duplicate-of-stronger-owner, or wording/internals freeze.

## Source Documents

Source specs:

- docs/specs/01-development-documentation-operating-model.md [DOM-5],
  [DOM-10], [DOM-15]
- docs/specs/10-cli.md [SB-CLI-1] (exit/timeout contracts exercised by
  CLI tests)
- docs/specs/16-python-library-api.md [SB-API-6] (watcher/polling
  contracts whose tests are pruned)
- docs/program-theory.md [THEORY-4] (truthful machine use — tests must
  fail on real regressions, not on rewording)

Audit provenance: seven-slice parallel audit, 2026-08-25, covering all
192 files under `tests/` (~75.6k lines), grounded in the 2026-08-24/25
CI failures (`test_process_broker_session` assert-False races,
`test_peek_generator_lifecycle` xdist worker crashes, Windows
`run_cli` 24s timeouts in `test_cli_move.py` /
`test_cli_write_output.py`). Full per-test findings live in the audit
session record; this plan carries every actionable disposition.

## Context and Key Files

- `tests/` — 192 files; per-slice dispositions below.
- `tests/conftest.py` — `run_cli` timeout budget (line ~951).
- `tests/helper_scripts/timing.py` — `drive_until` /
  `wait_for_condition` clock injection.
- `.github/workflows/test.yml`, `test-pg-extension.yml`,
  `test-redis-extension.yml` — pytest timeout bounds.
- `simplebroker/_retry.py`, `_retry_policy.py`, `_timestamp.py`,
  `_dump.py` — gain module-owned clock indirection (`_monotonic` /
  `_time_ns` aliases or a `clock=` parameter beside the existing
  `sleep=` seam).
- `simplebroker/_phaselock.py` — its EXISTING `_XattrProvider` seam
  (~:100) absorbs the xattr/platform patches; no new file-operation
  wrappers are added.
- `pyproject.toml` — Ruff `PLR2004` enablement for the
  no-magic-constants gate (Task 6.6), under the existing
  [DOM-10.1.1] suppression policy.

Concurrent-plan boundary: the active
`2026-08-24-comprehensive-review-findings-remediation-plan.md` and the
completed peek-generator-close work own product behavior on the
ephemeral early-close path. Task 2 of this plan only armors and
isolates the crash evidence; any product fix belongs to that plan's
owner.

## Invariants and Constraints

- No public API or CLI behavior changes. The production seams added
  are private, behavior-preserving indirection (alias defaults to the
  stdlib function; `clock=` defaults to the real clock).
- A test may be deleted only under one of three findings, recorded in
  the commit message slice notes: (a) cannot fail (tautology/vacuous),
  (b) duplicate of a named stronger owner, (c) freezes
  wording/internals with the behavior already owned by a named firing
  test. The owner claim is an equivalence claim, not a name match:
  the surviving owner must exercise the same contract at equivalent
  process topology, lifecycle stage, and public boundary; where the
  deleted test carried extra evidence (e.g. phased delivery or
  no-processing-after-stop in `test_multiprocess_single_queue`),
  that evidence is ported into the owner in the same slice. Deleting
  coverage without such an owner is out of contract.
- Anti-mocking rule going forward (enforced by review, recorded in
  lessons on completion), scoped to the demonstrated failure mode:
  what is prohibited is patching a shared stdlib module object where
  background threads, destructors, or concurrently running tests can
  observe it (clocks, randomness, `threading.Event`, PID lookup) —
  those move to module-owned aliases. Harmless synchronous
  fault-injection patches (e.g. a `Path.mkdir` PermissionError in a
  single-threaded test) remain permitted unless one is independently
  shown to leak; fault injection otherwise happens at sanctioned,
  documented seams (backend plugin hooks, the existing
  `_XattrProvider` seam, runner subclasses, injected
  `sleep=`/`clock=`, subprocess boundaries). Test cleanup must not
  become production architecture.
- Timing rule: Event/deadline synchronization only; every liveness
  valve uses `scale_timeout_for_ci`; no new negative-window
  (`assert not X.wait(t)`) assertions — prove ordering positively.
- Structural gates that guard enumerable contracts (preparse grammar
  conservation, closed error-code vocabulary, exit-code doc sync,
  `__all__` surfaces, SHA-pinned actions, spec verification-row
  bindings) are kept; only prose-fragment and double-entry mirrors go.

## Tasks

### Task 1 — CI flake mechanics: session/timing (slice 1)

1. `tests/test_process_broker_session.py`: adopt
   `scale_timeout_for_ci` for external liveness waits ONLY — thread
   joins and Event deadlock valves; never scale injected product
   durations or elapsed-time assertion bounds (e.g. the injected
   `0.05` product timeout and its `0.5`s completion bound at ~:829
   stay exact — scaling them would weaken the tested contract);
   pre-warm queues before patching so first-use SQLite setup is
   outside timed windows; replace the two negative timing windows
   (`assert not close_returned.wait(0.25)` at ~:814, `…wait(0.1)` at
   ~:1094) with positive happens-after observations (ordering list /
   `close_waiting` Event wrap, pattern at
   `tests/test_connection_transition_tables.py:323`). Replace the
   global `os.getpid` patch (~:870) with explicit pid arguments or a
   module-level `_getpid` indirection.
2. `tests/test_peek_generator_lifecycle.py` — owner direction
   (2026-08-25): the closeable-peek tests themselves increased CI
   load and initially introduced an unsafe destructor-reachable
   monkeypatch; the product change mainly reified and typed an
   iterator lifecycle that already existed. Correct test ownership
   and timing FIRST; change runtime only if a deterministic causal
   test demonstrates a product defect. Concretely, in this slice:
   verify no destructor-reachable patch remains on any path in the
   file; move the ephemeral early-close variants behind a subprocess
   probe now (`tests/test_sqlite_lifecycle.py:17` pattern), not
   conditionally, so a hard death fails one test instead of killing
   xdist workers; enable faulthandler capture for CI
   (`PYTHONFAULTHANDLER=1`) so any recurrence yields fatal-signal
   Python-frame tracebacks instead of a bare "worker crashed". Do
   not weaken assertions. Escalation to the
   comprehensive-review plan owner requires a deterministic
   reproducing test, not a crash signature.
3. `tests/test_managed_subprocess_transitions.py`: wrap all
   `timeout=2.0` waits in `scale_timeout_for_ci`.
4. `tests/test_concurrency.py::test_execute_with_retry_survives_real_sqlite_lock`:
   Event-released lock hold instead of `sleep(0.5)`.

### Task 2 — CI flake mechanics: stdlib-global patches (slice 2)

1. Add module-owned clock indirection: `_monotonic` in
   `simplebroker/_retry.py` / `_retry_policy.py`; `_time_ns` in
   `simplebroker/_timestamp.py` / `_dump.py` (or `clock=` parameters
   mirroring `sleep=`). Migrate every stdlib-clock patch site — the
   ~15 sites in `test_retry.py`, `test_retry_policy_coverage.py`,
   `test_runner_error_handling.py`, `test_runner_validation.py`,
   `test_timestamp_resilience.py`, `test_timestamp_edge_cases.py`,
   `test_dump_load.py`, plus the clock/`random` sites in
   `test_core_persistence_transition_tables.py`,
   `test_insert_messages.py`, `test_timestamp_advance.py`, the
   `time.monotonic` accelerator in `test_watcher_edge_cases.py`
   (~:622), and the shared-`time` patch sites in
   `test_dev_scripts.py` and `test_release_publication_script.py`.
   Add `_uniform` jitter-rng aliases in `simplebroker/watcher.py`
   (Task 5.9 depends on it) and in `simplebroker/_retry.py` /
   `_retry_policy.py` (the backoff-jitter patch sites at
   `test_retry.py:~92` and `test_retry_policy_coverage.py:~761`
   migrate to them). The `_paths` fault-injection sites
   (`Path.mkdir`/`Path.resolve` PermissionError/OSError,
   `os.access`) are synchronous single-threaded fault injection and
   stay as-is under the narrowed rule — migrate to real chmod-based
   conditions only where trivially portable (as Task 5.6 already
   does for `os.access`); no `_paths` production wrappers. Zero
   assertion changes.
2. `tests/test_retry_policy_coverage.py:~328`: pass the recording
   Event as the explicit `stop_event` argument instead of replacing
   `threading.Event` globally.
3. `tests/test_phaselock.py`: migrate the xattr-related global
   patches (`os.getxattr`/`os.setxattr` nulling, `sys.platform`) to
   the EXISTING `_XattrProvider` seam (`_phaselock.py:~100`) — no
   new production file-operation wrappers (owner review: that
   converts test cleanup into production architecture). The
   synchronous `Path.open`/`Path.stat`/`Path.unlink` fault patches
   stay unless independently shown to leak into concurrent work;
   the unguarded `Path.open` patches (~:1868, :1898) get
   path-guarded like their siblings. Raise the two 1.0s waiter lock
   budgets (~:1471, ~:1579) to `scale_timeout_for_ci(5.0)`.
4. `tests/test_watcher_edge_cases.py::test_signal_handler_not_main_thread`
   (~:584): drop the process-global `Mock` on
   `threading.current_thread`; run in a real non-main thread and
   assert `signal.getsignal(SIGINT)` unchanged.
5. `tests/helper_scripts/timing.py`: add `monotonic=` injection to
   `drive_until`/`wait_for_condition`; migrate
   `tests/test_timing_helpers.py` off the global `time` patch.

### Task 3 — CI flake mechanics: Windows subprocess volume (slice 3)

Owner direction (2026-08-25): setup subprocesses may be replaced by
Queue-API calls freely; a subprocess that exercises or verifies the
behavior under test may be replaced only when this plan names
surviving coverage at the same parser, process, exit-code,
stdout/stderr, and configuration boundary. The target shape is
layered: exhaustive cheap in-process tests, plus a small
representative end-to-end subprocess set per materially distinct
dispatch path and output mode. Reducing both layers at once is out of
contract.

1. `tests/conftest.py:~951`: do NOT raise the Windows base timeout up
   front. Land the process-count reductions below first, then measure
   (Windows CI runs after the slice lands); raise the base only if
   timeout failures persist, and then by the smallest observed amount
   — a preemptive 12→30s raise would give an effective ~65s CI
   ceiling and substantially weaken hang and startup-regression
   detection.
2. `tests/test_cli_move.py`: seed queues via the Queue API instead of
   `run_cli("write", …)` loops; delete the `time.sleep(0.001)` pacing
   (~:103, :113, :315, :387, :672, :816, :981 — timestamps are
   monotonic by contract); drop the redundant
   `_resolve_timestamp_filters` patch (~:288); pin the weak
   `rc in [1,2] or "error"` disjunctions (~:961, :1162). The
   concurrent-move operations under test keep their current scale
   (100 messages, 5 workers) — contention evidence is the point of
   those tests, and no replacement proving an equivalent race window
   at lower scale exists; only their *setup* spawns shrink.
3. `tests/test_cli_write_output.py`: convert most round-trip
   verifications to `Queue.peek()`, retaining at least one full
   process-to-process round trip — write → emitted ID on stdout →
   separate CLI `read` — including one JSON-mode case, as the
   end-to-end representatives. Drop the third copy of the
   registered-token matrix (owner:
   `test_cli_contract_sb_cli.py::test_sb_cli_3_write_token_matrix`
   plus in-process `test_cli_rearrange_args.py`).
4. `tests/test_json_output.py`: move the exhaustive newline-warning
   selector matrices (~:186, :220) in-process (`commands.cmd_*` +
   capsys, pattern at ~:139), retaining representative subprocess
   cases for each materially different output mode — normal, quiet,
   and JSON — because direct `cmd_*` calls do not cover argument
   parsing, process exit behavior, stderr routing, configuration
   loading, or cross-process warning-filter isolation.
5. `tests/test_message_by_timestamp.py`: delete the three
   grammar-fan-out validation tests (~:28, :60, :147 — ≈80 spawns)
   — enumerated: `test_timestamp_wrong_length_returns_error` (:28),
   `test_timestamp_non_digits_returns_error` (:60),
   `test_other_valid_timestamp_formats_rejected` (:147) — only with
   both layers named: exhaustive validation surviving in-process
   (`test_message_id_validation.py`), and one subprocess
   representative per distinct CLI dispatch path (`read`, `peek`,
   `delete`, `move` each take `-m` through their own parser/dispatch
   route) — extend
   `test_malformed_message_id_reports_error_but_absent_id_is_silent`
   to cover each command if it does not already. A validator unit
   test alone cannot catch parser or argument-rearrangement errors.
6. `tests/test_queue_validation.py`: reduce by equivalence class per
   LAYER, not per spelling: leading `-` (option/operand parsing),
   leading `.`, empty operand, length boundaries, allowed
   delimiters, and invalid characters exercise different layers —
   the parser-facing classes remain subprocess tests; only
   pure-validator duplicates move in-process. Parametrize so
   failures name the case; replace `tempfile.NamedTemporaryFile`
   blocks with `tmp_path`.
7. `tests/test_broadcast_integration.py`: replace both
   `wait_for_condition` polls (~:56, :114) with direct assertions —
   broadcast is synchronous, so this simplifies without weakening
   (owner-confirmed).

### Task 4 — Delete the cannot-fail tests (slice 4)

Each deletion names its finding: (a) cannot fail, (b) duplicate, (c)
wording/internals freeze.

- `tests/test_after_flag.py::test_after_index_usage` (~:776) — (a):
  asserts dropped index `idx_messages_queue_ts` against SQL the
  product never issues; passes by substring accident.
- `tests/test_edge_cases.py::test_clock_regression_during_claim`
  (~:25) — (a): patches `time.time`; code reads `time.time_ns`.
  Owner: `CLOCK_REGRESSION` transition-table case.
- `tests/test_core_persistence_transition_tables.py:~782`
  (fork-skip-helper meta-test) — (a): tests the file's own skip
  helper.
- `tests/test_queue_connection_manager.py::test_ephemeral_connection_lifetime`
  (~:121) — (a): full-Mock `DBConnection` asserting `__enter__`
  called. Owners: `test_ephemeral_mode_creates_new_connections`,
  `test_db_connection_lifecycle.py:171`.
- `tests/test_project_scoping.py::test_filesystem_root_detection_windows`
  (~:115) — CHANGE, not delete (owner review corrected the earlier
  cannot-fail claim: on the Windows CI leg it does not skip, calls
  the real `_is_filesystem_root`, and can fail if drive-root
  recognition breaks — the only other root coverage is Unix `/`).
  Replace with a Windows-only test using
  `Path(Path.cwd().anchor)`; remove the redundant `os.name` patch
  and the invented `C:`/`D:`/`Z:` roots.
- `tests/test_watcher_burst_mode.py::test_backoff_evidence_does_not_depend_on_poll_throughput`
  (~:121) and
  `tests/test_watcher_multiprocess.py::test_lock_test_process_waits_for_parent_start`
  (~:374) — (a): helper self-tests.
- `tests/test_watcher_edge_cases.py::test_watcher_exit_has_context_manager_protocol_signature`
  (~:45) — (b): [SB-API-6] contracts `__exit__` *behavior*
  (stop-and-join), owned by the real `with`-statement tests
  (`tests/test_watcher.py:453`, `:862`, `:2129` and the
  context-manager cases in this same file), which fail immediately
  on any signature break; the `inspect.signature` pin adds only the
  restatement. `::test_unsupported_message_type_is_not_exported`
  (~:41) — (a). Justification for the second: `simplebroker.watcher`'s
  module `__all__` is outside [SB-API-1]'s supported import surfaces
  (CHANGELOG 7.4.0); the guarded public surfaces are package root
  and `ext`, owned by `test_public_surface.py` /
  `test_ext_imports.py`, which stay.
- `tests/test_constants.py` `TestConstants` trims (~90 lines):
  whole-test deletions enumerated — `test_program_constants` (:86),
  `test_time_unit_constants` (:135), `test_watcher_constants`
  (:149), `test_project_scoping_constants` (:164), all (b) —
  duplicates of named behavioral owners, not cannot-fail;
  within-test trims (a) — the isinstance asserts and same-line
  duplicate restatements inside surviving `test_database_constants`
  (:91), `test_exit_codes` (:102), and `test_timestamp_constants`
  (:118). Deletion rule for this bullet:
  each removed value assert names its behavioral owner in the slice
  note (e.g. `MAX_TOTAL_RETRY_TIME` → the watcher absolute-timeout
  budget test); a constant with no behavioral owner is either kept
  as an explicitly labeled compatibility pin or gains a behavioral
  test — it is not deleted bare. Keep: version consistency, exit
  codes, on-disk names/magic, bit-layout invariants, all
  `TestLoadConfig`/`TestParseBool`/`TestConfigValidation`.
- `tests/test_cli_watch.py:~149` dead OR-assert — (a);
  `tests/test_commands_init.py::test_init_quiet_mode_suppresses_output`
  gains `captured.err == ""` (currently asserts the wrong stream);
  `tests/test_queue_move_watcher.py::test_stop_event_handling`
  vacuous `move_count` bounds deleted, join/liveness kept.
- `tests/test_after_flag.py::test_after_multiple_readers` dead
  contradiction branch (~:737) — (a).
- `tests/test_timestamp_edge_cases.py` dead `TimeAdvancer` class
  (~:50) — dead code.

### Task 5 — Procedure-reification rewrites (slice 5)

1. `tests/test_watcher.py`: the waiter-replacement cluster is NOT
   deleted — owner review corrected the earlier claim: the relevant
   table is `SM-POLLING` (not `SM-WATCHER`), and its REPLACE case
   only proves a distinct waiter displaces without closing
   (`test_watcher_transition_tables.py:126`), while the cluster
   proves exact public [SB-API-6] behavior documented at
   `watcher.py:~1430` (identical-object/`None` exact no-op, `None`
   transitions, data-version and local-activity survival,
   native-generation/backoff reset, deadline-failure exception
   atomicity, no waiter/provider/callback/detach invocation during
   replacement) — `PollingStrategy` is public through
   `simplebroker.ext`. Permitted reduction only: combine into three
   compact tests (exact no-op including `None`; successful distinct
   replacement with state-survival and reset assertions;
   deadline-failure exception atomicity) preserving all listed
   evidence. Keep BOTH canonical-default tests
   (`test_defaults_use_ambient_free_canonical_config_snapshot` and
   `test_all_defaults_derive_from_one_isolated_canonical_snapshot`)
   — they are not duplicates (ambient-ignored/explicit-wins vs
   single-resolver-call `calls == [{}]` with the injected snapshot)
   and the [SB-API-6] verification row names both; a merged
   replacement is acceptable only if it preserves the single-call
   and injected-snapshot evidence and updates the spec row. Still
   delete the two legacy SIGINT subprocess tests — enumerated:
   `test_graceful_shutdown_sigint` (~:471),
   `test_sigint_handler_installation` (~:590) — owners:
   `test_watcher_sigint_probe_transitions.py` rows
   `first-initialization-becomes-ready` and
   `interrupt-cleans-and-exits`, plus in-process
   `test_signal_handler_restoration` (~:2155); convert `sleep(0.1)`
   init waits to `wait_for_condition`.
2. `tests/test_timestamp_edge_cases.py`: rewrite
   `test_timestamp_generator_update_conflict` (~:375) and
   `test_timestamp_magnitude_preservation` (~:447) onto the file's
   `AtomicLastTimestampPlugin` seam; drop the Mock-runner SQL scripts.
3. `tests/test_paths_coverage.py`: delete
   `test_resolve_symlinks_safely_rejects_depth_exhaustion` fake
   (owner: six real-symlink subprocess tests in
   `test_symlink_security.py`); replace the `_is_valid_sqlite_db`
   mock with a real broker DB; keep at most one commented fake for
   the Windows-only resolve branch.
4. `tests/test_queue_api_additions.py`: delete
   `test_queue_filtered_move_closes_bounded_generator` (~:16), but
   only together with its replacement: extend
   `test_queue_api_comprehensive.py:805`'s real-generator close
   family to cover `move` (it currently proves read/peek only, so
   the fake-based test is today's only direct move-close owner —
   independent review caught this). Delete the five duplicating
   tests, enumerated: `test_queue_delete_all` (:52),
   `test_queue_delete_by_id` (:73),
   `test_queue_delete_empty_queue_returns_false` (:158),
   `test_queue_move_all` (:164), `test_queue_move_after_timestamp`
   (:216) — owners: `test_queue_api_comprehensive.py`
   `TestQueueDelete` / `TestQueueHighLevelMethods` rows covering the
   same operations at the same Queue-API boundary.
5. `tests/test_cli_main.py`: KEEP the three dispatch/status/vacuum
   sensor patches (~:354, :388, :421), adding a comment naming the
   contract: [SB-CLI-2] requires the backend receive the exact
   validated canonical target string, and a symlinked and resolved
   path produce identical filesystem effects, so no real-effect
   assertion can distinguish them (independent review reversed the
   earlier convert-to-effects proposal). Keep
   `test_main_preprocesses_each_invocation_once` with a comment
   naming the double-normalization bug class it guards.
6. `tests/test_project_scoping.py`: un-mock `_find_project_database`
   in the precedence test (~:393); replace the `os.access` patch with
   chmod + skip guards; replace the leak-prone `temp_db_cleanup`
   fixture with `tmp_path`; delete the duplicated
   `TestEnvironmentVariableParsing` class (owner:
   `test_constants.py`).
7. `tests/test_validation_lock_safety.py:~111`: real corrupt-bytes
   file instead of MagicMock `sqlite3.connect`.
8. `tests/test_connection_config.py` (~:302, :343): replace the two
   live-thread `_get_delay` spy tests with a two-part proof (owner
   review corrected the earlier owner claim — the previously cited
   tests prove database selection and ambient isolation, not polling
   delays): (i) `QueueWatcher` construction maps instance/env
   configuration into the strategy fields; (ii) direct
   `PollingStrategy` tests prove those fields determine the delay
   schedule (no threads either way). Update the [SB-API-6]
   verification row, which currently names
   `test_watcher_instance_config_controls_live_polling`.
9. `tests/test_watcher_burst_mode.py::test_polling_jitter` (~:514):
   direct `_get_delay()` assertions instead of five threads and 60s
   of sampling, using the module-owned `_uniform` jitter seam from
   Task 2 (a scripted sequence of rng values — no stdlib
   `random.uniform` patching, honoring the anti-mocking rule): every
   value inside the jitter bounds AND value variation present, so a
   constant in-range delay fails deterministically; parametrize the
   single/batch error twins; fix or delete the env-mutating
   `no_jitter` fixture.

### Task 6 — Prose and double-entry gate refits (slice 6)

1. `tests/test_delivery_contract_sb_delivery.py`: within-test trims
   in two enumerated nodes —
   `test_live_peek_stream_rejects_naive_cursor_completeness`
   (~:231–251: the six prose fragments and the plan-path/SHA pins)
   and
   `test_closeable_peek_lifecycle_contract_is_bound_to_real_backends`
   (~:254–268: the nine prose fragments) — keeping each test's
   verification-row bindings and adding a file/function existence
   check in the second; behavioral tests untouched.
2. Family sweep, keeping identifier tokens and bindings, deleting
   sentence fragments — enumerated nodes:
   `test_operations_contract_sb_ops.py::test_ops_language_core_promises`
   (~30 phrases);
   `test_message_identity_contract_sb_id.py::test_message_identity_contract_clause_inventory_and_authority`
   (trims at ~:246–274, registry/path/state-machine bindings kept);
   `test_python_library_api_contract_sb_api.py::test_api_public_surfaces_language`
   (~:73), `::test_api_queue_lifecycle_and_library_shape_language`
   (~:188),
   `::test_api_generators_watchers_sidecar_io_errors_language`
   (~:200), `::test_api_watcher_start_stop_cleanup_ownership_contract`
   (~:260, prose asserts only — identifier tokens stay),
   `::test_api_owned_runner_lifecycle_and_backend_v7_contract`
   (~:281, same), `::test_api_closeable_peek_iterator_contract`
   (docstring-wording pins at ~:140–145, protocol-shape asserts
   stay); `test_agent_kernel_contract.py` two prose tests
   (`test_agent_kernel_forbids_delete_while_peek_stream`,
   `test_agent_kernel_does_not_claim_identical_cli_python_packaging`
   — identifier tokens stay, sentence fragments go);
   `test_broadcast_contract_sb_bcast.py::test_sqlite_broadcast_mapping_binds_the_real_lock_owner_and_noop_hook`
   (`del runner` AST-shape assert at ~:240–248 and the
   newline-embedded spec assert at ~:218–221 go; call-ordering and
   no-Call checks stay).
3. `tests/test_program_theory_contract.py` — enumerated nodes:
   `test_lineage_is_bounded_and_current_first` (~:780, drop the
   QUOTE_ALLOWLIST line numbers and word budgets, keep structural
   checks); `test_repository_and_product_entry_orders` (~:640, drop
   the hardcoded `EXPECTED_READ_ORDER` third copy and AGENTS.md
   prose-ordering asserts, keep the index==hub derived equality);
   `test_core_concepts_resolve_to_registry_owners` and
   `test_core_concepts_route_specialized_contracts` (~:723–753,
   derive the routing from the theory/registry text instead of the
   hardcoded dicts).
4. De-mirror config double-entry, without weakening (owner review
   tightened the replacements): `test_release_script.py` command
   assertions become "each option appears exactly once with the
   intended value, and the command-line override still follows
   ambient `PYTEST_ADDOPTS`" (`test_release_script.py:139`) — not
   bare membership, which would admit duplicate or conflicting later
   options; `test_development_tool_floors_are_current` keeps exact
   minimums for tools where the repository relies on
   version-specific behavior and relaxes only the rest to names +
   floor-exists + derived minversion consistency; `test_ruff_policy.py`
   `directives == 168` → retired-ID non-reuse property; frozen CI
   formatter command string → per-directory containment with
   exactly-once semantics.
5. `tests/test_backend_plugin_resolution.py:~507`: interpolate
   `BACKEND_API_VERSION`. `tests/test_invalid_config_lifecycle.py`
   AST guard: glob-derive the module list.
6. Make the "no magic constants" policy executable instead of
   value-restated — simplest faithful version per owner direction
   (2026-08-25), two mechanisms only:
   - Enable Ruff `PLR2004` (magic-value-comparison) for
     `simplebroker/` and triage its 29 current findings: each becomes
     a named `_constants` value or a justified `noqa` routed through
     the [DOM-10.1.1] suppression registry (which validates Ruff
     `noqa` diagnostics — the reason a custom scanner cannot ride
     it: literals outside comparisons produce no Ruff diagnostic).
     Add a live-fire probe to `tests/test_ruff_policy.py` proving
     the rule fires.
   - Require every intentional constant declaration in
     `_constants.py` to carry a short explanation of meaning or
     units (tokenize-based adjacent-comment check for bare
     constants; `_CONFIG_FIELDS` already gated by
     `test_invalid_config_lifecycle.py::test_every_recognized_config_field_has_an_expected_form`
     — do not duplicate it).
   - Explicitly dropped as overlapping systems: the general AST
     literal sweep and the fallback `doc=` declaration registry.
   - Enforcement boundary (owner-specified, recorded so the limit is
     explicit): `PLR2004` mechanically prevents new unexplained
     comparison literals; `_constants.py` declarations mechanically
     require meaning or units; non-comparison literals in call
     arguments, defaults, arithmetic, and assignments remain
     review-enforced — call arguments and signature defaults require
     either a named constant or a local explanation. Locally named
     module constants are compliant; `_constants.py` is reserved for
     shared, cross-module, configuration, persistence, or
     public-contract values. (Measured baseline 2026-08-25: 109
     non-trivial numeric literals outside `_constants.py`/`_sql/`,
     14 in comparisons.)

### Task 7 — Redundancy collapse (slice 7)

1. `tests/test_after_flag.py`: rebuild on the deterministic
   `insert_messages` pattern (its own
   `test_after_iso_date_precise_boundary` (:443) is the model;
   `test_before_flag.py` is the target shape). Full disposition:
   SURVIVE unchanged or lightly adapted —
   `test_after_iso_date_precise_boundary`,
   `test_after_timestamp_heuristic`, `test_after_invalid_timestamps`,
   `test_after_missing_value`, `test_after_during_concurrent_writes`,
   `test_after_checkpoint_pattern`,
   `test_after_hybrid_timestamp_ordering`,
   `test_after_uses_persisted_cross_process_timestamp_order`,
   `test_after_negative_timestamps`,
   `test_after_scientific_notation_rejected`,
   `test_read_after_plain_word_with_e_reports_invalid_timestamp`,
   `test_after_error_messages_are_helpful`,
   `test_after_with_commit_interval`,
   `test_read_all_commit_interval_keeps_uncommitted_batch_on_output_failure`,
   `test_after_with_peek`,
   `test_after_empty_queue_after_postgres_schema_reset`. DELETE into
   the new parametrized boundary owner: `test_after_basic_filtering`,
   `test_after_exact_boundary`, `test_after_empty_queue`,
   `test_after_no_matches`, `test_after_zero`,
   `test_after_max_timestamp`, `test_after_queue_not_found`. DELETE
   into the new parametrized flag-combination owner:
   `test_after_with_all`, `test_after_with_json`,
   `test_after_with_timestamps`, `test_after_with_json_and_timestamps`,
   `test_after_single_message_mode`. DELETE into the new exact-ID
   format owner: `test_after_valid_timestamps`,
   `test_after_human_readable_formats`, `test_after_iso_date_formats`,
   `test_after_iso_datetime_formats`,
   `test_after_unix_timestamp_formats`,
   `test_after_mixed_timestamp_formats`,
   `test_after_naive_datetime_utc_assumption`. DELETE as heuristic
   duplicates (owners: `test_after_timestamp_heuristic` +
   `test_after_invalid_timestamps` parametrization):
   `test_after_timestamp_heuristic_edge_cases`,
   `test_after_unit_suffixes`. DELETE as defects (Task 4):
   `test_after_index_usage`; `test_after_multiple_readers` folds its
   live assertion into `test_after_during_concurrent_writes`. Net
   ~39 → ~19 nodes, wall-clock spin-waits removed.
2. Exactly-once dedup: delete
   `test_watcher.py::test_multiple_workers_exactly_once` (~:882) and
   `test_watcher_concurrency.py::test_worker_pool_exactly_once_delivery`
   (~:95) — owners: `test_watcher_race_conditions.py:229`,
   `test_watcher_thundering_herd.py:249`; delete
   `test_watcher_multiprocess.py::test_multiprocess_single_queue`
   (~:425) — owner: `…contention_preserves_exact_delivery`, which in
   the same slice absorbs the deleted test's phased-delivery probe
   (one consumer proven before bulk writes) and its
   no-processing-after-stop assertion per the equivalence invariant.
3. File retirements: `test_parse_exact_message_id.py` (fold one
   None-contract test into `test_message_id_validation.py`),
   `test_move_integration.py` (fold the interleave assertion into
   `test_move.py`), `test_compound_db_names.py` (fold five unit tests
   into `test_paths_coverage.py`), `test_cli_queue_metadata.py`
   (fold the prefix+claimed JSON row into `test_cli_metadata.py`),
   `test_cli_edge_cases.py` (merge survivors into
   `test_cli_main.py`).
4. `tests/test_db_connection_lifecycle.py`: delete four enumerated
   tests — `test_get_connection_caches_and_cleans_up_managed_resource`
   (:44), `test_cleanup_handles_registered_and_thread_only_resources`
   (:69), `test_cleanup_does_not_close_an_owned_core_twice` (:82),
   `test_cleanup_logs_registered_core_and_runner_failures` (:93) —
   owners: the `LAZY_CREATE`, `CLEANUP`, `REGISTERED_CLOSE_FAILURE`,
   `OWNED_CLOSE_FAILURE`, `RUNNER_CLOSE_FAILURE` rows in
   `test_connection_transition_tables.py:45–118`.
5. `tests/test_cross_thread_generator_probe.py`: keep
   `test_sqlite_other_core_default_contention_is_bounded` (:131)
   under the env gate; delete three enumerated tests —
   `test_sqlite_cross_thread_generator_probe` (:26),
   `test_sqlite_cross_thread_move_generator_restart_restores_source`
   (:75), `test_sqlite_cross_thread_transactional_sidecar_probe`
   (:106) — owners: the matching actor-protocol cases in
   `test_cross_thread_probe_transitions.py` (which run
   unconditionally in CI), with the recovered-queue re-read
   assertions folded in where not already present.
6. `tests/test_phaselock.py`: delete four enumerated tests —
   `test_no_xattr_non_strict_waiter_skips_when_phase_marked_while_lock_is_held`
   (~:1634, sleep-as-sync),
   `test_darwin_xattr_provider_returns_none_on_non_darwin_platform`
   (~:143), `test_darwin_xattr_provider_caches_initialization_failure`
   (~:156), `test_darwin_xattr_provider_retries_get_when_value_grows`
   (~:365) — owners: `MARKED_WHILE_WAITING`, `NON_DARWIN`,
   `CACHE_FAILURE`, `ERANGE_REPROBE` cases in
   `test_phaselock_transition_tables.py`, absorbing the byte-level
   assertion detail where the table case lacks it.
7. `tests/test_move_claim_patterns.py` — SEQUENCING DEPENDENCY
   (owner review): this family's cleanup depends on the
   claimed-row-preservation fix owned by
   `2026-08-24-comprehensive-review-findings-remediation-plan.md`
   (its Move principle: "Moving a claimed row keeps it claimed",
   with the [SB-DELIVERY-1] spec delta) landing first. Current move SQL clears the claim
   in both paths (`_sql/sqlite.py:188`, `:556`) and
   `test_move_by_id.py:92` asserts the moved claimed row becomes
   pending — semantics the owner has rejected. Do not prune this
   family against current semantics: explicitly retain and repurpose
   the by-ID test to prove the moved row keeps its ID and remains
   claimed at the destination once the product fix lands, and only
   then delete the four enumerated restatements —
   `test_move_with_mixed_claimed_unclaimed` (:170),
   `test_move_preserves_message_ordering` (:242),
   `test_move_empty_to_empty_queue` (:272),
   `test_multiple_sequential_moves` (:292) — owners:
   `test_move.py::test_move_only_unclaimed`,
   `::test_move_preserves_order`, `::test_move_empty_queue`, and the
   retained concurrency test in this file.
   `tests/test_message_claim.py`: delete two enumerated migration
   tests — `test_schema_migration_adds_claimed_column` (:336),
   `test_schema_migration_idempotent` (:385) — owners: the
   `MIGRATE_V1` / `CURRENT_IDEMPOTENT` / `CURRENT_REPAIR` rows of
   `SM-SQLITE-SCHEMA` in `test_core_persistence_transition_tables.py`.
   `tests/test_broadcast.py`: delete three enumerated grammar tests —
   `test_broadcast_queue_prefix_is_rejected_before_mutation` (:251),
   `test_broadcast_queue_prefix_can_be_literal_after_double_dash`
   (:261), `test_broadcast_empty_long_option_prefix_remains_literal`
   (:273) — owners: the in-process prefix/escape cases in
   `test_cli_rearrange_args.py` plus the subprocess representative
   in `test_cli_global_options.py`.
   `tests/test_queue_api_comprehensive.py`: parametrize the
   generator-close family — five cases after Task 5.4 adds `move` —
   into one parametrized test; the move case must survive the
   collapse.
8. `tests/test_performance.py`: route the four hardcoded absolute
   timeouts through `get_timeout()`.
9. Suite-wide stdlib-patch gate (deliberately here, AFTER Tasks 4-5
   have deleted or rewritten the remaining offender sites, not at
   Task 2's end): a grep sweep over `tests/` for `setattr`/`patch`
   targets resolving to the concurrently-observable shared stdlib
   attributes the narrowed rule prohibits (`time.`, `random.`,
   `threading.Event`, `os.getpid`) returns only the documented
   indirection points and per-instance/subclass seams; permitted
   synchronous fault patches are out of the gate's scope by design;
   the sweep and its result are
   recorded in the slice note so the invariant's "prohibited" claim
   matches the tree at the moment it becomes enforceable.

### Task 8 — CI workflow bounds (slice 8)

1. Bound `test-pg-extension.yml` and `test-redis-extension.yml`
   (currently unbounded at every level) at two layers: per-test
   pytest timeouts through the `bin/pytest-pg` / `bin/pytest-redis`
   wrappers, AND job-level `timeout-minutes` — the wrappers perform
   Docker/setup work before pytest starts (`_scripts.py:~620`),
   which pytest-timeout can never bound, so the job-level ceiling is
   the one that catches setup hangs. The wrappers add their timeout
   defaults ONLY when the caller did not pass `--timeout`,
   `--timeout-method`, or worker-restart controls (one
   add-missing-defaults helper), so an appended default can never
   silently override an explicit invocation — with tests covering
   both the default and explicit-override paths.
2. Extend
   `test_release_workflow.py::test_every_matrix_pytest_path_bounds_hangs_and_worker_loss`
   to derive pytest steps from every workflow file, recognizing
   wrapper invocations (`./bin/pytest-pg`, `./bin/pytest-redis`) as
   pytest steps — the current textual matcher does not — and
   asserting the job-level bound where the per-test contract cannot
   apply.

## Testing Plan

- Each slice runs the touched files plus, for deletions, the named
  surviving owner tests, before commit.
- After Tasks 1–3 land, monitor `gh run list --workflow=test.yml` over
  the following days: the specific failure signatures
  (`assert False`/`assert not True` in process_broker_session, worker
  crashes in peek_generator_lifecycle, `run_cli` timeouts on Windows)
  are the post-deploy success signals — they should not recur.
- Full suite (`uv run pytest`) green locally per slice; final slice
  runs the pg/redis backends via `bin/pytest-pg` / `bin/pytest-redis`
  and the release-gate doc checks (`bin/check-dom15-fixtures`,
  `bin/check-plan-context`).
- Coverage: compare branch coverage (`--cov-branch`) on
  `simplebroker/` before the first deletion slice and after the
  last. Coverage non-regression is a floor, not an authorization —
  deletion authority comes only from the per-deletion equivalence
  owner recorded in the slice note (assertions, process topology,
  and error classification can vanish with unchanged coverage).

## Rollback and Observation

- All slices are ordinary commits; rollback is `git revert` per
  slice. Production seam additions are behavior-preserving defaults,
  but once tests are migrated onto a seam the pair forms a coupled
  revert group: reverting the seam requires reverting its migrated
  tests in the same operation. Each seam-introducing slice names its
  revert group in the commit message.
- The new clock/rng/pid aliases get a short ownership note in the
  relevant implementation doc (06/07/09): alias lifetime, default
  binding, thread-visibility, and the supported fault surface —
  private is not architecture-free.
- If a deletion later proves load-bearing (a regression escapes that
  a deleted test would have caught), restore the test in behavioral
  form and record the miss in `docs/lessons.md`.

## Independent Review Loop

- After Tasks 1–3 (flake mechanics) and again after Task 7, run an
  independent review pass per [DOM-5], preferably a different agent
  family; incorporate or answer findings before the next slice.
- The Task 6 gate refits touch the contract-test family; the reviewer
  must confirm every deleted prose assert has a named surviving
  behavioral owner.

## Review Log

- 2026-08-25 — independent plan review, OpenAI Codex (different agent
  family), 12 P1 / 5 P2. Incorporated: move-close owner gap (Task
  5.4 now requires extending the comprehensive close family to
  `move` before deleting the fake-based test — verified: :805 covers
  read/peek only); canonical-target sensor patches kept, effects
  cannot distinguish symlinked vs resolved targets (Task 5.5
  reversed); stdlib-patch inventory completed plus a grep slice-exit
  gate (Task 2); jitter test gains a variation assertion (Task 5.9);
  deletion contract upgraded from name-match to equivalence
  (topology/lifecycle/boundary) with evidence porting (Invariants,
  Tasks 7.2); branch coverage as floor-not-authorization (Testing
  Plan); extension workflows get job-level bounds and the derived
  gate learns wrapper invocations (Task 8); constants deletions must
  name a behavioral owner per constant or keep a labeled
  compatibility pin (Task 4); `_CONFIG_FIELDS` description gate
  recognized as already existing (Task 6.6 de-duplicated); real
  Windows timeout ceiling stated (~65s effective, Task 3.1);
  equivalence-class sampling wording (Task 3.6); coupled revert
  groups and seam ownership doc notes (Rollback). Owner direction
  recorded in Task 1.2: correct test ownership and timing first;
  runtime changes only on a deterministic causal test demonstrating
  a product defect. Answered, not adopted: construction-level
  strategy-config assertions are sufficient wiring proof because the
  constructed `PollingStrategy` holds the instance values and its
  own unit tests own internal use of them (the live-thread spies
  they replace are the slice's flake vector); watcher-module
  `__all__` absence pin deletion stands because that module is
  outside [SB-API-1]'s supported surfaces; Task 6.6 breadth is
  explicit owner direction, and PLR2004/AST-sweep scopes are
  complementary (comparisons vs other literal positions), not
  duplicated policy.

- 2026-08-25 — re-review round (same Codex session) on the revised
  plan: 8/12 P1s RESOLVED, 1 ANSWERED-sound; residuals and new
  defects fixed in this revision: constants deletions reclassified
  (a)→(b) with named owners; `__exit__` signature deletion
  reclassified (b) with the real `with`-statement owners
  (`tests/test_watcher.py:453/:862/:2129`) since [SB-API-6] contracts
  the behavior those exercise; stdlib-patch inventory extended to
  `test_dev_scripts.py` / `test_release_publication_script.py` and
  the grep gate to `Path`/`os.`/`sys.platform`; Task 6.6's static
  inventory replaced with the [DOM-10.1.1] suppression-registry
  discipline so the gate fails only on new unmarked literals (policy
  retained per owner direction); Task 7.7 collapse now names five
  cases with move surviving; Out of Scope carve-out for
  deletion-contract-required behavioral owners; Task 5.9 rebuilt on
  a module-owned `_uniform` jitter seam (added to Task 2) so
  determinism does not violate the anti-mocking rule; faulthandler
  evidence claim corrected to fatal-signal Python-frame tracebacks.
- 2026-08-25 — round 3 (same session): 7/8 items RESOLVED; final two
  blockers fixed: `_retry`/`_retry_policy` gain their own `_uniform`
  jitter seams and the `_paths` fault sites migrate to chmod-based
  real conditions or module-owned wrappers (Task 2); the suite-wide
  stdlib-patch grep gate moved from Task 2's slice exit to Task 7.9,
  after the deletion/rewrite slices have removed the remaining
  offender sites, resolving the sequencing contradiction.
- 2026-08-25 — owner review of Task 3: the slice traded away real
  integration and stress coverage, not only redundancy. Rulings
  incorporated: the subprocess-replacement invariant (setup freely
  replaceable; behavior-exercising subprocesses only with named
  surviving coverage at the same parser/process/exit-code/stream/
  configuration boundary; layered shape — exhaustive in-process plus
  representative end-to-end); concurrent-move scale restored to
  100×5 (no equivalence proof exists for a smaller race window);
  write_output keeps a full write→ID→separate-CLI-read round trip
  incl. JSON; json_output keeps normal/quiet/JSON subprocess
  representatives; message_by_timestamp deletion gated on per-command
  dispatch representatives; queue_validation classes chosen per
  layer with parser-facing classes staying subprocess; the Windows
  timeout raise reversed to measure-first, raise-least-last;
  broadcast polling removal confirmed safe as-is.
- 2026-08-25 — owner review of Task 6.6: the item had accreted three
  overlapping systems (PLR2004, custom whole-tree literal scanner,
  tokenize comment gate) plus a fourth fallback (`doc=` registry),
  and wrongly claimed the custom scanner could ride the [DOM-10.1.1]
  suppression registry — that registry validates Ruff `noqa`
  diagnostics, and literals outside comparisons produce no Ruff
  diagnostic. Simplified per owner direction to two mechanisms:
  PLR2004 with triage of its 29 current findings, and the
  explanation-of-meaning-or-units requirement on `_constants`
  declarations; the AST sweep and `doc=` registry are dropped.
  Earlier Review Log entries describing the sweep mechanism stand as
  history and are superseded by this ruling.
- 2026-08-25 — owner review, 11 findings (4 P1, 7 P2), all
  incorporated: PollingStrategy replacement cluster retained (false
  owner claim corrected — table is `SM-POLLING` and its REPLACE case
  proves only distinct-waiter displacement; the cluster's
  [SB-API-6]-documented behaviors survive, optionally compacted to
  three tests); both canonical-default tests kept (the spec
  verification row names both; single-resolver-call evidence is not
  duplicated by the ambient test); the Windows filesystem-root test
  reclassified CHANGE (it runs for real on the Windows leg) and
  rebuilt on `Path(Path.cwd().anchor)`; move-claim family pruning
  gated on the pending claimed-row-preservation product fix with the
  by-ID test retained and repurposed; every remaining group deletion
  enumerated with pytest node IDs and owners in the plan itself;
  anti-mocking rule narrowed to concurrently-observable shared-state
  patches (existing `_XattrProvider` seam reused, `_paths`/phaselock
  production wrappers dropped, synchronous fault patches permitted,
  Task 7.9 gate scope narrowed to match); timeout scaling bounded to
  external liveness waits only, never injected product durations or
  elapsed-time bounds; connection-config rewrite now a two-part
  proof (construction mapping + direct delay-schedule tests) with
  the spec row update named; de-mirroring tightened to
  exactly-once-with-value assertions and retained version-specific
  floors; wrapper timeout defaults add-if-missing only, with
  override tests; completion gate moved to landing-SHA CI green
  across all four backends with the one-week watch as
  post-completion observation. Task 6.6 gained the owner-specified
  enforcement-boundary statement (mechanical vs review-enforced
  tiers, local-naming compliance, `_constants.py` reservation
  criteria, measured 109/14 baseline).
- 2026-08-25 — round 5 (same Codex session) on the eleven-finding
  revision: all incorporations present; three leftovers fixed in
  this revision — stale phaselock-hook claims in Context/Rollback
  and the Goal's over-broad "eliminate all process-global patches"
  aligned with the narrowed invariant; Task 7.7's external
  dependency now cites the comprehensive-review plan's claimed-row
  Move principle by name; every remaining group deletion enumerated
  with node IDs (Tasks 3.5, 4 constants within-test trims, 5.1
  SIGINT pair, 6.2 agent-kernel names, 7.1 full 39-node disposition,
  7.6 phaselock, 7.7 message-claim and broadcast).
- 2026-08-25 — rounds 6–7 (same session): round 6 caught the stale
  Class-line "clock/file injection seams" claim and incomplete Task
  6.1–6.3 node enumeration; both fixed (Class now declares clock/rng/
  pid alias seams with no file-operation wrappers; Task 6 enumerates
  every affected node with the kept/deleted split). Round 7 verified
  both RESOLVED with no new contradictions — verdict READY FOR
  EXECUTION.

## Deviation Log

- (empty)

## Out of Scope

- Any product fix for the peek early-close worker crash (owned by the
  comprehensive-review plan / peek-generator-close follow-ups).
- Keyset pagination, benchmark policy changes, and new contract tests
  beyond the named replacements and the behavioral owners the
  deletion contract itself requires (a deleted assert whose value
  has no owner gains one or stays as a labeled pin — that work is in
  scope by the Task 4 rule).
- Extension test suites under `extensions/` (audited only for the CI
  timeout wiring in Task 8).

## Completion Gate

- All eight slices committed with slice notes naming each deletion's
  finding and surviving owner.
- Full suite green on SQLite locally; CI green on the landing SHA
  across SQLite, Windows, PostgreSQL, and Redis — this completes the
  plan. One week of nonrecurrence of the three audited failure
  signatures is recorded as a POST-completion observation, not a
  completion blocker; any failed run in that window is inspected via
  `gh run view --log-failed` (a green `gh run list` alone cannot
  establish signature absence), with recurrences root-caused and
  their disposition recorded.
- Durable lessons recorded: the anti-mocking seam rule, the
  negative-window prohibition, the prose-vs-binding gate distinction,
  and the no-magic-constants policy's move from value-restating
  asserts to derived use-site gates.
- Status Index row flipped to `completed`.
