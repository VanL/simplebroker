# Whole-test-surface audit: operations appendix

Status: Audit findings delivered; remediation not authorized by this appendix.
Parent: [Whole-test-surface audit plan](2026-10-06-whole-test-surface-audit-plan.md).
Baseline: `d4d2634a9587409b06ece9ce593eb4c780f5da1b`.
Owner: Main audit agent, with independent integration review required.
Boundary: Read-only operations lane. No product/test changes, commits, pushes,
or releases. F = fix; C = consolidate without losing unique evidence;
D = deletion only after equivalence and sensitivity proof.

## Source Documents

- `docs/program-theory.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/specs/10-cli.md`
- `docs/specs/11-delivery.md`
- `docs/specs/13-message-identity.md`
- `docs/specs/14-timestamp-selection.md`

The test-audit skill was read in full. Exact test/support owners and focused
verification boundaries are recorded below; citations do not replace reading.

## Scope and evidence

All 125 assigned files were read completely, including helpers, embedded
programs and parameter data: 42,203 lines and 1,641 Python AST declarations
whose names start with `test_`. These counts are declarations, not collected
parameter instances; Hypothesis rules and embedded probe test functions are
also read but are not added to that declaration count. The exact membership
and retention account appears below.

The test-audit skill was used in Audit mode, not a pruning sweep. The shared
context, program theory, testing-patterns and relevant product owners were
consulted. The conceptual boundary matters here: actual queue, process,
backend, transaction and iterator-ownership boundaries are valuable even when
they overlap lower-layer unit tests. Merely enforcing today's spelling of
those boundaries is a different thing.

Candidate assertions were traced to the named production functions and
canonical clauses below. This lane did not run the full suite or mutate code
to demonstrate sensitivity. A focused read-only timestamp calculation was
executed with `uv run --locked python`: native ID
`1837025672140161024` represents 2028-03-18; the test's old conversion
`(native >> 12) // 1_000_000` produces 1984-03-18. The current parser accepts
the resulting ns form as `448492595737341952`, far below the original ID.
AST inventory counts were recomputed and matched the parent's lane totals.
HEAD still equals the pinned baseline at report generation.

No confirmed application correctness defect emerged from this lane. That is
not a claim that the application is defect-free. The stronger findings are
tests that can hide a correctness/liveness regression, or tests that can fail
on harmless maintenance. No deletion is approved by this report.

## Findings

### O1. Obsolete timestamp encoding plus weak selection assertions (F, high)

`tests/test_cli_move.py:308`
`TestTimestampFormats::test_after_unix_timestamp_formats`, especially lines
320–340; and `:377` `test_after_mixed_timestamp_formats`, lines 389–415.

The tests decode IDs as microseconds shifted left 12. The live format masks
the low 12 bits of epoch nanoseconds. The explicit-suffix test therefore uses
a cutoff decades too early, selects every seeded row, and passes because it
only requires at least two rows. The ISO/Unix branches of the mixed-format
test similarly require only at least one row. Native exact-count selection
does not rescue those independent parser routes.

Owner: `simplebroker/_timestamp.py:125` encoding,
`:490` `_parse_with_unit_suffix`, `:532` native/Unix parsing.
Contract: [SB-ID-1], [SB-CLI-5], [SB-SELECT-1].

Fix with an independent fixed ID corpus spanning each represented grain, and
exact expected bodies/IDs for s/ms/ns/ISO forms. Coarser units legitimately
select different rows, so do not assert all forms equal when they are not.
Keep both CLI parsing and actual selection. Related keepers:
`test_timestamp_bound_grammar::test_public_validator_preserves_integral_timestamp_forms`
and `test_timestamp_selection_contract_sb_select::test_cli_equivalent_iso_and_seconds_bounds_select_the_same_rows`;
they cover conversion/CLI equivalence but do not justify deleting all move
coverage. Validate that a wrong unit multiplier or ignored bound fails each
new case, then run the timestamp-format class across shared backends.

### O2. Windows test-level retry hides a failed supported write (F, high)

`tests/test_concurrency.py:154` `test_parallel_writes`, lines 175–187.

On Windows any `OperationalError`, not only a classified lock error, causes
a sleep, a fresh Queue and a second write. If the first call fails and the
second works, this test reports success. It tests retry-by-caller even though
the supported operation owns its retry budget already.

Owner: `simplebroker/db.py:2150` `_do_write_with_ts_retry` and
`:1451` `_run_with_retry`. The latter owns stop checks, elapsed budget and
progress token. [SB-ID-2] owns returned committed identity.

Capture and report the first operation failure; do not add another production
retry layer in the test. Retain independent writers and exact row
conservation. A first-call-fails/second-call-succeeds fault must fail the test;
run the real contention case on Windows as well. Existing
`test_timestamp_resilience` retry-budget seam tests protect retry-engine
behavior but cannot replace real independent SQLite contention.

### O3. Concurrent CLI move can loop forever on operational failure (F, high)

`tests/test_cli_move.py:818`
`TestConcurrentOperations::test_concurrent_moves_no_duplication`.

The worker only handles rc 0/2. Rc 1 is discarded and loops again. The start
barrier and joins have no timeout; there is no stop/finally cleanup or error
channel. A persistent dispatch failure hangs the test until a global kill
instead of reporting the original error.

Owner: `simplebroker/commands.py` `cmd_move` and CLI dispatch;
[SB-CLI-1] distinguishes operational failure 1 from empty 2;
[SB-DELIVERY-3]/[SB-ID-5] own conserved move identity.

Use one aggregate monotonic deadline, capture unexpected rc and stderr,
bounded barrier admission and owned thread/child cleanup. Do not serialize
the concurrent workload. If a product deadlock cannot be cleaned up from
threads, run the same contention topology in an isolated process with kill
escalation. Inject rc 1 and a stalled child to prove bounded diagnostic
failure; retain exact no-loss/no-duplication checks.

### O4. Exact-ID losing consumers may fail rather than return empty (F, high)

`tests/test_message_by_timestamp.py:353`
`test_concurrent_read_by_timestamp` and `:378`
`test_concurrent_delete_by_timestamp`.

Both only require one rc-0 winner. All four losers can return rc 1 with lock
or other errors and the test still passes. The read test checks the winner's
body but no losing output.

Owner: exact-ID command routing in `simplebroker/commands.py` and
transactional retrieve in `simplebroker/db.py`.
[SB-CLI-1] requires no-match 2; [SB-ID-4] exact addressing;
[SB-DELIVERY-1] committed claim.

Assert every loser is rc 2 with no output/diagnostic, and retain one correct
winner. A deliberately erroneous losing response must fail. Related
`test_operations_on_nonexistent_queue` protects the serial no-match boundary,
not concurrent losing races, so retain concurrency coverage.

### O5. Child/process ownership is not bounded on the failure path (F, high)

Concrete instances:

- `tests/test_cli_broken_pipe.py:242,271,287,304,431,444`:
  watch/peek/read/dump/SIGTERM probes call blocking stdout `readline()`
  before entering the helper's close-and-wait cleanup. A live child that
  never emits its first line hangs before cleanup.
- `tests/test_exactly_once_delivery.py:343`
  `test_concurrent_readers_safety`, joins at 365–366; and
  `tests/test_edge_cases.py` `test_concurrent_schema_migration`, join at 158:
  unbounded Process joins without exit-code proof or finally escalation.
- `tests/test_timestamp_resilience.py` `test_concurrent_writes_simple`:
  unbounded joins on non-daemon writer threads.
- `tests/test_move_after_exclusion.py` bounded polling helper invokes
  `subprocess.run` without a child timeout, defeating its outer deadline.
- The two mypy subprocesses in `test_queue_typing_contract.py`, and POSIX
  umask probe in `test_portability.py`, lack subprocess timeout ownership.

These are harness defects, not evidence of a product deadlock. Preserve the
real OS-pipe, independent process and resource boundaries. Owners are
`commands._print_stdout`, streaming command exception handling,
`BrokerDB` bootstrap/retrieve, and corresponding [SB-CLI-1],
[SB-DELIVERY-1,5,7] clauses. Pattern 7 of testing-patterns requires aggregate
process deadlines and diagnostic liveness evidence.

Use managed subprocess lifetime from immediately after spawn, bounded
first-output readiness, one coordination budget, exit-code/error capture,
and finally terminate/kill/reap escalation. For non-cancellable thread
deadlock probes, isolate the same concurrent workload rather than leaving a
live thread in a pytest worker. Do not increase product durations or relax
delivery assertions. Validate with a child that remains alive but emits
nothing, and another that hangs after readiness. Existing bounded timestamp
writer management in `test_safety_fixes.py` is a useful harness pattern, not
an equivalent replacement for the pipe/process tests.

### O6. Scheduling delay can false-pass a lock proof (F, medium)

`tests/test_backend_probe.py:81`
`test_backend_probe_serializes_same_core_calls`, line 127:
`second_started` is set before entering the probe. The negative 0.1-second
wait proves only that the second runner did not execute during that window.
If it is descheduled after its start signal, removing the lock can pass.

`tests/test_project_config.py:663`
`test_project_backend_setup_uses_config_file_phase_lock`: two executor
submissions and a 0.05-second initialize sleep do not prove overlap.
Sequential scheduling sees initialization complete and can produce one
initialize call even with exclusive phase locking missing.

Owners: `db.py:1467` materialization under `self._lock`;
`db.py:133` `_initialize_project_backend_target` delegates the config
file phase lock to PhaseLockService. These protect different resources and
must remain distinct tests.

Prove entry into the actual wait/acquisition boundary positively; hold the
first operation until the second reaches that boundary, then release and
assert ordering. Do not replace one arbitrary sleep with a larger one.
Validation: bypass the corresponding lock while deliberately delaying the
second thread before admission; the new oracle must still detect the broken
ordering. Existing materialization ownership assertion in the same probe
module complements, but does not replace, same-core serialization.

### O7. Version early-exit test does not observe command effects (F, medium)

`tests/test_cli_global_options.py:77`
`test_version_flag_before_command` uses `"write" not in stdout` as proof
that the write did not execute. A normal write is silent, so dispatch followed
by version output can pass.

Owner: `simplebroker/cli.py:2036` returns before command dispatch.
[SB-CLI-2]/[SB-CLI-3] own root action presentation/position.

Also inspect the actual isolated target/queue: no dummy message and no
unexpected target creation. Keep the real subprocess boundary. Mutating
dispatch while still printing the version must fail. Parser-only tests are
not an equivalent keeper.

### O8. Exact evidence mirrors impose multi-file maintenance without new behavior (C/F, medium)

Specific gates:

- `test_broadcast_contract_sb_bcast.py`, `FIRING_TESTS` equality.
- `test_operations_contract_sb_ops.py`, `EVIDENCE_MANIFESTS` equality.
- `test_timestamp_selection_contract_sb_select.py:170`
  `test_select_affected_evidence_rows_match_exact_executable_manifests`.
- `test_cli_contract_sb_cli.py`, exact SB-CLI-5/6 evidence manifests.
- `test_persistence_io_contract_sb_io.py:208` exact citation map; and
  `:231–245` exact collected whole-module test-name sets.
- `test_message_identity_contract_sb_id.py` duplicates named evidence
  requirements (subset guards are less restrictive than full equality).
- `test_delivery_contract_sb_delivery.py:267`
  `test_live_peek_stream_rejects_naive_cursor_completeness` only requires
  a nonempty section and that the spec cites its own name. It does not test
  cursor incompleteness despite its name.

Traceability is an explicit repository requirement. A generic check that
every cited path/node exists and each clause has firing evidence is useful.
The issue is a second manually hardcoded copy of the same citation list or
a fixed whole-module test inventory. Adding valid new regression coverage to
an extension dump/load module breaks the exact-set collection gate even when
its marker ownership and product behavior stay correct.

These gates read the winning specs directly; the listed actual firing tests
are the product keepers. For example, SB-SELECT strict bounds are protected by
`test_strict_open_bounds_on_queue_api`; late-ID filter semantics by
`test_move_behind_lower_bound_is_invisible_to_filter`; live rescan by
`test_live_peek_stream_deletion_visits_every_message`. Do not delete those.

Consolidate cross-spec path/node validity in one document gate. Preserve
required boundary membership as a subset where needed and separately assert
the direct opt-in suite has no routine shared/extension markers. Replace the
self-citing metadata test with that generic check. Before deletion, prove
missing/renamed citation and wrong effective collection marker fail, but a
new correctly marked firing test and coherent test rename do not. Equivalence
covers metadata only, never runtime behavior; no current deletion is approved.

### O9. Tests pin prose while their stronger runtime oracles already exist (F/C, medium)

Concrete examples:

- `test_cli_contract_sb_cli.py:748` in
  `test_sb_cli_6_newest_all_fails_before_target_inspection` requires the
  entire advisory sentence even though [SB-CLI-6] fixes conflict, pre-target
  failure, rc and structured error code, not that exact sentence.
- `test_cli_argument_parsing.py`
  `test_cleanup_help_uses_backend_generic_target_wording` requires a full
  cleanup-description phrase.
- `test_timestamp_selection_contract_sb_select.py`
  `test_select_filter_not_stream_offset`, `test_select_late_older_ids`,
  `test_select_watch_progress` demand English fragments. The executable
  late-ID/ordering tests in the same module are the actual behavior.
- `test_python_library_api_contract_sb_api.py` contains long prose-fragment
  tests alongside valuable public-signature/type checks.
- `test_constants.py:281`
  `test_configuration_guide_explains_vacuum_threshold_semantics` checks
  `percentage`, `0`, `100` anywhere in the whole guide. Those can come from
  unrelated settings, so the semantic claim can also false-pass.

Owner: CLI classifier/presentation in `cli.py`, canonical [SB-CLI-4,6],
[SB-SELECT-2,3,4], [SB-API-*] and configuration guide. Prefer stable codes,
keys, flags, exit states, clause links and behavior. Actionable guidance can
still be tested by the alternatives/constraints it conveys, not one complete
sentence. Scope any doc policy assertion to the owned section.
Validate with a semantics-preserving prose rewrite and a behavior/code
mutation. Keep closed public enum/schema/signature checks and grammar guidance
required explicitly by [SB-CLI-5]; not every text assertion is brittle.

### O10. Queue-name rejection property copies the production grammar (F, medium)

`tests/test_property_queue_names.py:40–64` derives valid strategies from
`db.QUEUE_NAME_PATTERN`; `_validator_accepts` explicitly mirrors the
production predicate and is used to exclude accepted names from rejection
cases. A production grammar that wrongly rejects a category can shrink the
generated valid set and expand the rejected set, with both properties agreeing.

Owner: `simplebroker/db.py:597` `_validate_queue_name_cached`;
[SB-DELIVERY-8] independently fixes ASCII allowed characters/start and 512
limit.

The cross-backend property “everything the validator accepts works in storage”
is still valuable. Separate that metamorphic property from an independent
contract grammar generator/oracle. Keep
`test_queue_validation.py` explicit ASCII/length/start examples and the
trailing-newline regression. Validate by changing allowed starts/character
classes: at least the contract property must fail. No blanket property-test
deletion.

### O11. Move-watcher tests forgive failed writes and do not hold handler state (F, high)

`tests/test_queue_move_watcher.py:244`
`TestQueueMoveWatcher::test_concurrent_operations` catches all writer
exceptions, prints them at 326, and sets expected count to
`5 + len(successful_writes)`. All five concurrent writes can fail while the
test passes by moving only the five initial rows. This misses the very
mixed-mode write/observe stress intended by the case.

`:359` `test_transaction_safety` ignores the result of
`move_completed.wait(1.0)`; the handler sets that event then sleeps 0.1.
The observer can run after the handler returns. A broken move that commits
after the handler can then pass. A handler never reached within the window
can also leave the expected state without proving the claimed boundary.

Owner: `watcher.py:2181` `_move_all_messages` calls Queue.move, then
increments/counts and dispatches; [SB-DELIVERY-2] requires committed move
before handler dispatch. [SB-ID-2] generated write and existing retry helper
own successful writes.

Fail on every unexpected writer error and require all ten exact bodies/IDs.
For transaction safety, gate the handler on a release Event, positively
observe its entry, inspect committed destination/source using an independent
connection while it is held, and release/join in finally. Validate failed
concurrent writes and deliberately delayed commit-after-handler fail.
Keep failure-isolation test: it covers handler exceptions, a distinct case.

### O12. Literal backend-API pin gate uses exact source formatting (F, low)

`tests/test_backend_plugin_resolution.py:570`
`test_first_party_extension_plugins_declare_literal_backend_api_version`
requires the substring `backend_api_version = <number>`. Annotation,
parentheses, legal whitespace, or an added comment containing that substring
can respectively false-fail or false-pass.

Owner: first-party extension plugin class declarations and
`simplebroker/_backend_plugins.py` compatibility comparison.
The design reason is valid: a third-party/extension artifact must declare its
literal compatibility version rather than alias the installed core's
`BACKEND_API_VERSION`. Preserve that boundary. Inspect the appropriate class
assignment as AST and require an integer literal with the supported value.
Validate formatting-only variants pass and aliased/dynamic declarations fail.
Runtime plugin-resolution compatibility tests remain separate keepers.

### O13. Bare-print AST gate is an incomplete proxy for stdout ownership (F/C, low)

`tests/test_commands_stdout_delivery.py:189`
`test_commands_module_has_no_unowned_stdout_prints` recognizes only bare
`print` and exact `file=sys.stderr` syntax. Equivalent stderr aliases fail;
`sys.stdout.write` or an imported print alias evade it.

Owner: `commands._print_stdout`, its flush handling and direct streaming
command closed-stdout boundaries; [SB-CLI-1,2]/[SB-DELIVERY-7].
The existing write/flush failure matrix in the same file is a stronger real
behavioral keeper, but does not automatically prove every command path is
covered.

Treat this as an architecture lint if intentionally retained, name its
limited scope, and use resolved output-sink references rather than exact
spelling. Expand the behavioral matrix before deleting it. Validate a new
unowned stdout path fails and an equivalent stderr alias passes. No proven
deletion equivalence yet.

### O14. Type rejection pins mypy's English diagnostic (F, low)

`tests/test_queue_typing_contract.py:205`
`test_generator_order_fixture_is_rejected_by_mypy`, line 227, counts exactly
three occurrences of `Unexpected keyword argument "order"`. A mypy release
that retains rejection but changes wording breaks the test.

Owner: public Queue generator signatures/overloads in `sbqueue.py`;
[SB-SELECT-5] intentionally excludes generator order. Keep actual negative
typechecking. Check errors at the three intended fixture call sites using
stable error codes/location and rejection outcome. Do not accept unrelated
type errors as evidence. Also bound the subprocess (O5). Validate a wording
change passes, while erroneously accepting any one of the three calls fails.

### O15. “Streaming limit” test does not distinguish eager full-input loading (F, medium)

`tests/test_security_fixes.py:15`
`test_stdin_size_limit_streaming` supplies the entire 11MiB payload and checks
only rejection. Replacing streaming read with an unbounded read followed by
size checking produces the same observed result.

Owner: `commands.py:353` `_read_from_stdin` deliberately reads bounded chunks
and stops once the limit is exceeded. [SB-DELIVERY-8] owns size acceptance;
bounded input handling is the named safety claim of this helper and test.

Retain the real CLI oversize boundary; add a controlled binary input source
whose further reads fail after enough bytes to prove over-limit, and assert
the consumer stops rather than draining an arbitrary tail. Do not pin the
exact 4096-byte implementation chunk if a different bounded chunk is safe.
Use independent read-budget/count evidence. A consume-all-then-check variant
must fail. Existing UTF-8 byte-limit tests protect size arithmetic, not
bounded consumption.

## Qualified observations and rejected shortcuts

The broad review found many mocks that are appropriate: driver absence,
transport ambiguity, broken write/flush, commit failure, invalid backend API,
clock rollback and fault-injected CAS outcomes cannot all be induced cheaply
or deterministically through a healthy substrate. Real state assertions
surround most of those seams.

Fixed wire records, unsafe JSON integers, exact IDs, public signatures,
closed error codes, canonical schema versions, byte limits, PRAGMA settings,
and native row-state checks are legitimate independent contracts. They are
not routine-dependency version pins. The historical 3.10.0 migration
reference in API guidance is a historical compatibility boundary, not the
current build-tool version.

The single-normalization call-count sensor in
`test_cli_main::test_main_preprocesses_each_invocation_once` explicitly owns
historical duplicate work with no same-result black-box symptom. Retain unless
the owner decides that internal cost policy no longer matters. Likewise,
`test_every_bare_constant_declaration_carries_an_explanation` is explicitly
required by the recorded no-magic-constants policy. Its regex is a limited
lint, not proof that comments explain anything; replacing that process policy
is not authorized by a test audit alone.

Queue/core/CLI, native SQLite, backend-neutral, async example and actual shell
boundaries cannot be called duplicates merely because all eventually read a
message. Iterator-resource assertions and transaction traces protect
ownership the final row set alone cannot show. Public API key/signature
inventories are not automatically implementation mirrors.

Further concerns need verification before promotion: external-schema cleanup
for ad-hoc opt-in pg↔redis/project-config fixtures; “many primitive exactly
once” routing sensors in Queue and CLI bounded-selection tests; fixed default
config inventory sizes; arbitrary timestamp timing comments in
`test_json_output::test_json_timestamp_edge_cases`. This appendix does not
claim those are confirmed defects, and proposes no deletion from them.

## Per-file full-read and retention ledger

Every row was read end to end. “Retention” describes the evidence worth
keeping, not an all-clean certification. O references identify findings above.

| File | Lines read | Test declarations read | Contract / retention evidence |
|---|---:|---:|---|
| `tests/compatibility/test_weft_artifact_versions.py` | 94 | 3 | Configured artifact version/source-origin proof, not current-version pins. |
| `tests/test_absolute_path.py` | 113 | 5 | Real relative/absolute CLI targets and cleanup scope. |
| `tests/test_after_flag.py` | 671 | 20 | Strict lower bounds, invalid grammar, escape handling, and pre-target failures. |
| `tests/test_alias_cli.py` | 247 | 10 | CLI alias operations over real stored state; keep public dispatch boundary. |
| `tests/test_aliases_db.py` | 194 | 14 | Alias graph, cycle, cache invalidation, and legacy queue shadowing. |
| `tests/test_backend_plugin_resolution.py` | 576 | 24 | Entry-point resolution and backend API boundary; literal-source concern O12. |
| `tests/test_backend_probe.py` | 265 | 7 | Materialization/retry/fork/poison boundaries; synchronization concern O6. |
| `tests/test_batch_delete.py` | 123 | 7 | Pending and claimed physical-delete isolation. |
| `tests/test_batch_delete_sqlite.py` | 104 | 3 | Native storage rows and chunk boundary beyond one bind page. |
| `tests/test_batch_operations.py` | 199 | 10 | Delivery-mode parity, limits, ordering, and validation. |
| `tests/test_before_flag.py` | 108 | 6 | Strict upper-bound selection and malformed forms. |
| `tests/test_broadcast.py` | 319 | 19 | CLI target grammar, patterns, literal bodies, no-target behavior. |
| `tests/test_broadcast_api.py` | 353 | 20 | Atomic copies and rollback with real persisted state. |
| `tests/test_broadcast_contract_sb_bcast.py` | 242 | 3 | Binding metadata plus SQLite execution-shape gate; O8. |
| `tests/test_broadcast_integration.py` | 167 | 4 | Real include/exclude, creation, ordering, claimed-only queues. |
| `tests/test_broker_factory.py` | 97 | 11 | Harness resolves real backend/core/Queue surfaces. |
| `tests/test_cleanup.py` | 884 | 30 | Owned namespace deletion, ordered report, literal hostile filenames. |
| `tests/test_cli_argument_parsing.py` | 160 | 7 | Grammar errors and clean help; prose concern O9. |
| `tests/test_cli_broken_pipe.py` | 459 | 18 | Actual OS pipe/delivery states; child lifetime concern O5. |
| `tests/test_cli_contract_sb_cli.py` | 884 | 31 | Public exits/JSON/errors/pre-target safety; O8/O9. |
| `tests/test_cli_dump_load.py` | 347 | 14 | CLI wire import/export, force/skew, invalid records. |
| `tests/test_cli_global_options.py` | 101 | 6 | Root action placement; version side-effect oracle O7. |
| `tests/test_cli_main.py` | 780 | 31 | Real parser/target/error ownership; single-normalization sensor retained. |
| `tests/test_cli_metadata.py` | 231 | 12 | CLI list/stats existence and claimed-only queue distinction. |
| `tests/test_cli_move.py` | 1180 | 47 | Public move grammar, bounds, state conservation; O1/O3. |
| `tests/test_cli_peek_include_claimed.py` | 65 | 4 | CLI flag ownership and claimed inspection. |
| `tests/test_cli_rearrange_args.py` | 680 | 57 | Registered-token grammar and option conservation. |
| `tests/test_cli_rename.py` | 175 | 11 | CLI rename state/errors and output. |
| `tests/test_cli_validation.py` | 214 | 11 | Directory/name/message errors, streams, exits. |
| `tests/test_cli_watch.py` | 258 | 7 | Real watch output/lifecycle with bounded readiness. |
| `tests/test_cli_write_output.py` | 281 | 22 | Silent default, explicit IDs, stored identity. |
| `tests/test_commands_error_ownership.py` | 332 | 15 | Ordinary exceptions remain unformatted at direct command seam. |
| `tests/test_commands_helpers.py` | 539 | 22 | Output errno, closed stdout, invocation validation seams. |
| `tests/test_commands_init.py` | 444 | 21 | Schema initialization, permissions, malformed path diagnostics. |
| `tests/test_commands_status.py` | 96 | 3 | Real direct status and target resolution. |
| `tests/test_commands_stdout_delivery.py` | 213 | 3 | Fault-injected write/flush plus real state; AST concern O13. |
| `tests/test_concurrency.py` | 366 | 5 | Real independent SQLite contention; Windows retry concern O2. |
| `tests/test_config_builder.py` | 624 | 40 | Public typed config builder, conversion, invalid keys/values. |
| `tests/test_config_coexistence.py` | 208 | 6 | Independent spawned configurations and serialization coexistence. |
| `tests/test_config_transport.py` | 384 | 17 | JSON/mapping/pickle transport and unknown envelope fields. |
| `tests/test_constants.py` | 1086 | 58 | Defaults/ranges/environment resolver; prose concern O9; policy lint retained. |
| `tests/test_core_persistence_transition_tables.py` | 1314 | 4 | Real migration/rollback/CAS/admission/fork transition tables. |
| `tests/test_cross_backend_dump_load.py` | 150 | 2 | Opt-in real PostgreSQL↔Redis preservation boundary. |
| `tests/test_custom_runner_integration.py` | 351 | 10 | Supplied runner transaction protocol plus real storage. |
| `tests/test_db_contract_edges.py` | 100 | 8 | Stop/no-create/validation edges before mutations. |
| `tests/test_delete_from_queues.py` | 114 | 7 | Physical deletion constrained to selected queues. |
| `tests/test_delivery_contract_sb_delivery.py` | 509 | 19 | Delivery states and real streams; binding mirrors O8. |
| `tests/test_dump_load.py` | 876 | 36 | Independent wire vectors, force/high-water/rollback/quiet policy. |
| `tests/test_edge_cases.py` | 278 | 5 | Schema/migration/corruption boundaries; child lifetime O5. |
| `tests/test_exactly_once_delivery.py` | 404 | 12 | Observer-visible committed claims and loss windows; O5. |
| `tests/test_example_async_stream_transitions.py` | 335 | 1 | Real async SQLite example demand/transaction/cancellation states. |
| `tests/test_exception_hierarchy.py` | 33 | 2 | Public exception inheritance. |
| `tests/test_ext_imports.py` | 91 | 3 | Public advanced exports and extension import isolation. |
| `tests/test_find_message_ids.py` | 175 | 8 | Literal substrings, wildcard escaping, queues and bounds. |
| `tests/test_generator_methods.py` | 380 | 15 | Queue/core stream order, errors, reentrant lifecycle. |
| `tests/test_has_pending_validation.py` | 17 | 1 | Wrong-type queue is rejected, not empty. |
| `tests/test_insert_messages.py` | 392 | 23 | Exact-ID insertion, collisions, zero/range rejection, rollback. |
| `tests/test_invalid_config_lifecycle.py` | 530 | 21 | Fresh import safety, typed consumer validation, resolver gates. |
| `tests/test_isolated_config.py` | 112 | 4 | Explicit per-instance config does not change another instance. |
| `tests/test_json_message_id_contract.py` | 145 | 6 | Owned JSON producers preserve IDs above 2**53 as strings. |
| `tests/test_json_output.py` | 666 | 29 | NDJSON shape/types and state; historical timestamp oracle note. |
| `tests/test_keep_newest.py` | 305 | 16 | Atomic pending window, aliases, reservations, invalid N. |
| `tests/test_key_material.py` | 79 | 7 | Type-sensitive normalized config/session identity. |
| `tests/test_latest_pending_timestamp.py` | 110 | 7 | Actual queue pending maximum differs from global high-water. |
| `tests/test_maintenance_policy.py` | 69 | 5 | Pure threshold/schedule boundaries with independent values. |
| `tests/test_malformed_target_diagnostics.py` | 71 | 2 | No leaked credentials and malformed-target rejection. |
| `tests/test_message_by_timestamp.py` | 855 | 29 | CLI exact-ID operations and isolation; loser-exit concern O4. |
| `tests/test_message_claim.py` | 697 | 19 | Claim persistence, native SQLite state, vacuum transitions. |
| `tests/test_message_id_validation.py` | 138 | 9 | Independent exact-ID range/type/canonical form vectors. |
| `tests/test_message_identity_contract_sb_id.py` | 310 | 2 | Message-ID authority/binding metadata; O8 consolidation. |
| `tests/test_message_size_contract.py` | 86 | 4 | Non-string/lone-surrogate rejection before any mutation. |
| `tests/test_misc.py` | 135 | 5 | CLI ID uniqueness and ambient-environment harness isolation. |
| `tests/test_move.py` | 176 | 9 | Real queue transfer, order, identity and no-match. |
| `tests/test_move_after_exclusion.py` | 153 | 2 | Watcher lower-bound/move exclusion; bounded subprocess note O5. |
| `tests/test_move_by_id.py` | 235 | 9 | Exact-ID move preserves original identity and delivery mode. |
| `tests/test_move_checkpoint_semantics.py` | 278 | 5 | Positive watcher checkpoint proof and late moved IDs. |
| `tests/test_move_claim_patterns.py` | 205 | 4 | Real claim/move conservation and transactional rollback. |
| `tests/test_multi_queue_watcher_example.py` | 391 | 10 | Published selector and drain/stop/lifetime behavior. |
| `tests/test_operations_contract_sb_ops.py` | 278 | 6 | Ops authority plus exact firing-test manifests; O8. |
| `tests/test_path_security.py` | 871 | 34 | Real containment/input attacks; shell-argument static guard retained. |
| `tests/test_paths_coverage.py` | 299 | 17 | Host-path branches and Windows-specific duck seams. |
| `tests/test_peek_generator_lifecycle.py` | 507 | 10 | Real core/Queue lease close before/after first iteration. |
| `tests/test_peek_include_claimed.py` | 128 | 8 | Mixed-state order and pending-only default. |
| `tests/test_peek_keyset_pagination.py` | 284 | 11 | Multiple small pages with exact native IDs. |
| `tests/test_peek_keyset_scaling.py` | 70 | 3 | SQLite VM-step growth, not wall-clock timing. |
| `tests/test_persistence_io_contract_sb_io.py` | 245 | 3 | Routine/opt-in marker ownership; exact-set concern O8. |
| `tests/test_portability.py` | 112 | 4 | POSIX umask/mode subprocess and resolve-failure warning. |
| `tests/test_pragma_settings.py` | 344 | 16 | Real PRAGMA policy and setup cursor finalization. |
| `tests/test_project_config.py` | 1662 | 60 | Lossless TOML/backend initialization/target selection; O6. |
| `tests/test_project_scoping.py` | 575 | 25 | Bounded project discovery and filesystem precedence. |
| `tests/test_property_cli_args.py` | 162 | 5 | Real normalizer+parser grammar properties. |
| `tests/test_property_dump_load.py` | 184 | 3 | Literal wire plus real generated round-trip/high-water. |
| `tests/test_property_message_roundtrip.py` | 130 | 5 | Unicode/size/NUL backend semantics. |
| `tests/test_property_queue_model.py` | 256 | 1 | Independent Python pending/claimed state model and rules. |
| `tests/test_property_queue_names.py` | 109 | 4 | Backend usability is valuable; rejection oracle mirror O10. |
| `tests/test_property_timestamp_validate.py` | 184 | 13 | Independent equivalent time forms and malformed values. |
| `tests/test_python_library_api_contract_sb_api.py` | 433 | 26 | Public signatures/types and metadata; O8/O9. |
| `tests/test_queue_api_additions.py` | 268 | 12 | Public metadata/results/repr/context over real storage. |
| `tests/test_queue_api_comprehensive.py` | 1003 | 49 | Public method surface/modes/filters; routing sensor noted. |
| `tests/test_queue_config_defaults.py` | 177 | 8 | Target/config path snapshot and scope defaults. |
| `tests/test_queue_metadata.py` | 163 | 12 | Queue public pending/claimed/total/existence stats. |
| `tests/test_queue_move_cross_target.py` | 151 | 7 | Target identity validation before connection work. |
| `tests/test_queue_move_watcher.py` | 567 | 12 | Move-before-handler and state; workload/gating concern O11. |
| `tests/test_queue_rename.py` | 171 | 10 | Pending/claimed rename, alias/version, no-mutation failures. |
| `tests/test_queue_typing_contract.py` | 227 | 2 | Real mypy call rejection/public overloads; O14. |
| `tests/test_queue_validation.py` | 134 | 12 | Independent allowed characters, starts, length, types. |
| `tests/test_safety_fixes.py` | 523 | 12 | Real delete safety, UTF-8 bytes, spawned ID uniqueness, cleanup. |
| `tests/test_security_fixes.py` | 143 | 6 | Real containment/size limits; streaming sensitivity O15. |
| `tests/test_shell_examples.py` | 1616 | 42 | Black-box Bash flow plus real state and hostile adapter seams. |
| `tests/test_sidecar.py` | 250 | 12 | Real transaction commit/rollback/session/lock and vacuum survival. |
| `tests/test_smoke.py` | 74 | 4 | Public CLI subprocess write/read/stdin/empty/FIFO. |
| `tests/test_sql_builder_validity.py` | 60 | 4 | SQLite actually executes example-used SQL builders. |
| `tests/test_sql_internals.py` | 229 | 10 | Real retrieve transaction/no-open-state and builder validation. |
| `tests/test_status_command.py` | 76 | 4 | Public status text/JSON/high-water/size/error output. |
| `tests/test_streaming.py` | 112 | 3 | Positive emission-before-exhaustion and real cross-page peek. |
| `tests/test_symlink_security.py` | 329 | 11 | Actual symlink containment/loops and JSON/plain failures. |
| `tests/test_target_redaction.py` | 167 | 11 | Independent escaped/malformed password/control-character corpus. |
| `tests/test_timestamp_advance.py` | 163 | 8 | Durable monotonic floor and known/ambiguous fault outcomes. |
| `tests/test_timestamp_bound_grammar.py` | 255 | 14 | Independent fixed conversions/limits/fraction guidance. |
| `tests/test_timestamp_edge_cases.py` | 480 | 28 | Clock rollback/conflict/cache/CAS and encoding boundaries. |
| `tests/test_timestamp_helpers.py` | 39 | 2 | Core and Queue allocation aliases monotonically advance. |
| `tests/test_timestamp_resilience.py` | 324 | 10 | Real inconsistency repairs, retry budget seams; O5 thread lifetime. |
| `tests/test_timestamp_selection_contract_sb_select.py` | 426 | 16 | Independent ID/order matrix plus binding/prose concern O8/O9. |
| `tests/test_worker_examples.py` | 901 | 34 | Published Bash acknowledgement/recovery with real CLI and shims. |
| `tests/test_write_returns_id.py` | 214 | 8 | Committed row identity, conflict retry, high-water distinction. |

## Follow-up verification and residual limits

Implement fixes in coherent slices with independent review. Start with O1,
O2, O4 and O11, because they silently forgive failures or use the wrong
oracle. Handle O3/O5/O6 as lifecycle/ordering work without weakening product
timing or serializing contention. O8/O9/O12/O14 are maintainability fixes,
not permission to drop firing behavior.

Focused commands should select each named test/class and its existing
keeper; run the backend-neutral selections via the pg/redis harness where
marked shared, and real Windows SQLite concurrency in CI. Run the repository
documentation gates after adding this appendix. The root agent owns final
integration verification and any commit.

Skill evaluation: the skill's contract-first distinction worked well. A
useful future refinement is to separate generic citation validity from a
second exact evidence manifest, and to distinguish documented architecture
cost sensors from arbitrary helper call counts. No skill file was changed.
