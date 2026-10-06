# Test audit: tooling and shared support

Date: 2026-10-06. Baseline: `d4d2634`. Report-only; no tests or product code changed.
Parent: [whole audit plan](2026-10-06-whole-test-surface-audit-plan.md).
All 53 inventoried files below were read in full. Candidates were traced to their
actual owner, sibling coverage and reachable failure paths. Priorities describe
test signal and containment, not a claim of an observed product defect.

## Source Documents

- `docs/program-theory.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/specs/01-development-documentation-operating-model.md`
- `docs/specs/10-cli.md`
- `docs/specs/16-python-library-api.md`

The test-audit skill was read in full. Exact test/support owners and focused
verification boundaries are recorded below; citations do not replace reading.

## Findings

### T1. Routine dependency updates still break literal policy mirrors (P2, fix)

`tests/test_release_workflow.py::test_every_uv_workflow_uses_the_repository_pin`
(line 256) requires the exact current UV_VERSION `0.12.0`. A valid update of all
pins fails it. `test_development_tool_floors_are_current` (179) fixes Ruff and
pytest-timeout lower bounds to exact strings even though raising a lower bound
preserves the needed minimum. `test_fuzz_dependency_group_is_opt_in` (163)
compares complete Atheris requirement strings rather than marker support and
opt-in behavior. The build pin failure was already repaired in `d4d2634`;
these are the same failure class elsewhere.

Owners: pyproject dependency groups, workflow env/setup steps, and
`bin/bump_uv.py::_configured_versions/_validate_versions/_update`.
These have real operator callers. `tests/test_bump_uv.py` exercises coordinated
updates and mismatch/rollback failures; it is not a substitute for checking
current repository consistency. Keep one derived cross-file consistency guard,
closed dependency-name policy if still intended, supported marker partitions,
and independently specified minimum capabilities. Do not freeze a chosen
version, specifier spelling, or whitespace.

Mixed case: `test_release_script.py::test_repository_backend_api_v9_handshake_and_floors_match`
(860) should retain exact runtime handshake and independently recorded historic
API-to-minimum-core mappings. Its current API `==9` assertion and exact README/
spec prose should not force test edits on a valid next API revision or wording
change. Current-version relationships can be derived; historical compatibility
facts cannot be derived from the current implementation without losing an oracle.
No coverage deletion is proposed here.

History: `d4d2634` addresses the triggering build update; the floors test itself
documents a prior exact-floor cleanup that left these exceptions.
Validation after repair: `uv run pytest tests/test_bump_uv.py tests/test_release_workflow.py tests/test_release_script.py`;
positive control is a valid higher pin/floor and negative control an inconsistent
or unsupported configuration.

### T2. PostgreSQL fixture reset can report isolation without completing it (P2, fix)

`tests/conftest.py::_reset_pg_tables` (504) catches every SimpleBroker
`OperationalError` across three reset statements, initializes the schema and
returns. Its comment assumes missing tables. PostgreSQL error translation also
uses this class for retryable errors, including serialization/deadlock/lock
errors. If messages were truncated but aliases reset fails, existing-schema
initialization takes its fast path and aliases/meta may remain from the prior
test. The fixture has many test callers and no product callers.

Owners inspected: the complete reset helper, PG runner translation/run seam,
and PG schema initialization, independently checked by the backend reviewer.
No existing reset proof establishes failure classification or complete reset
after missing-table recovery. Fix at this support boundary: classify the original
SQLSTATE, recreate only for missing schema/relation, then complete/reset again;
propagate other failures. Ensure reset is atomic or failed setup is explicit.
Do not teach tests to accept stale state. A fake runner may control SQLSTATE,
but a real PG reset probe is needed for isolation semantics.

History: recent fixture changes include `dc2e6cc` (shared released-backend
contracts) and `8c3abe8`; no evidence this explains any specific past CI failure.
Validation after repair: reset fault-injection tests plus the canonical PG runner
under default concurrency, including aliases/meta sentinel rows.

### T3. Managed subprocess waits are not actually bounded (P2, fix; unused seam candidate)

`tests/helper_scripts/managed_subprocess.py::OutputReader.get_output` (69)
drains until a timed queue read finds an empty interval. A continuously producing
child can keep it inside that loop beyond
`ManagedProcess.wait_for_output(timeout=...)` (127). The outer deadline is only
checked after the drain returns. `managed_subprocess` (313) advertises a normal
runtime `timeout` argument but never uses it; `run_subprocess` (419) calls
`wait()` without a timeout. The context owns teardown after its body returns;
that is not a bound on the body.

Concrete read-only owner probe: controlled arriving queue, 100 lines with 1ms
arrival delay; `wait_for_output("never", timeout=0.01)` returned False after
0.228s. No owner mutation or source edit. This proves deadline overshoot, not an
infinite live-child hang. Existing managed-process transition tests exercise
exit, escalation and cleanup, not sustained output during readiness waiting.

Keep cleanup/escalation proofs. Bound each output snapshot/deadline check. Either
give the normal runtime argument a real meaning or remove the misleading
internal argument. Repository-wide call search found no `run_subprocess` caller
outside its definition, so deletion is a candidate after confirming exports and
external support use. Existing `managed_subprocess` callers remain in scope;
this is not permission to prune the entire helper.

Also `test_dev_scripts.py::test_coverage_sigterm_saves_readable_data_from_terminated_process`
(1021) performs blocking `stdout.readline()` before bounded communicate.
A missing readiness line can prevent its finally cleanup. Preserve real SIGTERM
coverage persistence, use bounded readiness and guaranteed reap instead.

History: managed support participates in `86f73f5` state-machine audit and
Ruff/type work; those do not prove bounded readiness.
Validation: canonical managed-process transitions and signal test; controlled
never-ready and continuously-writing children with proven cleanup. Do not
increase suite timeouts.

### T4. Product-based calibration weakens performance regression detection (P2, fix oracle)

`tests/performance_calibration.py` measures BrokerDB writes and claims and
computes reference/current ratios. `tests/test_performance.py::get_timeout`
(130) divides baseline time by that ratio. On the slower-than-reference branch,
a 2x write regression halves its calibration ratio and doubles its allowed write
budget. Thus a common slowdown can cancel in measurement and oracle.
The mixed CLI budget also depends on product calibration. Python startup-tax
measurement is a distinct cost and should be distinguished from SB overhead.

This is not evidence of a current slowdown, and calibrated liveness limits remain
useful for platform variance. Separate those limits from performance claims:
use a same-host before/after comparison or a calibration workload independent of
the changing SB owner. Keep absolute independent throughput floors and
algorithmic/keyset scaling proofs. Benchmark reporting support has operator
callers; these test-only calibration helpers do not change runtime SB.

History: `8c3abe8`, `d94d7e2`, `1c49a60` affected these tests; the present
never-tighten rationale protects noisy fast hosts but does not solve a circular
regression oracle. No measured regression was asserted.
Validation: a focused slow-owner control should fail a regression gate while the
ordinary candidate passes under the same host/load; correctness timeouts need
not fail the same control.

### T5. Mixed CLI benchmark does not require its writes to succeed (P2, fix)

`tests/test_performance.py::test_sequential_mixed_cli_throughput` (515) accepts
exit 2 for every operation and only requires at least five total successes.
Five successful reads can satisfy the comment that all five writes succeeded,
even if every write incorrectly exits 2. Owner is the real CLI/run_cli boundary;
empty reads are valid, empty writes are not. Other write tests protect ordinary
write semantics but do not prove the measured workload performed its writes.

Keep per-operation identities, require successful write exits, and verify written
messages or the intended end state. Do not remove the benchmark or disallow valid
empty reads. Validate with a control returning empty only for the measured write
operations. Static reasoning establishes the gap; that control has not been run.

### T6. Backend benchmark leaks owned remote state on workload failure (P2, fix)

`tests/backend_benchmark.py::run_benchmarks` (481) cleans PG/Redis projects only
after workload success. A failed workload exits TemporaryDirectory first,
deleting the local project description without invoking remote cleanup. Docker
contexts still dispose their owned container, but explicit external DSN/URL modes
can leave benchmark-created schemas/namespaces behind.

`tests/test_backend_benchmark_smoke.py::test_postgres_benchmark_docker_mode_cleans_up_after_failure`
and its Redis counterpart prove container teardown only. They are valid harness
tests but not a keeper for project cleanup in external-service topology.
The distinct `bin/benchmark` trial-target finally path does not repair this owner.
Wrap project cleanup in finally, preserving the primary workload error, then
prove it runs before local metadata disappears. This is support, not broker
persistence corruption. Recent history includes `edf6e33` type work; no
failure-injection publication evidence was found for this path.

Validation: smoke suite with external-service mode and injected workload failure;
real owned-schema/namespace cleanup probe when services are available.

### T7. Exact duplicate CLI-coverage invocations add cost without new proof (P3, consolidation candidate)

`tests/test_dev_scripts.py::test_run_cli_atomically_promotes_readable_coverage`
(1697) calls precisely the same `_cli_coverage_real_child_publishes` payload as
transition `CHILD-SUCCESS-VALIDATE-PUBLISH` (885). Same setup, owner, topology,
inputs and assertions. Keep the transition invocation as the proposed keeper.
Likewise `test_cli_coverage_cleans_staging_when_runner_fails` (1800) invokes the
same timeout/runner-error helper cases already present in the transition table.

No product seam deletion is unlocked. Lower-level `_promote_cli_coverage`
tests versus wrapper `_publish_cli_coverage` tests are not automatically duplicates:
wrapper cleanup and reader validation can fail differently. Keep them until
case-by-case equivalence is shown. Validate collection/routing (including backend
marks) and focused transitions before removing either duplicate. No deletion
or keeper sensitivity run was performed in this report-only audit.

### T8. Some static guards freeze syntax or claim behavior they do not execute (P3, fix selectively)

`test_release_workflow.py::test_release_gate_pypi_job_keeps_tokenless_minimum_permissions`
(731) checks a literal indented YAML permission block. Keep tokenless scoped
permission policy, parse YAML and compare semantic scopes/DAG instead.
`test_product_section_registry_final_cutover.py::test_registered_product_owners_and_entry_links_resolve`
also requires exact explanatory README sentences. Keep real links and canonical
owners, not wording. `test_release_script.py::_remote_tag_reuse_note` tests mix
real tag/command safety output with incidental prose.

`test_scorecard_normalizes_invalid_repository_level_sarif_locations` (88)
protects important workflow trust separation but asserts jq source fragments,
not the transformation it names. The complete jq filter was read; no executable
normalization test was found. Add an actual invalid-location fixture and a valid
location preservation fixture at the jq boundary, retain the workflow permission/
job-routing guard. A no-op filter containing the fragments can pass the current
normalization assertions. No such mutation was run.

`test_release_helper_has_no_remote_tag_deletion_path` uses source spellings to
deny selected delete patterns. Keep no-remote-tag-mutation semantics, prove emitted
commands over reachable states rather than claim arbitrary unsafe command detection
from two grep patterns. Existing command-recording transition tests are a better
starting point, not an already verified exhaustive keeper.

Static inspection is justified for immutable action refs, package paths, permissions
and declared artifact contracts. Do not delete all workflow or documentation tests.
Focused validation must include actual owner dry-runs/executable jq controls, not
only the revised assertions.

### T9. Standalone example tests overclaim fork/close proof and rely on narrow real-time windows (P2, fix)

Final inventory reconciliation found two modules directly under examples/, outside
examples/tests/: both were subsequently read in full and added to this ledger.
`examples/test_sqlite_connect.py:537`, `TestSQLiteConnectionManager.test_fork_safety_simulation`,
only checks both returned values are sqlite3 connections after changing `_pid`.
It never requires a new identity, cleared phase state, or unusability of the old
connection in the simulated-PID case. Real fork recovery should abandon, not
close, inherited native handles; do not prescribe that simulated cleanup policy
for a real child. Disabling fork recovery can leave it green. The context-manager
test at :528 asserts only inside the context, then has a comment saying cleanup
occurred; the separate close test at :590 checks emptied tracking sets, not
raw-handle unusability.
Keep actual context-close and PID-recovery assertions at this example's owner,
not the similar but separate core SQLite owner.

The complete `examples/sqlite_connect.py` owner was read. Its `_handle_fork`
itself acquires `_setup_lock` and `_connections_lock`; the simulation cannot
establish safe recovery when another parent thread held either lock at fork.
This is a credible standalone-example correctness concern, not a reproduced
core SB defect. A bounded real held-lock fork probe would resolve it. Do not
quietly classify it as a timing flake or claim the core fork tests protect this
separate example implementation.

`TestUtilityFunctions.test_interruptible_sleep_normal` (:327) requires a 100ms real
sleep to finish within 150ms; the interrupted case relies on a sleeping sender
and a sub-100ms deadline (:336). Scheduler delays are not incorrect sleep behavior.
Use controlled Event/clock semantics for policy and a synchronized real wake
with a supported liveness bound for wiring. Many example manager tests close
only after successful assertions, and thread-local connection tests join without
a deadline. Register resources/threads in failure-path cleanup immediately.
The claimed unittest fallback cannot work without pytest (imported at module
scope), and its plain test classes are not unittest.TestCase subclasses. The
fallback is unreachable/nonfunctional; do not count it as verification.

`examples/test_async_pooled_broker.py` retains valuable real migration/sidecar,
commit-order, reversed-record normalization, cross-task batch-close and cancelled
BEGIN proofs. Its gated ordering/cancellation tasks have unbounded readiness
awaits and mostly success-path gate release/task cleanup. Give these probes
bounded coordination and finally cancel/await owned tasks before broker close;
do not weaken the actual transaction/rollback assertions. Relevant async runner,
stream, write and async_broker owners were read. No async product defect was
established. No controls or focused runs for these two added files ran in this
audit; history and external standalone consumers remain unverified. Retain all
coverage pending owner-boundary controls. Validation should use the repository's
examples gate, not the advertised unittest fallback.

## Retained false positives

Historical SQLite layout fixtures intentionally freeze old storage artifacts.
Negative typecheck consumers protect downstream API use rather than internal
compilation. Exact runtime backend handshake is a contract, unlike current package
version trivia. Public export inventories can be intended compatibility promises.
Coverage worker lifecycle probes exercise real pytest-cov wiring. Publication
tests substitute GitHub transport while executing the real release state machine.
State-machine manifest tests are structural evidence, not semantic correctness;
keep that distinction explicit. Queue-owned activity waiters are closed by Queue:
an initially suspected waiter leak was rejected after tracing ownership.

## Limits and suggested order

No full-suite run or exhaustive mutation campaign was performed for this audit.
The one bounded support probe above ran without edits. Prior release greens do not
establish sensitivity of these tests. Repair isolation/containment first, then
weak oracles, then dependency mirrors, and only then exact duplicate invocations.
Remediation needs independent preservation review and normal runner concurrency.
No product defect or production seam deletion has been established by this lane.
The skill correctly separated static contract guards from incidental snapshots;
no change to the skill is needed based on this lane.

## Full-file read ledger

All rows: read in full at baseline; owner tracing deeper for candidates above.
"No finding" means no supported actionable finding from this pass, not perfect
coverage or an exhaustive proof of absence.

| File | Lines | Test declarations |
| --- | ---: | ---: |
| `examples/test_sqlite_connect.py` | 851 | 45 |
| `examples/test_async_pooled_broker.py` | 519 | 10 |
| `fuzz/fuzz_cli_args.py` | 54 | 0 |
| `fuzz/fuzz_dump_load.py` | 60 | 0 |
| `fuzz/fuzz_timestamp_validate.py` | 59 | 0 |
| `tests/__init__.py` | 1 | 0 |
| `tests/backend_benchmark.py` | 860 | 0 |
| `tests/conftest.py` | 1075 | 0 |
| `tests/coverage_subprocess.py` | 56 | 0 |
| `tests/helper_scripts/__init__.py` | 22 | 0 |
| `tests/helper_scripts/broker_factory.py` | 111 | 0 |
| `tests/helper_scripts/cleanup.py` | 122 | 0 |
| `tests/helper_scripts/cross_thread_generator_probe.py` | 759 | 0 |
| `tests/helper_scripts/database_errors.py` | 129 | 0 |
| `tests/helper_scripts/functions.py` | 17 | 0 |
| `tests/helper_scripts/managed_subprocess.py` | 435 | 0 |
| `tests/helper_scripts/sqlite_legacy_layouts.py` | 107 | 0 |
| `tests/helper_scripts/timestamp_validation.py` | 24 | 0 |
| `tests/helper_scripts/timing.py` | 243 | 0 |
| `tests/helper_scripts/watcher_base.py` | 131 | 0 |
| `tests/helper_scripts/watcher_patch.py` | 42 | 0 |
| `tests/helper_scripts/watcher_sigint_script_improved.py` | 121 | 0 |
| `tests/helpers/__init__.py` | 1 | 0 |
| `tests/helpers/state_machine_contracts.py` | 56 | 0 |
| `tests/peek_pagination_benchmark.py` | 297 | 0 |
| `tests/performance_calibration.py` | 165 | 0 |
| `tests/state_machine_manifest.py` | 361 | 0 |
| `tests/test_agent_kernel_contract.py` | 109 | 7 |
| `tests/test_backend_benchmark_smoke.py` | 486 | 13 |
| `tests/test_benchmark.py` | 771 | 21 |
| `tests/test_bump_uv.py` | 268 | 4 |
| `tests/test_dev_scripts.py` | 3503 | 98 |
| `tests/test_doc_gates.py` | 58 | 3 |
| `tests/test_documented_exit_codes.py` | 56 | 4 |
| `tests/test_keep_newest_benchmark.py` | 33 | 1 |
| `tests/test_managed_subprocess_transitions.py` | 425 | 2 |
| `tests/test_no_dependencies.py` | 71 | 2 |
| `tests/test_performance.py` | 875 | 13 |
| `tests/test_performance_harness.py` | 106 | 6 |
| `tests/test_plan_context_gate.py` | 73 | 4 |
| `tests/test_product_section_registry_final_cutover.py` | 93 | 2 |
| `tests/test_program_theory_contract.py` | 786 | 17 |
| `tests/test_public_surface.py` | 60 | 3 |
| `tests/test_release_publication_script.py` | 439 | 14 |
| `tests/test_release_script.py` | 2640 | 79 |
| `tests/test_release_workflow.py` | 1243 | 52 |
| `tests/test_release_workflow_gate.py` | 152 | 7 |
| `tests/test_ruff_policy.py` | 401 | 11 |
| `tests/test_ruff_suppression_index.py` | 674 | 29 |
| `tests/test_state_machine_policy.py` | 448 | 10 |
| `tests/test_timing_helpers.py` | 290 | 18 |
| `tests/typecheck_fixtures/queue_delete_none.py` | 7 | 0 |
| `tests/typecheck_fixtures/queue_generator_order.py` | 9 | 0 |
