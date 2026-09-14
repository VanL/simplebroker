# Dead code and duplication cleanup

Class: 3 — repository-internal cleanup crossing the core package, backend
extensions, test infrastructure, runner lifecycle ownership, and process/fork
behavior. It does not change a public contract or own a release.

Plan type: implementation without spec revision.

Status is owned by `docs/plans/README.md`.

## Goal

Remove code proven to have no callers, collapse duplication where one existing
owner can preserve all current semantics, and leave load-bearing wrappers and
test seams in place. Turn the audit into small, reversible slices rather than
treating every short wrapper or repeated expression as dead code.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-4]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-2], [SB-API-3],
  [SB-API-9], [SB-API-10], [SB-API-11]
- `docs/specs/15-persistence-io.md` [SB-IO-2]
- `docs/implementation/02-repository-map.md`
- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/08-message-identity-and-write-visibility.md`
- `docs/implementation/10-ruff-suppression-registry.md`
- `docs/guides/backends.md`, especially “Cross-backend benchmarking”
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/plans/2026-09-12-config-object-simplification-plan.md`
- User-provided removal inventory:
  `/Users/van/.codex/attachments/e585ba75-b234-4f03-b5fe-1fad17f047a5/pasted-text.txt`

No spec text changes are proposed. This plan preserves the governing product
contracts and narrows implementation and test scaffolding only.

## Spec Baseline

- `f213b12d48fdad74872046c1cb81d03a863092e3` — the SimpleBroker spec tree
  at plan authoring time. The implementation must rebase its contract checks
  onto the then-current spec tree if the active configuration plan lands first.

## Context and Key Files

### Read before editing

- `simplebroker/db.py` and `simplebroker/_broker_session.py`: runner ownership,
  session identity, target normalization, and borrowed-runner teardown.
- `simplebroker/sbqueue.py`, `simplebroker/_backend_plugins.py`,
  `simplebroker/_backends/__init__.py`, and `simplebroker/_paths.py`: public
  construction boundaries, plugin resolution, and intentionally retained test
  seams.
- `extensions/simplebroker_redis/simplebroker_redis/{core,plugin,pool,runner,validation}.py`:
  lock registry behavior, namespace key ownership, plugin normalization, and
  direct-runner fallback.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py` and the PostgreSQL test
  fixtures in `tests/conftest.py`: runner shutdown and real-service ownership.
- `tests/conftest.py` and all modules under `tests/helper_scripts/`: package
  import routes, CLI child environment, backend fixtures, and process cleanup.
- `bin/benchmark.py`, `tests/backend_benchmark.py`, and
  `docs/guides/backends.md`: two intentionally different benchmark products.

### Comprehension gate

Before editing, record answers in the execution log. A wrong answer blocks the
slice until the cited owners are reread.

1. Why is `_BorrowedRunner` not redundant with `DBConnection._external_runner`?
   Expected answer: `_BorrowedRunner` masks destructive lifecycle methods on a
   caller-owned injected runner at every lower layer that receives it;
   `_external_runner` only records ownership at the `DBConnection` boundary.
   Removing the wrapper lets lower teardown close or shut down a resource the
   caller still owns, violating [SB-API-11].
2. Where may omitted configuration be resolved?
   Expected answer: at every supported public construction boundary, including
   direct command functions, `Queue`, `open_broker`, `DBConnection`, and direct
   supported runner/core entry points. Private callees may require the resolved
   `Config`; `cli.main` is not the sole entry point.
3. Why are the benchmark harnesses not merge candidates?
   Expected answer: `bin/benchmark.py` is the documented best-of-three
   CLI/API/optimized-API primitive matrix; `tests.backend_benchmark` is the
   older CLI-only bulk/status harness. Shared service setup does not make their
   workload contracts interchangeable.

### Current ownership

- `tests/helper_scripts/__init__.py` is a normal package initializer. It may
  export fewer names, but it remains so imports do not depend on namespace
  package behavior.
- `tests/helper_scripts/broker_factory.py::active_backend` is the existing
  reusable backend-name lookup.
- `tests/conftest.py::build_cli_env` owns the normal CLI child environment, but
  pipe-oriented tests have intentionally stricter ambient-environment and stdin
  behavior. A shared helper must model those differences explicitly.
- `simplebroker._broker_session` owns process-local session keys and SQLite
  target identity. `Queue` and `DBConnection` are separate public ownership
  boundaries and both must remain stable under relative paths and `chdir`.
- `extensions/simplebroker_redis.simplebroker_redis.validation.RedisKeys` owns
  broker Redis key construction.
- The weak-value Redis write-lock registry and its fork reset are load-bearing;
  `_SharedWriteLock` is not. Plain `threading.Lock` objects are weak-referenceable
  on every supported Python version (3.11+).

## Invariants and Constraints

1. Preserve all public CLI and Python behavior, import paths, exit codes,
   storage formats, backend API version, and configuration precedence unless
   the separate configuration plan explicitly changes them.
2. Keep `_BorrowedRunner` and its destructive-method masking. Keep a real
   caller-owned-runner lifecycle test.
3. Resolve omitted configuration at supported public boundaries. Do not make
   `cli.main` the only resolver and do not force public callers to construct a
   `Config` first.
4. Preserve SQLite target identity across relative paths, current-directory
   changes, `Queue`, `DBConnection`, session reuse, and waiter construction.
   Centralizing normalization is allowed; deleting an ownership-boundary
   normalization is not allowed without same-target evidence.
5. Preserve the Redis lock registry's weak lifetime, same-key sharing,
   different-key independence, thread safety, and post-fork reset.
6. Preserve direct `RedisRunner` construction with omitted `pool_options`.
   Plugin paths may normalize once, but the public runner fallback stays until
   a separately planned public-contract change says otherwise.
7. Keep backend-specific config semantics: PostgreSQL strips passwords and
   writes JSON-compatible TOML values; Redis uses its current quoting and URL
   behavior. Sharing a writer must not flatten those differences.
8. Keep cleanup best-effort and exhaustive. One failed project/schema/namespace
   cleanup must not prevent later independent cleanup attempts.
9. Do not make the PostgreSQL extension depend on a newly invented private core
   exception helper. Core-local and extension-local helpers may share an owner
   only through a versioned public extension surface or a coordinated release.
10. Do not merge the two benchmark harnesses. Any shared Docker/service helper
    must reduce code without changing either harness's workload, timing, output,
    cleanup, or documented invocation.
11. No new dependency, no new framework, no speculative helper API, and no
    line-count target. If consolidation needs a parameter matrix larger than
    the duplicated code it replaces, retain the local copies.
12. All edits use explicit file-list staging. Existing worktree changes,
    especially the active configuration simplification and coalescing work,
    are not part of this plan unless a task below names them.

## Rollout, Rollback, and One-Way Doors

Each slice must be independently revertible. Land dead test-code
deletions, test-plumbing consolidation, core-package simplifications, and Redis
simplifications as separate commits. No slice changes persistent data, so its
rollback is a targeted commit revert.

## Investigation Disposition Matrix

| Finding | Disposition | Owning slice |
|---------|-------------|--------------|
| `timestamp_test_utils.py` has no callers | Delete the module and stale exports | 1 |
| Nine unused path/platform helpers | Delete them | 1 |
| `create_dangerous_path` has five call sites across two test modules | Keep the shared helper | 1 |
| Old and instrumented SIGINT scripts are unused | Delete both; retain improved probe and update suppression registry | 1 |
| Four unused `DatabaseErrorInjector` methods | Delete them | 1 |
| Three unused timing helpers | Delete them; retain `wait_for_condition` | 1 |
| Two unused `WatcherTestBase` methods | Delete them | 1 |
| Broad helper package re-exports | Reduce to live package imports; keep `__init__.py` | 1 |
| `_test_backend_name` duplicates `active_backend` | Use `active_backend` | 2 |
| PG and Redis scope-name helpers match | No action: semantic wrappers cost little and prevent backend conditionals | No-action register |
| Four config writers and two cleanup scanners resemble each other | No action: backend encoding, secret handling, and cleanup policy differ | No-action register |
| `managed_subprocess` has two import routes | Update the two conftest importers to the owner module; remove re-export | 2 |
| Local subprocess CLI helpers partially repeat environment setup | No action: pipe, binary-stdin, collection, and backend-isolation policies differ | No-action register |
| PostgreSQL runner shutdown fallback is unreachable | Call `shutdown()` directly | 2 |
| `_require_test_dsn` follows a module skip | No action: it retains type narrowing and direct diagnostics | No-action register |
| Two private `_backend_plugins` helpers have no callers | Delete them and private `__all__` entries | 3 |
| One-entry built-in backend registry | Import SQLite directly; retain `_paths` wrappers as live seams | 3 |
| Three stable-exception formatters | No action: the local helpers are smaller and safer than a new cross-package owner | No-action register |
| Two SQLite normalization helpers | Consolidate implementation, retain public ownership-boundary normalization | 3 |
| `_SharedWriteLock` exists only for weak references | Replace wrapper values with plain locks; keep registry/fork behavior | 4 |
| Hand-built Redis metadata keys | Replace all five expressions in `plugin.py` with `RedisKeys.meta`; leave the cycle-sensitive validator owner unchanged | 4 |
| Redis pool options are recomputed | Return normalized backend options and parsed pool options together on plugin paths; retain direct-runner fallback | 4 |
| Session factory builder, `_session_key`, alias, `_SessionSpec` | Remove only `config_snapshot = config`; retain test seam and distinct key/spec roles absent stronger proof | 5 |
| `_BorrowedRunner` looks like forwarding boilerplate | No action: it is an ownership firewall required by [SB-API-11] | No-action register |
| Two benchmark harnesses share Docker setup | No action: workload and lifecycle policies differ | No-action register |

## Worklist

### 0. Freeze the evidence and ownership map

- [x] Record `git rev-parse HEAD`, `git status --short`, and per-file diffs for
  every intended edit before changing it. Label pre-existing changes and never
  absorb them by broad staging.
- [x] Re-run symbol, import, string, filename, and dynamic-import searches in
  SimpleBroker. Search both the working tree and `git show HEAD:<path>` because
  concurrent work may have deleted and renamed surfaces.
- [x] Record the three comprehension answers above in the execution log.
- Stop and re-plan if a proposed dead symbol has any production caller,
  plugin-registration reference, dynamic import, manifest entry, docs-owned
  command, or package/extension caller.
- Done signal: an updated disposition matrix with exact caller counts and a
  file delta table that separates pre-existing changes from this plan.

### 1. Delete only proven-dead test helpers

- [x] Delete `tests/helper_scripts/timestamp_test_utils.py`.
- [x] In `tests/helper_scripts/functions.py`, delete the nine zero-use helpers.
  Keep `create_dangerous_path`; it has five call sites across
  `tests/test_cli_validation.py` and `tests/test_constants.py`.
- [x] Delete
  `tests/helper_scripts/watcher_sigint_script.py` and
  `tests/helper_scripts/watcher_sigint_script_instrumented.py`. Keep
  `tests/helper_scripts/watcher_sigint_script_improved.py` and its transition
  tests.
- [x] Delete the four zero-use methods from
  `tests/helper_scripts/database_errors.py`, the three zero-use functions from
  `tests/helper_scripts/timing.py`, and the two zero-use methods from
  `tests/helper_scripts/watcher_base.py`.
- [x] Reduce `tests/helper_scripts/__init__.py` to live imports and exports.
  Keep the file and preserve imports used by `tests/test_cli_validation.py` and
  `tests/test_watcher_sigint_probe_transitions.py`.
- [x] Remove the deleted SIGINT script suppressions from
  `docs/implementation/10-ruff-suppression-registry.md` and regenerate or run
  the repository's suppression checks rather than hand-waving stale entries.
- Verify: targeted importing tests, `uv run pytest tests/test_cli_validation.py
  tests/test_runner_error_handling.py tests/test_watcher_sigint_probe_transitions.py
  tests/test_watcher_transition_tables.py`, Ruff, mypy, and a repository-wide
  zero-reference search.
- Done signal: all deleted names have zero remaining references, the improved
  SIGINT state-machine probe still fires, and no package import regresses.

### 2. Consolidate test plumbing without erasing backend semantics

- [x] Replace `tests/conftest.py::_test_backend_name` uses with
  `tests.helper_scripts.broker_factory.active_backend`; update
  `tests/test_peek_generator_lifecycle.py` to use the same owner.
- [x] Change `tests/test_cli_watch.py` and
  `tests/test_move_after_exclusion.py` to import `managed_subprocess` directly
  from `tests.helper_scripts.managed_subprocess`; remove the conftest re-export.
- [x] In the PostgreSQL worker fixture, replace the impossible
  `hasattr(..., "shutdown")` branch with direct `shutdown()`.
- Verify: `uv run pytest tests/test_cli_watch.py
  tests/test_move_after_exclusion.py tests/test_peek_generator_lifecycle.py
  tests/test_managed_subprocess_transitions.py`; then the SQLite, PostgreSQL,
  and Redis fixture smoke commands in the final gate.
- Done signal: one backend lookup owner and one subprocess-helper import route;
  any retained duplication has a stated semantic reason.

### 3. Simplify core package internals while preserving public boundaries

- [x] Delete `validate_backend_target` and `target_parent_directory` from
  `simplebroker/_backend_plugins.py`, including private exports and tests that
  only assert their existence.
- [x] Replace the one-entry `BuiltinBackend` registry in
  `simplebroker/_backends/__init__.py` with direct SQLite ownership. Keep
  `simplebroker/_paths.py` wrappers and their tests because they are live
  indirection/test seams.
- [x] Move the duplicate SQLite normalization algorithm to one private owner.
  Keep calls at `Queue` and `DBConnection` ownership boundaries and keep
  session/waiter identity stable.
- Verify: `uv run pytest tests/test_backend_plugin_resolution.py
  tests/test_db_connection_lifecycle.py tests/test_process_broker_session.py
  tests/test_queue_config_defaults.py tests/test_watcher_error_handler_contract.py
  tests/test_custom_runner_integration.py`; add/retain same-target tests that
  construct through both public boundaries and change cwd between them.
- Stop and re-plan if direct SQLite import creates an import cycle, if a helper
  must become public, or if a same-target case creates two sessions.
- Done signal: fewer internal owners with unchanged public imports, target
  identity, and runner ownership.

### 4. Simplify Redis internals

- [x] Replace `_SharedWriteLock` instances with plain `threading.Lock` objects
  in the existing `WeakValueDictionary`; retain registry locking and fork reset.
- [x] Replace all five production metadata-key constructions in
  `extensions/simplebroker_redis/simplebroker_redis/plugin.py` with
  `RedisKeys(namespace).meta` or the already-available `RedisKeys` instance.
  Do not mechanically change test strings that intentionally probe hostile or
  child namespaces.
- [x] Change plugin normalization so the normalized backend options and parsed
  `RedisPoolOptions` are produced together and reused. Preserve
  `RedisRunner(..., pool_options=None)` as a supported fallback that snapshots
  its own resolved configuration.
- Verify: `uv run pytest extensions/simplebroker_redis/tests/test_redis_pool.py
  extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py
  extensions/simplebroker_redis/tests/test_redis_plugin_validation_paths.py
  extensions/simplebroker_redis/tests/test_redis_core_behaviors.py`; run the
  managed real-Valkey gate in the final suite.
- Stop and re-plan if locks become strongly retained, direct runner behavior
  changes, or plugin and direct construction normalize different values.
- Done signal: lock lifetime/fork tests pass, no manual metadata-key
  construction remains in `plugin.py`, and each `_normalize_backend_options`
  invocation parses pool options once.

### 5. Apply only the proven session cleanup

- [x] Remove the no-op `config_snapshot = config` alias from
  `simplebroker/_broker_session.py` and use `config` directly.
- Verify: `uv run pytest tests/test_process_broker_session.py
  tests/test_connection_transition_tables.py
  tests/test_cross_thread_finalization_poisoning.py tests/test_fork_safety.py`.
- Done signal: the no-op alias is gone and no identity/construction abstraction
  is removed merely because fields overlap.

### 6. Reconcile docs, run final gates, and close

- [x] Update `docs/implementation/02-repository-map.md` only if an owner or
  file path changes. Update implementation docs 06/07 only if session or Redis
  ownership wording becomes stale. Update the Ruff registry for deleted files.
- [x] Re-run caller searches in SimpleBroker and compare against the final
  disposition matrix.
- [x] Run an independent review after slices 1–2, after slices 3–5, and on the
  final integrated diff. Address or explicitly disposition every finding.
- [x] Run the final verification matrix below. Record exact commands, commit
  identifiers, counts, skips, and residual risks.
- [ ] Land only by explicit file-list staging. Close this plan's index row to
  `completed` in the same change as the completion claim. Do not close the
  separate config plan unless its own gates pass.

## Testing Plan

Deletion does not naturally produce a red test. The substitute proof is a
four-part evidence chain: repository-wide caller search including dynamic names
and manifests; import/collection proof; targeted behavioral tests for the
surviving owner; and the full affected suite. For behavior-preserving
consolidation, first demonstrate that an existing focused test fails when the
surviving owner is temporarily broken, then restore it and make the refactor.

Do not mock the runner in the borrowed-runner proof, filesystem target
normalization, process-group SIGINT behavior, Redis locks, PostgreSQL schemas,
or Redis namespaces. Mocks are acceptable only for
injecting a failure after the real local ownership seam is exercised.

```text
dead-symbol search + import collection
                  |
                  v
targeted SQLite lifecycle / helper / SIGINT tests
                  |
          +-------+-------+
          |               |
          v               v
  real PostgreSQL     real Valkey
  fixture/lifecycle   lock/pool/key paths
          |               |
          +-------+-------+
                  |
                  v
       full SimpleBroker quality and docs gates
```

## Verification and Gates

Per-slice commands are listed in the worklist. Before completion run, from the
current implementation identifier:

```bash
uv run ruff check .
uv run ruff format --check simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages \
  --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs \
  $(find tests -type f -name '*.py' -not -path '*/__pycache__/*' \
    -not -path 'tests/typecheck_fixtures/*' | sort)
uv run mypy extensions/simplebroker_pg/simplebroker_pg \
  $(find extensions/simplebroker_pg/tests -name '*.py' -print) \
  --config-file pyproject.toml
uv run mypy extensions/simplebroker_redis/simplebroker_redis \
  $(find extensions/simplebroker_redis/tests -name '*.py' -print) \
  --config-file pyproject.toml
uv run pytest -m "not benchmark"
uv run --frozen --no-sync ./bin/pytest-pg --max-worker-restart=0
uv run --frozen --no-sync ./bin/pytest-redis --max-worker-restart=0
uv run --frozen --no-sync ./bin/packaging-smoke --python 3.11
uv run python bin/ruff_suppression_index.py --check
python3 bin/check-dom15-fixtures
bin/check-plan-context
```

A skipped real-service suite is not green evidence; start its managed service
or record a blocker.

Post-change success is positive local evidence: direct `RedisRunner`
construction still works, caller-owned runner teardown still leaves the
injected runner usable, and the package/extension tests pass without stale
imports or manifest entries.

## Independent Review Loop

Use a reviewer separate from the implementer. Give the reviewer this plan, the
baseline spec, the current diff, the disposition matrix, and verification
evidence. Ask for `PASS` or `BLOCKED` based on whether the plan can be
implemented confidently and whether it would degrade lifecycle or robustness.
Require the reviewer to existence-check every named path, symbol, and command
first. Record each finding as accepted, rejected with reason, or out of scope
with reason. A reviewer who cannot implement the plan confidently blocks the
next slice.

## No-Action Register

| Candidate | Why no action is planned | Reconsider when |
|-----------|--------------------------|-----------------|
| Delete `_BorrowedRunner` | It masks destructive lifecycle verbs on caller-owned injected runners; `_external_runner` does not protect lower-layer teardown | Ownership is redesigned under a revised [SB-API-11] contract with equivalent real-runner proof |
| Resolve config only in `cli.main` | The CLI is only one of several supported public construction paths | All other entry points are removed or contractually require a `Config` in a separately reviewed public change |
| Remove session-side target normalization | It protects identity when callers enter through different public owners or change cwd | A single immutable target object is created at every public boundary and same-target tests prove equivalence |
| Require Redis `pool_options` | Direct runner construction currently derives them | A separately versioned public-contract change migrates all callers |
| Merge benchmark harnesses | Their access modes, workloads, timing rules, and reports differ | One product contract replaces both and docs/tests are revised together |
| Share PG exception helper through a core private import | It creates an undeclared cross-package version dependency | The helper becomes a versioned extension surface with coordinated minimum versions |
| Delete `_require_test_dsn` solely because of `pytestmark` | It retains type narrowing and a direct diagnostic at each use | A typed fixture makes the impossible state unrepresentable with clearer failures |
| Make this cleanup depend on Weft or Taut | The sibling caller audit is relevant to the separate config redesign, not to these internal deletions | This plan begins changing a public surface consumed by a sibling |
| Merge PG/Redis scope-name helpers | The semantic wrappers are three lines each and avoid backend conditionals | A third backend needs the exact same naming policy |
| Merge backend config writers/cleanup scanners | Encoding, password handling, quoting, and cleanup failure policy differ | A backend-neutral core can be smaller without switches or weakened cleanup |
| Merge stable-exception formatters | The helper is four lines; a shared owner would create coupling, including across packages | A versioned public extension helper already exists for another reason |
| Inline `_session_key`, the session factory builder, or merge `_SessionKey`/`_SessionSpec` | They are identity, injection, and live-construction seams, not mere field duplication | Production and lifecycle tests no longer use those seams and a focused design proves one role |
| Consolidate local subprocess CLI helpers | Their pipe closure, binary stdin, collection, ambient backend, and output policies differ | Two helpers acquire the same full policy, not just the same `PYTHONPATH` lines |

## Execution Log

2026-09-12, implemented in the working tree on `main` at `fdf5ceb` (spec
baseline `f213b12` unchanged for the governing specs).

- Worktree state: the class-5 config simplification is still uncommitted and
  modifies most slice 3–5 files (`_backend_plugins.py`, `_backends/__init__.py`,
  `_broker_session.py`, `sbqueue.py`, `_runner.py`, Redis `core/plugin/pool/runner.py`,
  `tests/conftest.py`, `tests/test_backend_plugin_resolution.py`). File-list
  staging cannot separate those hunks, so this plan's changes are left
  uncommitted and must land with or after the config plan.
- Comprehension gate: answered as expected for `_BorrowedRunner`
  (destructive-verb firewall at lower layers vs. `_external_runner` ownership
  flag), omitted-config resolution (every public construction boundary), and
  the benchmark harnesses (distinct workload products).
- Caller evidence (HEAD and worktree, core/extensions/tests/examples/fuzz/bin/docs,
  plus `../weft` and `../taut`): zero callers for every deleted name. The four
  `DatabaseErrorInjector` methods are `database_locked_async`,
  `create_corrupted_database`, `busy_database`, `trigger_wal_mode_error`.
  Package-level `tests.helper_scripts` importers (preserved): `create_dangerous_path`
  (2 modules), `drive_until` (6 incl. both extension state-machine tests and
  `examples/tests/test_reference_reactor.py`), `scale_timeout_for_ci`,
  `wait_for_condition`, `WATCHER_SIGINT_SCRIPT_IMPROVED`, submodule `timing`.
- Slice notes: conftest also dropped the zero-importer `ManagedProcess` and
  `run_subprocess` re-exports. `_backends` now re-exports only the `sqlite`
  module; `_runner`/`_paths` keep module-attribute dispatch so
  `tests/test_runner_validation.py` monkeypatches still apply; the
  registry-only test `test_unknown_builtin_backend_fails_with_the_requested_name`
  was deleted. The shared normalizer owner is
  `simplebroker/_targets.py::normalize_sqlite_target`; `DBConnection`'s
  warning variant in `db.py` is intentionally untouched. Ruff registry:
  `[RUFF-SUP-008]` 13→9 and global `BLE001` 116→112.
- Incident: `functions.py` was trimmed ~40 s before `helper_scripts/__init__.py`
  was rewritten; every `conftest.py` import loads that `__init__`, so a
  concurrent session's collection failed in that window. Resolved by the
  `__init__` rewrite; full collection over `tests`, both extension suites,
  and `examples/tests` is clean.
- 2026-09-13 final runtime gates at `fdf5ceb` plus the working-tree delta:
  SQLite/core `3560 passed, 19 skipped`; PostgreSQL shared `1618 passed,
  12 skipped` and extension `319 passed, 7 skipped`; Redis shared `1610
  passed, 20 skipped` and extension `316 passed, 1 skipped`. The repeated Weft
  compatibility skip belongs to the separate config plan and is explicitly
  out of scope by owner direction.
- Static and artifact gates: Ruff check passed; Ruff format passed over 341
  files after mechanically formatting `tests/test_cli_broken_pipe.py`;
  production mypy passed 66 files; core-test mypy passed 214 files; PostgreSQL
  extension mypy passed 36 files; Redis extension mypy passed 29 files;
  Python 3.11 wheel/sdist packaging smoke passed for core and both extensions;
  Ruff suppression index, DOM-15 fixtures, plan context, and `git diff --check`
  passed.
- Verification correction: the first core-test mypy command included the two
  intentional negative fixtures and reported their expected five errors. The
  plan now matches CI by excluding `tests/typecheck_fixtures/*`; the corrected
  command passed all 214 included files.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| — | Slice 5 removes the `config_snapshot = config` alias | Already removed by the concurrent config-simplification work before this slice ran | The alias was a residue of that uncommitted diff; nothing left to do | None |

## Review Log

| Date | Reviewer | Verdict/findings | Disposition |
|------|----------|------------------|-------------|
| 2026-09-12 | Independent same-family plan reviewer | `BLOCKED`: unsafe downstream publication alternative; downstream work underspecified; `create_dangerous_path` location error; Redis done signals too broad; speculative tasks; final gates not exact | Downstream work and gating removed by owner scope direction; the config risk remains explicitly out of scope. Corrected the helper to five call sites across two modules (reviewer's claimed six invocations did not reproduce: four in `test_cli_validation.py`, one in `test_constants.py`). Narrowed Redis signals, moved speculative candidates to the no-action register, deleted the benchmark slice, and made core/extension gates exact. Re-review required. |
| 2026-09-12 | Same reviewer, scoped round 2 | `PASS`: verified F3–F6 fixes and confirmed no Weft/Taut dependency remains in the worklist, tests, gates, or done signals | Accepted. No new defect found. |
| 2026-09-12 | Independent correctness reviewer (implementation, slices 1–5) | `PASS`, no findings: zero remaining references to removed names; normalizer bodies identical to HEAD with all call sites retained; lock registry weak/guard/fork semantics intact; pool-option validation order unchanged; residual risk is only the pending real-service gates | Accepted. Same-target chdir tests (`test_process_broker_session`, `test_queue_config_defaults`) were already in the targeted green run; real PostgreSQL/Valkey gates run before landing. |
| 2026-09-13 | Independent final implementation reviewer | `PASS`, no findings: all load-bearing seams retained; deletions have zero callers; DBConnection normalization and Redis lock/pool proofs are meaningful and red-capable; config rewrite overlap introduces no cleanup-specific defect | Accepted. Full SQLite, PostgreSQL, Redis, static, docs, and packaging gates passed. |

## Out of Scope

- Redesigning the configuration model or repeating work owned by
  `2026-09-12-config-object-simplification-plan.md`.
- Changing Weft or Taut, gating this cleanup on them, or deciding the separate
  configuration plan's release policy. The audit did find live sibling callers
  of `ResolvedConfig` and `resolve_isolated_config`; that risk remains with the
  configuration plan and is not a completion condition here.
- Changing public CLI/Python contracts, backend API versions, persistence
  layouts, timestamps, delivery semantics, or queue behavior.
- Replacing test helpers merely because they are short.
- Introducing a general test-framework layer, backend abstraction framework,
  or shared benchmark product.
- Reformatting or cleaning unrelated files in the existing dirty worktrees.

## Fresh-Eyes Closeout Checklist

- [x] Every named file, symbol, test, spec code, and command exists at the
  implementation baseline.
- [x] Each deletion has a recorded caller search and surviving-owner proof.
- [x] Each retained candidate has a reason, not just inertia.
- [x] No task weakens caller-owned lifecycle, target identity, fork safety,
  cleanup exhaustion, or direct construction.
- [x] Review findings, deviations, and residual risks are fully dispositioned.
- [ ] The index row and final evidence reflect completed work, not transient
  worktree state.
