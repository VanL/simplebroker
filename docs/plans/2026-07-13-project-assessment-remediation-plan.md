# Project Assessment Remediation Plan

**Date:** 2026-07-13  
**Status:** Implemented and verified  
**Scope:** Confirmed defects, low-risk cleanup, coverage accuracy, and one evidence-gated investigation  
**Delivery rule:** Keep the units distinct in the diff and verification record. Do not require long-lived branches. The implementation may stay in one short-lived worktree and be reviewed as one coherent change.

## Purpose

Address the confirmed assessment findings without treating unproved suggestions as facts. The plan separates work that has a known safe fix from work that still needs evidence or an API design.

The released backend mix is SQLite, PostgreSQL, and Redis/Valkey. SQLite and PostgreSQL use SQL runners. Redis/Valkey uses its own core. Any lifecycle claim must name the backend or prove that it applies to all three.

## Delivery Units

| Unit | Contents | Intended outcome |
|---|---|---|
| A | SQLite Mock-path cleanup defect | Hotfix with a reproducing regression test |
| B | Metrics init, dead fallbacks, watcher test waits, safe docs | Low-risk cleanup |
| C | Branch coverage and SQLite schema tests | Accurate coverage signal and targeted branch tests |
| D | Cross-thread generator finalization | Investigation and follow-up design only |

Units A, B, and C are implementation work. Unit D is a bounded investigation. Its success condition is reliable evidence and a defensible next decision, not a speculative production patch.

## Finding Disposition

| ID | Finding | Disposition |
|---|---|---|
| F1 | A database path containing `Mock` can be deleted by `SQLiteRunner.cleanup_marker_files()` | Fix in Unit A |
| F2 | A transactional batch generator finalized on a foreign thread can leave the owner transaction and instance lock live | Investigate in Unit D; no production fix in this plan |
| F3 | Optional `SQLRunner` hooks are probed with `getattr`/`hasattr` | No change. This is an intentional extension contract, not a confirmed defect |
| F4 | Timestamp-conflict metrics are initialized lazily in the write path | Fix in Unit B |
| F5 | Python versions below 3.11 still have dead fallback code | Fix in Unit B |
| F6 | The configured coverage result does not measure branches | Fix measurement in Unit C; keep the existing threshold |
| F7 | SQLite schema migration branches have weak coverage | Add focused tests and remove only code proved unreachable in Unit C |
| F8 | Three watcher tests use fixed sleeps before assertions | Replace with bounded predicate waits in Unit B |
| F9 | A broad Windows-only test retry catches every exception and can hide a post-commit failure | Explicitly out of scope; this plan makes no change |
| F10 | Release example and lifecycle/exception docs are stale or incomplete | Apply only the verified documentation changes in Unit B |

## Decisions

1. Remove both `Mock`-substring deletion branches from `SQLiteRunner`. Phase-lock sidecar cleanup must depend on sidecar ownership and setup state, not the spelling of the database path.

2. Reproduce F1 through the public runner path. A normal `BrokerDB` construction creates the file before the runner records ownership, so it does not expose the defect. The regression test must pre-open a public `SQLiteRunner`, inject it into `BrokerCore`, write data, close it, and prove the database still exists and reopens.

3. Do not implement the proposed cross-thread poison flag. It cannot roll back the exact owner thread's transaction, cannot release a Python `RLock` from a foreign thread, does not wake callers already blocked on that lock, and cannot protect another broker instance from the leaked database write lock.

4. Do not add `RunnerCapabilities`. `SQLiteRunner` does not implement all proposed hooks, the optional probe helpers already exist in `simplebroker/_runner.py`, and resolving hooks once would change the current dynamic lookup behavior. `SQLRunner` remains the public extension contract exported through `simplebroker.ext`.

5. Keep `[tool.coverage.report].fail_under = 85`. Enable branch measurement, take a diagnostic baseline without enforcing the gate, add only the already justified schema tests, and rerun the real gate. If the final result still fails 85, stop Unit C without merging it and report the measured result. Do not lower the threshold in this plan.

6. Use `tests/test_sqlite_schema.py` for schema work. Prefer real SQLite state. A controlled fault seam is allowed only after proving that the production exception path is reachable but cannot be triggered deterministically. Do not mock the whole runner or connection.

7. Generator docs must describe the current contract only: create, consume, exhaust, and close a transactional generator on the same thread. Do not claim that foreign-thread finalization rolls back safely or poisons all backends.

8. No public API, dependency, or lockfile change is planned. If the investigation shows that one is required, stop and write a focused follow-up plan.

## Not in Scope

- A production repair for cross-thread generator finalization.
- A runner capability dataclass, required optional hooks, or a third-party runner migration.
- A generic poison state for SQLite, PostgreSQL, and Redis/Valkey.
- Lowering the coverage threshold.
- The Windows-only concurrency retry finding (F9), including investigation, test changes, or production changes.
- New queue semantics, generator APIs, backend SDKs, or exception hierarchy changes.
- Broad formatting, typing, or documentation cleanup outside the named files.

## Existing Code to Reuse

| Need | Existing code |
|---|---|
| Public SQLite runner reproduction | `simplebroker._runner.SQLiteRunner` and `simplebroker.db.BrokerCore` |
| Optional runner lifecycle probes | `close_owned_runner()`, `lease_runner_thread_connection()`, and `release_runner_thread_connection()` in `simplebroker/_runner.py` |
| Watcher polling | `tests/helper_scripts/timing.py` |
| SQLite schema tests | `tests/test_sqlite_schema.py` |
| Branch coverage combine workflow | Existing coverage configuration and CI combine helper |
| Released backend suites | Root tests plus `./bin/pytest-pg --fast` and `./bin/pytest-redis --fast` |

## File Boundaries

### Unit A

- `simplebroker/_runner.py`
- `tests/test_runner_error_handling.py`

### Unit B

- `simplebroker/db.py`
- `simplebroker/helpers.py`
- `simplebroker/_exceptions.py`
- `tests/test_timestamp_resilience.py`
- `tests/test_watcher.py`
- `README.md`

### Unit C

- `pyproject.toml`
- `tests/test_sqlite_schema.py`
- `simplebroker/_backends/sqlite/schema.py`, but only if Unit C proves a branch unreachable and deletes it

### Unit D

- Investigation tests or scripts under `tests/`, isolated in a subprocess
- This plan's Investigation Results section
- A new focused plan only if a viable design is found

No production file may change in Unit D.

## Test Discipline

- For a confirmed bug, add a test that fails for the confirmed reason before changing production code.
- Use public behavior for the final assertion. Internal state may be recorded only as extra diagnostic evidence.
- Never leave a timed-out worker thread alive inside pytest. Any test that can wedge a lock or transaction must run in a child process that the parent can terminate.
- A timeout is a safety bound, not a synchronization method. Use events, barriers, and bounded predicate waits for coordination.
- Keep failure output diagnostic: exception type, exception text, thread/process state, transaction state when observable, and whether the message committed.
- Run the smallest relevant test first, then the unit's full gate.

## Task 0: Baseline and Worktree Setup

Run from the repository root before implementation:

```bash
git status --short
uv sync --locked --extra dev --extra pg --extra redis
uv run pytest -n0 tests/test_runner_error_handling.py tests/test_sqlite_schema.py tests/test_concurrency.py -q
uv run pytest -n0 -q
```

Requirements:

- Start from current `main` in a clean or understood worktree. Separate branches are optional, not a coverage strategy or a delivery requirement.
- Broad coverage comes from running all released backend suites and the CI-equivalent coverage workflow, not from keeping branches alive.
- Do not edit `uv.lock`.
- Record any baseline failure before changing code. Do not attribute a pre-existing failure to this work.

## Unit A: Fix Mock-Path Database Deletion

**Priority:** P0  
**Expected production change:** Small and local

### A1. Add the real regression test

In `tests/test_runner_error_handling.py`, add one focused regression for the mistaken cleanup heuristic. Create a database path containing `Mock`, use a real public `SQLiteRunner` to create and populate it, close the runner, call public `cleanup_marker_files()`, and prove that the database remains and its data reopens intact.

The test must fail on the old code because the database file is deleted. It need not prove a new lifecycle rule or parameterize unrelated sidecar behavior. The defect is the name-based deletion code itself; removing that code is the fix.

### A2. Remove name-based database deletion

In `simplebroker/_runner.py`:

- Delete the `Mock`-substring branches that mark or unlink the database itself.
- Remove `_created_db` if it has no remaining use.
- Preserve cleanup of phase-lock sidecars according to the existing ownership/setup rules.
- Do not add another filename heuristic.

### A3. Preserve the existing sidecar regression

Keep the existing test that current setup sidecars are preserved. Delete the old test whose expected result was deletion of a mock-named database. Do not add a second mock-name sidecar test merely to disprove behavior that no longer exists.

### A4. Verify Unit A

```bash
uv run pytest -n0 tests/test_runner_error_handling.py -q
uv run pytest -n0 tests/test_sqlite_lifecycle.py tests/test_runner_lifecycle.py -q
uv run pytest -n0 -q
```

Acceptance:

- Closing a pre-opened public `SQLiteRunner` never deletes its database because `Mock` appears in the path.
- Reopened data is intact.
- Phase-lock sidecar behavior remains covered independently of the deleted database-name heuristic.
- No PostgreSQL or Redis/Valkey behavior changes.

## Unit B: Low-Risk Cleanup and Verified Docs

**Priority:** P2  
**Expected production change:** Mechanical, except for documentation wording

### B1. Initialize timestamp-conflict metrics eagerly

In `BrokerCore.__init__`, initialize `_ts_conflict_count` and `_ts_resync_count`, which are now created lazily in `write()`. Remove the corresponding `hasattr` initialization from the hot path. Change `get_conflict_metrics()` to read the initialized fields directly instead of using `getattr` defaults. Keep counter updates under the existing lock and preserve metric names and values.

Add a `pytest.mark.sqlite_only` structural test in `tests/test_timestamp_resilience.py` that creates a core and asserts both fields exist and equal zero before the first write. Then read the public metrics, perform a normal write, and prove the counters retain their current semantics. The direct field assertion is intentional because the current public getter's fallback otherwise hides lazy initialization. Do not add private SQL-core assertions to an existing `shared` test; Redis/Valkey has its own core even though it currently exposes counters with the same names.

### B2. Remove dead pre-3.11 helper fallbacks

In `simplebroker/helpers.py`, remove the `hasattr(Path, "readlink")` fallback in `_resolve_symlinks_safely()` and the `hasattr(Path, "is_relative_to")` fallback in `_validate_path_containment()`. Both APIs exist on Python 3.11. Do not broaden this task to other compatibility code, and do not change return values or exception behavior.

### B3. Replace the three identified watcher sleeps

In `tests/test_watcher.py`, change only these three tests:

- `test_error_handler_returns_false`: remove the fixed sleep, use the existing bounded `thread.join()`, and assert the thread stopped before inspecting the remaining message.
- `test_polling_with_data_version`: remove both the startup sleep and the processing sleep. The queued message is durable, so the watcher need not be running before the write. Use `wait_for_condition()` for `messages_received == ["test_message"]`.
- `test_handler_exception_no_error_handler`: replace the sleep with `wait_for_condition()` on the expected log text before stopping the watcher.

Each replacement must wait for the exact state used by the assertion, use a bounded timeout, and report the last observed state on failure. Do not replace sleeps that intentionally simulate work or establish timestamp spacing.

Run the affected tests repeatedly to expose races:

```bash
for i in {1..10}; do uv run pytest -n0 tests/test_watcher.py -q || exit 1; done
```

### B4. Correct documentation without promising an unbuilt fix

In `README.md` and `simplebroker/_exceptions.py`:

- In README `Releases`, replace the stale literal package versions in the release commands with `X.Y.Z`, followed by one sentence telling the reader to substitute the next unpublished version. Do not use angle-bracket shell placeholders because a copied command would be parsed as redirection.
- In the README watcher error-callback contract and the `StopException` class docstring, document `StopException` as control flow that should be re-raised rather than swallowed by a broad `except Exception` in user callbacks.
- In README `Delivery guarantees`, state that a transactional generator must be created, consumed, exhausted, and explicitly closed on the same thread. Recommend `contextlib.closing` when early exit is possible.
- State that foreign-thread finalization is unsupported and may leave the SQL-backed instance or transaction unusable.
- Do not claim a poison flag, cross-thread rollback, or common Redis/Valkey transaction behavior.
- Do not document `RunnerCapabilities`.

### B5. Verify Unit B

```bash
uv run pytest -n0 tests/test_timestamp_resilience.py tests/test_watcher.py tests/test_helpers_coverage.py -q
uv run pytest -n0 -q
PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast
PYTEST_ADDOPTS= uv run ./bin/pytest-redis --fast
```

Acceptance:

- Metrics exist before the first write and retain their values and names.
- The two named pre-3.11 compatibility fallbacks are removed without changing the covered helper behavior.
- The three watcher assertions use bounded state waits and pass ten consecutive targeted runs.
- Docs describe current behavior and distinguish SQL-backed lifecycle constraints from Redis/Valkey.

## Unit C: Measure Branches and Cover SQLite Schema Logic

**Priority:** P2  
**Expected production change:** Tests and coverage config; possible deletion of code proved unreachable

### C1. Enable branch measurement without changing the gate

Set `branch = true` in `[tool.coverage.run]`. Leave `fail_under = 85` unchanged.

Run the same coverage sequence used by CI, including all released backend suites and the existing append/combine flow:

```bash
uv run --frozen --no-sync coverage erase
export COVERAGE_PROCESS_START="$PWD/pyproject.toml"
export COVERAGE_FILE="$PWD/.coverage"
uv run --frozen --no-sync pytest --cov=simplebroker --cov=extensions/simplebroker_pg/simplebroker_pg --cov=extensions/simplebroker_redis/simplebroker_redis --cov-append --cov-report= --cov-fail-under=0 -m "not benchmark"
PYTEST_ADDOPTS= uv run --frozen --no-sync ./bin/pytest-pg --fast --cov=simplebroker --cov=extensions/simplebroker_pg/simplebroker_pg --cov=extensions/simplebroker_redis/simplebroker_redis --cov-append --cov-report= --cov-fail-under=0
PYTEST_ADDOPTS= uv run --frozen --no-sync ./bin/pytest-redis --fast --cov=simplebroker --cov=extensions/simplebroker_pg/simplebroker_pg --cov=extensions/simplebroker_redis/simplebroker_redis --cov-append --cov-report= --cov-fail-under=0
uv run --frozen --no-sync python .github/scripts/combine_coverage.py
uv run --frozen --no-sync coverage report --show-missing --fail-under=0
uv run --frozen --no-sync coverage json --pretty -o /tmp/simplebroker-coverage.json --fail-under=0
```

This first run is diagnostic. Record the aggregate branch-inclusive percentage and the `num_branches`, `covered_branches`, and `missing_branches` data for `simplebroker/_backends/sqlite/schema.py` from `/tmp/simplebroker-coverage.json`.

Continue only with the schema work already justified below. Do not add unrelated or low-value tests merely to recover the aggregate number.

### C2. Classify every uncovered schema branch

For each uncovered branch in `simplebroker/_backends/sqlite/schema.py`, classify it as one of:

1. Reachable with real database pre-state.
2. Reachable only under a real race or backend error.
3. Unreachable because setup serialization or idempotent SQL prevents it.
4. Defensive behavior whose reachability is still unknown.

Record the classification in the test name or a short nearby comment. Do not infer reachability from the presence of an `except` block.

### C3. Test reachable behavior

Add tests to `tests/test_sqlite_schema.py` using real SQLite files and `SQLiteRunner` for cases such as:

- Fresh schema setup.
- Existing metadata and messages tables.
- Claimed column present versus absent.
- Existing versus missing indexes.
- Repeated setup and migration idempotence.
- Rollback and exception propagation for a reachable failure.

Precreating a final schema object is not proof that an error handler runs. For example, `CREATE INDEX IF NOT EXISTS` can bypass an `already exists` exception path. Assert the path actually taken, not just the final schema.

### C4. Handle hard-to-trigger and unreachable branches

- If a path is reachable but nondeterministic, first prefer a real subprocess or concurrency test.
- If deterministic injection is necessary, wrap a real `SQLiteRunner` and fail one named operation. Assert rollback, propagated exception type, and final database state. Do not use a bare mock connection.
- If a path is proved unreachable under the supported SQLite behavior and phase-lock serialization, delete the dead handler and its misleading comment. Any such deletion makes `simplebroker/_backends/sqlite/schema.py` an expected production diff.
- If reachability remains ambiguous, leave the code and record the missing evidence. Do not invent a test that cannot exercise it.

### C5. Verify Unit C

```bash
uv run pytest -n0 tests/test_sqlite_schema.py tests/test_sqlite_setup_contention.py -q
uv run pytest -n0 -q
PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast
PYTEST_ADDOPTS= uv run ./bin/pytest-redis --fast
```

Then run the repository's complete CI-equivalent coverage sequence again, this time using the unmodified final `coverage report --show-missing` command so `fail_under = 85` is enforced. Inspect both the aggregate branch-inclusive result and the per-file JSON data.

If the final result is below 85, stop without merging Unit C. Record the result and open a separate threshold decision. Do not lower the threshold in this plan.

Acceptance:

- Branch measurement is enabled.
- The 85 gate is unchanged and passes, or Unit C stops with the measured shortfall and no threshold change.
- Each new schema test proves it reached the intended branch.
- Dead schema handlers are removed only with written reachability proof.
- Redis/Valkey remains included in the released-backend coverage run even though SQLite schema tests do not apply to it.

## Unit D: Investigate Cross-Thread Generator Finalization

**Priority:** P1 investigation  
**Expected production change:** None

### D1. Build a process-isolated reproducer

The parent test starts a child process with `multiprocessing.get_context("spawn")`, so it cannot inherit a lock or connection from pytest. Inside the child:

1. An owner thread creates and advances a transactional batch generator until a transaction is open and `BrokerCore._lock` is held.
2. A second thread closes or drops the generator.
3. Events establish the order. Fixed sleeps do not establish correctness.
4. The child records the close exception, owner connection transaction state, owner thread liveness, and whether a second broker instance can complete a write.
5. The parent enforces a short timeout and terminates the whole child if it wedges.

Never run this reproducer as an in-process helper thread with a join timeout. A timed-out thread would remain alive and could wedge the pytest process.

### D2. Map behavior by backend

- SQLite: prove whether the exact owner connection remains in a transaction and whether a second writer is blocked.
- PostgreSQL: run the equivalent integration case because its runner also has thread-local transaction state.
- Redis/Valkey: inspect and test its custom batch/core lifecycle separately. Do not infer SQL lock or transaction behavior.

### D3. Evaluate repair shapes

Compare at least these designs:

| Design | Required proof |
|---|---|
| Explicit transaction handle owned by the generator | Foreign cleanup can address the exact transaction; normal close and rollback stay compatible |
| Additive runner cleanup token/hook | Works for SQLite and PostgreSQL without making current optional hooks mandatory; third-party runner fallback is explicit |
| Same-thread fail-fast/documentation only | Prevents or detects misuse before a transaction/lock leaks; merely poisoning later calls is insufficient |

For each design, evaluate exact-owner rollback, Python lock ownership, callers already waiting on the lock, other broker instances, normal exhaustion, early close, garbage collection, fork behavior, third-party runners, and Redis/Valkey applicability.

### D4. Stop condition and output

Unit D ends when the evidence matrix is complete. Append the evidence and decision under `Investigation Results` below.

- If one design can prove exact cleanup and bounded failure across supported SQL backends, write a separate implementation plan with red tests and API compatibility notes.
- If no design can, document same-thread use as the supported contract and state what evidence is still missing.
- Do not edit `simplebroker/db.py` or runner production code in this unit.

Acceptance:

- No test can leave a live wedged thread in the pytest process.
- SQLite and PostgreSQL results are based on the owner transaction, not the foreign thread's thread-local connection.
- Redis/Valkey behavior is reported separately.
- No poison-flag fix is claimed without exact rollback and waiter-safety proof.

## Investigation Results

### 2026-07-14: cross-thread generator probe

**Revision and platform:** commit `524ef9d` plus this worktree; Darwin 25.5.0 arm64; Python 3.14.4. Each probe ran once in a spawned child process with a hard parent timeout. The diagnostic tests remain opt-in through `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1`, so normal test runs cannot deliberately wedge a backend transaction.

| Backend | Command | Observed result |
|---|---|---|
| SQLite | `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run pytest -n0 -s tests/test_cross_thread_generator_probe.py -q` | Foreign `generator.close()` raised `RuntimeError: cannot release un-acquired lock`. The exact owner connection remained in a transaction, the core lock remained unavailable, a caller already waiting on that core stayed blocked, and a second SQLite writer stayed blocked. Both original messages remained visible from the second core. |
| PostgreSQL | `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py -n0 -s -q` | Foreign close raised the same `RuntimeError`. The exact owner connection remained in transaction status `2`, the core lock remained unavailable, and the same-core waiter stayed blocked. A separate PostgreSQL writer completed, so the leaked transaction did not reproduce SQLite's database-wide write blockage, but the owning broker instance and transaction still leaked. |
| Redis/Valkey | `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= uv run ./bin/pytest-redis --fast extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py -n0 -s -q` | Foreign close completed without error. No core lock or waiter remained blocked, the second writer completed, and both original messages were visible. The SQL transaction failure mode does not apply to the Redis/Valkey core. |

**Design decision:** no production repair is justified in this plan. An explicit transaction handle is the only candidate that could address the exact owner transaction, but it still needs a lock-ownership design and compatibility work for both SQL runners. An additive cleanup token or hook has the same unresolved lock and waiter problem. A poison or fail-later state cannot repair the leaked owner transaction or wake callers already blocked on the lock. Redis/Valkey needs no equivalent SQL cleanup path.

The supported contract remains same-thread creation, consumption, exhaustion, and close for transactional generators on SQL-backed cores. A production fix requires a separate design that proves exact-owner rollback, bounded waiter behavior, normal and early close compatibility, garbage collection behavior, fork behavior, and third-party runner fallback.

## Final Verification and Scope Audit

During final verification, unexpected `ResourceWarning` failures exposed test-owned SQLite connections, subprocess pipes, and Redis/Valkey sockets from owner-created broker cores that were not shut down. The user expanded the acceptance gate to treat unexpected warnings as errors. Close those resources at their owners and set pytest's default warning filter to `error`; do not suppress the warnings.

### Verification results (2026-07-14)

- Root suite passed under the warning-as-error policy. The CI-equivalent root coverage run collected 1,688 passing tests and 14 platform/backend skips.
- PostgreSQL passed 851 shared tests and 101 extension tests, with three documented skips. All 165 applicable tests in the files touched by the final owned-core teardown audit also passed afterward.
- Redis/Valkey passed 844 shared tests and 79 extension tests, with ten documented skips and seven intentional deselections.
- Statement coverage is 92.3642% (8,927 of 9,665 statements), directly comparable to the prior line-only CI result. Branch coverage is 79.0006% (2,498 of 3,162 branches). The branch-inclusive aggregate is 89.0699%, above the unchanged 85% gate.
- An initial local result of 85.1407% was invalid because the coverage commands omitted CI's `COVERAGE_PROCESS_START` and `COVERAGE_FILE` environment. That omission lost subprocess traces from CLI and command tests. The workflow above now makes both variables explicit.
- `simplebroker/_backends/sqlite/schema.py` is 98.9130% combined coverage: 145 of 146 statements and 37 of 38 branches are covered. The one remaining branch is the documented defensive custom-runner check.
- Ruff check and format, mypy, the 80-test examples suite, release example typing, uv policy, and `git diff --check` pass.
- Warning cleanup closed test-owned SQLite handles, subprocess pipes, owned Redis runners, and cross-thread connections in the standalone SQLite example. No warning was suppressed.
- `uv.lock` and public exports are unchanged. Unit D changed no production code.

Each implementation unit must pass its focused gate before review. After Units A, B, and C are complete in the worktree, run:

```bash
uv sync --locked --extra dev --extra pg --extra redis
uv run pytest -n0 -q
PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast
PYTEST_ADDOPTS= uv run ./bin/pytest-redis --fast
uv run --frozen --no-sync ruff check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --frozen --no-sync ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --frozen --no-sync mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
uv run --frozen --no-sync pytest -n0 examples -q
uv run --frozen --no-sync python bin/release.py --check-example-types
python bin/bump_uv.py --check
git diff --check
git status --short
```

Also run the CI-equivalent branch-coverage workflow after Unit C. Confirm:

- `uv.lock` is unchanged.
- No public export changed.
- Unit D did not modify production code.
- Redis/Valkey was not omitted from generic backend claims or verification.
- F9 remains untouched and explicitly outside this plan.

## Acceptance Invariants

- User data is never deleted based on `Mock` appearing in a path.
- Supported behavior is proved through public APIs, with internals used only for diagnosis.
- The plan never asks a foreign thread to release another thread's `RLock` or roll back through the wrong thread-local connection.
- Optional runner hooks remain optional and dynamically resolved under the current public contract.
- Coverage truth is improved without silently lowering the gate.
- Concurrency investigations do not rely on timed-out live threads.
- SQLite, PostgreSQL, and Redis/Valkey are all treated as released backends, with backend-specific lifecycle behavior stated separately.

## Common Wrong Turns

- Testing F1 only through a normally constructed `BrokerDB`. That path does not reproduce the ownership state that triggers deletion.
- Calling `rollback()` on the foreign thread's runner connection and assuming the owner transaction was cleared.
- Adding a poison check only after acquiring the same leaked lock.
- Requiring SQLite to implement optional hooks merely to satisfy a capability test.
- Lowering `fail_under` after enabling branches without a separate decision.
- Precreating an idempotent SQLite object and claiming an exception handler ran.
- Describing Redis/Valkey with SQL transaction or lock semantics.

## Implementation Checklist

- [x] **A (P0, small):** Reproduce and remove Mock-path database deletion; preserve sidecar semantics.
- [x] **B (P2, small):** Eager metrics, dead fallback deletion, three watcher wait fixes, and verified docs.
- [x] **C (P2, medium):** Enable branch measurement, keep the 85 gate, classify and test SQLite schema branches.
- [x] **D (P1 investigation, medium):** Process-isolated cross-thread finalization evidence for SQLite, PostgreSQL, and Redis/Valkey; no production patch.
- [x] **Warning hygiene:** Close test-owned SQLite connections, subprocess pipes, and owner-created Redis/Valkey broker cores; make unexpected pytest warnings fail by default.
- [x] **Final gate:** Full tests, released backend suites, coverage, ruff, mypy, example checks, lockfile policy, diff check, and scope audit.

There are no open implementation decisions in Units A, B, or C. Unit D has explicit evidence gates and stop conditions, so it can be executed without guessing at a production design.

## GSTACK REVIEW REPORT

| Review | Runs | Status | Findings |
|---|---:|---|---|
| CEO Review | 3 | Stale | Earlier product-scope reviews predate this revision |
| Codex Review | 4 | Findings absorbed | Independent review concerns were incorporated; one earlier run was unavailable |
| Engineering Review | 8 | Clean after scope reduction | Unit E was removed; the remaining units have no new execution blocker |
| Design Review | 0 | Not applicable | No user-interface work |
| DevEx Review | 1 | Stale | Earlier review predates this revision |

**VERDICT:** Units A through C are implementation-ready. Unit D is investigation-ready and cannot silently expand into production changes.

Any production design arising from Unit D requires a new focused plan.

NO UNRESOLVED DECISIONS
