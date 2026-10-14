# Deep-Dive Critical Review Remediation Plan

Class: 5. Revises normative spec text in `docs/specs/01-development-documentation-operating-model.md` ([DOM-10.1]) and `docs/specs/16-python-library-api.md` ([SB-API-3]) via clarification-only Strategy D to match shipped code and policy tests, while remediating [DOM-5] risky boundary defects across SQL transaction interruption rollback, fork lock recovery, Redis multi-queue activity waiting and alias warning parity, and standalone example fork safety. No material program-theory revision and no process-policy change (not `+P`).
Plan type: Implementation with clarification-only spec alignment (Strategy D).
Owner: SimpleBroker product owner.
Boundary: Core SQL transaction rollback on `BaseException`, `_phaselock.py` and SQL `BrokerCore` fork safety, Redis alias `--quiet` suppression and multi-queue activity waiter wakeup latency, standalone `examples/` fork and event-loop safety, and spec/docstring/plan-header accuracy. Excludes [DOM-14] coalescing (handled separately per owner instruction).
Verification: Document gates (`bin/check-doc-paths`, `bin/check-plan-context`, `python3 bin/check-dom15-fixtures`), Ruff and suppression registry gates, state-machine policy gates (`tests/test_state_machine_policy.py`), and targeted plus full real-backend pytest suites.
Required action: Independently review this plan and its `## Proposed Spec Delta`, resolve review findings, then execute the ordered tasks.

## Goal

Remediate all non-coalescing findings verified during the 2026-10-14 deep-dive critical review of SimpleBroker `v8.7.0` (`1b42554`):
1. Ensure every SQL transaction handler in the Task 2 site inventory rolls back and clears `SQLiteRunner._transaction_owner` (or returns the PostgreSQL checkout cleanly) when interrupted by a non-`Exception` `BaseException` such as `KeyboardInterrupt`, without calling `rollback()` when `begin_immediate()` itself fails before acquiring transaction ownership.
2. Make `simplebroker/_phaselock.py` reset its process-lock registry on `os.fork()` and ensure all SQL `BrokerCore` alias/meta entry points invoke `self._check_fork_safety()` before touching inherited locks or runners.
3. Align `RedisBrokerCore.add_alias` with `BrokerCore.add_alias` on `_AliasShadowsQueueWarning` and `--quiet` (`_ALIAS_SHADOW_WARNING_SUPPRESSED`), and eliminate the 50ms polling sleep in `RedisMultiQueueActivityWaiter.wait()` by using condition-based wakeup on `_SharedRedisActivityListener`.
4. Fix the four fork-safety defects in the standalone `examples/sqlite_connect.py` reference implementation (`SQLiteConnectionManager`) and replace deprecated `asyncio.get_event_loop()` calls in `examples/async_wrapper.py` with `asyncio.get_running_loop()`.
5. Align `[DOM-10.1]` prose with the enforced Ruff `per-file-ignores` / `GLOBAL_IGNORES` policy, update `[SB-API-3]` and `Queue.conn` deprecation horizon wording from `8.3.x` to `8.x`, fix stale comments/docstrings in `simplebroker/watcher.py` and `simplebroker/db.py`, and remove stale `uncommitted` / pending-Windows-CI wording from the October 6 test-audit plans and `docs/plans/README.md`.

## Outcome Checklist

- [x] Every transaction handler in the Task 2 site inventory catches transaction-body `BaseException`, rolls back after successful BEGIN with secondary rollback errors suppressed, and re-raises without wedging SQLite transaction ownership or leaking a PostgreSQL checkout. Preserve retry wrappers around BEGIN, the timestamp-resync exception translation, and the distinct sidecar/generator cleanup contracts; do not mechanically rewrite every BEGIN caller.
- [x] `simplebroker/_phaselock.py` resets `_PROCESS_LOCKS_GUARD` and `_PROCESS_LOCKS` alongside `_DARWIN_XATTR_PROVIDER_LOCK` after `os.fork()`, and all SQL `BrokerCore` alias/meta entry points call `self._check_fork_safety()` before acquiring `self._lock` or touching the runner.
- [x] `RedisBrokerCore.add_alias` honours `_ALIAS_SHADOW_WARNING_SUPPRESSED` and emits `_AliasShadowsQueueWarning`, and `RedisMultiQueueActivityWaiter.wait()` wakes immediately via condition notification without a 50ms `time.sleep` polling loop while preserving `SM-ACTIVITY-WAITER` and `SM-REDIS-ACTIVITY-LISTENER`.
- [x] `examples/sqlite_connect.py` (`SQLiteConnectionManager`) safely recovers across `os.fork()` even when a parent thread holds `_setup_lock` or `_connections_lock`, abandons inherited SQLite connections without calling `.close()` in the child, guards `setup(phase)`, `get_connection()`, `close()`, and `__del__()` against cross-PID execution, and `examples/async_wrapper.py` uses `asyncio.get_running_loop()`.
- [x] `[DOM-10.1]`, `[SB-API-3]`, `simplebroker/sbqueue.py`, `simplebroker/watcher.py`, `simplebroker/db.py`, `docs/plans/2026-10-06-test-audit-remediation-plan.md`, `docs/plans/2026-10-06-whole-test-surface-audit-plan.md`, and `docs/plans/README.md` are aligned and pass all document and policy gates.

## Source Documents

Consulted startup surfaces: `AGENTS.md`; `docs/program-theory.md`; `docs/agent-context/README.md`; `docs/agent-context/decision-hierarchy.md`; `docs/agent-context/principles.md`; `docs/agent-context/engineering-principles.md`; `docs/agent-context/lessons.md`; `docs/lessons.md`.

- Theory: `docs/program-theory.md` [THEORY-2], [THEORY-3], [THEORY-4], [REV-THEORY-006].
- Source specs:
  - `docs/specs/01-development-documentation-operating-model.md` [DOM-5], [DOM-6], [DOM-10.1], [DOM-10.1.1], [DOM-11], [DOM-15], [DOM-16]
  - `docs/specs/10-cli.md` [SB-CLI-2]
  - `docs/specs/11-delivery.md` [SB-DELIVERY-3], [SB-DELIVERY-5]
  - `docs/specs/16-python-library-api.md` [SB-API-3], [SB-API-6], [SB-API-11]
  - `docs/specs/17-ops.md` [SB-OPS-3], [SB-OPS-5], [SB-OPS-6]
  - `docs/specs/product-section-registry.md`
- Implementation rationale:
  - `docs/implementation/04-cross-thread-finalization-poisoning.md`
  - `docs/implementation/06-process-session-core-ownership.md`
  - `docs/implementation/07-complexity-and-state-machine-map.md`
  - `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
  - `docs/implementation/10-ruff-suppression-registry.md`
- Runbooks:
  - `docs/agent-context/runbooks/writing-plans.md`
  - `docs/agent-context/runbooks/hardening-plans.md`
  - `docs/agent-context/runbooks/testing-patterns.md`
  - `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
  - `docs/agent-context/runbooks/maintaining-traceability.md`
  - `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`
- Related plans:
  - `docs/plans/2026-10-06-test-audit-remediation-plan.md`
  - `docs/plans/2026-10-06-whole-test-surface-audit-plan.md`
  - `docs/plans/2026-10-06-test-audit-tooling.md`
  - `docs/plans/2026-10-13-ruff-default-drift-plan.md`

## Spec Baseline

- `1b42554dfc0313c1837c23b0c7a5761f07627263` (`HEAD`, `v8.7.0`): governing specs, implementation docs, code, and tests at plan authoring time.
- Plan type: Implementation with clarification-only spec alignment.
- Promotion strategy: **D** (clarification-only spec edits in `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1] and `docs/specs/16-python-library-api.md` [SB-API-3] where code and tests already enforce the target behavior). No new `[SB-*]` or `[DOM-*]` section codes are introduced, and no traceability link claims change.
- Promotion baseline identifier: implementer records the spec-alignment worktree diff or commit SHA in the Execution Log when Task 1 applies the Strategy D clarifications.

## Proposed Spec Delta

| Spec file | Strategy | Sections touched |
| --- | --- | --- |
| `docs/specs/01-development-documentation-operating-model.md` | D — clarification-only alignment with existing `pyproject.toml` and `tests/test_ruff_policy.py` | [DOM-10.1] third requirement bullet (lines 291–294) and raw-diagnostic inventory paragraph (lines 320–326); add Related Plans backlink |
| `docs/specs/16-python-library-api.md` | D — clarification-only alignment with shipped `8.x` release line | [SB-API-3] fourth bullet (`Queue.conn` deprecation note, line 411); add Related Plans backlink |

### Exact delta 1 — `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1]

Replace lines 291–294:

```markdown
- global ignores are limited to explicitly documented repository-wide
  conflicts, and `per-file-ignores` is restricted to scoping `PLR2004` to
  `simplebroker/` by allowing only `PLR2004` on non-`simplebroker/` trees
  (`".github/**"`, `"benchmarks/**"`, `"bin/**"`, `"examples/**"`,
  `"extensions/**"`, `"fuzz/**"`, `"tests/**"`); `tests/test_ruff_policy.py`
  enforces the rule-code and shipped-package scope restriction, not that
  particular list of paths. Other suppressions are local, narrow, carry a
  reason, protect a tested invariant, and require explicit review before
  adoption
```

And replace lines 324–326:

```markdown
receiving false identity semantics. Global and per-file ignores outside the
policy-tested configuration in `pyproject.toml` are not permitted. Additional
baseline allowlists are not permitted. A cohesive parser, checklist, or state machine
must not be fragmented merely to lower its score.
```

### Exact delta 2 — `docs/specs/16-python-library-api.md` [SB-API-3]

Replace the entire existing `Queue.conn` compatibility bullet (currently lines 411–416), including its wrapped continuation lines, with this full bullet:

```markdown
- `Queue.conn` remains readable through the 8.x release line for compatibility
  with code that reached the connection manager directly, but it is deprecated
  and is not part of the supported surface. A later plan may relocate or remove
  it. Use `queue.session` to reach the minting scope when present,
  `session.connection()` for connection-level operations sharing that scope,
  or `open_broker()` for an independent connection.
```

## Context and Key Files

| File | Current structure & ownership | Required change |
| --- | --- | --- |
| `simplebroker/db.py` | `BrokerCore` owns SQL transaction orchestration (`_do_write_transaction` L2208–2231, `_do_insert_messages_transaction` L2260–2274, `_execute_transactional_operation` L2381–2402, `_resync_timestamp_generator` L3178–3220, `delete` L3480–3489, `delete_message_ids` L3519–3526, `delete_from_queues` L3593–3604, `rename_queue` L3633–3681, `broadcast` L3780–3814, `add_alias` L4060–4086, `remove_alias` L4094–4107), alias/meta methods (L3991–4107), and a pre-v6 comment in `write()` (L2048–2050). The schema-proof and v6-repair paths already catch `BaseException` and suppress secondary rollback errors. `sidecar` and `_yield_transactional_batches` instead track transaction-open state and have specialized owner-thread/finalization failure priority; preserve those paths. | Upgrade the 11 remaining `except Exception:` blocks after `begin_immediate()` so `self._runner.begin_immediate()` immediately precedes `try:` and `except BaseException:` runs `with contextlib.suppress(BaseException): self._runner.rollback()` (preserving `_resync_timestamp_generator`'s `except Exception as e:` -> `RuntimeError` translation while adding `except BaseException:` rollback for non-`Exception` interrupts). Add `self._check_fork_safety()` at the entry of `get_alias_version`, `resolve_alias`, `has_alias`, `list_aliases`, `aliases_for_target`, `get_meta`, `add_alias`, and `remove_alias`. Update the L2048–2050 comment in `write()` to reflect v6 schema (`ts` is the sole primary key). |
| `simplebroker/_backends/sqlite/maintenance.py` | `_vacuum_without_lock()` already calls BEGIN before its body `try:` but catches only `Exception`. | Keep the existing BEGIN placement; change the body handler to `except BaseException:` with suppressed secondary rollback errors. |
| `simplebroker/_backends/sqlite/schema.py` | `ensure_schema_v2` (L263–281), `ensure_schema_v3` (L294–316), and `validate_or_repair_current_v6` (L563–577) place `runner.begin_immediate()` before `try:` and catch `except BaseException:` with `with contextlib.suppress(BaseException): runner.rollback()`. However: (1) `initialize_database` (L77–139) catches `BaseException` at L136 but calls bare `runner.rollback()` without `contextlib.suppress(BaseException)`; (2) `ensure_schema_v4` (L329–338) and `ensure_schema_v5` (L349–360) place `runner.begin_immediate()` inside `try:` and catch `except Exception:`; and (3) `ensure_schema_v6` (L501–548) places `runner.begin_immediate()` at L502 inside the `try:` block that runs `runner.rollback()` at L540. | In `initialize_database` (L137), wrap `runner.rollback()` in `with contextlib.suppress(BaseException):`. In `ensure_schema_v4` (L329–338) and `ensure_schema_v5` (L349–360), move `runner.begin_immediate()` immediately before `try:` and catch `except BaseException:` with `with contextlib.suppress(BaseException): runner.rollback()`. In `ensure_schema_v6` (L501–548), place `runner.begin_immediate()` inside the outer pragma-restoring `try:...finally:` (after L501) and immediately before an inner transaction `try:...except BaseException:` block so a failure in `begin_immediate()` still restores pragmas via `finally:` without calling `runner.rollback()`. |
| `extensions/simplebroker_pg/simplebroker_pg/schema.py` | `initialize_database` (`initialize_under_lock` L283–313) and `migrate_schema` (L383–423) call `runner.begin_immediate()` before `try:` and catch `except Exception:`. In `PostgresRunner` (`extensions/simplebroker_pg/simplebroker_pg/runner.py:987-1062`), `begin_immediate()` marks `self._thread_local.in_transaction` and retains the checkout in thread-local `conn` or an explicit lease entry until commit/rollback settles it. There is no `_active_conn` field. | Add `import contextlib` and change `except Exception:` at L309 and L419 to `except BaseException:`, wrapping `runner.rollback()` in `with contextlib.suppress(BaseException):` before `_invalidate_meta_state(runner)` and `raise`. |
| `simplebroker/_phaselock.py` | Module-level `_PROCESS_LOCKS_GUARD = threading.Lock()` (L63), `_PROCESS_LOCKS: dict[Path, threading.RLock] = {}` (L64), and `_DARWIN_XATTR_PROVIDER_LOCK = threading.Lock()` (L108). `_reset_darwin_xattr_lock_after_fork()` (L111–116) is registered via `os.register_at_fork(after_in_child=...)` and currently resets only `_DARWIN_XATTR_PROVIDER_LOCK`. | Extend `_reset_darwin_xattr_lock_after_fork()` so it resets `_PROCESS_LOCKS_GUARD = threading.Lock()`, `_PROCESS_LOCKS = {}`, and `_DARWIN_XATTR_PROVIDER_LOCK = threading.Lock()` in the child process after `os.fork()`. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py` | `add_alias()` unconditionally emits `RuntimeWarning` when shadowing a queue. Alias/meta/pending commands reach the recovering runner through `_client` -> `runner.client`, and do not take core locks. | Change only alias warning suppression/category in Task 4. Do not add redundant core fork guards or a held-core-lock test that already passes before the change. |
| `extensions/simplebroker_redis/simplebroker_redis/plugin.py` | `_SharedRedisActivityListener` (L140–268) multiplexes Redis pub/sub notifications under `self._lock = threading.RLock()`. `RedisMultiQueueActivityWaiter.wait()` (L307–317) currently polls each child waiter with `waiter.wait(0)` and calls `time.sleep(min(0.05, remaining))`. | Add a condition-based multi-registration wait path (`wait_many`) on `_SharedRedisActivityListener` notified under `self._lock` whenever any queue version advances, listener stops, or listener errors, and delegate `RedisMultiQueueActivityWaiter.wait(timeout)` to it when all open child waiters are `RedisActivityWaiter` instances whose `_listener` is the same `_SharedRedisActivityListener` and whose `_stop_event` is `self._stop_event` (keeping the fallback loop for stub/mixed waiters in unit tests). |
| `examples/sqlite_connect.py` | Standalone `SQLiteConnectionManager` (L530 onward). `get_connection()` (L577) performs inline PID checking; `_handle_fork()` (L599) closes the inherited thread-local connection and acquires inherited locks. `setup(phase)` (L659), `close()` (L864), and `__del__()` (L898) lack a PID guard. No `_check_fork()` helper exists. | Introduce `_check_fork(self)` to compare `os.getpid()` with `self._pid` and call `_handle_fork(current_pid)` on mismatch. Reuse it at entry to `get_connection()`, `setup(phase)`, and `close()`. Replace locks and reset child bookkeeping without acquiring inherited locks; retain inherited connections in a module-level `_ABANDONED_FORK_CONNECTIONS` list without closing them. Make `__del__()` retain inherited handles before returning without close or unlink. |
| `examples/async_wrapper.py` | Standalone stdlib async wrapper around `Queue` and `QueueWatcher`. Lines 61, 249, and 344 call `asyncio.get_event_loop()` inside `async def` functions. | Replace all three `asyncio.get_event_loop()` calls with `asyncio.get_running_loop()`. |
| `simplebroker/sbqueue.py` | `Queue.conn` docstring (L232) says `This attribute remains readable through the` followed by `8.3 release line.`. | Change the existing two-line compatibility sentence to `This attribute remains readable through the` followed by `8.x release line.` |
| `simplebroker/watcher.py` | `QueueMoveWatcher` docstring (L2051) says `It inherits from QueueWatcher`. | Update docstring to `It inherits from BaseWatcher`. |
| `docs/specs/01-development-documentation-operating-model.md`, `docs/specs/16-python-library-api.md`, `docs/plans/2026-10-06-test-audit-remediation-plan.md`, `docs/plans/2026-10-06-whole-test-surface-audit-plan.md`, `docs/plans/README.md` | Spec and plan metadata files with minor prose/status drift noted above. | Apply Strategy D clarifications, update `2026-10-06-test-audit-remediation-plan.md` (L3, L279–281), `2026-10-06-whole-test-surface-audit-plan.md` (L4), and `docs/plans/README.md` (L36–37) to reflect committed (`208c3a8`) and Windows-CI-verified (`Test 38458241452`) state, and keep this plan's row in `docs/plans/README.md` (L32) current. |

### Comprehension Gate (Mandatory Before Editing)

The implementer must record answers to these three questions in the Execution Log before modifying code. Wrong answers block implementation until the cited owners are reread:

1. **Why must `SQLiteRunner.rollback()` run on `BaseException` (such as `KeyboardInterrupt`) after `begin_immediate()`, why must `begin_immediate()` precede `try:`, and why must `rollback()` itself be wrapped in `contextlib.suppress(BaseException)`?**
   - *Expected answer:* `SQLiteRunner._claim_transaction()` publishes the owning `Thread`, and a successful `begin_immediate()` starts a real SQLite transaction. If a transaction-body interrupt escapes an `except Exception:` handler, ownership and the transaction remain active. `rollback()` settles ownership; after driver rollback failure it invalidates the uncertain connection. BEGIN must finish before entering the body rollback handler so failed same-thread re-entrant BEGIN cannot roll back an existing outer transaction. `_admit_terminal_operation()` already protects foreign-thread ownership; assert the caller made zero rollback calls rather than inferring it from owner progress. Keep `run_with_retry(runner.begin_immediate)` / `self._run_with_retry(self._runner.begin_immediate)` where present. Suppress only secondary cleanup failures and re-raise the original body exception; keep `_resync_timestamp_generator`'s existing `Exception` to `RuntimeError` translation. Inject the regression interrupt in a body SQL call after BEGIN has returned, never in an override that raises after `super().begin_immediate()`. That override creates a different failure at the acquisition seam and bypasses the caller's body handler.
2. **Why must `examples/sqlite_connect.py` abandon inherited `sqlite3.Connection` objects in a forked child instead of calling `conn.close()` on them?**
   - *Expected answer:* Inherited SQLite connections are not child-owned resources. Explicit close or Python finalization can enter inherited SQLite or Python synchronization state whose owning parent thread no longer exists. Follow `simplebroker/_runner.py`'s retention pattern: replace inherited guards first and keep strong references without invoking cleanup. The rationale is unsafe inherited-resource cleanup; do not claim that traditional POSIX `F_SETLK` locks are shared open-file-description locks or that child close necessarily releases the parent's locks.
3. **Why can `RedisMultiQueueActivityWaiter` wait on a single condition in `_SharedRedisActivityListener` without losing per-queue wakeups or violating `SM-ACTIVITY-WAITER` / `SM-REDIS-ACTIVITY-LISTENER`?**
   - *Expected answer:* All `RedisActivityWaiter` instances created for the same `(pid, target, namespace)` share one `_SharedRedisActivityListener` and its re-entrant lock `self._lock` (`extensions/simplebroker_redis/simplebroker_redis/plugin.py:154,216`). Every per-queue `threading.Condition` in `self._conditions` is already bound to `self._lock`. Checking all registered queue versions under `self._lock` and waiting on a condition bound to `self._lock` (which is notified whenever any registered queue version increments, `_error` is set, or `_stop_event` is set) preserves the exact version-advance and error semantics of `_SharedRedisActivityListener.wait()` while eliminating the 50ms sleep between polls.

## Invariants and Constraints

- **No public API signature, CLI grammar, wire format, or storage schema changes.** All changes preserve the v6 SQL schema, Redis key/Lua contracts, and public Python/CLI signatures.
- **Original exception propagation on rollback:** When an operation or `BaseException` aborts a transaction after `begin_immediate()`, the original exception must propagate unchanged; `with contextlib.suppress(BaseException): runner.rollback()` must never swallow the primary exception or replace it with a rollback error.
- **BEGIN before the body rollback handler:** `_admit_terminal_operation()` already rejects a foreign-thread rollback before it touches the connection. Foreign-owner progress alone therefore cannot prove the caller avoided rollback. The dangerous case is failed same-thread re-entrant BEGIN: a misplaced rollback handler could roll back the existing outer transaction. Keep BEGIN outside the body handler at unchanged sites; relocate it only at `_resync_timestamp_generator` and SQLite `ensure_schema_v4`, `ensure_schema_v5`, `ensure_schema_v6` (inside the existing pragma-restoring outer `try:` for v6). A passthrough rollback call counter is the direct proof of no erroneous cleanup invocation.
- **Preserve `_resync_timestamp_generator` exception translation (`simplebroker/db.py:3178-3220`):** `_resync_timestamp_generator` is contracted to translate `Exception` into `RuntimeError(f"Failed to resynchronize timestamp generator: {e}") from e`. It must keep `except Exception as e:` (with `with contextlib.suppress(BaseException): self._runner.rollback()` before `raise RuntimeError(...) from e`) and add a separate `except BaseException:` clause that runs `with contextlib.suppress(BaseException): self._runner.rollback()` and re-raises non-`Exception` `BaseException`s unchanged.
- **Fork lock-replacement ordering:** In both `simplebroker/_phaselock.py` and `examples/sqlite_connect.py`, fork recovery in the child process must **never** attempt to `.acquire()` an inherited `threading.Lock` or `threading.RLock` before replacing it, because a parent thread that held that lock at `os.fork()` time does not exist in the child to release it.
- **Preserve state-machine contracts (`SM-ACTIVITY-WAITER` and `SM-REDIS-ACTIVITY-LISTENER`):**
  - `wait_many` must preserve live-wait queue version ordering, return `True` and advance only the first changed registration (by `+1` when `current > registration.version`), and raise `OperationalError` on the existing listener error path. `[SB-API-6]` does not define wait-after-close, and `SM-ACTIVITY-WAITER` owns cleanup only. Keep closed/empty cases on the existing bounded-sleep fallback instead of adding an immediate-return policy. External stop still returns `False` through the existing loop check.
  - Because unit tests in `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py:55-69` pass a lightweight `_Listener` test double into `RedisActivityWaiter`, `RedisMultiQueueActivityWaiter.wait()` must check `isinstance(w, RedisActivityWaiter) and isinstance(w._listener, _SharedRedisActivityListener) and w._stop_event is self._stop_event` on all `open_waiters = [w for w in self._waiters if not getattr(w, "_closed", False)]` before taking the `wait_many` fast path, falling back to the per-waiter loop when stub listeners are used.
  - `RedisMultiQueueActivityWaiter.close()` must remain idempotent and call `_run_activity_waiter_cleanup` across all child waiters so every queue registration is unregistered and every listener reference in `_activity_registry` is released.
- **Fatal vs. best-effort error paths:**
  - Transaction body errors and `BaseException` interrupts are **fatal** to the in-flight transaction (must roll back and re-raise).
  - Rollback cleanup during exception unwinding is **best-effort** (`contextlib.suppress(BaseException)`) so the primary failure is never masked, while in `PostgresRunner.rollback()` (`extensions/simplebroker_pg/simplebroker_pg/runner.py:1049-1062`) successful rollback calls `_finish_transaction(leased=leased)` in `finally`; rollback failure returns an unleased checkout via `_return_thread_conn()` or discards a leased checkout. The tests must exercise the actual path rather than an assumed `_active_conn` field.
- **Hidden couplings called out:**
  - `cmd_alias_add` in `simplebroker/commands.py:282-284` sets `_ALIAS_SHADOW_WARNING_SUPPRESSED` around `broker.add_alias(alias, target)`. `RedisBrokerCore.add_alias` is called through `BrokerConnection` on that exact path when `--quiet` is passed on a Redis target.
  - `examples/test_sqlite_connect.py:562-583` (`test_fork_safety_simulation`) simulates fork in-process by mutating `manager._pid = original_pid + 1000` in the same process. When `_handle_fork()` moves `conn1` into `_ABANDONED_FORK_CONNECTIONS` instead of calling `conn1.close()`, `test_fork_safety_simulation` must explicitly close `conn1` and remove it from `_ABANDONED_FORK_CONNECTIONS` during teardown so it does not leak an open SQLite handle in the test process.
  - `tests/test_ruff_policy.py` parses `docs/implementation/10-ruff-suppression-registry.md` and `pyproject.toml`. Using `contextlib.suppress(BaseException)` avoids `BLE001` entirely and requires zero new Ruff suppressions.

## Rollback, Rollout Sequencing, and One-Way Doors

- **One-way doors:** None. No storage schema migration, wire-format change, or public package publication is part of this plan.
- **Rollout sequencing:**
  1. Task 1 lands Strategy D spec clarifications and plan-index updates first so documentation gates validate against the target text.
  2. Tasks 2–5 land in independent, self-contained slices (SQL `BaseException` rollback -> core fork safety -> Redis parity & waiter wakeup -> standalone `examples/` fixes -> final cross-suite verification).
- **Rollback:** Every slice is a pure in-process runtime, test, or documentation change with no persistent format changes. Any slice can be reverted independently via `git checkout` / `git revert` without affecting existing SQLite databases, PostgreSQL schemas, or Redis namespaces.
- **Observable success signals:**
  - Zero wedged `SQLiteRunner._transaction_owner` states or open `in_transaction` connections after `KeyboardInterrupt` during any write, claim, move, delete, broadcast, alias, timestamp-resync, vacuum, or schema-migration transaction.
  - Zero child deadlocks when `os.fork()` occurs concurrently with `_phaselock` acquisition or `examples/sqlite_connect.py` setup/connection creation.
  - Condition notification wakes `RedisMultiQueueActivityWaiter.wait()` after multi-queue activity without waiting for the old 50ms sleep quantum (verify causally; optional local latency measurements are informational, not acceptance thresholds) and zero warnings emitted by `broker --quiet alias add` on Redis when shadowing an existing queue.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
| --- | --- | --- | --- | --- |
| Standalone example fork ownership (Task 5; [SB-API-11] rationale pattern) | Cross-PID destructor returns without close/unlink | Retain tracked inherited connections before that return | Real manager collection otherwise drops its fields and can invoke SQLite connection cleanup despite a manual __del__ probe passing | No public spec delta; completes the planned inherited-handle abandonment invariant |

## Tasks

### Task 1: Spec-Alignment Slice (Strategy D) and Plan Metadata Hygiene

- **Outcome:** Align `[DOM-10.1]` and `[SB-API-3]` with shipped code/tests, update `simplebroker/sbqueue.py`, `simplebroker/watcher.py`, `simplebroker/db.py` comment, and clean up stale `uncommitted` prose in the October 6 test-audit plans and `docs/plans/README.md`.
- **Files to touch:**
  - `docs/specs/01-development-documentation-operating-model.md` (lines 291–294, 324–326, and `## Related Plans`)
  - `docs/specs/16-python-library-api.md` (line 411 and `## Related Plans`)
  - `simplebroker/sbqueue.py` (line 232)
  - `simplebroker/watcher.py` (line 2051)
  - `docs/guides/python.md` (`Queue.conn` compatibility paragraph)
  - `CHANGELOG.md` (`[Unreleased]`)
  - `simplebroker/db.py` (lines 2048–2050)
  - `docs/plans/2026-10-06-test-audit-remediation-plan.md` (line 3 and lines 279–281)
  - `docs/plans/2026-10-06-whole-test-surface-audit-plan.md` (line 4)
  - `docs/plans/README.md` (lines 32, 36, 37)
- **Read first:** `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1], `docs/specs/16-python-library-api.md` [SB-API-3], `pyproject.toml` lines 152–168, `tests/test_ruff_policy.py` lines 100–130.
- **Steps:**
  1. Apply Exact Delta 1 to `docs/specs/01-development-documentation-operating-model.md` and Exact Delta 2 to `docs/specs/16-python-library-api.md`, adding a backlink to `docs/plans/2026-10-14-deep-review-remediation-plan.md` under `## Related Plans` in both specs.
  2. In `Queue.conn` at `simplebroker/sbqueue.py:234-235`, change the wrapped sentence `This attribute remains readable through the` / `8.3 release line.` to `This attribute remains readable through the` / `8.x release line.`
  3. In `simplebroker/watcher.py:2051`, update `It inherits from QueueWatcher` to `It inherits from BaseWatcher`.
  4. In the timestamp-conflict handler at `simplebroker/db.py`, replace the entire comment block beginning `# The only INSERT in _do_write_with_ts_retry targets the` and ending `# error message wording (SQLite vs Postgres).` with the following backend-neutral block. Replace the whole block so no duplicate tail remains:
     ```python
     # The only INSERT in _do_write_with_ts_retry targets the
     # messages table, whose unique constraints in schema v6 are on ts.
     # Any IntegrityError here is therefore a timestamp conflict, regardless
     # of backend error message wording (SQLite vs Postgres).
     ```
  5. In `docs/plans/2026-10-06-test-audit-remediation-plan.md` (L3 and L279–281), `docs/plans/2026-10-06-whole-test-surface-audit-plan.md` (L4), and `docs/plans/README.md` (L36–37), replace the stale `uncommitted` / pending-Windows-CI phrasing with the accurate committed (`208c3a8`) and Windows-CI-verified (`Test 38458241452`) state, noting that the standalone-example fork defect is addressed by `2026-10-14-deep-review-remediation-plan.md`. Flip row 32 in `docs/plans/README.md` from `draft` to `active` once implementation starts.
  6. Align `docs/guides/python.md`'s `Queue.conn` paragraph with the 8.x compatibility wording and lack of a runtime deprecation warning. Add `[Unreleased]` changelog entries for transaction-body interrupt rollback, SQL alias/meta fork rejection and process-lock reset, Redis alias quiet/category parity and condition-driven multi-queue wakeup, and standalone example fixes. Record these as the intended changes, and reconcile them against the built slices before closing the implementation plan. Keep README restatements and the listed implementation owners aligned wherever their descriptions become stale; do not invent a new product contract for unchanged semantics.
- **Stop-and-re-evaluate gate:** Stop if any edit in `docs/specs/01-development-documentation-operating-model.md` breaks `python3 bin/check-dom15-fixtures` or `tests/test_ruff_policy.py`.
- **Per-task verification:**
  - `bin/check-doc-paths`
  - `bin/check-plan-context`
  - `python3 bin/check-dom15-fixtures`
  - `uv run pytest tests/test_ruff_policy.py tests/test_python_library_api_contract_sb_api.py -q`

### Task 2: SQL `begin_immediate()` `BaseException` Rollback Hardening

- **Outcome:** Harden the transaction-body handlers in the site inventory below. Keep BEGIN outside the body rollback handler, retain any existing retry wrapper, roll back with `contextlib.suppress(BaseException)`, and clear ownership / return the PostgreSQL checkout when a body `BaseException` interrupts the transaction. `_resync_timestamp_generator` retains its outer ordinary-error translation; sidecar and generator finalization retain their separate failure-priority contracts.
- **Files to touch:**
  - `simplebroker/db.py`
  - `simplebroker/_backends/sqlite/maintenance.py`
  - `simplebroker/_backends/sqlite/schema.py`
  - `extensions/simplebroker_pg/simplebroker_pg/schema.py`
  - `tests/test_runner_error_handling.py`
  - `tests/test_sqlite_schema.py`
  - `extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py`
  - `extensions/simplebroker_pg/tests/test_pg_integration.py`
- **Read first:** `simplebroker/_runner.py` lines 475–729 (`SQLiteRunner._claim_transaction`, `begin_immediate`, `commit`, `rollback`), `simplebroker/db.py` lines 1645–1660 (`_publish_sqlite_schema_proof` reference pattern) and lines 2500–2510 (`_yield_transactional_batches` reference pattern), `simplebroker/_backends/sqlite/schema.py` lines 551–578 (`validate_or_repair_current_v6` reference pattern), `extensions/simplebroker_pg/simplebroker_pg/runner.py` lines 987–1062 (`PostgresRunner.begin_immediate`, `commit`, `rollback`).
- **Steps:**
  1. In `simplebroker/db.py`, update all 11 transaction blocks that currently catch `except Exception:` after `self._runner.begin_immediate()`:
     - `_do_write_transaction` (L2208–2231)
     - `_do_insert_messages_transaction` (L2260–2274)
     - `_execute_transactional_operation` (L2381–2402)
     - `_resync_timestamp_generator` (L3178–3220)
     - `delete` (L3480–3489)
     - `delete_message_ids` (L3519–3526)
     - `delete_from_queues` (L3593–3604)
     - `rename_queue` (L3633–3681)
     - `broadcast` (L3780–3814)
     - `add_alias` (L4060–4086)
     - `remove_alias` (L4094–4107)
     In the 10 standard sites (all except `_resync_timestamp_generator`), keep BEGIN immediately before the body `try:`. In `_execute_transactional_operation`, retain `self._run_with_retry(self._runner.begin_immediate)` instead of replacing it with a direct call. At the other sites use `self._runner.begin_immediate()`, and replace `except Exception:` / `self._runner.rollback()` with:
     ```python
     except BaseException:
         with contextlib.suppress(BaseException):
             self._runner.rollback()
         raise
     ```
     In `_resync_timestamp_generator` (L3178–3220), place `self._runner.begin_immediate()` outside a `try:` block that translates `Exception` or inside the outer `try:` that translates `Exception` into `RuntimeError`, while ensuring `rollback()` only runs after `begin_immediate()` succeeds:
     ```python
     with self._lock:
         try:
             self._runner.begin_immediate()
             try:
                 ...
                 self._runner.commit()
             except BaseException:
                 with contextlib.suppress(BaseException):
                     self._runner.rollback()
                 raise
         except Exception as e:
             raise RuntimeError(f"Failed to resynchronize timestamp generator: {e}") from e
     ```
     This preserves the `RuntimeError` translation if `begin_immediate()` or the body raises `Exception`, avoids calling `rollback()` if `begin_immediate()` itself fails, and rolls back + re-raises if `KeyboardInterrupt` or another non-`Exception` `BaseException` interrupts the transaction body!
  2. In `simplebroker/_backends/sqlite/maintenance.py` (`_vacuum_without_lock`, L210–220), retain `runner.begin_immediate()` immediately before `try:` and replace `except Exception:` with `except BaseException:` using `with contextlib.suppress(BaseException): runner.rollback()`.
  3. In `simplebroker/_backends/sqlite/schema.py`:
     - In `initialize_database` (L136–138), wrap `runner.rollback()` in `with contextlib.suppress(BaseException):`.
     - In `ensure_schema_v4` (L329–338) and `ensure_schema_v5` (L349–360), place `runner.begin_immediate()` immediately before `try:` and catch `except BaseException:` with `with contextlib.suppress(BaseException): runner.rollback()`, matching `ensure_schema_v2` and `ensure_schema_v3`.
     - In `ensure_schema_v6` (L501–548), keep the outer `try:...finally:` for `_restore_v6_rebuild_pragmas`, call `runner.begin_immediate()` at the start of the outer `try:`, and wrap the post-`begin_immediate()` migration body + `runner.commit()` in an inner `try:...except BaseException:` block that runs `with contextlib.suppress(BaseException): runner.rollback()` and `raise`.
  4. In `extensions/simplebroker_pg/simplebroker_pg/schema.py`, add `import contextlib` and update `initialize_under_lock` (L283–313) and `migrate_schema` (L383–423) so `except BaseException:` runs `with contextlib.suppress(BaseException): runner.rollback()` followed by `_invalidate_meta_state(runner)` and `raise`.
  5. Add red-green firing tests for every changed site in the inventory, parameterizing cases where they share the same harness. Use real SQLite connections and `SQLiteRunner` ownership. Arm a fault in the first body `runner.run()` after successful BEGIN, after fixture/setup work finishes, to raise the exact pre-created `KeyboardInterrupt` object. For `_execute_transactional_operation`, cover both claim and move. For `_resync_timestamp_generator`, prove an ordinary body error still becomes `RuntimeError` with the original cause, and a non-`Exception` interrupt propagates unchanged. Repeat representative write interruption with `SystemExit` and `GeneratorExit` to exercise the full non-`Exception` family without duplicating every site/family combination.
     - After ordinary rollback, assert `runner._transaction_owner is None` and the captured real connection's `in_transaction is False`; prove same-thread and second-thread write/claim progress with owned readiness and bounded cleanup.
     - For the sites whose BEGIN is relocated (`_resync_timestamp_generator`, `ensure_schema_v4`, `ensure_schema_v5`, `ensure_schema_v6`), add a same-thread nested-BEGIN failure case using an existing real outer transaction and a passthrough counter around the real `rollback()`. Assert the counter is zero, the outer transaction remains active, and its owner can still complete it. For v6 also assert pragma restoration survives failed BEGIN. An additional foreign-owner admission case may reuse existing tests, but owner progress alone is not distinguishing evidence; retain the counter if testing caller cleanup. Preserve contention retry classification.
     - Add secondary rollback-failure proof: fault the driver boundary while retaining real runner ownership handling, assert the primary interrupt's identity survives, ownership is cleared and the uncertain connection is invalidated. Reuse the existing `test_rollback_failure_invalidates_transaction_connection` setup in `tests/test_runner_error_handling.py`, located by symbol. Do not assert an invalidated connection remains usable; verify a fresh connection succeeds. If rollback and connection close both fail, retain the runner's existing unusable-until-close policy.
  6. In `extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py`, cover bootstrap and migration handler control flow including original-error propagation, secondary rollback failure, metadata invalidation and no rollback on failed BEGIN. In `extensions/simplebroker_pg/tests/test_pg_integration.py`, use the real `pg_runner` / `pg_dsn` / `pg_schema` fixtures and a unique managed schema to interrupt the first body SQL operation after successful BEGIN for both bootstrap and migration. Use `create_pg_v5_schema` from `extensions/simplebroker_pg/tests/conftest.py` for a genuine historical migration input, following `test_real_postgres_injected_v6_failure_rolls_back_v5_layout` in `extensions/simplebroker_pg/tests/test_pg_message_id_order.py`, so the fast path does not skip the body. Use an unleased runner for this checkout-return proof: assert `runner._in_transaction()` is false and `getattr(runner._thread_local, "conn", None)` is `None`; prove later schema initialization/migration succeeds and durable state has no partial schema/version publication. Do not replace `begin_immediate()`, `rollback()`, the pool, or checkout bookkeeping with test doubles in the real integration proof.

#### Task 2 Transaction Site Inventory

Each changed row needs a firing body-interruption case. BEGIN admission and secondary-cleanup failures may share representative cases per backend; they need not be repeated at every row.

| Owner | Changed transaction handlers | Proof owner |
| --- | --- | --- |
| `simplebroker/db.py` | `_do_write_transaction`, `_do_insert_messages_transaction`, `_execute_transactional_operation` (claim and move), `_resync_timestamp_generator`, `delete` / `_do_delete`, `delete_message_ids` / `_do_delete_message_ids`, `delete_from_queues` / `_do_delete_from_queues`, `rename_queue` / `_do_rename_queue`, `broadcast` / `_do_broadcast`, `add_alias`, `remove_alias` | `tests/test_runner_error_handling.py`; retain retry/translation coverage in `tests/test_transaction_error_propagation.py` |
| `simplebroker/_backends/sqlite/maintenance.py` | `_vacuum_without_lock()` already calls BEGIN before its body `try:` but catches only `Exception`. | Keep the existing BEGIN placement; change the body handler to `except BaseException:` with suppressed secondary rollback errors. |
| `simplebroker/_backends/sqlite/schema.py` | `initialize_database` (retain `run_with_retry(runner.begin_immediate)`), `ensure_schema_v4`, `ensure_schema_v5`, `ensure_schema_v6` | `tests/test_sqlite_schema.py` |
| `extensions/simplebroker_pg/simplebroker_pg/schema.py` | `initialize_database` / `initialize_under_lock`, `migrate_schema` | `extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py` plus real `extensions/simplebroker_pg/tests/test_pg_integration.py` |

Already-hardened sites are inspection/regression boundaries, not rewrite targets: `BrokerCore._publish_sqlite_schema_proof`, SQLite `ensure_schema_v2`, `ensure_schema_v3`, `validate_or_repair_current_v6`, and the separately owned `BrokerCore.sidecar` / `_yield_transactional_batches` cleanup state machines. `_BorrowedRunner.begin_immediate` is a forwarding method, not a transaction handler. The PostgreSQL plugin `_run_vacuum_body` is already BaseException-safe through `capture_pg_step`; keep it unchanged. Search both direct calls and retry-wrapped method references when checking inventory completeness.
- **Stop-and-re-evaluate gate:** Stop if changing `begin_immediate()` placement alters retry classification for `OperationalError` when `BEGIN IMMEDIATE` itself contends on a locked database (`run_with_retry` must still wrap `begin_immediate()` inside the retryable callable).
- **Per-task verification:**
  - `uv run pytest tests/test_runner_error_handling.py tests/test_transaction_error_propagation.py tests/test_sqlite_schema.py -q`; `uv run bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py extensions/simplebroker_pg/tests/test_pg_integration.py -q`

### Task 3: Core Fork Safety (`_phaselock.py` & SQL `BrokerCore` Entry Points)

- **Outcome:** `simplebroker/_phaselock.py` resets its process-lock dictionary and guard lock in the child after `os.fork()`, and all SQL `BrokerCore` alias/meta methods check fork safety before acquiring locks or running queries.
- **Files to touch:**
  - `simplebroker/_phaselock.py` (lines 60–125)
  - `simplebroker/db.py` (lines 3991–4107)
  - `tests/test_phaselock.py`
  - `tests/test_fork_safety.py`
- **Read first:** `simplebroker/_phaselock.py` lines 55–160, `simplebroker/db.py` lines 1680–1705 (`_check_fork_safety`), `docs/implementation/06-process-session-core-ownership.md` (fork safety section), `tests/test_phaselock_transition_tables.py` lines 405–465.
- **Steps:**
  1. In `simplebroker/_phaselock.py`, update `_reset_darwin_xattr_lock_after_fork()` (L111–116) so that after `os.fork()` in the child it resets `_PROCESS_LOCKS_GUARD`, `_PROCESS_LOCKS`, and `_DARWIN_XATTR_PROVIDER_LOCK`:
     ```python
     def _reset_darwin_xattr_lock_after_fork() -> None:
         """Give a fork child unlocked process and xattr discovery guards."""
         global _PROCESS_LOCKS_GUARD, _PROCESS_LOCKS, _DARWIN_XATTR_PROVIDER_LOCK

         _PROCESS_LOCKS_GUARD = threading.Lock()
         _PROCESS_LOCKS = {}
         _DARWIN_XATTR_PROVIDER_LOCK = threading.Lock()
     ```
  2. In `simplebroker/db.py`, add `self._check_fork_safety()` at the entry of `get_alias_version` (L3991), `resolve_alias` (L3996), `has_alias` (L4005), `list_aliases` (L4010), `aliases_for_target` (L4015), `get_meta` (L4024), `add_alias` (L4054, before `self._assert_no_reentrant_mutation_during_batch`), and `remove_alias` (L4088, before `self._assert_no_reentrant_mutation_during_batch`).
  3. Write real `os.fork()` tests in `tests/test_phaselock.py` and `tests/test_fork_safety.py` (skipped on non-POSIX platforms without `os.fork`) proving:
     - when a background thread in the parent holds a `_phaselock` per-path `RLock` or `_PROCESS_LOCKS_GUARD` across `os.fork()`, the child acquires `_acquire_process_lock()` without deadlocking on the inherited in-process lock,
     - SQL `BrokerCore` alias/meta methods (`get_alias_version`, `resolve_alias`, `has_alias`, `list_aliases`, `aliases_for_target`, `get_meta`, `add_alias`, `remove_alias`) promptly raise the existing fork `RuntimeError` before touching inherited locks/runners, even when a parent thread held `core._lock` at fork. Do not reopen or recover inherited SQL cores; a newly constructed child Queue/core remains usable.
  4. Apply the shared fork-probe harness rules below to all new real fork tests.
- **Stop-and-re-evaluate gate:** Stop if `tests/test_phaselock_transition_tables.py` (`SM-DARWIN-XATTR`) or `tests/test_state_machine_policy.py` fails.
- **Per-task verification:**
  - `uv run pytest tests/test_phaselock.py tests/test_phaselock_transition_tables.py tests/test_fork_safety.py -q`

### Task 4: Redis Alias Warning Parity & Condition-Based `RedisMultiQueueActivityWaiter`

- **Outcome:** `RedisBrokerCore.add_alias` respects `--quiet` (`_ALIAS_SHADOW_WARNING_SUPPRESSED`) and emits `_AliasShadowsQueueWarning`, and `RedisMultiQueueActivityWaiter.wait()` wakes via condition notification without a 50ms polling sleep.
- **Files to touch:**
  - `extensions/simplebroker_redis/simplebroker_redis/core.py` (lines 2071–2106)
  - `extensions/simplebroker_redis/simplebroker_redis/plugin.py` (lines 140–325)
  - `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py`
  - `extensions/simplebroker_redis/tests/test_redis_integration.py`
- **Read first:** `simplebroker/db.py` lines 135–155 and 4070–4080 (`_ALIAS_SHADOW_WARNING_SUPPRESSED`, `_AliasShadowsQueueWarning`), `simplebroker/commands.py` lines 270–295 (`cmd_alias_add`), `extensions/simplebroker_redis/simplebroker_redis/plugin.py` lines 140–325, `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py`, `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py`.
- **Steps:**
  1. In `extensions/simplebroker_redis/simplebroker_redis/core.py`, import `_ALIAS_SHADOW_WARNING_SUPPRESSED` and `_AliasShadowsQueueWarning` from `simplebroker.db`. In `RedisBrokerCore.add_alias` (L2096–2105), change `if shadows_existing_queue:` to `if shadows_existing_queue and not _ALIAS_SHADOW_WARNING_SUPPRESSED.get():` and change `RuntimeWarning` to `_AliasShadowsQueueWarning` (which subclasses `RuntimeWarning`, preserving `issubclass(..., RuntimeWarning)` compatibility).
  2. In `extensions/simplebroker_redis/simplebroker_redis/plugin.py`:
     - Add `self._any_condition = threading.Condition(self._lock)` to `_SharedRedisActivityListener.__init__`.
     - Notify `self._any_condition.notify_all()` under `self._lock` in `_SharedRedisActivityListener._run` whenever a queue version increments (L198) or `self._error` is recorded (L205), and in `close()` (L264).
     - Add `wait_many(self, registrations: Sequence[_QueueWaiterRegistration], timeout: float, *, stop_event: Any) -> bool` on `_SharedRedisActivityListener` that acquires `self._lock`, checks `self._error` and all `registrations` in order (advancing the first changed registration's version with the exact L248–253 logic and returning `True`), and otherwise waits on `self._any_condition.wait(min(0.05, remaining))` until `deadline`, `stop_event`, or `self._stop_event`.
     - In `RedisMultiQueueActivityWaiter.wait(self, timeout: float) -> bool`:
       - Take the fast path only when `not self._closed` and there is at least one open child. Filter `open_waiters = [w for w in self._waiters if not getattr(w, "_closed", False)]` for fast-path eligibility; closed/empty cases retain the existing loop and bounded sleep. Do not define a new wait-after-close policy.
       - Check whether all `w` in `open_waiters` satisfy `isinstance(w, RedisActivityWaiter) and isinstance(w._listener, _SharedRedisActivityListener) and w._stop_event is self._stop_event` and share the same `w._listener`. When true, delegate directly to `first_listener.wait_many([w._registration for w in open_waiters], timeout, stop_event=self._stop_event)`. Otherwise, retain the existing fallback loop for stub/test-double listeners.
  3. Add tests in `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py` and `extensions/simplebroker_redis/tests/test_redis_integration.py` verifying:
     - `cmd_alias_add(..., quiet=True)` on Redis emits zero warnings when shadowing an existing queue with messages, while `quiet=False` emits `_AliasShadowsQueueWarning`.
     - `RedisMultiQueueActivityWaiter.wait()` uses the real listener lock/condition: observe entry into `_any_condition.wait`, then publish activity and record the resulting notify/return order. Wrap only the instance `_any_condition.wait` call: publish readiness on entry, then delegate to the real condition with a long bounded timeout in place of the product's 50ms stop-check timeout. Record whether the real wait returned by notification rather than timeout. Thus a missing notify cannot pass by polling, while the listener lock, notification and version state remain real. Prove first-changed registration/version behavior, error notification, listener close and external-stop behavior, closed/empty bounded fallback and open-child fast-path eligibility, unrelated-queue isolation and retained mixed-listener fallback. Keep any timeout a bounded liveness valve, not a sub-50ms scheduler assertion. Retain real Redis multi-queue write-to-wake integration and existing state-machine gates. External stop has no notify hook, so the bounded 50ms condition timeout remains intentional for stop checks.
- **Stop-and-re-evaluate gate:** Stop if `tests/test_state_machine_policy.py` or `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py` reports any transition mismatch on `SM-ACTIVITY-WAITER` or `SM-REDIS-ACTIVITY-LISTENER`.
- **Per-task verification:**
  - `uv run pytest tests/test_state_machine_policy.py -q`; `uv run bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py extensions/simplebroker_redis/tests/test_redis_integration.py -q`

### Task 5: Standalone `examples/sqlite_connect.py` Fork Safety & `examples/async_wrapper.py` Loop Fixes

- **Outcome:** `examples/sqlite_connect.py` (`SQLiteConnectionManager`) is fork-safe when parent threads hold `_setup_lock` or `_connections_lock` across `os.fork()`, does not close inherited SQLite connections or unlink parent setup files in a forked child, and `examples/async_wrapper.py` uses `asyncio.get_running_loop()`.
- **Files to touch:**
  - `examples/sqlite_connect.py` (lines 550–705, 860–902)
  - `examples/test_sqlite_connect.py`
  - `examples/async_wrapper.py` (lines 60–65, 248–252, 344–351)
- **Read first:** `examples/sqlite_connect.py` lines 530–902 (`SQLiteConnectionManager`), `examples/test_sqlite_connect.py` lines 550–620, `docs/plans/2026-10-06-test-audit-remediation-plan.md` lines 265–280 (`T9`), `simplebroker/_runner.py` lines 60 and 297–303 (`_ABANDONED_FORK_CONNECTIONS` pattern).
- **Steps:**
  1. In `examples/sqlite_connect.py`:
     - Introduce `_check_fork(self) -> None`: read `current_pid = os.getpid()` and invoke `_handle_fork(current_pid)` if it differs from `self._pid`. Replace the existing inline PID check at the start of `get_connection()` with this helper. This is a new helper, not an existing method.
     - Add module-level `_ABANDONED_FORK_CONNECTIONS: list[sqlite3.Connection] = []`.
     - In `_handle_fork(self, current_pid: int)` (L599–614):
       - Do **not** acquire inherited `self._setup_lock` or `self._connections_lock`, and do **not** call `.close()` on inherited `sqlite3.Connection` objects.
       - Snapshot any existing connections in `list(self._all_connections)` and `getattr(self._thread_local, "conn", None)` into `_ABANDONED_FORK_CONNECTIONS`.
       - Replace `self._setup_lock = threading.Lock()`, `self._connections_lock = threading.Lock()`, `self._all_connections = set()`, `self._thread_local = threading.local()`, `self._completed_phases = set()`, `self._created_files = set()`, and set `self._pid = current_pid`.
     - Call the new `self._check_fork()` at the entry of `setup(phase)` (currently L659) and `close()` (currently L864), before accessing phase state or inherited locks, as well as `get_connection()` (currently L577).
     - In `__del__(self)`, when PID differs, retain tracked inherited connections in `_ABANDONED_FORK_CONNECTIONS` before returning without close/unlink. Use `self.__dict__.get("_all_connections", ())` so partially constructed instances stay safe. Prove actual manager collection, not just manual invocation while strong references still keep it alive.
  2. In `examples/test_sqlite_connect.py`:
     - Update `test_fork_safety_simulation` (L562–583) so that after verifying `conn1 is not conn2` under in-process PID mutation, teardown explicitly closes `conn1` and removes it from `_ABANDONED_FORK_CONNECTIONS` so no SQLite handle leaks in the test process.
     - Add a real `os.fork()` regression test (skipped when `not hasattr(os, "fork")`) where a parent thread holds `_setup_lock` and `_connections_lock` while a live parent SQLite connection has an active transaction and `.setup.status` files exist; parameterize child entry through unfinished `setup(SetupPhase.OPTIMIZATION)`, `get_connection()`, and `close()`. Use a separate inherited manager for actual collection before recovery, releasing the child’s copied fixture reference and other strong references. Verify collection retains its real connection without close or unlink. Precreate the parent connection-phase status marker and hold a real parent write transaction; child setup must not run connection setup SQL against that active transaction. Apply the shared fork-probe harness rules below and verify:
       - the child completes within a tight timeout without deadlocking on the held parent locks,
       - the parent's `.setup.status` files and active SQLite connection remain intact and usable after the child exits.
  3. In `examples/async_wrapper.py`, replace `asyncio.get_event_loop()` with `asyncio.get_running_loop()` at lines 61, 249, and 344.
- **Stop-and-re-evaluate gate:** Stop if `examples/sqlite_connect.py` imports any internal `simplebroker` module (it is a standalone zero-dependency copy-paste file and must use only Python stdlib).
- **Per-task verification:**
  - `uv run pytest examples/test_sqlite_connect.py tests/test_sqlite_connect_example.py examples/tests/test_recommended_python_examples.py -q`

## Testing Plan and Anti-Mocking Guidance

- **What must stay real (no mocks):**
  - Real `sqlite3` connections and real `SQLiteRunner` instances for `BaseException` transaction-interruption tests (`tests/test_runner_error_handling.py` and `tests/test_sqlite_schema.py`). Fault injection may subclass or wrap only a transaction-body `run()` call, armed after setup, and raise `KeyboardInterrupt` after BEGIN has returned successfully. Never raise after `super().begin_immediate()` from within an overridden BEGIN method. Keep BEGIN, rollback, ownership admission, and SQLite transaction state real; secondary driver rollback-failure injection follows the existing runner invalidation test seam.
  - Real `os.fork()` child processes (on POSIX) for `_phaselock.py`, SQL `BrokerCore` alias/meta fork rejection, and `examples/sqlite_connect.py` held-lock fork recovery. Never rely solely on monkeypatching `os.getpid()` when testing lock-held deadlock recovery or shared file-descriptor close avoidance.
  - Real `_SharedRedisActivityListener` threading/condition primitives for `RedisMultiQueueActivityWaiter` wakeup latency tests, plus real Redis integration for alias quiet parity and multi-queue activity. Real PostgreSQL bootstrap/migration interruption proof must retain the pool and checkout lifecycle. A missing server can produce an explicit local skip, but a skipped backend proof does not satisfy the final implementation gate; run `uv run bin/pytest-pg` and `uv run bin/pytest-redis` (Docker-managed services) before claiming integration readiness.
- **Contracts protected by tests:**
  - `SQLiteRunner` transaction ownership release and SQLite transaction rollback under `KeyboardInterrupt` across every changed handler in the Task 2 inventory, plus no bogus `rollback()` when `begin_immediate()` itself fails.
  - Child-process liveness and parent-file/lock preservation across `os.fork()` in `_phaselock.py`, SQL `BrokerCore` and `examples/sqlite_connect.py`.
  - `_AliasShadowsQueueWarning` emission and `--quiet` suppression parity on Redis.
  - Condition-driven multi-queue wakeup in `RedisMultiQueueActivityWaiter` proved by owned notification/return ordering without breaking `SM-ACTIVITY-WAITER` or `SM-REDIS-ACTIVITY-LISTENER`.

### Shared Fork-Probe Harness Rules

- Use the precise call-local threaded-fork `DeprecationWarning` filter already demonstrated in `tests/test_phaselock_transition_tables.py`: inside `warnings.catch_warnings()`, ignore only the message matching `^This process \(pid=\d+\) is multi-threaded, use of fork\(\) may lead to deadlocks in the child\.$`, call `os.fork()`, then exit the warning context before branching in either process. Python 3.15 may otherwise propagate warnings-as-errors after creating the child. Keep every other warning strict; do not add a global or module-wide filter.
- Publish readiness from the actual lock-owning thread and observe it before fork. Bound the entire child report/reap phase; close pipe ends, kill/reap unfinished children, release parent holder events and join threads in `finally`, including on assertion or warning failure. Verify `os.WIFEXITED(status)` before `os.WEXITSTATUS(status)`. Reuse `_wait_for_fork_child` in `tests/test_fork_safety.py` and the bounded result/cleanup pattern in `extensions/simplebroker_redis/tests/test_redis_pool.py` where applicable.
- Use `os._exit()` in manually forked probe children after explicit child-owned cleanup; do not run parent-owned finalizers or coverage cleanup accidentally. Follow the existing harness's coverage policy for each topology. New fork tests run under the standard harness concurrency and strict warnings, including Python 3.15; no worker cap or serial-only acceptance substitute.

## Verification and Gates

### Per-Task Gates
- Task 1: `bin/check-doc-paths && bin/check-plan-context && python3 bin/check-dom15-fixtures && uv run pytest tests/test_ruff_policy.py tests/test_python_library_api_contract_sb_api.py -q`
- Task 2: `uv run pytest tests/test_runner_error_handling.py tests/test_transaction_error_propagation.py tests/test_sqlite_schema.py -q`; `uv run bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py extensions/simplebroker_pg/tests/test_pg_integration.py -q`
- Task 3: `uv run pytest tests/test_phaselock.py tests/test_phaselock_transition_tables.py tests/test_fork_safety.py -q`
- Task 4: `uv run pytest tests/test_state_machine_policy.py -q`; `uv run bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py extensions/simplebroker_redis/tests/test_redis_integration.py -q`
- Task 5: `uv run pytest examples/test_sqlite_connect.py tests/test_sqlite_connect_example.py examples/tests/test_recommended_python_examples.py -q`

### Final Gates Before Claiming Completion
1. Document and policy gates:
   - `bin/check-doc-paths`
   - `bin/check-plan-context`
   - `python3 bin/check-dom15-fixtures`
   - `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`
   - `uv run ruff check .`
   - `uv run ruff format --check .`
2. Test suites:
   - `uv run pytest tests/test_ruff_policy.py tests/test_state_machine_policy.py tests/test_runner_error_handling.py tests/test_transaction_error_propagation.py tests/test_sqlite_schema.py tests/test_phaselock.py tests/test_phaselock_transition_tables.py tests/test_fork_safety.py examples/test_sqlite_connect.py tests/test_sqlite_connect_example.py examples/tests/test_recommended_python_examples.py -q`
   - `uv run bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py extensions/simplebroker_pg/tests/test_pg_integration.py -q`
   - `uv run bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py extensions/simplebroker_redis/tests/test_redis_integration.py -q`
   - Full core + extension + example suites: `uv run pytest`, `uv run bin/pytest-pg`, `uv run bin/pytest-redis`, `uv run pytest examples/test_sqlite_connect.py examples/tests -q`. Backend wrappers provision Docker services and set the required DSN/URL; Docker absence is an explicit blocker to those implementation gates, not an accepted skip.
3. Independent review of the completed implementation per `[DOM-11]`, followed by updating `docs/plans/README.md` to `completed` only when all gates pass and the user authorizes completion.

## Independent Review Loop

- Author family: Gemini
- Readiness revision author family: Codex.
- Reviewer family: Claude (the actual cross-family reviewer for this revision).
- Review invocation: `skills/call-agent/SKILL.md` (`claude -p "$PROMPT" --permission-mode plan --allowedTools "Read,Grep,Glob"`, 540s bound)
- **Prior same-family pass:** Subagent `b541e59e-227b-4583-b5c4-c58e97f4d53d` (`invoke_subagent` `research`, same model family as author); The actual cross-family reviews and dispositions for this revision are recorded below; the historical same-family pass is not the readiness gate.
- **Files to review:**
  - `docs/plans/2026-10-14-deep-review-remediation-plan.md` (including `## Proposed Spec Delta` and `## Revision Log`)
  - `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1]
  - `docs/specs/16-python-library-api.md` [SB-API-3]
  - `simplebroker/db.py`, `simplebroker/_phaselock.py`, `simplebroker/_backends/sqlite/maintenance.py`, `simplebroker/_backends/sqlite/schema.py`
  - `extensions/simplebroker_pg/simplebroker_pg/schema.py`, `extensions/simplebroker_redis/simplebroker_redis/core.py`, `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
  - `examples/sqlite_connect.py`, `examples/async_wrapper.py`

## Execution Log

Comprehension-gate answers and the Strategy D promotion baseline are recorded below. Append actual slice evidence before each completion claim.

| Slice | Comprehension answers / promotion baseline | Verification and observed result | Implementation commit |
| --- | --- | --- | --- |
| Preflight / comprehension | Class 5 inherited from this plan; approved Strategy D baseline `1b42554dfc0313c1837c23b0c7a5761f07627263`. Q1: successful BEGIN owns the transaction; body BaseException must trigger rollback without masking the primary failure. Failed same-thread nested BEGIN must not roll back the outer transaction; preserve retry wrappers and resync translation. Q2: retain inherited SQLite handles and replace child guards before touching locks, avoiding unsafe inherited cleanup/finalization. Q3: queue-version checks and the shared condition use the same listener RLock; notify under that lock prevents lost wakeups, while bounded waits retain external-stop responsiveness. | Pre-change spec alignment inspection failed on obsolete Ruff-empty-ignore and Queue.conn 8.3 wording. Existing unrelated guidance edits preserved; no storage/API signature change or coalescing work. | Execution evidence follows each slice. |
| Task 1 / Strategy D promotion | Worktree against `1b42554dfc0313c1837c23b0c7a5761f07627263`; spec-diff SHA-256 `be6b2f23073ea5caadd295929df14d880735f8475b5b1656127e59c6757c9001`. Both exact deltas applied with Related Plans backlinks before runtime fixes. | Spec inspection red before and green after; document paths, plan context, DOM-15 fixtures pass; Ruff/Python-API policy selection 37 passed at standard auto concurrency. Cross-family slice review dispatched. | Verified changes retained for integration review. |

## Out of Scope

- **[DOM-14] Coalescing sweep:** Explicitly excluded by the repository owner ("except for the coalescing, that will happen separately"). Do not modify `docs/coalescing.md` or retire/delete `retired-pending` plans in this unit of work.
- **Redis alias/meta/pending fork guards:** The original finding did not survive trace: `_client` delegates to `RedisRunner.client`, which already checks PID and recovers child-owned resources; these entry points do not use core locks. Do not add redundant guards or a non-firing held-core-lock regression. Existing Redis fork-policy tests remain regression coverage.
- **Removing `Queue.conn`:** `Queue.conn` remains deprecated and readable through the `8.x` line; removing it is a future major-version change.
- **Refactoring C901-suppressed functions or splitting `simplebroker/db.py` / `simplebroker/watcher.py`:** Explicitly protected by `[DOM-10.1.1]` and `docs/implementation/07-complexity-and-state-machine-map.md`.
- **Adding a partial index on `claimed = 1`:** Explicitly rejected in `docs/implementation/09-storage-schema-and-claim-lifecycle.md` (`[ALT-IMPL09-001]`).
- **`examples/async_pooled_broker.py` `AsyncSQLiteRunner` exception handlers:** `AsyncSQLiteRunner` serializes via `asyncio.Lock` rather than thread-owned `_transaction_owner`; out of scope for Task 2's `SQLiteRunner`/`PostgresRunner` transaction-ownership slice.

## Review Log

- **2026-10-14 — Independent plan review (`b541e59e-227b-4583-b5c4-c58e97f4d53d`):**
  - *Blocker 1 (`[DOM-10.1]` Delta 1 glob list vs `pyproject.toml` and `tests/test_ruff_policy.py`):* Accepted and fixed. Updated Exact Delta 1 to list the exact 7 globs in `pyproject.toml:158-167` (`".github/**"`, `"benchmarks/**"`, `"bin/**"`, `"examples/**"`, `"extensions/**"`, `"fuzz/**"`, `"tests/**"`) and describe them accurately as non-`simplebroker/` trees rather than `"non-runtime trees"`.
  - *Blocker 2 (Four nonexistent test file paths in verification commands):* Accepted and fixed. Replaced `tests/test_runner_transaction_ownership.py` with existing `tests/test_runner_error_handling.py` and `tests/test_transaction_error_propagation.py`; replaced `tests/test_schema_migration.py` with `tests/test_sqlite_schema.py`; replaced `examples/test_async_wrapper.py` and `tests/test_examples.py` with `examples/tests/test_recommended_python_examples.py` and `tests/test_sqlite_connect_example.py`.
  - *Blocker 3 (`simplebroker/db.py` `_resync_timestamp_generator` at L3178–3220 misidentified as `list_queues`, preserving `RuntimeError` translation, and removing inaccurate `except IntegrityError` note):* Accepted and fixed. Named `_resync_timestamp_generator` (L3178–3220) explicitly, specified nested `try:` after `begin_immediate()` so `BaseException` rolls back while `Exception` still translates to `RuntimeError`, removed the inaccurate `IntegrityError` parenthetical on `_do_write_transaction` / `_do_insert_messages_transaction`, and corrected all `simplebroker/db.py` line ranges.
  - *Blocker 4 (`simplebroker/_backends/sqlite/schema.py` `initialize_database` L137 and `ensure_schema_v6` L501–548, plus `import contextlib` in `extensions/simplebroker_pg/simplebroker_pg/schema.py`):* Accepted and fixed. Added `initialize_database` (L137 `contextlib.suppress(BaseException)`) and `ensure_schema_v6` (L501–548 placing `runner.begin_immediate()` before the inner rollback `try:` inside the outer pragma `try:...finally:`), and noted `import contextlib` in `extensions/simplebroker_pg/simplebroker_pg/schema.py`.
  - *Blocker 5 (`examples/sqlite_connect.py` class/method/attribute names and `test_fork_safety_simulation` teardown in `examples/test_sqlite_connect.py`):* Accepted and fixed. Corrected class name to `SQLiteConnectionManager` (L530–902), methods to `setup_phase()` (L578), `get_connection()` (L618), `close()` (L876), and `__del__()` (L898), attribute to `self._completed_phases = set()`, reference file to `simplebroker/_runner.py:60,297-303`, and added explicit `conn1` close + `_ABANDONED_FORK_CONNECTIONS` cleanup for `test_fork_safety_simulation` in `examples/test_sqlite_connect.py`.
  - *Blocker 6 (Comprehension Gate 1 line numbers and `OperationalError` vs `RuntimeError`):* Accepted and fixed. Updated `simplebroker/_runner.py` citations to L475–729 and `extensions/simplebroker_pg/simplebroker_pg/runner.py` citations to L987–1062, and corrected the foreign-thread exception type to `OperationalError`.
  - *Non-blocking improvements 1–5:* Accepted and incorporated (added `2026-10-06-whole-test-surface-audit-plan.md` L4 and `docs/plans/README.md` L36–37 stale `uncommitted` cleanup to Task 1; added `isinstance(w._listener, _SharedRedisActivityListener)` and `open_waiters` guard in Task 4; recorded `examples/async_pooled_broker.py` in Out of Scope).

## Revision Log

- **2026-10-14 — Owner-requested readiness corrections.** Corrected winning CLI/delivery spec paths and replaced phantom `[SB-CLI-9]` with `[SB-CLI-2]`; added residual-operation ownership. Corrected Task 5 to the actual `setup(phase)` entry point and explicitly introduced the shared PID-check helper. Separated inherited SQL-core rejection from Redis recovery. Restricted fault injection to the post-BEGIN transaction body, retained retry wrappers and specialized cleanup policies, added the changed-handler inventory and PostgreSQL/Redis real-backend proof owners, and required secondary rollback-failure proof. Added precise Python 3.15 fork-warning and bounded child-cleanup guidance. Replaced scheduler-speed assertions with causal Redis notification proof. The prior Review Log's method-name and readiness claims are superseded by this revision; independent review of the revised text is required before implementation.

### 2026-10-14 Cross-Family Review Dispositions

Claude read-only whole-plan review: exit 0, 373.7 seconds under a 540-second bound; verdict `BLOCKED`. Findings were checked against current code and Git objects before these changes.

| Finding | Disposition | Plan correction |
| --- | --- | --- |
| P2-1 Wrong evidence identifiers and nonexistent replacement text | Accepted | Real HEAD `1b42554dfc0313c1837c23b0c7a5761f07627263`; verified ancestor `208c3a8`; actual wrapped Queue.conn sentence and timestamp-conflict comment. |
| P2-2 Redis core fork guards and non-firing proof | Accepted; remove unnecessary work | Remove Redis guard implementation/probe, document `_client` -> recovering runner and absent core-lock use in the no-action boundary. |
| P2-3 Closed/empty waiter behavior treated as preserved | Accepted | Optimize only live compatible waiters; preserve closed/empty bounded fallback and `[SB-API-6]`'s undefined wait-after-close boundary. |
| P2-4 Failed-BEGIN proof and nonexistent ownership symbol | Accepted | Use `_admit_terminal_operation`, existing same-thread outer transaction and passthrough rollback counter at relocated-BEGIN sites. |
| P2-5 Backend gates could silently skip | Accepted | Name Docker-backed `bin/pytest-pg` / `bin/pytest-redis`, include Redis integration tests and prohibit skipped real-backend completion claims. |
| P2-6 Missing user-visible documentation alignment | Accepted | Add `[Unreleased]` changelog and Python-guide compatibility wording to Task 1; reconcile documentation with actual built slices. |

Other review observations addressed locally: add the Execution Log target, clarify ignore-policy delta, name the real condition-wrapper seam and already-safe PostgreSQL vacuum site, and declare the Claude cross-family review path. Concurrent guidance/check-plan-context edits remain their owner's work; verify against both the trusted HEAD gate and the current working-tree gate.

<details>
<summary>Claude whole-plan review findings (verbatim; dispositions above)</summary>

**[P2-1] Wrong evidence identifiers in Task 1 and the baseline.**
- Plan line 60 gives a SHA that doesn't exist.
- Task 1 step 5 says the test-audit work was "committed (`a8f0c82`)". That revision is unknown to git. The plan files actually landed in `208c3a8` ("Test cleanup"), which is an ancestor of `6923276`, so CI run `38458241452` (cited in `2026-10-13-ruff-default-drift-plan.md:91`) is valid evidence.
- Step 4 quotes a `db.py:2048-2050` comment about "rowcount" that doesn't exist. The real stale text is at `db.py:2047-2050` ("only unique constraints are the PK (auto-assigned) and the ts column").
- Step 2 quotes `Readable through the 8.3 release line`. The real docstring is at `sbqueue.py:234-235` and is wrapped across two lines.
- **Fix:** use the real SHA, `208c3a8`, and the exact current text. The replacement comment should keep the point that any IntegrityError here is a timestamp conflict.

**[P2-2] The Redis half of Task 3 rests on a false premise, and its proof can't fail.**
- `RedisBrokerCore._client` is `self._runner.client` (`core.py:247`), and that already calls `runner._check_fork()` (`redis runner.py:63-64`).
- None of the targeted alias, meta or pending methods (`core.py:1882-1904`, `2031-2131`) acquire `self._lock` or `_write_lock`.
- So "hold the parent's core lock across fork" exercises nothing. The test would pass before and after the change, or only fail on implementation-coupled `_pid`/`_lock` assertions.
- The plan's claim that `queue_exists()` is "guarded" is false: `core.py:1328-1333` has no `_check_fork_safety()`.
- **Fix:** remove Task 3 steps 3 and 5 and the Redis wording from Goal 2 and the Outcome Checklist. Keep the SQL `BrokerCore` guards and their real-fork rejection test, which is a genuine red→green deadlock fix.

**[P2-3] Task 4 adds new waiter behaviour but calls it a preserved contract.**
- Today `RedisMultiQueueActivityWaiter.wait()` (`plugin.py:307-317`) never returns early when closed or empty. It sleeps until timeout.
- `[SB-API-6]` explicitly "does not define `wait()` behavior after close", and `SM-ACTIVITY-WAITER` (`07-…map.md:391`) governs cleanup only.
- An immediate `False` makes `watcher.py:1436-1452` `continue` without sleeping. That's a latent hot loop for any caller that waits after close, which `[SB-API-6]` leaves undefined (the in-tree watcher serializes and won't hit it).
- **Fix:** keep the closed/empty cases on the existing bounded-sleep fallback, so `wait_many` is the only new path. Drop the "preserve: must return False immediately…" invariant.

**[P2-4] The failed-BEGIN proof can't tell a correct change from an incorrect one, and its rationale cites a nonexistent symbol.**
- `_require_transaction_owner_or_unowned` doesn't exist. The real gate is `_admit_terminal_operation` (`_runner.py:621-648`), which already rejects a foreign-thread rollback before touching the connection.
- So with real foreign ownership, a stray caller `rollback()` would be rejected, then suppressed. "Owner can still complete" passes whether or not BEGIN is inside the `try:`.
- Every `db.py` site except `_resync_timestamp_generator` already has BEGIN before the `try:` (`maintenance.py:210` too, despite the Context row saying otherwise).
- Only `_resync`, `ensure_schema_v4/v5/v6` and PG actually move.
- The real hazard is a same-thread re-entrant BEGIN failing, which would then roll back the outer transaction.
- **Fix:** limit the proof to the sites that move and assert with a passthrough rollback call counter, or drop the proof. Correct the rationale in Comprehension Gate Q1.

**[P2-5] The backend gates, as written, skip their real-backend tests.**
- `uv run pytest extensions/simplebroker_pg/...` skips without `SIMPLEBROKER_PG_TEST_DSN` (`pg conftest.py:21,33`), and Redis behaves the same way. The plan forbids accepting skips but never names the harness.
- **Fix:** use `uv run bin/pytest-pg <paths>` and `uv run bin/pytest-redis <paths>` in the Task 2, 3 and 4 gates and the final gates.
- Also add `extensions/simplebroker_redis/tests/test_redis_integration.py` to the Task 4 gate and the final list, since Task 4 adds tests there.

**[P2-6] Missing doc and changelog alignment for user-visible changes.**
- The plan has no `CHANGELOG.md` `[Unreleased]` step. Redis `--quiet` alias suppression, the warning class change and interrupt rollback are user-visible, and the project rules require recording them.
- `docs/guides/python.md:1116` still says "8.3.x".
- **Fix:** add both to Task 1.

</details>

### 2026-10-14 Scoped Cross-Family Verification

Claude read-only follow-up: exit 0, 96.1 seconds under the same 540-second bound. Verbatim verdict: **“READY. All six accepted findings and the listed clarifications PASS.”** The reviewer confirmed implementability without weakening robustness, with no new P1/P2 finding.

- New P3 comment wording: accepted. Task 1 now replaces the entire timestamp-conflict comment block, avoiding duplicated tail text and SQLite-only INTEGER terminology. The proposed block refers to `ts` uniqueness across SQL backends.
- Existing full-bullet replacement ambiguity: clarified that Strategy D replaces the entire wrapped `Queue.conn` bullet, not just its first source line.
- PostgreSQL failed-BEGIN test observation: retained as a small regression boundary with the other schema handler cases; the required failing proof of BEGIN relocation is explicitly limited to the four SQL sites that move. It is not cited as a pre-change PostgreSQL defect.

Planning verification: current-tree `bin/check-doc-paths`, `bin/check-plan-context`, and `python3 bin/check-dom15-fixtures` pass; the trusted HEAD version of `bin/check-plan-context` also passes against this plan tree. The doc/plan-context pytest selection passed 9 tests at standard auto concurrency. Actual source paths/section codes, named gate paths and Git evidence objects were checked. Implementation remains governed by the ordered tasks, comprehension gate and real-backend completion gates. The existing existence-check/readiness guidance covers the discovered failure modes; no additional skill or durable guidance change is needed in this unit.

### Implementation Slice Review: Task 1

Claude read-only review exited 0 in 57.8 seconds under a 540-second bound:
**no blocker**. F1/P3 accepted: distinguish Ruff's rule-code/scope enforcement
from the current configured path list. F2/P3 accepted: guide wording states no
runtime warning as of 8.7, without creating an extra future-warning contract.
These clarification refinements preserve Strategy D and the approved 8.x
Queue.conn compatibility statement; no policy or public signature changed.

### Task 2 Verification Evidence

The pre-fix control restored exact HEAD production bytes only for the owned
SQL files, then restored the saved candidate bytes in `finally`: 23 intended
SQLite failures and 6 intended PostgreSQL failures. Already-safe boundaries
passed the control. Candidate standard-auto gates: 106 core/SQLite tests and
64 Docker PostgreSQL 18 tests passed without skips. Owned Ruff/format checks,
suppression registry and strict production / CI-cohort test typing passed.
Cross-family SQL slice review is dispatched; full integration gates follow.

### Task 3 Fault-Probe Refinement

The initial held-operation-lock probe exposed both phase-lock failures and
seven SQL alias/meta deadlocks. `add_alias` already rejected indirectly through
its later pending-message query, but its earlier mutation check takes the
poison mutex. Holding that mutex alongside the operation lock made all eight
entry probes fail before the explicit entry guards (8 child liveness failures).
This expands the held-lock fixture to an existing hidden coupling, not a public
contract change or recovery policy. The two process-registry cases separately
failed on inherited per-path lock and inherited registry guard.

### Implementation Slice Review: Task 2

Claude read-only review exited 0 in 120.0 seconds under a 540-second bound:
**no blocker**. F1/P3 accepted: removed three duplicate existing foreign-key
pragma assertions introduced during test preparation. F2/P3 was the already
reviewed Task 1 timestamp comment, preserved in the shared db.py file. The
reviewer confirmed all changed handlers have firing coverage, retry and resync
translation remain intact, and real PostgreSQL rollback releases checkouts.
Existing diagnostic-note and alias-cache observations are outside this slice
and do not change its approved error-priority boundary.

### Task 3 Verification Evidence

Before correction both process-lock cases failed; the expanded held-operation
and poison-mutex fixture produced 8 intended SQL entry-point liveness failures.
After child lock-registry replacement and 8 entry guards, the standard-auto
phase-lock, transition-table, fork and state-machine-policy selection passed
121 tests with one expected Windows-only msvcrt skip. No mock replaces fork,
real lock acquisition, SQL connections, or child Queue operation.

### Implementation Slice Review: Task 3

Claude read-only review exited 0 in 73.7 seconds under a 540-second bound:
**no blocker**. F1/P3 accepted: distinguish a missing SQL rejection from failure
of the separately constructed child Queue in probe exit diagnostics. F2/P3
accepted without rename: keep the private callback name specified by the plan;
its body/docstring explicitly name all three reset owners. F3/P3 accepted:
retain the local bounded reap loop rather than adding a shared test framework.
The pre-existing inherited direct-core close/shutdown and inherited advisory
release hazards are outside these entry guards; no lifecycle recovery claim is
made for either path.

### Task 4 Verification Evidence

Five intended pre-fix failures established warning-category/quiet mismatch and
absence of condition-driven composite activity/error/close wakeups. Candidate
Docker-managed Redis targeted selection passed 144 tests at standard auto
concurrency; state-machine policy passed 13 tests. Real condition/lock/version
machinery and real Redis command/write-to-wake paths were retained. Only Pub/Sub
transport is scripted for causal notification fault proof. Owned Ruff/format
checks passed, with no new suppression or test-only production seam.

### Downstream Inspection

Weft's `weft/core/queue_wait.py` and multi-queue watcher use the same activity
waiter factory and serialized wait/close lifecycle. `weft/commands/queue.py`
uses unchanged broker alias signatures, and submission uses unchanged session
connection capabilities. No downstream API migration is required; this is a
source-usage inspection, not a claim that the downstream test suite ran.

### Implementation Slice Review: Task 4

Claude read-only review exited 0 in 123.7 seconds under a 540-second bound:
**no blocker**. It confirmed atomic version-check/wait/notify under one lock,
warning subtype/filter compatibility, registration order, credit consumption,
error/stop priority and unchanged closed/empty/mixed compatibility fallback.
O1/P3 is the intended internal listener-stop result: a fast-path composite
returns false promptly like scalar waiting; registry references prevent ordinary
listener closure while children remain open. This is not a new public
wait-after-close guarantee. O2/P3 concerns concurrent per-child close during
wait, outside [SB-API-6]'s serialized ownership contract.
O3/P3 accepted: replaced the weak pre-wait unrelated-activity assertion with a
causal two-condition-entry probe. A focused owner mutation returning true on
any condition notification failed at the required rescan witness; candidate
bytes were restored exactly. O4/P3 accepted: the stopped-child test explicitly
identifies its distinct-stop-event compatibility fallback. Updated Redis gate:
145 passed on Docker Valkey; extension typing passed 13 files. No runtime
change was needed for these review observations.

### Task 5 Verification Evidence

All three real-fork entry cases failed before correction: inherited get/close
locks reached the bounded child deadline, while setup exposed deletion of a
parent marker by an unmigrated finalizer. After lock/handle/created-file reset,
PID entry guards and the initial cross-PID finalizer return, the required standard-auto
example selection passed 52 tests. Release-style explicit typing passed 16
example files; changed-file Ruff/format and stdlib-only import inspection
passed. The subsequent independent review and finalizer refinement are recorded below. Inherited handles
are checked only by retained identity and `in_transaction`; SQL always uses
child-owned connections, with parent transaction usability checked after reap.

### Implementation Slice Review: Task 5

Claude read-only review exited 0 in 122.5 seconds under a 540-second bound:
**no blocker**, with F1/P2 identifying a remaining gap in the accepted inherited
handle rule. Accepted: cross-PID destructor must retain tracked connections
before object collection drops manager fields; a manual destructor call with
strong references is insufficient proof. F2/P3 accepted: exercise an unfinished
optimization phase so the setup probe reaches the real inherited setup mutex.
F3/P3 accepted: capture the example's own SQLiteConnectError family in child
probe diagnostics instead of adding a broad-exception suppression. The
existing autouse manager fixture owns parent cleanup, which the reviewer did
not observe; collection tests must deliberately release the child's copied
fixture reference to the victim manager while leaving parent teardown intact.
The deviation row declares this finalizer refinement before source changes.

### Task 5 Finalizer Refinement Evidence

Cross-PID `__del__` now retains tracked inherited connections before return.
The three real-fork probes remove the victim from the child's copied fixture
ownership list, drop manager and connection references, and invoke GC. Weakrefs
prove manager collection and connection survival; the observable connection is
a real SQLite driver subclass created through its public factory argument.
All three cases fail against the prior destructor and against an omitted-retention
mutation. Removing the setup PID guard fails at the bounded five-second held-lock
deadline on unfinished OPTIMIZATION. Exact candidate bytes were restored in
`finally` after each control. Candidate required selection: 52 passed at standard
auto concurrency. Ruff/format, explicit typing of 16 example files, stdlib-only
import inspection and diff whitespace checks pass.

### Final Integration Gate Dispatch

Source and tests frozen after all five implementation slices and review fixes.
Full core, Docker PostgreSQL, Docker Valkey, example suites and Python 3.15 fork
qualification run sequentially at standard auto concurrency. `UV_NO_SYNC=1`
keeps the locked, combined dev/pg/redis environment intact for backend wrappers.
An integrated read-only Claude review of the explicit owned delta is in flight
under the same 540-second bound; unrelated guidance edits are excluded.

### Black-Box CLI Acceptance

Public broker subprocess probes pass success/empty/error exit classes (0/2/1),
alias resolution versus literal queue preservation, invalid alias diagnostics,
malformed JSONL rejection without traceback, and exact empty stdout/stderr for
`alias add ... --quiet`. Initial global-position quiet probe exposed a
pre-existing parser issue: alias-add's false default overrides root `--quiet`
when placed before `alias`. Both parser definitions are unchanged by this delta;
CLI parsing repair is an out-of-scope observation, not covered by the command-layer
warning parity claim. No new CLI grammar was introduced.

### Documentation and Static Integration Checks

Current and trusted HEAD plan-context checks, document-path checks and DOM-15
fixtures pass. Whole-repository Ruff and format gates pass (452 files); the
symbol-keyed suppression registry passes with no additions. Strict production
typing passes 66 source files; changed core-test typing passes five files,
PostgreSQL production plus changed tests passes 13 files, Redis production
plus changed tests passes 13 files, and explicit example typing passes 16 files.
The Task 1 clarification after slice review is included in final Strategy D
spec-diff SHA-256 `f0f12d97443ffd178ea96ca506f603e12c9112d10456a688b6b24bba997be811` against the declared baseline.

Heavily used skill evaluation: `skills/call-agent/SKILL.md` supplied bounded,
read-only cross-family invocation and adequate scope/disposition mechanics; no
skill change needed. Test-audit authoring guidance preserved real ownership
and falsifiable witnesses. The hidden-lock fork lesson is recorded in
`docs/lessons.md`; no new runbook or test-only production seam is needed.

### Full-Suite Fixture Correction

Initial full Docker Valkey run exposed two existing contract-edge fixture
failures: `_listener()` bypasses construction with `object.__new__` and lacked
the newly owned shared condition. Added a real `threading.Condition` bound to
its existing real RLock, without weakening assertions or changing production.
Before correction: extension suite 390 passed, two failed, one opt-in skip.
Fixture Ruff/format and strict production-plus-fixture typing (11 files) pass.
The full Redis wrapper and remaining gates are rerunning with source/tests
frozen again; core and PostgreSQL greens remain valid because only that Redis
fixture changed. The integrated reviewer receives the tiny follow-up delta
separately if its initial snapshot predates this correction.

### Final Integrated Review and Dispositions

Claude read-only integrated review exited 0 in 341.1 seconds under a 540-second
bound: **no blocker**, no P1/P2 findings. It confirmed all five tasks and accepted
Task 5 F1/F2/F3 plus Redis O3 refinements. Integrated F1/P3 accepted: rewrapped
the timestamp comment without behavior change. F2/P3 declined simplification:
the condition fallback preserves the explicit pre-fix sensitivity-control path,
where the condition does not exist; it is not a production fallback. F3/P3 kept:
the current plan backlink is deliberate traceability and will be rewritten by
a separately authorized retirement sweep. No scope expansion was required.

### Final Runtime Gate Evidence

All suites ran at standard auto concurrency, with source/tests frozen during
each run. Full core: 4,052 passed, 18 platform/opt-in/backend-combination skips.
Docker PostgreSQL: 1,767 shared passed, 11 expected skips; 418 extension passed,
7 isolated/opt-in skips. Docker Valkey after fixture correction: 1,759 shared
passed, 19 platform/SQL-only skips; 392 extension passed, one opt-in diagnostic
skip. Full examples: 136 passed. Isolated locked Python 3.15 qualification:
160 passed, one Windows-only skip. Every new backend/fork/interrupt proof ran;
none was waived for absent services. Logs are `/tmp/simplebroker-final-*.log`.
The final source change after those suites is comment-only wrapping.

### Handoff State

Implementation and verification evidence is recorded; changes remain
uncommitted. `git log -1` still resolves baseline `1b42554`. The Status Index
row remains `active`, pending owner commit/completion rather than claiming
repository completion without a commit. Unrelated guidance edits are preserved
and excluded from the owned review delta. Coalescing remains untouched.

### Final Fixture Review and Rechecked Gates

Claude's scoped fixture follow-up exited 0 in 34.1 seconds under the 540-second
bound: **no blocker**, no P1/P2/P3 findings. It verified the condition shares
the production RLock and reaches real notify/wait paths without constructor
transport startup. The existing constructor-bypass fixture's maintenance cost
is accepted; adding a test-only production initialization helper is not justified.
After final comment wrapping and evidence updates, document paths, current and
trusted plan context, DOM-15 fixtures, whole-repository Ruff/format (452 files),
suppression registry and diff whitespace checks all pass. No tests or reviewer
processes remain in flight. No commit was made.

### Owner-Authorized Completion

The owner requested commit and completion on 2026-10-14. All implementation,
verification and independent-review gates above passed; the Status Index row
is closed as `completed` in this same change. The completion commit includes
only the reviewed implementation and its plan/spec/rationale/test/changelog
files. Unrelated guidance edits are excluded from staging and preserved byte
for byte. The independently scoped CLI quiet-placement repair follows in a
separate change; it is not part of this completed remediation plan.
