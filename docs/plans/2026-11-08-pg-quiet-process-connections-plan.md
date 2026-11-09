# PostgreSQL quiet-process connections

Class: 5 — normative text changes in [SB-API-3] and [SB-API-6]; a [DOM-5] risky trigger also fires (new connection cleanup lifecycle for pooled PostgreSQL connections, and an additive waiter contract), so the hardening checklist applies.
Status: active — implemented on branch `perf/sb-idle-connections`; cross-family review and owner decisions pending.
Owner: SimpleBroker product owner; implementation agent for SB-4 of the 2026-11-08 performance tracks.
Boundary: `PollingStrategy` native safety-recheck scheduling (core) and the PostgreSQL runner pool and activity waiters (extension). No change to the LISTEN listener, NOTIFY emission, Redis, SQLite, or session/lease ownership.
Verification: deterministic core strategy tests and transition table; real PostgreSQL 18 tests for quiet session count, NOTIFY wake and lost-notification recovery; full PostgreSQL extension suite; PostgreSQL-backed shared watcher suites; disposable postgres:18 measurement of sessions held by N quiet processes and wake latency, before and after.
Required action: review the proposed bound change (owner decision OD-1) and the default constants (OD-2) before landing.

## Goal

A process that is only waiting on a healthy PostgreSQL LISTEN-based activity
waiter should hold its LISTEN connection and not also a pooled data
connection. Today two mechanisms keep the data connection open forever:

1. `PollingStrategy` returns for a native "safety recheck" every
   `max(1, 10 × BROKER_MAX_INTERVAL)` seconds, drawn from `[1 s, 2 s)` by
   default. The built-in watcher (and Weft's reactor) then run a pending check
   through the pooled data connection, so it is used every 1–2 seconds.
2. The PostgreSQL command pool (`min_size=0`, `max_size=3`) keeps returned
   connections because psycopg-pool's `max_idle` defaults to 600 seconds and
   only closes a connection after a full idle period without any use.

Measured downstream (Weft, 1,250-task nested workload, PostgreSQL 18): about
2.4 sessions per running task, LISTEN plus data per process. The change must
stand on SimpleBroker's own contracts and measurement; the downstream number is
motivation only.

Outcomes:

- [x] A backend-native waiter can declare its own safety-recheck interval;
      the strategy default (and therefore Redis/Valkey and third-party
      waiters) is unchanged.
- [x] PostgreSQL waiters declare a 60-second recheck; NOTIFY wake latency is
      unchanged in kind (notification-driven).
- [x] The PostgreSQL pool returns idle data connections to the server after a
      bounded idle period (10 s idle checks).
- [x] The remaining lost-notification recovery bound is stated precisely in
      [SB-API-6] and proved by tests.
- [x] Before/after measurement on disposable postgres:18.

## Source Documents

Consulted (read order): `docs/program-theory.md` [THEORY-2], [THEORY-3],
[THEORY-4], [REV-THEORY-004], [REV-THEORY-006]; `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md`; runbooks `writing-plans.md`,
`hardening-plans.md`, `testing-patterns.md` (by reference);
`docs/agent-context/lessons.md` and `docs/lessons.md` (Golden Rules plus
post-watermark entries, notably 2026-08-25 "never assert a negative timing
window to prove blocking", 2026-10-06 native-waiter loop lesson, 2026-09-15
handle-close vs thread-cache lesson).

Source specs:

- `docs/specs/16-python-library-api.md` [SB-API-3] (PostgreSQL command pool,
  checkout lifecycle), [SB-API-6] (watchers, `PollingStrategy`, activity
  waiters, "safety recheck").
- `docs/specs/11-delivery.md` [SB-DELIVERY-2] (watch modes; unchanged).

Implementation rationale: `docs/implementation/06-process-session-core-ownership.md`
(PostgreSQL pool and listener paragraphs), `docs/implementation/07-complexity-and-state-machine-map.md`
(`SM-POLLING`, `SM-PG-LISTENER`).

Historical context (context-bound, not vetoes): retired
`2026-09-18-polling-strategy-native-deadline-and-local-wake-plan` at
`bdbacf3` (named the 1–2 s safety recheck and scoped waiter-protocol changes
out of that plan); commit `c1afc05` (introduced the native idle recheck as a
reduction from per-pass checks).

## Spec Baseline

- `5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` — `docs/specs/16-python-library-api.md`
  at plan authoring time.
- Plan type: implementation with spec revision. Promotion strategy B (atomic):
  the delta, code, tests and reciprocal backlinks land in one commit on the
  task branch.
- Promotion baseline identifier: the task-branch commit that carries this plan
  (see `## Review and execution record`).

## Context and Key Files

| Owner | Current behavior | Change |
|-------|------------------|--------|
| `simplebroker/watcher.py::PollingStrategy` | `_native_idle_poll_interval = max(1, 10 × max_interval)`; `_next_native_idle_poll_deadline()` draws `uniform(R, 2R)`; scheduled on `start()`, `replace_activity_waiter()` (initial, ≥1 s), local-activity consumption, and each quiet recheck return. Native wake (`waiter.wait()` true) does not reschedule. | Read and validate an optional `safety_recheck_interval` from the waiter being installed (before any mutation); cache it in `_waiter_safety_recheck_interval`; use it in place of the default when set; clear it on detach. |
| `simplebroker/watcher.py::BaseWatcher._process_messages` / `QueueWatcher._has_pending_messages` | Every strategy return without stop runs `queue.has_pending()` — on PostgreSQL a pooled checkout. | Unchanged. |
| `extensions/simplebroker_pg/simplebroker_pg/runner.py::PostgresRunner._create_pool` | `ConnectionPool(min_size=0, max_size=3, timeout=30)`; psycopg-pool `max_idle` default 600 s. Each operation checks out and returns (`lease_session_core_connection()` is `False`). | Pass `max_idle=_POOL_MAX_IDLE_SECONDS` (10 s). |
| `extensions/simplebroker_pg/simplebroker_pg/runner.py::PostgresActivityWaiter`, `PostgresMultiQueueActivityWaiter` | Register with the shared listener; `wait()` blocks on a condition; no recheck declaration. | Set `safety_recheck_interval = _ACTIVITY_SAFETY_RECHECK_SECONDS` (60 s). |
| `_SharedActivityListener` | One LISTEN connection per process, DSN and schema. | Unchanged. |
| `simplebroker/_broker_session.py`, `simplebroker/session.py` | Process session owns the runner/pool; `recycle_thread()` and `cleanup_connections()` release thread cores; on PostgreSQL they are not needed to return completed checkouts. | Unchanged; the pool, not the session, owns idle reaping. No new public release API. |

psycopg-pool 3.3.3 semantics (read at source, `psycopg_pool/pool.py`):
`ShrinkPool` runs every `max_idle` seconds; `_shrink_pool()` closes one idle
connection if `_nconns > min_size` and the minimum number of idle connections
during the elapsed period (`_nconns_min`) was above zero. `getconn()` lowers
`_nconns_min` when it empties the idle deque. A connection used once per
period therefore never closes; a single idle connection closes 10–20 s after
its last use with `max_idle=10`; a three-connection pool drains within 40 s.

Comprehension questions (answers required in the execution record before
editing):

1. Which action keeps a quiet PostgreSQL watcher's pooled connection alive
   today, and why does lowering `max_idle` alone not fix it? Expected: the
   1–2 s safety recheck's `has_pending` checkout lowers `_nconns_min` to zero in
   every idle-check period shorter than the recheck gap; with `max_idle`
   below the gap the connection would instead churn (close and reconnect every
   recheck).
2. Why must the recheck interval be a waiter declaration rather than a new
   strategy-wide constant? Expected: the loss model belongs to the backend
   ([THEORY-3] backend adapter ownership). Redis pub/sub silently resubscribes
   after a dropped connection and loses notifications in the gap, so its
   recheck must stay short; PostgreSQL LISTEN delivers every committed NOTIFY
   to a live session. A waiter declaration also flows through
   `replace_activity_waiter()` to embedders (Weft) without signature changes.

## Invariants and Constraints

- NOTIFY-driven wake is unchanged: a write to a watched queue still wakes the
  waiter through the listener. The recheck is not a wake path.
- The safety recheck remains for PostgreSQL; only its interval changes. The
  recheck still runs a real pending check through the normal Queue path.
- Default strategy behavior is exact for waiters that declare nothing (Redis,
  third-party, fakes): `R = max(1, 10 × max_interval)`, `[R, 2R)`, first
  schedule ≥ 1 s.
- `start()` and `replace_activity_waiter()` stay exception-atomic: an invalid
  declaration raises before detaching, closing or changing anything.
- `ActivityWaiter` protocol members (`wait`, `close`) are unchanged; the
  declaration is an optional attribute read through the existing optional-hook
  pattern (`getattr(..., None)`).
- Pool bounds (`max_size=3`, checkout timeout 30 s), checkout ownership,
  transaction/iterator retention, fork recovery and listener lifecycle are
  unchanged. `min_size` stays 0.
- No change to SQLite or Redis code paths, NOTIFY SQL, or the listener.
- No backward-compatibility shim; no new configuration key; no new dependency.

## Proposed Spec Delta

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/16-python-library-api.md` | B — atomic | [SB-API-6] new paragraph after the `wait_for_activity(timeout=None)` paragraph; [SB-API-3] new paragraph after the checkout-retention sentence; Verification rows [SB-API-3]/[SB-API-6]; [SB-API-3] implementation mapping; Related Plans |

### [SB-API-6] — insert after the paragraph ending "Validation precedes waiter or cadence state changes."

> While a backend-native activity waiter reports no activity, the strategy
> still returns from `wait_for_activity()` for a quiet safety recheck so its
> owner can check the queue. The recheck recovers activity whose backend
> notification was lost; it is not the wake path and does not change
> notification latency. `R` is the installed waiter's optional
> `safety_recheck_interval` attribute when it exists and is not `None`, and
> otherwise `max(1, 10 × max_interval)`. After each recheck, and after the owner
> consumes a local activity notification, the next recheck deadline is drawn
> uniformly from `[R, 2R)`. When `start()` or `replace_activity_waiter()`
> installs a waiter, its first deadline is drawn from `[R₀, 2R₀)` with
> `R₀ = max(R, 1)` seconds. The strategy notices a passed deadline when its
> current native wait pass ends; one pass lasts at most
> `P = max((1 + jitter_factor) × max_interval, burst_sleep)` plus scheduler
> delay. A caller deadline returns earlier without moving the recheck deadline.
> A declaration that is neither `None` nor a non-boolean `int`/`float` makes
> `start()` and `replace_activity_waiter()` raise `TypeError`; a non-positive,
> non-finite or float-overflowing declaration, or one whose `2R` is not finite,
> raises `ValueError`. Both raise before any strategy state changes and before
> any waiter is detached or closed. A committed write whose notification never
> arrives is therefore seen by the owner's check after the next strategy return,
> less than `2R′ + P` after the write (plus scheduler delay and the check
> itself), where `R′` is `R₀` until the first recheck or consumed local activity
> after installation and `R` afterward. PostgreSQL waiters declare `R = 60`
> seconds: with the default polling configuration the bound is under 120.115
> seconds. Redis/Valkey waiters declare none and keep the default: under 2.115
> seconds with the default configuration.

### [SB-API-3] — insert after the sentence "A suspended iterator or open transaction occupies its checkout until it is exhausted, closed, committed, or rolled back."

> The PostgreSQL command pool does not keep returned connections open
> indefinitely. Every 10 seconds, if the pool kept at least one idle connection
> throughout the preceding 10 seconds, it closes one idle connection. A
> process whose operations stop therefore keeps its last pooled connection for
> 10 to 20 seconds after its last operation and drains a three-connection pool
> within 40 seconds; its next operation opens a new connection. The activity
> listener's connection is separate and is not closed by this policy. With
> PostgreSQL waiters' 60-second safety recheck ([SB-API-6]), a process that is
> only waiting on healthy activity waiters holds its listener connection plus,
> after each recheck, one data connection for 10 to 20 seconds.

## Observable Difference Register

| Change | Class | Theory basis |
|--------|-------|--------------|
| Quiet PostgreSQL watcher processes return their data connection to the server (steady sessions per quiet process ≈ 1.1–1.2 instead of 2). | N (no loss; resource relief) | [REV-THEORY-004] (resource count must not scale with idle handles against server connection limits); [THEORY-3] process session / backend runner own reusable resources |
| PostgreSQL lost-notification recovery bound rises from `2R′ + P` ≈ 2.115 s to ≈ 120.115 s with default polling settings ([SB-API-6]); wake on delivered NOTIFY unchanged. | D (diminution in a failure mode) — owner decision OD-1 | [THEORY-4] explicit safety over magical recovery: the bound is now stated; the recheck covers only undetected listener-session loss or writers that bypass SimpleBroker; [THEORY-3] backend owns its loss model. Not on any `[ALT-*]` rejected path. |
| First operation after ≥ 10–20 s of quiet on PostgreSQL pays a new connection setup (measured below); applies to the first wake after quiet too. | D (latency for first operation after quiet) — owner decision OD-2 | [THEORY-1]/[THEORY-4]: operationally small, predictable default for many cooperating processes; tunable constants named at the edit point |
| More connection establishments for intermittently active PostgreSQL processes (one per quiet gap > ~20 s; one per recheck for fully quiet processes, i.e. every 60–120 s). | C (cost moved, no capability lost) | same as above |
| Optional `safety_recheck_interval` waiter declaration; invalid declarations raise `TypeError`/`ValueError` at `start()`/`replace_activity_waiter()`. | A (additive contract; owner-decided via this plan) | [THEORY-3] backend adapter owns substrate behavior; matches existing optional-hook pattern |
| Redis/Valkey, SQLite, third-party waiters: no change. | N | — |

## Hardening

- Hidden coupling: `_ACTIVITY_SAFETY_RECHECK_SECONDS > 2 × _POOL_MAX_IDLE_SECONDS`
  or the recheck keeps (or churns) the data connection. Marked at both
  constants and enforced by `test_pg_waiters_declare_recheck_longer_than_two_pool_idle_checks`.
- Hidden coupling: Weft's reactor treats every native-waiter return, including
  the safety recheck, as evidence and reads (`weft/core/tasks/multiqueue_watcher.py`
  `_activity_hint_action`). Its reads now follow the declared 60–120 s
  cadence. Caller deadlines still return earlier and are Weft's own cadence.
- Error priorities: an invalid declaration is a backend bug and is fatal at
  installation (no silent fallback). Pool shrink failures are psycopg-pool
  internal maintenance and unchanged.
- What not to mock: the PostgreSQL proofs use a real server, real listener,
  real pool and a real spawned watcher process; session counts come from
  `pg_stat_activity` through `get_connection_usage()`. Only module-owned
  policy constants are shortened in the child (seams, not mocks). Core cadence
  proofs patch the module-owned `_monotonic`/`_uniform` aliases only.
- Rollback: revert the single commit. No persistent state, schema or wire
  change; old and new processes interoperate. Either half can be reverted
  alone: removing only `max_idle` restores held connections; removing only
  the waiter declaration restores the 1–2 s recheck, which keeps the single
  data connection in use in every 10 s idle check, so it stays open as before
  (see the mixed-version note below).
- One-way doors: none. The waiter attribute is additive.
- Mixed versions: a new `simplebroker-pg` with an older core keeps the 1–2 s
  recheck (the attribute is ignored), so the 10 s idle check never finds a
  whole idle period and behavior matches today; a new core with an older
  extension sees no declaration and also behaves as today. No version floor
  change is required.
- Post-deploy signals: `get_connection_usage()` by owner shows quiet worker
  groups at about one session per process; `pg_stat_database.xact_commit` rate
  from quiet processes falls; backend `connections` counters show bounded
  reconnect churn.
- Stop-and-re-evaluate: if any existing PostgreSQL test depends on idle
  connections persisting beyond 10–20 s, or if measurement shows reconnect
  latency dominating wake latency, stop and report rather than retune silently.

## Tasks

1. Core tests (RED): `tests/test_watcher.py::TestPollingStrategy`
   (declared interval, start/replace, detach, invalid declarations);
   `tests/test_watcher_transition_tables.py` `NATIVE_SAFETY_RECHECK` row.
2. Core implementation: `simplebroker/watcher.py` (`_declared_safety_recheck_interval`,
   `PollingStrategy` field and scheduling).
3. PostgreSQL tests (RED): `extensions/simplebroker_pg/tests/test_pg_quiet_connections.py`.
4. PostgreSQL implementation: `extensions/simplebroker_pg/simplebroker_pg/runner.py`.
5. Spec promotion (strategy B) and docs: spec 16, PostgreSQL README,
   `docs/guides/python.md`, implementation docs 06 and 07, `CHANGELOG.md`,
   plan index row.
6. Verification and measurement (below); commit on the task branch.

## Testing Plan

| Proof | Test | RED evidence |
|-------|------|--------------|
| Declared interval governs `[R, 2R)` via `start()` and `replace()`; default unchanged when absent/`None` | `tests/test_watcher.py::TestPollingStrategy::test_native_safety_recheck_uses_installed_waiter_interval` | base: `(1.0, 2.0) == (30.0, 60.0)` fails |
| Declaration does not outlive its waiter | `::test_detached_waiter_declaration_does_not_outlive_installation` | base: `11.0 == 70.0` fails |
| Invalid declarations rejected before mutation | `::test_invalid_waiter_safety_recheck_interval_is_rejected_before_mutation` | base: DID NOT RAISE |
| SM-POLLING row | `tests/test_watcher_transition_tables.py::test_polling_fires_transition_table[SM-POLLING::NATIVE_SAFETY_RECHECK]` | base: deadline 51.5 ≠ 80.0 |
| Coupling: PG declared recheck > 2 idle checks (single and fan-in waiters) | `extensions/simplebroker_pg/tests/test_pg_quiet_connections.py::test_pg_waiters_declare_recheck_longer_than_two_pool_idle_checks` | base PG: no attribute |
| Quiet watcher keeps only LISTEN, stays there across a window the old cadence could not, and still wakes on NOTIFY in < 5 s | `::test_quiet_watcher_holds_only_listener_and_still_wakes_promptly` | base PG: "kept 2 sessions; expected 1" |
| Lost notification (row committed without `pg_notify` after the watcher is observed quiet) is recovered within `2R` + slack | `::test_safety_recheck_recovers_write_without_notification` | preservation proof (passes before and after); sensitivity control in the review record: fails with the recheck return disabled |
| First recheck after installation uses `max(R, 1)`; later rechecks use `R`; each return lands before the deadline plus one pass | `tests/test_watcher.py::TestPollingStrategy::test_first_recheck_after_installation_uses_one_second_floor` | contract pin (behavior predates this plan; the original delta misstated it) |
| The pass term includes positive jitter (`R = 60`, maximal jitter returns in `[120, 120.115)`) | `::test_recheck_bound_includes_one_jittered_native_pass` | contract pin |
| A declaration whose `2R` overflows is rejected before mutation; the largest finite range installs a finite deadline | `::test_invalid_waiter_safety_recheck_interval_is_rejected_before_mutation[1e+308/max]`, `::test_largest_finite_recheck_range_installs_finite_deadline` | prior commit: `DID NOT RAISE ValueError` for both overflow cases |
| Idle-decision probes declare their own recheck cadence (existing tests repaired) | `tests/test_watcher_thundering_herd.py::test_unrelated_write_does_not_drain_idle_watchers`, `::test_skip_idle_check_environment_controls_main_loop` | branch before repair: three PostgreSQL-backed failures waiting 10 s for an idle decision (see execution record) |
| No regression | full PostgreSQL extension suite; PostgreSQL-backed shared suite; full core suite; Redis integration and waiter-lifecycle suites | — |

## Verification and Gates

```bash
uv run pytest tests/test_watcher.py tests/test_watcher_transition_tables.py tests/test_connection_config.py
SIMPLEBROKER_PG_TEST_DSN=... uv run pytest extensions/simplebroker_pg/tests -m pg_only -n 4
SIMPLEBROKER_PG_TEST_DSN=... BROKER_TEST_BACKEND=postgres uv run pytest tests/test_watcher*.py -m shared -n 4
uv run ruff check . && uv run ruff format --check <touched>
uv run mypy simplebroker
bin/check-plan-context && python3 bin/check-dom15-fixtures && bin/check-doc-paths
```

## Out of Scope

- A liveness probe on the LISTEN connection, and replacing a failed shared
  listener in `_SharedActivityRegistry` (see `## Deviation Log` finding F-1).
- A public "release idle connections" API (not needed with bounded reaping;
  `BrokerSession` close remains the explicit release).
- Making the pool idle period or recheck interval configurable.
- NOTIFY fan-out, write-path contention, plugin discovery, first-use
  validation (other tracks).

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| process ([DOM-5] slice order) | Independent review of plan and delta before code | Coordinator runs cross-family review after the branch commit; author did not call reviewers | Track brief: coordinator owns cross-family review; nothing lands on `main` | n/a — review recorded below when run |
| [SB-API-6] test cadence | Existing shared tests unchanged | Two idle-decision probes in `tests/test_watcher_thundering_herd.py` now declare a 1 s recheck on their waiters | They relied on the former 1–2 s PostgreSQL recheck to observe an idle decision; the claims are unchanged (execution record) | n/a — conforms to promoted [SB-API-6] |

Finding F-1 (pre-existing, reported, not fixed here): `_SharedActivityRegistry.acquire()`
returns an existing listener without checking whether its thread failed. A
watcher retry after a listener error (server restart, `pg_terminate_backend`)
re-acquires the same failed listener while the old waiter still holds a
reference, so the retry fails again. See the execution record for evidence.

## Owner Decisions Needed

- OD-1: accept the PostgreSQL lost-notification recovery bound `2R′ + P`
  with `R = 60 s` (under 120.115 s with default polling settings; was under
  2.115 s), or choose a different `R`. Pending.
- OD-2: accept the 10-second pool idle check (first operation after ~10–20 s
  of quiet reconnects), or choose a different value; `R > 2 × max_idle` must
  hold. Pending.
- OD-3: whether to pursue F-1 (listener replacement) and, with it, a LISTEN
  liveness probe that would allow a short bound again without a data
  connection. Pending.

## Independent Review Loop

- Author family: anthropic (Claude)
- Reviewer family: openai (Codex), coordinator-run
- Review invocation: coordinator via `skills/call-agent/SKILL.md` (not invoked by the author)

## Review and execution record

### Comprehension answers (before editing)

1. The 1–2 s quiet safety recheck: each return runs `QueueWatcher._has_pending_messages()`
   → `Queue.has_pending()` → `PostgresRunner._run()` → `pool.getconn()`, which
   drops the idle deque to empty and sets `_nconns_min = 0` for that idle-check
   period. With the default 600 s period the connection never closes; with a
   period shorter than the recheck gap it would close and reopen around every
   recheck (churn) instead of staying closed.
2. Backend loss models differ (Redis pub/sub resubscribe gap vs PostgreSQL
   LISTEN session delivery), so the backend that owns the substrate declares
   the interval; a waiter attribute reaches every `start()`/`replace_activity_waiter()`
   caller including Weft without new parameters.

### Execution

Worktree: `perf/sb-idle-connections` from `perf/sb-base` = `5d29fbf`.
Environment: `uv sync --all-extras`; disposable `postgres:18`
(`max_connections=400`) container for iteration and measurement;
`bin/pytest-redis` for the Redis selection.

RED evidence (pre-change behavior):

- Core, against the unmodified strategy: the four declared-interval cases of
  `test_native_safety_recheck_uses_installed_waiter_interval` failed with
  `assert (1.0, 2.0) == (30.0, 60.0)`;
  `test_detached_waiter_declaration_does_not_outlive_installation` failed with
  `assert 11.0 == 70.0`; every invalid-declaration case failed with
  `DID NOT RAISE`. The absent/`None` cases passed (preservation).
- PostgreSQL, base extension (`5d29fbf` `simplebroker_pg` on `PYTHONPATH`)
  with the branch core: `test_pg_waiters_declare_recheck_longer_than_two_pool_idle_checks`
  failed with `'PostgresActivityWaiter' object has no attribute
  'safety_recheck_interval'`; `test_quiet_watcher_holds_only_listener_and_still_wakes_promptly`
  failed with `owner quiet-… kept 2 sessions; expected 1`;
  `test_safety_recheck_recovers_write_without_notification` passed
  (preservation proof). This also shows the core change alone does not
  release the connection.

Changing failing tests (test-audit "Changing a Failing Test"): with the
branch, three PostgreSQL-backed shared tests failed —
`tests/test_watcher_thundering_herd.py::test_unrelated_write_does_not_drain_idle_watchers`
and both `test_skip_idle_check_environment_controls_main_loop` cases — waiting
for an idle watcher's next decision within 10 s. Causality was confirmed by
temporarily setting the PostgreSQL declaration to 1 s (all six tests in the
module passed) and reverting. Conclusion: the tests were wrong about an
incidental cadence, not the product: their claims (an unrelated write never
drains idle watchers; `BROKER_SKIP_IDLE_CHECK` controls the idle loop) need
an idle decision, and on native backends that decision is the quiet safety
recheck whose cadence [SB-API-6] now lets the backend declare. Repair: the
tests declare a 1 s recheck on the created waiter through the public
`safety_recheck_interval` attribute (`_declare_prompt_safety_recheck`), which
keeps the assertions unchanged and makes the observation cadence explicit for
every backend.

Finding F-1 evidence (base `5d29fbf`, real PostgreSQL 18): a `QueueWatcher`
whose shared listener backend is terminated with `pg_terminate_backend`
handles the next message through retry plus initial drain (1.5 s), keeps the
same failed listener registered, re-raises on every later wait, and dies
about 14 s later with `OperationalError: consuming input failed: server
closed the connection unexpectedly`; a later write is not handled. Probe:
`listener_failure_probe.py` (coordinator scratchpad `sb4/`). Not changed by
this plan.

### Verification results (current tree)

| Command | Result |
|---------|--------|
| `pytest -n 4` (full core suite, SQLite) | 4080 passed, 19 skipped |
| `BROKER_TEST_BACKEND=postgres pytest tests -m "shared and not benchmark and not nested_xdist" --timeout=180 -n 4` (postgres:18) | 1809 passed, 12 skipped |
| `pytest extensions/simplebroker_pg/tests -m pg_only -n 4` (postgres:18) | 430 passed, 7 skipped (opt-in probes / serial-job skips) |
| `pytest test_pg_quiet_connections.py test_connection_usage.py test_pg_notify.py -n 4`, three repeats | 26 passed ×3 |
| `bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_integration.py extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py -n 4` | 56 passed |
| `ruff check .`; `ruff format --check` (CI paths) | clean |
| `mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis`; PostgreSQL extension + tests; touched root tests | no issues |
| `bin/check-plan-context`; `python3 bin/check-dom15-fixtures`; `bin/check-doc-paths` | OK |

After the review fixes (F1–F4), rerun on the current tree:

| Command | Result |
|---------|--------|
| Core watcher, config and doc-contract selection (15 files, SQLite, `-n 4`) | 350 passed |
| `BROKER_TEST_BACKEND=postgres pytest tests/test_watcher*.py -m shared -n 4` (postgres:18) | 213 passed |
| `pytest extensions/simplebroker_pg/tests/test_pg_quiet_connections.py`, three repeats | 3 passed ×3 |
| `pytest extensions/simplebroker_pg/tests -m pg_only -n 4` | 430 passed, 7 skipped |
| ruff check, ruff format --check (CI paths), mypy (core + extensions, PostgreSQL extension tests, touched root tests) | clean |
| `bin/check-plan-context`, `bin/check-doc-paths`, `python3 bin/check-dom15-fixtures` | OK |

### Measurement (disposable postgres:18, same machine, other agents running)

Script: N child processes, each one consuming `QueueWatcher` on its own
queue, connections labeled with `bind_connection_owner`; `pg_stat_activity`
sampled every 0.5 s and split into LISTEN vs other sessions; then one write per
queue after the quiet window ("cold") and one immediately after ("warm").
Base = `5d29fbf` sources on `PYTHONPATH`; branch = this worktree.

| N = 20 quiet processes | Base (180 s window) | Branch (300 s window) |
|------------------------|---------------------|-----------------------|
| LISTEN sessions | 20 | 20 |
| Data sessions, steady (from 30 s) | 20 (min 20, max 20) | mean 3.1 (min 0, max 8) |
| Sessions per quiet process | 2.0 | 1.16 |
| Server commits/s during window (incl. 2/s observer) | 14.7 | 1.2 |
| Wake p50 / max, first write after quiet | 12.3 / 26.4 ms | 23.1 / 114.9 ms |
| Wake p50 / max, immediately after | 11.6 / 30.1 ms | 13.4 / 25.0 ms |

Branch timeline: data sessions fall from 20 to 0 by about 20 s, stay at 0
until the first 60 s rechecks, then fluctuate between 0 and 8 as each process
reopens one connection per 60–120 s recheck and returns it 10–20 s later.

Single watcher, 8 rounds of 25 s quiet then two writes (base and branch run
concurrently): first-write wake p50 14.4 ms (base) vs 26.2 ms (branch); warm
p50 8.5 vs 10.2 ms; tails reached 126 ms (base) and 216 ms (branch) under the
shared machine's load. Raw `psycopg.connect` + `SET search_path` on this
container: p50 3.8 ms, p90 5.1 ms. The first operation after quiet adds the
connect, the pool worker hand-off and a cold prepared-statement cache.

### Cross-family review

Tier 1, Codex (codex exec -s read-only, reasoning effort high), 2026-11-08,
scoped-change review of plan+implementation at 6679e17c against 5d29fbf.
Verdict: blocker: F1, F3.

The reviewer judged the intended differences registered, both D rows
consistent with the theory, and backend-owned cadence plus pool-owned reaping
a good fit. Owner decisions OD-1, OD-2 and OD-3 remain pending.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F1 | P2 | The stated recovery bound (`2R` plus one quiet pass of at most `max_interval`) is false for valid declarations: installation clamps the first interval to `max(R, 1)` (R = 0.1 returned at 1.001 s > 0.3 s), and the pass excludes positive jitter (R = 60 returned at 120.109 s > 120.1 s). | Accepted, fixed: [SB-API-6] now states the installation floor `R₀ = max(R, 1)`, the pass bound `P = max((1 + jitter_factor) × max_interval, burst_sleep)`, and the bound `2R′ + P` (PostgreSQL under 120.115 s, default under 2.115 s with default settings); plan delta, difference register, OD-1, guide, PostgreSQL README, implementation doc 06, `_next_native_idle_poll_deadline` docstring and CHANGELOG aligned. Boundary tests `test_first_recheck_after_installation_uses_one_second_floor` (start and replace) and `test_recheck_bound_includes_one_jittered_native_pass` pin both terms. PostgreSQL policy unchanged (R = 60, max_idle = 10). |
| F2 | P3 | `safety_recheck_interval = 1e308` passes validation but `2R` overflows, installing an infinite deadline that silently disables quiet recovery. | Accepted, fixed: `_declared_safety_recheck_interval` rejects a declaration whose `2R` is not finite with `ValueError` before installation mutates state; [SB-API-6] states it. Tests add `1e308` and `sys.float_info.max` to the invalid-declaration cases (RED on 6679e17c: `DID NOT RAISE ValueError`) and `test_largest_finite_recheck_range_installs_finite_deadline` for the accepted boundary. |
| F3 | P2 | The lost-notification test inserted the silent row right after the first handler acknowledgement, before the drain's local follow-up check, so the row could be admitted without a safety recheck. | Accepted, fixed: the spawned watcher optionally reports each re-entry into `wait_for_activity()`; the test inserts only after two entries following the acknowledgement (the first consumes the drain's local follow-up and runs its pending check, the second begins a quiet native wait), drops the unrelated short pool idle period, and keeps the `2R` + slack assertion. Sensitivity control recorded below. |
| F4 | nit | The partial-rollback note predicted reconnect churn after removing only the waiter declaration; a 1–2 s recheck keeps the single connection in use in every 10 s idle check. | Accepted, fixed: the note now says either half can be reverted alone and that removing only the declaration keeps the connection open as before; the "roll both back together" requirement is removed. |

F3 sensitivity control (postgres:18, this worktree): with the recheck return
temporarily disabled in `PollingStrategy.wait_for_activity()` (the
`if _monotonic() >= self._next_native_idle_poll_at:` branch made unreachable,
then restored from a byte copy), the revised
`test_safety_recheck_recovers_write_without_notification` failed at its
receive deadline: `AssertionError: silent: pid=…, exit=None, alive=True`,
`1 failed in 30.96s`. Restored code: `3 passed` for the whole file. Under the
same mutation the 6679e17c version of the test also failed in three runs here
(30.97 s, 31.12 s, 31.20 s); its race is timing-dependent, and the reviewer's
deterministic reproduction stands. The revised test removes the race by
construction rather than by timing.
