# PostgreSQL generated-write lock hold

Class: 4 — changes the PostgreSQL execution path of every ordinary generated
write, adds a second realization of the shared SQL write behavior behind an
optional backend seam, and touches lock ordering that other mutations depend on.
Status: active — implemented on branch `perf/sb-write-lock`; cross-family review pending (coordinator-owned).
Owner: SimpleBroker message-identity layer (`simplebroker/db.py`, `simplebroker/_timestamp.py`) and the PostgreSQL backend (`extensions/simplebroker_pg/`).
Boundary: ordinary `write()` without `keep_newest` on PostgreSQL. SQLite, Redis, `keep_newest`, exact insert, broadcast, rename, standalone `generate_timestamp()`, and conflict repair keep their current realization.
Verification: real-PostgreSQL concurrency, lock-order, exhaustion, atomicity and fallback tests; shared suites on SQLite and PostgreSQL; probe and repository benchmark against disposable `postgres:18`.
Required action: preserve every `[SB-ID-*]` and checkpoint-visibility guarantee; never acquire a `messages` table lock before the high-water row lock on a generated write.

## Goal

On PostgreSQL, an ordinary generated write holds the singleton `meta` row lock
(`last_ts` high-water) from its compare-and-advance until `COMMIT`. Today that
section spans two client round trips (`INSERT ... pg_notify`, then `COMMIT`)
plus, under contention, a failed compare-and-advance, a `SELECT meta`, and a
second compare-and-advance after re-queueing on the lock. Downstream evidence
(Weft, 1,250-task nested workload) shows throughput flat at ~660-710 writes/s
from 4 to 96 writers, lock holders `idle in transaction / ClientRead >= 5 ms`,
and ~20% conflict retries. This plan removes the client from the locked section
and removes stale-candidate retries while keeping every timestamp, ordering,
uniqueness, and delivery guarantee.

## Source Documents

Consulted (read-order declaration): `docs/program-theory.md` [THEORY-2],
[THEORY-3], [THEORY-4], [REV-THEORY-003]; `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md`; runbooks `writing-plans.md`,
`hardening-plans.md`, `testing-patterns.md` (by reference), `review-loops-and-agent-bootstrap.md`
(by reference); `docs/agent-context/lessons.md` and `docs/lessons.md` Golden
Rules plus timestamp/lock ledger entries. No `MODULE-THEORY.md` exists for the
touched modules.

Governing contracts:

- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-2], [SB-ID-3], [SB-ID-4]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-2], [SB-SELECT-5]
- `docs/specs/11-delivery.md` [SB-DELIVERY-4], [SB-DELIVERY-8], [SB-DELIVERY-9]
- `docs/specs/16-python-library-api.md` [SB-API-11] (backend seam and handshake)
- `docs/specs/12-broadcast.md` [SB-BCAST-*] (broadcast lock scope that writers must not deadlock against)

Implementation rationale: `docs/implementation/08-message-identity-and-write-visibility.md`
(SQL ordinary writes, same-generator serialization, checkpoint limit) and
`docs/implementation/07-complexity-and-state-machine-map.md` (`SM-TIMESTAMP-GENERATOR`).

Skills: `test-audit` (authoring gate for new and changed tests).

## Spec Baseline

`5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` — `docs/specs/11`, `13`, `14`, `16`
at plan authoring time. Plan type: implementation against existing contracts.
No normative spec text changes; spec 13 gains a Related Plans backlink and
verification evidence only. `hardening: applies` (class 4).

## Contracts the lock provides today

The PostgreSQL write is `BEGIN` → `UPDATE meta SET last_ts=$c WHERE singleton
AND last_ts<$c RETURNING` → [on zero rows: `SELECT meta`, new candidate, repeat]
→ `INSERT ... RETURNING` + `pg_notify` → `COMMIT`. The row lock taken by the
`UPDATE` provides exactly these properties:

1. **Durable monotonic high-water** ([SB-ID-1], [SB-ID-3]): every allocation is
   strictly above the committed high-water it observed under the lock; repair
   and floor advances are guarded compare-and-advance and never move it back.
2. **Uniqueness of generated IDs** ([SB-ID-1]): allocations are serialized by
   the row lock and the `messages.ts` primary key rejects any residual
   collision (only reachable when stored rows sit above a corrupted high-water).
3. **Commit order equals ID order for generated writes** (checkpoint
   visibility, implementation doc 08 and `tests/test_write_visibility.py`): the
   lock is released only after the holder's commit is visible
   (`RecordTransactionCommit` → `ProcArrayEndTransaction` → lock release), so a
   later allocation can only commit after every lower generated ID is visible.
   A reader that sees ID `N` therefore already sees every generated ID below
   `N`; `after_timestamp` checkpoint readers never skip an ordinary write
   ([SB-SELECT-5] "Ordinary generated writes retain insertion-order behavior").
4. **Allocation and row are one atomic unit** ([SB-ID-2]): a rolled-back or
   rejected write neither advances high-water nor inserts a row; a raising call
   returns no ID.
5. **Hybrid encoding** ([SB-ID-1], [SB-ID-4]): the client computes the
   candidate from `time.time_ns()` (low 12 bits cleared) and its cached
   high-water; when the clock does not advance, the logical counter increments
   and, once exhausted, allocation fails until the clock catches up — it never
   rolls the counter into the next physical grain.
6. **Lock order** (implicit, `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
   comment above `LOCK_LAST_TS_ROW`): every transaction that takes both the
   high-water row lock and a `messages` table lock takes the row lock first.
   Broadcast, rename and `keep_newest` take `SHARE ROW EXCLUSIVE` on `messages`
   after the row lock; generated writes take `ROW EXCLUSIVE` after it.

Properties 3 and 4 require that allocation-to-commit stay serialized; any design
keeps the commit (WAL flush) inside the lock. What can leave the lock is the
client: network round trips and client scheduling between statements.

## Candidate directions and evaluation

Prototype evidence lives in the execution record below.

- **(a) One server-side statement** (`WITH allocated AS (UPDATE meta SET
  last_ts = GREATEST(last_ts+1, $c) ...) INSERT ... SELECT ... RETURNING` +
  `pg_notify`, autocommit). Removes every client step from the lock, but
  PostgreSQL acquires all relation locks of a statement at analysis time, so
  the writer holds `ROW EXCLUSIVE` on `messages` *before* it waits for the
  row lock. That inverts property 6. Reproduced: broadcast holding the row lock
  then requesting `SHARE ROW EXCLUSIVE` deadlocks with a queued single-statement
  writer (`DeadlockDetected`, writer aborted after `deadlock_timeout`=1 s). Safe
  only if broadcast, rename, `keep_newest` and exact insert all switch to
  "messages table first" — a global lock-order inversion across four call
  sites, two of them ordered in shared core. **Rejected for this slice;
  proposed as follow-up.**
- **(b) Never-failing advance** (`GREATEST`): required by every option; removes
  the `SELECT meta` + re-queue retry. Plain `GREATEST(last_ts+1, $c)` would
  roll the logical counter into the next physical grain after a far-future
  exact insert, violating [SB-ID-4]. The adopted form refuses exhaustion
  (zero rows, no mutation) exactly where `TimestampGenerator._next_components`
  would wait for the clock. **Adopted.**
- **(c) Autocommit single-statement writes.** The connection is already
  autocommit; the question is whether the write may commit at the protocol
  boundary instead of an explicit `COMMIT`. Yes: an implicit transaction is
  atomic and commits before results are returned. **Adopted in pipelined form.**
- **(P, adopted) Pipelined implicit transaction.** One psycopg pipeline
  carries `LOCK_LAST_TS_ROW` (`SELECT ... FOR UPDATE`) followed by the
  allocate-insert-notify statement (a), closed by one `Sync` that commits the
  implicit transaction. Lock order is unchanged (row lock first; the second
  statement's `messages` lock is acquired after it). The client never waits for
  a result inside the lock; the residual exposure is the non-blocking gap
  between the client's two statement sends and the `Sync` send (psycopg
  flushes per `execute`). Both statements stay prepared and server-side bound.
- **(Q, rejected) Multi-statement simple query** (`ClientCursor`, one Query
  message). Zero client gap and unchanged lock order, but client-side literal
  binding puts message bodies into the statement text: PostgreSQL's default
  `log_min_error_statement=error` would log bodies on any statement error. That
  is a confidentiality diminution versus server-side binding.

## Context and Key Files

Files to modify:

- `simplebroker/db.py` — `BrokerCore.write()` (retry ladder: IntegrityError →
  sleep → resync → `RuntimeError`), `_do_write_with_ts_retry()` (lock-contention
  retry via `_run_with_retry`), `_do_write_transaction()` (today: `begin_immediate`
  → `generate_timestamp()` → optional keep prepare → `INSERT_MESSAGE` → optional
  claim → `commit`). Add the server-allocated branch here, only for
  `keep_newest is None` and only when the plugin exposes the optional hook.
- `simplebroker/_timestamp.py` — `TimestampGenerator._reserve_candidates()`
  (local-only reservation, docstring already names "one atomic server-side
  operation"); add monotone publication of a server-chosen committed ID.
- `simplebroker/_backend_plugins.py` — `BackendPlugin` docstring lists optional
  getattr-probed hooks; document `write_generated_message`.
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py` — add
  `WRITE_GENERATED_MESSAGE`; keep `LOCK_LAST_TS_ROW` as the first statement.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py` — `PostgresRunner._run`
  owns per-statement checkout/translate/return; `_should_prepare` owns hot
  statement preparation. Add `run_transaction()` (pipelined, with a sequential
  `BEGIN`/`COMMIT` transport when libpq lacks pipeline mode).
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py` — add
  `write_generated_message()`; refresh lock-order comments.
- Tests: new `extensions/simplebroker_pg/tests/test_pg_write_allocation.py`;
  update `tests/test_write_visibility.py`, `tests/test_write_returns_id.py`,
  `tests/test_timestamp_resilience.py`, `tests/test_core_persistence_transition_tables.py`,
  and the write-carrier harness in `extensions/simplebroker_pg/tests/test_pg_core_finalization.py`.
- Docs: implementation docs 07 and 08, spec 13 (backlink + evidence),
  `extensions/simplebroker_pg/README.md`, `CHANGELOG.md`, `docs/lessons.md`,
  `docs/plans/README.md`.

Comprehension questions (expected answers; a wrong answer blocks editing):

1. *Why can the high-water row lock not be released before `COMMIT`?* Because
   commit order must equal generated-ID order for checkpoint readers; PostgreSQL
   row locks are transactional and released only after the commit is visible.
2. *Why does a single CTE statement deadlock with broadcast?* PostgreSQL takes
   `ROW EXCLUSIVE` on `messages` when the statement is analyzed, before the
   `UPDATE` waits for the row lock; broadcast holds the row lock and then waits
   for `SHARE ROW EXCLUSIVE`, which conflicts with the writer's table lock.
3. *What must the server do when the candidate is stale and the logical counter
   of the stored high-water is 4095?* Nothing: return no row and mutate nothing;
   the client refreshes and its generator waits for the clock or raises
   `TimestampError` (`[SB-ID-4]`).

## Invariants and Constraints

- Properties 1-6 above hold on every backend after the change.
- SQLite and Redis behavior and statement sequences are unchanged.
- `keep_newest` writes keep the existing transaction (row lock, then `SHARE ROW
  EXCLUSIVE`, insert, claim, commit).
- The server-chosen ID is `candidate` when `candidate > last_ts`, else
  `last_ts + 1` when the logical counter is below 4095; otherwise no allocation.
  This equals `_next_components()` evaluated against durable high-water with the
  reservation-time clock sample.
- The process-local cache never regresses: publication is a monotone maximum
  under the generator lock; a failed server operation may leave the cache ahead
  of durable state (harmless gap, already documented for direct backends).
- `write()` retry ladder, metrics, and exception types are unchanged; an
  `IntegrityError` from the backend still means "allocated ID already stored,
  nothing committed".
- Lock-contention retry (`_run_with_retry`) still wraps the whole operation;
  non-retryable transport failures stay outcome-ambiguous and are not retried.
- No backend API version bump: the hook is optional and additive in both
  directions (old core never calls it; new core falls back when absent).
- No schema change, no stored-data change, no new dependency.

## Observable-difference register

| Change | Class | Theory basis |
|--------|-------|--------------|
| PostgreSQL generated write sends one pipelined exchange; the high-water row lock is no longer held across client round trips | N — no contract change; latency/throughput only | [THEORY-4] explicit safety: guarantees unchanged |
| Stale candidates no longer cause compare-and-advance failure, `SELECT meta`, or re-queueing; the server allocates `last_ts + 1` | N — [SB-ID-2] "a stale candidate must not advance persisted high-water or insert a row" still holds literally: the stale value is never used | [THEORY-4] |
| A generated ID's physical component may come from the high-water it extends (`last_ts + 1`) instead of a post-conflict clock re-sample | N — the existing generator already carries the last physical component forward when the clock has not passed it; the ID's physical component stays within the write call's time window and never precedes the reservation sample | [SB-ID-1] encoding unchanged |
| Under forced fault injection, a PostgreSQL write whose candidate collides with a row above a corrupted high-water now self-heals after the resync rung instead of exhausting retries | C — strictly more recovery; the exhausting path was only reachable through test fault injection | [THEORY-4] explicit safety over magical recovery: recovery uses the existing guarded resync, no new mechanism |
| Shared conflict tests use `_reserve_candidates` as the PostgreSQL fault seam (as Redis does) and skip the "unresolvable after resync" case where the server allocates above the resynchronized high-water | N — test realization; production contract unchanged | — |
| `TimestampGenerator` gains monotone publication of a server-chosen ID | N — internal; new `SM-TIMESTAMP-GENERATOR` transition row | engineering principle §14 floor 2 |

No D (diminution) rows.

## Tasks

1. **RED tests first** (`extensions/simplebroker_pg/tests/test_pg_write_allocation.py`):
   one-exchange stale-candidate write (spy records runner exchanges; base issues
   begin/advance/select/advance/insert/commit); exhausted-counter refusal with a
   stale cache (no row, high-water unchanged, `TimestampError`); broadcast lock
   order (row lock held, writer queued, broadcast requests `SHARE ROW EXCLUSIVE`,
   no deadlock); hook atomicity on unique conflict; concurrent writers with a
   checkpoint reader; sequential transport fallback. Confirm the RED ones fail
   on base for the intended reason. Stop if a RED test passes on base.
2. **Generator publication** (`simplebroker/_timestamp.py`): add
   `_publish_allocated(timestamp)`; add `RESERVE_PUBLISH` to
   `TIMESTAMP_GENERATOR_TRANSITIONS`.
3. **Core branch** (`simplebroker/db.py`): in `_do_write_transaction`, when
   `keep_newest is None` and `write_generated_message` is callable on the
   plugin, reserve one candidate, call the hook, publish the committed ID; on
   `None` refresh and retry (bounded at six attempts like `generate()`), then
   raise the same `IntegrityError`. Stop if the branch needs any change to the
   SQLite or keep path.
4. **PostgreSQL realization**: `WRITE_GENERATED_MESSAGE` in `_sql.py`;
   `PostgresRunner.run_transaction()`; `PostgresBackendPlugin.write_generated_message()`;
   `_should_prepare` entry. Stop if any statement would take a `messages` lock
   before the row lock.
5. **Shared test seams**: update `tests/test_write_visibility.py` (record
   `run_transaction`), `tests/test_write_returns_id.py` and
   `tests/test_timestamp_resilience.py` (PostgreSQL fault seam, applicability
   skips with a replacement PG proof). Found during the gate run:
   `extensions/simplebroker_pg/tests/test_pg_core_finalization.py` uses a
   write's `BEGIN` as its GC carrier; its harness also arms on
   `run_transaction` (execution record).
6. **Docs**: implementation docs 07/08, spec 13 backlink/evidence, PG README,
   CHANGELOG, lessons, plan index.
7. **Measure** base and branch with the probe (writers 1/4/16/48/96, 100 ops)
   and `bin/benchmark.py --backends pg`; record below.
8. **Gates**: targeted SQLite and PostgreSQL suites, `bin/pytest-pg` shared
   release subset once, ruff, format, mypy, docs gates; commit.

## Testing Plan

- Real PostgreSQL 18 through `uv run ./bin/pytest-pg` (Docker) — never mock the
  backend, psycopg, the pool, or SQL execution. The only spies are pass-through
  recorders over the real runner; the only fault seams are the generator's
  candidate reservation (`_reserve_candidates`), the module clock/iteration
  seams already used by the suite, the durable `write_last_ts` corruption
  primitive, and a runner-module capability flag for the transport fallback.
- Contracts protected: uniqueness, durable monotonicity, commit order versus ID
  order for checkpoint readers ([SB-ID-1..3], [SB-SELECT-5]); exhaustion
  semantics ([SB-ID-4]); atomic allocation+row ([SB-ID-2]); lock order with
  broadcast ([SB-BCAST-*] scope); cache monotonicity ([SB-ID-3]).
- SQLite: the shared suites listed in Task 5 plus `tests/test_core_persistence_transition_tables.py`.

## Verification and Gates

Per task: the targeted module(s) above via `uv run pytest` (SQLite) and
`uv run ./bin/pytest-pg <paths>` (PostgreSQL). Final: `uv run ./bin/pytest-pg --fast -n 4`
once, `uv run pytest -n 4` once for the touched shared modules plus timestamp
and write suites, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy`
per `pyproject.toml`, `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
`bin/check-doc-paths`, `git diff --check`.

## Rollback, Rollout and Success Signals

Rollback: revert the commit. No schema, data, or API-version change; the old
path is restored for every process that restarts with the reverted code. Mixed
fleets are safe in any order: old and new writers use the same row lock first,
so their transactions serialize exactly as two old writers do. Rollout needs
both the new core (calls the hook) and the new PostgreSQL extension (provides
it); either alone keeps today's path.

Post-deploy signals: write throughput no longer flat across writer counts;
`pg_stat_activity` lock holders no longer `idle in transaction / ClientRead`
while holding the `meta` row; zero `UPDATE meta ... last_ts <` statements per
generated write; `get_conflict_metrics()` stays zero under contention; no
`DeadlockDetected` involving `meta`/`messages`; checkpoint readers observe no
generated row behind a later ordinary write (doc 08 signal).

One-way doors: none.

## Out of Scope

Global lock-order inversion (direction (a)), `keep_newest` pipelining,
broadcast/exact-insert allocation, NOTIFY fan-out, plugin-discovery caching,
pool idle policy, first-use validation (other tracks), backend API version or
package version bumps, releases.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Independent Review Loop

- Author family: Claude
- Reviewer family: Codex (selected by the coordinator from `docs/implementation/03-agent-inventory.md`)
- Review invocation: `skills/call-agent/SKILL.md` (coordinator-run, read-only, bounded timeout recorded in the review record)

Reviewer reads this plan, the governing specs above, implementation doc 08, the
diff, and the execution record. Prompt: the recommended prompt in
`docs/agent-context/runbooks/writing-plans.md` §8 plus: verify the lock-order
claim (property 6) against every caller of `LOCK_LAST_TS_ROW`,
`advance_last_ts`, and `generate_timestamp()` inside a transaction; verify that
the server allocation equals `_next_components()` against durable high-water;
and confirm the RED tests fail on base for the stated reason.

## Review and execution record

Cross-family review: pending; the coordinator records reviewer, invocation,
findings, and dispositions here. No external reviewer was called by the
implementer.

### Comprehension answers (implementer, before editing)

1. The row lock is transactional and released only after the holder's commit
   is visible; releasing earlier would let a later allocation commit before a
   lower one, and an `after_timestamp` reader could advance past the lower ID.
2. A single statement's `ROW EXCLUSIVE` lock on `messages` is taken at analysis,
   before its `UPDATE` waits for the row held by broadcast, which then waits for
   `SHARE ROW EXCLUSIVE` behind the writer's table lock — a cycle.
3. Allocate nothing and return no row; the client refreshes, then waits for the
   clock or raises `TimestampError`.

### Prototype evidence (disposable `postgres:18`, PostgreSQL 18.4)

- Naive single statement versus broadcast order: broadcast `BEGIN; SELECT ...
  FOR UPDATE`, writer runs the CTE, broadcast requests `SHARE ROW EXCLUSIVE` →
  writer `DeadlockDetected` after 1.01 s ("waits for ShareRowExclusiveLock on
  relation ... blocked by process ..."). Same scenario with the row lock
  statement first: no deadlock, writer completes after broadcast commits.
- Mixed workload through `BrokerCore` (`broadcast_mix.py`: 4 writer threads x
  200 ordinary writes, 40 broadcasts, 60 `keep_newest=3` writes, separate
  runners, `pg_stat_database.deadlocks` delta): base 0 deadlocks, this branch
  0, direction (a) single-statement mutant 36 deadlocks plus one failed
  operation (`TimestampError ... current transaction is aborted`). The
  lock-contention retry hides most of them from callers, at a 1 s
  `deadlock_timeout` stall each.
- psycopg 3.3.8 / libpq 18 pipeline in an autocommit connection: two
  statements without `BEGIN` report the same `pg_current_xact_id()`; a unique
  violation in the second statement rolls back the first and leaves the
  connection idle. psycopg flushes each queued statement immediately
  (`_execute_gen` → `_communicate_gen`) without waiting for results.
- Raw psycopg micro-benchmark (`proto_bench.py`, 100 ops per process, machine
  load average ~22 from other agents): 16 processes — current 808 writes/s
  (1,598 compare-and-advance retries for 1,600 writes), single CTE 1,389,
  multi-statement simple query 1,343, pipelined 1,222; 96 processes — 618, 714,
  846, 882. All variants produced unique IDs.

### RED evidence (new tests against base code `5d29fbf`)

`SIMPLEBROKER_PG_TEST_DSN=<container> .venv/bin/python -m pytest
extensions/simplebroker_pg/tests/test_pg_write_allocation.py -m pg_only -n 0`
before any production edit: 5 failed, 2 passed.

- `test_stale_candidate_write_is_one_exchange_allocating_above_high_water`:
  base issued `begin, advance, read_meta, advance, insert, commit` (the stale
  candidate lost the compare-and-advance) instead of one `run_transaction`.
- `test_exhausted_high_water_counter_refuses_without_mutation`: base's first
  exchange was `begin` (client-side path), not the server allocation.
- `test_generated_write_conflict_rolls_back_allocation`: no
  `write_generated_message` hook.
- `test_persistently_conflicting_candidate_self_heals_after_resync`: base
  ignored the reserved candidate and committed a fresh ID.
- `test_sequential_transport_preserves_generated_write_semantics`: no
  transport seam.
- The lock-order and checkpoint-reader tests pass on base by design: they
  guard contracts the base already keeps.

### Sensitivity checks (each restored and verified by `shasum`)

- M1, counter guard removed from `WRITE_GENERATED_MESSAGE` →
  `test_exhausted_high_water_counter_refuses_without_mutation` fails
  ("DID NOT RAISE TimestampError": the server rolled into the next grain).
- M2, `LOCK_LAST_TS_ROW` dropped (direction (a)) →
  `test_queued_writer_does_not_deadlock_broadcast_lock_order` fails with
  `OperationalError('deadlock detected ...')`. The first version of this test
  went through `write()` and passed under M2 because `_run_with_retry`
  absorbed the deadlock; it now calls the hook directly.
- M3, allocation committed separately from the insert → the one-exchange,
  exhaustion, conflict-rollback, and self-heal tests fail deterministically;
  the concurrent checkpoint-reader test (final form: 8 writers x 100 writes,
  thread-pool futures) failed in 5 of 10 runs (probabilistic end-to-end guard;
  earlier drafts 3 of 5 and 2 of 10).
- Every control was rerun after the last test restructuring (blind `except`
  replaced by `ThreadPoolExecutor` futures so no new `BLE001` suppression is
  needed): M2 still fails the lock-order test with `deadlock detected`.

### Verification runs

All on the final tree unless noted; PostgreSQL runs use a disposable
`postgres:18` container.

- `uv run --locked pytest -n 4` (full SQLite suite, once): 4061 passed, 19
  skipped, 1 failed — `tests/test_ruff_policy.py::test_approved_suppressions_match_the_registry`,
  because the new PostgreSQL test copied two `noqa: BLE001` blind-except
  suppressions (governed count 148 → 150). Per the suppression rule the test was
  refactored instead (thread-pool futures re-raise writer errors); rerun of
  `tests/test_ruff_policy.py`: 11 passed. No registry change.
- `uv run --locked ./bin/pytest-pg --fast -n 4` (once): shared suite 1764
  passed, 14 skipped; extension suite 9 failed, 425 passed, 7 skipped. All 9
  failures were `extensions/simplebroker_pg/tests/test_pg_core_finalization.py`:
  its harness forces garbage collection while the write's `BEGIN` is ACTIVE,
  and ordinary writes no longer send `BEGIN` ("the real BEGIN must reach
  ACTIVE"). Decision: the test's carrier, not its contract, changed; the
  harness now also arms on `PostgresRunner.run_transaction`. Sensitivity:
  reintroducing the pre-fix `BrokerCore.__del__` fails the same 7 of 9 cases
  with `connection socket closed` on the new carrier as on the old one (old
  carrier reproduced by disabling the hook); restored by `shasum`. Rerun of the
  extension suite against the container (`pytest extensions/simplebroker_pg/tests
  -m pg_only -n 4`): 434 passed, 7 skipped.
- Touched shared suites on PostgreSQL (`BROKER_TEST_BACKEND=postgres`,
  write/timestamp/insert/identity/queue-API/latest-pending/keep/broadcast
  modules): 153 passed, 3 skipped (the two applicability skips above plus the
  existing `get_data_version` skip). Same modules on SQLite: 163 passed.
- New module `extensions/simplebroker_pg/tests/test_pg_write_allocation.py`:
  7 passed, repeated runs.
- `uv run --locked ruff check .`: all checks passed; `ruff format --check`:
  459 files formatted; `mypy simplebroker bin/release.py
  bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg
  extensions/simplebroker_redis/simplebroker_redis`: no issues (68 files);
  `mypy extensions/simplebroker_pg/simplebroker_pg <pg tests>`: no issues (43
  files); core-test mypy (CI form): no issues (217 files).
- `bin/check-plan-context`, `bin/check-doc-paths`, `python3 bin/check-dom15-fixtures`,
  `git diff --check`: OK.
- Redis suite not run: no Redis code changed; `TimestampGenerator`'s Redis
  surface (`_reserve_candidates`) is unchanged and covered by the SQLite
  transition table.

### Measurements

Probe: `sb_write_contention.py <writers> 100` (each writer its own queue and
process; write then `peek_one` per op) against disposable `postgres:18`
(18.4, OrbStack, `synchronous_commit=on`), base worktree `5d29fbf` and this
branch selected by `PYTHONPATH`, interleaved base/branch per writer count.
Other agents' stress runs kept the host's 1-minute load average at 23-57 on 16
cores, so absolute numbers are noisy; compare within a row.

| Writers | Base r1 w/s, p50, p95 ms | Branch r1 | Base r2 | Branch r2 | Load |
|--------:|------|------|------|------|------|
| 1 | 124, 6.39, 14.14 | 199, 3.05, 7.80 | 510, 1.31, 2.67 | 232, 3.11, 10.05 | 25-37 |
| 4 | 412, 6.35, 22.73 | 374, 3.85, 14.17 | 294, 7.29, 21.98 | 582, 3.36, 20.14 | 24-34 |
| 16 | 555, 22.13, 59.46 | 962, 8.60, 39.18 | 435, 27.84, 62.43 | 630, 11.81, 71.36 | 23-34 |
| 48 | 302, 73.24, 531.81 | 628, 39.48, 265.85 | 409, 63.21, 378.21 | 779, 31.02, 209.72 | 31-57 |
| 96 | 323, 162.45, 919.61 | 459, 105.37, 708.42 | 312, 178.43, 948.48 | 471, 101.65, 709.71 | 34-37 |

Earlier base-only run at load ~21: 1/4/16/48/96 writers = 386/764/701/678/547
writes/s, p50 2.09/4.22/21.12/43.78/102.82 ms (matches the downstream evidence:
flat throughput, linear p50).

Single writer, 1,000 ops, three alternating pairs: base p50 1.30/1.09/1.01 ms,
branch 0.90/0.83/0.96 ms (the 100-op single-writer cells above are noise).

Lock-holder state, 48 writers, sampling `pg_stat_activity` every ~10 ms for
backends `idle in transaction` that hold a lock on `meta`: base 569 of 1,501
samples, branch 0 of 1,600. Wait-event samples at 96 writers: `Client/ClientRead`
61 (base) → 0 (branch); `Lock/tuple` ~70k → ~11.6k; `Lock/transactionid` ~20k → ~43k (the queue shifted from tuple to
transaction waits; cause not investigated); `LWLock/LockManager` ~1.3k → ~9k.

Repository benchmark `bin/benchmark.py --backends pg --access-types api
optimized-api --workloads writes mixed --format json` (own disposable
container, single client, 3x100 ops), two runs each:
optimized-api/writes median ops/s base 803/338, branch 530/630; api/writes
base 102/97, branch 107/89. Single-client results swing 2x between identical
runs on this host, so they show no regression signal either way; the
1,000-op single-writer probe above is the cleaner single-client comparison.

### Expected downstream effect

The section that serialized writers shrank from "two client round trips plus
client scheduling plus commit" to "server execution plus commit". Throughput
now rises with writers until the serial commit (WAL flush under the row lock)
or host CPU saturation binds; on this host that was ~1.5-2x at 16-48 writers
and ~1.45x at 96, with p50 roughly halved. Stale-candidate retries (about 20%
extra `UPDATE meta` in the Weft trace) are gone. What can hide the gain:
WAL-flush latency of the database host; CPU oversubscription of the client host
(lock hand-off needs the next waiter scheduled; `LWLock/LockManager` grew);
per-commit `pg_notify` fan-out, which PostgreSQL performs inside the commit
while row locks are still held (NOTIFY track); `keep_newest`, broadcast, and
exact-insert paths, which still hold the row lock across round trips; a
downstream pin to an older core or extension; and a psycopg build against
libpq older than 14 (sequential transport).

## Follow-up proposals

1. **Single-statement write with "messages first" lock order** (direction (a)).
   Removes the residual send gap. Requires broadcast and rename to take
   `SHARE ROW EXCLUSIVE` before the row lock, `keep_newest` to prepare its table
   lock before `generate_timestamp()` in core, and exact insert to take
   `ROW EXCLUSIVE` on `messages` before advancing `meta`; every future
   row-locking path must follow the inverted rule. Owner decision.
2. **Pipeline `keep_newest`** as `[row lock, SHARE ROW EXCLUSIVE, allocate+insert,
   guarded claim]`; the claim must not run when allocation refuses, and the
   CTE snapshot does not see the new row, so the cutoff needs its own proof.
3. **Pipeline exact insert and broadcast allocation** the same way where their
   statements do not depend on each other's results.
4. Throughput beyond one WAL flush per write would need a different
   visibility rule (for example readers bounded by the oldest in-flight
   allocation) — a contract-level change to checkpoint visibility, recorded
   only as a direction, not proposed.

## Fresh-eyes review

Re-read as a new engineer after implementation. Changes made: the lock-order
test now calls the hook (the `write()` version could not fail under the
mutation it exists for); Redis wording in implementation docs 07/08 corrected
(Redis commits exactly its reservation; only PostgreSQL publishes a higher
ID); the concurrency test's sensitivity is stated as probabilistic with the
deterministic proofs named. Overengineering check: the sequential transport
is kept because without it a libpq < 14 installation would lose PostgreSQL
writes entirely; it reuses the same statements. No new abstraction beyond one
optional hook, one runner method, and one generator method.

