# Project-target validation once per process session

Class: 5 — [SB-API-11] normative text changes (project-target admission granularity); the [DOM-5] risky trigger "changes a public contract" also fires, so the hardening checklist applies.
Status: active — implementation, spec promotion, and verification on branch `perf/sb-validate-once`; cross-family review pending (coordinator-run).
Owner: SimpleBroker product owner decides; implementation agent authored the branch.
Boundary: project-scoped service-target setup on the persistent (process-session) Queue path only. Ephemeral, injected-runner, `open_broker()`, explicit service targets, direct `_initialize_project_backend_target()` calls, and SQLite admission are unchanged.
Verification: RED/GREEN real PostgreSQL connection-count proofs, counting-backend lifecycle proofs, targeted core and PostgreSQL suites, lint/format/type gates, before/after connection and catalog-statement measurement on disposable `postgres:18`.
Required action: owner accepts or rejects the admission-granularity change recorded in the difference register (row D1) before landing.

## Goal

- [x] A persistent Queue (or `BrokerSession`-minted Queue) on a project-scoped
  service target performs live target validation at most once per process
  session, instead of once per handle.
- [x] An uninitialized, foreign, or incompatible schema is still rejected on
  the first use in each process session, and a failed admission is never
  recorded.
- [x] The record never crosses session keys (target, backend options,
  configuration snapshot) and ends with the session (last lease release,
  explicit close, `close_process_broker_sessions()`, fork).
- [x] Before/after measurement of connections and catalog statements per N
  new handles on a disposable PostgreSQL 18 server.

Motivation (downstream evidence, not a gate): Weft task processes on
PostgreSQL 18 opened about seven dedicated validation connections each, one
per persistent handle, each with five catalog statements (~5 ms). The change
stands on SimpleBroker's own contract: the process session already owns the
target's backend resources and their setup state ([REV-THEORY-004],
[REV-THEORY-006]); the per-handle repeat is not a contract any SimpleBroker
test or document requires.

## Source Documents

Consulted (read order of `docs/agent-context/context.index.yaml`):
`docs/program-theory.md` [THEORY-2], [THEORY-3] (process session and broker
target rows), [THEORY-4] (explicit safety over magical recovery; concrete
pressure justifies growth), [REV-THEORY-004], [REV-THEORY-006];
`docs/agent-context/README.md`, `docs/agent-context/decision-hierarchy.md`,
`docs/agent-context/principles.md`, `docs/agent-context/engineering-principles.md`;
runbooks `docs/agent-context/runbooks/writing-plans.md`,
`docs/agent-context/runbooks/hardening-plans.md`,
`docs/agent-context/runbooks/testing-patterns.md`;
`docs/lessons.md` Golden Rules and post-watermark entries (2026-08-26 fallback
schema read; 2026-09-07 handle snapshot identity; 2026-09-15 per-handle close
cannot infer session lifetime; 2026-10-14 fork guard before inherited locks).

Source specs:
- `docs/specs/16-python-library-api.md` [SB-API-3] (process session handle,
  shared session key) and [SB-API-11] (project setup admission, completion
  marker, mixed-version guarantee, fork policy)
- `docs/specs/product-section-registry.md` row "Python library / embedding API surfaces"

Implementation rationale: `docs/implementation/06-process-session-core-ownership.md`
("Ownership", "Project-scoped service bootstrap coordination", "Fork recovery
before process-owned locks").

No `MODULE-THEORY.md` exists under `simplebroker/` or `extensions/simplebroker_pg/`.

## Spec Baseline

- `5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` — `docs/specs/16-python-library-api.md`
  at plan authoring time.
- Plan type: implementation with spec revision. Promotion strategy B
  (atomic): requirement text, verification-row links, code, tests, and the
  implementation-doc update land in one branch commit.
- Promotion baseline: the branch commit that carries this plan (see
  `## Review and execution record`).

## Context and Key Files

Current flow at baseline (verified by reading and by measurement below):

- `simplebroker/db.py` `DBConnection._ensure_project_target_initialized()`
  holds a per-manager flag `_project_setup_complete` and lock
  `_project_setup_lock`. Every new `DBConnection` (one per persistent Queue,
  one per ephemeral operation) calls `_initialize_project_backend_target()`
  on first use when the target is a non-SQLite project target
  (`config_path is not None`).
- `_initialize_project_backend_target()` runs `PhaseLockService.run_phases()`
  with `completion_is_valid=initialized_target_is_current`, which calls
  `plugin.validate_target(..., verify_initialized=True)`. A present marker is
  only a hint; live validation decides. Validation failure (`DatabaseError`)
  reruns the phase (`initialize_target`); `OperationalError` propagates.
- `extensions/simplebroker_pg/simplebroker_pg/validation.py`
  `inspect_schema()` opens a dedicated autocommit connection (`connect()`) and
  issues `information_schema.schemata`, `pg_class/pg_namespace`,
  `SET search_path`, `information_schema.columns`, `SELECT magic,
  schema_version FROM meta` — one connection and five statements per call.
- `simplebroker/_broker_session.py` `_ProcessBrokerSession` is the
  process-local owner of one session key's runner/pool and thread cores
  (`_SessionKey` = pid, backend name, normalized target, frozen backend
  options, frozen config snapshot). The registry recovers after fork by
  abandoning inherited entries.
- `DBConnection._ensure_shared_process_owner()` rejects inherited SQL sessions
  and re-acquires child sessions for direct backends (resetting the
  per-manager setup flag) before any project setup.
- The session runner already caches schema setup for its lifetime
  (`PostgresRunner.run_exclusive_setup(SetupPhase.SCHEMA, ...)`); later cores
  on that runner do not re-bootstrap. The per-handle project validation is
  therefore the only per-handle schema work on the persistent path.
- SQLite never enters project setup (`backend_name == "sqlite"` returns);
  its admission is the scalar proof read on each new core's own connection
  ([SB-API-11] SQLite marker paragraph). No dedicated connection exists to
  remove.

Edit points: `_ProcessBrokerSession` (new record),
`DBConnection._ensure_project_target_initialized()` (consult the session record
on the shared path), spec [SB-API-11] and verification table, implementation
doc 06, `CHANGELOG.md` `[Unreleased]`, tests in
`tests/test_process_broker_session.py` and a new
`extensions/simplebroker_pg/tests/test_pg_project_admission.py`.

## Invariants and Constraints

1. The first use of a project service target in each process session performs
   live validation (or setup when validation fails) before any core for that
   session is created. No skip without a successful admission in the same
   session.
2. A failed admission (any exception, including `OperationalError`,
   `DatabaseError`, `StopException`) is never recorded; the next first use
   retries it.
3. The record is keyed exactly by the process-session key. Different targets,
   credentials (target string or config), schemas (backend options), or
   configuration snapshots never share a record.
4. The record ends with the session object: last lease release, `BrokerSession`
   close of the last lease, `close_process_broker_sessions()`, and fork
   (inherited sessions are abandoned or rejected; a child session starts
   unrecorded).
5. `_initialize_project_backend_target()` itself is unchanged: every direct call
   still validates live (existing tests call it twice in one process).
6. Ephemeral, injected-runner, and `open_broker()` managers keep their
   per-manager behavior. SQLite admission is untouched.
7. No new blocking: admission still serializes only on the existing per-manager
   lock and the cross-process phase lock, so stop-event cancellation keeps its
   current reach. Two first users racing on different handles may both
   validate, as today.
8. No backend API, plugin seam, schema, or storage format change; no
   `backend_api_version` bump.

## Observable Difference Register

| # | Change | Class | Theory basis |
|---|--------|-------|--------------|
| D1 | A persistent handle opened on a live process session after that session's successful admission no longer re-runs live validation. If another process drops, replaces, or migrates the target after admission, such a new handle no longer detects it (or re-initializes a dropped schema) at its first use; the session's existing handles never did. Detection and repair still happen on the next process session (any new process, or this process after its last lease ends). | D (narrow) — owner decision requested | [REV-THEORY-004]/[REV-THEORY-006]: a Queue is a capability over the session's resolved target, not the owner of a backend stack; admission is a fact about the session's use of the target, like its runner's schema setup. [THEORY-4] "explicit safety over magical recovery": re-initializing a target dropped under a live process was incidental per-handle recovery, not a named guarantee. [SB-API-11] already scopes the mixed-version guarantee to processes ("stop every old process"; "already-admitted old clients ... are outside"). [SB-OPS-7]: cleanup is not quiescent maintenance. |
| C1 | A persistent Queue reused after `close()` on a new process session now validates once for that new session; at baseline its per-handle flag skipped validation forever. | C — no loss (stricter, one validation per new session) | Same session-ownership basis; removes a per-handle record that outlived the session it described. |
| N1 | Ephemeral, injected-runner, `open_broker()`, explicit service targets, direct `_initialize_project_backend_target()` calls, SQLite. | N | — |
| N2 | Error types, messages, retry policy, phase-lock markers, capacity waiting. | N | — |
| N3 | First handle per session, and every handle when sessions do not overlap, behave exactly as at baseline. | N | — |

## Proposed Spec Delta

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/16-python-library-api.md` | B — atomic | [SB-API-11] paragraph "PostgreSQL project setup treats ..." (append); [SB-API-11] verification row (append tests); `## Related Plans` (add this plan) |

### [SB-API-11] — append to the paragraph ending "can skip setup."

> Project-target setup on the persistent Queue path is admitted once per
> process session. After live validation, or setup when validation fails,
> succeeds for a session, later persistent Queues and `BrokerSession` handles
> sharing that session key reuse the admission instead of validating again.
> A failed admission is not recorded; the next first use retries it. The
> record belongs to the session: it ends when the session ends (last lease
> released, explicit close, or process-wide session shutdown), a forked child
> never inherits it, and session keys that differ in target, backend options,
> or configuration snapshot never share it. A session admitted before another
> process changes the target is an already-admitted client for the
> mixed-version rule below; new sessions validate again. Ephemeral operations,
> injected-runner Queues, and `open_broker()` admit per connection manager.

## Tasks

1. Spec-promotion and plan (this file, index row, spec delta, Related Plans
   backlink). Verify: `bin/check-plan-context`, `bin/check-doc-paths`.
2. RED tests. Verify each fails at baseline for the intended reason
   (validation repeated per handle), and the guard tests pass at baseline.
   - `extensions/simplebroker_pg/tests/test_pg_project_admission.py` (real
     PostgreSQL; count `simplebroker_pg.validation.connect` dedicated
     connections; overlapping handles, minted session Queues, rejection of
     uninitialized/foreign/newer schemas on first use with no latch, second
     schema and second config in one process, session end, explicit reset).
   - `tests/test_process_broker_session.py` counting-backend project target
     (real SQLite under the counting facade, real phase lock): one validation
     per session key, failure not recorded, distinct keys, session end,
     simulated fork through the registry `_getpid` seam.
3. Implement: session record on `_ProcessBrokerSession`; shared-path
   `_ensure_project_target_initialized()` reads/writes it under the existing
   per-manager lock; non-shared path unchanged. Verify GREEN.
4. Docs: implementation doc 06 (ownership row and project bootstrap and fork
   paragraphs), `CHANGELOG.md` `[Unreleased]` "Changed".
5. Gates and measurement (below), commit, execution record.

Stop-and-re-evaluate gates: stop if any existing test depends on a new handle
re-running project setup on a live session (that would be evidence D1 removes
a relied-on capability); stop if admission would need a new in-process lock
that blocks without stop-event observation.

## Testing Plan

- Do not mock the PostgreSQL server, the phase lock, or the session registry.
  Count with module-owned seams only (`simplebroker_pg.validation.connect`,
  plugin `validate_target` wrappers on the counting backend, registry
  `_getpid`).
- Expected values are literals written in the test, not projections of the
  implementation.
- Commands:
  - `uv run pytest tests/test_process_broker_session.py tests/test_project_config.py -n 4`
  - `uv run ./bin/pytest-pg -n 4 extensions/simplebroker_pg/tests/test_pg_project_admission.py extensions/simplebroker_pg/tests/test_pg_ownership.py extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py`
  - one broad PostgreSQL run: `uv run ./bin/pytest-pg -n 4` (shared + extension suites)
  - one broad core run: `uv run pytest -n 4`

## Verification and Gates

`uv run ruff check .`; `uv run ruff format --check` on touched files;
`uv run mypy simplebroker bin/release.py`; extension mypy per
`CONTRIBUTING.md`; `bin/check-plan-context`; `bin/check-doc-paths`;
`python3 bin/check-dom15-fixtures`; `git diff --check`.

Measurement: scratch script (outside the repository) counting
`psycopg.connect`/`Connection.connect` and catalog statements per N=10 new
handles in one process, base tree vs branch tree, on a disposable
`postgres:18` container; cross-checked against `pg_stat_database.sessions`.

## Rollback, Rollout and Success Signals

Rollback: revert the branch commit; no data, schema, marker, or API state is
created, so revert is complete and order-independent. Rollout: core-only
change; extensions need no release, no `backend_api_version` change.
Success signal downstream: dedicated validation connections per process
session drop to one (PostgreSQL `pg_stat_database.sessions` growth and
`log_connections` lines per task process fall by the number of extra handles).
One-way doors: none.

Fatal vs best-effort: admission failure stays fatal to the operation exactly
as at baseline; recording success is a plain attribute write that cannot fail.

## Deviation Log

| Date | Deviation | Disposition |
|------|-----------|-------------|
| 2026-11-08 | Targeted PostgreSQL iterations ran against the disposable measurement container (`SIMPLEBROKER_PG_TEST_DSN` set directly) instead of one `bin/pytest-pg` container per iteration; the broad PostgreSQL gate used `bin/pytest-pg` once. | Same tests and server image; container reuse only saved setup time. |
| 2026-11-08 | `DBConnection._ensure_shared_process_owner()` no longer resets `_project_setup_complete`: shared managers never read that flag now, and the child session starts unadmitted. | Removes a misleading reset; behavior unchanged (fork test passes). |

## Independent Review Loop

- Author family: claude
- Reviewer family: pending — selected by the coordinator from `docs/implementation/03-agent-inventory.md`
- Review invocation: coordinator-run via `skills/call-agent/SKILL.md` (not run by the authoring agent per the track brief)

Review inputs: this plan, [SB-API-3]/[SB-API-11] at baseline and promoted,
the branch diff, the measurement output.

## Out of Scope

- Reusing an already-open pooled connection for the first admission. The
  first admission runs before the session runner exists (setup must precede
  core construction), and the plugin seam `validate_target(target, ...)` takes
  a target string, so reuse needs a backend-plugin seam change. Once-per-session
  admission leaves one dedicated connection per session; recorded as an owner
  option, not built.
- Once-per-process admission for ephemeral operations (no session owns the
  record; a process-global record would outlive every resource that could
  observe target changes).
- Withdrawing the record when the session runner observes missing-schema
  errors (`PostgresRunner.invalidate_bootstrap_state`). Considered for D1;
  not built (couples the backend-agnostic session to runner internals, covers
  only drops observed by an operation).
- Plugin discovery caching, write-lock contention, NOTIFY fan-out, idle pooled
  connections (other tracks).

## Fresh-Eyes Review

Author re-read against the invariants after implementation; findings are
recorded in the execution record.

## Review and execution record

### Cross-family review

(Coordinator fills the cross-family review.)

### Implementation

- `simplebroker/_broker_session.py` `_ProcessBrokerSession.__init__`,
  `project_target_admitted()`, `record_project_target_admitted()`: the
  session-owned record (one bool, set once, never cleared).
- `simplebroker/db.py` `DBConnection._ensure_project_target_initialized()` and
  `_project_target_admitted()`: the shared path reads/writes the session
  record under the existing per-manager lock; other managers keep
  `_project_setup_complete`. `_ensure_shared_process_owner()` drops the dead
  flag reset (deviation log).
- Spec [SB-API-11] paragraph, verification row and Related Plans; implementation
  doc 06 ownership row, project bootstrap paragraph, fork paragraph,
  verification and Related Plans; `CHANGELOG.md` `[Unreleased]` "Changed".

### RED evidence (baseline code, new tests)

- `test_pg_project_admission.py::test_persistent_and_minted_queues_share_one_admission_per_session`:
  `assert 7 == 1` (seven handles, seven inspection connections).
- `::test_unadmittable_schema_is_rejected_at_each_first_use_until_repaired`:
  refusals passed; failed at the repaired-session reuse, `assert 2 == 1`.
- `::test_new_sessions_validate_again`: `assert 1 == 2` (reused handle skipped
  validation on a new session; difference row C1).
- `::test_distinct_session_keys_in_one_process_are_each_validated`: passed at
  baseline (guard).
- `tests/test_process_broker_session.py::test_project_admission_runs_once_per_session_key`:
  three extra `validate` calls; `::test_failed_project_admission_is_not_recorded`:
  one extra `validate`; `::test_project_admission_ends_with_its_session_and_is_not_inherited`:
  passed at baseline (guard).

### Mutation checks (implementation reverted after each)

- Record before admission succeeds: `test_failed_project_admission_is_not_recorded`
  and `test_unadmittable_schema_is_rejected_at_each_first_use_until_repaired` fail.
- Process-wide record ignoring the session key and lifetime: all three core
  tests and three of four PostgreSQL tests fail.

### Gates (current tree)

- `uv run ruff check .` passed; `uv run ruff format --check simplebroker tests extensions/simplebroker_pg` 308 files formatted;
  `uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml` no issues (67 files);
  mypy over `extensions/simplebroker_pg/tests` no issues (32 files); root-test
  mypy on `tests/test_process_broker_session.py` no issues.
- `bin/check-doc-paths`, `bin/check-plan-context`, `python3 bin/check-dom15-fixtures`,
  `bin/coalesce-check`, `git diff --check`: OK.
- Targeted core modules (process session, project config, broker session, DB
  connection lifecycle, fork safety, SB-API contract, plan-context gate)
  `-n 4`: passed.
- Full core suite `uv run pytest -n 4`: passed (no failure or error entries in
  the `-ra` summary; skips only).
- `uv run ./bin/pytest-pg -n 4`: shared suite 1773 passed, 12 skipped;
  extension suite 431 passed, 7 skipped; exit 0.
- `uv run ./bin/pytest-redis -n 4` over Redis plugin edges, validation, pool
  and integration modules: 110 passed.

### Measurement (disposable `postgres:18`, N = 10 handles, one process)

Script outside the repository counts `psycopg.connect`/`Connection.connect`
calls, executed statements, and catalog statements (`information_schema`,
`pg_class`, `pg_namespace`, `pg_indexes`, `pg_depend`, `meta` reads) after the
target is initialized; server-side `pg_stat_database.sessions` agrees.

| Mode | Base connections / statements / catalog | Branch | Per later handle, base → branch |
|------|------------------------------------------|--------|---------------------------------|
| 10 overlapping persistent Queues | 11 / 86 / 43 | 2 / 41 / 7 | 1 conn, 8 stmts, 4 catalog → 0, 3, 0 |
| `BrokerSession` minting 10 Queues | 11 / 86 / 43 | 2 / 41 / 7 | same |
| 10 sequential open-write-close Queues | 20 / 122 / 70 | 20 / 122 / 70 | unchanged (each handle starts a new session) |
| 10 ephemeral writes | 20 / 122 / 70 | 20 / 122 / 70 | unchanged (out of scope) |

Wall time for the `BrokerSession` mode over five runs each: base 151–178 ms
(median 172), branch 72–88 ms (median 76), local loopback. SQLite: 10
persistent handles open one SQLite connection on both trees (no dedicated
validation connection exists there).

### Fresh-eyes notes

- A Queue reused after `close()` re-acquires its session lease inside
  admission; if that admission fails, the lease stays with the handle until
  its next `close()` or finalization, like any reused handle.
- Admission still precedes core creation for every session (invariant 1); the
  record is read without a lock because it is a single monotonic bool and a
  stale `False` only repeats today's validation.
