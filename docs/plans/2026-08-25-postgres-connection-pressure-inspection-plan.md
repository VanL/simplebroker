# PostgreSQL Connection-Pressure Inspection Plan

Status: completed
Class: 5. This work adds a public `Queue.backend_name` property and a public
`simplebroker_pg.get_connection_stats()` package-root API, changes the private
core-to-first-party-extension seam, and requires coordinated core and
`simplebroker-pg` releases. `[DOM-5]`, `[DOM-6]`, `[DOM-11]`, and `[DOM-15]`
therefore require an exact contract delta, risky-plan hardening, real-backend
proof, and independent review before implementation.
Plan type: implementation with spec revision

## Goal and Scope

Add a PostgreSQL-only operational inspection API without adding a capacity
method to the backend-neutral broker protocol. `Queue.backend_name` exposes the
resolved plugin name. A caller that has narrowed it to `"postgres"` may call
`simplebroker_pg.get_connection_stats(queue)`. The extension executes one
read-only catalog statement through the Queue's normal connection lease, core
lock, retry path, and existing runner connection.

The helper returns `sum(pg_stat_database.numbackends)` under the public field
name `numbackends`, plus the configured connection limits. This is a
zero-setup, server-wide pressure signal available to an ordinary PostgreSQL
role under stock catalog permissions. It intentionally includes
database-attached processes such as autovacuum workers that do not consume a
`max_connections` client slot. Callers must treat it as a conservative
admission numerator, not an exact client-connection count. The snapshot is not
a reservation and does not prevent races with other clients.

The helper requires no monitoring-role grant, installed function, schema,
table, or migration. It does not use `Queue.sidecar()`, add a generic backend
capacity protocol, or add methods to SQLite and Redis broker implementations.

## Source Documents

Source specs:

- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-3],
  [SB-API-7], [SB-API-11], and proposed [SB-API-13]
- `docs/specs/product-section-registry.md` Python library / embedding API row
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-6], [DOM-10], [DOM-11], [DOM-15]

Theory:

- `docs/program-theory.md` [THEORY-1] keeps the core small and requires a real
  consumer need. Weft's Manager admission guard is the concrete consumer.
- `docs/program-theory.md` [THEORY-3] assigns substrate-specific behavior to
  the backend. PostgreSQL SQL and parsing stay in `simplebroker_pg`; core owns
  connection lifecycle and serialization.
- `docs/program-theory.md` [THEORY-4] requires explicit failure and compatible
  public evolution. The helper is additive, validates its payload, and names
  the catalog metric instead of overstating its precision.

Downstream source and acceptance owner:

- `../weft/docs/plans/2026-08-25-manager-admission-control-plan.md`. That draft
  is input, not SimpleBroker authority. Its sidecar, `pg_stat_activity`, field,
  and release assumptions must be revised after this plan is accepted.
- `../weft/weft/context.py`, whose `WeftContext.backend_name` already returns
  the canonical `BrokerTarget.backend_name` value.

Primary PostgreSQL references:

- monitoring statistics and `pg_stat_database.numbackends`:
  <https://www.postgresql.org/docs/current/monitoring-stats.html>
- connection settings:
  <https://www.postgresql.org/docs/current/runtime-config-connection.html>

Process and implementation guidance:

- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/implementation/06-process-session-core-ownership.md`

## Baseline and Planning Receipts

Plan and spec baseline: `b8cfa509f8eb373b44416dedbc327b0e66530679`.

Current structure at that baseline:

- core version `7.4.2`, `simplebroker-pg` version `3.9.2`, backend API v7;
- `BrokerTarget.backend_name` is public, while `Queue` has no direct property;
- `Queue.get_connection()` owns persistent versus ephemeral operation leases;
- `BrokerCore` serializes runner operations with `_lock` and applies retry
  policy through `_run_with_retry()`;
- `Queue.sidecar()` and [SB-API-7] are for embedder-owned application tables;
- PostgreSQL and SQLite use shared SQL-backed `BrokerCore`, so a method added
  there would mechanically exist for both backends;
- `PostgresRunner.run()` serializes operations on a leased process-session
  connection with `_leased_operation_lock`;
- each process owns its own psycopg `ConnectionPool`; pool `get_stats()` cannot
  see other Weft processes or unrelated PostgreSQL clients.

The plan-authoring PostgreSQL 18 Docker probe used two unrelated ordinary
login roles and no `pg_monitor` or `pg_read_all_stats` grant:

| Probe | Before unrelated client | During unrelated client | Result |
|-------|-------------------------|-------------------------|--------|
| `sum(pg_stat_database.numbackends)` | 1 | 2 | Counts the unrelated role server-wide without a grant. |
| `pg_stat_activity` filtered by `backend_type = 'client backend'` | 1 | 1 | Omits the unrelated role because `backend_type` is null. |

A second real probe slowed an autovacuum worker. The ordinary role observed
`sum(numbackends) = 2` while a privileged classification found one client
backend and one autovacuum worker. This is the accepted tradeoff: the metric
can block early but does not hide established client pressure.

Additional probes rejected `pg_stat_ssl`, which hides unrelated-role rows,
and psycopg pool statistics, which cover only one in-process pool. Weft uses
spawned processes, so separate processes using the same DSN have separate
pools. An exact client count would require a monitoring grant, installed
`SECURITY DEFINER` function, or external privileged metric. The owner chose
`numbackends` to avoid deployment intervention and accepted overcounting. All
temporary planning containers were removed after the probes.

These receipts support the design; they do not replace repeatable PostgreSQL
15 and 18 tests.

## Spec Baseline

- Baseline `b8cfa509f8eb373b44416dedbc327b0e66530679` covers
  `docs/specs/16-python-library-api.md` and the product registry.
- Plan type: implementation with spec revision.
- Promotion baseline: record the reviewed spec diff identifier after Task 1.
  The active schema-remediation plan touches the same API spec. Rebase exact
  anchors after that plan closes; semantic changes require a deviation row.

## Proposed Spec Delta

Promotion strategy: **B - atomic spec, code, firing tests, and backlinks**.
The property and helper do not exist at baseline, so the promise must not land
ahead of implementation.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/16-python-library-api.md` | B - atomic | [SB-API-1], [SB-API-3], new [SB-API-13], verification, Related Plans |
| `docs/specs/product-section-registry.md` | B - atomic | Python library / embedding API range and gate |

### [SB-API-1] - add the narrow PostgreSQL surface

Add this row to the supported import-surface table:

> | `simplebroker_pg.get_connection_stats` | First-party PostgreSQL-only
> operational inspection. It belongs to the separately installed
> `simplebroker-pg` distribution and is not a portable core or cross-backend
> operation. |

Do not add a general rule canonizing every present or future package-root name
from every first-party extension. This change supports only the named helper
and the extension's already documented exports.

### [SB-API-3] - append to Queue lifecycle

> `Queue.backend_name` is a read-only string containing the resolved backend
> plugin name. Built-in names are `"sqlite"`, `"redis"`, and `"postgres"`;
> third-party plugin names are not restricted to that set. The property follows
> the same resolution owner as Queue operations, including a backend-aware
> injected runner, and performs no database I/O. `"pg"` is not an alias.

### New section [SB-API-13] - first-party PostgreSQL inspection

Insert before the cross-surface matrix:

> ## First-party PostgreSQL inspection [SB-API-13]
>
> `simplebroker_pg.get_connection_stats(queue) -> dict[str, int]` is a public
> PostgreSQL-only helper. The caller narrows on
> `queue.backend_name == "postgres"`; the helper rejects any other backend
> with `ValueError` before opening a connection. It is absent from the generic
> `BrokerConnection` protocol and has no SQLite or Redis implementation.
>
> The fresh returned dictionary has exactly these keys:
>
> - `numbackends`: `sum(pg_catalog.pg_stat_database.numbackends)` across the
>   server, including the helper's existing connection. Under stock catalog
>   permissions it counts established database-attached backends across roles
>   and databases. It can include autovacuum and other workers that do not
>   consume `max_connections`; it is a conservative pressure signal, not an
>   exact client-connection count.
> - `max_connections`: the server's configured limit.
> - `superuser_reserved_connections`: reserved superuser slots.
> - `reserved_connections`: PostgreSQL 16+ general reserved slots, or zero
>   when that setting does not exist.
>
> The helper requires exactly one one-column row containing a keyed JSON object
> with those fields. Every value has exact Python type `int`, not `bool`.
> `max_connections` is positive, other values are non-negative, and reserve
> values sum to less than `max_connections`. There is no
> `numbackends <= max_connections` validation because included workers can
> make it false. Malformed results raise `ValueError`; execution and permission
> failures retain SimpleBroker's public `DatabaseError` hierarchy.
>
> The metric needs no monitoring-role grant or installed object under stock
> permissions. A deployment that revokes catalog access may receive a database
> permission error; the helper neither grants access nor falls back to a
> narrower same-role count.
>
> The helper executes one parameter-free, read-only catalog statement through
> the Queue's operation lease and SQL-backed core lock/retry path. It opens no
> explicit transaction and does not use sidecar. A target-resolved persistent
> Queue reuses its process-session connection on that thread. An ephemeral
> Queue may open one connection. An injected runner is functionally supported
> but gains no stronger checkout-retention promise from `persistent=True`.
>
> The result is a non-atomic observation, not a permit. New connections can
> arrive after the statement; consumers must retain a safety margin and must
> not promise that admission cannot overshoot a PostgreSQL limit.

Change API ranges from `[SB-API-1]`...`[SB-API-12]` to
`[SB-API-1]`...`[SB-API-13]` in the registry, `docs/agent-kernel.md`, and
`docs/guides/python.md`. Add the new extension test to the registry gate.

## API and Architecture

Public use:

```python
from simplebroker_pg import get_connection_stats

if queue.backend_name == "postgres":
    pressure = get_connection_stats(queue)["numbackends"]
```

Do not add `Queue.get_connection_stats()`, a `BrokerConnection` method,
conditional dynamic attributes, or a portable capacity type. The import path
is the capability boundary; `backend_name` is not a closed enum.

Connection flow:

```text
caller
  -> simplebroker_pg.get_connection_stats(queue)
  -> Queue.get_connection() operation lease
  -> BrokerCore._run_backend_probe(...) private first-party seam
  -> fork/poison/open-batch checks
  -> BrokerCore._lock
  -> BrokerCore._run_with_retry(...)
  -> PostgresRunner.run(..., fetch=True)
  -> existing leased psycopg connection for a target-resolved persistent Queue
```

Read-only removes explicit transaction and PostgreSQL write-lock needs. It does
not remove SimpleBroker's connection lock. That lock protects connection
lifecycle, same-core overlap, retry/error state, and open-batch isolation.

Add a private SQL-backed `BrokerCore` method such as:

```python
def _run_backend_probe(
    self,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[tuple[Any, ...]]: ...
```

It checks fork safety and poison state, refuses same-thread at-least-once batch
re-entry, holds `BrokerCore._lock`, calls `runner.run(..., fetch=True)` through
`_run_with_retry()`, and materializes rows before unlock. It is a trusted
first-party SQL extension seam, not public raw SQL. Do not add it to
`BrokerConnection`, `simplebroker.ext`, Redis, or plugin protocols. The new PG
release raises its core floor. `BACKEND_API_VERSION` remains 7.

The extension may use a private structural protocol for this underscore
method. It must not import `BrokerCore` or inspect the runner.

Private statement in `simplebroker_pg._sql`:

```sql
SELECT pg_catalog.jsonb_build_object(
    'numbackends',
    COALESCE(
        (SELECT sum(numbackends)::bigint
         FROM pg_catalog.pg_stat_database),
        0
    ),
    'max_connections',
    current_setting('max_connections')::integer,
    'superuser_reserved_connections',
    current_setting('superuser_reserved_connections')::integer,
    'reserved_connections',
    COALESCE(current_setting('reserved_connections', true), '0')::integer
)
```

Qualify the catalog view and JSON constructor. Keep it parameter-free. Confirm
on PG15 and PG18 that an ordinary role sees unrelated-role backends and the
optional setting becomes zero before PG16. Do not introduce or advertise a
new server-version floor.

## Context and Key Files

Contract and rationale:

- `docs/specs/16-python-library-api.md`, product registry, agent kernel, and
  Python guide: property/helper contract, API range, example, and gates.
- `docs/implementation/06-process-session-core-ownership.md`: Queue lease,
  core lock, process-local pool limit, and connection lifetime rationale.
- `CHANGELOG.md`: property, helper, `numbackends` caveat, zero-setup
  expectation, and version floors.

Core and extension:

- `simplebroker/sbqueue.py`: one shared backend-resolution owner for
  `Queue.backend_name` and `_activity_waiter_identity()`.
- `simplebroker/db.py`: private materialized probe on SQL-backed `BrokerCore`.
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`: private statement.
- `extensions/simplebroker_pg/simplebroker_pg/connections.py`: helper and
  private typing protocol.
- `extensions/simplebroker_pg/simplebroker_pg/__init__.py`: export only the
  helper in addition to current names.
- `extensions/simplebroker_pg/README.md`: field precision, permissions,
  connection lifecycle, and errors.
- `extensions/simplebroker_pg/pyproject.toml`: released core floor.

Tests and release infrastructure:

- Queue identity tests, Python API contract tests, and public-surface tests.
- `extensions/simplebroker_pg/tests/test_connection_stats.py`: parser,
  lifecycle, cross-role/database, versions, and connection reuse.
- `.github/workflows/test-pg-extension.yml`: focused PG15 run; normal PG18.
- `tests/test_release_script.py`: backend API v7, dependency floor, docs.
- lockfiles only through repository release and `uv lock` paths.

## Required Comprehension Gate

Record correct answers before implementation:

1. A read-only query still needs `BrokerCore._lock` because the lock owns
   connection lifecycle, close/poison, retry/error, and batch boundaries.
2. `numbackends` crosses roles/databases without a monitoring grant but can
   count autovacuum and other workers outside `max_connections`. It is not an
   exact client count.
3. Psycopg pool stats are insufficient because pools are process-local; other
   Weft processes and unrelated clients are invisible.
4. Only a target-resolved process-session persistent Queue has the held
   checkout reuse promise. An injected runner has a weaker contract.
5. Backend API stays v7 because the new core method is private and consumed
   under a new extension core floor; no required backend protocol changes.

Missing or incorrect answers block implementation pending source rereading.

## Invariants and Constraints

1. No generic broker/plugin protocol gains a capacity/statistics method.
2. SQLite and Redis do not implement or expose the helper.
3. `Queue.backend_name: str` performs no I/O and is not a closed enum.
4. Canonical PostgreSQL is `"postgres"`; no `"pg"` alias.
5. SQL/parsing remain private to the PG extension; core sees a trusted probe.
6. Non-PG rejection occurs before connection acquisition.
7. One statement uses the normal lease, core lock, retry, and runner path.
8. Persistent target Queue reuse, ephemeral lifecycle, and injected-runner
   limitations remain explicit.
9. Open at-least-once batch re-entry is refused before SQL execution.
10. Payload and returned dict have exactly four integer fields; order is free.
11. `numbackends` is the unfiltered server-wide catalog sum, includes the
    observer, and may include non-client workers.
12. No validation/docs claim exactness, `numbackends <= max_connections`, or
    hard admission safety.
13. No monitoring role, function, schema, grant, or bootstrap is attempted.
14. PG15 yields `reserved_connections = 0`; PG16+ reads the setting.
15. Database failures remain `DatabaseError`; malformed data is `ValueError`.
16. No schema, table, sidecar, message, or configuration state is written.
17. [SB-API-7] sidecar semantics do not widen.
18. Backend API remains v7; only the new PG helper release needs the new core.
19. Existing core and PG exports remain intact.
20. Pool, watcher, listener, schema, and transaction refactors are out of scope.
21. Until the core minor is published, the local PG package version and core
    dependency floor remain at the already published `3.9.2` baseline. The
    core release driver reads that local version, requires it on PyPI, and
    synchronizes the root `pg` extra to it. Bumping PG early dead-ends the
    core-only release.

## Rollout, Rollback, and One-Way Doors

There is no database one-way door: no schema, role, grant, or state is added.
Before publication, core, extension, spec, and docs can revert together.

Rollout order:

1. land the reviewed implementation while keeping
   `extensions/simplebroker_pg/pyproject.toml` at the published `3.9.2`
   version/floor baseline;
2. dry-run, publish, and clean-install the next free core minor with the
   property/private probe (`7.5.0` at baseline). Confirm the release driver
   resolves the already published PG `3.9.2` baseline;
3. only after the core artifact exists, prepare the next free PG minor
   (`3.10.0` at baseline), raise its core floor to `>=7.5.0`, dry-run, publish,
   and clean-install it;
4. synchronize the root `pg` extra and locks. If that floor changed after the
   core minor, dry-run and publish a
   core metadata patch (`7.5.1` at baseline);
5. only then raise Weft floors and enable its opt-in guard. There is no database
   bootstrap prerequisite.

Before release, compare with current `bin/release.py` and workflows. In
particular, recheck `read_pg_extension_version()`,
`require_published_pg_baseline()`, and `sync_root_pg_extra_dependency()`.
Rebaseline claimed versions and run each dry-run only at its rollout phase,
including the conditional metadata patch. PyPI is irreversible;
post-publication rollback is a disabled/reverted Weft guard plus corrective
releases, never moved tags.

Post-release signals:

- clean artifacts import the property and helper;
- ordinary PG15/18 roles succeed without monitoring grant or installed object;
- unrelated clients raise/lower `numbackends`;
- an autovacuum probe demonstrates documented overcount;
- Weft reports compatible artifacts and retains its safety margin.

## Dependency-Ordered Tasks

### 1. Rebase and promote the public contract atomically

- Wait for or receive handoff from the overlapping schema-remediation plan.
- Re-read current specs, registry, release driver, versions; record a fresh
  baseline and all five comprehension answers.
- Add failing [SB-API-3]/[SB-API-13] structural tests.
- Land the Proposed Spec Delta with Tasks 2 and 3 in one atomic slice, including
  navigation, verification, rationale, README, changelog, and backlinks.
- Re-plan if implementation needs a generic protocol, closed enum, or sidecar.

### 2. Add Queue identity and the private core probe

- Share one backend-resolution owner between `Queue.backend_name` and activity
  waiter identity across targets and injected runners.
- Add `_run_backend_probe()` with fork, poison, batch, lock, retry, and
  materialization behavior. Do not export, protocolize, or add it to Redis.
- Add red-green no-I/O identity, serialization, retry, materialization, and
  open-batch refusal tests.
- Stop if raw psycopg access or another lock is required.

### 3. Add the PostgreSQL helper and validation

- Add the private SQL and `connections.py` helper.
- Reject non-PG before `Queue.get_connection()`; then acquire the Queue lease,
  narrow to the private protocol, execute once, and validate exact shape/types
  and numeric invariants.
- Accept `numbackends > max_connections`; add no role/database/process filter.
- Return a fresh dict. Do not expose SQL, catch `DatabaseError`, or inspect pool
  internals in production.
- Export only `get_connection_stats` in addition to current root names.

### 4. Prove PostgreSQL scope, versions, and connection reuse

- On PG18, use two unrelated ordinary roles without monitoring membership.
  Prove one role sees the other's connection in the same and another database.
- Assert caller is not superuser/member of `pg_monitor` or
  `pg_read_all_stats`; install no function and perform no grant.
- Add an isolated slow-autovacuum acceptance probe proving worker overcount.
  Preferred proof uses a dedicated serial Docker server with
  `autovacuum_naptime=1s`, one worker, high cost delay, and low cost limit. A
  test table uses zero vacuum threshold/scale factor; insert/delete enough rows,
  then poll with the privileged fixture role for that table's
  `backend_type = 'autovacuum worker'` within a fixed timeout. While the worker
  is present, call the public helper as the ordinary role and compare its
  `numbackends` with a privileged client-backend classification.
  If this cannot be made stable in CI after recorded attempts, the only
  permitted replacement is a three-part proof: an exact static/mutation test
  that preserves the unfiltered `sum(numbackends)` SQL, a version-pinned
  PostgreSQL source assertion showing database-attached autovacuum workers feed
  the counter, and the same real Docker probe recorded as a bounded manual
  pre-release acceptance command. Do not silently drop the edge or substitute
  a mocked catalog row.
- Use test-only pool stats or an equivalent direct receipt to prove a
  target-resolved persistent Queue takes no second checkout. Also prove
  ephemeral cleanup and injected-runner functionality without stronger claims.
- Add PG15 coverage for absent `reserved_connections`; retain PG18 for present.
- Cover malformed JSON, key/type errors, negatives, zero max, invalid reserve
  sum, `numbackends > max_connections`, and key-order independence. Only parser
  and lock/retry cases may use minimal fakes.

### 5. Run downstream compatibility and revise the handoff

- Run Weft's SimpleBroker compatibility suite without changing its floors.
- Hand off these required Weft-plan changes:
  - sidecar becomes the helper's private-core path;
  - `current_connections` becomes `numbackends` with explicit overcount;
  - `pg_stat_activity`/WAL classification becomes unfiltered
    `pg_stat_database` sum;
  - no monitoring grant or installed object prerequisite;
  - extension-only release becomes core-then-PG rollout.
- Require a safety margin and forbid exact/no-overshoot claims. The Weft owner,
  not this plan, applies and reviews its plan delta.
- Before either repository begins implementation from the existing Weft plan,
  require its owner to mark that reviewed draft as reopened, supersede it, or
  patch its normative architecture/tasks and invalidate the stale review
  receipt. The revised Weft plan must receive a fresh independent review. A
  prose handoff beside an unchanged “reviewed” plan is not sufficient.

### 6. Release exact SHAs and close the plan

- Keep PG at the published baseline through the core dry-run and publication.
  Bump its version/floor only after the core artifact exists. Dry-run each
  actual next version at that exact phase, including any metadata patch.
- Run prechecks on exact release SHAs; tag only after exact-SHA CI passes.
- Publish core, PG, then metadata patch if needed; wait for each artifact.
- Clean-install outside the source tree and run imports, identity, PG15/18
  ordinary-role, cross-role, overcount, and reuse probes.
- Record SHAs/artifacts/results/residual risk. Close the index row only after
  publications and downstream acceptance receipts exist.

## Testing Plan

### Firing test inventory

| Contract element | Required firing proof |
|------------------|-----------------------|
| Queue identity | Built-ins, third-party name, injected runner, and no-I/O behavior. |
| Non-PG rejection | SQLite raises `ValueError` before creating a DB path. |
| No generic method | No core export or `BrokerConnection` requirement. |
| Probe safety | Lock serialization, retry/error identity, batch refusal, materialization under lock. |
| Payload | Row/column, mapping, exact keys/types, max/reserve/nonnegative checks. |
| Ordinary access | Real role without monitoring membership or install succeeds. |
| Cross-role/database | Unrelated client in same/other DB raises `numbackends`. |
| Conservative overcount | Slow autovacuum can raise `numbackends`. |
| No false exactness | `numbackends > max_connections` remains valid. |
| Version behavior | PG15 missing optional setting; PG18 present setting. |
| Lifecycles | Persistent no-extra-checkout, ephemeral cleanup, injected runner. |
| Packaging | Clean wheel/sdist export; core floor; backend API exactly 7. |

Anti-mocking:

- Do not mock Queue/core/runner/pool/catalog/permissions in scope, reuse, or
  lifecycle proofs.
- Use real PG18 for ordinary access, cross-role/database, overcount, reuse; real
  PG15 for the missing setting.
- Minimal fakes are only for malformed payload and lock/retry unit cases.
- Prove no superuser, `pg_monitor`, `pg_read_all_stats`, or installed function.
- Do not access application tables in the probe.

Adversarial probes include unknown plugin names, raw/resolved targets,
backend-aware/non-aware runners, non-PG no-side-effect rejection, malformed and
huge JSON integers, `numbackends` above max, reserve sum at max, revoked catalog
access, live batch re-entry, retry/terminal failures, all Queue lifecycles,
unrelated-role clients in two databases, autovacuum, PG15, and PG18.

## Verification and Gates

```bash
uv run pytest \
  tests/test_backend_plugin_resolution.py \
  tests/test_queue_connection_manager.py \
  tests/test_python_library_api_contract_sb_api.py \
  tests/test_public_surface.py -q

SIMPLEBROKER_PG_TEST_IMAGE=postgres:18 \
  uv run ./bin/pytest-pg --fast -n0 -q \
  extensions/simplebroker_pg/tests/test_connection_stats.py
SIMPLEBROKER_PG_TEST_IMAGE=postgres:15 \
  uv run ./bin/pytest-pg --fast -n0 -q \
  extensions/simplebroker_pg/tests/test_connection_stats.py

uv run ruff check simplebroker tests \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests
uv run ruff format --check simplebroker tests \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests
uv run mypy simplebroker tests \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests --config-file pyproject.toml
uv build
uv build --package simplebroker-pg
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
uv run pytest tests/test_doc_gates.py tests/test_plan_context_gate.py -q

uv run pytest
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
```

Core-release phase, while local PG remains at published `3.9.2`:

```bash
python3 bin/release.py core --dry-run --version 7.5.0
```

PG-release phase, only after the core minor is published and the PG version
and dependency floor have been prepared:

```bash
python3 bin/release.py pg --dry-run --version 3.10.0
```

Conditional root-extra metadata phase, only after PG `3.10.0` is published:

```bash
python3 bin/release.py core --dry-run --version 7.5.1
```

The last phase is conditional on retaining the metadata patch. Rebaseline
claimed versions. Never run these as one pre-publication block: the release
driver's published-baseline gates make the ordering executable. Record changed
files, commands, results, and residual risk after each slice. Do not commit
merely to satisfy this plan when the owner has requested uncommitted review.

## Independent Review Loop

1. Plan review checks named files/callables, `numbackends` precision, lock,
   protocol non-change, zero-setup claim, versions, and all artifact dry-runs.
2. Core review rejects public raw SQL, duplicate resolution, unlocked access,
   or an unsupported backend API bump.
3. Extension review checks SQL, parsing, ordinary-role scope, PG15/18,
   autovacuum caveat, and checkout receipts.
4. Pre-release review checks the full diff, specs, exports, floors, dry-runs,
   Weft handoff, changelog, rollback, and exact-SHA evidence.

Record reviewers, findings, dispositions, and reruns. P0/P1 blocks progress.

## Independent Review Record

The first complete-draft review found:

1. **P1:** a general package-root rule canonized every backend export.
   **Fixed:** [SB-API-1] now names only this PG helper.
2. **P1:** privilege acceptance and direct-grant rollout conflicted.
   **Superseded:** owner selected unprivileged `numbackends`; all grants and
   privilege receipts are removed.
3. **P2:** PG12+ was claimed but only PG15/18 tested. **Fixed:** removed the
   floor claim; explicit tests do not create a new support promise.
4. **P2:** a conditional core metadata patch lacked a dry-run. **Fixed:** added
   the third dry-run and made retaining the step contingent on it.

The fresh review of the `numbackends` revision found:

5. **P1:** the core-first rollout could dead-end if the local PG version were
   bumped before the core release driver checked its published baseline.
   **Fixed:** added an invariant and phase gates that keep PG at published
   `3.9.2` through the core minor, then prepare PG, then the optional metadata
   patch.
6. **P2:** a handoff did not prevent the already-reviewed Weft plan from being
   used with superseded architecture. **Fixed:** Weft must explicitly reopen,
   supersede, or patch and invalidate the stale review, then receive a fresh
   independent review before implementation.
7. **P2:** the autovacuum firing proof lacked a deterministic recipe/fallback.
   **Fixed:** specified dedicated-server settings, trigger/poll/classification,
   and the only permitted source-pinned plus manual-probe fallback.

The 2026-08-26 final independent recheck verified all three dispositions
against `bin/release.py`, the current Weft plan, and the hardened autovacuum
proof. It found no remaining P0, P1, or P2 inconsistency. The plan may move to
`active`; implementation remains dependency-gated by Task 1 and the formal
Weft-plan reopen fence in Task 5.

The integrated implementation review found two P2 proof gaps. First, one
server-visible backend did not by itself prove that a persistent runner kept
the same pool checkout. The real-backend test now records the psycopg pool's
`requests_num` and exact leased connection before two helper calls and proves
both remain unchanged, while retaining server-visible close-to-zero evidence.
Second, the ordinary-role cross-role/database test skipped under the normal
PG18 xdist job. The Python 3.13 PG18 isolated serial workflow now runs the
entire connection-statistics file with the autovacuum probe enabled. PG15
retains its separate full serial file. Both version runs passed, and the fresh
re-review found no remaining P0-P2 finding.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Task 1 overlap gate | Wait for or receive handoff from the active schema-remediation plan before implementation. | The owner requested implementation while that plan's uncommitted work remained in the same worktree. Feature edits were kept surgical and verified with the combined full suites. | Waiting would not reduce file overlap because both changes were already present locally; preserving the other plan's edits and proving the integrated tree was the safer executable gate. | None; public behavior still matches [SB-API-3] and [SB-API-13]. |
| Task 6 publication order | Publish core, wait for it, then publish PG and any needed core metadata patch. | By owner direction, the final corrective core, PG, and Redis versions were prepared at one exact SHA and released by one `release.py all` batch. Core `7.5.1` already carried the final `simplebroker-pg>=3.10.0` extra and PG `3.10.0` required `simplebroker>=7.5.1`, so no later metadata patch was needed. | The earlier `v7.5.0` core tag was retained as an unpublished incident tag. Exact-SHA gates, local artifact smoke, immutable tags, and all three publication gates passed before closure. Concurrent publication created only a brief dependency-availability window; all matching artifacts are now public and clean-install together. | None; published dependency floors and API contracts match the plan's final state. |

## Execution Log

- 2026-08-25: authored from baseline
  `b8cfa509f8eb373b44416dedbc327b0e66530679` after PG18 cross-role,
  autovacuum, role-visibility, `pg_stat_ssl`, and pool-scope probes.
- 2026-08-26: owner selected `sum(pg_stat_database.numbackends)` to avoid
  database grants/objects and accepted worker overcount. Revised to expose
  `numbackends`, remove monitoring/bootstrap requirements, and retain the
  same-connection private-core path. Removed the temporary
  `SECURITY DEFINER` probe container. No feature code or public spec changed.
- 2026-08-26: PostgreSQL 15 and 18 Docker probes executed the exact planned SQL
  as an ordinary role. An unrelated role connected to another database moved
  `numbackends` from 1 to 2; both versions returned the four-field JSON and
  PG15 returned `reserved_connections = 0`. Temporary containers were removed.
- 2026-08-26: final independent plan review passed after release-order,
  downstream-authority, and autovacuum-proof corrections; no P0-P2 finding
  remained. Status moved from `draft` to `active`.
- 2026-08-26: implemented `Queue.backend_name`, the private locked/retrying
  SQL probe, and the PostgreSQL-only package-root helper. The helper rejects
  non-PostgreSQL Queues before target I/O, runs the unfiltered catalog sum on
  the Queue's existing connection path, validates the exact four-field
  payload, and leaves `BrokerConnection` and backend API v7 unchanged.
- 2026-08-26: added core concurrency/fork/batch/poison/retry tests and
  PostgreSQL parser, lifecycle, ordinary-role, cross-role/database, revoked
  catalog, PG15/18, and isolated autovacuum tests. The focused PG15 and PG18
  files passed; the opt-in PG18 autovacuum probe also passed and demonstrated
  the documented conservative overcount.
- 2026-08-26: revised the reopened Weft Manager admission plan from sidecar
  and `pg_stat_activity` to `Queue.backend_name` plus
  `get_connection_stats()`. A fresh independent re-review approved the
  revision with no remaining P0-P2 findings. Weft's current SimpleBroker
  compatibility selection passed.
- 2026-08-26: verification passed with 3,226 core tests, 1,503 shared PG plus
  291 PG-extension tests, 1,496 shared Redis plus 283 Redis-extension tests,
  full Ruff lint/format, documentation and plan gates, diff checks, and clean
  Python 3.11 wheel/sdist artifact installs. Targeted mypy passed for all
  feature files. Repository-wide mypy remains non-clean because the separate
  schema-remediation worktree changes currently report two redundant casts in
  `bin/benchmark.py` and one Redis test mapping-type error; none is in this
  feature's files.
- 2026-08-26: no release, version/floor bump, tag, or downstream enablement
  was performed. The plan remains `active` for the ordered core,
  PostgreSQL extension, optional metadata-patch, and Weft rollout phases.
- 2026-08-26: final integrated review initially found two P2 test gaps around
  pool-checkout reuse and PG18 serial ordinary-role coverage. Both were fixed;
  real PG18 passed the complete 33-test file including autovacuum, real PG15
  passed 32 tests with only the opt-in autovacuum case skipped, workflow/static
  checks passed, and independent re-review approved with no remaining P0-P2.
- 2026-08-26: the combined pre-release review found two blockers outside the
  inspection helper itself: a supported-old PostgreSQL schema could borrow a
  completed project PhaseLock marker, and a malformed SQLite reserved index
  name could be hidden by another equivalent index. Both were reproduced and
  fixed with firing tests. The final integrated core pass ran 3,227 tests with
  17 documented skips; PostgreSQL ran 1,504 shared plus 291 extension tests;
  Redis ran 1,497 shared plus 283 extension tests; packaging, Ruff, source and
  test mypy, docs, Weft compatibility, and diff gates passed. Re-review found
  no remaining code P0-P2. Release order remains core minor, PostgreSQL minor,
  then the conditional root-extra metadata patch.
- 2026-08-26: prepared a targeted implementation commit from an isolated
  staged-index export. That exact snapshot passed focused core/API tests,
  feature Ruff and mypy, documentation gates, real PG18 including autovacuum,
  real PG15, and Python 3.11 wheel/sdist artifact smoke. Concurrent
  schema-remediation hunks remain outside the commit.
- 2026-08-26: exact batch SHA
  `107bfc617f21ee4d1d197b3cbda3ea783ff86b17` passed hosted core Test run
  `32997358265`, PostgreSQL run `32985912183`, and Redis run `32997360753`.
  The full `release.py all` local gate then passed 3,230 core tests plus 14
  benchmarks, 1,504 shared PG plus 291 PG-extension tests, 1,497 shared Redis
  plus 283 Redis-extension tests, 119 examples, static/type/lock checks, and
  Python 3.11 artifact smoke.
- 2026-08-26: immutable tags `v7.5.1`,
  `simplebroker_pg/v3.10.0`, and `simplebroker_redis/v3.9.3` were created at
  the exact batch SHA. Release-gate runs `33000123765`, `33000111475`, and
  `33000117982` passed through Trusted Publishing and immutable GitHub Release
  publication. PyPI and GitHub each expose the expected wheel and sdist; the
  GitHub Releases also expose Sigstore bundles.
- 2026-08-26: an isolated Python 3.11 install from PyPI verified exact versions
  `simplebroker==7.5.1`, `simplebroker-pg==3.10.0`, and
  `simplebroker-redis==3.9.3`, imported the public helper, resolved PostgreSQL
  and Redis plugins, and exercised the published `Queue.backend_name` with a
  SQLite write/read. The earlier real PG15/18 ordinary-role, cross-role,
  autovacuum-overcount, and connection-reuse proofs remain the backend
  acceptance receipts. Weft compatibility passed and its revised admission
  plan received fresh independent approval; downstream enablement remains
  owned by Weft and is not a SimpleBroker release blocker.

## Out of Scope

- portable capacity/diagnostics API, generic broker method, closed enum, or
  backend API v8;
- Redis/SQLite connection diagnostics or general raw SQL;
- widened sidecar semantics;
- exact PostgreSQL process classification;
- monitoring roles, `SECURITY DEFINER`, runtime escalation, or installation;
- hard distributed permits or no-overshoot guarantee;
- Weft admission arithmetic/timers;
- pool, watcher, listener, schema, or transaction refactors;
- a new PostgreSQL server-version promise.

## Fresh-Eyes Review Checklist

- [x] Backend identity is accurate, open-ended, and no-I/O.
- [x] Capability exists only at the named `simplebroker_pg` surface.
- [x] Non-PG rejects before acquiring a connection.
- [x] Probe preserves fork, poison, batch, lock, retry, and materialization.
- [x] Sidecar is untouched.
- [x] Ordinary unprivileged role counts unrelated roles/databases.
- [x] `numbackends` is unfiltered and never described as exact or hard-bound.
- [x] Autovacuum fires the documented overcount caveat.
- [x] `numbackends > max_connections` is accepted.
- [x] PG15/18 behavior is proven without an untested floor claim.
- [x] Persistent reuse proves no extra checkout.
- [x] Payload and failure types are exhaustive and distinct.
- [x] Backend API v7 and release floors match code.
- [x] Local PG stays at the published baseline until the core minor exists.
- [x] Every retained artifact has a dry-run and exact-SHA evidence.
- [x] Weft receives sidecar, SQL, field, precision, and rollout corrections.
- [x] The stale reviewed Weft plan is reopened/superseded/patched and reviewed
      again before it can authorize implementation.
- [x] Specs, docs, changelog, locks, and plan/index states align.
