# PostgreSQL Connection Attribution and Usage Snapshot

Class: 5. Adds public Python API contracts and revises the winning product spec; hardening applies because process state, background pool creation, listeners, and fork recovery cross execution boundaries. No process-policy change and no material program-theory revision.
Plan type: Implementation with spec revision. Implementation authorized 2026-10-13 after the S1 review; publication remains a separate unit.
Owner: SimpleBroker product owner; current implementation follows the approved S1 plan.
Boundary: First-party PostgreSQL connection attribution and observation. Weft retains budgets, task identity, admission, reservations, and liveness.
Verification: Document gates for authoring; public-boundary, real PostgreSQL, transition-contract, compatibility, and downstream gates for implementation.
Required action: Review the exact delta, promote it using strategy A, record its promotion baseline, then implement the ordered slices.

## Goal

Add `simplebroker_pg.bind_connection_owner(label)` and `simplebroker_pg.get_connection_usage(queue)`. A caller binds an opaque accounting label before a dedicated process creates PostgreSQL resources. Several processes may use the same label. A reader obtains total conservative connection pressure and attributable client counts from one observed server population, through existing Queue resources. SimpleBroker interprets no task IDs and grants no permits.

## Outcome checklist

- [x] Public exports and typed result are defined without changing the core backend protocol or existing `get_connection_stats()`.
- [x] Bound labels cover pooled, listener, and direct first-party PostgreSQL connections, including later pool growth and listener creation.
- [x] Labels leave resolved targets, config, process-session keys, and listener registry keys unchanged.
- [x] One materialized activity population supplies both total and owner counts.
- [x] Real A/B/A processes prove aggregation and fresh observations after graceful closure; real growth and listener tests cover later connection creation and reuse.
- [x] Specs, implementation rationale, state-machine inventory, guides, compatibility checks, and changelog agree before completion.

## Source Documents

Consulted startup surfaces: `docs/program-theory.md`; `docs/agent-context/README.md`; `docs/agent-context/decision-hierarchy.md`; `docs/agent-context/principles.md`; `docs/agent-context/engineering-principles.md`; relevant runbooks below; `docs/agent-context/lessons.md`; Golden Rules and post-watermark entries in `docs/lessons.md`.

- Theory: `docs/program-theory.md` [THEORY-2], [THEORY-3], [THEORY-4], [THEORY-5], [REV-THEORY-004], [REV-THEORY-006]. Resource attribution refines existing ownership; application policy stays downstream.
- Source specs: `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-3], [SB-API-9], [SB-API-11], [SB-API-13]; `docs/specs/product-section-registry.md`, Python library / embedding API surfaces row.
- Process: `docs/specs/01-development-documentation-operating-model.md` [DOM-5], [DOM-6], [DOM-10.2], [DOM-11], [DOM-15], [DOM-16].
- Rationale: `docs/implementation/06-process-session-core-ownership.md`, especially the private SQL probe, PostgreSQL operation checkout lifecycle, listener registry, and fork sections; `docs/implementation/07-complexity-and-state-machine-map.md`.
- Runbooks: `docs/agent-context/runbooks/writing-plans.md`, `hardening-plans.md`, `writing-specs.md`, `testing-patterns.md`, `adversarial-acceptance-probes.md`, `maintaining-traceability.md`, `review-loops-and-agent-bootstrap.md`, `designing-agent-facing-interfaces.md`, and `external-skill-suites.md` (all in that runbooks directory).
- Skills consulted: `skills/interface-review/SKILL.md`; `skills/call-agent/SKILL.md`; external test-audit authoring guidance at `/Users/van/.agents/skills/test-audit/SKILL.md`. Apply its observable-contract, distinct-regression, ownership/cleanup, and sensitivity checks; repository harness rules win.
- Downstream inspected read-only: Weft `weft/core/manager.py::_read_postgres_connection_stats`, `weft/core/admission.py`, and its draft `2026-10-13-connection-aware-child-admission-plan.md`. The draft supplies demand evidence, not upstream product authority. Weft must pin the eventual released API before consuming it.
- PostgreSQL primary references: [activity population, permissions, and transaction caching](https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-STATS-VIEWS); [application-name storage limits](https://www.postgresql.org/docs/18/runtime-config-logging.html#GUC-APPLICATION-NAME); activity-column permission placement in [PostgreSQL 15](https://github.com/postgres/postgres/blob/REL_15_STABLE/src/backend/utils/adt/pgstatfuncs.c) and [PostgreSQL 18](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/pgstatfuncs.c), function `pg_stat_get_activity`.

## Spec Baseline

- `e8573fbb0dfa1126fef11a794b1214505693fb42`: governing spec, registry, implementation, code, and tests at authoring.
- This plan revises the spec itself. The two new APIs do not exist at this baseline.
- Promotion strategy: A, in-file text before implementation-link claims. Add new [SB-API-14] and [SB-API-15] in the existing active Python API spec. Do not reclassify that file or revise [SB-API-13]'s four-field contract.
- Promotion baseline identifier: implementer records the spec-promotion commit SHA, or the diff base plus exact promoted spec diff, in the execution log before code cites the new sections. This placeholder is not a compliance baseline.

## Context and Key Files

Existing files and current ownership:

| File | Current role and required action |
| --- | --- |
| `extensions/simplebroker_pg/simplebroker_pg/__init__.py` | Exports the runner, plugin factory, and old inspection helper. Export both new functions and the `ConnectionUsage` TypedDict here. |
| `extensions/simplebroker_pg/simplebroker_pg/connections.py` | Old helper checks backend before I/O, uses the first-party probe, and validates its exact four-integer payload. Add the new usage helper here; preserve the old parser and SQL. |
| `extensions/simplebroker_pg/simplebroker_pg/_sql.py` | Owns PostgreSQL statements, including the existing `POSTGRES_CONNECTION_STATS_SQL`. Add one activity-row query; do not replace the old statement. |
| `extensions/simplebroker_pg/simplebroker_pg/runner.py` | `PostgresRunner._create_pool()` configures each physical connection; `_check_fork()` rebuilds the pool. `_SharedActivityListener._run()` opens a separate connection. `_SharedActivityRegistry` shares listeners by PID, original DSN, and schema. Apply creation-time options without modifying those identities. |
| `extensions/simplebroker_pg/simplebroker_pg/validation.py` | `connect()` opens direct inspection/initialization connections and preserves capacity-refusal classification. Add binding options at this connection boundary without changing exception translation. |
| `extensions/simplebroker_pg/simplebroker_pg/plugin.py` | Creates runners and activity waiters, and cleanup also calls the direct connector. Audit all physical creation paths and retain existing lifecycle ownership. |
| `simplebroker/_broker_session.py` | Session identity includes target, backend options, and config. Read-only invariant owner; no new owner dimension. |
| `simplebroker/db.py` | `_run_backend_probe()` already materializes runner rows under the normal core lock/retry/fork/batch guards. Reuse it without adding a generic monitoring operation. |
| `extensions/simplebroker_pg/tests/test_connection_stats.py` | Existing ordinary-role, cross-database, lifecycle, permissions, and autovacuum proof. Retain the old API tests; extend the existing autovacuum scenario for new total semantics rather than clone it. |
| `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`, `test_pg_notify.py`, `test_pg_integration.py`, `test_pg_ownership.py` | Existing connection, waiter, shared-session, and backend ownership neighbors. Extend existing scenarios only where that proves a distinct wiring obligation. |
| `tests/test_backend_probe.py`, `tests/test_python_library_api_contract_sb_api.py` | Core probe guarantees and the product-contract inventory. Reuse existing coverage; update public surface/verification bindings rather than add prose-fragment assertions. |
| `tests/state_machine_manifest.py`, `tests/test_state_machine_policy.py`, `tests/helpers/state_machine_contracts.py` | Existing executable transition-table infrastructure. Register the new process binding machine without a new state-machine framework. |
| `.github/workflows/test-pg-extension.yml` | Default concurrent PostgreSQL suite plus existing serial PG18 statistics/autovacuum and PG15 compatibility steps. Include the new usage tests in those serial steps; retain default concurrent execution for other new proofs. |
| `extensions/simplebroker_pg/README.md`, `docs/guides/python.md`, `README.md`, `CHANGELOG.md` | Document both functions, limits, example, and first-party-only placement. Root README needs only the existing PostgreSQL inspection catalog/link aligned. |
| `docs/implementation/02-repository-map.md`, `docs/implementation/06-process-session-core-ownership.md`, `docs/implementation/07-complexity-and-state-machine-map.md` | Add the new module's ownership and explain label lifetime, creation-time capture, population/credit split, visibility, and no key mutation. |

New files, explicitly proposed and not existing baseline paths:

- `extensions/simplebroker_pg/simplebroker_pg/_connection_owner.py`: private process-wide binding owner and shared creation-option capture. One real state owner, not separate pool/listener registries.
- `extensions/simplebroker_pg/tests/test_connection_owner.py`: binding transition table, public input errors, threading, and fork proof.
- `extensions/simplebroker_pg/tests/test_connection_usage.py`: snapshot contracts and real multiprocess/pool/listener scenarios. Keep spawn worker functions module-level in this test module unless reuse actually warrants another helper.

Comprehension gate, before implementation edits: record answers in the execution log. Incorrect answers block code work until the cited owners are reread.

1. Why must owner labels stay outside DSN, config, and backend options? Expected answer: all can participate in session identity, and DSN also participates in the independent listener key; label-specific targets could create extra pools/listeners. Backend connection kwargs can carry metadata without changing those keys.
2. What can an ordinary observer count across another role? Expected answer: database-attached rows remain in total and application names can be visible, but `backend_type` may be NULL. Without visible client classification, that row receives no owner credit. Complete cross-role credit requires existing PostgreSQL visibility privileges; this API grants none.

## Invariants and Constraints

- One existing queue/session/pool model. No new target, config key, environment variable, backend protocol method, connection table, daemon, sampling cache, or dependency.
- `get_connection_stats()` keeps its exact signature, keys, SQL, conservative interpretation, permission behavior, and exception behavior.
- The process label is an accounting grouping value. It need not be unique per PID; wrapper and worker may share it. A process cannot rebind its live shared resources to another owner.
- Binding and first resource creation have one synchronization boundary. A concurrent bind either wins before creation options are captured or fails; it never produces a partially attributed pool/listener population.
- Binding applies to connections the first-party extension creates, including a caller-constructed `PostgresRunner`. It does not relabel pre-existing caller-created driver connections or connections created by third-party SQLRunner implementations.
- Capture label kwargs before pool background work or listener-thread startup. Every later physical connection from that pool receives the captured label. Fork recovery captures the child's state anew.
- Do not hold the binding lock during driver I/O, pool startup waits, listener readiness waits, or registry cleanup. Check PID before acquiring an inherited lock, following existing fork patterns.
- Label application is part of connection creation, not best-effort telemetry. A bound connection must not be published without its label. Preserve existing driver causes and cleanup/retry semantics on failure.
- No per-checkout label mutation. Caller SQL that later changes `application_name` changes what observation sees; the extension does not police or repair arbitrary caller SQL.
- Total and credits derive from the same returned row list. Never read the old total helper separately and subtract counts from another observation.
- Missing visibility or unrecognized labels provide no credit. Database failures propagate; they never become a zero-pressure success.
- Normal resource closure and PostgreSQL session disappearance own cleanup. No durable registry or death-detection policy moves upstream.

## Proposed Spec Delta

| Spec file | Strategy | Sections touched |
| --- | --- | --- |
| `docs/specs/16-python-library-api.md` | A: promote exact text without implementation claims; add mappings and firing verification rows with implementation | Add [SB-API-14], [SB-API-15] before the implementation-summary section; add catalog rows under [SB-API-1] and matrix rows under [SB-API-12] |

Exact proposed new sections follow. They are review targets until promotion; the promoted spec becomes the sole implementation contract.

### PostgreSQL process connection attribution [SB-API-14]

`simplebroker_pg.bind_connection_owner(label: str) -> None` is a PostgreSQL-only, process-wide connection-attribution binding. It opens no connection. Several processes may bind the same label; the label identifies a caller-defined accounting group, not a PID, authenticated identity, task, or broker namespace. A binding applies to every first-party PostgreSQL target subsequently used in that process.

Labels must be strings matching `re.fullmatch(r"[!-~]{1,54}", label)`: 1 through 54 non-space printable ASCII characters. Labels are case-sensitive, are not trimmed or normalized, and must contain no secrets. Non-string input or a nonmatching value raises `ValueError` without changing binding state. PostgreSQL receives exactly `sb-owner:` followed by the label as `application_name`, at most 63 ASCII bytes. No truncation or hashing is permitted. Binding overrides a DSN-supplied or ambient application name for first-party-created connections; an unbound process retains its existing application-name behavior. A marked application name is cooperative attribution and may be copied by other clients; it confers no authority.

The first successful binding wins for the process lifetime. Repeating that exact binding returns `None`, including after resources start. Another valid label raises `RuntimeError` and preserves the first binding. If first-party PostgreSQL resource creation has already started while unbound, any first binding raises `RuntimeError`. Resource creation starts when constructing a command pool, starting a listener, or attempting a direct first-party connection. A failed creation attempt still seals this state. Closing resources does not unbind or reopen binding. Creating a lazy Queue alone does not seal the state unless it actually starts such resource creation. Errors explain that binding belongs before resource creation in a new dedicated process.

Binding and resource creation are synchronized so one process cannot publish connections from mixed binding decisions. All threads use the same state. Pool workers and listener threads receive captured creation-time options; the binding does not alter DSN, target, backend options, Config, process-session identity, listener keys, or connection limits. Attribution covers initial and all later physical connections from a pool, listeners and later listener creation, and direct inspection, initialization, and cleanup connections. First-party `PostgresRunner` instances are covered even when constructed by the caller; caller-created driver connections and third-party SQLRunner implementations are not modified.

An actual PID change resets the private binding and sealed state before any inherited binding lock is acquired. A forked child must bind explicitly before its own resource creation to receive an owner label; otherwise its new first-party connections follow ordinary unbound behavior. A spawned child likewise binds explicitly. Parent connections retain their parent label. Existing Queue/runner fork rules and inherited `BrokerSession` rejection remain unchanged. There is no public reset, unbind, or rebinding context manager.

### PostgreSQL coherent connection usage [SB-API-15]

`simplebroker_pg.get_connection_usage(queue: Queue) -> ConnectionUsage` is a PostgreSQL-only inspection helper. `ConnectionUsage` is an exported `TypedDict` with exactly `total: int` and `by_owner: dict[str, int]`. Returned values are ordinary fresh dictionaries, including a fresh nested dictionary. Counts have exact Python type `int`, not `bool`. `total` is nonnegative; owner counts are positive; owners with zero credited rows are absent; and `sum(by_owner.values()) <= total`. Dictionary iteration order is not a contract. The reader need not bind a label.

The helper executes one parameter-free activity statement per probe attempt through `queue.get_connection()` and the existing first-party probe. Existing retry policy may repeat a failed attempt; only the successful attempt's complete result supplies the observation. The statement returns `application_name` and `backend_type` for every row of `pg_catalog.pg_stat_activity` whose `datid IS NOT NULL`. The complete materialized result of that statement is the observation population. No second total query, per-owner query, pool-statistics query, fallback population, or combination of different attempts is permitted.

`total` is the population's row count. It includes all databases and roles on the connected PostgreSQL server, idle and active sessions, the observing connection, and database-attached workers such as autovacuum and parallel workers. It includes unlabeled and unrelated clients. It excludes processes whose activity row has NULL `datid` and connection attempts not yet represented by a database-attached activity row. It is conservative database-attached backend pressure, not an exact count of consumed client slots, and has no upper-bound validation against `max_connections`. It is not restricted to the Queue's schema, database, role, or process. Equality to a separately executed `get_connection_stats()` is not promised.

For `by_owner`, a row earns one credit only when `backend_type` is exactly `client backend` and its application name is exactly `sb-owner:` plus a label valid under [SB-API-14]. Decode the label losslessly and group credits across all matching processes. Empty, NULL, near-match, malformed, or unrelated application names earn no credit. Non-client workers earn no credit even if they carry a valid owner marker. A hidden/NULL backend type earns no credit even when the application name is visible. Every uncredited row remains included in `total`. Thus `total - sum(by_owner.values())` means uncredited pressure, not necessarily unlabeled client connections.

Under stock permissions an ordinary observer can credit its own role's client connections. PostgreSQL may hide the backend type for other roles. Such rows remain in total but do not earn credit; cross-role complete attribution requires the observer's existing role visibility or monitoring privileges. The helper neither grants privileges nor fails merely because attribution is incomplete. Consumers must treat an omitted owner as zero observed credit, not proof of zero connections or process death.

The helper returns one internally coherent sampled population. It does not promise a globally atomic server instant, exhaustive knowledge of connection attempts, a launch reservation, or prevention of capacity overshoot. Under normal autocommit use each call obtains PostgreSQL's observation for that statement. Inside an existing caller-owned transaction PostgreSQL's activity-cache semantics apply and the population may reflect the transaction's earlier activity snapshot. The helper does not commit, roll back, clear that snapshot, or create an independent connection to refresh it. Direct or session-pooling PostgreSQL connections are the attribution model; transaction/statement-multiplexing proxies cannot be assumed to preserve client-process connection accounting.

The helper rejects non-PostgreSQL Queues with `ValueError` before target I/O. SQL, permission, and connection failures propagate through the existing public `DatabaseError` hierarchy. Existing fork, active-generator-batch, locking, retry, and caller-owned-runner rules remain in effect. No database failure is translated to an empty successful snapshot.

A persistent target-resolved Queue reuses its existing process session and borrows a normal operation checkout. The helper creates no monitoring-specific connection or pool, but normal lazy resource creation and pool growth remain possible. An ephemeral Queue can create and release operation resources. It creates no installed database object or schema migration. The existing `get_connection_stats()` API and the generic core protocol remain unchanged.

### Exact catalog and cross-surface additions

Add these rows to [SB-API-1]'s public surface table at implementation linking time:

| Surface | Meaning |
| --- | --- |
| `simplebroker_pg.bind_connection_owner` | First-party PostgreSQL-only process attribution binding under [SB-API-14]; supplied by the separately installed `simplebroker-pg` distribution. |
| `simplebroker_pg.get_connection_usage`, `simplebroker_pg.ConnectionUsage` | First-party PostgreSQL-only coherent usage observation and typed dictionary under [SB-API-15]; not portable core operations. |

Add these rows to [SB-API-12]'s CLI/library matrix at the same time:

| Concern | CLI surface | Python owner |
| --- | --- | --- |
| PostgreSQL process attribution | No CLI binding command | [SB-API-14] |
| PostgreSQL total and owner usage | No CLI inspection command | [SB-API-15] |

Extend the registry's existing embedding/API concern range to [SB-API-15] and its Gate cell to include the new owner/usage contract modules when those modules exist. Add [SB-API-14] and [SB-API-15] verification rows with every clause bound to the tests below. Do not add implementation pointers or claim passing new tests in the text-only promotion slice.

## Implementation Design

Keep the process binding state in one small private owner shared by all three creation paths. A small class holding the PID, label, startup state, and lock is reasonable; its exact name and field representation are internal choices. Production uses one instance, and pure tests can construct independent instances of that same owner without resetting production globals. The binder and creation-option capture must use the same PID-check/reset routine before acquiring the binding lock, so capture without a preceding bind also clears inherited attribution. Creation capture then seals binding and returns an ordinary private dictionary of connection kwargs for this creation. The public binder validates with the regex before mutation. No frozen options wrapper or client-PID registry is needed.

Name the binding lifecycle `SM-PG-CONNECTION-OWNER` and register its actual owner and firing table in the existing manifest and implementation map, as [DOM-10.2] requires. Cover the externally meaningful transitions: first binding, same-label idempotence, conflicting binding, first resource creation preventing late binding, failed creation remaining sealed, closure not reopening binding, child PID reset, and the two possible orders of concurrent bind/creation. Test invalid input once at the validation boundary; do not multiply it across internal field combinations. No four-state representation, production enum, or generic framework is required. The table's PID-reset row invokes the one spawn-isolated real fork scenario below; skip only that row where fork is unavailable, and do not duplicate it as a standalone test.

Pass captured `application_name` as driver connection kwargs rather than rewriting `_dsn`. The pool receives these kwargs when it is constructed, so psycopg's later worker-created connections get the same metadata before publication. The listener captures options before thread creation. Direct `validation.connect()` captures before connecting. Audit all `psycopg.connect` and `ConnectionPool` creation sites, including cleanup and pool fork recovery; use one option-building path. Preserve error translation, search-path configuration, capacity retry classification, and borrowed-resource teardown. Do not perform a post-connect `SET application_name` as the main mechanism.

The new query in `_sql.py` is exactly:

```sql
SELECT application_name, backend_type
FROM pg_catalog.pg_stat_activity
WHERE datid IS NOT NULL
```

Fetch once through the existing probe. Use ordinary tuple unpacking, compute total as `len(rows)`, and increment a Python dictionary only for eligible client markers, with normal handling for NULL and unknown activity values. Reuse the label regex for marker suffixes. A single materialized input makes consistency structural; no defensive row parser, second query, SQL JSON envelope, or sample cache is needed.

## Rollout, Rollback, and Success Signals

This is additive extension behavior with no installed object, storage change, or backend handshake bump. Ship the extension before downstream code imports it. This plan authorizes no release, publish, deployment, or Weft edit. Select versions and update package metadata/lockfiles only in the normal separately authorized release unit; no dependency is added here.

Weft rollout must install bindings before dedicated wrapper/worker broker startup and preserve the chosen label in its existing task evidence. Old unbound processes remain uncredited; they must not be assumed to have zero actual usage or zero future obligations. Weft owns its coordinated drain/upgrade and budget reconstruction. To roll back downstream use, stop importing/calling the new APIs and restart bound processes as necessary; live connection labels last until connection close. No database rollback is needed. Restarting is required to choose a different binding; do not add an unbind escape hatch.

After integration, inspect a real wrapper/worker pair: their pooled and listener PIDs carry the expected marker, their group count moves with physical connections, and shared Queue/session use still consumes the expected existing pool/listener capacity. Watch for owner under-credit from role visibility, unexpected uncredited pressure, duplicate pools, startup refusals, and lost notifications. These are observation signals, not a new metrics service. Admission success and task completion remain Weft's separate acceptance criteria.

## Ordered Tasks

1. [x] **Plan review and spec-promotion slice.** Independently review the current plan and exact delta against the recorded baseline and Revision Log. Apply [SB-API-14/15] text using strategy A, update the existing spec backlink, and record the promotion identifier. Preserve [SB-API-13]. Rerun the doc gates. Stop if review requires per-task sharing within one process, new persistence, a permission grant, or changed admission policy. Done signal: exact contract is promoted and independently reviewed, with no unsupported implementation claims.
2. [x] **Binding and connection-creation slice.** Add the private owner and public binder, creation kwargs in runner/listener/direct paths, state transition contract, and minimal package exports. Add binding/growth/listener tests first; missing APIs establish the initial red, then demonstrate behavioral sensitivity by withholding attribution on a real creation path. Before the usage helper exists, use the independent raw server observer for counts and markers in these tests. Add [SB-API-14] implementation/verification links, the binder catalog/matrix row, and reciprocal code citations with this code, not in a later slice. Update implementation docs 06/07 and manifest together. Run owner tests and runner/notify/integration neighbors at standard concurrency. Stop if owner changes enter identity material or a listener/pool is duplicated. Independently review this coherent slice before proceeding.
3. [x] **Snapshot slice.** Add `ConnectionUsage`, the query, and public helper in the existing inspection module. Write population and shared-label/freshness proofs first. Use the real probe path; read the old helper tests before editing. Extend slice 2's real growth/listener proof with public usage assertions without cloning setup or removing the independent PID/marker evidence. Add [SB-API-15] verification/mapping links, reciprocal citations, and registry/catalog updates with code. Run targeted usage/old-stat tests and the real subprocess scenarios; review the completed slice independently. Stop if the implementation needs a defensive driver-result parser, second query/population, monitoring-only connection, or stronger consistency than PostgreSQL supplies.
4. [x] **Compatibility and documentation slice.** Update the named guides/README catalog/changelog/repository map; wire the two new modules into existing PG18 serial and PG15 steps. Recheck Weft's import boundary and pinning prerequisite read-only. Exercise role visibility and malformed-marker cases against both server versions. Run interface review against the promoted public surface, then the final gates below. No launch-policy implementation is included.
5. [x] **Traceability reconciliation and closeout.** Answer all review findings and deviation rows; ensure promoted spec, implementation rationale, code, transition inventory, tests, and registry agree. Record current-state gates and residual limits. Evaluate heavily used skills/runbooks for useful corrections; record a lesson only if a reusable correction was actually discovered. Close the Status Index row only when implementation is complete. Follow the repository's explicit commit authorization rule before claiming a finished implementation slice or ready-to-land state.

## Testing Plan

Apply the test-audit authoring gate: every test names its observable contract, credible regression, gap in existing proof, and any dependency seam. Use pure tests for label grammar, binding decisions, and grouping. Use real PostgreSQL only for the connection-creation, visibility, transaction, and population boundaries that require it. The existing probe tests already own materialization, retry, locking, batch refusal, and terminal error propagation; do not recreate them in the new helper suite.

Public binding integration scenarios run in fresh spawned processes so a sticky process binding cannot contaminate a pytest worker. Pure binding cases use independent instances of the real private state owner, not a public reset API or a mock owner. Keep child functions module-level in the proposed test modules. Use owned Pipes/Events and positive readiness after actual resources exist, scaled aggregate deadlines, and failure diagnostics with phase/PID/exit/liveness. Always release, join, and reap owned threads/processes and close resources, including on failure. Do not use sleeps or negative timing windows as correctness evidence.

Keep PostgreSQL, pool growth, session reuse, listener creation, and LISTEN/NOTIFY real in their integration proof. For pure grouping tests, a scripted probe may supply legitimate catalog rows: production code computes the result. This exercises classification rather than inventing malformed driver formats. Reuse existing fixtures and state-table infrastructure; add no new test harness or test-only production hooks.

| Test home and proposed scenario | Observable proof and distinct regression |
| --- | --- |
| Owner module: `test_connection_owner_transition_table` | Fire the binding transitions named above against the actual owner: first/same/conflicting binding, creation and failed-start sealing, closure, PID reset, and concurrent bind/start ordering. Cover the same-label rule before/after creation within one coherent case, not separate internal-state inventories. The PID-reset row invokes the single real fork scenario below. |
| Owner module: pure label-validation cases | Through the production binding owner, test accepted minimum/maximum length, case and punctuation, plus non-string, empty, oversized, space/control/NUL/non-ASCII rejection. After rejection, a valid bind must still succeed. No PostgreSQL actor per grammar case and no special string-subclass promise or test. |
| Owner module: `test_owner_label_round_trips_and_overrides_application_name` | In one bound spawned child, use a maximum-length valid label containing punctuation and independently inspect the real connection marker. Verify lossless storage and binding precedence over an existing application name. A separate unbound control retains the configured application name. This proves real driver wiring; the pure cases cover grammar. |
| Usage module: `test_shared_owner_labels_across_processes` | Three real processes carry test-unique A/B/A labels, each holding two distinct physical checkouts. After acknowledged readiness, the same pre-opened parent observer Queue returns A=4/B=2. After gracefully closing one A process, bounded-poll the public helper on that same observer Queue until A=2/B=2. After closing the other A, bounded-poll until A is absent and B remains 2. Use a scaled aggregate deadline and report owned process/PID state on failure; a close acknowledgment alone does not prove PostgreSQL has removed its activity row. This one scenario proves aggregation, fresh calls, decreasing use, and absence of stale sample caching. No separate freshness actor or forced-death phase. Total includes the observer and other service/test connections, so do not assert global total=6. |
| Usage module: `test_pool_growth_keeps_owner` | One bound child grows one shared BrokerSession pool through one, two, and three concurrent held checkouts using threads and `sidecar(transaction=True)`. Observe distinct PIDs, their exact markers, and owner counts 1/2/3. Release and reuse PIDs; Queue/BrokerSession handles must still share the same runner/pool. Observe the fully held phase from the parent, since a fourth child checkout would exhaust the default pool. This detects missing kwargs on later pool connections without a backend-kill/retry scenario. |
| Usage module: `test_shared_listener_is_labeled_and_recreated` | One single-queue waiter and one fan-in waiter on the same target/schema share one labeled listener PID beyond the command pool. Confirm its PID/marker from an independent observer and prove actual notification delivery through a public waiter. Close both waiters, observe that listener PID disappear, then create a later waiter and prove its new labeled connection works. Existing notify/lifecycle tests own the larger unregister/close matrix; do not clone it. Never execute SQL concurrently on the listener connection. |
| Owner module: `test_direct_connector_keeps_owner` | A bound spawned child calls real `validation.connect()`, reads `SHOW application_name`, and closes it. Audit inspection/initialization/cleanup callers of that connector and run their existing ownership/error tests. No pass-through production wrapper or repeated plugin setup matrix. |
| Owner module: one real fork scenario, fired by the PID-reset table row | A spawned parent binds A, constructs/uses a first-party `PostgresRunner`, and forks while another parent helper thread holds the binding lock with acknowledged acquisition. The child first calls `bind_connection_owner(B)` without a preparatory private reset or creation-option capture, then uses that same inherited runner, exercising actual `_check_fork()` pool rebuilding. Successful binding under the inherited foreign-held lock, B-labeled rebuilt connections, and unchanged parent A prove recovery through the real path; no second unbound database child is needed. Release/join the lock holder and reap fork children even on failure. Skip this row only where fork is unavailable. This protects new inherited-state/lock hazards without duplicating existing BrokerSession fork rejection. |
| Usage module: `test_usage_population_and_marker_grammar` | Supply legitimate raw rows: shared labeled clients, NULL/empty/near-match/bad-suffix names, same-marker workers, hidden types, and future unknown backend types. Total includes every row; only recognized client rows earn credit. Check exact output keys/native counts and fresh dictionaries. Do not add a malformed outer-list/tuple/type matrix for the trusted fixed query. |
| Usage module: wrong-backend and failure forwarding | A non-PG Queue raises `ValueError` before creating a target. A scripted probe raises a public database error; the helper propagates it rather than returning zero. Existing core probe tests retain responsibility for cause, retry, and materialization behavior. No PUBLIC catalog grant is revoked. |
| Usage module: `test_usage_preserves_caller_transaction` | Warm a Queue with a caller-constructed `PostgresRunner`; enter `runner.begin_immediate()` and insert into a test-owned table through `runner.run()`. After the public usage read the pending row remains visible on that runner and invisible to another connection. Caller rollback removes it. This compact test detects unwanted commit/rollback; it needs no changing child population or assertion about permitted PostgreSQL activity caching. |
| Usage module: `test_usage_cross_role_visibility` | On real PG15/PG18, ordinary observer credits same-role clients but not a different role's marked client with hidden type. Grant only this test's unique observer role visibility and prove the other client's credit appears. Use unique roles/labels and restore grants/objects on every exit. No global permission mutation or exact-total delta in this concurrent test. |
| Extend existing cross-database statistics scenario | Add public usage assertions to `test_connection_stats_ordinary_role_counts_other_roles_and_databases`, reusing its controlled clients and database setup. In its existing serial-only lane, assert the new total's exact added-client delta, including unmarked/other-role/other-database use, and verify NULL-database auxiliary processes are excluded using an independent server query. Do not copy the old helper's global revoke into the new helper proof. |
| Extend existing autovacuum statistics scenario | During `test_connection_stats_includes_autovacuum_worker_overcount`, add assertions that the new total includes the established database-attached worker and gives it no credit. Reuse the opt-in isolated PG18 setup. Pure grouping rows separately cover a worker carrying a valid owner marker. |
| Existing public/API/type and state-policy tests | Bind new exports/annotations and consumer TypedDict use to the public contract; preserve the old helper's four fields and absence of new generic Queue/protocol operations. Register the actual binding owner/table in the existing manifest. Avoid prose-fragment or source-layout assertions. |

Counts concern physical established connections, including idle pool connections. Returning a checkout need not lower usage; physical close does. Acknowledged PID evidence proves pool growth and listener creation. Only the existing exact-total/autovacuum scenarios need the pre-existing isolated serial lanes: other xdist workers change the shared server population. Keep process aggregation, growth, listener, and unique-role visibility tests at standard concurrency.

Start with failing public behavior, not just successful imports. For sensitivity evidence, deliberately omit attribution on the listener and on later pool-created connections, observe the intended failing integration cases, restore exact code/Git state, and verify passing results. Use an isolated checkout or a narrowly reversible edit; never commit controls. Additional mutations are required only when inspection leaves a test's sensitivity uncertain. Do not impose the previous fixed five-control campaign or multiply obvious grouping cases merely to satisfy a checklist.

Adversarial floors apply at the real boundaries: invalid labels, marker mimicry, wrong backend, missing role visibility, database failure propagation, and concurrent binding/creation. No CLI surface is added, so new exit-code, filesystem-output, and batch-scanner probes are N/A. There is no new untrusted serialization parser to armor.

## Verification and Gates

Authoring gates (run now and after review edits):

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Implementation uses a single combined dependency environment. Sync once before concurrent lanes; nested backend wrappers resync dev+pg, so run wrapper invocations sequentially and do not race them with another uv sync:

```bash
uv sync --locked --extra dev --extra pg --extra redis
uv run --no-sync ./bin/pytest-pg --fast extensions/simplebroker_pg/tests/test_connection_owner.py extensions/simplebroker_pg/tests/test_connection_usage.py extensions/simplebroker_pg/tests/test_connection_stats.py
uv run --no-sync ./bin/pytest-pg --fast extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py extensions/simplebroker_pg/tests/test_pg_notify.py extensions/simplebroker_pg/tests/test_pg_integration.py extensions/simplebroker_pg/tests/test_pg_ownership.py
uv run --no-sync pytest tests/test_backend_probe.py tests/test_python_library_api_contract_sb_api.py tests/test_state_machine_policy.py tests/test_broker_session.py
```

The new paths in these implementation commands are proposed files, not runnable authoring checks. Default wrappers use automatic xdist concurrency and loadgroup. Do not cap workers to make failures disappear. For the existing exact-total/autovacuum proof and PG15/PG18 compatibility, additionally run the existing isolated statistics lane with both modules:

```bash
SIMPLEBROKER_RUN_AUTOVACUUM_PROBE=1 PYTEST_ADDOPTS= uv run --no-sync ./bin/pytest-pg --fast -n0 extensions/simplebroker_pg/tests/test_connection_stats.py extensions/simplebroker_pg/tests/test_connection_usage.py
SIMPLEBROKER_PG_TEST_IMAGE=postgres:15 PYTEST_ADDOPTS= uv run --no-sync ./bin/pytest-pg --fast -n0 extensions/simplebroker_pg/tests/test_connection_stats.py extensions/simplebroker_pg/tests/test_connection_usage.py
```

Final implementation gates, from current candidate state:

```bash
uv run --no-sync pytest
uv run --no-sync ./bin/pytest-pg
uv run --no-sync ruff check .
uv run --no-sync ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --no-sync mypy --config-file pyproject.toml simplebroker extensions/simplebroker_pg/simplebroker_pg
uv run --no-sync python bin/ruff_suppression_index.py --check
SIMPLEBROKER_REQUIRE_FULL_MANIFEST=1 uv run --no-sync pytest -n0 tests/test_state_machine_policy.py
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Also type-check the concrete PG test file list using the existing CI enumerator in `.github/workflows/test-pg-extension.yml`; put `--config-file pyproject.toml` before scope overrides. The full-manifest command requires both optional extension imports in the synced environment. Run documented examples through the installed candidate extension, confirm `simplebroker_pg.__file__` points at that candidate, and use the existing release driver's artifact checks when release is separately authorized. Record skipped server/version/permission probes as blockers to their corresponding acceptance claims, not as passing proof. Core pytest does not replace the PostgreSQL wrapper.

## Independent Review Loop

Prefer a bounded different-family review using `skills/call-agent/SKILL.md`; attempt current availability rather than trusting the old inventory. If unavailable, record the invocation/time bound/outcome and use a distinct same-family reviewer. No reviewer edits files or delegates further.

Review brief: review only this plan/delta at baseline `e8573fbb0dfa1126fef11a794b1214505693fb42` and named implementation boundaries. Existence-check paths/seams/commands first. Read program theory, the winning API spec, implementation doc 06, current extension creation/probe paths and tests, and the exact proposed sections. Accepted limitations are observation rather than permits, conservative worker overcount, explicit no-credit for invisible types, transaction-cache behavior, cooperative labels, and no multiplexing-proxy guarantee. Existing unrelated defects are observations unless this change worsens them. Prefer removing unnecessary machinery. Return PASS or BLOCKED against implementability and robustness, a P1/P2/P3 findings table with suggested dispositions, and separate non-actionable scope observations. Author dispositions decide action; every finding is accepted/fixed, rejected with rationale, or explicitly severed from scope. Re-review substantive accepted changes and each coherent implementation slice; final review receives current evidence, not earlier green claims.

## Revision Log

### S1: Owner-directed simplicity revision, 2026-10-13

Revision baseline: the preceding reviewed draft has SHA-256 `4ed94435f4f1dd97548b810e1931b78a91ea18f3d862a86306d5fd8b0caf3398`; the governing repository/spec baseline remains `e8573fbb0dfa1126fef11a794b1214505693fb42`. The exact before/after delta is supplied to the revision reviewers. This is still Class 5 with hardening; no new implementation or process-policy scope is added.

Reason: the owner authorized the simplicity-review cuts while retaining necessary safeguards and real integration tests. Previous reviews proved implementability, not minimality. The new proposed contract drops promises that would require code or tests without serving the consumer.

| Finding/cut | Current disposition and retained proof |
| --- | --- |
| Special string-subclass semantics | Removed from operative spec/design/tests. One ordinary regex/type check defines accepted labels; pure grammar cases and one maximum-length real roundtrip remain. |
| Defensive parsing of trusted driver rows | Removed the new list/tuple/type validation contract and malformed-result test matrix. Fixed SQL plus the existing materializing probe feed ordinary unpacking/grouping. NULL/unknown types, markers, and database-error forwarding remain covered. |
| Repeated freshness and process-death scenarios | A/B/A plus graceful closes through the same observer now proves aggregation and freshness. No forced-death phase or separate freshness actor. Growth, shared listener recreation, and the compact caller-transaction settlement check remain real. |
| Global catalog revoke | Removed for the new helper. Existing probe error tests plus one forwarding case cover failure propagation. Real ordinary-role visibility, other-database totals, and worker population evidence remain; existing statistics fixtures are extended rather than copied. |
| Second unbound fork database child | Removed the second database child, keeping one real inherited-runner/foreign-lock scenario. Binder and capture share the same PID-check/reset routine before locking; review must verify both entry points use it. The binding table retains unbound capture behavior without another database actor. |
| Fixed five-mutation campaign | Replaced by targeted listener/growth sensitivity controls and additional controls only where confidence is unclear, consistent with the test-audit guidance. Red-first behavior, exact restoration, and no committed controls remain required. |
| Prescribed internal class name/four-state taxonomy | Removed. One small private owner, synchronization, late-binding rejection, failed-start sealing, PID-before-lock reset, and the repo-required named firing transition contract remain. No new enum/framework/reset API is introduced. |

Review scope: only S1's delta and the risks it introduces; preserve precise total/credit semantics, old helper compatibility, unchanged resource identity, and all real creation seams. Check that removed tests have named remaining proof owners and that no historical row is mistaken for current instructions. No request to implement or promote spec text accompanies this revision.

Independent same-family S1 review identified S1-F1 (P2): a pre-binding child-state assertion could trigger unbound sealing or hide a missing public reset. Accepted: the first child entry is now public `bind_connection_owner(B)`, with no preparatory private reset/capture. B-labeled inherited-runner rebuilding and unchanged parent A provide the evidence. Scoped same-family verification returned PASS for S1-F1 with no new defect.

Claude's bounded S1 review exited zero in 117.9 seconds. It reviewed the earlier S1 snapshot and returned BLOCKED on the same pre-binding assertion (F1), which the scoped verification above has since confirmed fixed. It otherwise found the revision implementable and supplied two P3 clarifications:

| ID | Reviewer finding | Disposition |
| --- | --- | --- |
| F1 / S1-F1 | Pre-binding child-state inspection can show stale state, seal the unbound child, or deadlock. | Accepted and fixed: first child entry is public B binding, followed by real inherited-runner rebuilding. Same-family scoped verification passed. |
| F2 | The removed unbound fork database child was not fully recorded, and creation capture still needs its own PID reset. | Accepted. Added its explicit Revision Log row and historical supersession note. Design now requires both entry points to use the same PID-check/reset routine before locking; no extra database child or inspection hook is added. |
| F3 | The A/B/A close phases could invite an immediate assertion before PostgreSQL removes the backend row. | Accepted. Explicit bounded polling through the same public observer now owns eventual disappearance, with a scaled deadline and diagnostics. |

Final scoped same-family verification returned PASS for F1/F2/F3 with no new defects: public B binding is the first child entry, both creation/binding paths share PID reset before locking, the removed database child is recorded, and close observations use bounded polling. Current-state author verification passed the four document gates and the existing/proposed path check. Plan status remains `draft` because implementation is future work.

## Fresh-Eyes and Review Record

Historical record: the reviews through R3 below concern the original draft. S1 in the Revision Log supersedes their subclass, defensive-parser, test-duplication, new catalog-revoke, second unbound fork database child, exact-state-representation, and fixed-mutation instructions. Keep those findings as history; the revised Proposed Spec Delta, Implementation Design, and Testing Plan govern the next implementation. Earlier PASS results do not approve the S1 delta.

Author checks before independent review: new paths are explicitly labeled proposed; existing keys and creation paths were inspected; the old strict helper is preserved; raw activity rows remove needless JSON/CTE aggregation; lower-bound owner credit is documented rather than confused with hidden client count; pool release is distinguished from physical disconnect; full binding tests are spawn-isolated so global state does not contaminate pytest workers.

Authoring baseline gates: `python3 bin/check-dom15-fixtures`, `bin/check-plan-context` (0 in-flight plans), and `bin/check-doc-paths` passed before plan edits. Current-state author gates also passed after the draft/index/backlink existed, including `git diff --check` and one in-flight source declaration. A read-only existence check found 36 existing path claims, the three explicitly proposed new paths, and zero unexpected missing paths. Installed signatures confirm `ConnectionPool(..., kwargs=...)`, `BrokerCore.sidecar(transaction=...)`, and `BrokerSession.connect(...)`. Implementation/runtime gates have not run in this authoring unit and are not claimed.

Independent same-family review returned PASS with no actionable findings after checking the current slice-2/3 correction, existing paths/seams/packaging, and all four document gates. Independent Claude review used `claude -p <full-plan-brief> --permission-mode plan --allowedTools Read,Grep,Glob`, stdin closed, 540-second bound; it exited zero in 261.6 seconds. It returned PASS conditional on fixing its two P2 test gaps and supplied seven P3 simplifications/clarifications. No repository file was changed by the reviewer. A separate 120-second-bounded liveness probe exited zero with `PROBE-OK`. Review outputs are retained in the invocation transcript; the findings below quote each finding's opening sentence verbatim.

| ID | Severity | Reviewer finding (verbatim) | Author disposition |
| --- | --- | --- | --- |
| C1 | P2 | The fork test never exercises the runner's own fork rebuild. | Accepted after inspecting `_check_fork()` and the session factory. The single real fork scenario now reuses the inherited caller-constructed `PostgresRunner`; it checks child B and unbound cases under a held parent binding lock. Sensitivity control (e) reuses parent options to prove the failure. Removed incorrect fork sensitivity from the spawned growth case. S1 later removes the second database child and mandatory control (e), while retaining the real inherited-runner/foreign-lock proof. |
| C2 | P2 | Two server-wide mutations are not gated out of the concurrent lane. | Accepted after checking the existing PUBLIC catalog revoke and stable-count marker. Explicitly gate the new global catalog revoke and exact-total cases with `requires_stable_server_counts`; keep unique-role visibility grants and A/B/A/growth/listener tests concurrent. Restore catalog permissions in `finally`. S1 later removes the new revoke test entirely; existing isolated statistics tests remain unchanged. |
| C3 | P3 | It is unclear whether a caller-built `PostgresRunner` counts as an "external runner". | Accepted. Exact spec now includes caller-constructed first-party `PostgresRunner` and excludes caller-created driver connections and third-party SQLRunner implementations. |
| C4 | P3 | Ambiguous isolation for the transition table. | Accepted. Name the real private `_ConnectionOwnerBinding` class and singleton; table cases use new class instances, public binding scenarios use spawned processes, and no reset API is added. S1 retains one real private owner and isolation, but leaves its class name and field representation to the implementer. |
| C5 | P3 | Pool termination and replacement adds nothing. | Accepted after confirming default min_size=0 and one pool kwargs path. Removed the backend-kill/retry scenario. Concurrent 1/2/3 growth proves future physical creation uses the captured options; real inherited-runner rebuilding is separately covered by C1. |
| C6 | P3 | The direct-path test relies on a test-only seam. | Accepted simplification. Test real `validation.connect()` plus `SHOW application_name` in a bound spawned child; audit its existing plugin callers and retain their existing ownership/error tests. No pass-through production wrapper is required. |
| C7 | P3 | The caller-transaction test asserts a non-guarantee. | Accepted. Remove the cache-behavior assertion. Specify a warmed Queue with caller-constructed runner, `begin_immediate()`, test-owned table insertion, public usage read, own/foreign visibility checks, and caller rollback. This proves no commit or rollback by the helper through a concrete existing route. |
| C8 | P3 | The held-lock fork proof appears twice. | Accepted. The table's PID-reset row invokes the one spawn-isolated real fork scenario. There is no duplicate standalone fork test. The original scoped-warning requirement was later removed by R2 below because spawned actors do not inherit pytest's runtime warning filters. |
| C9 | P3 | Input-validation wording is loose. | Accepted clarification, with a different type choice from the suggested exact-type restriction. Accept string subclasses by base contents and retain a plain string, reject non-strings without coercion, and keep the already proposed `ValueError` contract. Add a firing string-subclass case. No backend limit requires rejecting valid string subclasses. Superseded by S1: ordinary regex/type validation remains; no subclass-specific promise or test is required. |

Claude's non-actionable observations are retained as design notes: fork reset and missing visibility can under-credit, an unbound reader seals later binding, all three held checkouts exhaust the default pool, and transition-table file placement is a manifest choice. The guide must explicitly mention read-before-bind sealing; the growth test must avoid demanding a fourth checkout. These notes add no new service or policy.

Scoped same-family round-2 verification returned PASS for C1 through C9 with no new defects and all four authoring gates passing. Claude's scoped round-2 invocation used the same read-only flags and 540-second bound, exited zero in 152.8 seconds, and returned PASS for all nine fixes with no new P1/P2 defects. It suggested the following optional P3 proof clarifications, accepted after checking the named test topology:

| ID | Reviewer finding (verbatim opening sentence) | Author disposition |
| --- | --- | --- |
| R1 | Held-lock fork proof (C1/C8): if the thread that calls `os.fork()` holds the binding lock and that lock is re-entrant (`RLock`), the child's main thread inherits it. | Accepted. A separate parent helper thread now owns the lock across the fork, with positive acquired/release acknowledgments and cleanup. That thread's absence in the child makes the intended bad-order control meaningful for either Lock or RLock. |
| R2 | The fork happens inside a spawned process, where pytest's `filterwarnings = ["error"]` (`pyproject.toml:107`) doesn't apply. | Accepted. Removed the unnecessary warning-suppression requirement from the actual test instructions; it supersedes that part of C8's first disposition. No broad warning filter or new warning-policy mechanism is introduced. |
| R3 | C9's string-subclass test can't distinguish "use the base string contents" from `str(label)` unless the subclass overrides something. | Accepted. The subclass overrides `__str__` with different contents; the real stored application name must still reflect its base string value. Superseded by S1's removal of that promise and test. |

Final scoped same-family verification returned PASS for R1/R2/R3 with no new defects. It verified the separate acknowledged lock-holder, removal of the inherited-warning-filter assumption, and the overriding-`__str__` test input. Accepted product limits and unrelated subsystem concerns remain closed. Heavily used planning, test-audit, and invocation guidance was evaluated: no new reusable rule or skill change is justified by this authoring pass.

Authoring scope is the dated plan, its Status Index entry, a non-normative backlink in the API spec, and the reviewer-availability note. The implementation plan remains `draft`; no new product spec sections or runtime APIs have been promoted by this authoring unit. Final handoff verification uses the four document gates above plus the read-only existing/proposed path check. Runtime PostgreSQL and implementation acceptance are expressly future work.

## Out of Scope

Weft admission arithmetic, budgets, thresholds, launch reservations, child priority, ancestry, task death, permit services, task-to-PID persistence, a monitoring daemon, SQLite/Redis analogues, CLI commands, arbitrary user-driver attribution, per-Queue labels, rebind/unbind, connection limits, session redesign, new dependencies, and package publication.

Plan-local alternatives: putting labels in target/config identity would multiply resources; storing a process map in `meta` would add stale-registration cleanup; querying total and owners separately would break coherent accounting; granting cross-role privileges would exceed an observational library's authority. Current evidence is sufficient to avoid these here without a new durable theory alternative record. Implementation doc 06 must retain the relevant architecture reasons when code lands.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
| --- | --- | --- | --- | --- |

## Execution Log

Implementation authorized by the owner on 2026-10-13. Class remains 5; no publication or downstream edit is authorized. S1 independent reviews above approve the current exact delta with all findings resolved.

Comprehension gate: (1) DSN, backend options and Config participate in session identity, while DSN also keys listeners. Owner metadata must travel as captured driver kwargs so labels cannot multiply resources. (2) Ordinary observers see database-attached population across roles but may not see other roles' backend types. Such rows remain total pressure and receive no credit; this API grants no visibility.

Strategy-A promotion baseline: uncommitted `docs/specs/16-python-library-api.md` at diff base `e8573fbb0dfa1126fef11a794b1214505693fb42`, adding the exact S1 [SB-API-14/15] sections verbatim before runtime implementation. Promoted file SHA-256: `83cb541803034bea37994f4730496e600f2ea21ee93d042260d3ee23c0b692e5`. Existing [SB-API-13] is unchanged. The plan's proposed sections now serve as historical review material; the spec governs implementation.

Dependency preflight: default uv 0.13.0 is outside the repository's required >=0.12,<0.13 range. Select a conforming installed tool or isolated tool installation; do not alter package requirements to bypass the gate.

### Implementation slice 1/2 evidence

The text-only spec promotion passed independent same-family review with no
P1-P3 findings. All four documentation gates passed. Initial public binder
smoke failed at the absent export, as expected. After wiring all three creation
seams, the owner suite passed 24 tests through the default-concurrency PG18
wrapper. An initial collection failure was a test error (OperationalError is
exported by `simplebroker.ext`, not the root); corrected without changing
runtime behavior. Installed `/Users/van/.local/bin/uv` 0.12.24 satisfies the
required version; use its PATH for all nested wrapper commands.

Execution sequencing adjustment: growth/listener scenarios live in the new
usage module and import the upcoming helper. Their full public usage proof
will run with slice 3, retaining independent raw PID/marker evidence instead
of staging and then rewriting the same tests. Binding slice review checks
creation kwargs and the real owner/direct/fork suite first.

### Binding review correction and snapshot evidence

Binding review found F1 (P2): lazy PID reset could race between two first
child threads and erase a binding. Accepted and reproduced deterministically:
`test_first_child_threads_share_one_binding_decision` observed successful
binding plus unbound `{}` creation options. Added a small production
`os.register_at_fork` child hook using the same reset routine before child
threads start; the lazy PID guards remain. The test controls only allocation
scheduling with real locks and no DB actor. All 25 owner tests then passed;
scoped independent verification returned PASS with no new findings.
This is a necessary safeguard discovered in implementation, not a new public
state or API. No change to [SB-API-14] is required.

Usage test collection first failed on the absent export. After implementing
the exact one-row-population query and ordinary grouping, the combined suite
produced 63 passes, two expected serial-only skips, and one test setup failure.
The listener test reused a Queue's cached closed waiter. Corrected the test
to close the handle and obtain a later handle from the same session, preserving
the existing Queue contract and the intended listener recreation proof.
Narrow BrokenPipe cleanup prevents an actor failure from hiding its original
diagnostic. All eight usage scenarios then passed at standard concurrency.
Snapshot slice independent review found no P1-P3 defect; sensitivity controls
and final gates remain pending.

### Compatibility and sensitivity evidence

The listener control omitted its connection metadata and failed at the exact
raw PID marker assertion (`''` instead of its `sb-owner:` marker). The growth
control cleared metadata only after the first configured physical connection
and failed at the second PID's marker. Both ran under standard wrapper
concurrency; originals were restored byte-for-byte. Restored runner SHA-256:
`6f321f746a842c893e6f157aed69cf2e00cdd3b590fcb1813e9245f5ab913223`.
Restored targeted/neighbour suites: 165 passed, two expected serial-only skips.

PG18 isolated statistics+usage: 42 passed, including cross-database exact
totals and opt-in autovacuum. PG15 isolated statistics+usage: 41 passed,
one expected PG18-only autovacuum skip. Full PG wrapper: shared lane 1767
passed/11 expected skips; extension lane 410 passed/7 expected serial or
opt-in skips. PostgreSQL skipped clauses in the concurrent lane are fired
by the serial runs above. Runtime and explicit 30-file PG test mypy checks
pass; no suppressions added. Driver kwargs use an ordinary private
`dict[str, Any]` because psycopg's overloaded `**kwargs` also admits typed
non-string connection arguments; no wrapper type or suppression is needed.

Initial full core suite: 4021 passed,18 expected platform/opt-in skips,two
failures from author omissions in structural traceability. Added the new
machine's structured inventory/ownership table row and extended the API
section inventory from 13 to 15; no runtime expectations were weakened.
Rerunning those gates and the full core suite from the corrected state.

Read-only downstream check: Weft manager still imports the unchanged
`get_connection_stats` through its optional PG boundary; admission logic
remains in its own controller. Weft must pin a future released extension
before adopting the additive APIs. No downstream file was edited.

### Different-family implementation review

Invoked repo call-agent review posture: `claude -p <verbatim plan and scoped
diff brief> --permission-mode plan --allowedTools Read,Grep,Glob`, stdin
closed, timeout540s. Exit0 in246.6s; verdict **no blocker**, seven P3 findings.
No repository file was changed by the reviewer. Exact opening findings and
author dispositions follow:

| ID | Reviewer finding (verbatim opening) | Disposition |
| --- | --- | --- |
| F1 | `total == count(*) - count(null)` says the same thing as the `attached` check above it. Nothing asserts that rows with NULL `datid` actually exist, so the exclusion proof only holds if they happen to. | Accepted: require positive NULL-database population before checking exclusion. The extra statement is an independent test oracle, not a production snapshot query. |
| F2 | The eager at-fork hook now always resets first, so the lazy PID checks never fire on CPython fork paths. No test exercises them. | Retain as a documented fallback, as the reviewer explicitly permits. Both entry points keep the plan's common pre-lock guard; private independently constructed state owners are not individually registered at fork. The eager hook owns safe concurrent first entry for the production singleton. No new runtime layer or test campaign. |
| F3 | The forced race depends on `owner_module.threading.Lock` being looked up when the lock is created. If the code changes to `from threading import Lock`, the test passes without the hook. | Accepted as a documented dependency control; added a precise maintenance comment. Its current red proof was observed on this actual lookup. |
| F4 | The three new exports are imported in tests but nothing checks they are in `__all__`. | Accepted: extend the existing public export subset assertion, without another test. |
| F5 | `fan_in = None` is assigned and never used. | Accepted: remove unused assignment. |
| F6 | The PostgreSQL inspection cell replaced the explicit `[SB-API-13]` with `[SB-API-15]`. The range still covers it. | Accepted clarity fix: use the explicit inspection range [SB-API-13]–[SB-API-15]. |
| F7 | The log still lists sensitivity controls as pending. Doc06 has one overlong sentence that runs two consequences together. | Accepted: append current closeout evidence and rewrap the rationale. Historical pending notes remain historical. |

Reviewer observations remain scope limits: separate test-oracle statements can
race unrelated server activity, so exact counts run only in the existing
isolated lane; role teardown follows existing fixtures; PG15 evidence is the
actual41-pass run above. No launch policy or permissions machinery was added.

### Interface and theory checks

Applied repo interface-review and call-agent skills. Independent interface
walk: principles1-5 and9-11 met by compact types, package-specific discovery,
clear names, one label marker, derived counts, shared synchronization and
explicit cooperative observation limits. Ratified departures: principle6
permits this explicitly documented process-lifetime setup because creation
metadata must precede shared resources; principle7 rejects invalid opaque
labels without normalization; principle8 uses ordinary Python exceptions and
None success following the existing library surface, with recovery guidance
in binding errors. Code evidence: `_connection_owner.py` lines35-77 and
`connections.py` lines117-149; guide lines892-940. No untested public result
fields or new enums. No new runbook candidates.

Class5 theory-possession probe: should launch reservations or deciding whether
an owner is dead be added beside this snapshot? No. [THEORY-2/3/5] assigns
application execution and policy downstream. The extension owns creation
metadata and PostgreSQL observation only; omitted credit is explicitly not
absence/liveness proof. The actual implementation respects that prediction.

Both new guide/extension README Python blocks executed against real PG18 with
the candidate installed path
`extensions/simplebroker_pg/simplebroker_pg/__init__.py`, using the documented
caller-supplied DSN/Queue context. Both returned one credit for the shared
example label. The test-owned schema and Docker service were cleaned up.

Skill/runbook evaluation: test-audit revealed no reusable guidance gap; its
existing dependency-control/sensitivity rule covers the fork scheduling proof.
Call-agent bounded execution/containment worked. Interface-review's declared
departure rule handles lifecycle setup. No durable lesson or guidance change
is justified beyond this implementation's local rationale.

### Current verification and uncommitted handoff

Post-review targeted PG18 serial suite: 67 passed (owner25 plus stats/usage42),
including positive NULL-database evidence and autovacuum. PG15 recheck:
41 passed, one intended PG18-only autovacuum skip. Current full-manifest plus
API inventory:39 passed. Runtime mypy56 files and explicit PG-test mypy30
files pass. Ruff check,352-file format check, suppression index, DOM15,
plan-context, doc-paths and diff-whitespace checks pass. The last full core
run passed4023/18 expected skips; full PG passed1767 shared+410 extension,
with only documented platform/opt-in skips. Current standard-concurrency
owner/usage/statistics rerun follows this record to confirm post-review edits.

Scoped final independent verification returned PASS for known structural
omissions and accepted Claude F1-F7 dispositions, with no new defects. It
corrected its own inventory citation: the machine row belongs to implementation
doc07, not agent-availability doc03. All review findings are addressed or
explicitly retained with rationale. No behavioral spec deviation remains;
the eager fork hook and staged growth/listener proof were declared above.

The requested APIs, integration tests, compatibility wiring and docs are
implemented in the working tree. No package versions, lockfiles, Weft code,
policy, installed DB object, commit or release were changed. Runtime ownership
is `_connection_owner.py`, inspection remains in `connections.py`; creation
edits are confined to runner/validation and package exports. New proof lives
in `test_connection_owner.py` and `test_connection_usage.py`; existing statistics
and state/API inventory gates were extended. The rest of this unit is the
named spec, rationale, registry, guide, CI, changelog and plan/index alignment.

Commit gate: the owner requested implementation, not a commit or publication.
Per AGENTS, do not create a commit merely to satisfy the closure rule. Leave
this plan/index **active, verified and uncommitted for owner review**. Ordered
task5's verification/traceability work is satisfied; formal closure remains
pending an authorized commit. HEAD remains `e8573fbb0dfa1126fef11a794b1214505693fb42`.
This is an explicit uncommitted handoff, not a ready-to-land claim. Remaining
product limits are the accepted sampled/transaction-cache/role-visibility
limits in [SB-API-15], not missing admission logic upstream.

Final post-review standard-concurrency rerun:65 passed, two intended
serial-only skips, after all accepted test cleanups and exact control
restoration. Both skipped contracts passed in the67-test PG18 serial lane.
Final documentation/whitespace gates rerun after this handoff record.

### Authorized closure and commit

The owner explicitly authorized closure and commit on 2026-10-13. This
supersedes the preceding uncommitted handoff: all ordered tasks are satisfied,
and the Status Index is completed in the implementation commit. The accepted
observation, visibility and transaction-cache limits remain documented in
[SB-API-15]. No publication or downstream rollout is included.

Closure verification reruns DOM15 fixtures, plan context, documentation paths,
whitespace, and the full-manifest/API inventory gate against this final tree.
Runtime and integration evidence above remains applicable: this closeout
changes only plan status and its execution record. Git log and clean status
verify the resulting commit after creation.
