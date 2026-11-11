# Runtime boundaries and failure handling

Class: 5 — public fork, target-lifetime, validation, CLI failure, and retry-contract changes; [DOM-5] risky triggers include cross-process ownership, deferred generator cleanup, and compatibility surfaces. Hardening applies. No program-theory or process-guidance revision is proposed.
Status: active
Date: 2026-11-11
Owner: SimpleBroker product owner; implementing agent for the approved slices.
Boundary: the seven investigated reports R1–R7 below, plus downstream Weft compatibility for R1.
Plan type: implementation with spec revision; user authorized implementation on 2026-11-11.
Verification: real process/backend regression proofs, named focused and integration gates, cross-family plan/delta and implementation reviews.
Required action: review the proposed support boundaries before promotion; implement the dependency-ordered slices without silently expanding the guarantees.

## Goal

Prevent silent loss or misrouting within supported use, reject detectably unsafe
SQLite fork use and overlong PostgreSQL schemas, and make destructive cleanup
and quiet-watcher recovery limits explicit. Address each finding through either
a code correction or a named support boundary with a verified supported path.
Do not promise automatic recovery across arbitrary fork or target replacement.

## Source Documents

Consulted the machine read order in `docs/agent-context/context.index.yaml`:
`docs/program-theory.md`, the context hub README, decision hierarchy, shared
and engineering principles, relevant runbooks, the lessons pointer, and the
Golden Rules plus dated lessons after the coalescing watermark.

- [Program theory](../program-theory.md): [THEORY-3] resource owners,
  [THEORY-4] explicit safety and Unix composition, [THEORY-5] application-owned
  process topology, [REV-THEORY-004] session resource ownership,
  [REV-THEORY-005] suspended operation ownership, [REV-THEORY-006] public sessions.
- [CLI contract](../specs/10-cli.md): [SB-CLI-1/2/4].
- [Delivery contract](../specs/11-delivery.md): [SB-DELIVERY-1/2/3/5/6].
- [Library contract](../specs/16-python-library-api.md): [SB-API-2/3/5/6/9/10/11].
- [Operations contract](../specs/17-ops.md): [SB-OPS-5/7].
- `docs/README.md`, `docs/specs/product-section-registry.md`,
  `docs/implementation/06-process-session-core-ownership.md`,
  `docs/implementation/07-complexity-and-state-machine-map.md`,
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`, and
  `docs/implementation/03-agent-inventory.md`.
- `docs/agent-context/runbooks/writing-plans.md`, `hardening-plans.md`,
  `writing-specs.md`, `review-loops-and-agent-bootstrap.md`, `testing-patterns.md`,
  `adversarial-acceptance-probes.md`, `maintaining-traceability.md`, and
  `designing-agent-facing-interfaces.md` (all under the same runbooks directory).
- `skills/call-agent/SKILL.md`, `skills/interface-review/SKILL.md`, and the
  authoring gate in `skills/test-audit/SKILL.md` before test implementation.
- Archived owner decisions: `178822b:docs/plans/2026-11-08-watcher-retry-streak-budget-plan.md`
  (normal processing wait is the recovery witness; public retry contract Q2
  deferred), and `178822b:docs/plans/2026-11-08-pg-quiet-process-connections-plan.md`.
  Live [ALT-IMPL06-001/002] preserve backend-owned rechecks and defer active pings.
- Upstream: [SQLite fork hazards](https://www.sqlite.org/howtocorrupt.html#_carrying_an_open_database_connection_across_a_fork_),
  [PostgreSQL identifiers](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS),
  [PostgreSQL idle session timeout](https://www.postgresql.org/docs/current/runtime-config-client.html),
  and [Python multiprocessing](https://docs.python.org/3.14/library/multiprocessing.html).

## Spec Baseline

- SimpleBroker `75616556e22cdc77490c0fb08590210ad3a2402b`: source, contracts,
  implementation rationale, and tests inspected and probed.
- Weft `00d929e942812e78ae92c7339c2ed0f86391b407`: client/runtime/Django paths
  inspected; client probe also ran against this SimpleBroker checkout.
- Promotion baseline: record the spec-edit commit or diff-base plus exact spec
  diff during the spec-promotion slice. Until then the proposed text below is
  a review target, not an active contract.
- No normative edit is made by this planning task. Approval of this plan is
  the decision point for the proposed restrictions, including conservative
  process-wide SQLite refusal and unsupported retained PostgreSQL clients.

## Investigation Disposition Matrix

| ID | Observed evidence at baseline | Disposition and owner |
|----|--------------------------------|-----------------------|
| R1 | Fresh child Queue: 20 writes return, 19 disappear when parent closes; raw child sqlite3 also reproduces. macOS and Linux confirmed; archived 8.3.0 confirmed on macOS. Linux 3.13 default multiprocessing fork loses 57/60; Linux 3.14 forkserver and macOS spawn retain 60/60. | Code refusal plus narrower [SB-API-11] support boundary; slice 1. No automatic before-fork close. |
| R2 | Actual legacy CLI first writer races reads: parent-author run has 20 invalid-header exits among 90 reads; all 30 writers succeed. A phase-controlled real connect creates the zero-byte file before WAL/header setup. | Fix legacy SQLite CLI preflight sequencing; slice 3. Keep initialized-file validation strict elsewhere. |
| R3 | PG old client caches route→old-target/version 1; separate-process CLI cleanup/recreation installs route→new-target/version 1; old client remains stale, even after observing a missing-meta error in the gap. Archived 8.8.1 refreshes in the same probe. | Declare cleanup a target-lifetime boundary and verify fresh-owner use; slice 5. Do not add live-target reincarnation support or pin stale results as intended. |
| R4 | Real hung-up terminal and closed fd 1 each leave watch alive claiming all 20 later messages. EPIPE control exits and leaves 20 pending. | CLI output failures are terminal except existing clean closed-pipe handling; slice 3. Library user-callback continuation stays supported. |
| R5 | Redis normal child exit without Queue use releases all 10 parent batch reservations; another consumer gets all 10; parent yields its remainder then raises stale-batch OperationalError. os._exit control preserves reservations. Archived 8.3.0 also reproduces. | Immutable batch process owner; slice 2. Child finalization abandons, child advancement refuses, parent retains settlement authority. |
| R6 | PG accepts a 63-byte schema and two 64-byte names sharing that prefix. Distinct cores share rows; cleanup via B deletes A. Full-name lock/channel identities differ although current_schema() is identical. | Reject overlong schema names before resource creation, across plugin and direct constructor paths; slice 4. No truncation or automatic data migration. |
| R7 | Real PG idle_session_timeout=10s kills quiet watcher at 36.115s after three losses and 2s/4s backoffs. Notification-only control survives four losses at 51s with five listener generations and 21 successful processing waits. | Preserve bounded retry policy, promote it into public docs, document listener idle-timeout deployment requirement, add real proof; slice 5. |

The investigation used disposable targets and causal pipe barriers. Temporary
probe scripts are not implementation dependencies: reconstruct the schedules
described in the testing matrix below in repository tests. Existing fork tests
passed while R1 reproduced: the new-instance test closes the parent before fork.
Weft's spawn launch paths are unaffected, but retained WeftClient embedding lost
19/20 submissions when its caller forked; manager startup alone was suppressed
to keep requests undrained. Django exposure was initially code-traced; the
implementation qualification later reproduced 20 acknowledged submissions with
19 missing through the real request lifecycle (see execution evidence).

### Principle-level diagnosis

R1 and R5 confuse a new wrapper/connection with resource ownership ([THEORY-3],
[REV-THEORY-005]); R3 and R6 confuse logical names or reusable counters with
storage identity; R2 validates a transient creation state as a completed object;
R4 applies a library callback policy to a terminal CLI output sink; R7 exposes
an intentional recovery policy whose public limits were never stated. These
require ownership, boundary validation, lifecycle documentation, or explicit
failure classification, not general retries or a new broker framework.

## Context and Key Files

Existing paths below must be reread before their slice. Paths explicitly marked
**new** are proposed artifacts, not claims that they already exist.

| Slice | Files and load-bearing entry points |
|-------|------------------------------------|
| 1 | **New** `simplebroker/_sqlite_fork.py` (leaf admission/token state); `simplebroker/_runner.py` (`get_connection`, fork recovery, tracked close); `simplebroker/_backends/sqlite/runtime.py` (version probe and setup connections); `simplebroker/_backends/sqlite/validation.py` (read-only validation connection); `simplebroker/db.py` (fork diagnostics); `simplebroker/_broker_session.py` (read existing graph abandonment, do not replace it). Extend `tests/test_fork_safety.py`, `tests/test_runner_lifecycle.py`, `tests/test_runner_validation.py`, `tests/test_validation_lock_safety.py`; **new** `tests/test_sqlite_fork_admission.py`. |
| 2 | `extensions/simplebroker_redis/simplebroker_redis/core.py`: `_claim_batch_generator`, at-least-once `move_generator`, `_exit_generator_batch`; read runner fork recovery and existing Redis batches/core/pool/finalization tests. **New** `extensions/simplebroker_redis/tests/test_redis_generator_fork.py`. |
| 3 | `simplebroker/cli.py::_validate_legacy_sqlite_target`; `simplebroker/commands.py::_write_stdout`, `_watch_message_handler`, `cmd_watch`, stdout disposal helpers; read watcher `_ErrorHandlerFailure` terminal path. Extend `tests/test_cli_main.py`, `tests/test_cli_watch.py`, `tests/test_commands_stdout_delivery.py`, `tests/test_commands_error_ownership.py`, `tests/test_cli_broken_pipe.py`, `tests/test_watcher_transition_tables.py`. |
| 4 | `extensions/simplebroker_pg/simplebroker_pg/validation.py`: `require_schema_name`, `quote_ident`; `runner.py`: `PostgresRunner`, both activity waiter constructors, shared registry/listener; `plugin.py`: structural-runner and validation/cleanup paths. Extend `test_pg_schema_validation_paths.py`, `test_pg_plugin_contract_edges.py`, `test_pg_ownership.py`, `test_pg_activity_waiter_lifecycle.py` under the PG tests directory. |
| 5 | PG `test_pg_ownership.py`, `test_pg_quiet_connections.py`; `tests/test_watcher_edge_cases.py` and state-machine tests; watcher retry code is read-only unless a test exposes a separate defect. Exact public text is below. |
| Documentation | `docs/specs/10-cli.md`, `16-python-library-api.md`, `17-ops.md`; `docs/implementation/06-process-session-core-ownership.md`, `07-complexity-and-state-machine-map.md`; `docs/guides/python.md`, `backends.md`; `docs/agent-kernel.md`; root `README.md`; PG and Redis extension READMEs; `CHANGELOG.md`; `docs/lessons.md` only for newly demonstrated durable corrections. Update repository map if adding the leaf module. |

Before editing, implementer records answers in the execution log:

1. Why is a fresh Queue insufficient after unsafe SQLite fork, and why can its
   parent not be repaired by child close? Expected: SQLite C-library state is
   inherited without the corresponding process locks; close may itself touch
   shared WAL state. Tokens detect unsafe inheritance but do not repair it.
2. Why may Redis child recovery not change a suspended generator's owner?
   Expected: connection recovery can update mutable core/runner PIDs, but the
   reservation token still belongs to the process that started that batch.
3. Why not reset watcher retries after startup, or shorten the data recheck?
   Expected: startup alone permits repeated first-wait failure forever; shorter
   data rechecks undo the accepted PG quiet-connection budget. R7 preserves both.

An incorrect answer blocks that slice until the cited owners are reread.

## Invariants and Constraints

- The parent remains authoritative for inherited SQLite resources and Redis
  reservations. Child hooks/finalizers do not acquire inherited locks, perform
  SQLite cleanup, settle parent batches, or silently acknowledge unsafe work.
- Queue remains a capability over a session. No per-Queue connection registry,
  new public session type, dependency, database migration, or background service.
- All managed SQLite physical openings, including temporary/version/validation
  connections, participate in one detection mechanism. A Queue count is not a
  physical-connection count. A completed ordinary close removes its registration;
  failed or ambiguous cleanup remains conservative.
- Existing borrowed-runner ownership, same-process foreign-thread finalization,
  explicit-close retry behavior, and thread-owned driver teardown survive.
- No stdout success after fatal delivery failure, no new claim after detecting
  it, and no rollback of an already committed claim/move. EPIPE remains clean
  exit 0. Library user callbacks retain documented continue/stop behavior.
- Valid PostgreSQL schema spelling is preserved. No lowercase conversion or
  truncation; invalid schemas create no pool, socket, listener, schema, or
  connection-attribution seal. Preserve non-Redis Queue's lazy acquisition timing.
- R3 does not imply automatic cache invalidation across destructive replacement.
  R7 adds no ping, notification heartbeat, retry knob, or changed quiet cadence.
- Keep no-native-fork Windows coverage meaningful through validation/admission
  tests and spawn controls; never silently skip a real-fork test on Linux/macOS.
- Keep default suite concurrency. Existing opt-in serial probes are additional
  topology-specific gates, not substitutes for ordinary concurrent acceptance.

### Hidden couplings and limits

SQLite setup and validation open connections outside the runner's tracked set.
Fork can occur while another thread is opening or closing one. The child must
see a published hazard without waiting for that parent's mutex. Token tracking
does not intercept arbitrary raw SQLite, third-party drivers, native fork calls
that omit Python hooks, or reentrant fork from signals/audit hooks/SQLite
callbacks that resumes an interrupted driver call. These remain unsupported.
It also does not guarantee every inherited temporary object's finalizer or
normal interpreter shutdown is SQLite-safe. Preserve existing graph retention;
do not add a global parent strong-reference connection registry. The minimal
tracked-connection finalizer in Task 1 handles creator-process GC accounting;
it does not expand the shutdown or arbitrary-fork guarantee.

Redis's `finally` first calls a lock-taking guard-release helper. The PID check
must precede that helper, including when a child has already recovered the core.
Resume after the last yielded row is still an advancement and must not commit.

CLI callback failure currently has an explicit `CALLBACK_ERROR_CONTINUES`
transition in `SM-CLI-WATCH`. Replace that CLI transition and implementation
row together; keep the distinct generic watcher callback-continue transition.
Raw final flushes can replace the real error or skip `watcher.stop()`.
Do not redirect a possibly reused stale fd onto `/dev/null`: that could close a
backend descriptor. Failed normal CLI wrappers must close before replacement so their buffered
bytes cannot be flushed again at GC. Replace both sys.stdout/sys.__stdout__ only when they reference the same
failed CLI-owned wrapper (likewise stderr); preserve healthy interpreter
originals and caller-owned stand-ins. Check absent/closed streams in parsed
watch dispatch before target resolution or validation opens a backend. Arbitrary external descriptor close/reuse during
execution cannot be repaired after the original write may already have hit a
new resource; no descriptor-recovery framework is promised.

Schema names participate in session identity, advisory locks, ownership
inspection, cleanup, and LISTEN channels. Rejection at one plugin path is not
enough because exported runners and structurally supplied runners bypass it.

## Proposed Spec Delta

Strategy A for the code-bearing paragraphs in [SB-API-2/5/11] and
[SB-CLI-1/2]: promote text before implementation-link claims, then add those
claims with each corresponding code/test slice. Strategy D for the explicit
cleanup/retry support boundaries in [SB-OPS-7], [SB-API-3/6], and the
[SB-OPS-5] cross-reference. Existing files remain active; no file-level
reclassification. Add this plan to their Related Plans sections at promotion.

### D1 — [SB-API-11], replace the paragraph beginning “A no-runner persistent Queue”

> A no-runner persistent Queue checks inherited process-session ownership before
> acquisition, project setup, or cleanup can enter a parent-owned lock.
> Inherited SQL-backed handles reject operation acquisition with RuntimeError.
> Redis handles may recover child-owned session state through the process
> registry. Releasing an inherited lease must not finalize parent resources.
>
> SQLite use in a forked child is unsupported if the parent had any SQLite
> connection open, opening, or closing at fork. This includes temporary and
> ephemeral operations while their physical connections are live. Creating a
> new Queue, BrokerSession, or SQLiteRunner does not repair that state.
> When SimpleBroker detects one of its own managed SQLite connections or
> in-flight connection lifetimes at a Python fork, subsequent managed SQLite
> operations in that child raise RuntimeError before entering
> SQLite or acquiring inherited resource locks. Refusal is process-wide,
> including different targets and memory databases, and remains in descendants
> until a fresh process image starts. PostgreSQL and Redis are not prohibited
> by this SQLite admission rule. The refusal cannot be repaired by retrying in
> that child; built-in watchers may apply their existing bounded retries before
> propagating it. This clause does not add a watcher exception-classification API.
>
> Detection covers connections opened by SimpleBroker, including its built-in
> SQLiteRunner, setup, and validation paths. Arbitrary raw SQLite connections,
> third-party runners, native forks bypassing Python hooks, and reentrant forks
> inside signals, audit hooks, connection factories, or SQLite callbacks are
> outside this detection guarantee. The guard does not make inherited SQLite
> finalization or interpreter shutdown safe. Applications must close all SQLite
> resources before fork, or use spawn/exec or a clean forkserver that has not
> opened SQLite in its preload code. Never-used handles and fully completed
> ephemeral operations do not by themselves make a fork unsafe. A managed
> connection closed by normal creator-process garbage collection retires its
> registration after closure, just as explicit close does. Unconfirmed cleanup
> remains conservatively registered; callers must use explicit close to make
> the pre-fork boundary deterministic.
>
> Newly constructed child handles remain usable subject to these backend
> boundaries. These rules do not transfer a suspended parent generator or
> change a third-party injected runner's ownership policy.

In the next BrokerSession paragraph, replace only “connect() in the child
always creates a new session” with:

> connect() in the child creates a child-owned session; its backend operations
> remain subject to the SQLite fork-admission rule above.

### D2 — [SB-API-5], append at the generator lifecycle boundary

> An at-least-once Redis claim or move iterator belongs to the process where
> its generator body first begins execution. Advancing that suspended iterator
> in a forked child raises RuntimeError before yielding another row, starting
> another batch, or settling the current batch. Closing or finalizing its child
> copy abandons that copy without acquiring inherited locks or committing,
> rolling back, or otherwise releasing the parent's reservation. This includes
> a child that merely exits normally. Fresh child-created iterators retain the
> backend's ordinary recovery behavior; same-process cross-thread cleanup keeps
> its existing semantics.

### D3 — [SB-API-2], insert after PostgreSQL option-validation paragraph

> PostgreSQL schema names must match the existing ASCII identifier grammar,
> must not be public, and must contain at most 63 bytes. SimpleBroker rejects
> longer names with DatabaseError rather than relying on PostgreSQL truncation.
> As with other acquisition failures, Queue may wrap this backend error in
> RuntimeError with DatabaseError as its cause; direct validation raises DatabaseError.
> This validation precedes connection, pool, listener, and database creation or
> mutation through plugin entry points and exported runner/waiter constructors.
> Non-Redis Queue construction remains lazy: rejection may occur on its first
> acquisition, but always before backend resource creation. Valid spelling is
> preserved. Existing overlong configurations require explicit operator repair;
> changing configuration cannot separate data already merged into one schema.

### D4 — [SB-CLI-1], insert after streaming closed-pipe paragraph

> For watch in consume, peek, and move modes, an output failure other than the
> recognized closed-pipe condition is terminal. After detecting it at the
> output boundary, the command admits no further message, releases its runtime
> resources, and exits 1. With writable stderr it emits the ordinary diagnostic;
> parsed JSON mode uses code ERROR and retryable false. Missing or closed stdout
> or stderr at startup is refused before target acquisition. If stderr is
> unavailable, the command exits 1
> without a diagnostic. For normal CLI-owned streams and streams absent or
> closed at launch, no interpreter-flush warning or exit status 120 may replace
> that result. This does not promise recovery after external code closes and
> reuses a raw stream descriptor while the command is running. A claim or move committed before detection remains
> committed; inspect state before retrying. This CLI policy does not change
> library watcher error-handler continuation.

### D5 — [SB-CLI-2], insert before “For an ordinary relative legacy-SQLite target”

> Ordinary legacy-SQLite command preflight does not classify a regular
> zero-byte target as corrupt merely because a concurrent first writer has
> created the file but not initialized it. It defers that state to the normal
> coordinated initialization and admission path. This also applies to a regular
> zero-byte file created by the caller: read, peek, exists, move, list, stats,
> and root --vacuum may initialize it through that existing path. Nonempty invalid files and
> invalid path types retain their error behavior. This does not relax the
> initialized-target checks used by init, cleanup, or project discovery. Once
> admitted, read returns its ordinary message or empty result; it does not
> reserve visibility of a concurrent write.

### D6 — [SB-OPS-7], insert before the SQLite-specific paragraphs

> For PostgreSQL, destructive target cleanup ends the lifetime of existing
> clients of the physical schema. Predictable cleanup requires quiescing target
> activity across processes, finishing iterators and transactions, stopping
> watchers, and closing every owning Queue, BrokerSession, open_broker/sidecar
> scope, and caller-owned injected runner. Closing one Queue while another
> owner retains the session is insufficient; thread-cache recycling does not
> replace ending the target lifetime. The rule applies even when different
> DSNs or options name the same physical target.
>
> After recreation, construct fresh owners. Retained clients across target
> destruction/recreation are unsupported, including their alias resolution,
> admission caches, and schema state. Cleanup neither waits for nor verifies
> quiescence. There is no live-client cache-invalidation or target-replacement
> recovery guarantee. SQLite's more detailed unsafe-overlap rules below remain
> in force; this paragraph does not add a new Redis cleanup guarantee.

Append to [SB-OPS-5] and [SB-API-3] as a short cross-reference:

> PostgreSQL aliases and retained client scopes are subject to the destructive
> target-lifetime boundary in [SB-OPS-7]; use fresh owners after recreation.

### D7 — [SB-API-6], insert after safety-recheck scheduling

> For ordinary retryable infrastructure failures, built-in watchers bound
> recovery per failure streak. A streak ends only
> after a processing wait returns normally after the latest start or restart,
> through activity or a quiet timeout; successful startup and initial drain
> alone are insufficient. The third failure in one streak is
> terminal. Retry delays before it are 2 and 4 seconds. The total retry budget
> is 300 seconds, checked between attempts rather than interrupting in-flight
> driver calls. The first streak is timed from run start; after a successful
> processing wait, a later streak is timed from its first failure. Retried
> failures emit warning diagnostics; terminal errors propagate through the
> normal synchronous or background-thread error path.
>
> PostgreSQL quiet processing waits can remain inside the strategy for the
> documented 30–60-second safety recheck. Repeated listener disconnects before
> any such wait returns exhaust the recovery budget, even if reconnect succeeds
> each time. Indefinite reconnect is not promised. Deployments requiring quiet
> continuous watching must configure the SimpleBroker target or role with a
> compatible idle-session policy, for example the existing DSN/libpq option
> options='-c idle_session_timeout=0'. That policy covers dedicated LISTEN
> sessions and command connections; there is no separate listener DSN setting.
> Server idle-session termination is distinct from SimpleBroker's command-pool
> idle reaping. The library does not override server timeout settings or add
> heartbeat queries to satisfy them.

### Difference register

| Changed baseline assumption | Preserved boundary |
|-----------------------------|--------------------|
| Fresh SQLite child Queue always usable → conservative detectable-fork refusal | SQL inherited-object guard, Redis recovery, borrowed runner ownership, close-before-fork success. |
| CLI callback output error may continue → terminal CLI output failure | Library callback continuation, EPIPE clean stop, already committed delivery effects. |
| Regex-valid PG schema accepted → portable maximum 63 bytes; direct/structural public schema bypass → rejection | Existing plugin public exclusion is extended explicitly to exported and structural entry points; spelling, lazy Queue and pre-I/O validation remain. |
| PG retained clients refreshed aliases in 8.8.1; counter ABA regression now has an explicit unsupported replacement boundary | Destructive cleanup remains non-quiescing; ordinary alias updates within a target lifetime retain freshness. |
| Retry details exist only in implementation → explicit public bounded policy | Existing processing-wait witness, 30–60s recheck, no active pings, no new configuration. |

## Tasks

Dependency map: spec promotion precedes code; slices 1–5 may then proceed in
disjoint code lanes, with one owner reconciling shared specs and implementation
docs. Slice 6 depends on their verification and R1's Weft coordination. The CLI
findings share one lane; do not give two agents overlapping edits to commands.py.
Prioritize R1's silent loss and R6's cross-tenant read/cleanup collision, followed
by R4's drain and R5's reservation corruption. Independent corrected slices may
ship with their corresponding contracts once their own gates pass; the combined
plan remains open until all seven reports and downstream coordination are closed.

0. [x] **Spec-promotion slice.** After plan/delta review and owner adoption,
   apply D1–D7 with the named strategies, add Related Plans links, and record
   the promotion baseline. Do not label new code-bearing clauses implemented
   until their slice lands. Stop if a proposed boundary is not accepted; amend
   and re-review that decision, not the entire unrelated subsystem.

1. [x] **SQLite admission and Weft coordination (R1).** Add one leaf module
   with creator PID, one integer active-lifetime count, a short parent-side
   reentrant registry mutex, and a sticky unsafe-fork flag. Each lifetime has
   one once-only accounting flag, not per-phase opening/live/closing states.
   The count covers the entire potentially live interval. State machine
   `SM-SQLITE-FORK-ADMISSION`: clean parent, active lifetimes, tainted child.
   Register an after-in-child hook before the first managed open. It reads
   the count without inherited locks, ORs nonzero into the sticky
   flag, installs fresh synchronization, and performs no SQLite call. Do not
   raise from the hook or acquire a before-fork barrier.

   Publish a token before each actual connect at the four audited sites
   (runner, validation, version probe, setup). Keep it through use and closing;
   remove only after confirmed successful close. A failed connect may retire
   its token only once no connection survived; failed or ambiguous close remains
   registered. Counter/once-only-flag updates alone hold the mutex, never driver
   I/O, container snapshots, or application callbacks. Reentrancy avoids a
   creator-process GC finalizer deadlocking on the accounting lock. Keep
   critical sections to integer/boolean state; no stale read/modify/publish
   sequence may overwrite a nested finalizer update. Tokens do not own strong
   connection references.

   Use one internal sqlite3.Connection subclass through factory= at those
   openings. Preserve signatures/settings, real driver behavior, and runner
   ownership. Use check_same_thread=False for these internally owned connections,
   including temporary ones: creator-process GC can run on another thread,
   while Python-level close otherwise rejects it. Keep runner operation/close
   synchronization; this does not make connections publicly thread-shareable.
   Its idempotent close retires the token only after actual driver
   close succeeds. Its minimal creator-process finalizer uses that same close
   path so GC of a dropped injected runner cannot leak a hazard for the life
   of the process. This is a necessary tracking lifecycle, not a new owner that
   closes reachable borrowed resources. Do not retire from a weakref callback
   that could run before the C connection closes. Failed finalizer close leaves
   the hazard conservative and must not be reported as safe.

   Check the immutable creator PID before any connection-level lock or driver
   close. A child copy must be retained in the existing abandonment mechanism
   (or its single leaf-owned equivalent) before returning from finalization;
   skipping Python close alone does not prevent the base C deallocator from
   closing it. Keep strong retention child-only; never retain parent connections
   globally. Existing session/runner graph retention remains. Normal interpreter
   teardown is still outside D1's guarantee. Verify subclass initialization
   failure, cyclic GC, and double-finalization behavior before integrating it.
   Treat constructor failure separately from closing a successful connection:
   native SQLITE_CANTOPEN closes its temporary handle before returning, while
   close on that uninitialized Python object raises ProgrammingError. Retire
   that verified no-handle path exactly once; do not poison later safe forks
   merely because a path was missing or unwritable. Qualify other ordinary
   native initialization failures against the supported driver implementation;
   preserve conservative accounting for ambiguous callback/audit failures. Do
   not infer “no live handle” from every constructor exception. The initial
   prototype deliberately fails this case; the corrected CANTOPEN probe passes
   on CPython 3.11/3.13/3.14. Source evidence:
   [CPython 3.11 native initialization](https://github.com/python/cpython/blob/3.11/Modules/_sqlite/connection.c#L183-L263)
   and [CPython 3.14 native initialization](https://github.com/python/cpython/blob/3.14/Modules/_sqlite/connection.c#L244-L330).

   Guard get_connection, run, owned_connection, begin_immediate, commit,
   rollback, the shared _run_exclusive_setup entry (all three public setup
   wrappers), and _settings_connection. Place the guard after existing PID
   recovery and before operation/setup/transaction locks or driver calls.
   Guard each of the three non-runner opening routes too. Do not reject close,
   is_setup_complete, or release_runner_thread_connection: cleanup and marker
   recovery must retain their existing abandonment/no-op behavior. Remove the
   post-connect PID check from the promised mechanism: a same-thread reentrant
   fork remains unsupported, rather than receiving a partial safety promise.

   Detection stays sticky, even for a different filename or :memory:. SQL
   diagnostics must not falsely tell PostgreSQL-only callers they share this
   restriction. Guidance: close all SQLite resources before fork or start a
   fresh process; a new Queue in this child is not a fix. Built-in watcher retry
   classification stays unchanged and may retry this refusal before propagating.

   Verify physical-lifetime cases in T1 before claiming the guard works.
   Stop and replan if implementation needs to close inherited connections,
   globally retain live parent connections, or synchronize a driver call at fork.
   Free-threaded or alternate-interpreter guarantees require actual qualification;
   do not infer them from the GIL. Review this coherent slice independently.

2. [x] **Redis batch ownership (R5).** Capture creator PID in the generator
   body before first reservation and retain it through subsequent batches.
   Check on resumption after each yield, including the final row, before
   any subsequent batch. In finally, test ownership before `_exit_generator_batch`, commit,
   rollback, or maintenance. Child advancement raises RuntimeError; child close
   skips settlement without returning from finally or suppressing another error.
   Do not compare mutable core/runner PID and do not add thread rejection.
   Scope the promise to at-least-once claim/move iterators, not every Redis
   iterator. Verify T2 and existing foreign-thread cleanup, then review.

3. [x] **CLI startup and output handling (R2/R4).** In legacy target preflight,
   defer only an existing regular zero-byte file to established runner setup.
   Retain the validator for nonempty files and invalid path types; do not weaken
   shared validation or change project admission without a separate causal proof.
   Include later header/no-schema and pre-commit setup interleavings in T3.

   Give both QueueWatcher and QueueMoveWatcher a CLI-owned error handler that
   raises a distinct private delivery-failure carrier through the existing
   terminal callback path, preserving the original error as its cause. Do not
   re-raise the same object and create `raise e from e` when the watcher unwraps
   `_ErrorHandlerFailure`. cmd_watch extracts the original exception, finishes
   cleanup, then raises it outside the carrier's exception handler so direct
   callers retain its type without a cyclic cause/context chain.
   Keep `_StdoutClosed`/StopWatching's clean stop.
   `_output_message` already flushes through `_print_stdout`; remove redundant
   raw stdout flushes from the watch callback/finalizer. Guaranteed stop must
   run despite diagnostic or cleanup failure, preserving the primary error.

   In cli._main for parsed args.command == "watch", validate absent/closed
   standard streams before _resolve_target, _validate_legacy_sqlite_target, and
   _validate_command_target. Target validation can open SQLite or a PG socket.
   If either stream is absent or already closed, return 1 before those opens
   (no diagnostic if stderr is unavailable). Installing a sink never authorizes
   this rejected watch to continue. Keep a
   cmd_watch check before _resolve_watch_inputs/banner as a direct-call backstop.
   After a fatal failure on a normal CLI-owned stream, close its failed wrapper
   BEFORE opening a sink; CPython's ordinary TextIOWrapper closes its buffer
   even when close raises the original flush error. Suppress only those expected
   stream-close failures, confirm the wrapper is closed, then open devnull and
   replace both sys.stdout/sys.__stdout__ only if they reference that same
   failed CLI-owned object (likewise stderr). For an injected stand-in, preserve
   the caller-owned object without closing it, replace only the public reference
   as existing disposal does, and never redirect the healthy interpreter
   original. Custom stream destruction remains the caller's responsibility. Retaining
   an open buffered wrapper or calling detach is insufficient: detach flushes.
   Never dup2 over a possibly reused descriptor. For a missing startup stream,
   keep the opened sink alive even if it gets fd 1 or 2; do not close it in a
   helper finally block. Preserve existing EPIPE clean termination and verify
   the fd-equals-devnull helper edge if the shared redirection helper changes.

   The watch-owned terminal path must handle unavailable stderr from _status,
   _emit_error, and final flushing without letting it replace exit 1 or skip
   cleanup. Check streams before _resolve_watch_inputs (alias resolution may
   open a backend) and the startup banner. Scope the CLI exception-emission
   guard to parsed watch commands: never call print(file=None), which falls
   back to stdout; suppress only expected stream I/O/closed/absent failures
   during that terminal emission, then return 1. Use the same close-before-
   replace mechanism; emit the diagnostic only if stderr remains writable.
   Preparse diagnostics are unchanged. Keep ordinary finite commands'
   payload/error semantics and generic library callback continuation. Do not
   add os._exit, a descriptor-identity framework, or pretend to recover arbitrary
   externally replaced raw descriptors. Qualify the normal-wrapper behavior
   on supported runtimes in T4; a runtime where closing leaves a live buffer
   blocks that guarantee until a safe terminal path is proved. Update the CLI
   transition table and implementation map, then review the interface.

4. [x] **PostgreSQL schema boundary (R6).** Add one pure shared schema validator
   in validation.py; require existing grammar, non-public, length <=63 bytes.
   Reuse from require_schema_name, PostgresRunner before pool construction, and
   both exported activity waiter constructors before key publication, socket,
   thread, connection, or owner-attribution capture. Structural-runner plugin
   paths must pass the same validator. Private registry/listener helpers may
   rely on those validated entrances; do not duplicate validation at every
   internal layer. The existing direct/structural public-schema bypass closes
   intentionally: record that compatibility change and add rejection coverage.
   Do not apply a schema-only public-name
   rule to generic SQL identifiers. Reject rather than hash/truncate user input.
   Verify T5 including destructive cleanup preservation; review independently.

5. [x] **Cleanup and quiet-watcher boundaries (R3/R7).** Align guides, PG README,
   root restatements, kernel, implementation rationale, and CHANGELOG with D6/D7.
   Use an existing DSN/libpq option or role policy scoped to the SimpleBroker
   target, such as conninfo `options='-c idle_session_timeout=0'`, to exempt its
   LISTEN sessions; no new backend_options key or independently configurable
   listener DSN is promised. Such a target-scoped option can cover command
   connections too; their existing client-pool idle reaping still applies.
   Name the 8.8.1-to-baseline alias-counter ABA regression in CHANGELOG, along
   with the required close-all-owners maintenance procedure and the newly
   explicit support limit. Do not portray earlier successful refresh as a
   user mistake or describe documentation as restoring live refresh.
   Add supported cleanup/recreate/fresh-session proof T6; retain ordinary alias
   mutation freshness tests. Add real server-idle-expiry evidence T7 alongside
   existing retry-state tests. Do not introduce a stale-cache “expected result”
   test, heartbeat mechanism, or alternate health witness. If the owner instead
   requires live replacement or continuous short-expiry recovery, stop that
   slice and author a revised delta with generation identity or a stronger
   recovery witness; do not quietly change this plan's disposition.

6. [ ] **Downstream and integration closure.** Complete Weft compatibility
   checks below, the final gates, independent combined review, and reciprocal
   spec/test/implementation links. Evaluate skill/runbook improvement candidates
   without silently promoting process rules. Record durable implementation
   lessons when evidence adds a correction not already owned elsewhere. On
   actual implementation completion, close this plan's Status Index row in the
   same change; until then it remains draft/active. Follow explicit-file staging
   and owner commit authorization. Do not claim landed completion without git-log
   evidence; an uncommitted review handoff must say so in the conversation.

## Testing Plan

Use `skills/test-audit/SKILL.md` at authoring: each test states its behavior,
credible failure, missing coverage, and smallest meaningful owner boundary.
Tests for code fixes must fail before the fix, or under a production-guard
mutation if existing behavior is safe by coincidence. Documentation-only
boundaries use positive supported-path evidence and inspection; do not invent
a failing test that asserts unsupported behavior as intended.

| Gate | Real proof and required controls |
|------|----------------------------------|
| T1 / R1 | Fresh exec-isolated parent opens persistent SQLite, forks, child attempts fresh Queue, parent closes at the recorded phase. Child now refuses before its first write acknowledgment; parent data remains readable from a fresh observer. Exercise low-level built-in injected runner, worker-thread connection, validation/setup/version probe, fork during opening and closing, failed close, held registry/runner locks, unrelated target, memory target, nested fork, and parent continued use. Controls: never-used handle, all handles explicitly closed before fork, dropped injected runner after confirmed GC closure, cyclic and foreign-thread GC, completed ephemeral operations, spawn and clean forkserver. Prove explicit close and GC retire once, missing/unwritable-path native open failure does not leak a count (then close other handles and prove a subsequent child succeeds), ambiguous failure stays conservative, and a fork during GC close still refuses. Test every audited opening route with real driver calls and barriers; run existing safe-fork tests after the GC case in the same process to expose state leakage. Use explicit multiprocessing.get_context("fork"/"spawn"/"forkserver") for behavioral gates. Forkserver preload with a live managed connection is a negative control. Separately retain a version-default smoke for the reported 3-worker schedule: Linux 3.13 refuses; clean 3.14 forkserver/macOS spawn retain 60/60. No assertion that raw third-party handles are detected. |
| T2 / R5 | Real Redis reservations, claim and move, normal child interpreter exit, explicit child close, and child next after an ordinary/final row. Independent consumer sees no released parent reservation; parent exhausts/commits without stale-token error. Recover core in child before touching old iterator to defeat mutable-PID guards. Hold another parent thread's core lock to prove child close does not acquire it. Preserve parent rollback, fresh child iterator, and same-process foreign-thread close. Run normal-exit cases inside a fresh harness subprocess so pytest teardown is not inherited. |
| T3 / R2 | CLI subprocesses against a fresh legacy file. Barrier after real sqlite connect publishes zero bytes; start reader, witness its attempted setup acquisition, then release writer. Read gives message or empty according to ordering, never invalid-header. Repeat at valid header before schema and before bootstrap commit. Enumerate the changed preflight set: read, peek, exists, move, list, stats, and root --vacuum, using each command's own success/empty result contract. Controls: caller-created regular zero-byte file for every affected command, absent read exit2, invalid nonempty file/error1, directory, existing valid empty/nonempty DB, and unchanged init/cleanup validation. One natural race stress companion is useful but not a rate threshold. |
| T4 / R4 | Real pty hangup/EIO with buffered residual bytes, fd1 absent/closed before process launch, and closed Python stdout/ValueError; EPIPE negative control remains0. Exercise consume, peek, and move, text and parsed JSON. Test stdout-only PTY failure with separate writable stderr for ERROR/retryable false assertions, both streams on the dead PTY, and absent stderr at launch. Normal CLI-owned wrappers close before sink allocation; their matching public/original references change, and forced GC/normal exit produce no late flush warning or exit120. In-process failing stand-in control preserves the healthy interpreter original and does not close the caller-owned stand-in. PG-target watch with either fd1 or fd2 absent at launch exits1 before target resolution/validation; no validation connection can take the missing standard descriptor, and installing a sink never lets watching continue. Verify default logging configuration for no-traceback assertions. After first failed handoff, prove exit1 before enqueueing 20 later messages; those stay pending/unmoved. Assert original exception cause without a self-cycle and guaranteed stop despite diagnostic/cleanup errors. A caller replacing/reusing raw descriptors mid-command is outside this guarantee. Retain generic callback-continue and finite-command broken-pipe tests. |
| T5 / R6 | 62/63-byte acceptance and 64 rejection with no resource acquisition for pure/constructor paths. Enumerate plugin/config/direct runner and waiter/structural-runner entrances, including public-schema rejection on formerly permissive direct/structural routes. Real PG: two distinct valid 63-byte schemas stay isolated and deliver notifications. A valid 63-byte schema with a message survives read/write/cleanup attempts through both colliding 64-byte inputs. Use a connection-creation spy only for the pre-I/O claim; storage/isolation/cleanup proof stays real. |
| T6 / R3 | Public BrokerSession aliases cached, finish/close all owners, separate-process CLI cleanup and recreation, fresh session resolves the new alias. Keep same-generation monotone-counter/seqlock and alias-update tests. Freshness after live replacement is outside the adopted contract and is not a passing assertion. |
| T7 / R7 | Dedicated disposable PG target with per-connection idle_session_timeout=10s. Observer-only hooks record actual listener generations and returned processing waits; quiet run reaches third failure with original OperationalError. Real relevant notifications, without queue rows, permit completed waits and repeated recovery. Assert causal failure counts and diagnostic classes, not exact 36s or 51s timing. Keep actual server/network/driver/retry delays; only an outer scaled deadline is a deadlock valve. Existing scripted first-processing-wait failure still terminates after 3 and healthy wait resets budget. No server-wide setting changes. |

For process probes use actual pipe/event phase witnesses, bounded child waits,
WIFEXITED checks, and finally-based reap/isolated target cleanup. Do not mock
fork, SQLite locks/WAL, Redis reservation scripts, PG truncation, or output
failures. Fault injection is restricted to module-owned connect/close/observer
seams and must delegate to real operations where the causal proof depends on
them. Keep delayed asynchronous listener failure after a trivial inner timeout
as an explicit counterexample for any future R7 redesign.

## Weft Compatibility and Release Order

The sibling checkout is a downstream verification target, not silently included
in this repository's write scope. Follow its own AGENTS guidance if a later
implementation task authorizes those edits. Required paths:

- `weft/client/_client.py` and
  `integrations/weft_django/weft_django/lifecycle.py`: retain inherited-handle
  detachment; allow the new refusal to surface, never reconnect around it.
- `docs/specifications/14-Python_API_Surfaces.md`,
  `13C-Using_Weft_With_Django.md`, `04-SimpleBroker_Integration.md` in Weft:
  narrow SQLite child recovery; preserve spawned runtime support.
- `tests/helpers/client_fork_probe.py`, `tests/core/test_client.py`,
  `integrations/weft_django/tests/test_lifecycle.py`, and
  `integrations/weft_django/tests/fixture_project/fixture_project/lifecycle_fork_probe.py`:
  split safe pre-fork
  close success from unsafe retained-session refusal. Reproduce the Django
  case before claiming it qualified. `weft/core/launcher.py` and
  `weft/core/runners/host.py` need no start-method change.

Before release, record downstream tests importing the candidate core path, not
only installed package metadata. Verify Weft safe submission, watcher and
session-lifetime paths with its pinned extensions. Coordinate guidance and pin
updates before claiming downstream compatibility. A missing downstream checkout
or unresolved compatibility failure is an explicit release blocker, not a reason
to remove the core safety guard.

## Verification and Gates

For this planning deliverable run `python3 bin/check-dom15-fixtures`,
`bin/check-plan-context`, `bin/check-doc-paths`, and `git diff --check`; inspect
the index, source references, and completed review dispositions. No runtime fix
is claimed by these documentation gates.

Implementation setup: reuse the already combined dependency environment;
otherwise sync once with `uv sync --extra dev --extra pg --extra redis` before
parallel lanes. Use `--frozen --no-sync` thereafter so lanes do not mutate each
other's environments. New test paths below are declared future artifacts.

```bash
uv run --frozen --no-sync pytest tests/test_sqlite_fork_admission.py tests/test_fork_safety.py tests/test_runner_lifecycle.py tests/test_runner_validation.py tests/test_validation_lock_safety.py
uv run --frozen --no-sync pytest tests/test_cli_main.py tests/test_cli_watch.py tests/test_commands_stdout_delivery.py tests/test_commands_error_ownership.py tests/test_cli_broken_pipe.py tests/test_watcher_transition_tables.py tests/test_watcher_error_handler_contract.py
uv run --frozen --no-sync ./bin/pytest-redis --fast --max-worker-restart=0 extensions/simplebroker_redis/tests/test_redis_generator_fork.py extensions/simplebroker_redis/tests/test_redis_batches.py extensions/simplebroker_redis/tests/test_redis_core_behaviors.py extensions/simplebroker_redis/tests/test_redis_activity_waiter_fork.py
uv run --frozen --no-sync ./bin/pytest-pg --fast --max-worker-restart=0 extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py extensions/simplebroker_pg/tests/test_pg_ownership.py extensions/simplebroker_pg/tests/test_pg_activity_waiter_lifecycle.py extensions/simplebroker_pg/tests/test_pg_notify.py extensions/simplebroker_pg/tests/test_pg_quiet_connections.py
uv run --frozen --no-sync pytest tests/test_watcher_edge_cases.py tests/test_python_library_api_contract_sb_api.py tests/test_cli_contract_sb_cli.py tests/test_operations_contract_sb_ops.py
```

Final ordinary gates: full `uv run --frozen --no-sync pytest`,
`uv run --frozen --no-sync ./bin/pytest-pg --max-worker-restart=0`,
`uv run --frozen --no-sync ./bin/pytest-redis --max-worker-restart=0`,
`uv run --frozen --no-sync ruff check .`,
`uv run --frozen --no-sync ruff format --check .`, the mypy commands below,
the documentation gates above, and
`uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`.
Verify actual CLI flags
and collected test selection before recording success. Record OS/Python/backend
versions and skips. Ordinary fork acceptance needs Linux and macOS, including
the Python 3.13 default-fork and 3.14 default-forkserver controls.

Type checking (same cohorts as `.github/workflows/test.yml`, using Bash and rg
to enumerate existing files; expected-failure type fixtures remain excluded):

```bash
uv run --frozen --no-sync mypy --config-file pyproject.toml simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
MYPYPATH=. uv run --frozen --no-sync mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs $(rg --files tests -g '*.py' -g '!tests/typecheck_fixtures/**' | sort)
uv run --frozen --no-sync mypy --config-file pyproject.toml extensions/simplebroker_pg/simplebroker_pg $(rg --files extensions/simplebroker_pg/tests -g '*.py' | sort)
uv run --frozen --no-sync mypy --config-file pyproject.toml extensions/simplebroker_redis/simplebroker_redis $(rg --files extensions/simplebroker_redis/tests -g '*.py' | sort)
```

Existing explicit topology gates are additional, preserving their CI policy:

```bash
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS="" uv run --frozen --no-sync pytest -n0 -q --timeout=180 --timeout-method=thread tests/test_cross_thread_generator_probe.py
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS="" uv run --frozen --no-sync ./bin/pytest-pg --fast -n0 -q extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS="" uv run --frozen --no-sync ./bin/pytest-redis --fast -n0 -q extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py
SIMPLEBROKER_RUN_AUTOVACUUM_PROBE=1 PYTEST_ADDOPTS="" uv run --frozen --no-sync ./bin/pytest-pg --fast -n0 -q extensions/simplebroker_pg/tests/test_connection_stats.py extensions/simplebroker_pg/tests/test_connection_usage.py
```

These existing probes cover driver/resource ownership and PG connection-creation
attribution. Do not hide new fork tests behind their opt-in switches. Reconcile
the current workflow opt-ins again at final integration, rather than trusting
this list after intervening CI changes.

## Rollback, Rollout, and Success Signals

- No data-format migration or backend API handshake change is intended. If a
  private cross-package signature must change, reassess the handshake before
  implementation instead of quietly changing a shared seam.
- Ship safety code and its boundary documentation together. Surface the SQLite
  refusal, PG schema-length rejection, and newly enforced public-schema
  rejection on direct/structural entrances as compatibility changes in CHANGELOG.
  Explicitly name the alias regression since 8.8.1 and its maintenance workaround;
  D6 narrows support and does not fix retained-client refresh across replacement.
  Existing deployments must use safe process startup or explicitly close all
  SQLite owners; existing overlong schemas need operator inspection/config repair.
  Do not auto-split, rename, delete, or claim to recover already intermingled data.
- R3 requires a maintenance procedure, not a live rollout: stop target users,
  finish and close owners, back up if needed, cleanup, recreate, create fresh
  owners. R7 recommends a target/role-scoped timeout policy that covers LISTEN
  sessions, not server-wide timeout changes or removal of command-pool idle reaping.
- Roll back each coherent code slice with its spec/docs if necessary, retaining
  operational mitigations. Reverting R1/R6 restores known silent-loss/isolation
  hazards, so it is not a safe unattended fallback; disable the affected usage
  or deploy a forward correction. Never use cleanup as a rollback mechanism.
- Success: unsafe managed fork produces a diagnostic before child writes;
  safe start modes retain acknowledged data; child Redis exit cannot release
  parent reservations; invalid PG names fail before I/O; ordinary startup does
  not emit false corruption; watch stops on dead output and leaves later work;
  fresh-owner cleanup works; quiet timeout exhaustion is explicit and documented.
  Use release smoke probes and existing diagnostics, not new telemetry services.

## Alternatives and No-Action Register

| Candidate | Disposition and reconsideration trigger |
|-----------|----------------------------------------|
| Automatically close SQLite connections before fork | Rejected for this plan: requires quiescing threads/transactions and safe callback ordering. Reconsider only with a concrete application requirement and a separately proved quiescence API. |
| Close inherited SQLite in the child | Rejected: SQLite cleanup itself can affect shared WAL state. No retry or PID reset makes it safe. |
| Detect only the same filename/inode | Deferred: URI, alias, attach, unlink/recreation and in-flight opens complicate identity. Conservative process-wide refusal is an explicit proposed compatibility cost. Reconsider if a demonstrated downstream need justifies a narrower proven boundary. |
| Docs-only SQLite restriction | Insufficient for detectable managed exposure while errors teach unsafe reconstruction; retain documentation for undetectable raw/external state. |
| Live PG schema replacement through an epoch, seeded initial counter, or direct alias reads | Deferred: seeding can cheaply reduce alias ABA risk, but a genuinely non-repeating seed needs an identity/collision policy and does not repair other retained schema/admission state. No full live-replacement requirement established. Reopens if retained-client replacement is explicitly required. Name the existing regression; do not revert to wall-clock version updates or imply the alternative requires a large framework. |
| Silently truncate overlong schema names | Rejected: conceals collisions and requires identity/lock/channel reconciliation; reject before I/O. |
| Infinite watcher reconnect or restart/drain resets | Rejected under the existing owner-selected bounded-failure policy. |
| Count one completed inner native wait as health | New deferred alternative, not falsely attributed to [ALT-IMPL06-002]. A 10-microsecond local timeout can precede asynchronous failure and permit endless flapping. Reconsider through an explicit recovery-policy revision with that counterexample. |
| Active listener pings or shorter PG safety rechecks | Preserve [ALT-IMPL06-001/002] and current connection budget; a new deployment requirement and owner decision are needed. |
| Change Weft's runtime spawn sites | No action: they already avoid this failure. Embedding/client/Django promises and tests still require coordination. |
| Broader performance or backend audit | Out of scope. Use a focused measurement only if connection/open/close bookkeeping shows a regression; no benchmark-matrix claim is made here. |

## Interface Design Review

Scope: proposed CLI errors, schema rejection, fork guidance and support docs at
the baseline above. This is a design walk, not a claim that the implementation
already satisfies the proposals. Final interface review repeats it against code.

| Principle | Design disposition and evidence target |
|-----------|-----------------------------------------|
| 1 Context economy | Met by design: short error and next action; D1/D3/D4, existing `commands.py::_emit_error` envelope. |
| 2 Progressive disclosure | Met by design: authoritative clauses plus kernel/guide pointers; Tasks 0/5/6. |
| 3 Self-explanatory names | Met by design: no new public knobs/types; RuntimeError/DatabaseError and existing ERROR. |
| 4 One identity | Met by design: reject schema aliases introduced by truncation; T5. |
| 5 Derive what is derivable | Met by design: PID/hazard and byte-length checks are library-owned; no caller hazard flag. |
| 6 No hidden setup | Existing retained sessions remain an explicit [SB-API-3] API; D6 names their lifetime. No new setup handshake. |
| 7 Teach, don't reject | Unsafe-input exception applies: truncation/fork use cannot be safely canonicalized. D1/D3 errors must supply the safe next action. |
| 8 Actionable messages | Met by design: new process or pre-fork closure; choose a valid schema; inspect delivery state before retry. T4/T5 assert guidance meaning without whole-string coupling. |
| 9 Atomicity/recovery | Existing claim-before-output boundary remains; D4 does not imply rollback. Concurrent merge is N/A for these CLI error paths. |
| 10 Trust boundary | D1 detection limits, D6 caller-owned quiescence, D7 server policy are explicit. |
| 11 User-shaped wire format | Met by design: keep current JSON error envelope and code set; no driver-internal state in output. |

Findings from author walk: IF1 (current CLI error continuation, `commands.py:1659`)
is addressed by Task 3/T4; IF2 (unsafe reconstruction advice, `db.py:1004`) by
Task 1/T1; IF3 (unbounded schema spelling, PG `validation.py:62`) by Task 4/T5.
Ratified judgments: none newly ratified by this document; proposed boundaries
await plan adoption. Verdict: implementation claims remain gated by the tests;
independent plan/delta verdict is recorded below. Runbook feedback: no new rule
proposed; existing identity, actionable-error, and ownership guidance fits.

## Independent Review Loop

- Author family: OpenAI Codex.
- Reviewer family: Anthropic Claude.
- Review invocation: `skills/call-agent/SKILL.md`; Claude 2.1.293 with
  `--safe-mode --permission-mode plan --tools "Read,Grep,Glob" --allowedTools "Read,Grep,Glob" --strict-mcp-config --mcp-config '{"mcpServers":{}}' --output-format stream-json --verbose`, stdin closed, 900-second process timeout, stdout/stderr retained outside the repository.
- Handoff, 2026-11-11: the OpenAI Codex session ended before the SQLite slice
  was integrated. Anthropic Claude finished that slice, integrated all slices
  and ran the combined gates. At the owner's direction, reviews of work
  finished in that phase use Google Antigravity: `agy` 1.5.5 with
  `--mode plan --sandbox --model gemini-4-argon-medium --output-format json --print-timeout 900s`,
  stdin closed, 990-second process timeout, native reads only. Containment was
  re-probed on 1.5.5 before use: a native read succeeded, and a requested
  repository write left the sentinel absent and the working-tree status
  unchanged (plan mode wrote its note under the CLI's own state directory).

Review the entire plan and D1–D7 against the baseline code, named test owners,
program theory, and standing alternatives. Verify paths/seams and causal premises
first. Evaluate implementability, safety, craft, and known CS principles; prefer
removing unnecessary machinery. The scope is these seven reports and necessary
shared seams. Other pre-existing defects are non-actionable observations unless
the plan worsens them. No new owner risks are represented as already accepted;
the documented unsafe-use cases are proposed limits, not hidden omissions.
Findings carry IDs, [P1]/[P2] severity where applicable, evidence, and suggested
disposition. Expansion requests are labeled for owner decision. Anything that
overturns a standing constraint is RECONSIDERATION REQUESTED. PASS/BLOCKED must
derive from confident implementability and whether implementation degrades
robustness. Author dispositions must appear in both task and spec text before
requesting scoped verification of accepted findings. Repeat review after each
coherent risky implementation slice and before combined completion.

### Review Log

| Round | Command/bound and outcome | Findings and author disposition |
|-------|---------------------------|---------------------------------|
| Plan1 | Claude 2.1.293, Anthropic; restricted Read/Grep/Glob, no MCP; exit 0 after 458.6s within 900s. Init inventory and successful native Read verified. Original verdict BLOCKED on F1. | Verbatim review in the appendix. F1–F9 dispositions below; source assertions checked, additional lifecycle/stdio probes run outside the repository. |
| Plan2 | Claude 2.1.293, same restricted tool inventory; exit 0 after 235.9s; success, no permission denials. Verdict PASS with nonblocking Task 3 amendments and a failed-open test note. | F1–F9 accepted amendments verified; optional F7 seed not re-polled. F4.1, F4.2, F1.1 accepted below. |
| Plan3 | Claude 2.1.293, same restricted inventory; exit 0 after 57.1s; success, no permission denials. Verdict PASS. | F4.1/F4.2/F1.1 verified consistently across delta, tasks and tests. Two optional P3 notes dispositioned below; no remaining plan-review blocker. |
| R5 | Claude 2.1.293; exit 0 after 151.6s. Verdict PASS. | Recorded in the Execution Log; verbatim below. |
| R2/R4 | Claude 2.1.293, restricted Read/Grep/Glob, no MCP, plan permissions; exit 0 after 271.7s; no permission denials. Verdict PASS, four optional P3 notes. | Dispositions in the Execution Log; verbatim below. |
| R6, R3/R7 | Claude 2.1.293, same inventory; exit 0 after 242.2s; no permission denials. Verdict PASS with F1 [P2] to decide. | F1, F2, F3, F5 accepted and applied; F4 not adopted. Execution Log; verbatim below. |
| R1 | agy 1.5.5 / gemini-4-argon-medium; exit 0, status SUCCESS, no denied actions, 133.5s. Verdict PASS WITH FINDINGS. | S1, S2 accepted and applied; S3 decided by the owner: keep the witness. Execution Log; verbatim below. |
| Final | agy 1.5.5 / gemini-4-argon-medium combined completed-work review; exit 0, status SUCCESS, no denied actions, 421.4s; working-tree status unchanged across the run. Verdict PASS WITH FINDINGS (two P3). | C1 and C2 accepted and applied. Execution Log; verbatim below. |

### Plan1 dispositions

| ID | Evidence and disposition | Body owner |
|----|--------------------------|------------|
| F1 | Accepted option (a): tracked real-connection factory, creator GC close before retirement, child-only resurrection/abandonment. Explicit-close-only tracking would leak false refusals; conservative permanent leakage is rejected. Shutdown remains excluded. | D1; Task 1; T1. |
| F2 | Accepted simplification: one active-lifetime count with once-only accounting, no four-state token data or duplicate snapshot; no promised post-connect repair for unsupported reentrant fork. | Task 1; T1. |
| F3 | Accepted: enumerate guarded runner entries and cleanup/marker exclusions; guard after PID recovery. Remove “non-retryable” classification claim and disclose current bounded watcher retries. | D1; Task 1. |
| F4 | Accepted gap, amended mechanism: real PTY probe proves close-before-replace is needed for residual buffers; replace both original/public references and retain the sink. A distinct carrier avoids self-causation. Deliberately narrow the guarantee to normal CLI-owned streams and launch-time absence/closure; external runtime descriptor reuse cannot be repaired after payload redirection. | D4; Task 3; T4. |
| F5 | Accepted: unavailable stderr means exit 1 without diagnostic; parsed-watch emission and cleanup cannot override that status. Separate stderr in the JSON-diagnostic PTY test; cover joint PTY failure separately. | D4; Task 3; T4. |
| F6 | Accepted: public exclusion on direct/structural entrances is an explicit compatibility change with tests/release notes. Validate public entrances; private registry/listener helpers rely on validated input. | D3; difference register; Task 4; T5; rollout. |
| F7 | Accepted required truthfulness: name the 8.8.1 regression and changed support boundary in CHANGELOG. Optional seeded-counter implementation deferred: it addresses alias ABA probability/identity, not the other retained target state. Reopens if the owner requires live replacement; no retained task depends on it. | R3; D6; Task 5; rollout; no-action register. |
| F8 | Accepted: existing DSN/libpq option or broker-role policy covers LISTEN and command connections; no separate listener configuration. Remove “By default” from fixed retry count. | D7; Task 5. |
| F9 | Accepted: explicit start-method contexts plus separate default smoke and unsafe forkserver-preload control; exact D5 paragraph anchor; caller-created zero-byte semantics named and tested. | D5; T1; T3. |

### Plan2 dispositions

| ID | Evidence and disposition | Body owner |
|----|--------------------------|------------|
| F4.1 | Accepted: cli._main validates targets before cmd_watch, so move the parsed-watch stream guard before _resolve_target and both validation calls; keep the command-level direct-call backstop. PG launch-closed control proves no early validation open can consume fd1. | Task 3; T4. |
| F4.2 | Accepted: only close/replace both references when the failed wrapper is also the interpreter original. Preserve caller-owned stand-ins and healthy __stdout__/__stderr__; test in-process ownership. | Hidden couplings; Task 3; T4. |
| F1.1 | Accepted and independently encountered by the prototype: ordinary failed-open followed by safe fork is a required T1 control. Verified native CANTOPEN cleanup has an explicit retirement path; ambiguous failures remain conservative. | Task 1; T1; execution evidence. |

### Plan3 notes

| ID | Disposition | Body owner |
|----|-------------|------------|
| F4.1-a | Clarified the existing startup-refusal intent: either missing/closed stdout or stderr causes exit 1 before any target open. The review note's hypothetical “stderr is missing and watch goes ahead” is not proposed. T4 now explicitly checks PG fd2 as well as fd1 refusal with zero validation opens; a sink never permits a rejected watch to continue. | D4; Task 3; T4. |
| F1.1-a | Accepted evidence limit: prototypes qualify only the versions actually run. The implementation must run lifecycle/admission tests across the repository's supported CI Python matrix (currently 3.11–3.15); the 3.13/3.14 multiprocessing-default reproduction is an additional topology check. | T1; execution log; final integration. |

The reviewer's severity labels are recorded verbatim, not treated as mandates
for expansion. No standing alternative is overturned. At the planning review these support limits awaited owner adoption; the
implementation request subsequently adopted D1–D7 as recorded below.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| D3 / SB-API-2 | Backend schema validation raises DatabaseError | Existing Queue acquisition wraps failures in RuntimeError with the backend error as cause | Preserve the existing public acquisition boundary rather than change unrelated error policy; real rejection test asserts the cause | Clarified D3 and promoted SB-API-2 wording; no runtime deviation from existing wrapping |
| Task 1 / SB-API-11 | A failed open retires its count on the verified native SQLITE_CANTOPEN path; other failed opens are qualified separately | A failed open retires only when the constructing thread saw the failed object finalized and a weak reference taken in that finalizer is dead | A callback on the driver's connect audit event can raise a nested cannot-open error while retaining the live outer connection, so an error code does not prove that no handle survived. The destruction witness covers every ordinary native open failure with one rule. Audit-hook reentrancy stays outside the D1 guarantee; the rule is conservative there and promises nothing new | None: [SB-API-11] does not name the mechanism. `docs/implementation/06-process-session-core-ownership.md` describes the revised rule |

D1–D7 were adopted by the implementation request and promoted. The register
above records implementation-time wording clarifications against that baseline.

## Execution Log and Fresh-Eyes Review

### R5 implementation and review, 2026-11-11

The Redis generator captures its creator PID on first execution, rejects child
resumption after every yield, and delegates finally settlement to one helper
whose first action is the immutable PID check. Same-process foreign-thread
finalization is unchanged. The independent review accepted this slice. Removed
its redundant loop-top checks: the after-yield check already precedes every
subsequent batch under the supported non-reentrant fork contract. Added four
public Queue normal-exit controls after the review's coverage suggestion.

The final 22 real-process cases all failed against baseline 75616556 in the
isolated control checkout (16.97 s), then the focused cohort passed 100 tests
(3.41 s). Full Redis wrapper passed 1,826 shared tests plus 452 extension tests;
19 shared platform/backend/opt-in skips and one extension opt-in skip were
expected. The explicit cross-thread finalization probe passed separately.
Source plus test mypy passed. Environment: macOS 27.0.1 arm64, CPython 3.14.9,
redis 8.1.0, real Valkey 7.2. Commands used UV_NO_SYNC=1 and explicit source paths
in addition to --frozen --no-sync because backend wrappers invoke uv internally.
Evidence: /tmp/sb-implementation-20261111/redis-results.md and its named logs.
Final combined qualification and review remain separate gates.

Review R5: Claude 2.1.293 / claude-opus-5-5[1m], 151.6 s, exit 0; emitted native
Read/Grep/Glob calls with plan permissions, no MCP servers, success result and
no permission denials. Verdict PASS. R5-1 and R5-2 accepted as above. R5-3 is a
pre-existing timestamp-helper recovery gap, outside the changed reservation
contract. R5-4 is optional; distinct raise/no-op policies and the small shared
settlement helper remain. Full findings are preserved below.

### Implementation entry, 2026-11-11

User request “Please implement per plan” adopts D1–D7 and authorizes the planned
implementation. Promoted the exact reviewed clauses into specs 10/16/17 against
baseline 75616556e22cdc77490c0fb08590210ad3a2402b; no implementation mapping is
claimed for code not yet written. Spec promotion uses an uncommitted diff over
that baseline until owner-authorized commit; the working tree is the evidence.

Comprehension answers: (1) Fresh wrappers inherit SQLite's C lock bookkeeping
without the parent's OS locks; child close can touch shared WAL state and is
not repair. Track physical lifetimes and refuse unsafe managed admission.
(2) A suspended Redis reservation belongs to its generator's original process;
mutable core recovery cannot transfer that authority. (3) Only a normal outer
processing wait proves recovery; startup/drain resets permit endless first-wait
flapping, and shorter rechecks undo the accepted quiet-connection budget.


Planning evidence: the baseline reproduction results are recorded in R1–R7;
source tree and historical identifiers were inspected by command. On 2026-11-11,
python3 bin/check-dom15-fixtures, bin/check-plan-context, bin/check-doc-paths,
git diff --check, and the explicit new-file whitespace check found no issues after
review amendments. The new-file git diff --no-index --check /dev/null check
returns 1 for the content difference with no whitespace diagnostics; this is
not a runtime-test failure. The context checker resolves one in-flight plan. R1–R7,
D1–D7 and T1–T7 each have one owning matrix/clause row. Current key paths exist;
three future paths are explicitly labeled new. Weft paths resolve in its sibling
checkout. At the planning handoff only this plan and its Status Index row had changed;
implementation had not started. The implementation records below separately track
comprehension answers, RED/GREEN commands, promotion identifiers, and per-slice
verification. A plan-review PASS is not approval of new support boundaries.

Additional plan-review feasibility evidence (temporary prototypes, not runtime
implementation): the author reran all eight tracked-connection modes on CPython
3.14.9. Explicit/repeated close, ordinary/cyclic/foreign-thread GC, child-only
resurrection, failed-close retry, and fork during GC close behaved as required.
The deliberately incomplete constructor path retained count 1 after CANTOPEN;
Task 1 now explicitly separates it. A corrected CANTOPEN prototype returned
count 0 on 3.11.15, 3.13.13, and 3.14.9, matching the linked native error cleanup.
The exploratory agent also ran the seven normal lifetime modes on 3.11.15 and
cyclic GC/child resurrection on 3.13.13. These probes qualify the design only;
production tests and supported-runtime integration remain required. Run T1
across the CI Python matrix (currently 3.11–3.15); the prototypes do not claim
qualification of 3.12 or 3.15.

The author reran the real PTY residual-buffer stdout/stderr probe on 3.14.9:
close reported EIO but left the wrapper closed, sink replacement followed,
normal exit was 1 with no finalizer warning. Before-exec fd1/fd2 closure yielded
None for both corresponding stream references and a clean exit 1. The external
raw-descriptor reuse counterexample remains deliberately outside D4.

Fresh-eyes check before handoff: preserve one ownership gate, one schema
validator, existing setup/terminal-error paths, and the existing watcher policy.
Do not add layers merely to satisfy this document. Recheck proposed paths marked
new versus existing paths, spec links, tests, slice dependencies, diagnostic
precedence, and the supported/unsupported distinction after review amendments.

### R2/R4 CLI implementation and review, 2026-11-11

The legacy preflight defers only a regular zero-byte file to coordinated setup
for read, peek, exists, move, list, stats and root `--vacuum`; nonempty files
and other path types keep the shared validation, and init/cleanup are unchanged.
A private carrier takes a fatal output error through the watcher's existing
terminal path; `cmd_watch` re-raises the original error after cleanup and
always stops the watcher. Missing or closed stdout/stderr is refused first in
`_main`, before selectors and target resolution. A failed CLI-owned wrapper is
closed before its sink is opened.

RED on the baseline sources: 29 failed and 36 passed for the first-use,
watch-output and transition selections, then 4 added cleanup/banner cases and
4 malformed-selector ordering cases failed before their repairs. GREEN: the
16-module focused cohort passed 449 tests with 3 Windows skips; focused runs
also passed on macOS CPython 3.11, 3.12, 3.13, 3.15 and Linux 3.13, 3.14.

Review R2/R4 verdict PASS with four optional notes. P3-3 accepted: the
`SM-CLI-WATCH` row now names `CLI_WATCH_TRANSITIONS` in
`tests/test_watcher_transition_tables.py`. P3-4 accepted: the first-use test
asserts a nonempty file in the header and commit phases. P3-1 not adopted:
narrowing the stderr handler to `OSError` would turn an unencodable diagnostic
on a healthy stream into a traceback instead of the specified exit 1, and no
supported sequence reaches the hypothetical classifier bug. P3-2 not adopted:
a wrapper that stays open after `close()` exists only on runtimes D4 excludes,
and failing loudly there is the intended result.

### R6, R3/R7 PostgreSQL implementation and review, 2026-11-11

One pure validator applies the existing grammar, rejects `public` and caps
names at 63 bytes. It runs before any resource is created in
`require_schema_name`, `PostgresRunner`, both exported waiter constructors and
`PostgresBackendPlugin.initialize_database`. R3 and R7 changed documentation
only; their tests prove the supported path (fresh owners after cleanup and
re-creation; bounded quiet-watcher failure and recovery under a real
per-connection 10-second `idle_session_timeout`).

Review verdict PASS with F1 [P2]. F1 accepted: a caller-supplied runner passed
to `BrokerCore` with the PostgreSQL plugin reached schema creation unvalidated;
`initialize_database` now validates it, and the `structural_core` rows failed
first (2 failed, assertion `invalid schema reached backend resource
acquisition`). F2 accepted: the duplicate plugin waiter checks were removed.
F3 accepted: publisher and watcher are stopped and joined before counts are
compared. F5 accepted: per-parameter message matching; the 0.1-second negative
wait is labeled a smoke check. F4 not adopted: this libpq reports the expiry as
a generic server-closed-connection error, so pinning idle-timeout text had
already failed valid cases, and a server-log assertion would couple the test
to the harness.

### R1 SQLite implementation, handoff and review, 2026-11-11

`simplebroker/_sqlite_fork.py` owns one count of managed physical lifetimes and
the sticky child refusal. All four production open sites use its factory. The
runner guards its eight admission entries after PID recovery and before any
resource lock; `DBConnection.get_connection` checks first so the general
three-attempt connection retry is not spent on a deterministic refusal.
SQLite-specific diagnostics no longer recommend constructing a new Queue.

RED on the baseline: the real-fork regression reported 20 acknowledged child
writes and 1 surviving.

The OpenAI Codex session ended while this slice was being revised and before
it was integrated. Anthropic Claude completed it:

- Failed-open accounting follows the Deviation Log row above.
- A `PYI034` return annotation on `_ManagedConnection.__new__` was corrected.
- Integration exposed four failures that the focused lane had not run. A test
  in `tests/test_sqlite_admission.py` passed `factory=` through a patched
  `sqlite3.connect`; its racing class now subclasses the managed connection.
  The plan-context fixture in `tests/test_doc_gates.py` copied the live index
  and assumed no real plan was in flight; it now neutralizes rows it is not
  testing. The tests type-check cohort reported 12 errors in the new test
  module; they are annotation fixes. One probe asserted that the
  construction-time weak reference to a resurrected object is dead, which
  holds on CPython 3.11–3.14 and not 3.15; the probe now asserts only what the
  contract needs, that the reference taken in the finalizer observes the
  resurrection. Production code did not change for any of the four.

Qualification after those changes, new suite `tests/test_sqlite_fork_admission.py`
(43 cases, no skips): macOS arm64 CPython 3.11.15, 3.12.13, 3.13.13, 3.14.9 and
3.15.2. Linux aarch64 (glibc 2.41, SQLite 3.46.1) CPython 3.11.18, 3.12.16 and
3.13.18, whose default start method is fork, and 3.14.10, whose default is
forkserver, each running that suite with `tests/test_fork_safety.py`,
`tests/test_core_persistence_transition_tables.py` and
`tests/test_sqlite_admission.py`: all exit 0. Linux images mounted the working
tree read-only and recorded the imported core path inside the container.

Review R1 (agy) verdict PASS WITH FINDINGS. It found no false admission or
ordinary false refusal, and confirmed the hook takes no inherited lock and that
each guarded entry refuses before a resource lock. S1 [P2] accepted: the
rationale in implementation 06 described the superseded error-code rule, this
plan lacked the deviation row, and the [SB-API-11] section mapping omitted the
new owner; all three are corrected. S2 [P3] accepted: the `FORK_RESET` and
`FORK_ACTIVE_RESET` rows of the runner table now describe reset followed by
refusal. S3 [P3] is an owner decision and is open: the construction witness
(about 30 production lines and three tests) exists only to stay conservative
when an audit callback retains or resurrects a connection whose open then
fails, which D1 already excludes. A flag set by the factory after a successful
open would retire ordinary failed opens with less machinery and would not be
conservative for that excluded case.

Owner decision on S3, 2026-11-11: keep the witness. Measured cost on CPython
3.14.9 for one open and close of a file database: raw driver 33.5 microseconds,
managed connection without the witness 35.2, with it 36.4; nothing per
statement. Its failure direction is a refused fork after a failed open, not
lost data, and the `clean:cantopen` transition row detects that on any
interpreter in the matrix. The accepted cost is maintenance: the three
construction probes follow CPython finalizer and weak-reference behavior and
may need adjusting on a new interpreter, as one did for 3.15. Revisit only if
a supported interpreter breaks the rule.

### Combined qualification, 2026-11-11

Environment: macOS 27.0.1 arm64, CPython 3.14.9; PostgreSQL and Valkey in
wrapper-managed containers. The shared environment's editable installs were
re-pointed from a lane worktree to this checkout before these runs, and the
imported paths were verified.

| Gate | Result |
|------|--------|
| `uv run --frozen --no-sync pytest` (default options) | 4288 passed, 18 skipped (Windows-only cases, opt-in probes, cross-backend pipes that need both servers, artifact expectations) |
| Same suite with every marker selected (adds `benchmark` and `nested_xdist`) | 4309 passed, 18 skipped |
| `./bin/pytest-pg --max-worker-restart=0` | shared 1832 passed, 13 skipped; extension 532 passed, 7 skipped (opt-in probes) |
| `./bin/pytest-redis --max-worker-restart=0` | shared 1826 passed, 19 skipped; extension 452 passed, 1 skipped (opt-in probe) |
| Cross-thread finalization probes, opt-in | SQLite 1 passed; PostgreSQL 5 passed; Redis 1 passed |
| PostgreSQL connection stats and usage probes, opt-in | 42 passed |
| `ruff check .`, `ruff format --check .`, `bin/ruff_suppression_index.py --check` | pass (464 files formatted) |
| mypy, four CI cohorts | pass (69, 223, 49 and 33 source files) |
| `bin/check-dom15-fixtures`, `bin/check-plan-context`, `bin/check-doc-paths`, `git diff --check` | pass |

Not run here: Windows, free-threaded builds, and Linux for the PostgreSQL and
Redis suites (CI owns those).

### Weft companion, 2026-11-11

The companion plan `2026-11-11-simplebroker-sqlite-fork-compatibility-plan.md`
is a draft in an isolated Weft checkout at `00d929e9`; the owner's Weft
checkout is untouched. Its first review (Claude) requested changes. The
revision replaces the invalid PostgreSQL wrapper command with direct node-id
runs, records import provenance inside the probes, states the merge rule (one
landing change carries the spec delta, the tests and the dependency floor and
lock bump; nothing is version-gated), pins the held-lock proof on the refusal
arm, fixes the refusal boundary at the first submission, and restricts the
safe arm to public close with a fresh-observer durability count.
Review 2 (agy) returned PASS WITH FINDINGS and confirmed every finding of
review 1 resolved; its three should-fix notes (where the completion gate reads
Theory Fit, the password the direct PostgreSQL command needs, and a provenance
assertion that must also hold for an installed release) and one optional
wording note are applied.

Downstream smoke, candidate core on `PYTHONPATH`, import paths verified inside
the run, Weft sources unmodified: Weft's default suite reported 6986 passed,
51 skipped and 1 failed; the focused client, submission-facade, multi-queue
watcher, Django lifecycle and spawn-transport modules reported 232 passed,
2 skipped and 2 failed. The failures are exactly the two real-fork tests that
assert the behavior D1 withdraws
(`tests/core/test_client.py::test_client_fork_reset_uses_child_bounded_state`
and the Django held-lock fork test), each with the propagated
`SQLite access is unsafe after fork` error. Nothing else in Weft changed
result, and Weft neither catches nor retries around the refusal. This is a
gate smoke through a source overlay, not released-package qualification.

### Final combined review, 2026-11-11

The combined review (agy) found no cross-slice contradiction, no enumerable
contract element without a firing test, and no user-visible change missing
from the changelog. Verdict PASS WITH FINDINGS. C1 [P3] accepted: the
[SB-API-2], [SB-API-5] and [SB-OPS-7] implementation mappings now name the
extension modules that own the behavior. C2 [P3] accepted: the `SQLRunner`
protocol docstring no longer says a runner always recreates connections after
fork. On S3 the reviewer again called both keeping and simplifying safe.

Reader caution: that review's descriptive passages name some identifiers that
do not exist in the tree (for example a `_ConstructionAttempt` class, a
`_LIVE_CONNECTIONS` counter, two Weft test paths, and `ValueError` for schema
rejection, which is `DatabaseError`). Its two findings were checked against
the files and are real. Its no-finding conclusions rest on a static read whose
summary is imprecise in those places, so the executed gates above, not that
narrative, are the evidence for correctness.

After applying C1 and C2 the documentation gates, lint, the core mypy cohort
and the default suite were rerun: all pass, 4288 passed and 18 skipped.

### Open at this record

- The owner authorized committing this change and merging it to `main` on
  2026-11-11; `git log` is the record. Publishing a release is a separate
  owner step.
- Release numbering, owner decision 2026-11-11: 8.8.2 / 4.8.2 / 4.8.2 were
  prepared on `main` but never tagged or published (the package index and the
  remote tags end at 8.8.1 / 4.8.1), so no version or floor changes. This
  plan's changelog entries are folded into the existing `[8.8.2]` section,
  and the alias-version counter is described there as introduced in that
  release.
- Task 6 stays open: the Weft companion plan needs owner adoption and can land
  only with a released SimpleBroker that contains D1, because Weft resolves
  SimpleBroker from the package index. Until then downstream compatibility is
  not claimed, and the Status Index row stays active.
## Out of Scope

No runtime implementation in the planning turn; no release, deployment, commit,
schema migration, data repair, automatic pre-fork closure, raw SQLite discovery,
general target-replacement framework, new backend, heartbeat, changed retry
budget, coalescing sweep, or broad process-guidance rewrite. Downstream write
authorization and release decisions remain explicit.

## Appendix: Plan1 review, verbatim

<details>
<summary>Anthropic Claude findings and original verdict before amendments</summary>

# Independent review: runtime boundaries and failure handling plan (baseline 75616556)

Every existing path, entry point and gate the plan names exists, including `bin/*` and the contract test files. The three paths marked **new** are absent, as labeled. Causal claims I checked in source:

- **R1:** the four managed open sites are `_runner.py:356`, `validation.py:54`, `runtime.py:35` and `runtime.py:107`. There are no others in `simplebroker/`.
- **R2:** a zero-byte file is rejected at `validation.py:43–46`, reached from `cli.py:1711–1723`.
- **R4:** `cmd_watch` passes no error handler (`commands.py:1651–1666`), so the default handler continues. The raw flush in the `finally` at `commands.py:1671–1679` can raise before `watcher.stop()`.
- **R5:** the `finally` blocks at `core.py:1102–1108` and `core.py:1360–1366` call the lock-taking `_exit_generator_batch` (`core.py:347`) and then roll back.
- **R6:** `PostgresRunner.__init__` (`runner.py:976–988`) builds a pool with no schema validation. The waiter paths take `runner.schema` without checking it (`plugin.py:1002–1008`, `1039–1045`).
- **R7:** the stated numbers match `watcher.py:954` and `990–1041` and `_constants.py:212` (3 failures, 2 s/4 s delays, 300 s budget).

## Findings

**F1 [P1] SQLite: a connection freed by garbage collection never retires its token, so later forks are refused for the life of the process.**
The plan retires a token "only after confirmed successful close," and says tokens hold no strong reference to the connection. `sqlite3.Connection` doesn't support weak references (`_runner.py:247`), so nothing can observe a GC-driven close. The main case is a caller-owned injected `SQLiteRunner` that is dropped without `close()`. It has no finalizer, so its connections are closed by deallocation and never pass through the wrapper. Every later fork from that parent then refuses SQLite, even though the fork is actually safe.

In pytest/xdist workers this also leaks state between tests. Existing fork tests that close before forking and expect the child to succeed can start failing depending on test order. T1's exec-isolated parent protects only the new tests.

**Suggested disposition (choose one and write it into Task 1 and D1):**
- (a) Open managed connections through a minimal `sqlite3.Connection` subclass passed as `factory=`. It is still a real sqlite3 connection, it gains `__weakref__`, and it can define a `__del__` that runs closing → `close()` → retire. That `__del__` must check the creator PID and must not close in a child. Retiring after close avoids the window you'd get from a weakref callback.
- (b) Accept the conservative leak and document it: "a SQLite connection reclaimed without `close()` leaves the process fork-tainted." Then move every existing fork test that expects child success into exec-isolated parents.

I recommend (a). Either is implementable, but the plan must choose one.

**F2 [P2] SQLite: the four-state token machine is more than the child needs.**
The child only ORs in "were any tokens published?" One live-count (or a multiset of ids, if diagnostics need them) plus the sticky flag gives the same safety. Under the GIL, an atomic reference swap or counter needs no separate snapshot. Free-threading is already out of scope.
- Separately, the post-connect PID check can fire only after a same-thread reentrant fork, such as from a signal handler between bytecodes. The plan declares that case unsupported.

**Suggested disposition:** drop the opening/live/closing distinction unless a test requires it. Either remove the post-connect check or describe it honestly as best-effort.

**F3 [P2] SQLite: the guarded entry points aren't listed, and the refusal is retried despite being "non-retryable."**
- "SQLiteRunner operation entries" doesn't say which ones. `close()`, `is_setup_complete()` and `release_thread_connection` must not raise. `is_setup_complete()` is called inside `_recover_after_fork_if_needed` (`_runner.py:326`, after `_pid` is updated). A guard there would break recovery partway through.
- D1 calls the refusal non-retryable, but `_run_with_retries` (`watcher.py:1027`) catches every `Exception`. A watcher in a tainted child will retry for about 6 s and then fail.

**Suggested disposition:** list the guarded entries: `get_connection`, `run`, `begin_immediate`, `commit`, `rollback`, setup, and the three non-runner open sites. Run the guard after the existing PID recovery. Either drop "non-retryable" from D1 or state that built-in watchers apply their bounded retries before failing. Watcher code stays read-only.

**F4 [P2] CLI stdout: the safe way to neutralize a failed stdout is left open, and D4/T4 may promise more than can be delivered.**
The plan rules out `dup2` onto a stale descriptor but gives no replacement mechanism. After a non-EPIPE failure, the old `TextIOWrapper` may still hold unwritten bytes and stays referenced by `sys.__stdout__`. When it is deallocated at shutdown, it retries the flush. That prints "Exception ignored…", and if fd 1 has been reused by a PostgreSQL socket, it writes into that socket.

**Suggested disposition:**
- Specify the mechanism: `os.open(devnull)`. If that returns the stdout fd, keep it as the sink and don't close it; this is the fd-equals-devnull edge. Otherwise swap the wrapper only.
- Either prove the residual-buffer case in T4 or narrow D4 to "no exit 120 and no traceback."
- The CLI error handler should not re-raise the same exception object. Through `_ErrorHandlerFailure` that becomes `raise e from e` (`watcher.py:1215`). Wrap it in a private carrier instead.

**F5 [P2] CLI: a dead stderr is not covered.**
With a hung-up pty, stderr is usually the same terminal. `_emit_error` then raises from inside the CLI's error handler. CPython's final flush also fails on stderr, which yields exit status 120. D4 hedges with "with writable stderr," but T4 asserts a stderr `ERROR` line in the pty case.

**Suggested disposition:** state the result when stderr is dead (exit 1, no diagnostic, stderr neutralized the same way as stdout). Make sure `_emit_error` and `_status` failures can't escape. In T4, route stderr separately from the hung pty in the case that checks the JSON diagnostic.

**F6 [P2] PostgreSQL schemas: one shared validator quietly adds the `public` rejection to more entry points.**
Today `PostgresRunner(dsn, schema="public")` and structural runners exposing `schema="public"` are accepted. D3 plus "one pure shared validator" rejects them, but the difference register only mentions the 63-byte limit.

**Suggested disposition:** either list the `public` rejection on exported and structural paths as a compatibility change in the register and CHANGELOG, or apply only grammar and length there. Validate at the public entrances only: `require_schema_name`, `PostgresRunner.__init__`, the two waiter constructors, and the plugin structural-runner branches. Leave out the private registry and listener; they are reachable only through those entrances.

**F7 [P2, owner decision / possible expansion] R3: describing the 8.8.1→baseline regression as a support boundary needs to be truthful.**
The investigation matrix itself records that 8.8.1 refreshed correctly. The stale route is classic ABA: `alias_version` restarts at 0 (`schema.py:69–70`). The result is silent, unbounded misrouting into a queue nobody reads. D6 is honest about the boundary, but the rollout section names only the SQLite refusal and the PostgreSQL schema limit as compatibility changes in CHANGELOG.

**Suggested disposition:**
- Required: name this regression and its required maintenance procedure in CHANGELOG.
- Owner option, not a blocker: seed the initial `alias_version` from a non-repeating value at bootstrap. That is a one-line change, keeps the counter (no return to wall-clock versions), adds no epoch machinery or migration, and breaks the alias ABA.

**F8 [P2] D7: the "dedicated LISTEN session" exemption can't be configured separately.**
The listener and the command pool use the same DSN and role (`validation.connect`, `runner.py:1009`). Operators can only set `idle_session_timeout` per role, per database, or through DSN `options`, and all of those apply to every SimpleBroker connection.

**Suggested disposition:** recommend `options=-c idle_session_timeout=0`, or a role-level setting for the broker role, on the SimpleBroker DSN. Also drop "By default" from "the third failure…": there is no public setting for it, and the phrase implies one.

**F9 [P2] Tests and spec anchors.**
- T1 should choose start methods with explicit `multiprocessing.get_context(...)` rather than relying on version defaults. Add a forkserver whose preload opens SQLite as a negative control.
- D5's anchor, "after SQLite legacy target validation rules," doesn't match a distinct paragraph in `[SB-CLI-2]` (`10-cli.md:124–156`). Name the exact anchor.
- D5 should also say that a user-created zero-byte file is now set up by `read`, `list`, `stats` and `--vacuum` the same way as a first-writer file.

**Interface contracts:** no new public types or settings are added. The changed contract surfaces are the error classes (D1 `RuntimeError`, D3 `DatabaseError`), the watch exit code 0→1 for non-EPIPE output failures, and the F6 `public` rejection. Only F6 is missing from the difference register.

## Standing constraints

Nothing overturns a standing constraint, so no RECONSIDERATION REQUESTED.
- Redis Task 2 is sound. A creator PID captured inside the generator body, checked before `_exit_generator_batch`, catches the case where the child has already recovered the core. Core recovery already resets `_lock` and `_active_generator_batches` (`core.py:296–299`). Same-thread foreign finalization is unaffected.
- Inherited SQLite connections are still abandoned, never closed.
- ALT-IMPL06-001/002 hold: no listener pings, unchanged rechecks.
- No user callbacks are reset on startup.

## Non-actionable observations

- Finite commands already share the stale-fd `dup2` hazard in `_redirect_stdout_to_devnull` (`commands.py:119–136`). The plan doesn't make it worse.
- Process-wide refusal for unrelated targets and `:memory:` goes beyond what is causally required, since the hazard is per inode. Rejecting identity-based refusal is reasonable given URI, alias and attach paths.
- When `LOGGING_ENABLED` is on, `_handle_error` logs `exc_info`. T4's "no traceback" check should pin the default configuration.
- With `_POOL_MAX_IDLE_SECONDS = 10` and the reaper running on a 10–20 s cycle, a server timeout of about 20 s or less can also kill pooled data connections. This predates the plan.

## Verdict: **BLOCKED**, on F1 only

- **Implementable with confidence?** Yes for Tasks 2–5 once F3–F9 are folded into the text. Not yet for Task 1: GC-closed connections are an undecided core behavior.
- **Degrades safety or robustness?** Not on safety: every change refuses more, never less. Robustness does degrade under F1: permanent false refusals, and fork tests whose results depend on test order.

After F1 is resolved by either option, with D1 and Task 1 amended to match, I expect PASS. A scoped re-check of the accepted findings would be enough.
</details>

## Appendix: Plan2 review, verbatim

<details>
<summary>Anthropic Claude scoped verification and follow-up findings</summary>

# Plan2 scoped review: runtime boundaries and failure handling

**Verdict: PASS.** Two new P2 amendments should go into Task 3 before that slice is implemented; neither blocks spec promotion. One P3 note covers T1.

I checked the baseline `75616556` with Read, Grep and Glob only. I did not re-poll the deferred optional F7 counter seed.

## Verification of accepted findings

**F1 is resolved.** D1, Task 1 and T1 now pick option (a) and spell out the details:
- managed opens use a real `Connection` subclass passed as `factory=`;
- the token is retired only after a successful close, on both the explicit-close and creator-GC paths, never from a weakref callback;
- the child keeps its copy alive before the base C deallocator can close it;
- there is no global strong reference in the parent;
- a failed constructor stays conservative.

This doesn't make anything less robust than today. Today a dropped connection is closed by the C deallocator. The new finalizer does the same close earlier, in the same creator process. Both close paths go through the subclass's `close()`. The runner's `_close_tracked_connection` (`_runner.py:815–824`) and the settings-failure close (`_runner.py:373–376`) both reach it.

`check_same_thread=False` is new only for the temporary connections (`validation.py:54`, `runtime.py:35`, `runtime.py:107`). The runner already sets it (`_runner.py:363`).

The child-side retention fits the existing `_ABANDONED_FORK_CONNECTIONS` mechanism (`_runner.py:297–303`).

**F2 is resolved.** Task 1 now uses one count plus a once-only flag per lifetime. The extra token states, the snapshot and the post-connect PID promise are gone. A reentrant lock with integer-only critical sections means a GC finalizer that runs inside a critical section cannot deadlock.

**F3 is resolved.** Every guarded name exists:
- `get_connection` :328, `owned_connection` :435, `_settings_connection` :452
- `run` :670, `begin_immediate` :699, `commit` :723, `rollback` :743
- `_run_exclusive_setup` :898, which covers all three setup wrappers at :862–896

The excluded `close` :766, `is_setup_complete` :1021 and `release_runner_thread_connection` :217 all exist. Putting the guard after PID recovery keeps marker recovery (:325–326) intact. Callers of `rollback` in `db.py` and `schema.py` mostly wrap it in `contextlib.suppress(BaseException)`, so a refused rollback won't hide their primary errors. D1 now describes watcher retries truthfully.

**F4 is resolved for the carrier and the residual buffer.** Today `_handle_error` raises `_ErrorHandlerFailure(eh_error, e) from e` (`watcher.py:676`) and later raises `error_handler_exception from handler_exception` (`watcher.py:1215`). A separate carrier therefore avoids the self-cause loop. Re-raising the original after cleanup, outside the handler, leaves no context loop.

Close-before-replace is sound for CPython's standard streams. Standard stdio wrappers don't own their file descriptor, so closing them doesn't close fd 1. The bytes still sitting in the closed buffer can never be flushed. `LOGGING_ENABLED` defaults to `False` (`_constants.py:470`), so the no-traceback assertion holds under the default configuration. Two new findings below concern the mechanism itself.

**F5 is resolved.** D4 states the exit-1-without-diagnostic result. Task 3 confines the stderr guard to the parsed watch command's error emission (`cli.py:2189–2194` → `_emit_classified_cli_error`) and explicitly forbids `print(file=None)`. T4 separates the stderr-writable case from the both-dead-PTY case.

**F6 is resolved.** The `public` rejection on the direct and structural entry points is now listed in the difference register, Task 4, T5 and the rollout. The entry points are `require_schema_name` (`validation.py:62`) and its plugin uses at `plugin.py:303/542/1013/1050`, plus `PostgresRunner` and the waiters.

**F7 (required part) is resolved.** Task 5 and the rollout name the 8.8.1 regression, the maintenance procedure and the support limit, and don't describe earlier refresh as user error.

**F8 is resolved.** The listener connects with the same `self._dsn` (`runner.py:372–374`) that `validation.connect` uses (`validation.py:129–132`). That matches D7's "covers dedicated LISTEN sessions and command connections". "By default" has been removed.

**F9 is resolved.** The D5 anchor exists verbatim (`10-cli.md:145`). The D1 replacement phrase exists, wrapped across lines (`16-python-library-api.md:1316–1317`). T1 now names explicit start-method contexts, the separate version-default smoke run, and the forkserver-preload negative control. D5 and T3 list the caller-created zero-byte cases for each command.

**Standing constraints are preserved.** Inherited SQLite is never closed in the child. There are no PG pings or shorter rechecks, no callback/startup resets, and Redis foreign-thread cleanup is retained. No proposed limit is presented as already ratified: the Plan1 dispositions and the Interface Design Review both say adoption is still pending.

## New findings caused by the amendments

### F4.1 [P2] The launch-time stream check runs too late to come "before backend acquisition"

**Evidence:** Task 3 puts the check "before `_resolve_watch_inputs` … and the startup banner", which is inside `cmd_watch`. But `_main` already does the following before it dispatches to `cmd_watch`:
- `_resolve_target` (`cli.py:2155`);
- `_validate_legacy_sqlite_target` (`cli.py:2167`), which opens SQLite through `is_valid_database`;
- `_validate_command_target` (`cli.py:2183`), which for non-legacy targets calls `plugin.validate_target(..., verify_initialized=True)` (`cli.py:1770`). For PostgreSQL that opens a socket.

When fd 1 or 2 is missing at launch, that socket can take the lowest free descriptor. The "keep the opened sink alive even if it gets fd 1 or 2" step only helps if it happens before any backend open.

**Suggested disposition:** Do the parsed-watch stream check, and any sink that occupies the free descriptor, in `_main` for `args.command == "watch"` before `_resolve_target`. Leave `cmd_watch`'s own check as a backstop for in-process callers. Add one T4 case: PG-target watch with fd 1 closed at launch gives exit 1, and the validation connection does not land on fd 1. This is a Task 3 and T4 wording change; D4 is fine as written.

### F4.2 [P2] Unconditional replacement of `sys.__stdout__`/`sys.__stderr__` can hit a healthy interpreter stream

**Evidence:** Task 3 says "replace both sys.stdout/sys.__stdout__ (or both stderr references)". `cmd_watch` is called in-process by tests (`tests/test_commands_error_ownership.py:247–278`, using capsys), and embedding code can do the same. In those cases `sys.stdout` is a stand-in object and `sys.__stdout__` is the healthy original.

If a stand-in fails, the current wording would close a stream the caller owns, and then permanently point the interpreter's original reference at devnull. That state leaks into the rest of the process or the xdist worker.

**Suggested disposition:** Close and replace both references only when the failed object `is sys.__stdout__` (or `sys.__stderr__`), meaning the stream really is CLI-owned. Otherwise replace only the public reference and leave the caller's object unclosed. That matches the current `_replace_stdout_with_devnull` scope (`commands.py:109–116`). Add an in-process T4 control asserting that `sys.__stdout__` is unchanged after a failure of a stand-in stream.

### F1.1 [P3] T1 doesn't name a failed open as a control

**Evidence:** Task 1 makes a failed constructor conservative until it is proved that no native handle survived. That proof is listed only under "verify subclass initialization failure". T1 lists "failed close" but has no case for a failed open followed by a safe fork. If the proof is never written, every ordinary connect failure leaves the process permanently refusing SQLite after fork, which is the same false-refusal problem F1 fixed. Examples are a permission or I/O error on the read-only validation open, or the setup open.

**Suggested disposition:** Add to T1, per qualified runtime: a failed managed connect, then explicit close of everything else, then a fork whose child succeeds. A runtime where the proof fails stays conservative, and that should be recorded as a residual.

## Residual limits

These are accepted, and all are documented in the plan:
- Interpreter shutdown, reentrant or native forks, raw or third-party SQLite connections, and free-threaded runtimes are not covered.
- A refused child may spend its existing watcher retry budget before failing.
- A descriptor that external code closes and reuses while the command runs is not covered.
- Retained PostgreSQL clients across target replacement are not supported, and the optional counter seed is deferred with its reconsideration trigger.
- Normal-wrapper close semantics must be qualified per runtime in T4.
- Every proposed limit still needs owner adoption.
- All runtime gates are future work; only the documentation gates have run.
</details>

## Appendix: Plan3 review, verbatim

<details>
<summary>Anthropic Claude final scoped recheck</summary>

**Plan3 scoped recheck: F4.1, F4.2, F1.1**

I used Read and Grep only, at baseline `75616556`. I made no writes, used no shell or subagents, and wrote no plan file.

### Findings

**F4.1: verified.** In `cli.py:2130-2184`, `_main` calls `_resolve_target` (2155), then `_validate_legacy_sqlite_target` (2167), then `_validate_command_target` (2183), and only then `_dispatch_command`. The pre-target steps before 2155 (`_run_pre_target_action`, the write/keep check and the selection-filter check) don't touch a backend. So putting the parsed-watch stream guard ahead of `_resolve_target` comes before every backend open the plan names. Keeping the `cmd_watch` check as a backstop for direct calls, ahead of `_resolve_watch_inputs` and the banner, is still correct. The T4 case (PG target, fd1 closed at launch, exit 1, no validation connection acquired) tests this ordering directly.

- **F4.1-a [P3, test note only]:** Task 3 says any sink installed at startup must hold its free standard descriptor before the backend is acquired. That covers the case where only stderr is missing and watch goes ahead. T4 lists "absent stderr at launch" but only asserts descriptor occupancy for the fd1/PG refusal. Suggest that the stderr-absent case also check that fd2 belongs to the sink, not a backend socket, before acquisition. Otherwise a later diagnostic could be written into a PG socket. SQLite's own guard keeps its files off fds 0–2, so the risk is mainly PG/Redis sockets. This doesn't block.

**F4.2: verified.** The ownership rule now reads the same way in Hidden couplings, Task 3 and T4:
- The failed wrapper is closed, and both references replaced, only when it is also the interpreter's original stream.
- A caller-owned stand-in is not closed; only `sys.stdout`/`sys.stderr` is replaced. That matches what `commands.py:109-116` (`_replace_stdout_with_devnull`) already does.
- The healthy `__stdout__`/`__stderr__` is preserved.

D4's scope (normal CLI-owned streams plus streams absent or closed at launch) matches this. Custom stream finalization and external reuse of raw descriptors are explicitly excluded. Keeping the existing EPIPE dup2 path doesn't conflict, because that is the clean closed-pipe exit, not the fatal-output path. No further consequence.

**F1.1: verified as implementation guidance.** T1 now explicitly requires: an ordinary missing or unwritable path fails to open natively with no leaked count, then all other handles are closed, then a later child succeeds. Task 1 handles the verified `SQLITE_CANTOPEN` no-handle path separately and retires it exactly once. Two things make that consistent:
- The once-only accounting flag means the later finalizer, whose `close` raises `ProgrammingError` on the uninitialized object, can't poison or double-retire the count.
- Constructor failures from callbacks or audit hooks that are ambiguous stay conservative, and the plan doesn't treat every constructor exception as "no live handle".

`requires-python >= 3.11` means `sqlite_errorcode` is available for that classification.

- **F1.1-a [P3, note]:** The CI matrix (`.github/workflows/test.yml:93`) also includes 3.12 and 3.15, which the CANTOPEN prototype didn't cover (it ran on 3.11, 3.13 and 3.14). The mandatory T1 gate across the matrix covers this. When recording execution evidence, just don't describe the prototype as qualifying 3.12 or 3.15.

I found nothing that touches the standing constraints: no child SQLite close, no active PG ping or changed recheck, no callback reset, and no newly ratified risks. The proposed boundaries still await owner adoption.

### Verdict: **PASS**

All three accepted amendments are implemented consistently across the spec delta, the task text and the tests. The two P3 notes are optional test and evidence wording, not blockers.
</details>


## Verbatim implementation review R5

I didn't find any blocking issues in the R5 slice. I read `core.py` (the two generators, the new helper, `_check_fork_safety`, the guard helpers and the settle/rollback scripts), the new test file, the T2/D2/Task 2 sections of the plan, [SB-API-5] at `docs/specs/16-python-library-api.md:717`, the existing foreign-thread and rollback tests, and the Queue-level wrappers. I used only Read/Grep/Glob and changed nothing. I did not re-run the tests; the pass/fail evidence is the logs you supplied.

## Findings

**R5-1 — Low (redundant code, not covered by any test). Owner decision.**
The check at the top of each loop (`core.py:1121`, `core.py:1378`) can only fail in the unsupported case.
- **Evidence:** `owner_pid` is set in the same frame with no `yield` before the first check, so that first check always passes. Every later pass back to the loop top comes from resuming at a `yield`. That resume has already run the after-yield check (`core.py:1137`, `core.py:1395`), including after the last row. So a different PID only reaches the loop top through a fork during a driver call or from inside the generator's own thread, which the plan already excludes (plan:158-160).
- **Tests:** none of the 18 cases would fail if these lines were removed. The [SB-API-5] promise of no new batch already holds through the after-yield check.
- **Disposition:** Plan Task 2 (plan:453) does require "Check before another batch", so keeping it is compliant. If you want to drop unneeded code, remove both lines and change that Task 2 sentence. Otherwise keep them and note in the execution log that they are a backstop with no test of their own. Either way, not a blocker.

**R5-2 — Low (test coverage). Owner decision, outside T2.**
The new tests only drive `RedisBrokerCore` directly. D2's wording is general enough to read as covering the public `Queue.claim_generator`/`move_generator` too, which wrap the core in `with self.get_connection()` (`sbqueue.py:858`, `sbqueue.py:1657`).
- **Persistent Queue:** a child close runs `release_connection_after_use`, which returns early for an inherited session (`db.py:1280`) before taking any session lock. After that, the core helper controls. I see no gap here.
- **Ephemeral Redis Queue:** I did not trace `DBConnection.__exit__` in the child.
- **Disposition:** T2 as written only asks for core-level proof, and the reported R5 bug happened "without Queue use", so the slice meets its gate. A Queue-level child-exit case belongs to the full-integration step, not this slice.

**R5-3 — Info (existing behaviour, slightly more visible now).** No change suggested in this slice.
- **What changed:** a child that closes an inherited generator without first recovering the core now leaves the parent's entry in `_active_generator_batches`. Before, the child's close removed it, along with the bug of settling the parent's batch.
- **Where it shows:** `generate_timestamp`/`get_ts` (`core.py:382`) checks for reentrant mutation without calling `_check_fork_safety` first. In that child it can raise "batch is being yielded", or block on the inherited `_lock`.
- **Why it isn't new:** the same happened at baseline in any child that didn't close the generator. Any other public operation recovers the core and clears the entry (`core.py:307`).
- **Disposition:** note only. If wanted later, add `_check_fork_safety()` to `generate_timestamp` as a separate change.

**R5-4 — Nit.**
- The helper tells claim from move by `target_queue is None`, and the PID test is written twice: once raising (`_require_generator_process`), once returning (helper line 1096). Both are acceptable for two call sites.
- `generator.throw()` in the child isn't tested. It takes the same `finally` path as `close`, which is tested.

## Checks that passed (no finding)

- **Settlement and errors:**
  - The early `return` sits inside the helper, not in `finally`, so `RuntimeError` and `GeneratorExit` still propagate. The `finally` block contains no `return`, `break` or `continue`.
  - The child does nothing before the PID check, so `_exit_generator_batch`'s `_lock`, the commit/rollback scripts and `_record_maintenance_activity` all run in the parent only. `locked_close` proves the inherited lock isn't taken, because a blocked child hits `_reap`'s 15s kill.
  - On a child advance after the last row, the after-yield check raises before `completed = True`, so the child can't commit.
- **Child recovery (`recover=True` cases):** calling `peek_one` in the child resets the core's mutable PID, lock and guard table first. The old iterator still refuses to advance and still skips settlement. This shows the stored `owner_pid` is what decides, not the core's mutable state.
- **Ownership timing:** `owner_pid` is captured when the generator body first runs, matching "first begins execution" in the spec. A generator created in the parent and first started in the child belongs to the child.
- **Unchanged behaviour:**
  - The exactly_once branches are untouched.
  - Same-process foreign-thread close still works, since the helper checks only the PID (existing test at `test_redis_core_behaviors.py:1043`).
  - Parent rollback is still covered (`test_redis_batches.py:17`).
  - There is no thread guard and no reliance on the core's mutable PID.
- **Test quality:**
  - Each case runs in a fresh subprocess, so the child never inherits pytest teardown.
  - The fork happens before the parent's `try`, so the child never runs `shutdown()` or `cleanup_target`.
  - The checks use real Redis state: an independent consumer must see no duplicates, the parent must finish and commit without a stale-batch error, and for moves the target must hold exactly the moved rows.
  - Every child finishes with a normal return, so ordinary interpreter finalization is the thing being tested, which was the reported trigger.
  - Your logs show all 18 failed on baseline, so each case actually exercises the fix.
- **Your standing constraints:** the child never settles the parent's work, and same-process foreign-thread finalization still works. No watcher callbacks are touched. No reconsideration requested.

## Verdict: PASS

The slice does what D2 and Task 2 ask, covers T2 with real Redis tests, adds no new parent-side risk, and leaves the code simpler than the duplicated `finally` blocks it replaced. R5-1 and R5-2 are your call and neither blocks this slice. Full integration is not yet claimed, which matches what you said.

## Verbatim implementation review R2/R4

~~~text
**Verdict: PASS.** I found no blocking or P2 issues. There are four optional P3 items and a few observations. This review was read-only: I didn't run anything. The four RED/GREEN logs I opened match the counts in the report, and I relied on the report for which tests failed and why.

## Confirmed against the plan (D4, D5, Task 3, T3/T4) and spec 10

- **Empty-file race (D5).** `cli.py:1715-1723` skips the preflight check only when the target is a regular file of zero bytes. Nonempty files, directories and other non-regular paths still go through the same check as before. Init and cleanup take their own path earlier (`cli.py:2160`), and the project-discovery check isn't touched.
- **Callback error carrier.** `commands.py:80-131` adds a private carrier that is distinct from the original error, so there's no `raise e from e`. `cmd_watch` (`commands.py:1737-1752`) takes the original error back out, copies cleanup notes onto it, and raises it after the `except` block, so the error has no self-reference loop. `_finish_watch` (`1756-1779`) always calls stop, and keeps the original error ahead of any stderr or stop failure. Both watcher types use the new handler. Library continuation is unchanged: only `cmd_watch` passes the handler, and `watcher.py:657-676` is untouched.
- **Clean stop on a closed pipe (exit 0).** The closed-pipe check still runs first in `_write_stdout` (`commands.py:207-209`). `_redirect_stdout_to_devnull` and its `dup2` logic are the same as the baseline. Removing the extra stdout flushes is safe because `_output_message` → `_print_stdout` already flushes after every message.
- **Close before sink, ownership.** `_retire_failed_standard_stream` (`commands.py:98-109`) closes the failed wrapper before `open(devnull)`. It replaces both `sys.stdout` and `sys.__stdout__` only when they are the same object. A caller's stand-in stream is left open, and the healthy interpreter original is left in place. There is no `dup2` on the failure path, so nothing promises to repair a reused raw descriptor.
- **Missing streams.** The check runs first inside `_main`'s `try` (`cli.py:2141`), before global flags, selectors, `_resolve_target` and both validation calls. The direct-call backstop is at `commands.py:1688`. The sink is kept alive when it lands on fd 1 or 2.
- **Stderr failures.** `_emit_watch_cli_error` (`cli.py:2200-2216`) never calls `print(file=None)` and always ends in exit 1. It applies only to parsed `watch` commands; diagnostics before parsing are unchanged.
- **Finite commands.** The new flag defaults to False. Catching ValueError and AttributeError in `_write_stdout` re-raises them unchanged when the flag is off, so finite commands behave as before.
- **Suppression registry.** There is exactly one `SIM115` (`commands.py:168`), shared by the closed-pipe fallback and the watch retirement path. Registry rows `10-ruff-suppression-registry.md:73` and `:121` name the new owner function with a count of 1. I agree with keeping this single shared allocation point.
- **State-machine row.** `07-…map.md:426` now describes `CALLBACK_ERROR_TERMINAL` and points generic continuation to `SM-WATCHER-LIFECYCLE`.
- **Tests use real seams.** Real subprocesses, a real PTY hangup (EIO), real fds closed before `exec`, real SQLite, and real OS lock contention. The patches only pause or observe and then call the real function (`sqlite3.connect`, `_fetchone_and_close`, `SQLiteRunner.commit`, `_AdvisoryLock._try_lock`, `_start_strategy`, `_resolve_target`). Storage, locks and process creation are not faked.
- **RED explanations are believable.** The 14 empty-file cases fail with "invalid header" at HEAD. The PTY cases hang. The missing-stream cases reach target resolution. The transition test returns 0 and reaches the second callback.

## Findings (all P3, optional)

1. **P3: `cli.py:2214` catches more than stream errors.** It catches ValueError and AttributeError around all of `_emit_classified_cli_error`. A bug in that code (for example the unsupported-code ValueError at `commands.py:246-247`) would close a healthy stderr, replace it with /dev/null and drop the diagnostic. Task 3 says to suppress only expected stream failures. Suggested fix: retire stderr only for OSError, or only when stderr is now None or closed, and otherwise re-raise. The exit code stays 1 either way.
2. **P3: one failure path can replace the original error, only on runtimes we haven't tested.** If `close()` leaves the wrapper open, `_retire_failed_standard_stream` re-raises (`commands.py:107-108`). Called from `_finish_watch:1765`, that error replaces the original one. Called from `cli.py:2208` or `:2215`, it escapes `_main`. The plan treats such runtimes as not covered, so failing loudly is acceptable. If you want to keep the original error anyway, turn that failure into a note in `_finish_watch`.
3. **P3 (docs): wrong test file in the state-map row.** In `07-…map.md:426` the "Transition table" column says `tests/test_cli_watch.py`. The row now names `CALLBACK_ERROR_TERMINAL`, which lives in `tests/test_watcher_transition_tables.py` (line 388 of the same doc says so). The column was already wrong at the baseline.
4. **P3 (test): the "header" phase doesn't check its file state.** `test_cli_first_use.py:85-86` asserts a zero-byte file only for the "zero" phase. The "header" phase never asserts `st_size > 0`, so if it ever duplicated the zero phase, nothing would catch it. The "header" and "commit" phases pass at HEAD, so they are regression controls rather than RED tests, which the report already says.

## Observations (no action needed)

- **Pending-count check runs after exit.** `test_cli_watch_output_failure.py:146-154` counts pending messages after the process has exited. That proves the exit and that the first effect stayed committed. The proof that no second message is attempted comes from the transition row (`test_watcher_transition_tables.py:909-973`). Together they match T4.
- **Unencodable text is now terminal.** A UnicodeEncodeError while writing a watch message now stops the watch with exit 1, where the baseline continued after the message had already been claimed. That fits D4 and is safer.
- **Unreadable empty files.** A zero-byte file that can't be read now skips the preflight permission message and fails later in setup. The error changes, but no data is at risk.
- **Caller stand-in replaced permanently.** For an in-process caller whose own replacement stdout is already closed, `_check_watch_streams` permanently swaps `sys.stdout` for a /dev/null file. The plan allows this.
- **Already true before this change:**
  - Startup config warnings are printed with `print(file=None)` when stderr is missing (`cli.py:2227`).
  - Closed pipe plus a stderr flush failure gives exit 1 (`commands.py:1764-1767`). This is no worse than before.

Left to the parent: `git log` shows nothing committed yet. Linux and other Python versions still need testing, and the combined gates haven't been run.
~~~

## Verbatim implementation review R6, R3/R7

~~~text
# Review: PostgreSQL R6 slice plus R3/R7 qualification tests (baseline `75616556`)

I only read files for this review. I didn't run anything or change any files. I couldn't write a plan file or call ExitPlanMode because neither tool is available in this session, so the review is all here. I checked the production diff, the three new or edited test files, plan D3/D6/D7, Tasks 4/5, T5–T7, spec 16 lines 175–185, the CHANGELOG entries, and the owning code in core and the watcher.

## Findings

**F1 [P2]: Caller-injected runners with their own `schema` can still create a schema without validation.**
- **What happens:** You can call `BrokerCore(custom_runner, backend_plugin=PostgresBackendPlugin())` with a runner that isn't a `PostgresRunner` (`db.py:1461`, `1500`). That runner's `schema` is never validated:
  - `BrokerCore.__init__` always runs `_setup_schema` → `plugin.initialize_database` (`db.py:1541`, `1658`).
  - That reads `runner.schema` without checking it (`plugin.py:650`) and passes it to `schema.initialize_database`.
  - That takes the schema-setup lock keyed on the full name (`schema.py:220`, `286`), then runs `CREATE SCHEMA IF NOT EXISTS "<64 bytes>"` (`schema.py:257`, `292`). PostgreSQL truncates that name.
  - So the R6 symptom remains on this path: the lock keys use the full name while the physical schema is the shortened one. `"public"` is also accepted here.
- **Why it matters:** Spec 16:180–181 now promises validation "precedes … database creation or mutation through plugin entry points", and `initialize_database` is a plugin entry point. Task 4 also says structural-runner plugin paths must use the same validator. Your summary says "borrowed structural paths included", but only the waiter paths are covered.
- **Not a regression:** the baseline behaves the same way. The problem is that the promoted spec now claims coverage this path doesn't have.
- **Suggested fix:** use `schema_name = validate_schema_name(cast(...).schema)` at `plugin.py:650`, and add a `structural_core` entry to the T5 entrance matrix. That test needs a stub runner whose `run` raises `AssertionError`; it should expect a `DatabaseError`, or a wrapper whose cause is one. No internal layer needs its own check: `prepare_alias_mutation`, `vacuum` and `database_size_bytes` only run after setup has passed this point.
- **Alternative (owner decision):** narrow the spec text so it explicitly excludes caller-injected core runners. I recommend the one-line fix. The CHANGELOG line about "structural … entry points" already covers it.

**F2 [P3, simplification]: The two validation calls added in the plugin's waiter methods are redundant.**
- Both waiter constructors now validate the schema as their first statement (`runner.py:813`, `898`). That happens before the registry acquire, the connection-owner capture, or any socket or thread is created.
- Nothing in `plugin.py:1003–1016` or `1040–1053` does any I/O between reading `runner.schema` and calling the constructor.
- Removing the calls at `plugin.py:1006` and `1043` changes no behaviour, and the `structural_waiter` and `structural_multi_waiter` rows still pass. This follows Task 4's "do not duplicate validation at every internal layer".
- Combined with F1, the net `plugin.py` change becomes a single call in `initialize_database`.

**F3 [P3]: The healthy-notifications case in T7 can occasionally fail.**
- In `_run_with_retries`, the warning is logged at `watcher.py:1035`, before `_handle_retry` records the failure at line 1040.
- The test counts `retry_diagnostics` and `watcher.failures` (`test_pg_idle_session_recovery.py:166–172`) while the watcher is still running, because `stop()` only happens in the `finally` block.
- If another idle expiry lands between those two reads, the counts differ by one. The window is microseconds roughly every 10 seconds, so failures would be rare.
- **Fix:** in the notify branch, stop the publisher and the watcher and join both before comparing the counts.

**F4 [P3, optional]: T7 doesn't prove the failures were caused by the idle timeout.**
- The test checks the error class and the counts, which is what T7 asks for.
- It doesn't check that each failure came from server idle-session termination (SQLSTATE `57P05`, or the server message, on `__cause__` when present).
- Without that, an unrelated connection loss would also satisfy the test. Adding the check costs little and keeps the test from relying on timing.

**F5 [P3, optional]: Two T5 assertions are looser than they need to be.**
- `match="63|public"` lets each parameter pass on the other parameter's message. Matching per parameter would be tighter.
- In the isolation test, `not waiters[1].wait(0.1)` only watches for a misrouted notification for 0.1 seconds. Message-row isolation is the stronger evidence. Either lengthen that window or label it a smoke check.

## Things I checked that hold up

- **Validator:** one pure function. It checks the existing grammar, rejects `public` and caps names at 63 characters. Because the regex is ASCII-only, 63 characters equals 63 bytes. It is called before any resource is created, in `require_schema_name`, `PostgresRunner` (before `_owner.capture()` and the pool), and both waiter constructors (before registry acquire).
- **No new mechanisms:** no hashing, truncation, migration or format change. Private listener and registry helpers rely on the validated entrances.
- **Regex `.match` and `$`:** `$` still accepts a trailing `\n`. That predates this change and isn't made worse: the length cap counts the newline correctly. Observation only.
- **D3 wording:** plan D3 (lines 257–267) and spec 16:175–185 match word for word. The `RuntimeError` wrapper clarification is accurate:
  - The real test asserts `RuntimeError` with a `DatabaseError` cause on Queue read/write.
  - The lazy waiter path raises `DatabaseError` directly, which "may wrap" covers.
  - Minor simplification: the deviation-log row says there is no runtime deviation, so it reads better as a clarification note than as a deviation.
- **T5 tests can fail when they should:**
  - Without the fix, the spies raise `AssertionError` rather than `DatabaseError`.
  - The collision test runs real reads, writes and cleanup against the 63-byte prefix target. Without the fix, the server truncates the long name, including in the `information_schema` name comparison during cleanup, so the long names reach the prefix schema and the test fails. That matches the six failures you saw on the baseline.
  - Every test cleans up its schemas in `finally`.
- **T6:** this is the positive supported-path proof the plan asks for, and it doesn't assert the stale behaviour as expected.
  - Owners are closed before the CLI cleanup, init and alias recreation; the fresh session then sees `new-target`.
  - The test can't silently pass against the wrong target: `.broker.toml` controls the target and schema (`verify_env`, `plugin.py:298–316`). If the CLI acted on anything else, the `resolve_alias == "new-target"` and `old-target` peek assertions would fail.
  - Observation: unlike the existing `_run_cli` helpers, the subprocess inherits the ambient environment and `PYTHONPATH`. That's acceptable given those assertions.
- **T7 respects the standing constraints:**
  - The hooks only observe, and each calls `super()`.
  - It reads the listener's backend PID and libpq parameters without running queries.
  - It uses real per-connection `idle_session_timeout` and a real `pg_notify` on the independently written channel literal.
  - Nothing is mocked: no clocks, retries or rechecks.
  - Generations are valid because the listener has no internal reconnect (`runner.py:369–427`), so each new backend PID is a new strategy start.
  - The quiet case asserts that the terminal error is the original third error object.
  - **ALT-IMPL06-001/002 are preserved:** no pings, no shortened recheck, no new health witness, and no callback resets.
- **CHANGELOG:**
  - It names the 8.8.1 → 8.8.2 alias-counter regression truthfully, along with the maintenance workaround and the narrower support boundary.
  - It lists the stricter `public` and length rules for direct and structural entry points as compatibility changes.
  - The idle-timeout guidance matches D7.
- **Observation:** spec traceability rows don't reference the new tests yet. Task 6 owns that, so it isn't a finding against this slice.

## Verdict

**PASS** for what this slice actually does: the R6 entrances you enumerated, plus the T6 and T7 proofs. It doesn't weaken any standing constraint.

F1 needs a decision before Task 4 closes, because the promoted spec currently promises coverage for a public path that isn't validated. The fix is one line plus one test row; the alternative is narrowing the spec. F2–F5 are optional simplifications and test-hardening.
~~~

## Verbatim implementation review R1

~~~text
### Findings

- **S1 [P2]**
  - **File:Line:** [06-process-session-core-ownership.md](file:///Users/van/Developer/simplebroker/docs/implementation/06-process-session-core-ownership.md#L594-L596), [2026-11-11-runtime-boundaries-and-failure-handling-plan.md](file:///Users/van/Developer/simplebroker/docs/plans/2026-11-11-runtime-boundaries-and-failure-handling-plan.md#L421-L431), and [16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L1396-L1400)
  - **Evidence:**
    1. `docs/implementation/06-process-session-core-ownership.md:594-596` still states: *"A verified native cannot-open failure retires without mistaking the uninitialized wrapper's close error for a live connection; ambiguous failure remains conservative."*
    2. `docs/plans/2026-11-11-runtime-boundaries-and-failure-handling-plan.md:421-431` (Task 1) still prescribes error-code-based `SQLITE_CANTOPEN` retirement with no Deviation Log entry recording the post-review revision to the `_ConnectionFactory` finalizer/weakref witness.
    3. `docs/specs/16-python-library-api.md:1396-1400` (`_Implementation mapping_`) omits `simplebroker/_sqlite_fork.py` (as well as `simplebroker/_backends/sqlite/runtime.py` and `simplebroker/_backends/sqlite/validation.py`).
  - **Concrete failing sequence:** A maintainer or agent reading implementation rationale or Task 1 to modify `_sqlite_fork.py` relies on the stale `SQLITE_CANTOPEN` description, or misses `_sqlite_fork.py` when tracing `[SB-API-11]` implementation owners from the spec mapping.
  - **Suggested disposition:** Update `docs/implementation/06-process-session-core-ownership.md:594-596` to describe the non-owning `_ConnectionFactory` witness (retiring a failed constructor attempt only when its finalizer ran on the constructing thread and a fresh weakref taken inside `__del__` is dead), record the revision in the plan's Deviation Log / Task 1 notes, and add `simplebroker/_sqlite_fork.py` to `_Implementation mapping_` in `docs/specs/16-python-library-api.md`.

- **S2 [P3]**
  - **File:Line:** [test_core_persistence_transition_tables.py](file:///Users/van/Developer/simplebroker/tests/test_core_persistence_transition_tables.py#L1021-L1028) and [test_core_persistence_transition_tables.py](file:///Users/van/Developer/simplebroker/tests/test_core_persistence_transition_tables.py#L1101-L1108)
  - **Evidence:** In `SQLITE_RUNNER_TRANSITIONS`, `FORK_RESET` (`:1025-1027`) still declares `next_state="child connection open"`, `effects="discard inherited connection and create a new one"`, `expected_result="new connection is distinct"`, and `FORK_ACTIVE_RESET` (`:1105`) declares `next_state="child idle with fresh connection"`. However, `_assert_runner_fork_reset` (`:1146-1154`) now asserts `pytest.raises(RuntimeError, match="SQLite access is unsafe after fork")` on `runner.get_connection()` and verifies only that `_pid`, `_transaction_owner`, and `_transaction_admitted_operations` were reset before the refusal.
  - **Concrete failing sequence:** Auditing `SM-SQLITE-RUNNER` transition metadata against what `_assert_runner_fork_reset` actually executes shows the table prose claiming a fresh child connection is opened when the test actually proves sticky fork refusal after state reset.
  - **Suggested disposition:** Update the `FORK_RESET` and `FORK_ACTIVE_RESET` transition descriptions in `tests/test_core_persistence_transition_tables.py` so `next_state`, `effects`, and `expected_result` reflect state reset plus fork refusal under `[SB-API-11]`.

- **S3 [P3, owner decision]**
  - **File:Line:** [_sqlite_fork.py](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L84-L97) and [_sqlite_fork.py](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L121-L153)
  - **Evidence:** `_ConnectionFactory` (~25 lines of witness machinery plus weakref re-registration in `__del__`) exists to distinguish a genuinely deallocated failed constructor from an audit-hook failure on `sqlite3.connect/handle` that retains `args[0]` and raises a nested `SQLITE_CANTOPEN` (or resurrects the partially constructed object). In `[SB-API-11]` (`docs/specs/16-python-library-api.md:1369-1371`), audit-hook manipulation is outside the supported detection guarantee.
  - **Concrete failing sequence:** None — the current `_ConnectionFactory` implementation is sound and does not miscount. Note, however, that any simpler alternative must still avoid calling `super().close()` on an uninitialized `_ManagedConnection` (which raises `sqlite3.ProgrammingError: Base Connection.__init__ not called` and leaves `accounted = True` if retirement is gated on `super().close()` succeeding).
  - **Suggested disposition:** Owner decision: either retain `_ConnectionFactory` as-is (it is localized to `_sqlite_fork.py`, zero-dependency, and makes even adversarial `sqlite3.connect/handle` object retention provably conservative), or, if you prefer minimal machinery under the declared support boundary, replace `_ConnectionFactory` with a boolean `_initialized` flag set after `super().__init__` so `__del__` retires uninitialized instances directly without calling `super().close()`.

---

### Answers to Review Questions

1. **False admission in supported usage:** None found. Every SimpleBroker-managed SQLite open site (`SQLiteRunner.get_connection` in `_runner.py:359`, `check_version` in `runtime.py:36`, `setup_connection_phase` in `runtime.py:108`, and `validate_database` in `validation.py:55`) routes through `_sqlite_fork.connect`. `_Lifetime.__init__` increments `_active_lifetimes` inside `_ManagedConnection.__new__` before C-level `pysqlite_connection_init` / `sqlite3_open_v2` runs, and `_Lifetime.retire()` decrements it only after `super().close()` returns without raising in the creator PID. `_after_fork` latches `_unsafe_fork = _unsafe_fork or _active_lifetimes > 0` across single and nested forks.
2. **False refusal (count leak) in ordinary usage:** None found.
   - *Failed opens at the four sites (e.g., missing parent dir, directory path, bad URI/permissions, invalid keyword args):* CPython's C constructor unwinds and drops the sole reference on the constructing thread before `sqlite3.connect` raises; `_ManagedConnection.__del__` runs synchronously on that thread, records `finalizer_thread = threading.get_ident()` and a fresh `weakref.ref(self)` (which immediately dies when deallocation finishes), and `factory.retire_failed_construction()` retires the count before re-raising.
   - *Exceptions between `__new__` and `__init__`:* If `_Lifetime()` raises (e.g., `_unsafe_fork` is already true), `_active_lifetimes` was not incremented. Once `self._lifetime = _Lifetime()` binds, `__del__` and `retire_failed_construction()` cover any failure before `__init__` completes.
   - *Close errors:* If `super().close()` raises, `lifetime.retire()` is skipped so the count stays positive while the handle remains live; a subsequent successful `close()` or GC pass retires it.
   - *GC on another thread:* `connect()` forces `check_same_thread=False`, so creator-process cyclic GC on any thread succeeds in `super().close()` and retires the lifetime.
3. **`_after_fork` and `_Lifetime` lock/reentrancy safety:** Safe.
   - `_after_fork` takes no locks, reads/writes module globals while the child is single-threaded, and replaces `_lock = threading.RLock()`.
   - `_lock` is a `threading.RLock`. If creator-process GC triggers a `_ManagedConnection.__del__` on the same thread while inside `with _lock:` in `_Lifetime.__init__` or `_Lifetime.retire`, the `RLock` permits reentrant acquisition without deadlock.
   - In `_Lifetime.__init__`, `_active_lifetimes += 1` and `self.accounted = True` contain no intermediate statements and cannot overwrite a nested finalizer's decrement under CPython's bytecode execution (`LOAD_GLOBAL`, `LOAD_CONST 1`, `BINARY_OP +=`, `STORE_GLOBAL` has no intermediate drop into arbitrary Python between load and store on the same thread; GC check intervals occur at bytecode/allocation boundaries before or after the increment). In `retire()`, `self.accounted` is checked and cleared inside `_lock`, ensuring at-most-once retirement.
4. **Guarded and unguarded runner entries:**
   - Every guarded entry in `SQLiteRunner` (`get_connection`, `owned_connection`, `_settings_connection`, `run`, `begin_immediate`, `commit`, `rollback`, `_run_exclusive_setup`) calls `self._recover_after_fork_if_needed()` first and `ensure_sqlite_allowed()` second, before acquiring `_operation_lock`, `_setup_lock`, `_connections_lock`, `_transaction_condition`, or PhaseLock files.
   - `DBConnection.get_connection` (`db.py:947-951`) calls `self._ensure_shared_process_owner()` (preserving the `"Queue used in forked process..."` error for inherited Queues) and then `ensure_sqlite_allowed()` before entering the connection retry loop or session locks.
   - Leaving `close()`, `is_setup_complete()`, and `release_runner_thread_connection()` unguarded is necessary and sound: `_recover_after_fork_if_needed()` calls `is_setup_complete()` at `_runner.py:328`/`410` (so guarding it would break recovery before `ensure_sqlite_allowed()` is reached and break cleanup), `close()` in a child abandons inherited connections via `_recover_after_fork_if_needed()` and closes only the child's empty tracked set, and `SQLiteRunner` does not implement `release_thread_connection`.
5. **`_ConnectionFactory` witness complexity:** Addressed in **S3** above (owner decision, non-blocking).
6. **Documentation alignment:** Addressed in **S1** above (`docs/implementation/06-process-session-core-ownership.md:594-596` is stale after the revision; spec `[SB-API-11]` prose and `SM-SQLITE-FORK-ADMISSION` row in `07-complexity-and-state-machine-map.md:434` match the revised code).
7. **Test quality and contract coverage:** `tests/test_sqlite_fork_admission.py` is thorough, uses real `os.fork` / `multiprocessing` contexts and real `sqlite3` connections in subprocess-isolated probes, and covers the original parent-close write-loss regression, all 11 `SM-SQLITE-FORK-ADMISSION` transition rows, all 4 open sites + `sqlite3.connect` audit-hook opening window, all 4 held mutexes, all 10 runner entry actions + cleanup, GC during close, `spawn`/`forkserver`/`preloaded-forkserver`, failed close retry, child GC resurrection into `_ABANDONED_CONNECTIONS`, and the audit-hook failed-construction edge cases. The only test-table drift is in `tests/test_core_persistence_transition_tables.py` (**S2**).

---

### Verdict

PASS WITH FINDINGS

### Verification Limits

Read-only static review using native file-reading tools only (`read_file`), per instructions. No commands, pytest runs, linters, typecheckers, or git operations were executed; runtime test pass claims, CPython version matrix results, and working-tree diff completeness are accepted as host-reported evidence.
~~~

## Verbatim final combined review

~~~text
### Findings

- **C1 (P3 — Optional / Spec-mapping hygiene): `[SB-API-2]`, `[SB-API-5]`, and `[SB-OPS-7]` omit the extension modules that now own D2, D4, and D6 from their `_Implementation mapping_` blocks.**
  - **Evidence:**
    - In [16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L169-L185), `[SB-API-2]` specifies D4 (PostgreSQL `backend_options.schema` 63-ASCII-byte cap and `"public"` rejection across plugin, [`PostgresRunner`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/runner.py#L975-L987), [`PostgresActivityWaiter`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/runner.py#L804-L814), [`PostgresMultiQueueActivityWaiter`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/runner.py#L888-L899), and structural runner routes), and its `_Verification mapping_` ([16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L451)) cites [test_pg_schema_boundaries.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/tests/test_pg_schema_boundaries.py). However, `[SB-API-2]`'s `_Implementation mapping_` ([16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L411-L418)) lists only core modules (`simplebroker/_constants.py`, `simplebroker/project.py`, `simplebroker/_project_config.py`, `simplebroker/_key_material.py`, `simplebroker/_broker_session.py`, `simplebroker/db.py`, `simplebroker/_targets.py`) and omits [validation.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/validation.py#L92-L120), [plugin.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/plugin.py#L646-L654), and [runner.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/runner.py#L804-L987).
    - In [16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L719-L727), `[SB-API-5]` specifies D2 (Redis `at_least_once` generator creator-PID ownership across `os.fork()`, abandonment on child `close()`/finalization, and `RuntimeError` on child `next()`), and its `_Verification mapping_` ([16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L740-L742)) cites [test_redis_generator_fork.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_redis/tests/test_redis_generator_fork.py). However, `[SB-API-5]`'s `_Implementation mapping_` ([16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L729-L731)) lists only `simplebroker/sbqueue.py` and `docs/specs/11-delivery.md`, omitting [core.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_redis/simplebroker_redis/core.py#L92-L115).
    - In [17-ops.md](file:///Users/van/Developer/simplebroker/docs/specs/17-ops.md#L258-L283), `[SB-OPS-7]` specifies PostgreSQL target-lifetime cleanup boundaries and cites [test_pg_ownership.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/tests/test_pg_ownership.py#L531-L576) in `_Verification mapping_`, but its `_Implementation mapping_` ([17-ops.md](file:///Users/van/Developer/simplebroker/docs/specs/17-ops.md#L270-L274)) lists only SQLite/CLI modules (`simplebroker/_backends/sqlite/plugin.py`, `simplebroker/_backends/sqlite/validation.py`, `simplebroker/cli.py`), omitting [plugin.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/plugin.py).
  - **Suggested disposition:** Append the owning extension module paths to the `_Implementation mapping_` blocks of `[SB-API-2]`, `[SB-API-5]`, and `[SB-OPS-7]`.

- **C2 (P3 — Optional / Docstring drift): [`SQLRunner`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L94-L100) protocol docstring in [simplebroker/_runner.py](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L97) still says `Must be fork-safe (recreate connections after os.fork())` without qualifying SQLite.**
  - **Evidence:**
    - [simplebroker/_runner.py](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L97) states `- Must be fork-safe (recreate connections after os.fork())`.
    - Under `[SB-API-11]` ([16-python-library-api.md](file:///Users/van/Developer/simplebroker/docs/specs/16-python-library-api.md#L1456-L1485)) and [`SQLiteRunner.get_connection`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L337-L338), [`SQLiteRunner`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L194-L214) is the built-in [`SQLRunner`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L94) implementation and only recreates connections after `os.fork()` when zero managed SQLite connections were live in the parent at `os.fork()`; otherwise [`_recover_after_fork_if_needed`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L284-L320) abandons inherited state and [`ensure_sqlite_allowed`](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L149-L152) refuses child SQLite access with `RuntimeError`.
  - **Suggested disposition:** Update [simplebroker/_runner.py](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L97) to `- Must be fork-safe (abandon inherited state without touching parent locks; recreate connections or refuse per [SB-API-11])`.

---

### Combined Review Analysis (Questions 1–7)

#### 1. Cross-Slice Consistency
- **Coherent fork contract across SQLite, Redis, and PostgreSQL:**
  - **SQLite (`[SB-API-11]`):** Process-wide child refusal (`RuntimeError`) when any managed SQLite connection was live in the parent at `os.fork()`, regardless of whether the child uses an inherited handle or constructs a fresh [`Queue`](file:///Users/van/Developer/simplebroker/simplebroker/sbqueue.py#L77)/[`BrokerDB`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1930)/[`BrokerSession`](file:///Users/van/Developer/simplebroker/simplebroker/_broker_session.py) on any SQLite path or `:memory:`. Clean forks (zero live SQLite connections at `os.fork()`, or `spawn`/clean `forkserver`) remain allowed.
  - **Redis (`[SB-API-5]`):** Inherited [`BrokerCore`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_redis/simplebroker_redis/core.py#L588)/[`Queue`](file:///Users/van/Developer/simplebroker/simplebroker/sbqueue.py#L77) recovery in a forked child remains supported, while `at_least_once` generators ([`_claim_batch_generator`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_redis/simplebroker_redis/core.py#L1391-L1486) and [`move_generator`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_redis/simplebroker_redis/core.py#L1622-L1717)) are bound to `creator_pid = os.getpid()`: child `close()`/GC abandons local generator state without running [`_release_pending_claim`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_redis/simplebroker_redis/core.py#L103-L115) or touching `self._lock`, and child `next()` raises `RuntimeError` even after `core._check_fork_safety()` has updated `core._pid`.
  - **PostgreSQL (`[SB-API-11]`):** Retains pool replacement on fork (`_owner` PID check) and raises `RuntimeError("SQL connections cannot be shared across processes. Create a new Queue/BrokerDB instance in the child process.")` only when an inherited [`DBConnection`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L943) or unrecovered [`BrokerCore`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1856) is used directly across processes; creating a fresh PostgreSQL [`Queue`](file:///Users/van/Developer/simplebroker/simplebroker/sbqueue.py#L77)/[`BrokerDB`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1930)/[`BrokerSession`](file:///Users/van/Developer/simplebroker/simplebroker/_broker_session.py) in the child remains supported.
- **Error-message isolation:**
  - [`DBConnection._ensure_shared_process_owner`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1010-L1019) and [`BrokerCore._check_fork_safety`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1856-L1865) explicitly branch on `self._backend_plugin.name == "sqlite"`. PostgreSQL callers never receive the SQLite-specific `"Creating a new Queue/BrokerDB in this child does not make SQLite safe"` message, and SQLite callers never receive the misleading `"Create a new Queue/BrokerDB instance in the child process"` guidance.
- **Cross-slice test and doc alignment:**
  - In [test_core_persistence_transition_tables.py](file:///Users/van/Developer/simplebroker/tests/test_core_persistence_transition_tables.py#L766-L768), [`_assert_timestamp_fork_reset`](file:///Users/van/Developer/simplebroker/tests/test_core_persistence_transition_tables.py#L758-L779) closes `core._runner` before `os.fork()` so `SM-TIMESTAMP-GENERATOR`'s `FORK_RESET` transition exercises the supported clean-fork path under `[SB-API-11]`.
  - No slice leaves stale descriptions of another slice's prior behavior across [README.md](file:///Users/van/Developer/simplebroker/README.md), [docs/agent-kernel.md](file:///Users/van/Developer/simplebroker/docs/agent-kernel.md), [docs/guides/python.md](file:///Users/van/Developer/simplebroker/docs/guides/python.md), [docs/guides/backends.md](file:///Users/van/Developer/simplebroker/docs/guides/backends.md), or the implementation docs.

#### 2. Contract Completeness
Every enumerable contract element touched by R1–R7 has a firing test:
- **R1 (`[SB-API-11]` SQLite fork admission):**
  - All 10 operational [`SQLiteRunner`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L194) entries ([`get_connection`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L334), [`run`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L350), [`owned_connection`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L406), [`_settings_connection`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L322), [`begin_immediate`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L461), [`commit`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L468), [`rollback`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L472), [`setup`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L494), [`setup_with_stop_event`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L502), [`run_exclusive_setup_conditionally_with_stop_event`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L642)) plus [`close`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L476) and [`is_setup_complete`](file:///Users/van/Developer/simplebroker/simplebroker/_runner.py#L628) under a parent-held `_operation_lock`: tested in [`test_inherited_runner_entries_refuse_before_parent_locks`](file:///Users/van/Developer/simplebroker/tests/test_sqlite_fork_admission.py#L633-L651).
  - Parent-held accounting and runner locks (`registry`, `_connections_lock`, `_setup_lock`, `_transaction_condition`): tested in [`test_inherited_accounting_and_runner_locks_do_not_wedge_child`](file:///Users/van/Developer/simplebroker/tests/test_sqlite_fork_admission.py#L655-L660).
  - All 4 managed opening routes (`runner`, `validate`, `setup_phase`, `check_version`) plus pre-open `sqlite3.connect` audit barrier: tested in [`test_managed_sqlite_open_routes_are_barred_before_connect`](file:///Users/van/Developer/simplebroker/tests/test_sqlite_fork_admission.py#L621-L629).
  - `SM-SQLITE-FORK-ADMISSION` transition table (`unsafe:write`, `unsafe:injected`, `unsafe:memory`, `unsafe:nested`, `clean:never-used`, `clean:ephemeral`, `clean:explicit`, `clean:gc`, `clean:cycle`, `clean:foreign-gc`, `clean:cantopen`): registered in [state_machine_manifest.py](file:///Users/van/Developer/simplebroker/tests/state_machine_manifest.py#L67) and fired in [test_sqlite_fork_admission.py](file:///Users/van/Developer/simplebroker/tests/test_sqlite_fork_admission.py#L579-L617).
  - `spawn`, `forkserver`, and preloaded-`forkserver` compatibility, `failed-close`, `ambiguous-open`, `nested-cantopen`, `construction-gc:foreign`, `construction-gc:resurrect`, `child-gc`, and `default-schedule`: tested in [test_sqlite_fork_admission.py](file:///Users/van/Developer/simplebroker/tests/test_sqlite_fork_admission.py#L664-L720).
- **R2 (`[SB-CLI-2]` CLI first-use target race):**
  - All 7 read-like/maintenance commands (`read`, `peek`, `exists`, `move`, `list`, `stats`, `--vacuum`) across all 3 first-writer phases (`zero`, `header`, `commit`), caller-created 0-byte file initialization, non-SQLite junk/directory rejection (`EXIT_ERROR`), and non-relaxation of `broker init` / `broker --cleanup` on 0-byte files: tested in [test_cli_first_use.py](file:///Users/van/Developer/simplebroker/tests/test_cli_first_use.py#L21-L168) with real subprocesses via [cli_first_use_probe.py](file:///Users/van/Developer/simplebroker/tests/helper_scripts/cli_first_use_probe.py).
- **R4 (`[SB-CLI-1]` `broker watch` output failure):**
  - `BrokenPipeError` (`EXIT_SUCCESS = 0`) vs non-pipe `OSError` (`EIO` on PTY hangup -> `EXIT_ERROR = 1` with zero post-failure writes/commits) across `consume`, `peek`, `move` and `plain`/`json`, dead `stdout`+`stderr`, `stderr` failure during `terminal-flush` and `banner`, closed startup fds `1`/`2` before target resolution and direct [`cmd_watch`](file:///Users/van/Developer/simplebroker/simplebroker/commands.py#L1395), closed `sys.stdout`, caller-owned stream preservation, cleanup-failure `__notes__` without cause/context cycles, and `SM-CLI-WATCH` `CALLBACK_ERROR_TERMINAL`: tested in [test_cli_watch_output_failure.py](file:///Users/van/Developer/simplebroker/tests/test_cli_watch_output_failure.py#L49-L344) and [test_watcher_transition_tables.py](file:///Users/van/Developer/simplebroker/tests/test_watcher_transition_tables.py#L800-L979).
- **R5 (`[SB-API-5]` Redis generator fork ownership):**
  - Both `claim` and `move` generators across `exit`, `close`, `locked_close` (parent holding `core._lock`), `close` after core recovery (`recover=True`), `advance` mid-batch and on `final_row`, `fresh` child generator creation, and public [`Queue`](file:///Users/van/Developer/simplebroker/simplebroker/sbqueue.py#L77) (`persistent` and `ephemeral`): tested in [test_redis_generator_fork.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_redis/tests/test_redis_generator_fork.py#L183-L262).
- **R6 (`[SB-API-2]` PostgreSQL schema boundaries):**
  - 62-byte and 63-byte valid schemas preserved, 64-byte (`"T" * 64`) and `"public"` schemas rejected with `ValueError` before resource acquisition across all 17 entrances (`require`, `verify`, `init`, `create_runner`, `initialize`, `validate`, `inspect`, `cleanup`, `runner`, `waiter`, `multi_waiter`, `plugin_waiter`, `plugin_multi_waiter`, `structural_core`, `structural_waiter`, `structural_multi_waiter`, `lazy_queue_waiter`), live 63-byte target protection against 64-byte `read`/`write`/`cleanup`, and 63-byte sibling schema storage + `NOTIFY` isolation: tested in [test_pg_schema_boundaries.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/tests/test_pg_schema_boundaries.py#L32-L202).
- **R3 (`[SB-OPS-7]`) and R7 (`[SB-API-6]`):**
  - Closed-owner [`BrokerSession`](file:///Users/van/Developer/simplebroker/simplebroker/_broker_session.py) -> CLI `--cleanup`, `init`, `alias add` -> fresh [`BrokerSession`](file:///Users/van/Developer/simplebroker/simplebroker/_broker_session.py) positive boundary test in [test_pg_ownership.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/tests/test_pg_ownership.py#L531-L576).
  - Real PostgreSQL `options="-c idle_session_timeout=10000"` quiet terminal expiry (3 generations) vs healthy `NOTIFY` recovery (4+ losses across 5+ generations with `retry_count == 1`) in [test_pg_idle_session_recovery.py](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/tests/test_pg_idle_session_recovery.py#L113-L201).

#### 3. Public-Contract Honesty
- [CHANGELOG.md](file:///Users/van/Developer/simplebroker/CHANGELOG.md#L8-L56) records every user-visible behavior change under `[Unreleased]` (`### Fixed`, `### Changed`, `### Documented`).
- Specifically, [CHANGELOG.md](file:///Users/van/Developer/simplebroker/CHANGELOG.md#L45-L50) explicitly names the `8.8.2` alias-version counter mechanism (`meta.alias_version` starting at `0` in a recreated schema and reusing version `1` after the first alias write, vs `8.8.1`'s `generate_timestamp()` refresh) and states the maintenance requirement to close all [`BrokerSession`](file:///Users/van/Developer/simplebroker/simplebroker/_broker_session.py)/[`BrokerCore`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1687)/[`BrokerDB`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1930)/[`Queue`](file:///Users/van/Developer/simplebroker/simplebroker/sbqueue.py#L77)/watcher owners before `broker --cleanup` or `cleanup_target()`.
- [README.md](file:///Users/van/Developer/simplebroker/README.md#L667) (and lines [778](file:///Users/van/Developer/simplebroker/README.md#L778), [876](file:///Users/van/Developer/simplebroker/README.md#L876), [952](file:///Users/van/Developer/simplebroker/README.md#L952)) and [docs/agent-kernel.md](file:///Users/van/Developer/simplebroker/docs/agent-kernel.md#L199) (and lines [294](file:///Users/van/Developer/simplebroker/docs/agent-kernel.md#L294), [390](file:///Users/van/Developer/simplebroker/docs/agent-kernel.md#L390)) accurately restate the winning specs without contradiction, and [test_doc_gates.py](file:///Users/van/Developer/simplebroker/tests/test_doc_gates.py#L516-L558) enforces those synchronization points.

#### 4. Spec / Implementation / Test Alignment
- [06-process-session-core-ownership.md](file:///Users/van/Developer/simplebroker/docs/implementation/06-process-session-core-ownership.md#L569-L670) and [07-complexity-and-state-machine-map.md](file:///Users/van/Developer/simplebroker/docs/implementation/07-complexity-and-state-machine-map.md#L387-L434) match the shipped implementation in [simplebroker/_sqlite_fork.py](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py), including the revised failed-open rule in [`_ConnectionFactory`](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L42-L93) (pre-incrementing `_LIVE_CONNECTIONS` before `sqlite3.connect()` and retiring a failed constructor's slot only when finalization completed on the constructing thread and the weakref is dead, or on non-reentrant `SQLITE_CANTOPEN` before `sqlite3.connect` audit hook firing).
- The only alignment gap is **C1** (adding the extension module paths to the `_Implementation mapping_` blocks of `[SB-API-2]`, `[SB-API-5]`, and `[SB-OPS-7]`).

#### 5. Plan Record
- In [2026-11-11-runtime-boundaries-and-failure-handling-plan.md](file:///Users/van/Developer/simplebroker/docs/plans/2026-11-11-runtime-boundaries-and-failure-handling-plan.md), the Deviation Log ([lines 1079–1114](file:///Users/van/Developer/simplebroker/docs/plans/2026-11-11-runtime-boundaries-and-failure-handling-plan.md#L1079-L1114)), Execution Log ([lines 1116–1276](file:///Users/van/Developer/simplebroker/docs/plans/2026-11-11-runtime-boundaries-and-failure-handling-plan.md#L1116-L1276)), task checkboxes (Tasks 0–5 checked; Task 6 open by design for Weft companion adoption and coordinated release), Status Index row in [docs/plans/README.md](file:///Users/van/Developer/simplebroker/docs/plans/README.md#L32) (`active`), and verbatim slice reviews ([lines 1278–1544](file:///Users/van/Developer/simplebroker/docs/plans/2026-11-11-runtime-boundaries-and-failure-handling-plan.md#L1278-L1544)) are truthful, complete, and supported by the recorded host-verified evidence.

#### 6. Regression Risk to Primary Downstream (Weft)
Four changes can break an existing caller that relied on previously unguarded behavior; each is a deliberate, documented narrowing:
1. **D1 (`[SB-API-11]` SQLite child refusal after fork with live parent SQLite handle):** Any caller on the POSIX `fork` multiprocessing start method that holds an open SQLite [`Queue`](file:///Users/van/Developer/simplebroker/simplebroker/sbqueue.py#L77)/[`BrokerDB`](file:///Users/van/Developer/simplebroker/simplebroker/db.py#L1930)/[`BrokerSession`](file:///Users/van/Developer/simplebroker/simplebroker/_broker_session.py) in the parent across `fork()` and opens a fresh SQLite instance in the child now receives `RuntimeError`. This is the exact failure mode surfaced in the recorded Weft smoke run (2 real-fork tests in `tests/core/test_manager_lifecycle.py` and `tests/core/test_resource_limits.py`) and is tracked by open Task 6 before release.
2. **D4 (`[SB-API-2]` PostgreSQL schema >63 ASCII bytes and direct-route `"public"` rejection):** Callers passing `schema` >63 bytes (previously silently truncated by PostgreSQL to 63 bytes, risking cross-tenant collision) or passing `schema="public"` directly to [`PostgresRunner`](file:///Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg/runner.py#L975)/waiters now fail immediately with `ValueError`.
3. **D3 (`[SB-CLI-1]` [`cmd_watch`](file:///Users/van/Developer/simplebroker/simplebroker/commands.py#L1395) non-pipe output failure):** Programmatic callers of [`cmd_watch`](file:///Users/van/Developer/simplebroker/simplebroker/commands.py#L1395) (or CLI `broker watch` attached to a hung-up PTY) now stop on the first non-pipe output `OSError` (`EXIT_ERROR = 1` in [`main`](file:///Users/van/Developer/simplebroker/simplebroker/cli.py#L1557)) instead of logging and continuing to consume/move messages into a dead sink.
4. **D2 (`[SB-API-5]` Redis `at_least_once` generator across `os.fork()`):** A forked child advancing a parent-created Redis `claim_generator()`/`move_generator()` now raises `RuntimeError` instead of settling the parent's reservation, and child `close()`/GC is a local no-op instead of rolling back the parent's claim.

#### 7. Unnecessary Machinery (Owner Decisions)
- **Owner Decision — Slice Finding S3 ([`_ConnectionFactory`](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L42-L93) / [`_ConstructionAttempt`](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L27-L39) in [simplebroker/_sqlite_fork.py](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L27-L93)):**
  - [`_ConnectionFactory`](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L42-L93) (~65 lines of production code plus 3 white-box probes in [test_sqlite_fork_admission.py](file:///Users/van/Developer/simplebroker/tests/test_sqlite_fork_admission.py#L417-L535)) exists so that a failed `sqlite3.connect()` call (such as `SQLITE_CANTOPEN` on a missing directory or unopenable path) can decrement its pre-counted `_LIVE_CONNECTIONS` slot and still remain fail-closed if a custom `sys.addaudithook` raises after `sqlite3.connect` and either resurrects the partially constructed `sqlite3.Connection` or finalizes it on another thread.
  - Since custom `sys.addaudithook` callbacks and reentrant forks are already outside the `[SB-API-11]` guarantee, there are two simpler alternatives if the owner wants less machinery:
    1. **Simplest fail-closed:** Unconditionally increment `_LIVE_CONNECTIONS` before `sqlite3.connect()` and decrement only when `sqlite3.connect()` returns a `Connection` that later completes `close()`/finalization (a failed `sqlite3.connect()` permanently marks the process as having touched SQLite before fork).
    2. **Simple `CANTOPEN` recovery without audit-hook resurrection machinery:** Decrement in the `except BaseException:` block around `sqlite3.connect()` when no `Connection` was returned to the caller, relying on `[SB-API-11]`'s existing exclusion of custom audit-hook injection.
  - Retaining [`_ConnectionFactory`](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py#L42-L93) as written is also safe and well-isolated in [simplebroker/_sqlite_fork.py](file:///Users/van/Developer/simplebroker/simplebroker/_sqlite_fork.py).

---

### Verdict
PASS WITH FINDINGS

### Verification Limits
This review was performed strictly read-only using native file-reading and search tools without running git, pytest, linters, or subagents. All behavioral, cross-slice, and contract-completeness conclusions are based on static inspection of the production code, tests, specs, and documentation in the working tree alongside the host-verified test and gate results supplied in the prompt.
~~~
