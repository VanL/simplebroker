# Process-Session Core Ownership

Status: current

## Contract

The public packaging surface for targets, handles, and embedding is
`docs/specs/16-python-library-api.md` `[SB-API-*]` (see also README
“Embedding SimpleBroker in Your Project” for orientation). Persistent queue
handles for one resolved target share process-local backend state, while
transient handles do not.

This document explains the internal ownership boundary that preserves that
behavior. It does not define a public extension interface.

## Why the Boundary Exists

Process-session state and backend construction have different reasons to
change:

- session identity, refcounts, operation admission, drain, and thread-local
  caching are backend-independent lifecycle policy;
- `BrokerDB`, `BrokerCore`, runner allocation, checkout rollback, and concrete
  close methods are backend construction policy.

Keeping both in `simplebroker/_broker_session.py` required local imports from
`simplebroker/db.py`, while `db.py` already imported the session registry. It
also duplicated runner construction across direct and SQL-backed branches.

The private factory seam keeps each policy with its concrete owner and makes
the import graph one-way:

```text
simplebroker/db.py -> simplebroker/_broker_session.py
```

`_broker_session.py` must not import or dynamically resolve `db.py`.

## Ownership

| Owner | Responsibilities | Must not own |
|-------|------------------|--------------|
| `simplebroker/_broker_session.py` | Canonical session spec/key, registry identity and refcount, admission state, active-operation and active-core-creation drains, per-thread core cache | Concrete core classes, backend dispatch, runner state |
| Private adapter in `simplebroker/db.py` | SQLite/direct/SQL core construction, runner publication, thread-checkout lease rollback, concrete core close, final runner close | Registry identity, refcounts, operation depth, thread-local session cache |
| Backend plugin and runner | Concrete backend operations, connection/pool implementation, backend cleanup hooks | Process-session sharing policy |

The factory Protocol is private and owned by its caller in
`_broker_session.py`. The production adapter satisfies it structurally.
Backend packages do not implement it, and it is not exported through
`simplebroker` or `simplebroker.ext`.

### Lifecycle scope and activity waiters

Queue target binding uses Redis's existing `init_backend` normalization to
resolve Config-derived namespaces before any runner or listener is created.
Only normalized options are retained; the supplied target string and project
metadata stay intact. The detached descriptor supplies storage, move rejection
and waiter grouping. Whole Config values still govern session sharing, but
unrelated Config tuning is not move identity. Other plugins keep their existing
initialization timing; their target-enrichment rules are not interchangeable.

Lifecycle verbs describe the receiver's ownership scope, not a global type
hierarchy. `close()` releases resources owned by a handle or runner.
`shutdown()` is an optional stronger runner operation when that receiver owns
shared or process-wide substrate; an implementation may alias the operations
when the scopes coincide. `close_owned_runner()` prefers callable `shutdown()`
and falls back to `close()`, but only at a SimpleBroker-owned runner boundary.
An explicitly injected runner remains caller-owned. For SQL-backed cores,
`_BorrowedRunner` encodes that boundary by making both destructive lifecycle
verbs, `close()` and `shutdown()`, no-ops while continuing to delegate
operational runner methods. This keeps the SQL borrowed-wrapper teardown paths
ownership-safe without adding an owner flag to the public runner surface.

### Explicit caller-thread release

The process session keeps one cached core per thread. Closing a persistent
Queue releases its registry lease only. It does not infer that the calling
thread is finished with the shared cache from the set or order of Queue closes.
Final session shutdown still drains active operations, disposes every cached
core, and closes the shared factory.

Before session end, only `Queue.cleanup_connections()` or
`BrokerSession.recycle_thread()` explicitly asks the process session to release
the calling thread's cached core. Other threads' caches are unaffected. If the
thread is inside a Queue operation, one boolean thread-local cleanup request is
deferred until the outermost operation exits. The Queue remains usable and
reacquires a core on its next operation.

### Public session handle

`BrokerSession` is a lifetime façade over the existing registry, not a second
registry or an operation surface. The handle acquires one lease for a resolved
target and Config snapshot. Queues minted through `session.queue(name)` use the
same target and snapshot, acquire their ordinary persistent-Queue leases, and
remain in the handle's scope inventory until close. `session.connection()`
uses a temporary shared `DBConnection` lease so connection-level work reuses
the same runner or pool without joining that Queue inventory.

The inventory is the owning edge. A minted Queue holds only a weak reference
back to the handle for `queue.session`, so dropping the handle can run its
lease-only finalizer immediately instead of waiting for cyclic garbage
collection. A strong Queue-to-handle edge would add no ownership guarantee:
the handle already retains every minted Queue, while a retained Queue is
allowed to outlive the handle and reacquire an ordinary persistent-Queue lease
through a new process session when reused.

The calling thread must close all iterators and exit all Queue or session
connection contexts using the same process-session key before closing the
handle. The active-operation depth belongs to the key and thread, so an
operation opened through a sibling handle or a directly constructed persistent
Queue also blocks close on that thread. Close rejects such an operation before
changing scope state; otherwise it admits no new
queues or connections, releases the calling thread's cache, closes every
minted Queue, and drops the handle lease. Queue and process session calls occur
outside the handle lock. Ordinary failures do not prevent later independent
cleanup steps. On context exit, an ordinary cleanup failure is attached to an
already propagating body exception instead of replacing it; a cleanup
`BaseException` retains priority. Interruptions preserve completed idempotent
steps and leave admission closed. The finalizer releases only the handle lease
because the collector thread does not own another thread's cache.

The handle is strictly process-local. Every active method rejects an inherited
handle before acquiring its lock, for every backend. Inherited `close()` is a
silent no-op and detaches the child copy of the finalizer, so child cleanup
cannot release parent resources. A child constructs a new handle; Queues minted
by the parent retain their separate existing backend-specific fork policy.

Idle cleanup takes one raw active-operation hold before detaching the core and
releases that hold exactly once in its surrounding `finally`. Claiming first
publishes the core into an invocation-owned carrier, then removes it from
reusable TLS and the session-owned set. This prevents terminal timeout from
closing the same core while its disposal is active. An unfinished claim is
restored to session ownership while the session remains live, but never
re-adopted after terminal shutdown. A late failure after terminal timeout is
therefore surfaced to the disposer and is not retried by the closed session.

Ordinary failures participate in exception-note priority; non-ordinary
`BaseException` values propagate after live-session ownership is restored.
After terminal timeout, a late operation exit clears its local bookkeeping
without disposing the core that terminal shutdown already claimed.

Operation acquisition publishes two ownership facts: session operation depth
and the manager's operation-session stack entry. Rollback snapshots both. A
failure before stack publication releases only a newly acquired session
operation; a failure after publication pops and releases that exact operation.
This prevents a failed nested acquisition from consuming its outer operation.

Shared non-SQLite SQL cores carry a private successful-release latch inside
`BrokerCore.close()`. Explicit repeated close and `__del__()` therefore cannot
surrender a replacement core's runner lease. A session-managed core calls a
backend's thread-checkout release hook only when construction acquired that
lease; otherwise final factory shutdown remains the sole owner of runner close.
SQLite keeps its
existing tracked-snapshot retry behavior: successful connections leave the
snapshot; failed closes remain tracked.

### Suspended closeable Queue operations

`Queue.read_generator()`, `Queue.peek_generator()`,
`Queue.move_generator()`, and `Queue.stream_messages()` are outer Python
generators whose `Queue.get_connection()` contexts stay open while their
delegated backend iterators are suspended. The high-level
`all_messages=True` read, peek, and move views return those generators or a
close-forwarding result-shaping generator. That outer Queue seam owns public
iterator cleanup; backend `BrokerConnection` generator implementations remain
ordinary iterators and do not acquire a second public lifecycle interface.

The first advancement enters the context on the caller's thread. Exhaustion,
an advancement failure, or explicit close unwinds it on that same thread. For
a persistent Queue, context exit ends the process-session operation. Its
thread-local core remains cached unless Queue close or explicit connection
cleanup requested deferred release. PostgreSQL returns an operation checkout
unless an open transaction still needs it. For a no-runner
ephemeral Queue, it closes the operation-owned `DBConnection` and releases its
private core. For a Queue with an injected runner, it invokes the lexical
operation release hook but retains the Queue-owned borrowed core until
`Queue.close()`; neither step closes or shuts down the caller-owned runner.

This split is why the public promise is synchronous Queue-operation exit and
owned cleanup invocation, not unconditional physical connection destruction.
It also makes close thread-affine: the process-session operation stack is
thread-local, so this design does not transfer a suspended operation to a
foreign cleanup thread. Peek traversal is `[SB-DELIVERY-4]`; ownership for
read, move, and stream iterators is `[SB-DELIVERY-6]`; the common public shape
is `[SB-API-5]`. This section records the implementation reason for those
contracts.

### Watcher finalization boundary

A watcher weak-reference finalizer captures only a weak reference. During
ordinary collection that reference is dead and the callback takes no action.
During interpreter-exit finalization, a still-live watcher is stopped through
its normal serialized lifecycle, preserving daemon-thread shutdown without
calling Queue thread-local cleanup directly. A running watcher releases its
own thread's cached core in its run `finally`, regardless of whether the Queue
was supplied by the caller; the Queue reacquires on its next use on that
thread. An idle stop closes the strategy and an internally created Queue's
lease, but does not release the stop caller's cached core. A Queue supplied by
the caller remains caller-owned: the watcher never closes its lease.

### Trusted first-party operational probes

`BrokerCore._run_backend_probe()` is a private seam for a first-party SQL
extension to materialize one read through the core's existing runner. It is
not raw SQL for embedders and is deliberately absent from the public
`BrokerConnection` protocol and backend API handshake.

The seam checks fork and active-batch state, acquires the poison-aware core
lock, and materializes the runner result inside the normal retry call before
releasing that lock. Read-only SQL still needs this serialization: the lock
owns connection overlap, retry state, poison publication, close interaction,
and at-least-once batch isolation, not only write transactions.

`simplebroker_pg.get_connection_stats()` enters `Queue.get_connection()` and
uses this seam. A target-resolved persistent Queue therefore reuses its
thread-local process-session core and borrows a PostgreSQL checkout. An ephemeral
Queue owns and releases one operation connection. An injected runner is
supported through its borrowed core, but `persistent=True` does not strengthen
the runner owner's checkout contract. Psycopg pool statistics are not a
substitute for the PostgreSQL catalog probe because each process owns its own
pool and cannot see other processes or unrelated clients.

The same operation-scoped rule applies to sidecars on a persistent Queue or
`BrokerSession`. A non-transactional sidecar statement borrows and returns a
checkout through the shared runner. A transactional sidecar block retains that
checkout because commit and rollback must use the same PostgreSQL session. An
ephemeral Queue instead owns a runner for the sidecar session and closes it on
exit. Embedders must use the public sidecar surface and bound transactional
blocks; independent raw connections bypass session ownership, while abandoned
transactions on a shared session can consume every bounded checkout and hold
later operations until the pool timeout.

### Project-scoped service bootstrap coordination

Project-scoped PostgreSQL and Redis targets reuse `PhaseLockService` with the
resolved `.broker.toml` as the coordination target. The config file is the
shared local identity for a service target, just as the database path is the
identity for SQLite setup. This serializes first initialization and migration
without extending backend transactions across catalog inspection and schema
DDL. Explicit service targets have no config-path identity and retain direct,
backend-owned idempotent initialization.

The completion marker remains a cache hint. Before it can skip setup, the
backend must validate a current initialized target. PostgreSQL therefore
distinguishes `verify_initialized=True` (current version and shape) from the
`False` admission used by `initialize_target()` and connection setup, which
must still let an older owned schema reach migration. A restored older schema
cannot borrow the config file's newer marker.

The service keeps `PhaseLockService`'s platform policy. POSIX may accept a
validated marker without taking the advisory lock, preserving the normal CLI
startup fast path. Windows acquires and releases the lock before trusting the
marker, giving marker observation a happens-after edge on the prior owner.
Forcing the Windows policy on POSIX would serialize every project CLI startup
and was rejected after it caused PostgreSQL CLI timeouts. Controlled
PostgreSQL and Redis CLI A/B runs with the native policy remained within trial
noise.

An `ActivityWaiter` sits below that boundary. It owns a backend activity
registration or composite registration, not the runner, pool, listener
substrate, or process session. It therefore exposes only `close()`. Terminal
state lives on the waiter itself and is set before cleanup. This keeps direct
defensive close calls safe without a caller-side identity ledger, including
after the first close reports cleanup failure. PostgreSQL and Redis registry
release remains a separate owned cleanup action from listener unregister, so
both are attempted after an ordinary unregister failure when safe. The exact
public failure order is owned by `[SB-API-6]`; backend API v6 makes that
obligation enforceable at plugin resolution.

The PostgreSQL and Redis listener registries acquire a counted reference under
their registry lock before waiter registration can begin. Release removes the
entry only when the last reference is surrendered, then closes the detached
listener outside the registry lock. This closes the lookup/register gap: a new
waiter cannot receive a listener concurrently selected for final close.

### Fork recovery before process-owned locks

Every process-owned state holder checks its PID before acquiring an inherited
lock that protects that state. On change, the owner replaces all of its locks
and process-bound resources first, then continues in the child. This ordering
applies to runner setup, timestamp cache reads and refresh, Redis core
initialization and maintenance, and the backend activity registries. Checking
after lock acquisition is not recovery: the child may already be waiting on a
lock held by a vanished parent thread.

The process-session registry checks PID before acquire, release, or shutdown
takes its global lock. Child recovery retains the inherited entry graph without
finalizing backend resources, then creates an empty registry and a fresh lock.
Child shutdown touches only child-owned entries. The retry hot-loop diagnostic
guard also resets its lock and counters before acquisition; its warning remains
process-wide. Both use the existing single-threaded first-child-access rule.

A persistent Queue's DBConnection also checks its retained session-key PID
before project setup or session admission. It reads the registry's `_getpid`
seam, so the ownership fact and current-process comparison cannot diverge.
SQL acquisition rejects the inherited manager before retry or lock acquisition.
Direct backends such as Redis acquire child-owned state through the registry,
then reset manager-local project setup and operation bookkeeping. Cleanup and
release ignore inherited operation leases; close uses the registry's stale-PID
release path. The registry retains old graphs without finalization. This does
not transfer parent generators or change injected runners' backend fork policy.

Transaction-owner progress belongs to the runner, not the process session.
When several thread-local cores share one runner, their separate core locks do
not serialize a transaction. The runner must keep a successful transaction
owner's path to `commit()` or `rollback()` clear. `SQLiteRunner` does this with
condition-guarded admission: foreign operations wait without holding the
per-call operation lock, while the owner continues to use its thread-local
connection. Deliberately shared SQLite reads and writes both wait behind an
active transaction, and that wait is bounded by the configured SQLite busy
timeout.

PostgreSQL keeps one bounded pool per process-session key. Each operation
borrows a checkout; transaction and iterator boundaries keep that checkout
until settlement, after which it returns to the pool. Idle thread-local cores
therefore consume no pool slots, while active threads can make independent
progress within the bound. The default pool maximum is 3 and checkout timeout is
30 seconds. The activity listener owns one separate connection, so one active
process-session target uses at most four PostgreSQL connections by default.
Deployment sizing starts with the database-wide connection budget and maximum
broker process count, with capacity reserved for other clients. Pool exhaustion
is translated to SimpleBroker `OperationalError` and reported instead of
sharing another operation's connection or leaking a driver-pool exception.
Redis
uses a direct core and its session-owned command pool; it does not enter the
SQL runner transaction protocol.

This checkout lifecycle stays below the public Queue and `BrokerSession`
surfaces. Ordinary callers do not check physical connections in or out and do
not recycle thread caches to restore PostgreSQL pool capacity. The visible
constraints are bounded simultaneous operations, the checkout timeout, and the
existing obligation to close suspended iterators.

PostgreSQL's explicit lease registry is reserved for maintenance that needs
connection identity across several operations, such as its session-level
vacuum advisory lock. Entries are keyed by `threading.Thread`, so a successor
cannot inherit an earlier owner's pin. Failure discard removes only the
owner's uncertain checkout while retaining logical lease depth. Fork recovery
abandons the inherited pool and clears the registry. Terminal shutdown detaches
remaining explicit pins before returning them, preventing a duplicate return.

`SQLiteRunner.close()` observes the same admission boundary. An explicit close
behind a foreign live owner can wait through the configured busy timeout and
raise the retryable admission error without closing other tracked connections.
First-party best-effort shutdown paths suppress that bounded cleanup failure;
explicit callers must handle it. A foreign orphan is still restart-required.

The process session's bounded drain is not a deadline for all shutdown work.
It closes admission and waits before disposing cached cores, but core disposal
still takes each core's operation lock and can wait for a slow operation. The
caller therefore closes iterators before their owner. PostgreSQL operation
checkout ownership does not add abandoned-iterator recovery or make concurrent
standalone runner shutdown a supported cancellation mechanism.

Runner close is resource-scoped, not terminal. At its linearization point,
`SQLiteRunner.close()` advances the connection generation and snapshots all
connections then tracked by that runner. It closes that owned snapshot and
keeps failed closes tracked so cleanup can be retried safely. The runner itself
remains reusable: an operation linearized later may acquire a distinct
connection in the new generation, including when its acquisition overlapped
the close but registered after the snapshot. Terminal operation admission
belongs to the process session and private factory. Their closed states prevent
new core or runner publication; adding a permanent `_closed` latch to the
runner would assign that ownership to the wrong layer and break intentional
close-then-reuse behavior.

Fork recovery deliberately retains inherited SQLite connections, PostgreSQL
pools, and Redis client/pool references in the child. Dropping a reference can
run inherited cleanup and enter a process-owned lock held by a vanished parent
thread, recreating the hazard recovery is meant to avoid. Redis `close()` and
the PostgreSQL finalizer therefore check PID first and abandon inherited
resources without closing them. Sibling forks cannot grow the parent's
copy-on-write retained lists; only a nested-fork lineage can accumulate
references. A cap or cleanup policy would therefore trade a hypothetical
nested-lineage memory cost for an unsafe finalization path; either requires
measured harmful growth and a proven close-free disposal mechanism. A warning
is also unjustified until that growth is observed.

## Acquisition

`DBConnection.__init__()` and `DBConnection._ensure_shared_session()` are the
only production acquisition sites. Both pass the same module-level factory
builder. The builder accepts a copied `_SessionSpec`; it is not a bound method
and does not retain a `DBConnection` or its stop event.

The registry resolves one `_SessionSpec` per acquisition. Its `_SessionKey`
selects an existing entry. The registry invokes the builder only when the key
is new, so a repeated acquisition cannot allocate and discard an unused
factory.

Configuration is one read-only Config with uppercase unprefixed keys. The
session key includes all values, including application fields, and an existing
Config is retained without ambient resolution. Naming views and broker-only
projections are absent. Verify `tests/test_process_broker_session.py` and the
shared configuration tests: equal broker fields alone do not justify merging
distinct application configurations.

`serialize_config()` transports resolved values and namespace as JSON, not field
records or process resources ([SB-API-2]). `deserialize_config()` reconstructs
through `resolve_config()` using receiver-owned declarations. This keeps one
field-validation path and avoids importing code selected by payload data.
Receivers establish their own declaration identities and process sessions;
transport never promises shared session identity across processes. Reuse local
field declarations across child handles as within any one process. Verify
`tests/test_config_transport.py` and `tests/test_config_coexistence.py` when
changing this boundary, including real child-process broker use.

For ordinary pickle, Config converts its two mapping proxies to plain dict state
and restores them on reception. Python owns callable and subclass reconstruction;
resolved values are not revalidated. This preserves declaration behavior for
later overrides without adding a separate import registry. JSON remains the
choice for receiver-supplied declarations. Neither path transfers sessions.

Ordinary configuration inputs are detached at capture; acquisition detaches
supported option containers
once. The registry key and the lazy factory both derive from that same detached
snapshot, so later nested caller mutation cannot leave an old key describing
new factory inputs. Key material preserves primitive and container type
distinctions and treats mappings and sets as order-insensitive. Unsupported
opaque values retain process-local object identity through a strong reference.
That fallback may create an extra session for distinct but value-equivalent
objects; it cannot make distinct configuration share backend resources.

`BrokerTarget` snapshots the top level of `backend_options` into an ordinary
dict when the descriptor is constructed. That prevents later mutation of the
caller's source mapping from changing a session target while preserving the
existing shallow nested values, pickling, `dataclasses.replace()`, and direct
mapping mutation compatibility. The JSON transport decoder validates boolean
and optional-path field types exactly; it does not reinterpret truthy payloads.

A `Queue` binds a separate effective target at construction using the same
container snapshot rules. Its persistent session, later ephemeral operations,
move compatibility checks, and waiter arguments therefore describe the same
target even when the original descriptor is edited. `Queue.db_target` returns
a detached reporting value so it cannot expose the internal options. Edited
standalone descriptors can still configure new handles. The acquisition
snapshot remains necessary for direct `DBConnection` consumers.

### SQLite ownership admission

SQLite ownership is checked before SimpleBroker connection setup, schema
bootstrap, or phase-lock sidecars are allowed to write. A
`user.simplebroker.magic` xattr whose value exactly matches the database magic
is authoritative positive evidence and skips the SQL check. SimpleBroker writes
that xattr only after schema setup and magic verification succeed.

When the xattr is absent, unavailable, malformed, or different, admission uses
the runner's ordinary read-write connection to read `meta.magic`. An explicit
foreign value fails construction before SimpleBroker setup. Missing metadata
keeps the established empty/legacy bootstrap behavior. Opening the connection
can perform SQLite's own normal recovery or WAL coordination; that is outside
the invariant. The invariant is specifically that SimpleBroker does not run
its setup or schema writes before checking the stored magic.

The xattr is deliberately only a positive cache. SimpleBroker does not add an
inode fingerprint, generation ledger, or second read-only connection to defend
against an external in-place overwrite that preserves xattrs. That case is an
external ownership violation. On ordinary cache misses, reusing the runner
connection avoids an extra open on a construction path that can be hot.

PostgreSQL sidecar SQL is adapted only when parameters are present. Its qmark
scanner treats quoted tokens, comments, and dollar-quoted bodies as opaque and
maps `??` to a literal question mark. It doubles original percent signs for
psycopg's parameter-template parser; psycopg restores them before PostgreSQL
sees the statement. Parameter-free SQL reaches psycopg byte for byte, while
psycopg remains the owner of bind-count validation.

## Runner Publication

The production adapter serializes runner publication with this private state
machine:

```text
empty -> creating -> ready
  |         |          |
  +---------+----------+-> closed
```

Only the thread that changes `empty` to `creating` calls the backend runner
constructor. The external constructor runs outside the adapter condition.
Other creators wait for `ready` or `closed`.

After construction, the creator reacquires the condition:

- if the factory remains open, it publishes the candidate as `ready`;
- if close won the race, it closes the unpublished candidate and raises the
  normal closed-session error.

`close()` changes any state to `closed`, detaches a published runner, wakes
waiters, and is idempotent. No runner can be published after that transition.

## Core Creation and Shutdown

A cache miss increments `_active_core_creations` under the session condition
after the open-state check. One outer `finally` decrements it after every
outcome, including construction failure, cache publication, close-time
discard, and discard failure.

`close_all()` performs these steps:

1. mark the session closing under the session condition;
2. wait for active operations and core creations against the existing shared
   deadline;
3. mark the session closed and detach cached cores;
4. close detached cores through the factory;
5. close the factory and its published runner.

Potentially blocking core disposal happens outside the session condition. The
deadline bounds the drain wait, not arbitrary backend constructor or close
calls. If the deadline expires, in-flight work is not cancelled:

- factory shutdown is deferred until every admitted core creation finishes, so
  a slow constructor cannot lose its runner underneath it;
- a late unpublished runner candidate closes itself;
- an in-flight checkout rollback continues;
- a core returned to the closing session is discarded rather than cached.

After ownership is detached, an ordinary `Exception` from one core, factory,
or registry-session close does not prevent the remaining safe closes. One
failure remains primary and later failures are retained as diagnostics without
making their incidental order public behavior. A `BaseException` outside
`Exception` keeps propagation priority and may interrupt later cleanup.

For runner-backed cores, a successful checkout stays leased for the cached
core lifetime. If construction fails, the adapter releases that checkout once.
If release also fails, the construction error remains primary and the release
failure is attached as secondary exception evidence.

## Change Guidance

When changing this area:

- keep `_SessionSpec` as the single input to keying and factory construction;
- keep the factory builder module-level and identical at both acquisition
  sites;
- do not add a default builder in `_broker_session.py`;
- do not move private or transient `DBConnection` construction paths through
  the process-session factory;
- test runner publication, rollback, and deadline races through the production
  adapter rather than a session-only mock;
- retain real PostgreSQL and Redis same-target allocation tests.

## Verification

The core lifecycle proof is in `tests/test_process_broker_session.py`.
Public closeable Queue-operation release is proved in
`tests/test_delivery_contract_sb_delivery.py::test_closeable_queue_iterator_releases_operation_on_same_thread`.
Finalizer-thread isolation, cleanup failure priority, non-ordinary ownership
restoration, terminal-timeout behavior, hookless runner ownership, never-used
close, and cleanup-before-close are proved in `tests/test_process_broker_session.py`.
Owned-runner verb selection is proved in `tests/test_runner_lifecycle.py`.
Caller-owned injected-runner retention across direct and manager-driven
teardown is proved in
`tests/test_custom_runner_integration.py::test_sql_borrowed_runner_masks_destructive_verbs_across_teardown`.
Activity-waiter terminal transitions and cleanup order are proved in
`extensions/simplebroker_pg/tests/test_pg_activity_waiter_lifecycle.py` and
`extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py`.
First-party service-backed allocation proof is in:

- `extensions/simplebroker_pg/tests/test_pg_integration.py`;
- `extensions/simplebroker_redis/tests/test_redis_integration.py`.

The core suite also includes an AST gate for the one-way import rule and
subprocess tests for both module import orders plus registry atexit shutdown.

## Related Plans

- retired: 2026-08-25-verified-review-findings-remediation-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns caller-owned
  borrowed-runner shutdown masking.
- retired: 2026-08-25-closeable-queue-iterator-contract-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns public closeable
  Queue iterator ownership.
- retired: 2026-08-24-comprehensive-review-findings-remediation-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns target
  snapshots, pre-lock fork recovery, Redis listener ownership, and PostgreSQL
  sidecar adaptation.

- retired: 2026-08-23-correctness-and-concurrency-review-remediation-plan —
  source `23d6c9d1` (local-only pin); see the ledger in
  `docs/plans/README.md`
- retired: 2026-08-11-activity-waiter-terminal-close-contract-plan — source
  `27f9ae4`; see the ledger in `docs/plans/README.md`
- retired: 2026-05-04-process-local-broker-session-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
- `docs/plans/2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`
- retired: 2026-07-29-code-quality-cleanup-plan — source `197629e2`; see
  the ledger in `docs/plans/README.md`
- retired: 2026-07-29-process-session-core-factory-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
