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
a persistent Queue, context exit ends the process-session operation while the
thread-local core and backend checkout remain cached. For a no-runner
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
thread-local process-session core and held PostgreSQL checkout. An ephemeral
Queue owns and releases one operation connection. An injected runner is
supported through its borrowed core, but `persistent=True` does not strengthen
the runner owner's checkout contract. Psycopg pool statistics are not a
substitute for the PostgreSQL catalog probe because each process owns its own
pool and cannot see other processes or unrelated clients.

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

Transaction-owner progress belongs to the runner, not the process session.
When several thread-local cores share one runner, their separate core locks do
not serialize a transaction. The runner must keep a successful transaction
owner's path to `commit()` or `rollback()` clear. `SQLiteRunner` does this with
condition-guarded admission: foreign operations wait without holding the
per-call operation lock, while the owner continues to use its thread-local
connection. Deliberately shared SQLite reads and writes both wait behind an
active transaction, and that wait is bounded by the configured SQLite busy
timeout.

PostgreSQL uses its existing backend-specific equivalents: a leased operation
lock for a shared retained connection, or a pool checkout retained for a
non-leased transaction. Redis uses a direct core and does not enter the SQL
runner transaction protocol.

`SQLiteRunner.close()` observes the same admission boundary. An explicit close
behind a foreign live owner can wait through the configured busy timeout and
raise the retryable admission error without closing other tracked connections.
First-party best-effort shutdown paths suppress that bounded cleanup failure;
explicit callers must handle it. A foreign orphan is still restart-required.

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

Acquisition recursively detaches supported option and configuration containers
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
