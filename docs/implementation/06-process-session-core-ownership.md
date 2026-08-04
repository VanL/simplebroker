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

## Acquisition

`DBConnection.__init__()` and `DBConnection._ensure_shared_session()` are the
only production acquisition sites. Both pass the same module-level factory
builder. The builder accepts a copied `_SessionSpec`; it is not a bound method
and does not retain a `DBConnection` or its stop event.

The registry resolves one `_SessionSpec` per acquisition. Its `_SessionKey`
selects an existing entry. The registry invokes the builder only when the key
is new, so a repeated acquisition cannot allocate and discard an unused
factory.

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
First-party service-backed allocation proof is in:

- `extensions/simplebroker_pg/tests/test_pg_integration.py`;
- `extensions/simplebroker_redis/tests/test_redis_integration.py`.

The core suite also includes an AST gate for the one-way import rule and
subprocess tests for both module import orders plus registry atexit shutdown.

## Related Plans

- retired: 2026-05-04-process-local-broker-session-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
- `docs/plans/2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`
- retired: 2026-07-29-code-quality-cleanup-plan — source `197629e2`; see
  the ledger in `docs/plans/README.md`
- retired: 2026-07-29-process-session-core-factory-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
