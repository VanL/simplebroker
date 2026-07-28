# Cross-Thread Finalization Poisoning

## Purpose and Scope

This document explains the one-way poison lifecycle used by SQL-backed
`BrokerCore` instances when an `at_least_once` transactional generator or a
`sidecar()` session is finalized on a thread other than the thread that owns
its suspended lock and transaction state. The affected public generator
wrappers are `Queue.read_generator()`, `Queue.move_generator()`, and
`Queue.stream_messages(batch_processing=True, commit_interval=N)` when
`N > 1`.

The mechanism makes an already-fatal ownership violation visible and bounds
later operations on that instance. It does not heal the abandoned lock or
transaction in process:

```text
HEALTHY
   |
   | foreign-thread finalization
   v
POISONED ── poison check ──> OperationalError(retryable=False)
   |
   | process restart only
   v
NEW HEALTHY INSTANCE
```

Redis and Valkey do not use this SQL transaction-and-lock path and are outside
the mechanism.

## Governing Contract and Plan

- `README.md`, **Delivery guarantees**, owns the public same-thread contract,
  diagnostic behavior, and restart instruction.
- `docs/plans/2026-07-27-cross-thread-generator-orphan-healing-plan.md`
  records the design evidence, rejected recovery approaches, and verification
  matrix.

Product ownership is layered (`docs/specs/product-section-registry.md`);
delivery/generator concerns remain `readme-only` in the root README.
This document explains why the
implementation has its current boundaries; it does not create a second
behavior contract.

## Design Rationale

### A permanent latch, not in-process recovery

The suspended frame owns a `threading.RLock` hold and, for transactional
operations, thread-local runner state. A foreign thread cannot safely release
that hold or roll back the owner thread's transaction. `BrokerCore` also does
not own every runner resource needed for cross-thread teardown.

Foreign finalization therefore publishes a permanent, per-core poison latch
and a first-cause diagnostic under a small dedicated mutex. Publication is
set-once and never clears the latch. It performs no runner call, transaction
cleanup, core-lock release, or owner-thread batch-state mutation. Recovery is a
process restart, which closes the owning connection and discards its
uncommitted transaction.

The latch carries no per-frame ownership slot because suspendable holds can
nest. Keeping only the first cause makes concurrent publication monotonic and
avoids pretending that the implementation can identify or heal one unique
orphan.

### Lock-wrapper boundary

`BrokerCore` wraps its re-entrant lock with a poison-aware adapter. Normal
`with self._lock:` acquisition checks the latch before waiting, probes the
underlying lock in bounded intervals, checks after each missed probe, and
checks once more after successful acquisition. The post-acquisition check
covers a recycled thread identifier after poison publication.

The wrapper deliberately exposes only the audited lock surface:

- context-managed acquisition performs poison checks;
- `acquire_held()` and `release_held()` serve the two suspendable-hold sites,
  and only the owning thread releases a hold;
- explicit `acquire()` and `release()` preserve raw `RLock` compatibility;
- `_is_owned()` delegates explicitly for the serialized-operation assertion;
- no broad attribute forwarding creates an unreviewed acquisition path.

The mutation guard also checks poison before reaching the core lock. Existing
operations that already entered a runner call are not synchronized barriers:
one in-flight same-thread call may finish after another thread publishes
poison. The foreign publication path itself never touches runner or
transaction state.

Transactional generator frames compare the current `Thread` object before
every yield, after every resumption, and before commit. An unsupported normal
`next()`/`send()` from a foreign thread publishes poison and terminates the
generator without another runner call or owner-state cleanup. Foreign
exception resumption publishes poison and re-raises the incoming exception.

### Sidecar capability closure

Foreign resumption of `sidecar()` branches on the captured owner `Thread`
object immediately after the yield, before commit or rollback. It publishes
poison and either re-raises the incoming exception or returns from a clean
foreign exit.

An outer `finally` still calls `SidecarSession.close()` on every exit. That
close only invalidates the yielded Python capability. It does not call the
runner, alter transaction state, or release the core lock, so it is safe on
the foreign thread and preserves the rule that a session is invalid after its
`with` block.

## Boundaries and Residuals

### Per-instance diagnosis versus database-wide contention

Poison is local to one `BrokerCore`. Sibling cores and other processes sharing
the same SQLite database cannot observe the latch, although they can encounter
the abandoned write transaction at the database layer. Under the default
configuration, the existing SQLite busy timeout and operation retry budget
bound that contention; it is not the prompt poison diagnostic seen by the
poisoned core.

### Persistent shared-wrapper operation lease

A persistent shared `Queue` begins an operation lease and records its session
on the owner thread's `DBConnection` thread-local stack. If
`Queue.read_generator()`, `Queue.move_generator()`,
`Queue.stream_messages(batch_processing=True, commit_interval=N)` with
`N > 1`, or `Queue.sidecar()` unwinds on a foreign thread, that thread cannot
pop the owner's stack. The session's active-operation count therefore remains
nonzero.

No transferable lease or session-layer recovery is added. On final
`Queue.close()`, the shared session may wait its existing five-second drain
bound before it reaches the poisoned core and raises the diagnostic. Closing a
non-last shared lease still returns. Other session modes can return, suppress
the internal diagnostic, or raise it as described by the README.

### Pre-publication thread-identifier recycling

The suspended frame compares `Thread` objects by identity, not integer thread
identifiers, when it resumes. The lock adapter's post-acquisition check stops
recycled-identifier re-entry after poison has been published.

There remains a narrower pre-publication case: an owner thread can exit while
leaving a hold suspended, its identifier can be recycled, and a new thread can
enter the same core before finalization publishes poison. Mechanically
excluding that schedule would require nesting-aware ownership machinery whose
complexity is not justified for simultaneous violations of the same-thread,
never-abandon, and thread-lifetime contracts.

## Operational Cost and Diagnostics

On a healthy instance, every context-managed core-lock acquisition performs
two short acquisitions of the poison-state mutex: one before attempting the
core lock and one after it succeeds. A contended acquisition adds one bounded
core-lock probe and poison-state check per probe interval. Explicit raw lock
acquisition remains a compatibility path and does not add poison checks.

A foreign finalization emits a best-effort `RuntimeWarning`; later poison
checks raise non-retryable `OperationalError` with the message prefix
`cross-thread finalization`. Absence of the warning is not evidence that
cleanup succeeded: warning emission and poison bookkeeping are deliberately
unable to replace the owner thread's cleanup, and exceptional teardown can
prevent observable warning delivery.

The diagnostic instructs operators to restart the process. After exit, the
abandoned transaction is discarded and interrupted messages remain available
for a later consumer. They are not silently committed, and the poisoned
process does not claim to recover them itself.

## Key Files

| Path | Responsibility |
|------|----------------|
| `simplebroker/db.py` | Poison latch, lock adapter, mutation guard, transactional generator and sidecar finalization |
| `simplebroker/sbqueue.py` | Public read, move, batch-stream, and sidecar wrappers plus thread-affinity documentation |
| `simplebroker/_sidecar.py` | Local `SidecarSession` capability invalidation |
| `simplebroker/_broker_session.py` | Existing shared-session lease and five-second close-drain boundary |
| `tests/test_cross_thread_finalization_poisoning.py` | Core lifecycle, sidecar finalization, public close outcomes, shared-lease residual, diagnostics, and nesting |
| `tests/test_sidecar.py` | Same-thread sidecar contract and transaction regressions |
| `tests/test_process_broker_session.py` | Baseline shared-session close and operation-lease behavior |
| `tests/helper_scripts/cross_thread_generator_probe.py` | Process-isolated backend finalization probe |
| `tests/test_cross_thread_generator_probe.py` | SQLite restart recovery, move-source restoration, sidecar, shutdown, and cross-core boundedness probes |

## Change Guidance

Before changing this mechanism:

1. Read the README Delivery guarantees section and the related plan.
2. Preserve the one-way lifecycle. Do not add rollback, lock release, latch
   clearing, or session-lease transfer on a foreign thread.
3. Audit every lock-across-yield site before changing the lock wrapper or
   claiming coverage of all suspendable holds.
4. Keep foreign publication free of runner calls and verify behavior with real
   databases and real threads, not an `RLock` mock.
5. Run the targeted poisoning suite, sidecar and process-session tests, the
   backend process probes, and the full backend suites named in the plan.
6. Update the README, CHANGELOG, generator and sidecar docstrings, this
   document, and firing tests together for any contract change.
