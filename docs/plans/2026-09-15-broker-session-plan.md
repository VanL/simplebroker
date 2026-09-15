# BrokerSession: name the process session, subtract last-user inference

Status: draft
Class: 5 — public library contract change: new package-root type, reverted
(unreleased) `persistent=True` close semantics, watcher ownership rule, and a
downstream (Weft) adoption boundary. Hardening applies: deferred cleanup, fork,
compatibility surface, rollout order.
Plan type: implementation with spec revision; promotion strategy B (atomic) per
slice. Each slice is one commit; the order below is the landing order.
Owner: SimpleBroker core.
Baseline: `004a7e9` (includes the unreleased `f4cc5d6`, `9d2cf99`, and
`004a7e9` lifecycle work on top of published 8.2.2 at `712926b`).
Target release: 8.3.0 (additive public API; behavior changes are against
unreleased work only).

## Goal

Give the process session a public name, `BrokerSession`, and make it the way an
embedder says "this thread is done with the shared resources." Do that by
subtraction first: the unreleased last-user stack (per-thread user counts,
pending-cause sentinels, session-bound registration, the main-thread exemption,
and the late-claim retry) is deleted, and `Queue(..., persistent=True)` returns
to its published 8.2.2 meaning — `close()` drops this handle's lease; the
thread cache lives until explicit `cleanup_connections()` or the last lease in
the process. Then the session is added as an extra lease plus the list of
queues it minted, with `recycle_thread()` as the explicit thread-cache release.
Queue stays the only verb surface. One lifecycle model results, reasoned once.

The owner selected this design on 2026-09-15 after three implementation-review
rounds each found a new defect in the inference machinery, and after comparing
it against the current tree with those defects fixed. The trade is explicit:
the simplest worker pattern (`with Queue(..., persistent=True)` inside a thread
function) no longer cleans its thread cache automatically; the remedy is one
line: `with BrokerSession.connect(...) as session:` around the worker body,
whose exit recycles that thread's cache before dropping the lease, or
`cleanup_connections()` on the way out.

## Source Documents

Source specs:
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-3], [SB-API-6],
  [SB-API-11]
- `docs/specs/11-delivery.md` [SB-DELIVERY-6]
- `docs/specs/product-section-registry.md` (owner of the Python surface rows)

Theory: `docs/program-theory.md` [THEORY-3] (process session is already a
named core concept: "process-local owner of reusable backend resources"),
[THEORY-4] (explicit safety over magical recovery; concrete pressure justifies
growth), [REV-THEORY-004] (queue handles do not each own a backend stack),
[REV-THEORY-005] (suspended operations retain their ownership context). No
theory change: this plan names an existing concept publicly and adds a
[REV-THEORY-006] entry recording that naming in the final slice.

Implementation rationale and history:
- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
  (`SM-PROCESS-SESSION`, `SM-POLLING`, watcher lifecycle rows)
- `docs/implementation/10-ruff-suppression-registry.md`
- `docs/plans/2026-09-15-session-lifecycle-integrity-remediation-plan.md`
  (completed; this plan supersedes its last-user and retry decisions and
  retains its nested-acquisition, `GeneratorExit`, claim-carrier, and watcher
  finalizer decisions)
- source-pinned `f4cc5d6:docs/plans/2026-09-15-close-thread-resource-release-plan.md`
  (retired-pending; its worker-release goal is re-expressed as the explicit
  session API)
- source-pinned `197629e2:docs/plans/2026-05-04-process-local-broker-session-plan.md`
  ("Do not add a public `Broker` class in the first implementation. A public
  broker facade is a reasonable future API" — this is that API, scoped to
  lifetime only)

Runbooks consulted: writing-plans, hardening-plans, testing-patterns,
designing-agent-facing-interfaces, adversarial-acceptance-probes,
review-loops-and-agent-bootstrap. Review skills: `skills/call-agent/SKILL.md`,
`skills/interface-review/SKILL.md`.

## Spec Baseline

- `004a7e9` — `docs/specs/16-python-library-api.md` [SB-API-1/3/6/11] at plan
  authoring time. [SB-API-3] at this baseline describes the last-user model
  this plan removes.
- Promotion baseline identifiers are recorded per slice in the Execution Log
  as each atomic slice commits.

## Evidence and Exact Scope

| Probe / fact (real SQLite unless noted) | Observed at `004a7e9` |
|------------------------------------------|------------------------|
| Three implementation-review rounds of the last-user stack | Each round produced at least one new confirmed defect in the inference: stale registration disposing a sibling's core; a new user not cancelling a pending release; a stranger's unmatched release running terminal cleanup and swallowing its failure; a foreign thread's late-claim failure re-raised at unrelated callers. |
| Weft teardown (`weft/core/tasks/base.py` `_finalize_task_once`) | Runs `cleanup_connections()` then `close()` on the stop caller's thread, not the driver thread. Last-user gave it no benefit; a run thread recycling itself does. |
| `open_broker()` | Builds a private `DBConnection` (`share_in_process` off). Weft's `context.broker()` therefore creates a fresh core, and on PostgreSQL a fresh pool, per call. |
| Five worker threads each `with Queue(persistent=True)` behind a main-thread anchor | Under 8.2.2 semantics: five cached SQLite connections until the anchor closes. This plan restores that behavior for the implicit path and documents the one-line remedy. |
| Watcher handler forwarding through a sibling persistent Queue per message | Under 8.2.2 semantics: zero core rebuilds. Restored. |
| Example test suites (`examples/tests` and the two `examples/test_*.py` modules) | Use `queue.conn.get_core()` in the reference reactor tests. `Queue.conn` is undocumented but copied; it is a compatibility surface. |

Out of scope: Weft source changes (separate repo; adopts after the PyPI
release); `open_broker` signature changes; any operation verb on
`BrokerSession`; async session API; pool sizing; cross-process sharing;
splitting `db.py`; physical deletion of retired plans; publishing.

## Context and Key Files

| File | What it owns today | Planned change |
|------|--------------------|----------------|
| `simplebroker/_broker_session.py` | `_ProcessBrokerSession`: TLS core, `operation_depth`, `user_count`, `cleanup_pending` sentinel, `_CoreDisposalClaim`, drain hold, late-claim retry, registry with refcounted `_SessionKey` (pid, backend, target, options, config), `acquire_process_broker_session` / `release_process_broker_session`, atexit close, fork recovery. | Slice 2 deletes user count, sentinels, retry, hold counter, `_close_terminal_cores`, `_first_present_failure`; `cleanup_pending` becomes a plain boolean; `_restore_claimed_core` re-adopts only while not closed; `close_all` regains its idempotent early return. Keeps claim carrier, `_release_operation_hold`, `_pop_operation_depth_locked`, `current_thread_operation_depth`, `_claim_pending_cleanup_locked`, `cleanup_current_thread`. Must still not import `db.py`. |
| `simplebroker/db.py` | `_ProcessSessionCoreFactory` (create/close_core/close, session-managed latch marking), `DBConnection` (per-handle lease adapter: `_ensure_shared_session`, `_has_inherited_shared_session`, `_ensure_shared_process_owner`, `_register_thread_use`, operation-session stack, `cleanup`, `close`), `BrokerCore.close` latch and hookless rule, `open_broker`. | Slice 2 deletes `_register_thread_use` and the `shared_user_session` marker; `close()` becomes lease-only through `_capture_process_session_cleanup`. Keeps the dual-snapshot unwind in `_get_shared_connection`, the latch, and the hookless rule. Slice 4 reuses `DBConnection(..., share_in_process=True)` for `session.connection()`. |
| `simplebroker/sbqueue.py` | `Queue.__init__` builds `DBConnection(share_in_process=persistent and runner is None)`; `get_connection` (hoisted acquisition, `GeneratorExit` branch); `cleanup_connections`; `close`; `_install_finalizer` (lease release via `conn.close()`); `_canonicalize_queue_target`. | Slice 2 restores lease-only docstrings. Slice 4 adds a private `_session` attribute (default `None`) and the public read-only `session` property. No constructor parameter, no change to `close()`. |
| `simplebroker/session.py` (new) | — | Slice 4: `BrokerSession` façade over the registry: `connect`, `queue`, `recycle_thread`, `connection`, `close`, context manager, read-only `target`/`backend_name`/`config`, fork guard, lease-only finalizer. Imports `sbqueue` and `db`; nothing imports `session.py` except `__init__.py` and tests. |
| `simplebroker/__init__.py` | Package-root `__all__` ([SB-API-1]). | Slice 4 exports `BrokerSession`. |
| `simplebroker/watcher.py` | `BaseWatcher.stop()` owns idle cleanup via `owns_cleanup` when lifecycle is `IDLE`; `_cleanup_runtime_resources` closes the strategy then calls `_cleanup_thread_local` → `queue.cleanup_connections()`; `run_forever`'s finally runs the same on the run thread; module-level `_finalize_watcher_lifecycle` stops a live watcher at interpreter exit; the default data-version getter bumps on persistent core replacement. | Slice 3: idle `stop()` closes strategy objects and the internally built Queue's lease but never calls `cleanup_connections()` on the caller's thread; the run thread's exit path keeps recycling its own cache; a caller-supplied Queue is never closed by the watcher. |
| `tests/test_process_broker_session.py`, `tests/test_connection_transition_tables.py`, `tests/test_watcher.py`, `tests/test_watcher_cleanup.py`, `tests/test_delivery_contract_sb_delivery.py` | Real-SQLite lifecycle proofs; `SM-PROCESS-SESSION` rows including `WORKER_RETAIN_NON_LAST_USER` / `WORKER_RELEASE_LAST_USER`. | Slice 2 deletes the last-user proofs named in Task 2 and rewrites the worker rows to retention; slice 3 adds idle-stop/run-thread proofs; slice 4 adds `tests/test_broker_session.py`. |
| `tests/test_fork_safety.py`, `extensions/simplebroker_redis/tests/test_redis_pool.py` | Real-fork proofs for inherited persistent Queues ([SB-API-11]). | Slice 4 adds inherited-handle cases using the same helpers. |
| `docs/specs/16-python-library-api.md` | [SB-API-1] surface table; [SB-API-3] lifecycle; [SB-API-6] watchers; [SB-API-11] fork; verification rows; Related Plans. | Slices 2, 3, 4 each promote their exact delta atomically. |
| `docs/guides/python.md`, `README.md`, `CHANGELOG.md`, implementation docs 06/07/10, `docs/program-theory.md` | Lifecycle guidance, Unreleased changelog entries from `f4cc5d6`/`9d2cf99`, state-machine rows, suppression registry, theory revisions. | Slice 2 rewrites the two Unreleased entries; slices 3–5 add theirs; slice 6 reconciles. README gains one Embedding paragraph in slice 4 (it has no lifecycle restatement today). |
| `examples/` | Reference reactor and multi-queue watcher use persistent Queues and `queue.conn`. | Slice 5 deprecates `Queue.conn` in docs only; examples are not edited except where slice 3's watcher rule changes an assertion. |

Read before editing: the whole of `_ProcessBrokerSession`, `DBConnection.close`
and `_get_shared_connection`, `Queue.get_connection`/`close`/`_install_finalizer`,
`BaseWatcher.stop`/`run_forever`/`_cleanup_runtime_resources`, and the
`[SB-API-11]` fork paragraph.

Comprehension gates (answer in the Execution Log before slice 2; a wrong answer
blocks until the named owner is reread):

1. After slice 2, what releases a worker thread's cached core? Expected:
   only `cleanup_connections()` called on that thread (immediately when idle,
   deferred to outermost operation exit otherwise), or terminal session
   shutdown when the last lease in the process is released. `Queue.close()`
   never does. (Owner: [SB-API-3] after promotion; `cleanup_current_thread`.)
2. What is a `BrokerSession` handle, and what does `with` do on exit?
   Expected: one extra registry lease on the same `_SessionKey` plus the
   scope's inventory of every Queue it minted, retained until close. Two
   handles on one key share one process session and one thread cache per
   thread. It owns no operations. Exit calls `close()`, which recycles the
   calling thread's cache, closes every minted queue, then drops the lease at
   most once; the garbage-collection finalizer drops the lease only, and only
   if `close()` never did. An inherited handle is never recovered.
   (Owner: slice 4 spec text; `_ProcessBrokerSessionRegistry.acquire`.)
3. Which watcher thread recycles what? Expected: the run thread recycles its
   own cache when it leaves `run_forever`/`run`; an idle `stop()` from another
   thread recycles nothing and only closes the strategy and the watcher's own
   Queue lease; the watcher never closes a caller-supplied Queue. (Owner:
   slice 3 spec text; `BaseWatcher.stop`.)

## Invariants and Constraints

- `Queue("jobs").write("x")` stays ephemeral, session-free, get-in/get-out.
  `persistent=True` without a session stays legal and means exactly what 8.2.2
  published: lease on close, cache until explicit cleanup or session end.
- `Queue` is the only operation surface. `BrokerSession` gains no
  write/read/peek/move/stats/alias methods, now or later. `session.connection()`
  yields the same `BrokerConnection` type `open_broker` yields.
- N handles or Queues on one key must never mean N runners or pools
  ([REV-THEORY-004]). `connect()` and `session.queue()` acquire through the
  existing registry key; no second registry.
- `recycle_thread()` is per thread per process session, not per handle. It
  never drops a lease and never creates a backend connection just to close it.
- Deferred release keeps its established rule: while this thread has an open
  Queue operation, explicit release waits for the outermost operation exit,
  then recycles once ([REV-THEORY-005]). `GeneratorExit` stays lifecycle
  control. A failed nested acquisition never alters the outer operation.
- Once a core leaves reusable TLS it stays owned by the session until the
  disposing scope finishes, including on `BaseException`. After the terminal
  timeout, a returning disposer closes its own claim and surfaces its own
  failure; it does not re-adopt into a closed session and does not re-enter
  `close_all`. No retry machinery.
- Minted Queues are scope-owned: the handle retains every Queue it minted
  until `close()` and closes them all then. There is no early-removal
  protocol, no callback from `Queue.close()` into the handle, and no liveness
  flag on Queue; early close and reuse are covered by `Queue.close()` being
  idempotent and reopen-on-use being existing behavior. Retention of many
  transient Queues by one long-lived handle is the accepted cost; the falsifier
  that would justify early removal is a consumer that mints unbounded
  transient Queues from one handle, which no consumer demonstrates today.
- `session.close()` recycles the calling thread's cache, closes the minted
  queues through their public `Queue.close()`, then drops its own lease, in
  that order. The context manager's exit is `close()`, so `with` alone is
  complete cleanup for the thread that runs it. A minted Queue in use on
  another thread behaves exactly as today's cross-thread `Queue.close()`. The
  finalizer path never recycles.
- Fork: a handle is process-local and is never recovered in a child. Every
  method checks the stored key pid before any lock; `queue()`,
  `recycle_thread()`, `connection()`, and `__enter__` raise on an inherited
  handle for every backend; `close()` on an inherited handle marks itself
  released and finalizes nothing; `connect()` in the child is always a new
  session. Parent-minted Queues keep their own existing fork policy. Handle
  recovery (fresh lock, rebound finalizer, abandoned list) is deliberately not
  built; the only consumer, Weft, spawns rather than forks.
- Hidden coupling, thread of teardown: `session.close()` recycles the cache of
  the thread that calls it. Closing from a different thread than the one that
  used the cache (Weft's `_finalize_task_once` runs on the stop caller's
  thread today) recycles the wrong cache and leaves the worker core until the
  session ends. The spec, the guide, and the kernel embedding paragraph state
  the rule: close or recycle the session on the thread that used the cache.
  Weft adoption is "move teardown onto the driver thread or call
  `recycle_thread()` there, then replace `context.broker()` with
  `session.connection()`"; pool sharing without the teardown move still leaks
  worker cores.
- Recycle is reachable only through a live handle on the key or through
  `Queue.cleanup_connections()`. A closed handle's `recycle_thread()` is a
  no-op even when sibling handles still share the key; do not expect a closed
  handle to recycle for siblings.
- Watcher: a handle owns leases; a thread owns cache cleanup. The run thread
  releases its own cache when it exits, whoever owns the Queue, because the
  cache belongs to the thread and session, not to a Queue. A caller-supplied
  Queue's lease is never closed by the watcher; if that Queue was used on the
  run thread, its cache on that thread is released at run exit and it
  reacquires on next use. Idle stop from another thread never touches the
  caller's cache.
- Handle admission and closing are coordinated under the handle's lock:
  `queue()`, `connection()`, and `__enter__` on a closing or closed handle
  raise `RuntimeError("BrokerSession is closed. Create a new session with
  BrokerSession.connect().")`; `recycle_thread()` and
  `close()` on a closed handle are no-ops. Once closing has begun the handle
  never re-admits. The minted list is read under the lock and Queues are
  closed outside it; the lock is never held while calling into a Queue or the
  process session.
- Ordinary cleanup failures follow [RUFF-SUP-035]: every independent step is
  attempted, the first failure is raised with later ones noted. A
  `BaseException` during `close()` propagates immediately, per the same
  policy. Shutdown is not transactional: completed steps stay completed, the
  registry lease is released at most once (the release call and the
  released mark plus finalizer detach sit in one `try`/`finally`), nothing is
  rolled back, and admission is not reopened. Because every step is
  idempotent (recycle on an empty cache is a no-op, `Queue.close()` is a
  no-op when already closed, the lease is guarded by the released mark), a
  later `close()` simply re-runs the steps and completes what was skipped; a
  handle dropped instead releases its lease through the finalizer only if
  that release never happened.
- Fatal versus best-effort: a `recycle_thread()` or `close()` failure surfaces
  to the caller after the remaining independent actions are attempted, using
  `_capture_process_session_cleanup`; finalizer paths suppress ordinary
  failures as today; `BaseException` keeps priority everywhere.
- No new public flags, config keys, dependencies, timers, background threads,
  thread registries, or ownership maps beyond the session's list of minted
  queues. `_broker_session.py` still does not import `db.py`.
- `Queue.conn` stays present and functional through 8.3.x; it is deprecated in
  prose and docstring only. No `DeprecationWarning` in this plan.
- Each slice is one commit that leaves the tree green on the full gate list.
  Rollback is `git revert` of that commit; slices 3–5 depend on slice 2 and
  are reverted in reverse order.

## Proposed Spec Delta

Promotion strategy: B (atomic) inside each slice — requirement text, link
claims, code, tests, and backlinks land together per commit, so no reciprocity
debt exists between commits.

| Spec file | Slice | Sections touched |
|-----------|-------|------------------|
| `docs/specs/16-python-library-api.md` | 2 | [SB-API-3] lifecycle paragraphs replaced; [SB-API-3] verification row |
| `docs/specs/16-python-library-api.md` | 3 | [SB-API-6] watcher ownership sentence; verification row |
| `docs/specs/16-python-library-api.md` | 4 | [SB-API-1] surface table row; new [SB-API-3] subsection "Process session handle"; [SB-API-11] fork sentence; verification rows; Related Plans |
| `docs/specs/16-python-library-api.md` | 5 | [SB-API-3] `Queue.conn` deprecation sentence |

### [SB-API-3] — replace every paragraph from "For a persistent Queue using a process-shared session" through "...propagates from the iterator's `close()`." (slice 2)

> For a persistent Queue using a process-shared session, each thread that uses
> the Queue caches one core and backend checkout for that session. `close()`
> releases that Queue's session lease and nothing else. When the last lease on
> that session in this process is released, the session ends: in-flight
> operations are drained under the existing bounded policy, every cached core
> is disposed, and the shared runner or pool is closed. Repeated close is a
> no-op for that Queue's connection manager. A Queue closed on a thread that
> never used it, including finalization on an arbitrary thread, releases the
> lease only.
>
> A thread's cached core is released before session end only by explicit
> request: `cleanup_connections()` on that thread, or `BrokerSession.recycle_thread()`
> on that thread. With no open Queue operation on the calling thread the
> release completes synchronously; otherwise it is deferred until the
> outermost Queue operation on that thread exits, then performed once. This
> does not relax the requirement to close a Queue's iterators before closing
> that Queue. Other threads' caches are never affected. The Queue remains
> usable afterward and reacquires on its next operation. Returning a backend
> checkout need not disconnect a pooled connection. These operations do not
> create a backend connection just to close it.
>
> Worker threads that must not leave a cached core behind while another
> handle keeps the session alive release it explicitly before exiting: hold a
> `BrokerSession` in a `with` block on that thread (its exit recycles the
> thread's cache), or call `cleanup_connections()`.
>
> An ordinary local-release failure is surfaced, but the Queue's lease
> release is still attempted; later cleanup failures are retained as exception
> diagnostics. If deferred release fails during operation unwind, an active
> application exception remains primary; otherwise the release failure
> propagates. A failed core is not reused and remains owned by its session for
> terminal cleanup while the session is live; after the session has ended, a
> late release failure is surfaced to its caller and not retried. A failed
> nested acquisition does not release or alter the outer operation.
> Non-ordinary `BaseException` behavior retains the existing propagation
> priority and failed-core ownership. `GeneratorExit` raised to close an
> iterator is lifecycle control rather than an application failure, so a
> deferred release failure propagates from the iterator's `close()`. A shared
> SQL runner without an independent thread-checkout release hook remains owned
> by the session factory until session end.

### [SB-API-6] — insert after the paragraph ending "...a custom polling provider retains its supplied semantics." (slice 3)

> Watcher stop distinguishes thread ownership from lease ownership. A thread
> owns its cached core; a handle owns its lease. The run thread releases its
> own cached core when it leaves `run_forever()` or `run()`, regardless of
> who owns the Queue: a caller-supplied Queue that was used on the run
> thread loses its cache on that thread at run exit and reacquires on its
> next use there, exactly as after `cleanup_connections()`. The watcher never
> closes a caller-supplied Queue's lease; its owner or its `BrokerSession`
> does that. A `stop()` call from another thread while the watcher is idle
> closes the strategy and, for a Queue the watcher constructed, that Queue's
> lease; it never releases the calling thread's cached core.

### [SB-API-1] — add a row to the public surface table (slice 4)

> | `simplebroker.BrokerSession` | Process-session lifetime handle: one extra lease on the shared backend resources for one resolved target and config, a minting point for persistent Queues on that target, and the explicit thread-cache release. It owns no queue operations. |

### [SB-API-3] — new subsection after the lifecycle paragraphs (slice 4)

> ### Process session handle
>
> This explicit lifetime handle deliberately introduces visible session setup:
> reusable backend resources span calls and need a named owner. Setup stays
> inspectable through `connect(target, config)` and the `target`,
> `backend_name`, and `config` attributes; ordinary `Queue(...)` use requires
> no session setup.
>
> `BrokerSession.connect(db_path | BrokerTarget, *, config=None)` returns a
> handle holding one lease on the process session for that resolved target
> and configuration. Two handles whose target, backend options, and
> configuration snapshot resolve to the same session key share one process
> session, one runner or pool, and one cached core per thread; they are not
> two pools and not two caches.
>
> `session.queue(name)` returns a persistent `Queue` bound to the same target
> and configuration. Minted Queues are scope-owned resources: the handle
> retains every Queue it minted until the handle closes, and closes each one
> then through the public `Queue.close()`. Each minted Queue also holds its
> own lease, so it stays usable if the caller keeps a reference after
> `session.close()`, exactly as a persistent Queue closed and reused does
> today. Closing a minted Queue early is harmless and its later close at
> scope exit is a no-op; a minted Queue reused after an early close is
> closed again at scope exit. `queue.session` returns the owning handle for
> a minted Queue, closed or not, and `None` for an ephemeral,
> injected-runner, or directly constructed persistent Queue. A handle held
> open while minting an unbounded number of transient Queues retains them
> all; that workload wants one handle per scope, not one handle per process.
>
> `session.recycle_thread()` releases the calling thread's cached core for
> this process session. Every live handle and every Queue sharing the same
> session key on this thread is affected; another handle on the same key does
> not have a separate cache. It does not drop leases. If a Queue operation is
> open on this thread the release is deferred until the outermost operation
> exits, then performed once. On a thread with no cache it is a no-op and
> creates nothing. After the handle is closed it is a no-op even when other
> handles still share the key; recycle through a live handle on that key or
> through `Queue.cleanup_connections()`. Release the cache from the thread
> that used it: a session closed or recycled on a different thread releases
> that thread's cache, not the worker's.
>
> `session.connection()` is a context manager yielding a `BrokerConnection`
> for broadcast, statistics, and alias operations, as `open_broker()` does,
> but over this handle's shared session rather than a private connection. The
> block holds its own temporary lease on the session for its duration; it is
> not one of the handle's recorded Queues, and a `session.close()` on another
> thread during the block ends the session only after that lease drops.
>
> `session.close()` performs three steps in order: it releases the calling
> thread's cached core exactly as `recycle_thread()` does, closes every Queue
> the handle minted through the public `Queue.close()`, then drops the
> handle's lease. Closing is idempotent, and the handle admits no new Queues
> or connections once closing has begun. Every step is attempted after an
> ordinary failure, and the first failure is raised with later ones attached
> as notes. A non-ordinary `BaseException` propagates immediately: completed
> steps stay completed, the lease is released at most once, and no step is
> rolled back or re-admitted; a later `close()` re-attempts only what was
> not completed. The thread-cache step means a handle
> held for a thread's lifetime and closed on that thread leaves nothing
> behind without any further call; a handle closed on a thread with no cache
> releases nothing there. When that lease was the last on the session in this
> process, the session ends as described above. A minted Queue still in use on
> another thread observes exactly what a cross-thread `Queue.close()` observes
> today. The handle is a context manager whose exit calls `close()`, so
> `with BrokerSession.connect(...) as session:` is complete cleanup for the
> thread that runs it. A handle that is garbage-collected without `close()`
> releases only its lease and never touches the collecting thread's cache.
>
> Read-only attributes: `target` (the normalized target, as `Queue.db_target`
> reports it), `backend_name`, and `config` (the retained snapshot).

### [SB-API-11] — insert after "...low-level injected runner's own fork policy." (slice 4)

> A `BrokerSession` handle is process-local and is never recovered in a
> forked child, whatever the backend. Before any lock, every method compares
> the handle's stored key pid to the current pid. `queue()`,
> `recycle_thread()`, `connection()`, and entering `with` on an inherited
> handle raise `RuntimeError`; `close()` and context exit on an inherited
> handle release nothing and finalize no parent resource, so entering `with`
> fails loudly while closing is silent; both are deliberate. Queues the
> parent minted keep their own existing fork policy ([SB-API-11] above), which
> for a direct backend still recovers child-owned session state; the handle
> does not. `connect()` in the child always creates a new session.

### [SB-API-3] — append to the Queue construction bullets (slice 5)

> - `Queue.conn` remains readable through 8.3.x for compatibility with code
>   that reached the connection manager directly, but it is not part of the
>   supported surface and may be relocated or removed under a later plan. Use
>   `queue.session` to reach the minting scope when present,
>   `session.connection()` for connection-level operations sharing that
>   scope, or `open_broker()` for an independent connection.

## Small Implementation Design

### Slice 2 — subtraction (commit 2)

`_broker_session.py`:
- Delete `_PENDING_CLEANUP_EXPLICIT`, `_PENDING_CLEANUP_LAST_USER`,
  `add_thread_user`, `drop_thread_user`, `_retry_closed_claims`,
  `_retry_closed_claims_after_operation`, `_first_present_failure`,
  `_close_terminal_cores`, `_release_cleanup_disposal_holds_since`, and the
  `cleanup_disposal_hold_count` TLS attribute.
- `cleanup_pending` is set to `True`; `_request_current_thread_cleanup_locked`
  loses its `cause` parameter; `_claim_pending_cleanup_locked` tests the
  attribute with `hasattr`.
- `_CoreDisposalClaim` gains `hold_taken: bool = False`, set inside
  `_request_current_thread_cleanup_locked` when the raw drain hold is taken;
  `_cleanup_current_thread` releases the hold in its `finally` only when
  `claim.hold_taken`, via `_release_operation_hold`.
- `_restore_claimed_core` re-adds to `_cores` only when `not self._closed`;
  after the terminal timeout a returning disposer's failure surfaces to it
  (raised, or attached when an application failure is active) and is not
  retried.
- `close_all` regains `if self._closed: return` and the
  `_capture_process_session_cleanup` loop with `partial(self._factory.close_core, core)`.
- `_end_operation` keeps its shape minus the retry call in `finally`.
- Keep the `__notes__` tuple snapshot in the two note helpers; add
  `if primary is failure: return primary` so a self-note is impossible.

`db.py`:
- Delete `_register_thread_use` and its two call sites; delete the
  `shared_user_session` marker; `DBConnection.close()` shared branch becomes:
  `_capture_process_session_cleanup(None, partial(release_process_broker_session, key))`
  inside the existing `finally` that clears lease state.
- Keep `_shared_operation_stack_depth`, the dual-snapshot unwind in
  `_get_shared_connection`, the `BrokerCore.close` latch, and the hookless
  rule.

`sbqueue.py`: docstrings of `close()` and `cleanup_connections()` state the
lease-only and explicit-release meanings. No logic change.

`watcher.py`: no change in this slice (the persistent-only token guard and
the finalizer rewrite stay).

Tests (real SQLite, real threads; fault injection only at the concrete
`close_core` seam or `BrokerDB.shutdown`):
- Delete: `test_worker_close_releases_its_sqlite_core_while_session_stays_live`,
  `test_worker_last_registered_manager_releases_thread_core`,
  `test_worker_sibling_queue_closes_reuse_registered_thread_core`,
  `test_reopened_manager_registers_again_for_replacement_session`,
  `test_new_user_cancels_pending_last_user_release`,
  `test_new_user_does_not_cancel_pending_explicit_cleanup`,
  `test_worker_last_close_defers_until_sibling_iterator_exits`,
  `test_session_user_count_cannot_override_main_thread_exemption`,
  `test_late_disposal_failure_gets_closed_session_retry`, and the
  reentrant-hold-count test.
- Rewrite `WORKER_RELEASE_LAST_USER` as `WORKER_CLOSE_RETAINS_CACHE`
  (worker closes its last Queue; identity survives until session end) and
  keep `WORKER_RETAIN_NON_LAST_USER`; restore the main-thread
  `RETAIN_WHILE_REFERENCED` identity assertion.
- Add: five workers each `with Queue(persistent=True)` behind an anchor
  retain five cores until the anchor closes, and the same workers calling
  `cleanup_connections()` before exit leave zero (this is the documented
  remedy); a disposer whose `close_core` fails after the terminal timeout
  raises to itself, the session's `_cores` stays empty, and no second
  `close_core` runs; a `BaseException` between claim and dispose in explicit
  cleanup restores ownership and releases the hold exactly once.
- Keep unchanged: nested-acquisition, `GeneratorExit`, iterator-close
  failure, finalizer-on-collector isolation, anchorless main reuse, hookless
  runner, drain, terminal-timeout-no-double-dispose, watcher token tests.

Docs: rewrite [SB-API-3] per the delta; implementation doc 06 section
"Caller-thread release on persistent Queue close" becomes "Explicit
caller-thread release"; doc 07 `SM-PROCESS-SESSION` row; replace the two
Unreleased CHANGELOG entries from `f4cc5d6` and `9d2cf99` with one entry that
states the retained fixes and the explicit-release rule; regenerate the ruff
suppression registry (RUFF-SUP-035 returns to four directives; RUFF-SUP-007
count drops with the deleted tests); mark the remediation plan's index row
`superseded — by 2026-09-15-broker-session-plan.md (nested acquisition,
GeneratorExit, claim carrier, and watcher finalizer decisions retained)`.

Stop and re-plan if the subtraction requires touching the drain timeout,
poison recovery, `SQLiteRunner`, or any extension.

### Slice 3 — watcher ownership (commit 3)

`watcher.py`: split `_cleanup_runtime_resources` into strategy close (always)
and thread recycle (run-thread exit only). The `owns_cleanup` idle path in
`stop()` calls strategy close and, when the watcher constructed its own Queue,
`Queue.close()` on it; it does not call `cleanup_connections()`. `run_forever`
and `run` keep recycling in their `finally` on the run thread, unconditionally:
the cache belongs to the thread, so a caller-supplied Queue used on that thread
sees its run-thread cache released and reacquires on next use. What the
watcher never does to a caller-supplied Queue is close its lease; record which
case applies at construction (`self._owns_queue`). `_finalize_watcher_lifecycle`
is unchanged (it calls `stop()`).

Tests (`tests/test_watcher_cleanup.py`, `tests/test_watcher.py`, real
SQLite): idle stop from main leaves main's cached core in place (the earlier
sibling-core bug); run thread exit disposes the run thread's core and, for an
internally built Queue, its lease; a caller-supplied Queue survives watcher
stop with its lease intact, loses only its run-thread cache, and reacquires on
its next operation there; interpreter-exit finalizer on a live
watcher still stops it (existing test, retained). Update the watcher lifecycle
transition table row.

Docs: [SB-API-6] sentence; implementation doc 06 "Watcher finalization
boundary"; guide watcher section; CHANGELOG.

### Slice 4 — `BrokerSession` (commit 4)

`simplebroker/session.py`:
```python
class BrokerSession:
    @classmethod
    def connect(cls, db_path, *, config=None) -> "BrokerSession":
        resolved = resolve_config(config=config)
        target = _canonicalize_queue_target(db_path, config=resolved, runner=None)
        key, process_session = acquire_process_broker_session(
            target, config=resolved, factory_builder=_build_process_session_core_factory)
        return cls(key, process_session, target, resolved)
```
- Instance state: `_key`, `_process_session`, `_target`, `_config`,
  `_queues: list[Queue]` (append-only until close; never pruned),
  `_lock: threading.Lock` (non-reentrant; never held while calling into a
  Queue or the process session), `_closing: bool` (set once, never cleared),
  `_released: bool`,
  `_finalizer = weakref.finalize(self, release_process_broker_session, key)`
  detached in the same `finally` that marks the lease released.
- `queue(name)`: fork guard; then under `_lock`: if `_closing` raise
  `RuntimeError("BrokerSession is closed. Create a new session with
  BrokerSession.connect().")`; construct
  `Queue(name, db_path=self._target, persistent=True, config=self._config)`
  (registry acquire only; no session lock; no callback into the handle); set
  `queue._session = self`; append; return. Constructing under the lock closes
  the create-then-append race against a concurrent `close()`.
- No `_forget`. `Queue.close()` does not call back into the handle. The
  handle's list is the scope's inventory until `close()`.
- `recycle_thread()`: fork guard; if `_released` return; else
  `self._process_session.cleanup_current_thread()` (outside `_lock`).
- `connection()` and `__enter__`: fork guard, then raise
  `RuntimeError("BrokerSession is closed. Create a new session with
  BrokerSession.connect().")` when `_closing` or `_released`.
- `connection()`: fork guard; `@contextmanager` over
  `DBConnection(self._target, None, config=self._config, share_in_process=True)`
  as `conn`: yield `conn.get_connection()`, release with
  `release_connection_after_use()` (mirroring `Queue.get_connection`'s
  three exit branches), then `conn.close()`. That `DBConnection` takes and
  drops its own temporary lease; it is never appended to `_queues`.
- `close()`: first, before any lock, the pid check: if inherited, mark
  `_released = True`, detach the finalizer (it is bound to the parent key and
  would be a registry no-op in the child anyway), and return without touching
  anything. Then under `_lock`: if `_released` return; set `_closing = True`;
  copy `_queues`; release the lock. Outside the lock, in order, each through
  `_capture_process_session_cleanup` so every independent step is attempted
  after an ordinary failure: (1) `self._process_session.cleanup_current_thread()`
  (the calling thread's cache; deferred by the existing pending rule if an
  operation is open on this thread; a no-op on a thread with no cache; the
  caller closes its own iterators first, as the existing invariant requires);
  (2) `Queue.close()` on each copied Queue (idempotent; no callback into the
  handle); (3) the lease step, written as
  `try: release_process_broker_session(self._key) finally: self._released = True; self._finalizer.detach()`
  so the lease is released at most once whatever the release raises, exactly
  as `DBConnection.close()` marks `_shared_released` in a `finally` today.
  Raise the retained ordinary failure if any. A `BaseException` from any step
  propagates immediately per [RUFF-SUP-035] with no restore: `_closing` stays
  set, the list stays intact, and a later `close()` re-runs the same
  idempotent steps and completes whatever was skipped; if the interruption
  hit inside the lease step the lease is already marked released. `__exit__`
  calls `close()`; `__enter__` runs the fork guard and the closing check and
  returns `self`, so `with` on an inherited handle fails loudly while
  `close()` on it is silent. The finalizer path is
  `release_process_broker_session` only, never `cleanup_current_thread`,
  because the collecting thread is not the owner; it is detached together
  with the released mark so it can never act on a replacement session that
  reuses the same key.
- Fork guard `_ensure_process_owner()`: if `self._key.pid == _getpid()`
  return; otherwise raise
  `RuntimeError("BrokerSession used in a forked process. Create a new session in the child process.")`
  for every backend. No in-place recovery: no lock replacement, no finalizer
  rebinding, no minted-list abandonment. Called at the top of `queue`,
  `recycle_thread`, `connection`, `__enter__`, before `_lock` is touched, so
  a lock held by a vanished parent thread is never acquired. Not called by
  `close()`, which checks the pid itself and returns.
- Properties: `target` returns a detached descriptor exactly as
  `Queue.db_target` does (`replace(target, backend_options=snapshot_key_material(...))`
  for a `BrokerTarget`; the string otherwise), never the stored `_target`
  object, so a caller cannot mutate backend options and split the handle's
  recorded session key from later minted queues; `backend_name` (from the
  target plugin without I/O, as `Queue.backend_name` does); `config` (the
  read-only snapshot).
  They read instance fields only and take no session lock, so they skip the
  fork guard; if any property ever dereferences `_process_session`, it must
  gain the guard.

`sbqueue.py`: `self._session = None` in `__init__`; `session` property. No
change to `close()`.

`__init__.py`: import and export `BrokerSession`.

Tests, new `tests/test_broker_session.py` (real SQLite, real threads; the
counting backend fixture for lease and runner counts; PG/Redis integration
additions in their extension suites):
- two `connect()` on one key share one registry entry and one thread cache;
  `create_runner_calls == 1` with N queues and M handles.
- `queue()` takes its own lease; closing the handle recycles the closing
  thread's cache (raw SQLite handle closed, asserted), closes every minted
  queue, and the session ends only when the last lease drops; a queue closed
  by the user first is closed again at scope exit as a no-op (asserted: one
  lease release for it); a queue closed early and then reused before scope
  exit is closed again at scope exit and its second lease is released
  (asserted); a minted queue the caller keeps after `session.close()` is
  usable and reacquires; `queue.session` returns the handle before and after
  close; `close()` on a thread with no cache builds no `BrokerDB`.
- `with BrokerSession.connect(...) as session:` inside a worker thread body,
  five workers behind a main-thread anchor: zero retained worker cores after
  the workers join, with no other call in the body; the same body with an
  exception escaping the `with` still leaves zero.
- `recycle_thread()` on a worker: SQLite handle physically closed, sibling
  handle's queue reacquires; on a thread with no cache: no `BrokerDB` built;
  deferred while an iterator is open, performed at `iterator.close()`.
- `connection()` shares the session (no second runner on PG; same
  `_ProcessBrokerSession` on SQLite) and returns a working `BrokerConnection`.
- `queue.session` identity for minted Queues; `None` for the three other
  kinds.
- Fork (`tests/test_fork_safety.py` helpers, and `test_redis_pool.py` for the
  direct backend): an inherited handle raises on `queue()`,
  `recycle_thread()`, `connection()`, and `with` before any lock for both
  SQLite and Redis; `close()` in the child is a no-op that finalizes nothing,
  proved with the handle lock held by another parent thread at fork time (the
  child's `close()` must return, not block); dropping the inherited handle in
  the child releases nothing in the parent; `connect()` in the child builds a
  new session; a parent-minted Redis Queue still recovers on its own as today.
- Admission versus close: `queue()` racing `close()` on another thread never
  yields an unowned Queue (either it is closed by the handle or the call
  raises the actionable "Create a new session" error); `queue()`, `connection()`, and `with`
  after `close()` raise; `recycle_thread()` and `close()` after `close()` are
  no-ops; the handle lock is not held while `Queue.close()` runs (real Queue,
  real lock, a Queue whose close blocks on an Event while another thread
  calls `queue()` and gets the closed error rather than a deadlock).
- Close failure semantics: an ordinary failure injected at `BrokerDB.shutdown`
  during the recycle step still closes every minted Queue and releases the
  lease, and raises the first failure with the later ones as notes; a
  `KeyboardInterrupt` injected at the recycle seam propagates immediately,
  admission stays closed, the lease is still held, a second `close()`
  completes the remaining steps, and a variant that drops the handle instead
  releases the lease through the finalizer; a `KeyboardInterrupt` injected
  inside the registry release (after the refcount drop, at the concrete
  `close_all` seam) leaves the handle marked released with its finalizer
  detached, and a second `close()` or garbage collection releases nothing
  again (the counting backend's release count stays at one; a fresh
  `connect()` on the same key afterward is unaffected).
- Handle dropped without close releases only its lease (finalizer); no thread
  cache is touched on the collector thread.
- Public API contract test in `tests/test_python_library_api_contract_sb_api.py`:
  root export, no operation verbs on the type, context-manager shape.
- Weft-shape acceptance: one session per task thread, `queue()` per name,
  `close()` on the task's own thread leaves zero cores for that thread with no
  separate recycle call; `close()` from the stop caller's thread instead
  leaves the task thread's core until session end (documented, asserted).

Docs: [SB-API-1] row; [SB-API-3] subsection; [SB-API-11] sentence;
verification rows; guide "Embedding" section with the worker-thread remedy
and the recycle-sharing sentence; README Embedding paragraph with a five-line
example; implementation doc 06 "Public session handle"; doc 07 rows; CHANGELOG
"Added"; theory [REV-THEORY-006] "Process session named publicly" citing
[THEORY-3] and this plan.

Stop and re-plan if `session.py` needs to import from `watcher.py`, if
`_broker_session.py` would need to import `db.py`, if a second registry or
ownership map appears, or if any verb method is proposed for the type.

### Slice 5 — `Queue.conn` deprecation (commit 5)

Docstring on the attribute, the [SB-API-3] bullet above, a guide note, and a
CHANGELOG "Deprecated" line. No warning, no removal, no example edits.
Stop if any first-party code path other than examples and tests still needs
`queue.conn` for something `queue.session` or `open_broker` cannot do; that is
a missing public capability to plan separately, not a reason to keep `conn`
supported.

### Slice 6 — traceability reconciliation (commit 6)

Backlinks from `session.py`, `_broker_session.py`, `db.py`, `sbqueue.py`, and
`watcher.py` to their spec sections; implementation index and repository map
rows for `session.py`; agent inventory if it lists lifecycle owners;
`docs/lessons.md` entry (inference of "thread done" from per-handle closes is
the wrong altitude; name the lifetime); `docs/agent-kernel.md` embedding
paragraph; interface-review record; plan Status Index row flipped to
`completed` with the promotion baseline identifiers; this plan's Execution Log
and Review Log completed.

## Tasks (one commit each)

1. [ ] **Commit 1 — plan.** This file plus its Status Index row (`draft`).
   Independent review of the plan and the four spec deltas before commit 2
   (different agent family preferred; PASS/BLOCKED with dispositions recorded
   below). Interface review of the [SB-API-1/3/6/11] prose with all eleven
   principles recorded. `session.connection()` is in scope (decided at plan
   review on the measured private-pool-per-call cost of `open_broker`).
   Done signal: review verdicts and dispositions recorded; gates below pass.
   Commit subject: `Plan BrokerSession and last-user subtraction`.
2. [ ] **Commit 2 — subtraction.** Slice 2 exactly as designed. Red first:
   the new retention, remedy, no-retry, and hold-once tests fail on `004a7e9`;
   record their failures. Then delete, implement, promote [SB-API-3], align
   docs, regenerate the suppression registry, supersede the remediation
   plan's row.
   Done signal: targeted selection, full suite, PG and Redis wrappers, static
   gates, document gates green; Weft's task-runtime connection suite green
   against this tree through Weft's own environment (record the imported
   path).
   Commit subject: `Restore lease-only persistent Queue close and remove last-user inference`.
3. [ ] **Commit 3 — watcher ownership.** Slice 3. Red first for the idle-stop
   and caller-supplied-Queue cases.
   Done signal: watcher suites, transition tables, examples tests green.
   Commit subject: `Make watcher stop release only what the watcher owns`.
4. [ ] **Commit 4 — BrokerSession.** Slice 4. Red first for every bullet in
   its test list. Adversarial floors from
   `docs/agent-context/runbooks/adversarial-acceptance-probes.md`: this adds a
   public type but no parser, CLI grammar, output format, or persistence; the
   lifecycle, fork, and concurrency cases are the acceptance probes.
   Done signal: all gates plus real PG and Redis session tests green;
   interface review re-run on the promoted text only.
   Commit subject: `Add BrokerSession as the public process-session handle`.
5. [ ] **Commit 5 — Queue.conn deprecation.** Slice 5.
   Commit subject: `Deprecate Queue.conn in favor of session and open_broker`.
6. [ ] **Commit 6 — traceability.** Slice 6; flip the Status Index row.
   Commit subject: `Reconcile BrokerSession traceability and close the plan`.

Stop and revise the plan if correctness requires thread-liveness tracking,
cross-thread cleanup, a reaper, per-handle caches, changing the terminal drain
timeout, or a verb on the session type.

## Testing Plan

Real threads, real SQLite connections, real `Queue`, real production factory
and registry. Internal counters support physical-close checks; they never
replace them (assert `sqlite3.ProgrammingError: closed database` on the raw
handle, or the counting backend's lease/release counts). Fault injection only
at `factory.close_core`, `BrokerDB.shutdown`, or `runner.release_thread_connection`;
never stub the session, registry, or `DBConnection`. Events and positive order
evidence; timeouts are liveness valves. Fork tests use the existing real-fork
helpers, never `os.fork` mocks.

| Case | Home |
|------|------|
| Lease-only close retains worker caches until anchor close; explicit remedy releases them | `test_process_broker_session.py` (slice 2) |
| No retry after terminal timeout; failure surfaces to the disposer once | `test_process_broker_session.py` (slice 2) |
| Hold released exactly once under `BaseException` in explicit cleanup | `test_process_broker_session.py` (slice 2) |
| Idle stop never recycles the caller's thread; run thread recycles itself; caller-supplied Queue untouched | `test_watcher_cleanup.py`, `test_watcher.py` (slice 3) |
| Shared key: one runner, one cache per thread across handles and queues | `test_broker_session.py`, PG and Redis integration (slice 4) |
| Handle close closes every minted queue then drops the lease at most once; early-closed and reused queues | `test_broker_session.py` (slice 4) |
| Interrupted close: no rollback, no re-admission, lease released once; later close completes | `test_broker_session.py` (slice 4) |
| `recycle_thread()` immediate, deferred, no-op, after-close | `test_broker_session.py` (slice 4) |
| `connection()` shares the session | `test_broker_session.py`, PG integration (slice 4) |
| Fork: inherited handle raises or recovers per backend; child connect is new; inherited close finalizes nothing | `test_fork_safety.py`, `test_redis_pool.py` (slice 4) |
| Dropped handle releases only its lease | `test_broker_session.py` (slice 4) |
| Public contract shape | `test_python_library_api_contract_sb_api.py` (slice 4) |
| Weft-shaped task lifecycle | `test_broker_session.py` (slice 4) and Weft's suite (gate) |

## Verification and Gates

Planning gates (run before commit 1):

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Lifecycle-change gates (commits 2, 3, and 4). The targeted selection is the
list below; `tests/test_broker_session.py` exists only from commit 4 and is
added to the selection and the mypy list at that commit:

```bash
uv run --locked pytest -n 0 tests/test_process_broker_session.py tests/test_connection_transition_tables.py tests/test_watcher.py tests/test_watcher_cleanup.py tests/test_watcher_stop_contract.py tests/test_watcher_error_handler_contract.py tests/test_delivery_contract_sb_delivery.py tests/test_peek_generator_lifecycle.py tests/test_custom_runner_integration.py tests/test_fork_safety.py tests/test_runner_lifecycle.py tests/test_python_library_api_contract_sb_api.py tests/test_queue_connection_manager.py examples/tests
uv run --locked pytest
uv run --locked bin/pytest-pg -n 0
uv run --locked bin/pytest-redis -n 0
```

Static and document gates (every commit; prose-only commits 1, 5, and 6 run
these plus the targeted selection and skip the backend wrappers, which add no
evidence for prose):

```bash
uv run --locked ruff check .
uv run --locked ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --locked mypy simplebroker bin/release.py
MYPYPATH=. uv run --locked mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs tests/test_process_broker_session.py tests/test_connection_transition_tables.py tests/test_watcher.py tests/test_watcher_cleanup.py
uv run --locked python bin/ruff_suppression_index.py --check
python3 bin/check-dom15-fixtures && bin/check-plan-context && bin/check-doc-paths && git diff --check
```

Downstream gate (commits 2 and 4): run Weft's
`tests/core/test_task_runtime_connections.py` and its task lifecycle suites
from the Weft checkout's own environment with this tree first on the import
path; record the exact command and the imported `simplebroker/__init__.py`
path. Weft source is not edited; its adoption is a separate Weft plan after the
8.3.0 release.

## Rollout, Rollback, and Success Signal

No schema, storage, dependency, or backend-handshake change. Landing order is
the commit order; commits 3–5 depend on commit 2. Rollback is `git revert` per
commit in reverse order. Nothing in this plan is published; the CHANGELOG
entries stay under Unreleased until the 8.3.0 release process.

Success after adoption: the five-worker probe shows zero retained cores when
workers hold a session or recycle, and exactly the documented retention when
they do not; the forwarding-handler probe shows zero core rebuilds; Weft, after
moving task teardown onto the driver thread (or calling `recycle_thread()`
there) and replacing `context.broker()` with `session.connection()`, shows one
PG pool per target per process and zero retained driver-thread cores after
task stop; no new lifecycle defect class appears in the first two review
rounds of a change to `_broker_session.py`.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Review Log and Dispositions

2026-09-15, plan review round 1 (independent, different family): **PASS with
required dispositions**, all accepted and written into the plan before
commit 1:

| ID | Finding | Disposition |
|----|---------|-------------|
| R1 | Direct-backend fork recovery replaced `_key`/`_process_session`, after which `close()` would close parent-owned Queues still on `_queues`, violating "releasing an inherited handle never finalizes parent resources". | Accepted. Recovery clears `_queues` without closing its members; [SB-API-11] delta says the inherited minted list is abandoned, not closed; invariant added. |
| R2 | Weft's teardown runs on the stop caller's thread; `session.close()` recycles the calling thread, so pool sharing alone would still leak driver cores. Hidden coupling, not a test footnote. | Accepted. Invariant and [SB-API-3] sentence "release the cache from the thread that used it"; guide and kernel paragraphs in slices 4 and 6; success signal and Weft adoption note now name the teardown-thread move. |
| R3 | A closed handle's `recycle_thread()` is a no-op even when siblings share the key: the one per-handle exception to "per thread per key". | Accepted. Spec sentence and invariant: recycle through a live handle on the key or `Queue.cleanup_connections()`. |
| R4 | `connection()`'s temporary `DBConnection` lease must not join `_queues`. | Accepted. Design line and spec sentence; a concurrent `session.close()` ends the session only after that lease drops. |
| R5 | `__enter__` on an inherited SQL handle raises while `close()` is silent; document the asymmetry in the spec. | Accepted. [SB-API-11] delta and design text. |

Non-blocking items, all applied: `connection()` fixed in scope (Task 1 waffle
removed); `_forget` only after a successful close so `session.close()` retries
a failed one; iterators-first clause under `session.close()`; properties skip
the fork guard because they take no lock; example-suite count removed;
suppression-registry regeneration and the remediation-plan supersede row stay
in slice 2. The reviewer confirmed: no verbs on the session, no required
session for `Queue("jobs")`, no `db.py` split, no `DeprecationWarning` on
`conn`, claim carrier stays and retry goes. Hardening checklist: all rows yes
after R1 and R2. Classification class 5 with hardening confirmed.

2026-09-15, plan review round 2 (independent, different family): **BLOCKED as
written** on four P1 lifecycle-boundary findings; the explicit-session
decision and the bare-persistent-Queue tradeoff were confirmed and not
reopened. All four accepted; one with a modified disposition:

| ID | Finding | Disposition |
|----|---------|-------------|
| R6 | Direct-backend fork recovery replaced the key but kept the inherited handle lock (deadlock if a parent thread held it at fork) and the finalizer bound to the parent key (child lease leaked on garbage collection); inherited `close()` must check pid before any lock. | Accepted. Recovery replaces `_lock` first, rebinds the finalizer to the child key, clears `_closing` and the minted list; `close()` checks pid before acquiring `_lock`. Real-fork tests for the held-lock and finalizer cases added to slice 4. |
| R7 | `queue()` could construct, race a concurrent `close()` clearing the list, then append to a closed handle; post-close `queue()`/`connection()`/`__enter__` were unspecified; `Queue.close()` calling `_forget` under a non-reentrant lock could deadlock. | Accepted. `_closing` flag; admission checked and Queue constructed under `_lock`; `close()` snapshots under the lock and closes outside it; post-close `queue()`/`connection()`/`__enter__` raise `RuntimeError("BrokerSession is closed")`; `recycle_thread()`/`close()` stay no-ops. |
| R8 | `_capture_process_session_cleanup` catches `Exception` only, so a `KeyboardInterrupt` during recycle would skip queue closes and lease release while the plan promised every step is attempted. | Accepted with modification. The repository's [RUFF-SUP-035] policy is that `BaseException` propagates immediately rather than running further cleanup, and this plan keeps it. What it guarantees instead is that nothing is lost: on `BaseException`, the unclosed snapshot is restored to the list, `_closing` is cleared, and the lease-only finalizer stays attached, so a later `close()` completes and garbage collection releases the lease. Firing tests for the ordinary path (every step attempted, first failure raised with notes) and the interruption path (state restored, later close completes). |
| R9 | The watcher contract said both "the run thread releases its cache" and "caller-supplied Queues are never recycled", which cannot both hold because the cache belongs to the thread. | Accepted, taking the reviewer's preference: the run thread always recycles its own cache; a caller-supplied Queue's lease is never closed; the [SB-API-6] delta states the shared-cache effect (reacquire on next use on that thread). Slice 3 text and tests corrected. |

Smaller corrections applied: `target` returns a detached descriptor built the
way `Queue.db_target` builds one, never the stored object; the gate block is
split so `tests/test_broker_session.py` appears only from commit 4 and
prose-only commits skip the backend wrappers.

2026-09-15, plan review round 3 (conceptual, posed for consideration rather
than application). Each was weighed against the code and the consumer before
deciding; all three changed the plan:

| ID | Question | Decision and reasoning |
|----|----------|------------------------|
| R10 | Are minted Queues independent handles or scope-owned resources? The `_forget` protocol made ownership unstable: an early-closed then reused Queue left the scope, while `queue.session` still named it. | Scope-owned. The handle retains every minted Queue until `close()` and closes them all then; `_forget` and the `Queue.close()` callback are deleted. Early close and reuse are covered by idempotent `Queue.close()` plus existing reopen-on-use. The alternative, independent Queues with no automatic close, was rejected because it gives up the one-block cleanup that is the type's reason to exist. The retention cost for a long-lived handle minting unbounded transient Queues is accepted and recorded as the falsifier that would justify early removal; no consumer shows that workload, and Weft is directed to one handle per task scope. |
| R11 | Interrupted `close()` had become transactional: restore the list, clear the closing flag, promise a later close finishes. The registry releases the lease before `close_all()`, so a `BaseException` there cannot be rolled back, and a key-bound finalizer left attached could later act on a replacement session with the same key. | Narrowed. Interruption propagates immediately; completed steps stay completed; the lease is released at most once (release call, released mark, and finalizer detach in one `try`/`finally`, the same shape `DBConnection.close()` uses); nothing is rolled back; admission never reopens. Because every step is idempotent, a later `close()` simply re-runs them; no restore machinery. The round-2 finding correctly identified the helper mismatch; it did not establish a need for resumable shutdown, and this plan does not build one. |
| R12 | Is automatic fork recovery for the handle necessary breadth? Consistency with Redis Queue recovery argued for it; the process-local concept argued against. | Not built. Every inherited handle rejects use for every backend; `close()` on it is a silent no-op; callers `connect()` in the child. The fresh-lock and finalizer-rebinding requirements from R6 disappear with the recovery they served; the pre-lock pid check stays. Weft, the only downstream, uses `multiprocessing` spawn, so no consumer inherits handles. Parent-minted Redis Queues keep their existing recovery unchanged. |

2026-09-15, interface review of the proposed [SB-API-1/3/6/11] text:
**PASS after three required dispositions.** The eleven-principle walk found
one deliberate departure and two teaching defects: the governing spec delta
now explains why visible session setup is warranted and remains inspectable;
the closed-handle error carries the `BrokerSession.connect()` recovery action;
and the `Queue.conn` deprecation distinguishes minting-scope, shared-session,
and independent-connection replacements without promising an unplanned move.
Ratified: lifetime-only handle, shared `connection()`, scope-owned minted
Queues, reject-all inherited handles, silent inherited close, and prose-only
deprecation. No enumerable enum/status/flag contract was added; closed and
forked-handle cases have firing tests. Runbook feedback: no new candidate.

## Execution Log

- 2026-09-15: Plan authored against `004a7e9`. Design selected by the owner
  after comparing the last-user model (with its review findings fixed) against
  the explicit session: compatibility with published 8.2.2 semantics, removal
  of the inference defect class, and explicitness for agent users outweighed
  automatic cleanup for hand-written thread pools. Comprehension gate answers,
  red evidence, promotion baseline identifiers, and gate results are appended
  here per commit.
