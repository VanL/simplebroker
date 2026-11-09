# PostgreSQL per-queue activity notification channels

Class: 4 — risky: changes the cross-process PostgreSQL wake protocol (writer
NOTIFY channel and listener subscription lifecycle), adds rollout-order
sensitivity for mixed client versions, and changes a background listener
thread's state machine. No normative spec text changes (no channel layout is
specified anywhere), so not class 5.
Status: active — implemented, measured and documented on branch `perf/sb-notify-fanout`; cross-family review and owner decisions pending.
Owner: SimpleBroker product owner; implementation agent for track SB-3.
Boundary: `extensions/simplebroker_pg` notification SQL, channel identifiers, and the process-local shared activity listener. Core, SQLite, Redis and every public signature are out of scope.
Verification: real-PostgreSQL wire, wake, race and subscription tests; SM-PG-LISTENER transition table; full PostgreSQL extension suite; lint/format/types; before/after fan-out measurement on disposable `postgres:18`.
Required action: review the two owner decisions in "Owner decisions" before release; do not release this extension without the CHANGELOG upgrade-order note.

## Goal

- [x] A PostgreSQL write, move, rename or broadcast stops being delivered to listener
      connections that wait on none of the touched queues.
- [x] Every existing wake guarantee holds: a waiter wakes for its queue (writes,
      move targets, both rename names, every broadcast target, writes through an
      alias), the `*` wildcard wakes every waiter, and a write committed after
      waiter construction returns always wakes it.
- [x] Pre-change writers (schema channel) still wake post-change waiters.
- [x] Measure server `xact_commit` per write and listener notification
      deliveries with N listening processes and M writers, before and after.

## Source Documents

- Theory: `docs/program-theory.md` [THEORY-2], [THEORY-3] (watcher/waiter and
  backend adapter rows), [THEORY-4] (explicit safety over magical recovery;
  concrete pressure justifies growth), [THEORY-5]; [REV-THEORY-004] (queue
  fan-out became connection fan-out; same pressure shape here).
- Contracts: `docs/specs/16-python-library-api.md` [SB-API-6] (activity
  waiters are wake hints; terminal close), [SB-API-11] (backend activity-waiter
  hooks); `docs/specs/11-delivery.md` [SB-DELIVERY-2], [SB-DELIVERY-3] (watch
  and move outcomes rely on drains, not on notifications);
  `docs/specs/12-broadcast.md` [SB-BCAST-4] (one transaction, every target
  written); `docs/specs/17-ops.md` [SB-OPS-4] (rename), [SB-OPS-5] (aliases
  resolve to canonical names before operations).
- Implementation rationale: `docs/implementation/07-complexity-and-state-machine-map.md`
  (`SM-PG-LISTENER`), `docs/implementation/06-process-session-core-ownership.md`
  (activity waiters, listener registry, connection budget),
  `docs/implementation/10-ruff-suppression-registry.md` ([RUFF-SUP-019] listener `_run`).
- Product docs: `extensions/simplebroker_pg/README.md` "Multi-Queue Activity Waiters".
- Process: `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/testing-patterns.md`.

No spec states the notification channel layout. Contract surface touched is
behavioral only: which waiters a notification can reach.

## Spec Baseline

- `5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` — all files above at plan
  authoring time. Plan type: implementation; no spec revision.

## Measured premise (falsified in part)

The brief's premise is that narrowing the channel stops a write from waking
every listening backend. On PostgreSQL 18 that is false for the server side.

Raw probe (`postgres:18.4`, 20 listener connections, 200 NOTIFYs, psycopg
3.3.8), comparing all listeners on the notified channel against every listener
on a different channel:

| Mode | server `xact_commit` per NOTIFY | notifications delivered to clients |
|------|-------------------------------|------------------------------------|
| listeners on the notified channel | 21.00 | 4,000 |
| listeners on other channels only | 21.00 | 0 |

PostgreSQL through 18 (`src/backend/commands/async.c` `SignalBackends`) signals
every listening backend in the same database that is not caught up, regardless
of channel; each signaled backend runs one transaction
(`ProcessIncomingNotify`) to read the shared queue, then forwards only entries
for channels it listens on. Schemas therefore do not isolate; databases do.
PostgreSQL commit `282b1cde9` ("Optimize LISTEN/NOTIFY via shared channel map
and direct advancement", master 2026-01-15, fix `0f24332ae` 2026-05-27) makes
the notifier signal only backends listening on the notified channel and
advance uninterested ones without waking them. That lands in PostgreSQL 19
(RC as of 2026-11-08; GA planned 2026-11-19).

Consequence for this change:

- PostgreSQL ≤ 18: per-queue channels remove the forwarded notification, the
  network packet, and the listener-thread wake for unrelated writes. Server
  `xact_commit` per write stays `1 + listening backends in the database`.
- PostgreSQL ≥ 19: the same change also removes the server-side signal and
  read transaction for uninterested listeners. A single schema channel can
  never benefit, because every listener listens on it.

No SimpleBroker-side mechanism removes the PostgreSQL 18 server cost without a
new durable interest registry (see Alternatives). Measuring PostgreSQL 19
requires pulling a `postgres:19` image, which was not authorized in this track.

## Context and Key Files

Files to modify:

- `extensions/simplebroker_pg/simplebroker_pg/_identifiers.py` — today only
  `activity_channel_name(schema)` = `simplebroker_` + md5(schema)[:24]. Add
  `queue_activity_channel_name(schema, queue_name)`.
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py` — `INSERT_MESSAGE`
  (every write and every broadcast target), the `move` branch of
  `build_retrieve_query` (target queue), `RENAME_QUEUE_MESSAGES_COUNT` (old and
  new names) each compute the schema channel inline in SQL with
  `'simplebroker_' || substr(md5(current_schema()), 1, 24)` and send the queue
  name as payload. `_should_prepare` in `runner.py` matches the INSERT and move
  statement prefixes; those prefixes must not change.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py` —
  `_SharedActivityListener` (one per process, DSN and schema, created by
  `_SharedActivityRegistry.acquire`): its thread connects, LISTENs the schema
  channel, publishes readiness, then loops `conn.notifies(timeout=0.1,
  stop_after=64)` and routes payloads: `*` advances the wildcard version, a
  registered queue name advances that queue's version, anything else is
  ignored. `register_queue` / `register_queue_set` are pure in-memory refcount
  operations today. `PostgresActivityWaiter` / `PostgresMultiQueueActivityWaiter`
  acquire a registry reference and register in their constructors.
- `extensions/simplebroker_pg/tests/test_pg_notify.py`,
  `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py`
  (SM-PG-LISTENER table; uses `_ScriptedPGConnection`, a fake for only the
  nondeterministic psycopg transport).
- Docs: `extensions/simplebroker_pg/README.md`, `CHANGELOG.md` (Unreleased),
  `docs/implementation/07-complexity-and-state-machine-map.md` (SM-PG-LISTENER
  row), `docs/implementation/10-ruff-suppression-registry.md` if the `_run`
  suppression changes, `docs/plans/README.md` index.

Load-bearing facts:

- Waiter construction precedes the watcher's first drain
  (`simplebroker/watcher.py` `_create_activity_waiter` then `strategy.start`),
  and the native strategy re-polls every `max(1.0, 10 × max_interval)` s
  (randomized 1–2×) even while the waiter is quiet. Writes committed before a
  waiter's registration completes are therefore owned by the drain; writes
  committed after must reach the waiter.
- Aliases are resolved to canonical names before every write, move, rename,
  watch and waiter creation ([SB-OPS-5]: `Queue` uses literal names; `@name`
  resolves in commands). The inserted row's `queue` column, which supplies the
  notification payload, is always canonical.
- psycopg 3.3 `Connection.notifies()` holds the connection lock for the whole
  generator; notifications received while another command runs go to the
  connection's backlog and are yielded first by the next `notifies()` call.
- Queue names match `^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$` (≤ 512 chars) and schema
  names `^[A-Za-z_][A-Za-z0-9_]*$`; both are ASCII, so PostgreSQL `md5(text)`
  and Python `hashlib.md5(str.encode("utf-8"))` hash identical bytes in every
  server encoding.

Comprehension checks (answer in the Execution Log before runtime edits):

1. Why must `register_queue` not return until the per-queue LISTEN has
   committed? Expected: a write that commits after registration returns but
   before LISTEN commits is never delivered to that backend, and the caller's
   drain already ran or is running, so the wake would be lost until the idle
   re-poll.
2. Why can a registration fast path not trust "channel is in the listening
   set" unless channels leave that set before UNLISTEN is executed?
   Expected: an in-flight reconciliation pass may be about to UNLISTEN the
   channel; removing it from the set under the lock first forces the new
   registrant onto the slow path, which requests a later pass that re-LISTENs.

## Design

Wire protocol (stable; mixed versions depend on it):

- Schema channel (unchanged): `simplebroker_` + `md5(schema)[:24]`. Post-change
  writers no longer NOTIFY it. Post-change listeners keep LISTENing it so the
  `*` wildcard and pre-change writers still wake them.
- Queue channel (new): `simplebroker_q_` + `md5(schema || chr(31) || queue)[:24]`,
  39 bytes (< 63, `NAMEDATALEN - 1`), never equal to a schema channel (hex has
  no `q`). SQL computes it from `current_schema()` and the canonical queue name
  in the same statement that writes the row; Python computes it in
  `_identifiers.queue_activity_channel_name`. Payload stays the queue name, and
  the listener keeps routing by payload, so a hash collision costs one ignored
  delivery, never a lost or misrouted wake.

Listener (one thread owns the connection; no cross-thread connection use):

- The thread waits on a selector over the libpq socket and a private wake
  socket pair (0.1 s cadence retained for stop checks). Each pass: reconcile
  subscriptions, then drain `conn.notifies(timeout=0)` (backlog first, one
  nonblocking poll), then wait.
- Reconciliation reads, under the listener lock, the requested generation and
  the desired channel set `{queue_channel(q) for registered q}`; removes
  channels it will UNLISTEN from the committed-listening set before executing;
  sends all UNLISTEN/LISTEN statements in one simple-protocol execute (one
  implicit transaction); then adds new channels to the set and publishes the
  applied generation.
- `register_queue` / `register_queue_set` update refcounts and snapshot
  versions first (so any delivery during the subscription window counts as a
  wake — conservative), then return immediately if every needed channel is
  already committed; otherwise request a generation, wake the thread and wait
  for the applied generation, bounded by the listener's startup timeout (5 s).
  On timeout the registration is rolled back and `OperationalError` is raised;
  if the wait raises anything (any `BaseException`), the registration is
  rolled back and the exception propagates.
  If the listener already failed or stopped, registration returns at once and
  the first `wait()` reports the stored error exactly as today.
- Last unregister of a channel requests a pass (UNLISTEN) without waiting.
- Routing, version clamping, fan-in entries, error publication and close are
  unchanged; close additionally wakes the thread and wakes subscription waiters.

## Invariants and Constraints

- W1: A write/move/rename/broadcast committed after a waiter's constructor
  returns advances that waiter's queue version, unless the listener records an
  error (which `wait()` then raises).
- W2: A NOTIFY on the schema channel whose payload is a registered queue name
  (pre-change writer) or `*` routes exactly as before.
- W3: Routing remains payload-based; no notification wakes a waiter for a
  queue it did not register (same as today).
- W4: No public signature, backend API version, schema version, configuration
  key or dependency changes. `_should_prepare` prefixes stay matched.
- W5: The listener connection is used only by the listener thread.
- W6: Registry lifecycle, fork recovery, terminal close order ([SB-API-6]) and
  the 4-connection default budget are unchanged; the wake socket pair is owned
  and closed by the listener.
- W7: No change to core, SQLite or Redis code paths.
- Not mocked: PostgreSQL, psycopg, LISTEN/NOTIFY delivery, SQL channel
  computation. Timing control in the race test wraps the real connection's
  cursor only to delay the real LISTEN; the transition table keeps its
  existing transport fake.

## Observable-difference register

| Change | Class | Theory basis |
|--------|-------|--------------|
| Writers NOTIFY the per-queue channel instead of the schema channel | N — same-version waiters wake for exactly the same operations | [THEORY-4] explicit safety: wake hints unchanged in meaning |
| Listener connections no longer receive notifications for unregistered queues (fewer packets, listener-thread wakes, client CPU) | C — no loss | [THEORY-4] concrete pressure (measured fan-out) |
| PostgreSQL ≥ 19: uninterested listener backends are not signaled and run no read transaction | C — no loss | same |
| PostgreSQL ≤ 18: server `xact_commit` per write unchanged (`1 + listening backends in the database`) | N — measured, not improved | — |
| First registration of a queue in a process that already has a listener adds one LISTEN round trip and one server transaction; last release adds one UNLISTEN | D — bounded (one round trip per first registration; one per last release) | [THEORY-4] "Concrete pressure justifies growth" (measured fan-out); owner decision 2, pending |
| A process's first waiter (fresh listener) pays connect + schema-channel LISTEN at startup and then a separate queue-channel LISTEN round trip and server transaction before its constructor returns: two LISTEN transactions where the baseline had one | D — bounded (one extra round trip per fresh listener; not folded into startup because registration follows `acquire`) | [THEORY-4] "Concrete pressure justifies growth"; owner decision 2, pending |
| Registration can raise `OperationalError` if the subscription is not acknowledged within the listener startup timeout (5 s); registration is rolled back | A — mirrors the existing listener-startup timeout instead of degrading silently | [THEORY-4] explicit safety over magical recovery |
| Pre-change writer → post-change waiter: wakes via the retained schema channel | N | — |
| Post-change writer → pre-change waiter: the old waiter is not notified and wakes on its native idle re-poll (default 1–2 s; measured 44–1275 ms) or caller polling until upgraded; no message loss | D during mixed-version rollout only; avoidable by upgrading waiting processes first | [THEORY-4] "Explicit safety over magical recovery": the guarantee is named narrowly (a waiter wake is a hint, delivery is owned by the drain and the named idle re-poll), so the rollout degrades latency, never delivery; contracts [SB-API-6], [SB-DELIVERY-2]. Rollout policy (clean break vs dual-NOTIFY transition) is owner decision 1, pending — not accepted |
| `*` on the schema channel still wakes every waiter | N | — |
| `pg_listening_channels()` on a listener backend lists per-queue channels | C — operator-visible detail | — |
| PostgreSQL ≤ 18 listener backends scan their channel list per queue entry (`IsListeningOn` is linear); cost grows with queues per process | N — microseconds per entry at realistic counts | — |
| Each listener owns one local socket pair (two file descriptors) | N | — |

## Owner decisions

Both decisions are open (pending owner); nothing in this branch accepts them.

1. Mixed-version rollout (pending). Author recommendation: no dual-NOTIFY shim; document "upgrade
   waiting processes before or together with writers" (post-change waiters
   accept both protocols, so listeners-first has no wake gap). Alternative: a
   transition release whose writers NOTIFY both channels, removed one release
   later; it doubles NOTIFY payloads and delays all benefit by a release.
2. PostgreSQL ≤ 18 value (pending): accept that the gain there is client-side
   only (and the LISTEN round trips in the register), with the server-side gain
   arriving with PostgreSQL 19. A PostgreSQL 18 server-side gain needs either
   a durable interest registry (rejected here) or deployment guidance to put
   unrelated brokers in separate databases.

## Alternatives

- Alternative N1: Fixed hashed bucket channels (B channels per schema,
  LISTEN only buckets with registered queues). Bounded LISTEN set per backend,
  but unrelated queues sharing a bucket still wake each other (probability
  ≈ buckets-listened / B per write), and LISTEN churn still exists when a
  bucket gains its first queue. Rejected: strictly less precise than per-queue
  channels for the same subscription machinery; revisit only if PostgreSQL
  shows a per-backend channel-count cost at realistic queue counts.
- Alternative N2: LISTEN every queue channel lazily but never UNLISTEN.
  Rejected: long-lived listeners drift back toward schema-wide delivery.
- Alternative N3: Registering thread executes LISTEN directly on the
  listener connection. Rejected: `notifies()` holds the connection lock for its
  whole wait, so registration either waits up to the 0.1 s poll or forces a
  lock-order hazard with the listener lock; backlog deliveries could also be
  delayed until the next poll.
- Alternative N4: Durable interest registry (table of queues with live
  waiters; writers NOTIFY only when a row exists). Only option that also cuts
  PostgreSQL ≤ 18 server cost, but adds a storage-schema change, a crash
  cleanup lifecycle for stale rows, and a commit-order proof between interest
  publication and writer checks. Out of scope for a wake-protocol change;
  would need its own class-5 plan.
- Alternative N5: Dual NOTIFY (schema + queue channel) as a permanent
  compatibility path. Rejected: every post-change listener must LISTEN the
  schema channel (wildcard, old writers), so it would still receive every
  write and nothing improves.

## Tasks

1. Plan + index row (this file). Gate: `bin/check-plan-context`.
2. RED tests (real PostgreSQL), each confirmed failing on the baseline for the
   intended reason before runtime edits:
   - writer wire test: a raw psycopg session LISTENs the independently
     computed queue channels and the schema channel; write, move, rename and
     exact broadcast notify only the per-queue channels with the queue payload;
   - listener subscription: `pg_listening_channels()` on the listener backend
     is exactly the schema channel plus registered queue channels, and drops a
     channel after its last waiter closes;
   - unrelated writes are not delivered to the listener connection (counting
     wrapper around the real connection's `notifies`) while a related write is
     delivered and wakes;
   - race: with the real per-queue LISTEN delayed, waiter construction does
     not return before LISTEN commits, and a write issued right after it
     returns wakes the waiter;
   - schema-channel compatibility (pre-change writer payload and `*`) for
     single and multi-queue waiters; broadcast wakes every target waiter; a
     write through an alias wakes the canonical queue's waiter only.
   - SM-PG-LISTENER transitions `SUBSCRIBE-QUEUE`, `UNSUBSCRIBE-LAST`,
     `RESUBSCRIBE-DURING-UNLISTEN`, `SUBSCRIBE-TIMEOUT`,
     `SUBSCRIBE-INTERRUPTED` (added for review finding F1) and
     `REGISTER-AFTER-FAILURE` (transport fake extended with a selectable
     descriptor and a statement gate);
   - waiter constructors release their registry reference when registration
     fails (`test_pg_activity_waiter_lifecycle.py`).
   Stop and re-evaluate if any new test passes on the baseline (it would not
   be proving the change).
3. Identifiers + SQL. Stop if `_should_prepare` stops matching.
4. Listener reconciliation, wake socket, registration ack and rollback,
   constructor cleanup on registration failure. Stop if any existing
   SM-PG-LISTENER transition must change meaning.
5. Targeted suites, then the full PostgreSQL extension suite via
   `bin/pytest-pg`, then lint/format/types and doc gates.
6. Measurement: base vs branch, N ∈ {20, 60} listener processes, M = 4
   writers × 500 unrelated writes, plus a related-write latency phase.
7. Docs: extension README (channel and upgrade-order note), CHANGELOG
   Unreleased, implementation docs 06 (rationale) and 07 (SM-PG-LISTENER
   row), spec Related Plans backlink, ruff registry if needed, this plan's
   evidence; commit on the branch.

## Testing Plan

Commands (from the worktree, Docker available):

```bash
uv run --no-sync ./bin/pytest-pg --fast -n 4 extensions/simplebroker_pg/tests/test_pg_notify.py extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py
uv run --no-sync ./bin/pytest-pg -n 4 extensions/simplebroker_pg/tests
uv run --no-sync ruff check . && uv run --no-sync ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests
uv run --no-sync mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
uv run --no-sync mypy extensions/simplebroker_pg/simplebroker_pg $(find extensions/simplebroker_pg/tests -name '*.py' | sort) --config-file pyproject.toml
uv run --no-sync python bin/ruff_suppression_index.py --check
python3 bin/check-dom15-fixtures && bin/check-plan-context && bin/check-doc-paths
```

Anti-mocking: PostgreSQL and psycopg stay real in every wake, wire and race
test; expected channel names are independent literals computed in the test
(md5 over the documented byte layout), never imported from the code under
test.

## Rollback, Rollout and Success Signals

- Rollback: revert the commit. Reverted writers return to the schema channel,
  which post-change and pre-change listeners both LISTEN, so rollback has no
  wake gap in either direction. No data, schema or configuration migration.
- Rollout: release in `simplebroker-pg` (minor). Upgrade waiting processes
  before or together with writers; a writer-first upgrade leaves old waiters on
  idle re-poll latency until they restart.
- Success signals: on PostgreSQL 18, listener-connection deliveries per
  unrelated write drop from N to 0 and listener CPU per write drops; on
  PostgreSQL 19, server `xact_commit` per unrelated write drops toward 1.
  `pg_listening_channels()` on listener backends shows per-queue channels.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Out of Scope

Redis (pattern-subscribes its whole namespace; same client-side fan-out shape),
the native idle re-poll cadence and held connections (another track), write
lock contention (another track), plugin discovery cost (another track), any
PostgreSQL 18 server-side mechanism (Alternative N4).

## Independent Review Loop

- Author family: Claude (implementation agent)
- Reviewer family: pending — coordinator runs the cross-family review
- Review invocation: coordinator-owned per the track brief (`skills/call-agent/SKILL.md`)

## Review and execution record

Tier 1, Codex (codex exec -s read-only, reasoning effort high), 2026-11-08,
scoped-change review of plan+implementation at 8e2d7f24 against 5d29fbf.
Verdict: blocker: F1. The reviewer judged the generation handshake and
socket-pair wake a direct, standard single-owner reconciliation pattern.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F1 | P2 | `register_queue` / `register_queue_set` rolled back only on subscription timeout. An interrupt (reproduced with `KeyboardInterrupt`) during the subscription wait left phantom queue registrations and a fan-in entry on a listener still held by another waiter, so their channels stayed subscribed with no owning waiter. | Accepted, fixed: both methods unwind their registration (`unregister_queue` / `unregister_queue_set`) when awaiting the subscription raises any `BaseException`, then re-raise. New `SM-PG-LISTENER` transition `SUBSCRIBE-INTERRUPTED` (single and set, anchor registration retained, gated LISTEN) and real-PostgreSQL `test_interrupted_waiter_construction_leaves_no_phantom_subscription[single/multi]` (public constructors, anchor waiter retains the listener) assert no phantom refcounts, conditions or fan-in entries, registry refcount 1, and that the orphaned channels end UNLISTENed; all three failed on 8e2d7f24. |
| F2 | P3 | `RESUBSCRIBE-DURING-UNLISTEN` did not prove the registrant reached the subscription wait before UNLISTEN was released; a late-scheduled registrant let the unsafe mutation pass. | Accepted, fixed: the test wraps the subscription condition's `wait` with a witness event and asserts it fired while the UNLISTEN gate is still closed before releasing it. Under the unsafe mutation it now fails deterministically (`registrant took the fast path`, 3/3 runs). |
| F3 | P3 | The difference register omitted the extra queue-LISTEN round trip for a fresh listener, and the mixed-version D row named contracts and policy but no theory principle; rollout policy must stay an open owner decision. | Accepted, fixed: added the fresh-listener row; the mixed-version row now names [THEORY-4] "Explicit safety over magical recovery"; both owner decisions are marked pending, not accepted. |

### Execution Log

Comprehension answers (recorded before runtime edits):

1. A write that commits after `register_queue` returns but before the
   per-queue LISTEN commits is never delivered to the listener session
   (PostgreSQL delivers only notifications committed after LISTEN), and the
   caller's drain may already have run, so only the idle re-poll would find
   it. Registration therefore waits for the listener thread's applied
   generation.
2. A reconciliation pass that is UNLISTENing a channel must remove it from
   the committed set under the lock before executing; otherwise a
   re-registering waiter sees the channel as committed, returns, and loses
   every notification after the UNLISTEN commits.

Premise probe (raw psycopg, `postgres:18.4`, 20 listeners, 200 NOTIFYs):
same-channel listeners 21.00 `xact_commit` per NOTIFY and 4,000 client
deliveries; other-channel listeners 21.00 and 0 (diagnostic probe outside
the repository; method as stated).

RED on the baseline code (`./bin/pytest-pg --fast -n 4`, before runtime edits):

- `test_writer_operations_notify_only_their_queue_channels`: every
  notification arrived on `simplebroker_<md5(schema)>`, none on the per-queue
  channels.
- `test_listener_subscribes_only_registered_queue_channels`:
  `pg_listening_channels()` was only the schema channel.
- `test_unrelated_writes_are_not_delivered_to_the_listener`:
  `assert 20 == 0` (all 20 unrelated notifications delivered).
- `test_waiter_construction_waits_for_its_queue_listen`:
  `assert 4.7e-05 >= 0.5` (construction never subscribed).
- SM-PG-LISTENER `SUBSCRIBE-QUEUE`, `UNSUBSCRIBE-LAST`, `SUBSCRIBE-TIMEOUT`
  failed (no queue LISTEN, no UNLISTEN, no timeout).
- Preservation tests passed on the baseline as intended:
  schema-channel compatibility, broadcast, alias, `REGISTER-AFTER-FAILURE`,
  and every existing SM-PG-LISTENER transition with the extended transport
  fake.

Mutation proofs (each reverted from a saved copy; `grep -c MUTATION` = 0):

- `_await_subscriptions` returns without waiting: the race test fails at
  `elapsed >= delay`; with that assertion also disabled it fails at
  `assert waiter.wait(2.0) is True` (`False`): the write committed after
  construction was never delivered. `SUBSCRIBE-QUEUE`, `UNSUBSCRIBE-LAST` and
  `SUBSCRIBE-TIMEOUT` also fail.
- Channels leave the committed set only after UNLISTEN:
  `RESUBSCRIBE-DURING-UNLISTEN` fails (`[(0, 0)] == []`, registration returned
  on the fast path while UNLISTEN was in flight).
- Constructors without the registry release on registration failure:
  `test_postgres_waiter_registration_failure_releases_listener_reference`
  fails for single and multi (missing `release:` event).

Green (current tree):

- `uv run --no-sync ./bin/pytest-pg --fast -n 4` (shared PostgreSQL suite and
  extension suite, once): `1766 passed, 12 skipped`; `441 passed, 7 skipped`;
  exit 0.
- `uv run --no-sync ./bin/pytest-pg -n 4 extensions/simplebroker_pg/tests`
  after the final lifecycle-test edit: `441 passed, 7 skipped`.
- `ruff check .` clean; `ruff format --check` (CONTRIBUTING path set) clean;
  both CI mypy commands for core and the PostgreSQL extension: no issues;
  `bin/ruff_suppression_index.py --check` exit 0 (no suppression cardinality
  change; `_run` keeps `[RUFF-SUP-019]` and `[RUFF-SUP-004]`);
  `check-dom15-fixtures`, `check-plan-context`, `check-doc-paths` OK;
  `tests/test_plan_context_gate.py`, `tests/test_doc_gates.py` pass.

Existing-test edit: `test_pg_runner_lifecycle.py::test_listener_handles_unknown_registrations_and_surfaces_errors`
hand-builds listener internals; it now also seeds the subscription state so
its registration takes the committed fast path. Its assertions are unchanged.

Measurement (`postgres:18.4` container, `max_connections=400`, host macOS,
psycopg 3.3.8; diagnostic script outside the repository, method below). N listener processes each hold one real SimpleBroker waiter
on its own queue; M = 4 writer processes write 500 messages each to their own
queues (2,000 unrelated writes). Per-write values subtract a control phase
with identical processes and no timed writes. Server counts come from
`pg_stat_database.xact_commit` after every backend exited. Listener
deliveries are counted by wrapping the real listener connection's
`notifies()`; listener CPU is `time.process_time()` summed over listener
processes. Related phase: one writer writes 10 timestamped messages to each
listener queue; latency is write → waiter wake → read.

| Tree | N | server xact/write | listener deliveries/write | listener CPU ms/write | writes/s | related wakes / writes | related deliveries | related p50 / p95 ms |
|------|---|------------------:|--------------------------:|----------------------:|---------:|-----------------------:|-------------------:|---------------------:|
| base | 20 | 21.02 | 20.00 | 0.55 | 680 | 200 / 200 | 4,151 | 1.87 / 4.44 |
| branch | 20 | 21.01 | 0.00 | 0.019 | 694 | 200 / 200 | 200 | 1.83 / 4.95 |
| base | 60 | 61.02, 61.04, 61.01 | 60.0 | 2.42, 2.16, 2.36 | 298, 427, 381 | 600 / 600 | 37,654 | 2.41 / 3.73 |
| branch | 60 | 61.01, 61.00 | 0.00 | 0.039, 0.097 | 651, 401 | 600 / 600 | 600 | 2.10 / 4.84 |

Reading: server transactions per write are unchanged on PostgreSQL 18
(1 writer + N listeners), as the premise probe predicted. Listener deliveries
for unrelated writes drop from N to 0 and listener CPU per write drops by
roughly 25–60×. Writer throughput on this shared host is too noisy to claim
(base 298–427, branch 401–651 writes/s at N = 60). Wake count and latency in
the related phase are unchanged.

Mixed-version probe (diagnostic script outside the repository: a real
`QueueWatcher` in one process and a writer in another, 5 writes 3 s apart,
latency write → handler):

| Watcher | Writer | latencies ms |
|---------|--------|--------------|
| base | base | 27, 14, 9, 5, 14 |
| branch | branch | 17, 4, 9, 27, 9 |
| branch | base | 17, 4, 6, 4, 7 |
| base | branch | 1275, 995, 44, 496, 673 |

Every message was delivered in every combination; only the old-watcher /
new-writer pair falls back to the idle re-poll, matching the register's D
row and the README upgrade-order note.

Not measured: PostgreSQL 19 (needs a `postgres:19` image pull, not authorized
in this track). Expected effect there is server `xact_commit` per unrelated
write falling from `1 + N` toward 1; verify before claiming.

Review round 1 fixes (F1–F3, see the review table above):

- F1 RED on 8e2d7f24 runtime (`./bin/pytest-pg --fast -n 4 ... -k
  "interrupted_waiter or INTERRUPTED"`): transition `SUBSCRIBE-INTERRUPTED`
  `assert {'anchor': 1, 'alpha': 1, 'beta': 1, 'jobs': 1} == {'anchor': 1}`;
  real-PostgreSQL `[single]` `assert {'anchor': 1, 'jobs': 1} == {'anchor': 1}`;
  `[multi]` `assert {'anchor': 1, 'alpha': 1, 'beta': 1, 'jobs': 1} ==
  {'anchor': 1}`. All pass after the unwind.
- F2: under the unsafe "leave the committed set after UNLISTEN" mutation the
  hardened `RESUBSCRIBE-DURING-UNLISTEN` fails 3/3 with
  `registrant took the fast path`; it passes on the real code.
- Reruns after the fixes: `./bin/pytest-pg -n 4 extensions/simplebroker_pg/tests`
  `444 passed, 7 skipped`; shared PostgreSQL state-machine/watcher/waiter
  files (`tests/test_state_machine_policy.py`,
  `tests/test_watcher_transition_tables.py`, `tests/test_activity_waiter_api.py`,
  `tests/test_watcher.py` under `./bin/pytest-pg --fast -n 4`) `71 passed`;
  `tests/test_state_machine_policy.py`, `tests/test_plan_context_gate.py`,
  `tests/test_doc_gates.py` pass; ruff check/format, both CI mypy commands,
  suppression index, `check-dom15-fixtures`, `check-plan-context`,
  `check-doc-paths` clean.


### Cross-family review, round 2

Tier 1, Codex (codex exec -s read-only, reasoning effort high), 2026-11-08,
round-2 verification scoped to F1 over `8e2d7f24..f7d12270`. Verdict: PASS,
no new defect in scope. Both registration methods catch `BaseException` from
the subscription wait, release their registration (the multi-queue path also
removes its fan-in entry while preserving other owners' references) and
re-raise. `SUBSCRIBE-INTERRUPTED` is falsifiable: it failed with either
8e2d7f24 method loaded. Extra public-constructor probes with
`KeyboardInterrupt`, `SystemExit` and `RuntimeError` passed.
