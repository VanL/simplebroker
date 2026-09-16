# Deep-dive review remediation: smallest sufficient fixes

Status: completed
Class: 5 — public behavior and configuration corrections, PostgreSQL connection
ownership, and documentation clarifications. No process or runbook changes.
Owner: SimpleBroker core and first-party backend maintainers.
Plan type: implementation with spec revision; strategy B (atomic text/code/tests)
for runtime units; strategy D for documentation-only clarifications.
Baseline: `b48a642` (8.3.0 plus test stabilization).
Release versions: select during release preparation, accounting for unit 9's
configuration compatibility change. This plan does not authorize publication.

## Goal

Fix demonstrated failures with small changes at their existing owners. Adopt
one bounded PostgreSQL process-session pool with per-operation checkouts, as
selected by the owner. Use documentation where the issue is an existing
backend limitation or caller obligation. Do not turn rare environmental cases,
unsupported iterator use, or cosmetic concerns into new runtime machinery.
The reviewed reproductions establish no acknowledged-message loss or data
corruption from these findings. Redis evidence concerns rejected writes under
contention; PostgreSQL connection sharing was deliberate, not an accidental
implementation mistake.

This completed plan supersedes all earlier proposed remedies. Original unit
numbers are retained for traceability. The historical review below is evidence
of the discussion, not an additional task list.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-4], [THEORY-6],
  [REV-THEORY-004]: explicit failure, queue handles do not own backend stacks,
  and one possession probe at class-5 completion.
- `docs/specs/16-python-library-api.md` [SB-API-2], [SB-API-3], [SB-API-6],
  [SB-API-9], [SB-API-11]: config, session/cache ownership, watcher borrowing,
  errors, and runner teardown.
- `docs/specs/17-ops.md` [SB-OPS-4], [SB-OPS-5], [SB-OPS-7]: rename, aliases,
  and validation-before-cleanup. No cleanup behavior change is planned.
- `docs/specs/11-delivery.md` [SB-DELIVERY-6]: iterator owner and close order.
- `docs/specs/10-cli.md` [SB-CLI-2]: complete-filename guidance.
- `docs/implementation/06-process-session-core-ownership.md` and
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`: ownership
  and transaction rationale to update with their respective code changes.
- `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`, and
  `docs/agent-context/runbooks/testing-patterns.md`: plan and verification rules.

## Scope and Disposition

| Original unit | Resolution | Reason / priority |
|---|---|---|
| 1: watcher stop event | Small runtime fix | Caller-supplied Queue reuse actually fails after watcher stop. |
| 2: PostgreSQL alias locking | Small runtime fix | Conflicting lock order has a concrete transaction path. |
| 3: PostgreSQL connections | Bounded session pool, per-operation checkouts | Owner-selected concurrency improvement; the one substantial design change. |
| 4: Redis conflicts | Small adapter using existing `_retry` | Improve resilience to explicit, safe-to-retry conflicts; exhaustion remains possible. |
| 9: configuration ranges | Existing resolver validates six fields | Earlier, useful errors for invalid configuration; not a data-integrity emergency. |
| 3a: shutdown wording | Documentation only | Preserve iterator-before-owner obligation; do not promise a total shutdown deadline. |
| 5: release records | Correct verified historical facts | No release-script gate, restored API, or retrospective version change. |
| 5b: possession probe | Use existing completion rule | One probe recorded here; no runbook or process change. |
| 6: filename length | Documentation only | Owner's complete-filename rule; no PhaseLock or admission change. |
| 7: late iterator reuse | Defer | Existing close-order contract excludes the reproduction. |
| 8: CLI corners | Defer | Cosmetic or already-correct behavior does not justify this patch. |
| 8b: backend opening failures | One broad documentation sentence | No sidecar detection, cleanup change, or special-case tests. |
| 10: hygiene | Defer general maintenance | Keep only a small user-example correction; no ledger sweep or code cleanup. |

## Context and Key Files

| Owner / files to read before editing | Current behavior and intended change |
|---|---|
| `simplebroker/watcher.py`; `tests/test_watcher_cleanup.py`; `tests/test_watcher_stop_contract.py` | The watcher replaces a borrowed Queue's stop event. Restore its previous event through existing cleanup. |
| `simplebroker/db.py`; `extensions/simplebroker_pg/simplebroker_pg/plugin.py`; `extensions/simplebroker_pg/tests/test_pg_queue_rename.py` | Rename locks metadata before aliases; add does the reverse; remove deletes an alias row before metadata. Use the existing alias hook before either resource. |
| `simplebroker/_broker_session.py`; `simplebroker/db.py`; `extensions/simplebroker_pg/simplebroker_pg/runner.py`; `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`; `tests/test_process_broker_session.py` | The session caches cores per thread, but PG operations shared one leased connection. Make ordinary checkouts operation-scoped while retaining explicit connection pinning for vacuum. Read every use of `_leased_conn`, `_lease_depth`, and `_leased_operation_lock`, including failures, vacuum, fork, and shutdown. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py`; `extensions/simplebroker_redis/simplebroker_redis/scripts.py`; `simplebroker/_retry.py`; `simplebroker/_retry_policy.py`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py` | Short hand-written conflict loops reject writes. Redis interprets results; the existing retry engine supplies bounded waits. |
| `simplebroker/_constants.py`; `tests/test_constants.py`; `tests/test_invalid_config_lifecycle.py`; `docs/guides/configuration.md` | Five integer coercers lack range checks; `_sync_mode` silently falls back. Validate once in the field table. |
| `CHANGELOG.md`; `docs/guides/python.md` | Correct release attribution from tags and simplify the peek-watcher example to a watcher-owned Queue. |

Before runtime edits, record answers to these comprehension checks here. A
wrong answer requires rereading the named owner before implementation:

1. What does watcher cleanup restore? The previous event, not necessarily a
   fresh unset event. A caller's existing shutdown signal must retain meaning.
2. What bounds PG resources? One pool per session key, default maximum 3,
   with one checkout per active operation and a separate listener. One
   process-session target therefore has a four-connection default ceiling.
   Queue and idle-thread counts do not multiply checkouts; transactions and
   suspended iterators retain their checkout until settlement.
3. Why can Redis retry a conflict but not a lost response? The enumerated
   conflict results occur before mutation. A lost response may follow a commit.

## Invariants and Constraints

- Borrowed Queue leases remain open. Internally constructed Queues remain
  watcher-owned. Restore stop state only when the existing cleanup owner
  releases the watcher; a timed-out join does not transfer cleanup ownership.
- Alias-changing PG operations take the existing aliases advisory lock before
  metadata or alias-row locks. Keep transaction boundaries and retry policy.
  This removes the identified inversion, not every possible database deadlock.
- PG checkouts belong to active operations, not Queues or idle thread caches.
  Use a default pool cap of 3, preserve the 30-second timeout and constructor
  overrides, and keep the listener separate. Never fall back to sharing
  another operation's connection after pool exhaustion.
- Separate PG connections remove the client-side shared-connection bottleneck.
  Database locks, limited pool capacity, and application dependencies can
  still block progress. SQLite keeps per-thread connections; Redis keeps its
  process-session command pool and separate listener.
- Preserve normal core-close-before-runner-shutdown ordering and fork recovery.
  A drain deadline expiring does not prove an operation stopped or its use was
  unsupported. Do not add abandoned-iterator recovery or shutdown promises.
- Redis retries only definite no-mutation conflicts. Keep the write lock, Lua
  protocols, success bookkeeping, and public exhaustion error. No transport
  retries are added by this unit.
- Range validation happens once in the resolver. Preserve existing integer
  coercion and case-insensitive valid sync modes. No defensive pragma checks.
- No storage migration, new dependency, public config key, CLI parser change,
  PhaseLock change, cleanup change, release gate, or general maintenance sweep.

## Spec Baseline and Proposed Spec Delta

Baseline `b48a642`. Runtime text, code, tests, and verification mappings land
atomically per unit. Documentation clarifications use strategy D. Do not put
PG-specific lock implementation details or blanket deadlock guarantees in the
backend-neutral product spec; record the lock order in implementation notes.

**[SB-API-3], unit 3:** replace the first sentence of the persistent-session
cache paragraph with the following, retaining the existing close/recycle rules:

> For a persistent Queue using a process-shared session, each thread that uses
> the Queue caches one core for that session. SQLite uses a connection per
> thread. PostgreSQL borrows a checkout from a bounded session pool for each
> operation and retains it only while a transaction or suspended iterator needs
> connection continuity. Redis borrows command connections from its session
> pool. PostgreSQL checkout exhaustion raises after the pool timeout; an idle
> thread-local core does not consume a pool slot.

**[SB-API-3], unit 3a:** append to the same lifecycle paragraph:

> The bounded drain is not a deadline for all shutdown work. Close iterators
> before closing their owner; disposing a core can wait for its active operation.

**[SB-API-2], unit 9:** append after the relative-location invalid-value rule:

> A value outside a field's documented range is an invalid value: a negative
> `BUSY_TIMEOUT` or `WAL_AUTOCHECKPOINT`, a non-positive `CACHE_MB`,
> `MAX_MESSAGE_SIZE`, or `READ_COMMIT_INTERVAL`, or a `SYNC_MODE` other than
> `FULL`, `NORMAL`, or `OFF` after case normalization. These fields do not
> substitute defaults for invalid values.

**[SB-CLI-2], unit 6:** retain the owner-approved sentence already added:

> Choose a database name so that every complete filename, including any
> SimpleBroker sidecar suffix, is fewer than 255 characters long. This limit
> applies to the filename component, not the full directory path.

**[SB-API-9], unit 8b:** retain only the owner-approved broad statement:

> Backend-specific issues preventing the successful opening of the database
> connection will cause operations to fail.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|---|---|---|---|---|
| [SB-API-3] | Per-thread cached core; backend-specific checkout lifetime | PG shares one runner and bounded pool; Redis pools commands | Backend ownership differs; PG change now selected explicitly | Backend-specific wording above, with PG implementation |

## Tasks

Keep runtime units independently reviewable. Unit 2 spans core and PG and must
be tested as one change. Unit 3a accompanies unit 3's lifecycle documentation.
Other units need no artificial serial dependency. Documentation units do not
require new runtime tests or behavior-change release notes.

### Unit 1 — restore a borrowed Queue's stop event

Capture the prior Queue event before installing the watcher's event. Restore
it through `Queue.set_stop_event` in the existing idle/run cleanup paths,
including ordinary cleanup failure handling. Do not add lifecycle states or
reach into connection internals. Session acquisition already reapplies the
manager's stop event to the acquiring thread's core.

Tests: actual write/read after idle stop and run stop, including a second
thread that previously used the Queue; preservation of a caller's original
stop event; restoration after another cleanup action fails; internally owned
Queue still closes. Keep existing start/stop race tests. Update [SB-API-6]'s
verification mapping and CHANGELOG. Check Weft's existing detach-stop-event
workaround; removing it is a separate downstream change. Reassess if the fix
requires a new watcher ownership state.

### Unit 2 — remove the PostgreSQL alias lock inversion

In PG `prepare_queue_operation(operation="rename")`, acquire the existing
alias lock before `LOCK_LAST_TS_ROW`. In core `remove_alias`, call the existing
optional `prepare_alias_mutation` hook before DELETE, as add already does.
Keep the later rename acquisition if convenient; the transaction advisory lock
is reentrant. Record the order in the storage implementation notes and the
fix in CHANGELOG. No new locks or retry layer.

Use two real-PG coordinated regressions. Pause add after its alias lock, before
metadata; start rename, signal just before it requests the alias lock, then
release add. Baseline rename already holds metadata; fixed rename does not.
For remove, pause after deleting an alias targeting the renamed source, before
metadata, and use the same rename-request signal. Use populated source queues.
Do not require both workers to pass locks that become mutually exclusive after
the fix. Record actual driver SQLSTATE `40P01` through transparent wrappers:
existing retry can hide a deadlock from the final public result. After the fix,
assert zero recorded deadlocks and correct messages/aliases. Retain existing
rename/alias suites and check the optional hook leaves other backends unchanged.
Reassess only if another concrete lock cycle appears; do not promise universal
PG deadlock freedom.

### Unit 3 — bounded PostgreSQL session pool with per-operation checkouts

Preserve one pool per process-session key, use a default maximum of 3 and the
30-second checkout timeout, keep existing constructor overrides, and keep the
listener separate. This gives one process-session target a default ceiling of
four PostgreSQL connections. Deployment sizing starts from the database-wide
connection budget and maximum broker process count. Acquire lazily for each
operation and return the checkout when the operation completes. An open
transaction or suspended iterator retains its checkout until commit, rollback,
or close. Idle thread-local cores consume no pool slots. Pool exhaustion
surfaces as an error after the existing timeout.

Process-session core construction must select the operation-scoped path rather
than the runner's explicit connection pin. Preserve the explicit pin for
vacuum, whose session advisory lock spans several transactions. Track whether
core construction actually acquired a pin so `BrokerCore.close()` releases
only what it owns and never closes a caller-supplied shared runner.

Replace the global explicit leased connection/depth with a `_lease_lock`-
guarded registry keyed by `threading.Thread`, with connection and lease depth
per entry. This registry serves vacuum and any other explicit internal pin; it
does not hold ordinary process-session checkouts. Preserve nested pins, avoid
reusable integer thread IDs, and call blocking `getconn` outside the registry
mutex.

Discarding a failed connection returns its pool slot and clears transaction
state while retaining an explicit pin's logical depth, so a later operation can
obtain a replacement. Remove the global operation lock because distinct active
operations use distinct checkouts. Fork resets inherited pool, registry, and
lock state. Terminal sweep detaches explicit pins before returning connections;
an already-detached pin cannot be returned twice by a later release.

Before editing, trace normal release, failure discard, and terminal shutdown
and record how each reaches the same ownership bookkeeping. Session shutdown
closes admission, performs its bounded drain, then disposes cached cores before
closing the runner. Core disposal takes the core operation lock and can wait
for a slow operation even after the drain deadline. Core creation already
coordinates deferred factory shutdown. Preserve these safeguards; do not use
"drain expired" to justify returning an in-use connection. Standalone runner
owners must finish users before closing; concurrent standalone shutdown is not
a new cancellation facility. Reassess if this ownership change requires a new
shutdown state machine or changes supported same-thread transaction behavior.

Tests: the unchanged 20-thread watcher pre-check completes through a three-slot
pool without caller recycling; an ordinary completed operation returns its
checkout; and B completes before A resumes a suspended at-least-once iterator.
Use Events and generous liveness timeouts. A one-slot pool reports exhaustion
only while another operation or explicit pin actively holds its slot, then lets
the waiter proceed after release. Check explicit-pin successor identity, nested
pins, discard/replacement, interruption between checkout and publication,
teardown without duplicate returns, vacuum, and fork. Close iterators before
owners. Run real-PG integration in addition to focused lifecycle tests.

Update [SB-API-3], the process-session implementation notes, PG README and
CHANGELOG with resource bounds, operation lifetime, and remaining contention.
Unit 3a adds only the close-order clarification above; no terminal code change.

### Unit 4 — Redis conflict retries through the existing engine

Keep result interpretation and timestamp recovery in the Redis core. Use one
small adapter around `execute_retry`, with a private conflict exception and
`retry_on` matching only it. Reuse `expo`, default jitter, `stop_after_delay`,
and `max_delay` for remaining-budget sleep clamping; no hand-written backoff.
Initial private policy: 30-second elapsed budget, exponential factor 1 ms,
base 2, per-sleep cap 250 ms. The elapsed budget and sleep cap follow the
existing SQLite retry policy;
validate against the existing contention workload, rather than adding a new
benchmark or instrumentation project. The budget bounds conflict retries, not
the duration of a blocking transport operation.

ID collisions advance that bounded backoff sequence. A stale high-water fence
refreshes and retries immediately without consuming the ID-collision sequence;
sleeping after the refresh lets a peer make the next candidate stale before it
is reserved. Pass the Redis core's stop event into retry sleeps and translate
interruption through the existing operation-specific exhaustion error so
shutdown is not held behind the full conflict budget.

| Path | Definite no-mutation conflict | Recovery before another attempt |
|---|---|---|
| Single write | `-1`, `-6` | Resync for ID collision; refresh last timestamp for stale fence |
| Atomic broadcast | `-1`, `-3`, `-6` | Regenerate candidates; resync or refresh as above |
| Pattern broadcast | Existing `insert_messages` conflict `IntegrityError` from `-1`/`-3` | Regenerate records after resync; do not catch unrelated integrity failures |

Confirm each classification against the actual Lua mutation boundary. Preserve
`_ts_conflict_count`, notifications, maintenance accounting, and existing public
`RuntimeError` on exhaustion. Keep queue-growth `-4` separately attempt-capped.
Do not retry connection errors, lost responses, or post-success bookkeeping.
The process write lock stays held for the whole single-write retry operation,
potentially the full budget; sibling writers therefore wait too. Document this
latency tradeoff and allow explicit exhaustion under sustained contention.

Tests cover each listed conflict path, successful recovery beyond three
conflicts, bounded exhaustion, accounting, and a connection error attempted
only once. Inject only at the eval/clock/sleep seam; keep actual Redis execution
in integration tests. For a real-Valkey contention run record successes,
rejections, elapsed time, and pending-message accounting. Improvement under that
workload is evidence, not a guarantee of zero rejections under every load.
Update Redis README and CHANGELOG. Reassess if retries require Lua protocol or
write-lock changes; neither is part of this unit.

### Unit 9 — validate configuration ranges once

In the existing `ConfigField` validators, require `BUSY_TIMEOUT` and
`WAL_AUTOCHECKPOINT` to be non-negative; require `CACHE_MB`, `MAX_MESSAGE_SIZE`,
and `READ_COMMIT_INTERVAL` to be positive. Preserve current `int(value)` coercion
before checking ranges. The existing `_load_max_future_skew` helper rejects
additional input types, so do not blindly reuse it and silently widen scope.
Use small local range validators, not a new validation framework.

Make `_sync_mode` reject values other than FULL/NORMAL/OFF after existing case
normalization. Use the resolver's existing source-warning and effective-invalid
`InvalidConfigError` path. Keep `WAL_AUTOCHECKPOINT=0` and lowercase valid modes.
Do not add downstream pragma guards. Document the six ranges in the config
guide and [SB-API-2], and explicitly record removal of invalid-mode fallback in
CHANGELOG; choose the release version for this actual compatibility change.

Test each field below its bound, valid boundary values and modes, invalid sync
mode, repair by a later valid source, existing integer-coercion compatibility,
and CLI error-before-target-open through the existing invalid-config harness.
The `CACHE_MB=-1` reproduction must fail during config admission, not in SQL.
Reassess if this requires changing resolver precedence or unrelated fields.

### Documentation and completion units

- **5:** correct CHANGELOG facts only. Confirm all three 8.2.0 removed exports
  (`ResolvedConfig`, `resolve_isolated_config`, `snapshot_config`) against
  `v8.1.1` and label the removals accurately. Restore the missing 7.3.2 entry
  from its actual tag history; separate later 7.4.0 additions rather than moving
  today's whole bullet. Correcting the record does not undo a breaking release.
  No release-script changes or restoration of old APIs.
- **6 and 8b:** retain exactly the two owner-approved statements above. No
  special-case code or tests for long filenames or externally malformed sidecars.
- **10, limited:** change the peek-watcher example in `docs/guides/python.md`
  from a caller-created ephemeral `Queue("tasks")` to `queue="tasks"`, letting
  the watcher construct and close its persistent Queue. No general docs sweep.
- **5b:** at completion, perform one [THEORY-6] possession probe and record the
  result here. Existing policy already requires it; add no runbook or gate.

## Testing Plan and Verification

For runtime fixes, first capture the focused failing behavior at the baseline,
then verify the changed behavior. Keep real databases and ownership paths in
integration tests. Transparent synchronization wrappers and targeted fault
injection are allowed; replacing the runner, pool, or Lua behavior is not proof
of concurrency correctness. Existing unit-level fake-pool tests remain useful
for failure bookkeeping. Do not add tests merely to mirror documentation.

Run the affected unit's focused tests during implementation. Before closing the
combined work, run core and both backend suites with their real dependencies,
plus the existing lint/type gates. Inspect the actual scripts for environment
requirements and record commands, results, and any unavailable dependency.

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
uv run --locked pytest -n 0 tests/test_watcher_cleanup.py tests/test_watcher_stop_contract.py tests/test_process_broker_session.py tests/test_constants.py tests/test_invalid_config_lifecycle.py
uv run --locked bin/pytest-pg -n 0
uv run --locked bin/pytest-redis -n 0
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check simplebroker tests extensions/simplebroker_pg extensions/simplebroker_redis
uv run --locked mypy simplebroker
uv run --locked python bin/ruff_suppression_index.py --check
```

Downstream: inspect Weft's connection/watcher use before runtime implementation;
run its `tests/core/test_task_runtime_connections.py` and relevant multi-queue
watcher tests against the candidate through its own environment, recording the
imported SimpleBroker path. Do not remove its workaround in this repository.
For unit 9, exercise each changed CLI config field through the existing admission
harness. There is no CLI grammar or output-format change to audit in unit 8.

## Rollout, Rollback, and Success Signals

No schema or persisted-format changes. Configuration acceptance changes in unit
9; document it explicitly. PG connection use can rise to the session pool cap,
plus the listener; multiple session keys have separate pools. Active operations
wait when the pool is full; idle worker cores hold no slots. Redis retries can
increase write latency up to the stated retry budget, excluding transport time.

Keep each runtime unit reversible before release. Unit 2's core hook and PG
ordering must travel together for the complete fix. Reverting unit 3 restores
shared-connection serialization; reverting unit 4 restores shorter conflict
retries; reverting unit 9 restores the previous invalid-value behavior. None
requires a data migration. After publication use corrective releases, not tag
rewrites. Before release, reconcile dependency floors and follow the existing
`bin/release.py` driver, which requires extension baselines published before the
core release; do not invent a parallel release gate. Publication is separate.

Observable success: borrowed Queue reuse works without detachment workarounds;
the coordinated PG alias regression records no deadlock; independent PG threads
progress within pool capacity and completed operations return their slots; Redis shows safe
conflict recovery and exact success accounting; invalid config fails at admission.
These are bounded claims, not absence-of-failure promises under arbitrary load.

## Independent Review and Closeout

Independent review covers the rewritten plan and each coherent runtime slice,
with special attention to PG release/discard/shutdown ownership and Redis's
no-mutation retry boundary. Prefer a different reviewer family when available;
record findings and their dispositions. At final review reconcile spec deltas,
implementation notes, changed tests, downstream evidence, and release notes.
Run the existing possession probe once. No new review or release tooling.

The writing-plans and hardening runbooks already call for narrow scope and
existing-owner reuse; no runbook change is needed. At implementation closeout,
record any new durable lesson rather than mechanically restating this plan.
Close the status-index row only when the planned work is implemented, verified,
and committed under the repository's completion rules. This revision is a plan
update, not a claim that runtime work is complete.

## Deferred and Out of Scope

- **7:** no closed-runner sentinel, post-shutdown iterator guard, or recovery
  for abandoned transactions. Reopen only for a supported close-order failure
  or an explicit requirement to support the presently excluded use. This is a
  product-behavior deferral, not a coalescing-corpus deferral;
  `docs/coalescing.md` tracks plan, lesson, and promotion sweep conditions, so
  unit 7 does not get a row in that table.
- **8:** no warning transport replacement, argparse hints/epilog, error-label
  cleanup, or new empty-stdin tests. Existing warning behavior includes repeated
  invocation semantics and should not be altered for cosmetic reasons.
- **10:** no stale-pin/coalescing sweep, C901 count cleanup, dead-helper removal,
  unrelated test rewrite, or watcher-restart promise. The proposed claim that
  automatic vacuum never runs on CLI/ephemeral paths was not supported by code
  and is dropped, not added to documentation. After `stop()` restores a
  caller-supplied Queue's prior stop event, create a new watcher to run again;
  restarting the stopped watcher does not rewire that Queue and remains outside
  the supported lifecycle.
- No filename enforcement, PhaseLock retry changes, sidecar troubleshooting
  catalogue, special cleanup diagnostics, immutable validation, or SQLite/Redis
  connection-ownership redesign. Unit 8b is only the broad sentence above.

## Historical Review (Superseded Proposals)

The following records preserve prior review evidence. Their proposed remedies,
severity labels, fixed timings, and mandatory gates are superseded by the active
scope above. In particular they do not authorize a sentinel, immutable open,
filename admission limit, special sidecar handling, or process changes.


2026-09-16, plan review round 1: independent read-only review (same family,
different session; a different-family pass is still required before
implementation). Verdict **BLOCKED** on units 3, 7, and 8 plus two delta
corrections. Every finding was verified against the code by the author before
disposition; all nine are accepted and the plan text above is the revised
version.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F1 | P1 | Unit 8 claimed `mode=ro` would stop the cleanup preflight creating `-wal`; the preflight already uses `mode=ro` (`validation.py:52`) and a probe on SQLite 3.50 shows it still creates `-wal` and `-shm`; only `immutable=1` creates nothing. | Accepted: unit 8 now specifies `immutable=1` with the locking tradeoff bounded to cleanup by [SB-OPS-7]. |
| F2 | P1 | Unit 3 said thread-local lease state; terminal `close_all` closes other threads' cores from the closing thread, so thread-local state cannot be swept. | Accepted: unit 3 now specifies a `_lease_lock`-guarded registry keyed by thread ident, calling-thread-only release, and a shutdown sweep that returns and rolls back. |
| F3 | P1 | Unit 3 left the fate of `_leased_operation_lock` to the implementer; it also guards `shutdown` and `release` against an in-flight transaction. | Accepted: removed, with the `in_transaction` registry field and the shutdown sweep taking over that role; the 842 retained acquisition deleted with it. |
| F4 | P2 | Pool exhaustion test would wait on the hard-coded 30 s pool timeout. | Accepted: the test patches the pool instance's timeout, never the default. |
| F5 | P1 | Unit 7's "operation entry raises" had no named seam; `_session_managed` is non-SQLite only and reusing it changes `close()`. | Accepted: a factory-installed `_ClosedRunner` sentinel replaces the runner after terminal shutdown; the probe-flag alternative is recorded as rejected. |
| F6 | P2 | Unit 4 named the wrong module for the deadline constants and left the value unstated; starvation under `_write_lock` unaddressed. | Accepted: local constants mirroring `db.py:213-214`, `apply_jitter` from `_retry.__all__`, 30 s value and the tradeoff stated, broadcast loops included, growth loop excluded. |
| F7 | P2 | Unit 6's limit was unstated and the test did not pin the boundary; `_db_name_path` is in `_constants.py`. | Accepted: 216-byte limit derived from the status temp form, tests at 216 and 217, file corrected. |
| F8 | P2 | The [SB-API-6] restart sentence overpromised: a released watcher with a cleared stop event runs again. | Accepted: sentence qualified; unit 10's SM row must match. Also moved the fail-fast sentence from [SB-CLI-2] to [SB-API-11]. |
| F9 | P3 | Nine stale pin labels, not eight; four cues unmapped. | Accepted: counts corrected, mapping recipe given, fourth cue must be found not guessed. |

Round-1 observations without a disposition needed: unit 1's cross-thread
boundary is now stated in the unit; unit 5's 7.3.2 content and gate sites are
named; class 5 with hardening and the 3+P runbook edit were confirmed correct.

2026-09-16, plan review round 2 (owner-supplied, different family): verdict
**BLOCKED** on seven findings. Each was reproduced or read against the code by
the author before disposition; all seven are accepted.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| R2-1 | P1 | Unit 8 unlinked dangling sidecars before validation, violating [SB-OPS-7] "failed validation leaves the whole namespace untouched" (`17-ops.md:205`). | Accepted: cleanup split into unit 8b; no unlink before validation; a dangling or non-regular owned entry aborts with a diagnostic naming the path; rejection with dangling sidecars present is a required test. |
| R2-2 | P1 | `immutable=1` bypasses the WAL as well as locks; reproduced: schema living only in an un-checkpointed WAL validates under `mode=ro` and reports `no such table` under `immutable=1`. | Accepted: `immutable=1` withdrawn everywhere; the preflight keeps `mode=ro`. |
| R2-3 | P1 | Terminal cleanup cannot reach the proposed runner sweep: `close_all` calls `core.close()` first, which takes the core lock a suspended transactional iterator holds. Reproduced on SQLite: `close_all` with a 0.3 s drain budget did not return until the iterator closed. | Accepted as a code path; scope narrowed on owner direction: the sequence violates the existing iterator-before-owner rule, so unit 3a narrows the promise ([SB-API-11] clarification, strategy D) and adds a supported-sequence test; no shutdown redesign. An abandoned set, an owner-thread hook, and then a bounded lock acquisition were each drafted and withdrawn. |
| R2-4 | P2 (downgraded from P1 on owner direction: a design question, not a reproduced defect) | An `in_transaction` field does not replace synchronization; what protects connection use once `_leased_operation_lock` is gone was unstated; `getconn` under the registry mutex could starve. | Accepted as a question and answered in unit 3: per-thread exclusivity plus the drain's zero-active-operations observation mean no statement is in flight when `shutdown` returns connections; `getconn` moves outside `_lease_lock`. Per-entry locks and a revocation flag were drafted and withdrawn; either requires a failing sequence within supported usage. |
| R2-5 | P2 | `get_ident()` is reused after thread exit while the session retains caches, so a successor thread could inherit a lease. | Accepted with the small remedy: key the dict by the `threading.Thread` object; thread-replacement test required. A token scheme was drafted and withdrawn. |
| R2-6 | P2 | The closed-runner sentinel cannot make "next advancement fails" true for buffered yields; reproduced with a peek iterator returning a buffered item after shutdown. | Accepted: the stronger guarantee was newly proposed and is withdrawn; the [SB-DELIVERY-6] delta promises only no reconnection, which the sentinel enforces without checks at advancement. The test asserts buffered yield, then the closed-session error on the next fetch, then zero descriptors. |
| R2-7 | P2 | The 216-byte limit applied to every compound-path component would reject valid directory names. | Accepted: limit applied to the terminal filename component only; `<217-byte directory>/broker.db` is a required passing case. |

Round-2 direction adopted: unit 8's wording changes and the destructive
cleanup change are separate units; every concurrency unit carries a stress
test alongside its deterministic failure-sequence tests.

2026-09-16, round-2 validation and owner scope direction. Each round-2
finding was re-validated by the author: R2-1 by the spec text
(`17-ops.md:205`); R2-2 by a probe that SIGKILLs a broker process after five
writes and finds 131 KB of committed rows only in the WAL, which `mode=ro`
validation reads and `immutable=1` cannot (the reviewer's original wording
implied the broker closes and reopens between setup phases; it does not: the
runner connection persists across setup and writes and the temporary setup
connection at `runtime.py:107` is closed without replacing it; the finding
stands on the crash case alone); R2-3 by the hang reproduction; R2-4 by
reading `_get_thread_conn` (`getconn` under `_lease_lock` at 731-735); R2-5
as a Python fact; R2-6 by the reviewer's probe; R2-7 by `_paths.py:181`
applying the component rule per part. Owner direction then tightened three
scopes: narrow the terminal-drain promise instead of redesigning shutdown
(R2-3), keep the no-reconnection guarantee only (R2-6), and treat R2-4 as a
design question answered by the drain argument rather than new machinery.

2026-09-16, round-3 owner direction (ten points, applied). Fix the
demonstrated failures, preserve existing ownership boundaries, add no
guarantee the product does not need; one clear owner and one consistent path
even at the cost of a little more code. Changes: unit 1 keeps restoration in
the existing cleanup path including its failure handling, with no new watcher
state; unit 2 states the rule as alias lock before metadata locks in every
alias-changing operation and adds an Event-coordinated conflicting-order
test, with stress as supplementary evidence; unit 3 must record how normal
release and terminal shutdown interact before implementation and may not
claim separate connections remove database contention; unit 4 is rebuilt on
`execute_retry` with the Redis core owning conflict recognition and recovery,
retrying only explicit no-mutation conflict results, measuring contention
before the budget is fixed, and describing the baseline as rejected writes;
unit 5 is split so the possession probe (5b) cannot delay runtime fixes;
unit 6 classifies at the existing retry boundary with every declared errno
category tested and no new abstraction; unit 7 is deferred with two reopen
conditions and its sentinel withdrawn, so no [SB-DELIVERY-6] delta lands;
unit 8b becomes a targeted diagnostic for the dangling-link case with no
blanket sidecar rule, independent of unit 8's wording; unit 9 validates once
at resolution and records the fallback removal as an explicit compatibility
change; unit 10 lands alone. Unit 3 remains the one substantial design task
and still requires a different-family review pass before implementation.

## Execution Log

- 2026-09-16: Independent scoped reviews of active units 1–3 and of units
  4/9/documentation passed. Clarified that only the Redis elapsed budget and
  sleep cap are borrowed from SQLite's policy, not its initial delay.
  `check-dom15-fixtures`, `check-plan-context`, `check-doc-paths`, and
  `git diff --check` passed for the revised plan. Runtime implementation and
  its tests remain future work.

- 2026-09-16: Owner requested the smallest best resolutions across the whole
  plan. Rewrote active scope to five runtime units (1, 2, 3, 4, 9), limited
  documentation, and explicit deferrals. Incorporated independent PG and
  Redis/config review: valid alias test scheduling, actual teardown ordering,
  all safe conflict codes, whole-budget write-lock cost, range-only coercion,
  and verified release attribution. Removed unsupported auto-vacuum claims.
  Original review history is retained only as superseded evidence.


- 2026-09-16: Owner narrowed unit 8b to one broad documentation statement:
  "Backend-specific issues preventing the successful opening of the database
  connection will cause operations to fail." All earlier sidecar-specific
  diagnostics, cleanup changes, and special-case tests in the review history
  are superseded. No runtime changes are authorized by this unit.

- 2026-09-16: Owner selected a bounded process-session pool with per-thread
  checkouts for PostgreSQL. This preserves sharing across Queues while
  allowing independent transactions on worker threads. Unit 3 now records
  the pool bound, checkout timeout, explicit recycling, and the remaining
  database-lock and pool-capacity limits. SQLite's per-thread connections
  and Redis's command pool remain unchanged. This records the design
  decision only; no runtime implementation was made. Independent scoped
  review passed after clarifying that 16 is the default pool maximum;
  documentation and whitespace checks passed.

- 2026-09-16: Owner narrowed unit 6 to documentation only: the complete
  filename, including its sidecar suffix, must be fewer than 255 characters.
  This supersedes earlier 216-byte admission and fail-fast retry proposals
  in the review history. No filename-validation or phase-lock change is
  authorized by this unit. Independent scoped review of the proposed note
  passed: it states naming guidance without promising new enforcement.

- 2026-09-16: Owner decision on unit 9: "a range-invalid config value is an
  invalid value." Unit 9 rewritten from owner-gated to specified; [SB-API-2]
  delta added; release version left to closeout. No code changed.

- 2026-09-16: During implementation, owner set the PostgreSQL pool default to
  3. With the separate shared listener, one process-session target has a
  four-connection default ceiling. This replaces the earlier default of 16;
  deployment sizing is governed by the database-wide connection budget and
  maximum broker process count. Weft's TaskMonitor has at most three relevant
  broker-using threads: its reactor and two worker-local durable-effect lanes.

- 2026-09-16: Owner rejected thread-lifetime checkout retention after the
  unchanged 20-thread watcher regression exposed permanent pool-slot capture by
  idle worker cores. Unit 3 now uses per-operation checkout and retains a
  connection only for an active transaction, suspended iterator, or explicit
  internal maintenance pin. The test-side recycling workaround was reverted;
  the original watcher concurrency test passes through the three-slot pool.

- 2026-09-16: Acceptance runs use each repository harness's default parallel
  worker configuration. A serial PostgreSQL run remains supplementary evidence;
  serial Redis verification was stopped. `-n 0` is reserved for isolating a
  specific failure because it can hide concurrency regressions.

- 2026-09-16: Unit 1 restores the caller-supplied Queue's prior stop event so
  the Queue remains usable after watcher cleanup. It does not make the watcher
  restartable. The existing
  `test_run_after_stop_is_a_noop_and_does_not_resurrect_resources` contract
  keeps a released watcher terminal, so no watcher-restart promise or test is
  added here.

- 2026-09-16: Unit 7 remains a recorded deferral because its reproduction
  violates the existing iterator-before-owner close order. Retaining that
  decision in this completed plan is intentional traceability, not a request for
  runtime work or an in-task coalescing sweep. Reconsider it only with a
  supported close-order failure or an explicit contract change; ordinary plan
  coalescing remains a separately authorized maintenance action.

- 2026-09-16: Final PostgreSQL lifecycle review kept five small corrections in
  Unit 3: pool checkout timeout is translated to the public operational error;
  unmatched release cannot return a live transaction; final explicit release
  clears thread transaction markers; an ordinary failed checkout is returned
  before bootstrap invalidation takes setup locks; and a real suspended claim
  iterator asserts checkout retention followed by return on close. These close
  failure-order and evidence gaps without changing the pool design.

- 2026-09-16: Final Redis contention review reproduced starvation when a
  clock-lagged writer slept after refreshing a stale fence. Unit 4 now retries
  `-6` immediately without advancing the ID-collision backoff, keeps backoff for
  `-1`/`-3`, and makes every retry sleep observe the core stop event. Real eval
  seam tests cover conflict mapping and interruption; a fake monotonic clock
  proves nonzero waits accumulate against the elapsed budget.

- 2026-09-16: Re-evaluated every suppression added by this remediation before
  updating the registry. Future-based thread probes weakened bounded liveness
  because executor shutdown can wait after `Future.result()` times out; a
  shared capture helper separated actor cleanup from its failure result. A
  Redis helper plus mutable retry-state object reduced local complexity but
  split the one-use Lua result protocol across owners. An alias-test
  coordinator reduced the test function's score but carried two runners,
  three events, mutation and pause state, and failure lists away from the
  causal assertions. The existing local forms were smaller and kept state and
  cleanup with their sole owners. Independent re-review agreed on simplicity,
  maintainability, locality, and bounded-liveness grounds.

- 2026-09-16: [THEORY-6] possession probe: asked where automatic PostgreSQL
  failover and cluster-membership ownership would belong. The program account
  predicts the substrate or operator, not SimpleBroker; placing it here would
  introduce hidden topology, replication, and reconnect state that a review
  should flag before implementation. The current pool change stays within the
  process-session resource boundary, so the probe passes.

- 2026-09-16: Acceptance evidence used standard concurrency. `bin/pytest-pg`
  passed 1,742 shared and 340 extension tests; `bin/pytest-redis` passed 1,734
  shared and 375 extension tests. After the Ruff suppression index was
  reconciled, the complete ordinary suite passed 3,907 tests. Ruff
  format/check, mypy, DOM-15, plan-context,
  doc-path, suppression-index, and whitespace gates passed. A local-editable
  downstream Weft PostgreSQL run passed 251 targeted TaskMonitor, monitor-store,
  runtime-connection, and multi-queue watcher tests with one SQLite-only skip.
  Final diff review found no added skip or xfail and confirmed that removed
  assertions described the deleted global checkout lock; replacement tests
  cover operation return, transaction continuity, pool exhaustion and reuse,
  independent progress, cleanup, and deadlock SQLSTATE absence.

- 2026-09-16: Final independent Redis review found that an immediate stale-fence
  retry could bypass stop observation because it does not sleep. The retry
  operation now checks the core stop event before every attempt. The review
  passed after a test proved cancellation stops the immediate path after one
  attempt with no sleep. Interruption still maps to the repeated-conflict
  exhaustion error, as selected by the owner.
