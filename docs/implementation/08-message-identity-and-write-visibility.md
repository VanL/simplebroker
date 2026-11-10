# Message Identity and Write Visibility

Owner: timestamp allocation in `simplebroker/_timestamp.py`; exact-insert
admission in `simplebroker/_message_insert.py`; SQL write transactions in
`simplebroker/db.py`; PostgreSQL server-allocated ordinary writes in
`extensions/simplebroker_pg/simplebroker_pg/plugin.py`
(`write_generated_message`), `extensions/simplebroker_pg/simplebroker_pg/_sql.py`,
and `extensions/simplebroker_pg/simplebroker_pg/runner.py`
(`run_transaction`); Redis ordinary-write allocation and row publication in
`extensions/simplebroker_redis/simplebroker_redis/core.py` and
`extensions/simplebroker_redis/simplebroker_redis/scripts.py`.

Boundary: realizes `[SB-ID-1]` through `[SB-ID-4]` from
`docs/specs/13-message-identity.md`. It does not own strict timestamp
selection semantics, watcher lifecycle, move identity, patterned-broadcast
atomicity, or dump record ordering. The live-peek section explains cursor
mechanics under `[SB-DELIVERY-4]`; the delivery spec owns traversal semantics.
Persistence load delegates its header-floor meaning to
`[SB-IO-4]` while using the same high-water machinery described here.

Verification: shared reserved-zero and SQL transaction-ordering tests; the
real-SQLite `SM-TIMESTAMP-GENERATOR::SHARED_INSTANCE_SERIALIZATION` transition;
a real two-connection PostgreSQL monotone-resync test; real-PostgreSQL
server-allocation, lock-order, exhaustion, atomicity, and checkpoint-reader
tests; plus real-Valkey
stale-fence, two-writer visibility, monotone-resync, command-count, same-core
contention, and `SM-REDIS-WRITE` transition tests.

Required action: keep zero decodable but reject it at exact-insert admission;
keep every expected Redis Lua result before the first mutation; preserve the
single shared conflict budget and the post-commit wakeup/maintenance order;
never narrow one `TimestampGenerator` instance's lock below its complete
allocation, durable advance or refresh, and cache-publication transition;
server-allocated writes publish only through the monotone
`_publish_allocated`; a PostgreSQL generated write locks the high-water row
before any statement that touches `messages`.

## Selection-bound conversion boundary

`TimestampGenerator.validate()` owns syntax-to-integer conversion but not the
later selection semantics. Exact message-ID mode keeps its strict 19-decimal-
digit grammar. Non-exact bounds strip once, then reject values over 128 Unicode
code points before digit folding, regular-expression work, integer conversion,
or echoing the rejected value. This is a fixed work gate, not a new timestamp
format or configuration surface.

Accepted ISO datetimes are normalized to aware UTC and converted relative to
the Unix epoch from integer `timedelta.days`, `.seconds`, and `.microseconds`.
The implementation never calls float `timestamp()` or `total_seconds()` on
this path. Clearing the logical-counter bits therefore produces exactly the
same hybrid bound as an equivalent integral seconds, milliseconds, or
nanoseconds spelling. Range failures in the distinct exact-ID owner name the
public concept as a message ID, while the legacy serialized `timestamp` field
remains unchanged.

## Why zero is rejected at insertion

ID `0` is the empty-high-water and lower-bound origin. New exact insertion is
therefore narrower than exact selection: normalized zero is rejected by
`normalize_insert_records` before duplicate detection, high-water computation,
or backend mutation.

The lower layers intentionally remain broad. `normalize_message_id`, SQL
schemas, and Redis `encode_id` still represent zero so an old target can be
peeked, moved, deleted, and dumped. Moving the check into any of those layers
would turn recovery compatibility into backend-specific behavior.

`load_lines` performs the same check while it still owns the source line
number. This gives an actionable diagnostic before the current batch flush.
Load is streaming, so the rule does not roll back aliases or earlier batches
that were already applied.

## SQL ordinary writes

SQLite uses the shared `BrokerCore.write()` transaction owner. The real runner
sequence is:

`begin → high-water compare-and-advance → row insert → commit`

The pass-through recorder in `tests/test_write_visibility.py` observes that
sequence without replacing SQL execution. Redis is explicitly excluded from
that SQL-only proof.

### PostgreSQL server-allocated writes

PostgreSQL implements the optional `write_generated_message` backend hook, so
`BrokerCore._do_write_transaction()` routes an ordinary write (no
`keep_newest`) through `_do_server_allocated_write()`. The core reserves one
local candidate (`TimestampGenerator._reserve_candidates`), and the plugin runs
one pipelined implicit transaction through `PostgresRunner.run_transaction()`:

`lock high-water row (LOCK_LAST_TS_ROW) → allocate max(candidate, last_ts + 1), insert, notify (WRITE_GENERATED_MESSAGE) → Sync commits`

Both statements and the Sync leave the client without waiting for a result,
so the row lock is held only while the server executes and commits. The
previous sequence held it across the `INSERT` and `COMMIT` round trips, and a
stale candidate added a failed compare-and-advance, a `meta` read, and a second
trip to the back of the lock queue. Measured on the evidence workload this made
PostgreSQL write throughput flat in writer count; see
`2026-11-08-pg-write-lock-hold-plan.md` at `178822b`.

What the row lock still guarantees, and why each piece of the shape exists:

- **Commit order equals ID order.** The lock is released only after the
  holder's commit is visible, so the next allocation is above every visible
  generated ID and commits after it. Checkpoint readers therefore keep the
  guarantee the shared transaction gives them. The commit's WAL flush stays
  inside the lock; that is the irreducible serial cost.
- **The server's choice is the generator's choice.** `candidate` when it is
  above durable high-water, otherwise `last_ts + 1` in the same physical grain.
  That is `TimestampGenerator._next_components()` evaluated against durable
  high-water with the reservation-time clock sample, so stale candidates cost
  nothing. When the stored logical counter is already 4095 the statement
  allocates nothing (zero rows, no mutation); the core refreshes its cache and
  the generator waits for the clock or raises `TimestampError` (`[SB-ID-4]`).
  A plain `GREATEST(last_ts + 1, candidate)` would roll into the next physical
  grain instead.
- **Lock order.** The row lock statement must come first. PostgreSQL takes a
  statement's relation locks when it analyzes it, so a single combined
  statement would hold `ROW EXCLUSIVE` on `messages` while waiting for the row
  — the inverse of broadcast, rename, and `keep_newest` (row lock, then
  `SHARE ROW EXCLUSIVE`). That inversion was reproduced as `deadlock detected`
  and is pinned by `test_queued_writer_does_not_deadlock_broadcast_lock_order`.
- **Atomicity.** Everything up to the Sync is one implicit transaction; an
  `IntegrityError` (allocated ID already stored above a corrupted high-water)
  rolls back the allocation too, and `write()`'s existing ladder resynchronizes
  and retries. After resync the server allocates above the repaired high-water,
  so a persistent candidate conflict self-heals on PostgreSQL.
- **Failure reporting.** The shared transaction reports a failed
  compare-and-advance as `TimestampError` (contention retried for the
  timestamp budget, then a known failure; anything else
  `outcome_ambiguous=True`) and insert or commit failures as the native
  database error. The pipelined write keeps that split by phase:
  `run_transaction` reports which statement a failure reached, the plugin
  marks a failure of the row-lock statement (where the write waits for and
  takes high-water) `OperationalError._high_water_phase`, and core runs the
  whole exchange through `simplebroker/_timestamp.py::_run_durable_advance`,
  the classifier `_store_if_greater` also uses. Only marked failures get the
  timestamp-budget contention retry; allocate-and-insert failures (including
  parameter adaptation, such as a NUL body) and commit failures leave it at
  once, stay native, and their contention is retried by the write's
  stop-aware operation retry. Retryable contention on the row lock now gets the documented
  contention retry; on the shared transaction PostgreSQL could not retry
  inside the aborted transaction.
- **Transport.** psycopg flushes each pipelined statement as it is queued, so
  the residual client exposure inside the lock is the non-blocking gap between
  the client's sends, not a round trip. Without libpq pipeline support
  (libpq < 14) the runner runs the same statements inside an explicit
  `BEGIN`/`COMMIT`: same semantics, the old round-trip cost.

Client-side binding of a multi-statement simple query would remove the send
gap too, but it puts message bodies into statement text that PostgreSQL logs
on statement errors by default. Moving every `messages`-locking path to
"table lock first" so a single statement suffices is a proposed follow-up in
the plan, not current behavior.

`tests/test_write_visibility.py` records the PostgreSQL write as one
`run_transaction` call; real-PostgreSQL proofs live in
`extensions/simplebroker_pg/tests/test_pg_write_allocation.py`, including a
TCP relay that checks on the wire that both statements and the single Sync
are sent while the row lock is still being waited for.

When `keep_newest` is present, shared SQL extends the same transaction:

`begin → high-water compare-and-advance → backend lock preparation → row insert → claim older pending rows → commit`

The claim cutoff is the highest-N boundary selected by public message ID on
the existing partial pending `(queue, ts)` index. A missing cutoff row makes
the update a no-op. SQLite's `BEGIN IMMEDIATE` already serializes writers.
PostgreSQL locks the singleton high-water row first, then takes a transaction-
scoped `SHARE ROW EXCLUSIVE` table lock so move, exact insert, broadcast,
delete, rename, and other row mutations cannot interleave. That lock is
intentionally coarse and can stall unrelated queues. If insert, trim, or
commit fails with a known rollback, the generated high-water, row, and claims
all roll back together.

## Same-generator timestamp serialization

One `TimestampGenerator` PID guard runs before its lock is acquired. A child
therefore replaces the inherited lock and clears its inherited cache before
lazy initialization, cached reads, refresh, candidate calculation, durable
compare-and-advance, conflict repair, or cache publication. After that guard,
one lock owns each complete transition. The backend compare-and-advance
protects durable monotonicity across generator instances and processes. It
does not serialize the process-local `_last_ts` cache shared by threads using
one generator.

Before commit `6f9bd065`, candidate calculation was locally locked but the
durable attempt and cache publication were not part of that same critical
section. Thread A could durably store `T` and pause before publishing it to
`_last_ts`; thread B could then durably store and publish `T+1`; thread A could
resume and regress `_last_ts` to `T`. Durable state remained monotonic, but the
stale cache caused later candidates to begin behind the surviving high-water
and spend conflict retries unnecessarily.

The full critical section is therefore intentional. Backend contention retry
waits may occur while this same-instance lock is held; moving the durable CAS
or its retry outside the lock would reopen the stale-cache ordering. The
`SHARED_INSTANCE_SERIALIZATION` firing row pauses the real SQLite plugin's
advance method, proves another thread cannot enter it through the same
generator, then checks both durable high-water and `_last_ts` against the
maximum returned value.

Reservation-based writes (Redis and PostgreSQL ordinary writes) use a
different transition: `_reserve_candidates` updates only the cache under the
lock and the backend advances durable high-water outside it. Redis commits
exactly the reserved candidate, so the reservation is already the publication.
PostgreSQL may commit an ID above the reservation (`last_ts + 1`), so the core
then installs it with `_publish_allocated`, a monotone maximum under the lock
(`RESERVE_PUBLISH` firing row). A monotone publication cannot regress
`_last_ts`, so the stale-cache hazard above does not reopen even though the
durable step is outside the generator lock. A failed backend operation can
leave the cache ahead of durable state; later candidates simply start higher.

## SQL conflict repair

Shared SQL conflict repair reads persisted high-water and the maximum stored
message ID inside one transaction, then calls the backend's guarded
compare-and-advance operation. SQLite's `BEGIN IMMEDIATE` serializes this
repair against competing writers. PostgreSQL's corresponding operation is an
ordinary READ COMMITTED `BEGIN`, so its initial reads may be stale by the time
the repair mutates `meta`. The guarded `UPDATE ... WHERE last_ts < candidate`
therefore remains required on both backends: a later concurrent high-water
wins instead of being overwritten by the stale repair.

Before commit, repair reads the transaction-visible surviving high-water into
the generator cache. Under PostgreSQL READ COMMITTED this sees either the
already-committed contender or the repair transaction's own guarded update.
The warning and local cache report that surviving value, which may be greater
than the maximum the repair originally observed. Keeping this read before
commit avoids reporting a failed repair after its mutation already committed;
a later commit failure can leave only a safe high cache value. `write_last_ts`
remains an explicit administrative/test corruption primitive; repair never
uses it.

## Redis ordinary writes

`RedisBrokerCore._write_message` reserves one process-local candidate through
`TimestampGenerator._reserve_candidates(1)`. Reservation changes only the
local cache. `WRITE_MESSAGE` then receives the raw decimal ID, its 19-digit
encoding, the queue, and the body.

Before its first mutation, Lua checks:

1. namespace ownership;
2. candidate collision in the body and all-ID structures;
3. whether persisted high-water is already greater than or equal to the
   candidate.

Ordering comparisons use padded 19-digit strings. Lua numbers cannot preserve
the full signed 64-bit ID range, and raw variable-width decimal strings do not
sort numerically. On success, the script stores raw `last_ts` and publishes the
body, all-ID index, pending index, and queue registry in the same server-side
operation.

Result `-1` means the candidate already exists. Result `-6` means the
high-water fence is stale. Both are proven pre-mutation outcomes and share one
30-second elapsed retry budget. An ID collision resynchronizes and advances a
bounded exponential backoff sequence. A stale fence refreshes persisted state
and retries immediately; it neither sleeps nor consumes the ID-collision
backoff sequence, because a peer write during that sleep could make the next
candidate stale again. Retry sleeps observe the core stop event and translate
interruption through the operation's existing exhaustion error. Redis transport
failure after `EVAL` remains outcome-ambiguous and is translated without retry.

All `RedisBrokerCore` instances for the same target and namespace serialize
candidate reservation through the data `EVAL` within one process. Without that
boundary, concurrent cores can repeatedly reserve below a competing commit and
spend the elapsed conflict budget on local scheduling. The weak process registry
does not retain dead targets, and it resets both its guard and target locks
after a fork so a child cannot inherit a lock held by a vanished parent thread.
Different processes remain concurrent and are reconciled by the Lua fence and
bounded retry protocol.

With `keep_newest`, the same `WRITE_MESSAGE` script also receives the claimed
and reserved queue keys. Before its first write, it type-checks every key it
will touch, computes the lowest displaced pending IDs, verifies that every one
has a body, and rejects any active reservation intersection. Success updates
high-water, inserts the new row, and moves every displaced ID from pending to
claimed in one script. Reservation conflict is retryable but does not consume
the timestamp-conflict budget. A missing displaced body is a non-retryable
integrity failure. Stale-batch recovery is best-effort preparation outside the
script; the script's reservation recheck is the atomic guard.

Redis conflict repair reads current high-water and the maximum stored ID, calls the
backend's compare-and-advance operation, then refreshes the local generator.
It never performs an unconditional high-water write, so a concurrent later
advance cannot be overwritten backward.

## Persistence-load high-water restoration

Dump v1 samples broker-global `last_ts=H` before traversal even when no pending
message carries that ID. It passes the exclusive query equivalent of the
inclusive H bound to each backend peek, so concurrent messages above H are not
part of this dump. At the signed-ID ceiling no `H + 1` is representable and no
filter is needed because every valid ID is already `<= H`. Load validates the
bound inline as defense against legacy or hand-built input; this preserves streaming and
does not promise rollback of earlier batches.

Before destination mutation, `load_lines()` decodes H's physical component and
compares it with one local wall-clock sample. Any positive lead emits the
public `DumpClockSkewWarning`. A lead beyond the configured 300-second default
is rejected unless force is explicit. This makes a header-only check sufficient
without reading or spooling the complete input. Five minutes is SimpleBroker's
availability/safety tolerance, informed by MIT Kerberos's conventional
300-second default, not a general claim about clock correctness.

After replay, load calls backend API v7 `advance_last_timestamp()` with H.

The operation must always issue the backend's atomic compare-and-advance. A
direct backend can reserve candidates in the process-local generator cache
without persisting them, and a rolled-back SQL write can likewise leave the
cache ahead. Comparing the header only with that cache would incorrectly skip
the durable restore. After the monotone attempt, the generator reads persisted
high-water once, rejects an observation below H, installs a valid observation
in its cache, and returns it. There is no discarded initialization read. A
later writer may make the cache stale immediately, which is the ordinary
`[SB-ID-3]` contract.

`TimestampError.outcome_ambiguous` makes recovery explicit. A non-retryable
write/transport failure or final-read failure after the attempt sets it true
because the monotone update may have committed. Exhausted retryable lock
contention and a final observation below H are known failures and leave it
false. Load is intended for fresh state but does not enforce it; it is
merge-like for disjoint data and may be partially applied. Recovery guidance
therefore depends on the marker and still accounts for earlier replayed state.

Pub/Sub notification remains a post-commit hint. Maintenance accounting
remains best-effort after `_write_message` returns. Neither participates in
the data commit.

## Checkpoint limit

The visibility guarantee is deliberately limited to generated `write()`, with
or without the optional keep transition.
Standalone `generate_timestamp()` persists an ID without a row. Exact
insertion and ID-preserving move can introduce an older ID later. Redis
patterned broadcast retains its Python snapshot and separate allocation path.
Those paths can place an ID behind an already advanced checkpoint, so this
implementation does not turn `after_timestamp` into a durable broker offset.

## Live peek cursor

`BrokerCore.peek_generator()` owns SQLite/PG page progression through
`_retrieve`; `RedisBrokerCore.peek_generator()` uses `_peek_rows` with the same
public-ID boundary. The first page preserves the caller's lower and upper
bounds. Later pages set the strict lower bound to the last returned ID and
retain the upper bound, with zero offset. This seeks through the existing
queue/ID indexes instead of revisiting an ever-growing prefix. It also avoids
positional skips when previously returned rows are deleted or moved out.
Bodies-only iteration still retains the page's final ID internally. No ID
arithmetic is needed, including at the signed maximum.

Exact-ID selection takes precedence over ranges in the query builders, so it
must fetch once and terminate; advancing only the range bound would repeat
the exact row. Redis combines eligible pending and claimed candidates with
per-state lookahead, then emits the global next ID; concatenating whole sorted
pages could emit past an unseen candidate in another state. Neither path
changes selection outside peek generators or introduces a public cursor API.

Page fetching finishes before user yields; the Queue closeable iterator still
owns operation entry and exit. The cursor is a value, so deleting its row does
not invalidate it. Buffered rows can outlive removal, and older exact inserts
or ID-preserving moves can arrive behind the cursor. Empty or short pages end
iteration. These are live observation limits, not claims of queue emptiness or
exclusive processing. `tests/test_peek_keyset_pagination.py` verifies traversal;
`tests/test_peek_generator_lifecycle.py` verifies resource ownership.

Larger OFFSET pages would reduce overhead while retaining repeated prefix
work and positional skips. A snapshot or server-side cursor would change
consistency and resource lifetimes. A fixed-start upper watermark would change
live-tail observation without recovering older arrivals. None is needed for
forward progress; consumers still own checkpoint recovery and processing.
Rollback reverts code and `[SB-DELIVERY-4]` together without data conversion,
restoring OFFSET cost and skipping. Restart affected processes to adopt either
version; existing iterators retain their process's code.

## Rollback and observation

Code and contract must be reverted together. Neither the SQL guarded repair nor
the Redis script changes stored representation, so rollback needs no migration,
but it reopens backward-resync races.

Post-release signals are Redis write error rate, terminal timestamp-conflict
rate, high-water regressions, generated rows observed behind later ordinary
writes, and direct-core latency. The deterministic performance gate is one
data `EVAL` for a steady-state ordinary write; timing samples are diagnostic,
not pass/fail thresholds.

The PostgreSQL server-allocated write likewise needs no migration: reverting
the core or the extension restores the shared transaction, and mixed fleets
serialize correctly because both shapes lock the high-water row first. Its
deterministic gate is one `run_transaction` exchange per ordinary write.
Post-release signals are write throughput that scales past one writer, no
`idle in transaction` holder of the `meta` row, zero conflict metrics under
contention, and no `deadlock detected` involving `meta` and `messages`.

## Related plan

- retired: 2026-11-08-pg-write-lock-hold-plan.md — source `178822b`;
  see the ledger in `docs/plans/README.md`
  — PostgreSQL server-allocated generated writes and their lock-order proof.

- retired: 2026-09-11-keyset-peek-pagination-plan — source `3868d7a`;
  see the ledger in `docs/plans/README.md`.

- retired: 2026-08-25-verified-review-findings-remediation-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns bounded
  timestamp-bound admission, integer ISO conversion, and message-ID diagnostic
  ownership.
- retired: 2026-08-23-correctness-and-concurrency-review-remediation-plan —
  source `23d6c9d1`; see the ledger in
  `docs/plans/README.md`
- retired: 2026-08-12-bounded-live-dump-plan — source `d0d2de9`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-30-reserved-zero-and-redis-write-atomicity-plan — source
  `5023710`; see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
