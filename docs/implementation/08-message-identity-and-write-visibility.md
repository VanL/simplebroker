# Message Identity and Ordinary-Write Visibility

Owner: exact-insert admission in `simplebroker/_message_insert.py`; SQL write
transactions in `simplebroker/db.py`; Redis ordinary-write allocation and row
publication in `extensions/simplebroker_redis/simplebroker_redis/core.py` and
`extensions/simplebroker_redis/simplebroker_redis/scripts.py`.

Boundary: realizes `[SB-ID-1]`, `[SB-ID-2]`, and `[SB-ID-4]` from
`docs/specs/13-message-identity-contract.md`. It does not own strict timestamp
selection, watcher lifecycle, move identity, patterned-broadcast atomicity, or
dump format.

Verification: shared reserved-zero and SQL transaction-ordering tests, plus
real-Valkey stale-fence, two-writer visibility, monotone-resync, command-count,
same-core contention, and `SM-REDIS-WRITE` transition tests.

Required action: keep zero decodable but reject it at exact-insert admission;
keep every expected Redis Lua result before the first mutation; preserve the
single shared conflict budget and the post-commit wakeup/maintenance order.

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

SQLite and PostgreSQL use the shared `BrokerCore.write()` transaction owner.
The real runner sequence is:

`begin → high-water compare-and-advance → row insert → commit`

The pass-through recorder in `tests/test_write_visibility.py` observes that
sequence without replacing SQL execution. Redis is explicitly excluded from
that SQL-only proof.

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
high-water fence is stale. Both consume one three-conflict budget. An ID
collision keeps the prior sleep-then-resync posture; a stale fence refreshes
persisted state before reserving again. A third conflict is terminal. Redis
transport failure after `EVAL` remains outcome-ambiguous and is translated
without retry.

One `RedisBrokerCore` serializes candidate reservation through the data `EVAL`.
Without that local boundary, concurrent callers on the same core can reserve
in increasing order but reach Redis out of order, spending the three-conflict
budget on local scheduling rather than cross-core contention. The lock is
reset after a fork so a child cannot inherit a locked parent boundary.
Different cores and processes remain concurrent and are reconciled by the Lua
fence and bounded retry protocol.

Conflict repair reads current high-water and the maximum stored ID, calls the
backend's compare-and-advance operation, then refreshes the local generator.
It never performs an unconditional high-water write, so a concurrent later
advance cannot be overwritten backward.

Pub/Sub notification remains a post-commit hint. Maintenance accounting
remains best-effort after `_write_message` returns. Neither participates in
the data commit.

## Checkpoint limit

The visibility guarantee is deliberately limited to ordinary `write()`.
Standalone `generate_timestamp()` persists an ID without a row. Exact
insertion and ID-preserving move can introduce an older ID later. Redis
patterned broadcast retains its Python snapshot and separate allocation path.
Those paths can place an ID behind an already advanced checkpoint, so this
implementation does not turn `after_timestamp` into a durable broker offset.

## Rollback and observation

Code and contract must be reverted together. Redis data written by the new
script remains readable by older releases, so rollback needs no migration, but
it reopens the two-operation visibility and backward-resync races.

Post-release signals are Redis write error rate, terminal timestamp-conflict
rate, high-water regressions, generated rows observed behind later ordinary
writes, and direct-core latency. The deterministic performance gate is one
data `EVAL` for a steady-state ordinary write; timing samples are diagnostic,
not pass/fail thresholds.

## Related plan

- `docs/plans/2026-07-30-reserved-zero-and-redis-write-atomicity-plan.md`
