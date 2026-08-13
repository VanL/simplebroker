# Message Identity and Ordinary-Write Visibility

Owner: exact-insert admission in `simplebroker/_message_insert.py`; SQL write
transactions in `simplebroker/db.py`; Redis ordinary-write allocation and row
publication in `extensions/simplebroker_redis/simplebroker_redis/core.py` and
`extensions/simplebroker_redis/simplebroker_redis/scripts.py`.

Boundary: realizes `[SB-ID-1]` through `[SB-ID-4]` from
`docs/specs/13-message-identity.md`. It does not own strict timestamp
selection, watcher lifecycle, move identity, patterned-broadcast atomicity, or
dump record ordering. Persistence load delegates its header-floor meaning to
`[SB-IO-4]` while using the same high-water machinery described here.

Verification: shared reserved-zero and SQL transaction-ordering tests, a real
two-connection PostgreSQL monotone-resync test, plus real-Valkey stale-fence,
two-writer visibility, monotone-resync, command-count, same-core contention,
and `SM-REDIS-WRITE` transition tests.

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
high-water fence is stale. Both consume one three-conflict budget. An ID
collision keeps the prior sleep-then-resync posture; a stale fence refreshes
persisted state before reserving again. A third conflict is terminal. Redis
transport failure after `EVAL` remains outcome-ambiguous and is translated
without retry.

All `RedisBrokerCore` instances for the same target and namespace serialize
candidate reservation through the data `EVAL` within one process. Without that
boundary, concurrent cores can repeatedly reserve below a competing commit and
spend the three-conflict budget on local scheduling. The weak process registry
does not retain dead targets, and it resets both its guard and target locks
after a fork so a child cannot inherit a lock held by a vanished parent thread.
Different processes remain concurrent and are reconciled by the Lua fence and
bounded retry protocol.

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

The visibility guarantee is deliberately limited to ordinary `write()`.
Standalone `generate_timestamp()` persists an ID without a row. Exact
insertion and ID-preserving move can introduce an older ID later. Redis
patterned broadcast retains its Python snapshot and separate allocation path.
Those paths can place an ID behind an already advanced checkpoint, so this
implementation does not turn `after_timestamp` into a durable broker offset.

## Rollback and observation

Code and contract must be reverted together. Neither the SQL guarded repair nor
the Redis script changes stored representation, so rollback needs no migration,
but it reopens backward-resync races.

Post-release signals are Redis write error rate, terminal timestamp-conflict
rate, high-water regressions, generated rows observed behind later ordinary
writes, and direct-core latency. The deterministic performance gate is one
data `EVAL` for a steady-state ordinary write; timing samples are diagnostic,
not pass/fail thresholds.

## Related plan

- retired: 2026-08-12-bounded-live-dump-plan — source `d0d2de9` (local-only
  pin); see the ledger in `docs/plans/README.md`
- retired: 2026-07-30-reserved-zero-and-redis-write-atomicity-plan — source
  `5023710`; see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
