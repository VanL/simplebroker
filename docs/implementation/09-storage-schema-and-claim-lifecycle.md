# Storage Schema and Claim Lifecycle

Status: Active
Boundary: SQLite storage layout, claim-based deletion rationale, and
cross-process setup coordination. Exact delivery behavior is owned by
[`docs/specs/11-delivery.md`](../specs/11-delivery.md) (`[SB-DELIVERY-*]`)
and residual operations by [`docs/specs/17-ops.md`](../specs/17-ops.md)
(`[SB-OPS-*]`); this document explains why the realization looks the way
it does.

## Database schema

SimpleBroker uses a single SQLite database with Write-Ahead Logging (WAL) enabled:

```sql
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Ensures strict FIFO ordering
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL UNIQUE,            -- Unique hybrid timestamp serves as message ID
    claimed INTEGER DEFAULT 0              -- For read optimization
);
```

**Key design decisions:**
- The `id` column guarantees global FIFO ordering across all processes
- The `ts` column serves as the public message identifier with uniqueness enforced
- WAL mode enables concurrent readers and writers
- Claim-based deletion enables ~3x faster reads

## Concurrency and delivery realization

**Consume claim boundary:** Read and move operations use atomic backend
transitions. A message is claimed exactly once under normal consume
delivery, and the claim commits before the message is handed to the
caller ([`[SB-DELIVERY-1]`](../specs/11-delivery.md)). This is not a
promise of exactly-once application processing or external side effects:
a crash between the claim commit and the handoff can leave a message
claimed and not handed off.

**FIFO Ordering:** Messages are read in write order for a queue, regardless of
which process wrote them. SQLite uses the autoincrement `id` plus serialized
write transactions; other backends must preserve the same public ordering
contract.

**Message Lifecycle:**
1. **Write Phase**: Message inserted with unique timestamp
2. **Claim Phase**: Read marks message as "claimed" (fast, logical delete)
3. **Maintenance Phase**: Explicit `--vacuum` or a due opportunistic check permanently removes claimed messages

This optimization is transparent to the delivery contract: claimed rows
are never selected again for ordinary pending delivery
([`[SB-DELIVERY-1]`](../specs/11-delivery.md)).

**Why are read messages marked claimed before vacuum removes them?** Claiming
keeps reads fast and atomic while deferring physical cleanup. Vacuum removes
claimed rows later. This is why queue stats distinguish pending, claimed, and
total rows.

## Cross-process setup coordination

**Why does `_phaselock.py` exist?** SQLite setup has to be safe across
processes and platforms. The phase-lock module coordinates setup work with
file locks and extended-attribute fallback so multiple processes do not race
schema or optimization phases. It is internal, but deliberately self-contained.
