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
- Claim-based deletion separates the consume commit from later physical cleanup,
  avoiding deletion work on the read handoff path

## Concurrency and delivery realization

**Consume claim boundary:** Read and move operations use atomic backend
transitions. A message is claimed exactly once under normal consume
delivery, and the claim commits before the message is handed to the
caller ([`[SB-DELIVERY-1]`](../specs/11-delivery.md)). This is not a
promise of exactly-once application processing or external side effects:
a crash between the claim commit and the handoff can leave a message
claimed and not handed off.

**Buffered CLI delivery seam:** Batched at-least-once `read --all` keeps the
active claim transaction open across yielded records and commits when the
generator is resumed after its final yield. The CLI therefore flushes each
record at the stdout seam before asking the generator for another record. If
the consumer has closed the pipe, `_StdoutClosed` closes the iterator while
the batch is still uncommitted, so its claims roll back and remain eligible for
retry. A flush after the batch would be too late: resuming past the final yield
would already have committed the claims.

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

Ordinary setup, status publication, and vacuum keep their coordination-file
names stable while an operation may hold them. Global `--cleanup` is the sole
exception: it deliberately removes the complete known target namespace,
including coordination and maintenance files. It does not join the ordinary
SQLite or phase-lock lifecycle. If cleanup overlaps any SimpleBroker activity
or raw SQLite connection, old and replacement file generations can diverge and
the exact storage, coordination, and client outcomes are undefined. This is why
cleanup is specified as a destructive operator action, not a quiescence or
maintenance protocol. See [`[SB-OPS-7]`](../specs/17-ops.md).

## Filesystem permission boundary

SimpleBroker does not impose one cross-platform sharing mode on SQLite state.
Private-directory placement is the usual machine-local boundary. Cross-user
deployments instead require effective access to the main database, every SQLite
and SimpleBroker companion file, and the containing directory. POSIX mode,
ownership, ACL, and umask policy or Windows ACL policy belongs to the operator;
the full deployment condition is in the
[`configuration guide`](../guides/configuration.md#general-security-considerations).

## Related Plans

- [`2026-08-06 pre-release review remediation`](../plans/2026-08-06-pre-release-review-remediation-plan.md)
- [`2026-08-06 audit remediation`](../plans/2026-08-06-audit-remediation-plan.md)
