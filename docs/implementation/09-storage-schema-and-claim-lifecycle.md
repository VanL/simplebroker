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

## Ownership admission before setup

An existing non-empty SQLite file is inspected through a read-only connection
before runner setup can enable WAL or create schema. An explicit `meta.magic`
value belonging to another product is rejected at that boundary. A missing
table, missing value, absent file, or empty file is not treated as foreign:
those states continue through the existing legacy/bootstrap path, which owns
the final schema and validity diagnostics. This narrow three-state check avoids
mutating a file that has positively identified another owner without inventing
a second schema classifier.

## Schema migration serialization and publication

SQLite v2 and v3 upgrades use `BEGIN IMMEDIATE` before the schema state that
controls a migration is read. The v2 claimed-column check and the v3 timestamp
index check are repeated under that write transaction, so another connection
that wins before lock acquisition becomes observed state rather than an
exception-message race. Repair of a missing v3 index uses the same transaction;
an initial no-lock check may only skip work when an already-current database has
the named index.

Before creating the v3 unique index, the migration queries for duplicate
`messages.ts` values inside the owned transaction. Only that observed data
state produces the actionable duplicate-timestamp diagnostic. An unrelated
`IntegrityError` from index creation propagates unchanged. Migration success
does not depend on native exception prose.

The claimed column and the named `idx_messages_ts_unique` index are checked
again before their schema versions are written. The version callback writes on
the same runner transaction in production, so neither its update nor the DDL is
durable until commit succeeds. A callback used by a test may record that it was
invoked even when the database transaction later rolls back; invocation is not
publication.

The v3 postcondition deliberately checks only the established index name. It
does not validate the index definition or repair a same-named index with a
different shape.

## Concurrency and delivery realization

**Consume claim boundary:** Read and move operations use atomic backend
transitions. A message is claimed exactly once under normal consume
delivery, and the claim commits before the message is handed to the
caller ([`[SB-DELIVERY-1]`](../specs/11-delivery.md)). This is not a
promise of exactly-once application processing or external side effects:
a crash between the claim commit and the handoff can leave a message
claimed and not handed off.

Materialized SQL claim and move operations have one transaction shape:
begin, execute, then commit every non-empty result before return; an empty
result or ordinary exception rolls back. There is no deferred
commit-after-return mode. Keeping that order in one path makes the public
handoff boundary auditable and avoids a private flag that implied an unused
second delivery policy.

A move changes queue binding, not delivery state. SQL therefore updates only
the queue column. Redis removes the ID from its source pending or claimed set
and inserts it into the matching destination set. `require_unclaimed=False`
only permits a claimed row to be selected by exact ID; it does not release the
claim or make that row pending again.

**Physical-delete transaction boundary:** SQL queue-specific and all-queue
delete explicitly begin before the backend plugin mutation, commit once on
success, and roll back an ordinary failure before it propagates. This matches
the exact-id and multi-queue SQL paths and makes [SB-OPS-3] atomicity hold for
injected non-autocommit runners as well as the default SQLite runner. Redis
keeps its separately specified per-queue orchestration and partial-result
boundary.

**Buffered CLI delivery seam:** Batched at-least-once `read --all` keeps the
active claim transaction open across yielded records and commits when the
generator is resumed after its final yield. The CLI therefore flushes each
record at the stdout seam before asking the generator for another record. If
the consumer has closed the pipe, `_StdoutClosed` closes the iterator while
the batch is still uncommitted, so its claims roll back and remain eligible for
retry. A flush after the batch would be too late: resuming past the final yield
would already have committed the claims.

Finite CLI output has a different boundary. Finite commands buffer through
the same exact write classifier and perform one command-owned final flush.
They flush only after producing output, so an empty selection keeps its
ordinary result. A closed consumer returns error `1`; the five streaming
families retain clean stop `0`. For `write` and `rename`, mutation commit
precedes result rendering. Output failure does not roll the mutation back, so
the diagnostic directs callers to inspect state before retrying.

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

Fallback status cleanup follows the same ownership boundary as publication.
When a missing or undersized SQLite target makes completion markers stale,
the runner rechecks that condition only after the phase lock is acquired and
then discards the stable status file and abandoned temp files before reading
markers. A waiter never performs ordinary stale-marker cleanup before locking:
that could delete the current owner's exclusively created temp file between
flush and atomic replace. Read-only completion checks report a fresh target as
incomplete without mutating marker state; the subsequent locked setup owns any
required cleanup. The invalidation applies even while xattrs are the active
marker backend. If best-effort deletion leaves the stable fallback file in
place, xattr mode atomically replaces it with an empty status generation before
publishing xattrs, so a later fallback opener cannot trust state from the
deleted database.

## Filesystem permission boundary

SimpleBroker does not impose one cross-platform sharing mode on SQLite state.
Private-directory placement is the usual machine-local boundary. Cross-user
deployments instead require effective access to the main database, every SQLite
and SimpleBroker companion file, and the containing directory. POSIX mode,
ownership, ACL, and umask policy or Windows ACL policy belongs to the operator;
the full deployment condition is in the
[`configuration guide`](../guides/configuration.md#general-security-considerations).
Phase-lock files use ordinary file creation filtered by that policy. Reopening
the stable lock sidecar does not rewrite its existing mode. Each fallback
status publication remains an atomic replace through an exclusively created
temporary file, so that new generation uses the umask active for that
publication.

## Deferred storage alternatives

### [ALT-IMPL09-001] Add a claimed-row index from the million-row vacuum probe

Disposition: deferred
Owner: SimpleBroker product owner
Governs: claimed-row deletion and automatic/explicit vacuum maintenance
Source record: [ALT-RF20260824-001] in docs/plans/2026-08-24-failure-path-and-contract-findings-resolution-plan.md
Candidate: Add a SQLite partial index on claimed messages, a schema migration
or current-version repair, and a no-claimed fast path for vacuum eligibility.
Why plausible: Claimed existence and batch deletion currently scan without a
claimed-row index. A synthetic table with one million pending rows produced
roughly 15–22 ms scans while core maintenance held its process-local lock.
Evidence:
- contemporaneous: `BrokerCore._record_maintenance_activity()` and `_should_vacuum()` run the
  synchronous eligibility check only after scheduled committed activity.
- contemporaneous: `simplebroker._maintenance.vacuum_is_eligible()` and [SB-OPS-6] normally
  trigger automatic cleanup at the configured ratio or above 10,000 claimed
  rows, far before the synthetic mostly-pending tail case under ordinary
  maintenance.
- contemporaneous: `GET_VACUUM_STATS` still aggregates total rows, so a partial claimed index
  does not remove the main O(N) eligibility scan.
- owner-recalled: Explicit `broker --vacuum` is a one-shot administrative process. The
  observed roughly 22 ms scan is inconsequential beside process setup and the
  requested maintenance action absent a documented operator latency target.
Reason: The isolated probe does not represent ordinary maintained queue state,
and it does not establish user-visible harm. An index would add migration,
repair, write, and legacy-database costs without removing the aggregate scan.
Current consequence: Add no claimed-row index, schema version, persistent
counter, fast path, or vacuum-query change.
Reconsider when: A production trace or reproducible end-to-end benchmark under
the default automatic-vacuum policy attributes a user-visible latency or
throughput regression in a documented supported workload to these scans; the
product contract is expanded to recommend million-row steady-state tables; or
explicit CLI vacuum misses a documented operator latency objective because of
these scans. The isolated million-row microbenchmark alone is not a trigger.
After a trigger fires, measure index creation, repair, and write costs on
representative legacy database sizes before adoption. A fired trigger reopens
evaluation; it does not adopt the index.
Promoted to: none

## Related Plans

- completed: 2026-08-24-comprehensive-review-findings-remediation-plan —
  read-only SQLite ownership admission and claim-preserving cross-backend move

- completed: 2026-08-24-failure-path-and-contract-findings-resolution-plan —
  source for [ALT-IMPL09-001], commit-before-handoff cleanup, and explicit SQL
  delete ownership; implemented and verified from baseline `1b8ecfa0`
- retired: 2026-08-06-pre-release-review-remediation-plan — source `84159198`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
