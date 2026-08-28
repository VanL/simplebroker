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
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL PRIMARY KEY,       -- Public message ID and sole row key
    claimed INTEGER DEFAULT 0              -- For read optimization
);
```

**Key design decisions:**
- The `ts` column is both the public message identifier and physical row key
- Explicit `ORDER BY ts` gives every bounded operation one cross-backend order
- WAL mode enables concurrent readers and writers
- Claim-based deletion separates the consume commit from later physical cleanup,
  avoiding deletion work on the read handoff path

## Ownership admission before setup

SQLite admission uses the runner's one normal connection. It does not open a
read-only probe and then reopen for writes. Before any SimpleBroker setup phase,
one scalar statement reads magic, stored schema version, optional proof rows,
and SQLite's `PRAGMA schema_version` cookie from one SQLite statement snapshot.
Foreign
magic and a newer owned version fail at this point, before connection setup,
bootstrap DDL, index cleanup, metadata writes, or marker mutation. Opening the
normal connection may still perform SQLite-owned recovery and coordination.

An absent `meta` table, missing value, absent file, or empty file is not treated
as foreign. Those states continue under the existing schema lock, where an
existing `messages` table follows legacy preparation and a target without it
follows fresh bootstrap. This is not a current-shape classifier.

## Schema migration serialization and publication

Fresh bootstrap publishes the current version only after its complete atomic
schema transaction succeeds. A pre-existing unversioned `messages` table is
instead seeded at version 1. Each later migration publishes its own version in
the transaction that installs that version, so a failed v3 migration cannot
durably claim v5.

SQLite v2 and v3 upgrades use `BEGIN IMMEDIATE` before the schema state that
controls a migration is read. The v2 claimed-column check and the v3 timestamp
index check are repeated under that write transaction, so another connection
that wins before lock acquisition becomes observed state rather than an
exception-message race. Repair of a missing v3 index uses the same transaction;
an initial no-lock check may only skip work when an already-current database has
an equivalent non-partial unique index over `messages.ts`.

Before creating the v3 unique index, the migration queries for duplicate
`messages.ts` values inside the owned transaction. Only that observed data
state produces the actionable duplicate-timestamp diagnostic. An unrelated
`IntegrityError` from index creation propagates unchanged. Migration success
does not depend on native exception prose.

The claimed column and semantic timestamp-uniqueness rule are checked again
before their schema versions are written. The version callback writes on the
same runner transaction in production, so neither its update nor the DDL is
durable until commit succeeds. A table-level `UNIQUE(ts)` autoindex or an
equivalent named index satisfies v3 regardless of physical order. If the owned
name `idx_messages_ts_unique` is occupied by a conflicting definition and no
equivalent rule exists, setup fails without dropping it.

Schema v6 is a one-way rebuild of the broker-owned `messages` table. It copies
`queue`, `body`, `ts`, and `claimed`, verifies the row count, replaces the v5
table, recreates only the canonical `(queue, ts)` indexes, checks foreign-key
integrity, and publishes version 6 last in the same transaction. The rebuild
temporarily disables connection-level foreign-key enforcement when needed and
restores the prior setting on success or failure. Caller-owned tables, indexes,
rows, sequence entries, and references to the supported `messages.ts` key stay
unchanged. Unsupported objects that depend on retired `messages.id` do not
block the SQLite rebuild; attached indexes and triggers are dropped with the
old table, while a detached view or foreign-key definition may survive broken.
Take a whole-file backup before migration if the target may contain such
objects. Additions inside reserved broker objects are unsupported and have no
preservation promise, though current-shape admission tolerates extra columns;
sidecars are objects outside those reserved tables and indexes.

## Schema completion proof

The path-level schema marker is a coordination cache, not live schema
proof. After the existing idempotent setup, migration, and repair routine
succeeds, SimpleBroker records a proof-algorithm version and the current SQLite
schema cookie in `meta`, then publishes the marker. A marker skips the slow path
only when those scalar rows match. Missing proof or a changed cookie takes the
same schema lock, rechecks proof there, and lets one owner run the existing slow
path while waiters observe its receipt.

The receipt deliberately does not attest columns, every index, mutable metadata,
or message data. Ordinary open performs no schema inventory or data scan. A
proof-algorithm bump invalidates an old receipt when the setup/repair algorithm
changes without requiring a stored product-schema version bump. A crash after
repair but before proof publication causes another idempotent pass; it cannot
publish a false fast path.

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

**Public-ID ordering:** Bounded read, peek, and move select by `ts`. The default
direction is ascending; an explicit newest request is descending. Ordinary
generated writes therefore remain FIFO-like, but an exact insertion of a lower
public ID is deliberately returned earlier by the default order. SQLite claim
and move SQL returns `(body, ts)` and shared core code sorts those rows in the
requested direction because SQLite does not specify DML `RETURNING` order.
Ascending generators use the same public-ID normalization. This is logical
order rather than engine order and is part of backend API v8.

Redis realizes the same finite order over its fixed-width encoded ID members.
Oldest uses `ZRANGEBYLEX`; newest uses `ZREVRANGEBYLEX` with reversed open
bounds. Claim and move Lua scripts return rows in selection order. If reserved
members fill the script's bounded scan budget without a result, directional
continuation resumes beyond the last scanned member with an exclusive bound. A
partly productive window continues inside the same bounded invocation until
the requested limit, range exhaustion, or scan budget. Pending and claimed
peek results are merged and sliced in the requested direction only after both
state sets have applied the same bounds. Generator batches retain their
ascending cursor and expose no reverse control.

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

## PostgreSQL schema migration and public-ID storage

PostgreSQL schema v6 uses the same public-key shape as SQLite: `messages.ts`
is the primary key, and the broker-owned table has no `order_id` column or
owned sequence. Fresh setup creates that shape directly. A v5 upgrade takes a
transaction-scoped advisory lock derived from the database-local namespace and
managed schema name, then bypasses cached setup state and rereads
`meta.schema_version` on the same connection. This database lock is the
correctness boundary for direct targets and for distinct project configs that
name one schema. The outer `.broker.toml.lock` PhaseLock still serializes one
project's startup, but cannot establish target identity across those cases.

The v5 migration then takes `ACCESS EXCLUSIVE` on `messages`, removes only the
owned legacy indexes, drops `order_id` with `RESTRICT`, promotes the existing
unique `ts` index to the primary key, and installs the canonical `(queue, ts)`
access paths. It publishes version 6 last in that transaction. `RESTRICT`
makes a sidecar dependency on the retired private column fail without schema
or metadata mutation; migration never uses `CASCADE`. Independent sidecar
tables, views over the public `ts` key, indexes, foreign keys, rows, and
sequences are outside the rewritten object and remain unchanged.

A waiter takes the same advisory lock and makes its decision from live
metadata. If it sees v6, it refreshes its runner cache and does not replay v5
DDL. Healthy current startup performs only the semantic shape read. If an
owned required access index is missing, setup attempts to recreate the two
canonical owned indexes and validates again; an incompatible primary key or
conflicting index still fails closed. Projection-based admission may overlook
extra reserved-table columns, but this does not make such modifications
supported or give them a migration-preservation contract.

PostgreSQL bounded read, peek, move, maintenance, and query classification use
`ts` only. The retrieval CTE orders the finite eligible set by `ts ASC` for
oldest or `ts DESC` for newest, and the outer result repeats that logical
order. The implementation never treats PostgreSQL `RETURNING` order as a
contract.

## PostgreSQL vacuum session ownership

PostgreSQL vacuum uses a session advisory lock because deletion batches commit
independently and exclusion must survive those commits. The runner therefore
leases one physical checkout across lock acquisition, every successful batch,
maintenance, and unlock. A transaction advisory lock would end at the first
batch commit and cannot satisfy this boundary.

Batch begin releases its provisional leased-operation lock if replacement
checkout fails, and cleans an acquired checkout plus that lock when interrupted
before it can publish the transaction marker. Every later failed open batch
attempts rollback, including a body `BaseException`, so that same lock is
settled before session unlock and logical release. An ordinary rollback failure
remains a note on a body `BaseException`; a rollback `BaseException` retains
priority with the body as explicit cause and context.

A leased begin, commit, or rollback failure detaches and physically closes its
checkout instead of returning it as an ordinary idle pool connection. Logical
lease depth remains positive and a later operation acquires a replacement.
This is necessary even when a later unlock on the replacement returns `false`:
the failed session may own the session advisory lock, so it must be dead before
`false` can safely mean that no reusable session retains the lock.

If the acquisition or unlock query raises, completion is unknown: the
server may have granted or retained the lock before the response was lost.
An unhandled uncertain acquisition would be worse than an uncertain unlock —
the pooled survivor would make every later vacuum observe try-lock `false`
and silently no-op. The PostgreSQL
runner detaches that checkout under its leased-operation lock followed by its
lease lock, clears transaction markers, then closes and pool-returns the
physical connection. Session close releases any residual server lock, and the
pool supplies a replacement instead of reusing the uncertain session. The
logical lease depth remains positive so an outer process-session borrower
keeps ownership; vacuum's final release removes only vacuum's nested level.
A completed unlock returning `false` proves that session does not own the lock
and needs no discard or warning.

Body, rollback, unlock, discard, and logical-release failures are recorded
separately and resolved explicitly. Ordinary cleanup failures do not erase a
body `BaseException`; a cleanup `BaseException` retains priority while prior
failures remain inspectable through context or ordered notes. This is backend
session cleanup under `[SB-OPS-6]`, after queue delivery has already completed.

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

- completed: [2026-08-25-schema-and-representation-assumption-remediation-plan](../plans/2026-08-25-schema-and-representation-assumption-remediation-plan.md)
  — single-connection admission, factual migration receipts, schema-proof
  caching, semantic timestamp uniqueness, and engine-order independence
- active: [2026-08-27-message-id-order-and-newest-selection-plan](../plans/2026-08-27-message-id-order-and-newest-selection-plan.md)
  — public-ID order, bounded newest selection, and surrogate-free SQL schemas
- active: [2026-08-25-verified-review-findings-remediation-plan](../plans/2026-08-25-verified-review-findings-remediation-plan.md)
  — PostgreSQL vacuum session discard after uncertain unlock
- completed: 2026-08-24-comprehensive-review-findings-remediation-plan —
  read-only SQLite ownership admission and claim-preserving cross-backend move

- completed: 2026-08-24-failure-path-and-contract-findings-resolution-plan —
  source for [ALT-IMPL09-001], commit-before-handoff cleanup, and explicit SQL
  delete ownership; implemented and verified from baseline `1b8ecfa0`
- retired: 2026-08-06-pre-release-review-remediation-plan — source `84159198`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
