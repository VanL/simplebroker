# F21 Schema Phase-Lock Timing Investigation

Date: 2026-07-17

Finding: F21 in `2026-07-16-code-review-findings-remediation-plan.md`

Scope: measurement and decision only; no production timeout change

## Decision

The 15-second proposal threshold is crossed. The 10-million-row tier had a
29.622-second median and the 50-million-row tier had a 273.111-second median.
A concurrent opener using the current 20-second schema phase-lock timeout can
therefore time out while a healthy one-time migration is still making
progress.

Open a separate proposal for migration-aware waiting. The leading bounded
option is a migration-specific wait budget, because it can cover the measured
50-million-row case without changing ordinary phase-lock contention behavior.
The proposal should compare that option with holder-liveness-aware waiting and
an operator-runbook step before choosing a production design. Unit H makes no
production change for F21.

## Environment

- Host: MacBook Pro `Mac16,5`, Apple M4 Max
- CPU: 16 cores (12 performance, 4 efficiency)
- Memory: 128 GB
- OS: macOS 26.5.1, arm64
- Python: 3.14.4
- SQLite: 3.50.4
- Filesystem: APFS on the internal SSD
- SimpleBroker baseline: `21183714c696c5f60e9f9ba7aff0c08fa0c30bf4`
- Free space before the run: 399 GiB

The machine was not placed into a dedicated benchmark mode. Normal filesystem
caching and background host activity were allowed, so the three raw values are
retained and the median is the decision statistic.

## Reproducible protocol

For each 1M, 10M, and 50M row tier:

1. Create a SQLite database with the production `BrokerDB` constructor and
   automatic vacuum disabled.
2. While that production connection remains open, remove only the v3-v5
   artifacts (`idx_messages_ts_unique`, `queue_aliases`,
   `alias_version`, and `idx_messages_pending_queue_ts`) and set
   `meta.schema_version` to `1`. The resulting messages table retains the v1
   production shape: `id`, `queue`, `body`, `ts`, and `claimed`, together with
   the v1 queue and unclaimed indexes.
3. Populate the v1-shaped database through the public
   `BrokerDB.insert_messages` path in 10,000-row calls. Each row uses queue
   `bench`, a 1,024-byte ASCII body, and a distinct exact message ID.
4. Close the writer. Assert the exact row count, schema version `1`, and the
   expected messages-table columns.
5. For each of three repetitions, create an APFS copy-on-write clone of the
   pristine v1 database. Start a real `SQLiteRunner`, call the production
   `migrate_schema(..., current_version=1, ...)` chain, and persist each schema
   version through the same `meta` upsert used by the SQLite backend plugin.
6. Measure only the v2-through-v5 migration chain with
   `time.perf_counter()`. Afterward, assert schema version `5` and the v3 and
   v5 indexes, close the runner, and remove the clone.

The public bulk-write path was chosen over synthetic SQL inserts so message
validation, exact-ID normalization, transaction handling, timestamp metadata,
and the SQLite runner remained production behavior. Copy-on-write clones made
all repetitions start from identical v1 bytes without including fixture-copy
time in the migration measurement.

## Results

| Rows | Fixture bytes | Populate time | Run 1 | Run 2 | Run 3 | Median |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 1,431,056,384 | 6.184s | 2.317s | 2.803s | 2.544s | 2.544s |
| 10,000,000 | 14,328,651,776 | 63.165s | 31.014s | 29.622s | 23.375s | 29.622s |
| 50,000,000 | 71,917,793,280 | 324.553s | 216.921s | 372.106s | 273.111s | 273.111s |

The 10M tier is about 13.3 GiB and the 50M tier about 67.0 GiB. The wide 50M
spread reinforces the need for headroom; using only the fastest observed run
would still exceed the current 20-second timeout by more than tenfold.

## Interpretation and follow-up boundary

The result does not show that the migration holder is unhealthy. It shows that
a healthy holder can outlive a concurrent opener's fixed wait budget. Raising
the ordinary phase-lock timeout globally would also delay detection of genuine
contention elsewhere, so this memo does not recommend that unscoped change.

The follow-up proposal should name:

- which setup phase owns the larger budget;
- whether waiter progress or holder liveness can be observed without a new
  compatibility surface;
- cancellation behavior and the upper bound for a dead holder;
- a Weft deployment-size check to confirm which measured tier is currently
  representative;
- tests with a real SQLite migration holder and concurrent opener; and
- a rollback path to the current fixed timeout.

Until that proposal lands, operators opening a large legacy database should
avoid starting concurrent SimpleBroker processes during its first migration.
