# Bounded Dump and Safe Watermark Restore Plan

The first implementation slice restored the dump-header watermark but left
the live export unbounded. Owner review reopened the plan: the header value now
also becomes the inclusive message-ID bound for the dump, and load gains a
header-only future-skew gate. This revision also dispositions the completed-work
findings F2 through F10 before the coordinated release may land.

Class: 5 — normative `[SB-ID-*]`, `[SB-IO-*]`, and `[SB-API-*]` changes.
The public persistence and backend-compatibility boundaries also fire the
risky trigger, so hardening is mandatory.

Plan type: implementation with spec revision.

## 1. Goal

Make v1 `last_ts=H` carry one coherent boundary across export and restore:
`dump_lines()` exports only pending messages with IDs at most H, and
`load_lines()` restores durable broker-global high-water to at least H. Before
mutating the destination, load compares H's physical-time component with local
wall time. It warns for apparent future clock skew up to the configured limit,
rejects greater skew by default, and permits an explicit `--force` override
that still warns. Preserve the v1 wire format, documented integer/string input
compatibility, ordinary `insert_messages()` behavior, and coordinated backend
API v7 set. Close the correctness, API, UX, documentation, and release-history
findings discovered in completed-work review before landing.

## Source Documents

- `docs/program-theory.md` [THEORY-1] through [THEORY-5]
- `docs/specs/13-message-identity.md` [SB-ID-1] through [SB-ID-4]
- `docs/specs/15-persistence-io.md` [SB-IO-1] through [SB-IO-4]
- `docs/specs/16-python-library-api.md` [SB-API-*]
- `docs/implementation/08-message-identity-and-write-visibility.md`
- `docs/guides/backends.md` and `simplebroker/ext.py` backend-version guidance
- `docs/guides/configuration.md` and `simplebroker/_project_config.py` config
  ownership (environment/typed overrides versus backend-only `.broker.toml`)
- `bin/release.py` and the release-gate workflows for coordinated versioning
- MIT Kerberos `clockskew` reference:
  <https://web.mit.edu/kerberos/www/krb5-1.21/doc/admin/conf_files/krb5_conf.html>
  (documented default 300 seconds)
- downstream contract: `../taut/docs/specs/08-persistence-io.md` [PIO-4],
  [PIO-6], [PIO-7]
- repository startup context and the writing-plans, hardening-plans,
  testing-patterns, adversarial-acceptance-probes, maintaining-traceability,
  and review-loop runbooks

## 3. Context and Key Files

- `simplebroker/_dump.py`: emitted v1 headers contain `last_ts`; the first
  slice now restores it, but export still calls unbounded `peek_generator()`.
- `simplebroker/_constants.py`, `simplebroker/commands.py`, and
  `simplebroker/cli.py`: own the configurable skew threshold, command/API
  plumbing, warning text, and `load --force` escape hatch.
- `simplebroker/_message_insert.py` and `simplebroker/db.py`: ordinary exact
  insertion intentionally advances high-water to `max_id + 1`; this stays.
- `simplebroker/_backend_plugins.py`: internal broker-connection protocol used
  by the loader and publicly exported for backend authors; adding a required
  member requires a backend API handshake advance.
- `extensions/simplebroker_redis/simplebroker_redis/core.py` and `scripts.py`:
  direct Redis restore must match SQL atomicity.
- `tests/test_dump_load.py`, `tests/test_insert_messages.py`, and existing
  PostgreSQL/Redis dump-load pipe suites own real proof. The named pipe tests
  currently prove ordinary message replay, not header-only floor restoration,
  so this plan adds dedicated backend-conformance cases.

Comprehension gate before runtime edits; record answers in the execution log:

1. Why must load never lower `last_ts` to equal the header? Expected: ordinary
   insert owns `max_id + 1`, high-water is monotone, and the destination may
   already contain a higher value because freshness is caller-owned.
2. Why must an empty dump still apply header `last_ts`? Expected: high-water
   may name no current row; claimed or selected-out messages may leave no row
   that can reconstruct it, and future generated IDs must remain greater.
3. Why can the durable compare-and-advance not be skipped when the local
   generator cache is already above the header? Expected: direct-backend
   candidate reservation and failed or rolled-back writes may leave the
   process cache ahead of persisted `last_ts`; the load contract is about the
   durable broker-global floor, not that cache.
4. Why is checking only the header sufficient for the load skew policy?
   Expected: the same dump operation samples H before traversal and passes an
   inclusive H bound to every backend message scan, so no emitted message ID
   can exceed H. This does not make aliases, claims, deletes, or queue
   membership a frozen point-in-time snapshot.
5. Why is no full-file load preflight required? Expected: the new writer-side
   invariant makes the first line authoritative; load can reject excessive
   skew before applying aliases or message batches.

An incorrect answer blocks implementation until the cited owners are reread.

## 4. Invariants and Constraints

1. `dump_lines()` keeps its signature and v1 wire format. It samples
   `last_ts=H` once, emits H in the first line, and exports only pending message
   records whose IDs are `<= H`. All first-party backends must apply the bound
   in their real query/iteration path; a post-hoc load scan is not evidence.
2. Every v1 input must contain `last_ts`; omission is invalid. The deployment
   owner guarantees there are no retained legacy dumps requiring compatibility
   with the formerly unbounded/missing-watermark interpretation of v1.
3. `dump_lines()` always emits canonical 19-digit string `last_ts`. Load keeps
   the documented input compatibility and accepts either an integer or an
   exact 19-digit string. Compatibility does not permit omission.
4. Header `last_ts=H` is validated through the public message-ID boundary. It
   is both an inclusive upper bound on this dump's message records and a lower
   floor for restored allocation state. Load checks `id <= H` as each message
   is already parsed and rejects a violation before inserting that record. This
   is not a separate pass; earlier mutations retain the documented partial-load
   behavior.
5. Header-only and no-row-at-H dumps restore a durable watermark at least H,
   even when the connection's process-local timestamp cache is already above
   H. Every successfully generated later ID is greater than H. Ordinary direct
   `insert_messages()` retains its existing far-future behavior; this plan
   adds a load-only guard and does not claim a broker-wide insertion invariant.
6. Ordinary `insert_messages()` retains `max_id + 1`, top-of-range rejection,
   validation, and atomicity.
7. Restore is intended for fresh state, but freshness is a caller-owned
   precondition rather than an enforced invariant. Duplicate IDs are the only
   existing freshness signal. Load is mutating and may be partially applied;
   header-only and disjoint loads may merge into non-fresh state. Existing
   message batches retain their current transaction behavior; after replay, load
   monotonically advances durable allocation state to at least the header
   floor. A higher value observed by the final durable read is reflected in the
   connection cache; a later concurrent advance may make that cache stale
   immediately under `[SB-ID-3]`.
8. `advance_last_timestamp(timestamp)` is a required backend API v7
   `BrokerConnection` member. Core, SQLite, PostgreSQL, and Redis ship as one
   coordinated compatibility set; an API-v6 plugin is rejected by the
   existing exact-version handshake rather than failing later in `load_lines()`.
9. `DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS = 300` is the named default and
   `BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS` is its non-negative integer config
   key. It follows the existing `load_config()` environment default plus
   `resolve_config(config)` typed-override precedence. The project `.broker.toml`
   remains a backend-target file and does not gain policy fields. Five minutes is
   SimpleBroker's chosen operational tolerance, informed by MIT Kerberos's
   conventional 300-second default allowable clock skew; it is not asserted as
   a universal maximum for clocks.
10. Load decodes H's physical component and compares it with one local wall
    clock sample immediately after parsing the first line and before any
    destination mutation. Positive skew at or below the configured threshold
    loads but warns. Greater skew fails before mutation unless explicitly
    forced; forced load still warns. No complete-input pre-scan is performed.
11. The exact public/command interfaces are
    `load_lines(broker, lines, *, force=False, config=None)` and
    `cmd_load(db_path, *, force=False, quiet=False, config=None)`. `config` is
    a `Mapping[str, Any] | None`; `resolve_config(config)` supplies the sole
    threshold, so there is no competing explicit-threshold parameter.
    `broker load --force` and the corresponding keyword-only Python override
    bypass only the excessive-future-skew refusal. They do not bypass dump
    validation, duplicate detection, backend compatibility, or timestamp-floor
    errors. Library callers receive `DumpClockSkewWarning`, a public
    `UserWarning` subclass importable as
    `from simplebroker import DumpClockSkewWarning`, through `warnings.warn()`
    and never get an
    unconditional print. `cmd_load` captures that category to avoid Python's
    default file/line rendering and emits one `broker load: warning:` line to
    stderr. Global `--quiet` suppresses that human warning under `[SB-CLI-2]`,
    including a forced load; force still executes the warning path.
12. The warning reports H, apparent skew, and the allocation consequence. It
    must not claim 9,999 writes: the 12-bit counter permits at most 4,095
    further broker-global generated IDs from a counter value of zero, and
    potentially fewer depending on H's low bits, before allocation waits for
    wall time and may raise `TimestampError`.
13. `advance_last_timestamp()` rejects `None` with `TypeError`, always attempts
    the durable monotone operation without a discarded initialization read,
    and verifies that its one final durable observation is at least the
    requested floor. A lower observation is a loud contract failure.
14. `TimestampError(message, *, outcome_ambiguous=False)` exposes the public
    boolean `outcome_ambiguous` attribute. Every existing call site defaults to
    `False`. Timestamp-floor errors set it explicitly:
    Exhausted retryable lock contention and a final observed value below the
    floor are known failures; a non-retryable write/transport failure after an
    attempt and a post-attempt final-read failure are outcome-ambiguous.
15. `load_lines()` validates that `advance_last_timestamp` is callable before
    consuming input or mutating the destination, so unsupported structural
    broker doubles raise `TypeError` rather than a late `AttributeError`.

Fatal: malformed header watermark, duplicate ID, detected incompatible
destination, API-version mismatch, excessive future skew without `--force`,
failure to establish the durable header floor, or failure of the final durable
read. Non-fresh state is not itself detected or rejected. Outcome ambiguity is
reported according to invariant 14.

## 5. Spec Baseline

- `6dd3281893255a2ce6c79f66787bc739fa1c436f` — [SB-ID-*], [SB-IO-*], and
  [SB-API-*] at plan authoring.
- Promotion baseline: baseline `6dd3281893255a2ce6c79f66787bc739fa1c436f`
  plus the current worktree diff to `docs/specs/13-message-identity.md` and
  `docs/specs/15-persistence-io.md` and
  `docs/specs/16-python-library-api.md`; document/context gates must reproduce
  it.

## 6. Proposed Spec Delta

Promotion strategy: A — edit active specs before implementation mappings claim
the new behavior; add reciprocal mappings with code and tests.

### [SB-IO-1]/[SB-IO-2] addition

> `dump_lines()` samples broker-global `last_ts=H` before traversal and emits H
> in the first record. Every emitted message ID is at most H; each backend scan
> receives that inclusive upper bound. The bound excludes concurrent later
> writes from this dump without promising a frozen snapshot for aliases,
> claims, deletes, moves, or queue membership. Load validates the bound inline
> while parsing and rejects a message with `id > H` before inserting it; this
> adds no complete-input preflight and does not roll back earlier batches.

### [SB-IO-4] addition

> A valid v1 header must contain `last_ts`; `load_lines()` treats it as the
> restored broker high-water floor. After replay, the target high-water is at
> least that value; a header-only dump therefore preserves future allocation
> monotonicity. Header H is also the inclusive upper message-ID bound guaranteed
> by `[SB-IO-1]`; load therefore needs no full-file preflight to assess dump
> time skew. Message batches retain their current partial-apply behavior.
> Every successfully generated later ID is greater than both the header floor
> and every restored message ID.
> Current writers emit `last_ts` as the canonical exact 19-digit string.
> Load accepts either an integer or an exact 19-digit string under
> `[SB-ID-4]`; a missing `last_ts` is invalid.
>
> After parsing the header and before destination mutation, load compares H's
> physical component with local wall time. Any positive future skew warns. Skew
> at most `BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS` (default 300) proceeds; greater
> skew fails unless the caller explicitly forces it, and forced load still
> warns. The override bypasses only this refusal. Load is intended for a fresh
> destination but does not enforce freshness; it is mutating and may be
> partially applied when later failures occur.

### [SB-ID-3]/[SB-ID-4] addition

> Persistence load monotonically advances allocation state to at least a
> supplied dump-header high-water after replay. It never lowers a watermark.
> This does not change ordinary `insert_messages()`, which continues to
> advance above its largest supplied ID.

### [SB-API-11] addition

> Backend API v7 requires `BrokerConnection.advance_last_timestamp(timestamp)`.
> The operation validates an integer timestamp, monotonically advances durable
> broker-global high-water to at least that value regardless of the current
> process-local cache, then reads durable high-water once. It refreshes the
> connection cache and returns the value observed by that final read. That
> observation may immediately become stale under `[SB-ID-3]`; a concurrent
> higher value is never lowered. If the final read fails after the monotone
> advance was attempted, the operation raises `TimestampError`; the durable
> outcome may already satisfy the requested floor and callers must treat the
> result as outcome-ambiguous. Core rejects older or newer backend API versions
> through the exact-version handshake.

The promoted API text must additionally require explicit `TypeError` for
`None`, no initialization read before the monotone attempt, a final
`observed >= requested` postcondition, and the exact
`TimestampError.outcome_ambiguous` contract above. The persistence API contract
must document the early `TypeError` capability check and the exact keyword-only
force/config signatures above.

### Configuration and CLI delta

- Add `DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS = 300` and config key
  `BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS`; accept non-negative integers only and
  carry it through environment defaults, typed overrides, config documentation,
  and validation tests. Do not extend `.broker.toml`.
- Add `broker load --force`; default remains safe refusal. Add corresponding
  keyword-only command and `load_lines()` arguments so CLI and embedders have
  the same policy.
- Use public `DumpClockSkewWarning(UserWarning)` for `load_lines()`, export it
  from `simplebroker/__init__.py` beside `load_lines`, and add a root import
  contract test. Capture it
  in `cmd_load` and emit a clean `broker load: warning:` stderr line unless
  global `--quiet` is set, with no duplicate Python warning rendering. Catch
  `TimestampError` in `cmd_load`,
  retain exit 1, and include recovery guidance based on
  `outcome_ambiguous` rather than treating every failure as ambiguous.
- The five-minute default follows MIT Kerberos's documented default allowable
  `clockskew` of 300 seconds as operational precedent; SimpleBroker owns its
  independent availability/safety rationale.

### Version and compatibility delta

- Advance `BACKEND_API_VERSION` and the SQLite/PostgreSQL/Redis plugin literals
  from 6 to 7.
- Add backend API v7 to `simplebroker.ext`, `[SB-API-11]`, backend-author
  guidance, resolution tests, and release guards.
- Prepare a coordinated core 7.3.0, PostgreSQL 3.8.0, and Redis 3.8.0 package
  set. Set both extension core floors to `simplebroker>=7.3.0`; set the root
  `pg` and `redis` extra floors to `simplebroker-pg>=3.8.0` and
  `simplebroker-redis>=3.8.0`.
- Add `7: "7.3.0"` to `BACKEND_API_MIN_CORE_VERSION` without changing the
  historical mappings.
- Update the exact owned seams: root `pyproject.toml` and
  `simplebroker/_constants.py`; both extension `pyproject.toml` files; root,
  PostgreSQL, and Redis `uv.lock` files; both extension READMEs;
  `docs/guides/backends.md`; `simplebroker/ext.py`; `bin/release.py`; and their
  release/version/handshake tests.
- Regenerate locks without invoking the release driver: run `uv lock` at the
  root, `uv lock --project extensions/simplebroker_pg`, and
  `uv lock --project extensions/simplebroker_redis`. `bin/release.py` is
  inspection/test evidence in this plan, not an execution command: its
  non-dry batch path may commit, tag, push, and start publication workflows.
- Publication remains an irreversible follow-up requiring its own explicit
  authorization and exact-SHA release plan. No component may be published as
  API v7 until all three artifacts are ready from the same exact SHA.

## 7. Rollout and Rollback

Implement, reconcile, and verify the coordinated API-v7 set before Taut raises
its dependency floor. Before publication, rollback is one coordinated revert
of bounded export, header-floor restore, skew config and warning category,
Python arguments, CLI `--force`, API handshake, package metadata, and guidance.
After publication, the v1 bounded-export/header-floor pair and API-v7 handshake
remain coordinated compatibility behavior and are not separately reverted;
use a corrective release. The skew threshold/default, warning wording, and
CLI refusal policy can be changed in a later coordinated contract release
without changing the v1 bound. Publication is not authorized by this
implementation plan and requires exact-SHA release gates in a separate plan.

## 8. Dependency-Ordered Tasks

1. Reopen and re-promote the contract before more runtime work.
   - Promote the exact bounded-export, header-only skew policy, force boundary,
     caller-owned freshness, warning/error classification, and config/API text
     into `[SB-IO-*]`, `[SB-ID-*]`, `[SB-API-*]`, and the winning config/CLI
     owners. Backlink this revised plan and run document/context gates.
   - Remove the former allowance for records newer than H and mark the earlier
     implementation-complete evidence stale. Do not add conformance mappings
     until their exact nodes pass.
2. RED/GREEN: bounded live export.
   - Sample H once before yielding the header. Pass an inclusive H bound through
     `peek_generator()` on SQLite, PostgreSQL, and Redis. Because the existing
     API's `before_timestamp` is exclusive, use a reviewed overflow-safe
     conversion or add a named inclusive helper; never compute an unchecked
     `H + 1` at the signed-ID ceiling.
   - Force a concurrent write after header emission and prove its ID is absent;
     prove IDs equal to H are retained. Preserve pending-only selection,
     ordering, filters, streaming, and largest-queue memory behavior.
   - Reject a hand-built record with `id > H` inline before inserting that
     record, while retaining the documented partial-apply behavior for earlier
     batches. This is parser validation during ordinary load, not a pre-scan.
   - Prove the same invariant on real PostgreSQL and Valkey. A mocked iterator
     or post-load rejection is not backend evidence.
3. RED/GREEN: header-only future-skew safety.
   - Add the default/config field and validation first. Inject or patch the wall
     clock in tests; do not sleep.
   - Check decoded physical time immediately after the first line and before
     aliases or batches. Fire exact boundary cases: past/equal time (silent),
     +1 ns/grain and exactly configured skew (warn and load), just beyond
     (reject with zero mutation), and just beyond with force (warn and load).
   - Assert warning text names H, approximate skew, at-most 4,095 broker-global
     allocations (or the lower exact remaining counter budget), and possible
     `TimestampError` until wall time catches up. Avoid a 9,999 claim.
   - Prove `broker load --force`, `cmd_load(..., force=True)`, and the public
     Python load override match. `--force` must not suppress any unrelated
     validation or persistence error.
   - Prove `DumpClockSkewWarning` is emitted once for Python callers; CLI
     translates it once without file/line noise. Cover quiet/non-quiet crossed
     with forced/non-forced warning paths; quiet suppresses only display.
4. RED/GREEN: timestamp advancement correctness (F2, F6, F7, F8).
   - Remove the unused initialization read. Reject `None` explicitly. After the
     attempt, read durable state once and fail if the observation is below the
     requested floor; exercise deleted SQL meta rows on SQLite and PostgreSQL.
   - Add `TimestampError(..., outcome_ambiguous=False)` and its public boolean
     attribute. Classify exhausted retryable locking as a
     known failure, non-retryable attempted writes as ambiguous, final-read
     failure as ambiguous, and a final lower observation as known failure.
   - Preserve the cache-ahead and concurrent-winner proofs. Redis transport
     faults must exercise the real EVAL boundary rather than replace it.
5. RED/GREEN: load API and CLI hardening (F3, F5).
   - Capture and validate a callable `advance_last_timestamp` before consuming
     the iterable. Raise a documented typed compatibility error with zero
     mutation for an old structural broker double.
   - Catch `TimestampError` at `cmd_load`, keep exit 1 and the `broker load:`
     prefix, and give different recovery advice for known versus ambiguous
     outcomes. Add CLI firing tests.
6. Preserve and reverify the completed API-v7/header-floor slice.
   - Keep mandatory int/exact-string header parsing, durable monotone restore,
     cache-ahead/concurrent-winner behavior, coordinated API-v7 literals,
     versions, dependency floors, locks, and release guards.
   - Replace the obsolete newer-than-header acceptance test with bounded-export
     tests. Stop if ordinary `insert_messages()` semantics change.
7. Documentation, release history, and housekeeping (F4, F9, F10).
   - State that load is intended for fresh state but freshness is caller-owned,
     mutating, merge-like for disjoint data, and potentially partial. Correct
     the Fatal list; do not make `--force` a freshness switch.
   - Move already-published 7.1.0 entries out of Unreleased under
     `## [7.1.0] - 2026-08-11`; keep bounded dump/skew/API-v7 work Unreleased
     and note the mandatory-header compatibility boundary.
   - Add `advance_last_timestamp` to exhaustive surface lists in `_dump.py` and
     `[SB-IO-*]`; deduplicate only the positive header helpers identified by
     review; ensure this currently untracked plan lands with its index row.
   - Record the redundant post-insert durable read as a one-shot efficiency
     follow-up, not a blocker for this safety change.
8. Reconciliation and closure.
   - Update implementation rationale, conformance mappings, config reference,
     CLI/library docs, backend guidance, changelog, plan evidence, and Taut
     dependency guidance.
   - Run an independent revised-plan review before runtime resumes, reviews
     after the bounded/skew and error-classification slices, and a final
     completed-work review. Close the Class-5 row only in the owner landing
     commit. Do not publish.

## 9. Testing Plan

Use vertical red-green TDD. Keep real storage, exact insertion, and the
existing SQL/Redis compare-and-advance primitives. Fault hooks may control
phase order but may not replace the backend operation.

```bash
uv run pytest -q tests/test_dump_load.py tests/test_cli_dump_load.py tests/test_constants.py tests/test_insert_messages.py tests/test_property_dump_load.py
uv run pytest -q tests/test_core_persistence_transition_tables.py tests/test_backend_plugin_resolution.py tests/test_release_script.py
uv run --project extensions/simplebroker_pg --extra dev pytest -q extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py
uv run --project extensions/simplebroker_redis --extra dev pytest -q extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py
```

The PostgreSQL and Redis commands must execute against real configured
services; skips are not evidence for the new bounded-export, header-floor, or
missing-meta cases. Add dedicated header-only/no-row-at-H assertions rather
than relying on ordinary pipe replay, because `insert_messages()` already
advances above pending message IDs. Time tests use an injected/patched clock,
exercise inclusive threshold edges, and assert zero destination mutation on
the early-refusal path. Configuration tests cover environment defaults, typed
override, invalid, and negative inputs, plus a guard that `.broker.toml` did
not acquire this policy field.

## 10. Verification and Completion Gates

The prior full-suite evidence predates the bounded-export, skew-policy, and
F2-F10 remediation slices and is not completion evidence for this revision.
Run targeted tests per slice. Final gates are the repository's current full
test, Ruff, format, mypy, docs/context, status-index, coalescing, and diff
checks. Independent review of this materially revised plan and spec/API delta
precedes further implementation. Independent completed-work review follows
the cross-backend/API-v7 slice and final reconciliation. Release-equivalent
gates prove the coordinated metadata set, but no tag or publication occurs.

## 11. Independent Review Log

Reviewer: Claude, read-only, from `/Users/van/Developer`. Review v1
compatibility, inclusive bounded export on real SQL/Redis paths, rejection of
records newer than the header, header-only skew/force behavior, monotone empty
restore, exact config/Python/CLI/error semantics, and whether the plan
accidentally changes ordinary insertion. Verdict must be PASS/BLOCKED under the
repository rubric.

| Round | Finding | Disposition | Result |
|---|---|---|---|
| 1 | P2: downstream chronology correctness depends on this unpublished floor | Accepted: rollout and downstream firing test already block a Taut floor raise until a real release exists. | PASS |
| 1 | P3: filename/title drift after scope narrowing | Accepted with the filename note above; no rename/path churn is needed for an unambiguous indexed plan. | PASS |
| 1 | P3: Taut should derive H from its existing emitted header | Accepted downstream; this plan retains unchanged `dump_lines()`. | PASS |
| 2 | P1: cache-ahead state can skip the durable floor | Accepted: durable-state invariant, comprehension gate, and cache-ahead/concurrent-winner firing tests added. | Revision requires review |
| 2 | P1: required broker method was added without a backend API bump | Accepted: coordinated backend API v7 set, handshake tests, version floors, and release guards added. | Revision requires review |
| 2 | P2: existing pipe tests do not fire header-only cross-backend behavior | Accepted: dedicated SQLite/PostgreSQL/Redis floor cases and no-skip evidence required. | Revision requires review |
| 2 | P2: numeric header input lacked explicit evidence | Accepted: canonical string output and documented integer/exact-string input are explicit invariants and tests. | Revision requires review |
| Owner decision, 2026-08-13 | No legacy dumps omit `last_ts`; integer input remains documented compatibility | Missing `last_ts` stays invalid. Integer and exact-string header forms remain accepted and tested. | Binding |
| Owner decision, 2026-08-13 | Coordinated first-party release is available | Use backend API v7 across core, PostgreSQL, and Redis; publication remains separately authorized. | Binding |
| 3 | P1: Strategy-A task order omitted `[SB-API-11]` promotion | Accepted: task 1 now promotes all three normative owners and runs gates before v7 runtime work. | Round 3 PASS |
| 3 | P1: “concurrent winner” promised an impossible perpetually-current cache and left final-read failure undefined | Accepted: contract now returns the final-read observation, permits immediate staleness, and makes post-attempt read failure a fatal, outcome-ambiguous `TimestampError`. | Round 3 PASS |
| 3 | P2: exact compatibility text and safe coordinated metadata workflow were underspecified | Accepted: exact int/string text, versions, floors, files, three `uv lock` commands, and release-driver prohibition are explicit. | Round 3 PASS |
| 3 round 2 | Promotion baseline omitted `[SB-API-11]`; invariant 7 overpromised cache freshness; promotion tables claimed not-yet-existing tests | Accepted: baseline now names all three specs, invariant is scoped to the final-read observation, and conformance mappings were deferred until exact nodes passed. | Round 3 PASS |
| 4 completed-work review | P2: public `load_lines()` docs omitted outcome-ambiguous `TimestampError` and retained partial mutations | Accepted: Raises now names the error, ambiguity, and persistence of earlier aliases/flushed batches. | Round 2 PASS |
| 4 completed-work review | P2: index and execution evidence retained pre-implementation/absence wording | Accepted: index now states implemented/verified status; evidence names forced `OperationalError`. | Round 2 PASS |
| Owner decision, 2026-08-13 | Header must actually bound a live dump; a whole-file load preflight is too costly | Implemented: export only IDs `<= H`; load assesses future skew from the first line with no pre-scan. | Binding; PASS |
| Owner decision, 2026-08-13 | Future dump watermarks need a load-only safety gate | Implemented: default 300 seconds; warn within, reject beyond, `--force` warns and proceeds; config and Python parity. | Binding; PASS |
| F2 | `TimestampError` conflates known failure with outcome ambiguity | Implemented: the public boolean `outcome_ambiguous` marker classifies write/read phases and drives recovery guidance. | PASS |
| F3 | Old structural broker fails late with raw `AttributeError` | Implemented: callable capability check occurs before input consumption or mutation. | PASS |
| F4 | Plan claimed non-fresh destinations were fatal without enforcement | Implemented per owner decision: freshness is caller-owned; load is documented as mutating, merge-like, and potentially partial. | PASS |
| F5 | CLI does not handle floor `TimestampError` at command boundary | Implemented: command prefix, conditional recovery guidance, exit 1, and firing tests. | PASS |
| F6 | Missing SQL meta row can silently return below the requested floor | Implemented: final observed-value postcondition plus real SQLite/PostgreSQL corruption tests. | PASS |
| F7 | `None` relies on an optimizable assertion | Implemented: explicit `TypeError` remains active under `python -O`. | PASS |
| F8 | Fresh generator performs an unused durable read | Implemented: advance performs the durable attempt followed by one final read, with call-order coverage. | PASS |
| F9 | Published 7.1.0 entries remain under Unreleased | Implemented: dated 7.1.0 section contains published entries; new work remains Unreleased. | PASS |
| F10 | Required method omitted from exhaustive surface lists | Implemented: module and canonical-spec surface lists name `advance_last_timestamp`. | PASS |
| Final review, round 1 | Exact test-mypy failed on private fixture access; typed skew config accepted booleans/floats; `cmd_load` swallowed unrelated warnings; shared CLI test assumed SQLite. | Fixed with typed test casts, strict integer parsing, warning replay and firing test, and `sqlite_only` scope. Exact core-test mypy plus PostgreSQL/Redis CLI shards pass. | PASS |
| Final review, round 2 | Required real Redis EVAL transport-fault proof was absent; plan/status evidence was stale; configuration guide said 31 rather than 32 keys. | Added a real EVAL-then-disconnect ambiguity test, reconciled plan/status evidence, and corrected the catalog count. | PASS |
| Attached review N3a/N3b | Parser-totality treated the skew warning as a crash under warnings-as-errors; deferred warning replay could alter exception timing. | Suppressed the informational warning inside the parser property/Atheris replay and replaced recording/replay with immediate category-aware display. | PASS |
| Attached review N3c | Invalid environment values can fail during package import before the CLI error boundary. | Confirmed as a pre-existing repository-wide lifecycle problem widened by the new key. A local catch cannot work because package import fails first; silent fallback defaults are rejected. Track under a separate hardening plan. | Deferred, explicit IR-1 |
| Attached review N4 | A lagging source watermark can make bounded dump omit anomalous rows without a signal. | Ratified existing behavior: live concurrent rows above H are intentionally identical at this boundary, so an exclusion warning would be a false positive during normal operation. Spec and changelog now state the boundary and upgrade recovery. | PASS |
| Attached review N5-N8 | Floor/force assertions, subprocess timing margins, state-machine transitions, and avoidable timestamp test doubles were weak. | Strengthened each firing proof; retained only sanctioned fault hooks and used real SQLite pass-through spies for success/order behavior. | PASS |
| Attached review N9/N10 | Untracked landing files and interface-review evidence. | Both files are included in this closing changeset; completed and recorded the required eleven-principle interface review. | PASS |

## 12. Out of Scope

- frozen aliases, claims, deletes, moves, or queue membership
- a new `dump_lines()` parameter, generic broker snapshot API, or format v2
- changing ordinary exact-ID insertion or its batch atomicity
- changing the direct `insert_messages()` far-future policy; this plan guards
  only persistence load
- Taut projection or sidecar work
- enforcing fresh/empty load destinations
- removing the redundant message-bearing load refresh; retain as a measured
  one-shot efficiency follow-up
- tags or publication; those require a separately authorized exact-SHA release
  plan after this coordinated set is ready

## 13. Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|---|---|---|---|---|

No accepted deviations. The reopened slices and remediation are implemented,
verified, independently reviewed, and closed by the owner-directed changeset.

## 13a. Execution and Verification Log

The rows below record both the original header-floor/API-v7 slice and the
completed reopened bounded-export, future-skew, and F2-F10 implementation.

| Date | Slice | Evidence | Result |
|---|---|---|---|
| 2026-08-13 | Comprehension gate | Confirmed high-water is monotone; empty/filtered/claimed dumps can lack a row for the header value; process-local candidate reservation and rollback can put cache ahead of durable state. | PASS |
| 2026-08-13 | Cache-ahead RED/GREEN | New public-load regression failed with persisted `0` below requested floor `1786624101405954047`, then passed after `advance_to_at_least()` always issued durable compare-and-advance and performed one final read. | PASS |
| 2026-08-13 | Concurrent observation and final-read failure | Forced a higher durable value between monotone advance and final read; cache/return observed that value. Forced final-read `OperationalError` after committed advance; `TimestampError` reported outcome ambiguity and durable floor remained installed. | PASS |
| 2026-08-13 | Parser/state-machine | Integer-header compatibility, mandatory field, canonical string output, former later-than-header acceptance (superseded by this revision), header-only/claimed floor, cache-ahead, concurrency, outcome ambiguity, parser property double, and `SM-DUMP-LOAD` fixtures. | HISTORICAL PASS; superseded behavior replaced by the passing bounded-export rows below |
| 2026-08-13 | Coordinated backend API v7 | Core 7.3.0; PostgreSQL/Redis 3.8.0; exact API-v7 literals; dependency/root-extra floors; three lockfiles; public/backend/release guidance and guards. | PASS; 143 targeted tests, Ruff, MyPy 62 files, three lock checks |
| 2026-08-13 | Real backend acceptance | `uv run ./bin/pytest-pg -q extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py`; `uv run ./bin/pytest-redis -q extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py`. | PASS; 3 PostgreSQL and 3 Valkey tests, no skips |
| 2026-08-13 | Full current-state gates | `uv run pytest`; repository-wide Ruff format/check; MyPy core/release/extensions; three `uv lock --check` commands; DOM-15, plan-context, doc-path, coalescing, and diff checks. | PASS; 2580 passed, 17 platform/opt-in skipped; all static/docs gates green |
| 2026-08-13 | Bounded live export and inline load defense | Sampled one H, applied the inclusive backend query bound, rejected records above H without pre-scanning, and exercised SQLite plus real PostgreSQL/Valkey shared dump paths. | PASS |
| 2026-08-13 | Header-only skew policy | Exact five-minute boundary, physical-grain comparison, warning text and allocation budget, excessive-skew pre-mutation refusal, force, quiet, typed override, public warning import, and garbage-validation preservation. | PASS |
| 2026-08-13 | Timestamp/error hardening | Known versus ambiguous failures, cache-ahead and concurrent winner, one final durable read, `None` under `python -O`, SQLite/PostgreSQL missing metadata, and real Redis EVAL-then-disconnect ambiguity. | PASS |
| 2026-08-13 | CLI/API integration | Exact public signatures, pre-consumption capability failure, conditional recovery guidance, unrelated-warning replay, strict non-negative integer config, and SQLite/PostgreSQL/Redis CLI shards. | PASS |
| 2026-08-13 | Reopened full-state gates | `uv run pytest -q`; repository Ruff format/check; exact core-test MyPy plus core/release/extensions MyPy; three lock checks; DOM-15, plan-context, doc-path, and diff checks. | PASS; 17 platform/opt-in skips; no failures |
| 2026-08-13 | Owner-directed minor version bump | Advanced the coordinated unpublished set from core 7.2.0/extensions 3.7.0 to core 7.3.0/extensions 3.8.0; raised both extension core floors and root extra floors; regenerated all three locks; dated the changelog entries. | PASS; 144 release/API tests, Ruff, core MyPy, three lock checks, docs gates, and diff check |
| 2026-08-13 | Attached-review remediation | Parser-totality warning suppression with a deterministic future-header example; immediate CLI warning translation; floor/force assertions; stable subprocess skew margins; expanded `SM-DUMP-LOAD`; real-backend pass-through timestamp spies; traceability and compatibility notes. | PASS; full core suite with 17 expected skips, 15 PostgreSQL shared tests, 15 Valkey shared tests, Ruff, exact test/core/extension MyPy, three lock checks, docs/coalescing/diff gates |

## 14. Fresh-Eyes Gate

Re-check every named seam exists, mandatory `last_ts` is explicit,
header-only restore has real backend proof, every emitted message ID is `<= H`
on SQLite/PostgreSQL/Redis, and excessive skew is rejected from the header
before mutation without a full-file pre-scan. Check the exact five-minute edge,
forced warning, Python/CLI parity, truthful 4,095-or-fewer allocation warning,
caller-owned freshness language, cache-ahead advancement, missing-meta failure,
typed ambiguity, early structural capability check, API-v6 rejection, integer
header compatibility, and the 7.1.0 changelog boundary. Confirm no generic
snapshot or direct-`insert_messages()` far-future policy entered the contract.

## 14a. Agent-Facing Interface Review

Scope: the `broker load [--force]` CLI delta and its stderr/quiet behavior in
the current uncommitted worktree. Baseline: the promoted `[SB-CLI-2]` and
`[SB-IO-4]` contracts. The required eleven-principle walk follows.

| Principle | Result | Evidence / rationale |
|---|---|---|
| 1. Context is the scarcest resource | Met | Success stays silent; warnings and failures are one-line diagnostics (`simplebroker/commands.py:1189-1219`). |
| 2. Progressive disclosure | Met | `load --help` owns the force explanation at the parser (`simplebroker/cli.py:406-415`); the persistence spec owns detail. |
| 3. Self-explanatory names | Met | `--force`, `--quiet`, and `broker load: warning:` read at point of use (`simplebroker/cli.py:406-415`, `simplebroker/commands.py:1189-1195`). |
| 4. One identity per thing | Not applicable | The CLI adds no new resource identity. |
| 5. Derive what is derivable | Met | Load derives physical skew and remaining logical capacity from header H; callers supply neither (`simplebroker/_dump.py:187-220`). |
| 6. No hidden session setup | Met | Input is stdin; force is explicit; the environment/config threshold is inspectable and documented under `[SB-IO-4]`. |
| 7. Teach, don't reject | Met | Ordinary skew warns and proceeds; only excessive skew is refused, with an explicit force path (`simplebroker/_dump.py:206-220`). |
| 8. Every message carries its action | Met | Warning names capacity/wait consequences; timestamp failures distinguish inspect/recreate from clean retry (`simplebroker/commands.py:1211-1219`). |
| 9. Atomic writes with recovery | Declared departure | Streaming load may be partial by design; `[SB-IO-4]` and the callable doc direct retry into a clean destination (`simplebroker/_dump.py:230-258`). |
| 10. Draw the trust boundary | Met | `--force` bypasses only operator-judged clock-skew refusal, never format or persistence validation (`tests/test_cli_dump_load.py::test_load_force_does_not_bypass_format_validation`). |
| 11. Mental-model wire format | Met | The CLI consumes the existing line-oriented dump stream and reports its 1-based input line, not storage details (`simplebroker/_dump.py:282-314`). |

Interface findings:

| ID | Severity | Location | Finding | Disposition |
|---|---|---|---|---|
| IR-1 | P2 | module-level `load_config()` consumers, including `simplebroker/_broker_session.py:19` | Invalid environment config can raise during package import before the CLI error boundary. This predates the dump change, but the new key adds another trigger and fails the generic no-traceback probe. | Confirmed; requires a separate CLI-wide configuration-lifecycle plan. A local `cli.py` catch was tested and rejected because package import fails first. Do not hide bad config with fallback defaults. |
| IR-2 | P2 | `simplebroker/commands.py:1178-1200` | Deferred warning replay changed warning timing and could mask an active load exception. | Fixed with an immediate category-aware `showwarning` shim; unrelated filters and timing remain authoritative. |

Ratified judgment: do not warn merely because traversal excludes a row above H.
A normal concurrent write and anomalously lagging source metadata are
indistinguishable during a live logical export; warning would report expected
concurrency as corruption. `[SB-IO-2]` now states this boundary. Verdict:
**no dump/load-delta blocker; IR-1 remains a repository-wide CLI hardening
blocker before claiming the generic adversarial no-traceback floor.**
Runbook feedback: no new reusable principle candidate; the existing
no-traceback and immediate-action principles already identify both findings.

## 15. Revision Log

| Date | Revision | Evidence / decision |
|---|---|---|
| 2026-08-13 | Corrected Class 4 to Class 5 with mandatory hardening; added durable cache-ahead semantics, concurrent-winner proof, documented integer/string header compatibility, coordinated backend API v7, cross-backend firing tests, and separate publication authorization. Kept far-future allocation policy out of scope. | Independent completed-work review findings plus owner decisions in this session. |
| 2026-08-13 | Corrected Strategy-A promotion order, scoped return/cache semantics to one final durable read, defined outcome-ambiguous read failure, and made exact package floors/files plus non-publishing lock commands explicit. | Independent revised-plan review round 3. |
| 2026-08-13 | Reopened completion: made header H an inclusive export bound, added configurable 300-second header-only load skew policy and `--force`, accepted caller-owned destructive/partial load semantics, and planned F2-F10 remediation. Prior completion evidence is historical only until the new slices pass. | Owner decisions and attached completed-work review. |
| 2026-08-13 | Advanced every coordinated package by one additional minor version and dated the completed changelog section for 2026-08-13. | Owner direction before commit. |
| 2026-08-13 | Dispositioned attached N3a-N10 review: restored parser-totality under warnings-as-errors, made CLI warning translation immediate, strengthened skew/state-machine tests, replaced avoidable timestamp fakes with pass-through real-backend spies, aligned traceability/docs, and completed the required interface review. | Attached review plus reproduced regressions; IR-1 retained as explicit CLI-wide follow-up. |
