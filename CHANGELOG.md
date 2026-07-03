# Changelog

All notable changes to SimpleBroker will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Fixed
- `watch --after` now applies its strict checkpoint filter in consume mode, not
  only peek mode. Consume-mode watchers no longer claim and deliver messages at
  or before the checkpoint boundary.
- Malformed `-m` / `--message` IDs now use the shared command error path:
  plain mode prints `invalid message ID: expected exactly 19 digits within range`
  on stderr and exits `1`, while `--json` emits the same diagnostic as a
  structured `INVALID_MESSAGE_ID` error. Valid but absent IDs still remain the
  silent "no matching message" case and exit `2`.

### Changed
- Exact message-ID APIs now share one validator. Python APIs that target exact
  IDs accept integer IDs or exact 19-digit string IDs; malformed string IDs
  raise `ValueError`, unsupported types such as `bool` raise `TypeError`, and
  `after_timestamp` / `before_timestamp` remain int-only bounds.

### Documented
- Documented the move-vs-checkpoint skip: `move` preserves a message's original
  timestamp (stable IDs), so any timestamp-checkpoint consumer — `peek --after`,
  `read --after`, a peek-mode watcher, a consume-mode watcher started with
  `after_timestamp`, or a hand-rolled `ts > last_seen` filter — permanently
  skips messages moved into its queue behind its checkpoint. Consume without a
  timestamp filter or rescan from `--after 0`. Added a README caveat, watcher
  docstring caveats, and characterization tests pinning the behavior.

### simplebroker-pg
- `rename_queue()` now takes the singleton meta row before the `messages` table,
  matching writer and broadcast lock order. This avoids a retryable `40P01`
  deadlock when aliases are retargeted during a concurrent write.

## [4.10.0] - 2026-07-02
### Fixed
- `write()` now allocates its timestamp inside the insert transaction, so the
  `last_ts` advance and the message row become visible atomically. Previously
  a concurrent writer could commit a higher timestamp during another writer's
  lock wait, letting checkpoint readers (`peek --after`, peek-mode watchers)
  permanently skip a message. Applies to SQLite and Postgres (see the
  simplebroker-pg entry below); the Redis backend was already atomic.
- `broker write --help` and `broker broadcast --help` now show help.
  Previously `write --help` failed with a queue-name error and
  `broadcast --help` silently broadcast the literal message `--help` to every
  queue. Use `-- --help` to write a literal `--help` message.
- `alias` is registered in the argument rearranger's subcommand set. Previously
  global-looking flags after `alias` were hoisted to global position:
  `broker alias add a b --cleanup` deleted the database. Such flags now fail
  with "unrecognized arguments".
- `--cleanup` combined with a command is now rejected ("--cleanup cannot be
  used with commands"), matching the existing `--status`/`--vacuum` guards.
  Previously the command was silently dropped and the database deleted.
- Retry classification is backend-neutral: runners can set
  `OperationalError.retryable` instead of relying on SQLite lock-message
  matching (`True` forces retry, `False` forbids it, `None` keeps the
  SQLite marker fallback). See the simplebroker-pg entry below for the
  Postgres side.
- `build_move_by_id_query` (private `_sql` module, used by
  `examples/async_pooled_broker.py`) emitted invalid SQL (`ORDER BY` after
  `RETURNING`); the example's move-by-ID path crashed. All four legacy
  builders now have executable-SQL tests.
- README: removed the nonexistent `watch --quiet` flag (use global `-q`),
  documented the `--compact` global option, fixed the `alias` command row,
  and replaced the network-filesystem recommendation with a corruption
  warning (WAL mode is single-host).

### Removed
- Internal: byte-identical duplicated alias methods on `BrokerDB` (behavior
  unchanged; `BrokerCore` remains the single implementation).

### Internal
- Test harness scrubs ambient `BROKER_*` environment variables in
  `pytest_configure`, before any test runs (`BROKER_TEST_BACKEND`
  allowlisted).
- mypy now type-checks against the Python 3.11 floor instead of 3.14.
- `ext.py`, `SQLRunner`, and `BackendPlugin` now document the real
  backend-author contract, including the getattr-probed optional hooks.

### simplebroker-pg 2.5.0
- Postgres contention SQLSTATEs (`55P03` lock_not_available, `40001`
  serialization_failure, `40P01` deadlock_detected) are now marked
  `retryable`, so the core retry machinery retries them instead of failing
  immediately.
- `prepare_broadcast` takes the meta `last_ts` row lock before the messages
  table lock, matching the write path's lock order now that writers allocate
  their timestamp inside the insert transaction. The old order deadlocked
  against in-flight writes. Requires simplebroker>=4.10.0.

## [4.9.0] - 2026-06-18
### Added
- Added `BrokerConnection.rename_queue(...)`, `QueueRenameResult`, and
  `broker rename <old> <new> [--json]` for retagging existing queue state
  without a schema migration. SQLite, Postgres, and Redis support the API;
  claimed messages are included, target queues must be empty, and aliases
  targeting the old queue retarget by default.

### Fixed
- Postgres activity listeners now drain batched notifications from one poll,
  ensuring queue-specific notifications emitted in the same transaction are not
  dropped.

### simplebroker-pg 2.4.0
- Added Postgres backend support for
  `BrokerConnection.rename_queue(...)`, including table-level serialization and
  old/new queue LISTEN/NOTIFY wakeups. Requires simplebroker>=4.9.0.

### simplebroker-redis 2.6.0
- Added Redis backend support for
  `BrokerConnection.rename_queue(...)`, implemented as one atomic Lua script
  that preserves pending/claimed state, rejects active source reservations, and
  publishes old/new queue activity. Requires simplebroker>=4.9.0.

## [4.8.0] - 2026-06-18
### Added
- Added `Queue.latest_pending_timestamp()` and
  `BrokerConnection.latest_pending_timestamp(queue)` to retrieve the newest
  pending message timestamp for one queue without scanning message history.
  SQLite, Postgres, and Redis backends implement the API; SQLite schema v5 adds
  `idx_messages_pending_queue_ts` for the lookup.
- Coverage-guided fuzzing (Atheris) of the timestamp parser and the
  dump/load parser, reusing the existing Hypothesis properties through
  their external-fuzzer hook (`fuzz/`, weekly `Fuzz` workflow with a
  persistent corpus). Verified to rediscover the known YYYYMMDD parsing
  quirk from a cold corpus in seconds.

### Fixed
- Postgres schema migration now idempotently recreates
  `idx_messages_queue_ts_order_unclaimed` even when the schema already reports
  the current version, repairing drifted schemas without a version bump.

### simplebroker-pg 2.3.0
- Added Postgres backend support for
  `BrokerConnection.latest_pending_timestamp(queue)`, using the existing
  pending queue/timestamp index.
- Postgres schema migration now idempotently recreates
  `idx_messages_queue_ts_order_unclaimed` for current-version schemas when the
  index is missing. Requires simplebroker>=4.8.0.

### simplebroker-redis 2.5.0
- Added Redis backend support for
  `BrokerConnection.latest_pending_timestamp(queue)`, returning the newest
  pending timestamp while skipping IDs reserved by active at-least-once
  batches. Requires simplebroker>=4.8.0.

## [4.7.0] - 2026-06-11
### Changed
- Pre-epoch ISO dates passed to timestamp parsing (e.g. `--after 1950-01-01`)
  now clamp to the Unix epoch, uniformly meaning "everything". Previously
  they parsed to negative internal values that the CLI accepted by accident
  while API bound checks rejected them.

### Added
- Added `broker dump` and `broker load`: a versioned ndjson backup/restore and
  backend-migration format on stdout/stdin, with repeatable `--include`/`--exclude`
  queue-name globs on dump (aliases match on either their own name or their
  target; exclude wins; the flags compose). Mirrored Python APIs:
  `dump_lines(broker, *, include, exclude)` and `load_lines(broker, lines)`
  (plus `LoadResult`). Dumps are deterministic (header, sorted aliases, sorted
  queues, ascending message-ID order; pending messages only) and restore with
  exact message IDs on any backend — `broker dump | BROKER_BACKEND=postgres
  broker load` migrates a broker in one pipeline. Built entirely on the public
  connection surface; no backend changes.
- Property-based test suite (Hypothesis): timestamp-parser totality and
  round-trip properties, cross-backend queue-name/body round-trip properties,
  and a stateful reference-model test that runs identical operation sequences
  against the SQLite, Postgres, and Redis backends.

### Fixed
- `TimestampGenerator.validate()` now raises `TimestampError` (as documented)
  instead of leaking `ValueError` for ISO dates beyond the year-2262
  timestamp horizon. CLI behavior is unchanged.
- Invalid queue names now raise `QueueNameError` and invalid message bodies
  (oversize, or not UTF-8 encodable) raise `MessageError`, as the docstrings
  always promised. Both exception types now subclass `ValueError`, so
  existing `except ValueError` handlers keep working unchanged.
- Queue-name validation now uses `fullmatch`, closing a regex quirk where a
  name with a trailing newline (e.g. `"jobs\n"`) was accepted despite the
  documented character set. Such names — previously creatable only through
  the Python API — are now rejected at first use, matching the prefix
  validator's existing strictness.
- `bin/pytest-pg` now applies the default suite paths for flag-only
  invocations (`pytest-pg -q` previously made the extension phase collect
  zero tests and exit 5).

### simplebroker-redis 2.4.1
- Aligned validation errors with simplebroker 4.7.0: invalid queue names
  raise `QueueNameError` and oversize or non-UTF-8-encodable bodies raise
  `MessageError` (both `ValueError` subclasses), matching the SQLite and
  Postgres backends. Caught by the new cross-backend property suite.
  Requires simplebroker>=4.7.0 (where those exception types gained their
  `ValueError` base).

## [4.6.0] - 2026-06-10
### Added
- Added `include_claimed` to the public peek surface — `Queue.peek/peek_one/
  peek_many/peek_generator`, the `BrokerConnection` protocol, and the CLI
  (`broker peek --include-claimed`). When set, peeks return claimed (consumed
  but not yet vacuumed) messages merged with pending ones in message-ID order.
  Claimed rows are deletion-pending; peeking never changes claim state. This
  completes the public claimed-row round trip started by
  `find_message_ids(include_claimed=...)`.

### simplebroker-redis 2.4.0
- Implemented `include_claimed` peeks by merging the per-queue pending and
  claimed ZSETs. Requires simplebroker>=4.6.0.

## [4.5.0] - 2026-06-10
### Added
- Added a public sidecar-table API for embedding applications: `Queue.sidecar()` /
  `BrokerCore.sidecar()` yield a `SidecarSession` for caller-owned tables in the
  broker's database, inheriting the broker's retry loop, locking, and
  ephemeral-vs-persistent connection discipline. Exported `SidecarSession`,
  `SidecarUnavailableError`, and `RESERVED_TABLE_NAMES` from `simplebroker.ext`.
- Promoted the watcher subclassing contract into `simplebroker.ext`: `BaseWatcher`,
  `PollingStrategy`, `default_error_handler`, and the new public `StopWatching`
  exception (the former private `_StopLoop`, which remains as an alias).

### simplebroker-redis 2.3.1
- `RedisBrokerCore.sidecar()` raises `SidecarUnavailableError` (the Redis backend has
  no SQL storage). Requires simplebroker>=4.5.0.

## [4.3.0] - 2026-06-01
### Changed
- Replaced the 4.1/4.2 exact-ID APIs with a single API-only `insert_messages(...)` method on broker handles and `Queue`. It accepts one or more exact-ID records, advances `last_ts` above the largest supplied ID inside the same transaction, and inserts pending messages with their supplied IDs.

### Removed
- Removed `import_message(...)`, `import_messages(...)`, and `write_reserved_message(...)` before they had external compatibility requirements.

### simplebroker-pg 2.2.0
- Bumped the Postgres extension for the SimpleBroker 4.3.0 exact-ID `insert_messages(...)` API.

### simplebroker-redis 2.2.0
- Bumped the Redis extension for the SimpleBroker 4.3.0 exact-ID `insert_messages(...)` API.
- Fixed Redis `last_ts` advancement to compare 64-bit timestamp strings exactly instead of converting them to Lua numbers.

## [4.2.0] - 2026-06-01
### Added
- Added API-only `write_reserved_message(...)` for writing a message with a previously generated broker message ID.
- Added API-only `import_messages(...)` for atomic dump/load restore that advances `last_ts` above the imported message IDs before inserting rows.

## [4.1.0] - 2026-06-01
### Added
- Added API-only `import_message(...)` on broker handles and `Queue` for restoring pending messages with exact historical message IDs. The supplied ID must be lower than the broker's current `last_ts`.

## [4.0.0] - 2026-05-30
### Changed
- Dropped Python 3.10 support. SimpleBroker and its Postgres and Redis extension packages now require Python 3.11+.
- `.broker.toml` parsing now uses the Python standard library `tomllib` module.

### simplebroker-pg 2.0.0
- Dropped Python 3.10 support. The Postgres extension now requires Python 3.11+ and SimpleBroker 4.0.0+.

### simplebroker-redis 2.0.0
- Dropped Python 3.10 support. The Redis extension now requires Python 3.11+ and SimpleBroker 4.0.0+.

## [3.8.0] - 2026-05-20
### Added
- Added `delete_from_queues(...)` for backend-level physical deletion across several queues, with strict `before_timestamp` filtering and consistent SQLite, Postgres, and Redis behavior.
- Added `find_message_ids(...)` on broker handles and queues for API-only, literal, case-sensitive message body substring search that returns message IDs without mutating messages.

### Changed
- `list_queues()` now returns queue names only, including claimed-only queues. Use `list_queue_stats()`, `get_queue_stats()`, or `get_queue_stat()` when counts are needed.
- `broker list` now prints queue names by default. Use `broker list --stats` to include pending, claimed, and total counts.
- Updated README and async examples to use names-only queue listing and explicit stats APIs for counts.

### simplebroker-pg 1.6.0
- Added Postgres implementations for multi-queue physical delete, literal body substring ID search, and names-only queue listing with prefix filtering.

### simplebroker-redis 1.1.0
- Added Redis implementations for multi-queue physical delete, literal body substring ID search, and names-only queue listing from the Redis queue-name set.
- Redis batch deletion now refuses active at-least-once reserved batches, while body search skips active reserved IDs without mutating them.

## [3.7.1] - 2026-05-18
### Fixed
- Make connection phases strict on POSIX

## [3.7.0] - 2026-05-17
### Added
- Added full delete APIs, including batch delete
- Released redis 1.0 backend
### Changed
- Changed all delete APIs to actually delete, not just claim. Read still performs a claim

## [3.6.0] - 2026-05-14
### Added
- Added a shared command-layer error formatter. Commands that already accept `--json` now emit structured JSON errors on stderr when `--json` is present, while non-JSON invocations keep the canonical prose error format.

### Changed
- Invalid `-m` / `--message` IDs now return exit code `1` as invalid input. Valid message IDs that do not match any message still return exit code `2`.
- `init`, `--cleanup`, and `--vacuum` status messages now go to stderr instead of stdout, keeping stdout reserved for command payloads.
- `--quiet` now suppresses `--vacuum` and `--cleanup` status output consistently.
- `broker alias list --target <queue>` now emits no stdout and returns exit code `2` when no aliases match the target.
- Moved owned-runner shutdown into `simplebroker._runner` and removed the internal `_runner_lifecycle.py` module.
- Materialized batch APIs continue to accept `delivery_guarantee="at_least_once"` and satisfy it with stricter exactly-once materialization. Use generator APIs for retry-on-stop batch processing.

### Fixed
- `--cleanup` now validates SQLite targets as SimpleBroker databases before deleting them, including legacy path mode and project-config SQLite targets.
- SQLite `last_ts` advancement now uses a single `UPDATE ... RETURNING` query instead of a separate `SELECT changes()` statement.
- Hot-path batch retrieval and SQLite metadata reads avoid redundant list copies when the runner already returned a list.

### simplebroker-pg 1.4.0
- Compatibility release for SimpleBroker 3.6.0. The Postgres backend inherits the updated core CLI behavior when used with SimpleBroker 3.6.0; there are no Postgres-specific schema or runner changes in this release.

## [3.5.0] - 2026-05-13
### Added
- Added `--before <timestamp>` for `read`, `peek`, and `move`, using the same timestamp formats as `--since` and strict `ts < timestamp` filtering.
- Added `before_timestamp` range filtering to the public `Queue` and `BrokerCore` read, peek, move, and stream APIs.
### Changed
- Moved --since to --after to mirror --before

## [3.4.0] - 2026-05-13
### Added
- Added targeted queue metadata APIs: `Queue.exists()`, `Queue.stats()`, `BrokerCore.queue_exists()`, `BrokerCore.get_queue_stat()`, and `BrokerCore.list_queue_stats(...)`.
- Added CLI metadata commands and filters: `broker exists`, `broker stats`, and `broker list --prefix ... --json`.
- Added required backend SQL namespace entries for exact queue stats and prefix-filtered queue stats. No schema migration is required.

## [3.3.0] - 2026-05-05
### Added
- Added an internal phase-lock coordinator that serializes ordered setup phases with advisory lock files, durable xattr completion hints, and a status-file fallback for filesystems without usable xattrs.
- Added `create_activity_waiter_for_queues(...)` and top-level `ActivityWaiter` exports for integrations that need one backend-native wake hint across several queues, with polling remaining the fallback when a backend has no efficient hook.
- Added PostgreSQL multi-queue activity waiter support using the existing process-local shared LISTEN/NOTIFY listener instead of one listener connection per watched queue.

### Changed
- Persistent queue handles for the same resolved backend target now share process-local backend session state, preventing backend runner or pool allocation from scaling with same-process queue count while preserving thread and process isolation.
- Backend plugins can opt into the multi-queue activity waiter hook without changing the required `BackendPlugin` protocol surface.

## [3.2.0] - 2026-05-01
### Added
- Added the repo-local `bin/release.py` helper for core and `simplebroker-pg` releases, including version validation, release-state checks, preflight commands, and tag planning.

### Fixed
- Serialized SQLite schema bootstrap across processes with a versioned setup marker, preventing high-concurrency xdist/worker runs from entering schema creation or migration for the same fresh database at the same time.

## [3.1.9] - 2026-04-23
### Fixed
- Fixed resolution of the broker.toml file for the pg backend to use the same rules as broker.b

## [3.1.5] - 2026-04-08
### Fixed
- Addressed a race in concurrent lock file handling under high load

## [3.1.4] - 2026-04-06
### Changed
- Add a `pg` packaging extra so `simplebroker[pg]` installs the external `simplebroker-pg` plugin.
- Lower `simplebroker-pg` to Python 3.10+ and align local development with uv source mapping for the sibling extension.

## [3.1.3] - 2026-04-05
### Fixed
- Fix inconsistencies between the pg and sqlite backends

## [3.1.1] - 2026-04-04
### Fixed
- Normalize partial config overrides against the default config snapshot so non-SQLite backends do not fail connection setup with missing shared keys like `BROKER_AUTO_VACUUM_INTERVAL`.

## [3.1.0] - 2026-04-03
### Added
- Add piped input support

## [3.0.0] - 2026-04-01
### Added
- Pluggable backend system with `BackendPlugin` protocol and entry-point registration.
- `BackendSQLNamespace` contract with import-time validation (`ensure_backend_sql_namespace`).
- `ResolvedTarget` and `serialize_broker_target`/`deserialize_broker_target` for backend-agnostic target resolution.
- `.broker.toml` project configuration for multi-backend support.
- Postgres backend extension (`simplebroker-pg` 1.0.0) with advisory lock serialization, LISTEN/NOTIFY watcher wakeups, typed singleton meta table, and connection pooling via `psycopg_pool`.
- Backend-agnostic test infrastructure (`broker_factory`, `broker_target`/`broker`/`queue_factory` fixtures) — 543 of 831 tests now run on both SQLite and Postgres.

### Fixed
- Timestamp conflict detection in `BrokerCore.write()` no longer depends on SQLite-specific error message text.
- `initialize_target` on Postgres rejects any pre-existing schema that isn't ABSENT or OWNED.

### Changed
- Backend plugin runner protocols use public `schema`/`dsn` properties instead of private attribute casts.
- Test container for Postgres runs with `max_connections=300` to support concurrency test workloads.

## [2.9.0] - 2026-03-30
### Changed
- Refactored SQLite-specific internals into dedicated internal backend modules under `simplebroker._backends.sqlite` and moved built-in SQL definitions into `simplebroker._sql.sqlite`.
- Standardized internal backend access through the new internal backend resolver used by helpers, runner setup, and database orchestration.
- Preserved the public API and CLI behavior while making the built-in SQLite implementation easier to isolate from future external backends.

## [2.8.6] - 2026-03-28
### Fixed
- Injected `runner=` queues now execute queue reads and writes through the supplied `SQLRunner` in both persistent and non-persistent modes instead of silently falling back to the built-in SQLite path.
- `stream_messages(..., all_messages=False)` now yields at most one message in both peek and consume modes.

### Changed
- Materialized batch APIs (`claim_many()`, `move_many()`, `Queue.read_many()`, and `Queue.move_many()`) commit before returning their result lists. Passing `delivery_guarantee="at_least_once"` is supported and satisfied by the stricter exactly-once materialization behavior; use the generator APIs for retryable batch processing.
- Injected runners are now explicitly caller-owned in the docs and examples, and are reused for the lifetime of the `Queue` object.
- Updated README wording to emphasize SimpleBroker as simple to install and operate, and refreshed documentation around generator batch semantics and delete behavior.

## [2.8.5] - 2026-02-10
### Changed
- Fixed `move_generator(..., exact_timestamp=...)` in exactly-once mode to honor the filter correctly.
- Transaction start failures now propagate as operational errors instead of being treated as empty queue results.
- Unified alias resolution for CLI queue operations (`peek`, `move`, `watch`, and `delete`) when using `@alias`.
- At-least-once generator semantics now commit only after a full batch is yielded; interrupted batches are rolled back for retry.
- Switched internal DB core lock to `RLock` to avoid re-entrant deadlocks during generator-driven callbacks.
- Replaced command-layer private DB handle access with public DB methods for claimed/overall stats.
- Updated docs to clarify at-least-once rollback/retry and lock contention tradeoffs for larger batch sizes.

## [2.8.4] - 2025-11-06
### Added
- `Queue.last_ts` lazy cache plus `refresh_last_ts()` for on-demand meta reads.
### Changed
- Watcher polling refreshes cached timestamps automatically on `PRAGMA data_version` changes.
- Documentation and regression tests covering timestamp caching behaviour.

## [2.8.3] - 2025-11-05
### Changed
- Fix regression in allowing an external dependency
- Add tests to enforce no external dependencies

## [2.8.2] - 2025-11-05
### Changed
- Internal exception handling, not visible in API

## [2.8.1] - 2025-11-05
### Added
- Added -p/--pattern to list command
### Changed
- Updated tests to be more deterministic on Windows
- Added run_with_retry helper and .set_stop_event so that queue watchers process stop events more quickly

## [2.7.2] - 2025-10-27
### Added
- Added .get_meta method on BrokerCore/BrokerDB to provide a readout of the meta table
- Added additional tests

## [2.7.1] - 2025-10-24
### Changed
- Updated tests on windows to use unbuffered I/O and longer timeouts

## [2.7.0] - 2025-10-24
### Added
- Selective broadcast support via `broker broadcast --pattern`, using fnmatch-style globs.
- Queue alias management commands (`broker alias add/remove/list`) with explicit `@alias` usage.
- Alias cache auto-refresh on version changes and warning when alias names already have messages.
- CLI option `broker alias list --target <queue>` and reverse lookup helper in the Python API.
- Dedicated test suites for alias DB/CLI behaviour.

### Changed
- Alias resolution now only occurs when the queue name is prefixed with `@`, keeping plain queue names untouched.
- Schema migration for alias support now wraps DDL in transactions and gracefully handles existing tables/indexes.

## [2.6.2] - 2025-10-23
### Added
- Set auto vacuum to INCREMENTAL by default 
- Added --compact paramenter to --vacuum (also enables incremental auto vacuum)
- No changes needed from calling code

## [2.6.1] - 2025-10-21
### Added
- Added --json as on option for the --status flag 
- Refactored commands.py to consolidate common logic
- Added tests

## [2.6.0] - 2025-10-21
### Added
- Added --status flag that efficiently reports global information about the database 
- Added .generate_timestamp() (alias: .get_ts()) methods on the Queue and db objects that delegate to the TimestampGenerator

## [2.5.1] - 2025-10-13
### Changed
- Updated Python version support: now requires Python 3.10+ (dropped 3.8, 3.9)
- Added Python 3.14 support
- Updated CI/CD testing matrix and package classifiers

## [2.5.0] - 2025-08-13
### Changed
- Added config keyword argument to functions that use config values
- Allows easier testing, passing configs from API users
- All existing code should work without change

## [2.4.0] - 2025-08-13
### Added
- Added project scoping rules, gated by BROKER_PROJECT_SCOPE=True
- Created new environment options for default database name and default db dir (-f/-d)
- Added tests

## [2.3.2] - 2025-08-11
### Changed
- Moved common functionality in Watchers to BaseWatcher
- Exposed PollingStrategy as an optional parameter to Watchers 
- Added __str__ and __repr__ to Queues for convenience

## [2.3.1] - 2025-08-11
### Added
- Default message handlers: `simple_print_handler`, `json_print_handler`, `logger_handler`
- Default error handler: `default_error_handler` with config-aware internal wrapper
- Example demonstrating all default handlers (`examples/simple_watcher_example.py`)

### Changed
- Error handlers now use function defaults instead of None checks
- Simplified error handling code paths and improved type signatures

## [2.3.0] - 2025-08-10
### Changed
- **BREAKING**: Watcher API reorganization for improved consistency:
  - All Watcher classes now use queue-first parameter order: `(queue, handler, *, db=None, stop_event=None)`
  - `QueueWatcher(queue_name, handler, db=db_path)` instead of `QueueWatcher(db_path, queue_name, handler)`
  - `QueueMoveWatcher(source_queue, dest_queue, handler, db=db_path)` follows same pattern
  - Removed complex dual-convention detection logic (175+ lines eliminated)
  - Simplified BaseWatcher architecture with consistent Queue object delegation
- Updated all examples and tests to use new API
- Improved docstring examples throughout watcher.py

### Technical
- Maintains CLI backward compatibility
- All watcher functionality preserved with cleaner, more intuitive API
- Enhanced test coverage for new API patterns

## [2.2.0] - 2025-08-09
### Added
- Connection tracking for persistent mode database connections
- Instance ID support for testing scenarios

### Changed
- Comprehensive API refactoring and improvements
- Enhanced Queue API with new methods and better error handling
- Improved connection management and resource cleanup
- Better Windows test compatibility and cleanup logic
- Enhanced signal handling responsiveness

### Technical
- Major internal restructuring (7,000+ lines changed)
- New comprehensive test suites for queue operations and SQL internals
- Improved batch operations and generator methods
- Better connection pooling and resource management

## [2.1.0] - 2025-08-06
### Added
- Constants centralization in `_constants.py`
- Thundering herd mitigation for watchers to prevent resource contention
- Comprehensive watcher test suites:
  - Burst mode handling
  - Metrics collection and monitoring
  - Multiprocess coordination
  - Race condition prevention
  - Performance optimization

### Changed
- Improved time handling with `time.monotonic()` for better accuracy
- Enhanced Windows test compatibility
- Better CLI command parsing and BaseWatcher refactoring
- Improved timestamp validation and jitter handling

### Technical
- Extensive test quality improvements
- Better test isolation and determinism
- Enhanced performance calibration system

## [2.0.1] - 2025-07-20
### Fixed
- Windows test compatibility and finalization logic
- Queue resource cleanup for Windows environments
- Minor formatting and documentation improvements

### Added
- Code coverage reporting with Codecov integration
- Additional test coverage and README badges

## [2.0.0] - 2025-07-19
### Added
- **New Python API** for programmatic queue operations:
  - `Queue` class with context manager support for simplified message handling
  - `QueueWatcher` and `QueueMoveWatcher` for real-time monitoring
- **Extension system** for custom backends:
  - `SQLRunner` protocol in `simplebroker.ext` for alternative database implementations
  - `TimestampGenerator` exposed for consistent timestamp handling across extensions
  - Example implementations included in new `examples/` directory
- `move` command to transfer specific messages by ID between queues
- Comprehensive examples directory with Python API usage and shell script patterns

### Changed
- **BREAKING**: Complete internal refactoring (CLI remains unchanged):
  - Core logic split into `BrokerCore` (database-agnostic) and runner implementations

### Technical
- Maintains full backward compatibility for CLI usage

## [1.5.0] - 2025-07-12
### Changed
- **BREAKING**: Renamed `purge` command to `delete` for consistency
- **BREAKING**: Renamed `watch --transfer` to `watch --move` for clarity
- Added UNIQUE constraint on timestamp column for improved data integrity

## [1.4.0] - 2025-07-11
### Added
- `watch` command with three distinct modes:
  - **Consume mode** (default): Process and remove messages
  - **Peek mode** (`--peek`): Monitor without consuming
  - **Move mode** (`--move`): Drain all messages to another queue

## [1.3.0] - 2025-07-09
- Performance optimizations
- New --vacuum flag and list --stats command to manage claimed messages

### Changed
- Improved differentiation between empty queue and filtered messages situations
- Better test determinism with adjusted timing

## [1.2.0] - 2025-07-09
### Added
- New `--since` flag to filter messages by timestamp
### Fixed
- Better application-level retry logic to handle SQLite's immediate return behavior
- Improved database contention handling during setup
- Enhanced concurrency logic in tests

## [1.1.2] - 2025-07-07
### Fixed
- Updated database setup contention logic

## [1.1.1] - 2025-07-07
### Changed
- Python 3.8 and 3.9 compatibility improved

## [1.1.0] - 2025-07-07
### Added
- Application-level retry logic for database operations
- Type checking support
- Windows compatibility improvements

### Fixed
- Type annotations and test errors
- Python 3.8 compatibility in cli.py
- Windows compatibility in parallel writes test
- Cross-platform path handling

### Changed
- Updated tests to use multiprocess for better simulation of expected usage
- Use non-destructive peek for test verification
- Various formatting improvements

## [1.0] - 2025-07-06
### Added
- Initial release of SimpleBroker
- Lightweight message queue backed by SQLite
- FIFO message guarantees
- Simple command-line interface
- Full test suite
