# Changelog

All notable changes to SimpleBroker will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
