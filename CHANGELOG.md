# Changelog

All notable changes to SimpleBroker will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

