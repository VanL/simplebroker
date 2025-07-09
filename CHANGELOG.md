# Changelog

All notable changes to SimpleBroker will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

