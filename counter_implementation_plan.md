# Counter Implementation Plan for Strict FIFO

## Overview
Implement a counter-based timestamp solution to guarantee strict FIFO ordering in concurrent write scenarios.

## Pre-Coordination

### Shared Implementation Details
- **Location**: Modify `BrokerDB` class in `simplebroker/db.py`
- **Approach**: Add thread-safe counter to generate unique timestamps
- **Backward Compatibility**: Maintain existing table schema (ts REAL)
- **Performance**: Minimal overhead (<0.03% of write latency)

### Implementation Strategy
1. Add instance variables for counter and lock
2. Modify `write()` method to use counter-enhanced timestamps
3. Ensure thread safety with proper locking
4. Maintain microsecond precision with counter as fractional nanoseconds

### Testing Requirements
- Verify strict FIFO ordering in concurrent scenarios
- Ensure backward compatibility with existing data
- Confirm performance remains acceptable
- Test with high concurrency (8+ threads)

## Task Breakdown for Parallel Execution

### Task 1: Core Implementation
Modify `db.py` to add counter-based timestamps:
- Add `_write_lock` and `_counter` instance variables
- Update `write()` method with thread-safe counter logic
- Maintain existing API compatibility

### Task 2: Test Updates
Update `test_concurrency.py` to verify strict FIFO:
- Enhance parallel write test to check exact ordering
- Add test for timestamp uniqueness
- Verify no performance regression

### Task 3: Documentation
Update inline documentation and comments:
- Document the counter approach in `db.py`
- Add notes about FIFO guarantees
- Update any relevant docstrings