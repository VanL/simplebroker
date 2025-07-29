"""Tests for performance optimizations."""

import sys

import simplebroker.cli
from simplebroker.cli import main
from simplebroker.db import _validate_queue_name_cached


def test_parser_caching():
    """Test that argument parser is cached between calls."""
    # Clear any existing cache
    simplebroker.cli._PARSER_CACHE = None

    # Call main twice with minimal args that return quickly
    # We'll use --version which exits immediately

    original_argv = sys.argv
    try:
        # First call should create parser
        sys.argv = ["broker", "--version"]
        try:
            main()
        except SystemExit:
            pass

        # Cache should now be populated
        assert simplebroker.cli._PARSER_CACHE is not None
        first_parser = simplebroker.cli._PARSER_CACHE

        # Second call should reuse cached parser
        sys.argv = ["broker", "--version"]
        try:
            main()
        except SystemExit:
            pass

        # Should be the same parser instance
        assert simplebroker.cli._PARSER_CACHE is first_parser
    finally:
        sys.argv = original_argv


def test_queue_name_validation_caching():
    """Test that queue name validation is cached."""
    # Clear the cache
    _validate_queue_name_cached.cache_clear()

    # First call - should be cached
    result1 = _validate_queue_name_cached("test_queue")
    assert result1 is None  # Valid queue name

    # Check cache stats
    info = _validate_queue_name_cached.cache_info()
    assert info.hits == 0
    assert info.misses == 1
    assert info.currsize == 1

    # Second call with same name - should hit cache
    result2 = _validate_queue_name_cached("test_queue")
    assert result2 is None

    info = _validate_queue_name_cached.cache_info()
    assert info.hits == 1
    assert info.misses == 1

    # Different queue name - cache miss
    result3 = _validate_queue_name_cached("another_queue")
    assert result3 is None

    info = _validate_queue_name_cached.cache_info()
    assert info.hits == 1
    assert info.misses == 2

    # Invalid queue name - should also be cached
    result4 = _validate_queue_name_cached("")
    assert "empty" in result4

    # Same invalid input - should hit cache
    result5 = _validate_queue_name_cached("")
    assert result5 == result4

    info = _validate_queue_name_cached.cache_info()
    assert info.hits == 2
    assert info.misses == 3


def test_cache_size_limit():
    """Test that cache respects size limit."""
    # Clear the cache
    _validate_queue_name_cached.cache_clear()

    # Default max size is 1024
    # Add more than 1024 unique queue names
    for i in range(1100):
        _validate_queue_name_cached(f"queue_{i}")

    # Cache size should not exceed maxsize
    info = _validate_queue_name_cached.cache_info()
    assert info.currsize <= 1024
    assert info.misses == 1100

    # Verify LRU eviction - early entries should be evicted
    # This will be a cache miss because it was evicted
    _validate_queue_name_cached("queue_0")
    info = _validate_queue_name_cached.cache_info()
    assert info.misses == 1101
