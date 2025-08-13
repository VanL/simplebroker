"""Tests for the _constants module."""

import os
from unittest.mock import patch

import pytest

from simplebroker._constants import (
    # Database
    DEFAULT_DB_NAME,
    EXIT_QUEUE_EMPTY,
    # Exit codes
    EXIT_SUCCESS,
    LOGICAL_COUNTER_BITS,
    LOGICAL_COUNTER_MASK,
    MAX_ITERATIONS,
    MAX_LOGICAL_COUNTER,
    # Message constraints
    MAX_MESSAGE_SIZE,
    # Project scoping constants
    MAX_PROJECT_TRAVERSAL_DEPTH,
    MAX_QUEUE_NAME_LENGTH,
    # Watcher
    MAX_TOTAL_RETRY_TIME,
    # Time units
    MS_PER_SECOND,
    MS_PER_US,
    NS_PER_SECOND,
    NS_PER_US,
    PHYSICAL_TIME_BITS,
    # Program
    PROG_NAME,
    SCHEMA_VERSION,
    SIMPLEBROKER_MAGIC,
    SQLITE_MAX_INT64,
    # Timestamp constants
    TIMESTAMP_EXACT_NUM_DIGITS,
    UNIX_NATIVE_BOUNDARY,
    US_PER_SECOND,
    WAIT_FOR_NEXT_INCREMENT,
    # Database phases
    ConnectionPhase,
    # Version
    __version__,
    _parse_bool,
    # Functions
    load_config,
)


class TestConstants:
    """Test that all constants are defined with expected values."""

    def test_version(self) -> None:
        """Test version constant is consistent with pyproject.toml."""
        assert isinstance(__version__, str)

        # Check consistency with pyproject.toml
        import re
        from pathlib import Path

        pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
        with open(pyproject_path, encoding="utf-8") as f:
            content = f.read()

        # Find version in pyproject.toml using regex
        # Look for version = "x.y.z" pattern
        match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
        if not match:
            raise ValueError("Could not find version in pyproject.toml")

        pyproject_version = match.group(1)
        assert __version__ == pyproject_version, (
            f"Version mismatch: __version__={__version__} but "
            f"pyproject.toml has version={pyproject_version}"
        )

    def test_program_constants(self) -> None:
        """Test program identification constants."""
        assert PROG_NAME == "simplebroker"
        assert isinstance(PROG_NAME, str)

    def test_database_constants(self) -> None:
        """Test database-related constants."""
        assert DEFAULT_DB_NAME == ".broker.db"
        assert SIMPLEBROKER_MAGIC == "simplebroker-v1"
        assert SCHEMA_VERSION == 1
        assert isinstance(DEFAULT_DB_NAME, str)
        assert isinstance(SIMPLEBROKER_MAGIC, str)
        assert isinstance(SCHEMA_VERSION, int)

    def test_exit_codes(self) -> None:
        """Test exit code constants."""
        assert EXIT_SUCCESS == 0
        assert EXIT_QUEUE_EMPTY == 2
        assert isinstance(EXIT_SUCCESS, int)
        assert isinstance(EXIT_QUEUE_EMPTY, int)

    def test_message_constraints(self) -> None:
        """Test message and queue constraint constants."""
        assert MAX_MESSAGE_SIZE == 10 * 1024 * 1024  # 10MB
        assert MAX_QUEUE_NAME_LENGTH == 512
        assert isinstance(MAX_MESSAGE_SIZE, int)
        assert isinstance(MAX_QUEUE_NAME_LENGTH, int)
        assert MAX_MESSAGE_SIZE > 0
        assert MAX_QUEUE_NAME_LENGTH > 0

    def test_timestamp_constants(self) -> None:
        """Test timestamp generation constants."""
        assert TIMESTAMP_EXACT_NUM_DIGITS == 19
        assert PHYSICAL_TIME_BITS == 52
        assert LOGICAL_COUNTER_BITS == 12
        assert LOGICAL_COUNTER_MASK == (1 << LOGICAL_COUNTER_BITS) - 1
        assert MAX_LOGICAL_COUNTER == 1 << LOGICAL_COUNTER_BITS
        assert UNIX_NATIVE_BOUNDARY == 2**44
        assert SQLITE_MAX_INT64 == 2**63

        # Verify bit calculations
        assert LOGICAL_COUNTER_MASK == 0xFFF  # 12 bits of 1s
        assert MAX_LOGICAL_COUNTER == 4096

        # Verify total bits don't exceed 64
        assert PHYSICAL_TIME_BITS + LOGICAL_COUNTER_BITS <= 64

    def test_time_unit_constants(self) -> None:
        """Test time unit conversion constants."""
        assert MS_PER_SECOND == 1000
        assert US_PER_SECOND == 1_000_000
        assert MS_PER_US == 1000
        assert NS_PER_US == 1000
        assert NS_PER_SECOND == 1_000_000_000
        assert WAIT_FOR_NEXT_INCREMENT == 0.000_001
        assert MAX_ITERATIONS == 100_000

        # Verify relationships
        assert US_PER_SECOND == MS_PER_SECOND * MS_PER_US
        assert NS_PER_SECOND == US_PER_SECOND * NS_PER_US

    def test_watcher_constants(self) -> None:
        """Test watcher-related constants."""
        assert MAX_TOTAL_RETRY_TIME == 300  # 5 minutes
        assert isinstance(MAX_TOTAL_RETRY_TIME, int)
        assert MAX_TOTAL_RETRY_TIME > 0

    def test_connection_phase_constants(self) -> None:
        """Test database connection phase constants."""
        assert hasattr(ConnectionPhase, "CONNECTION")
        assert hasattr(ConnectionPhase, "OPTIMIZATION")
        assert ConnectionPhase.CONNECTION == "connection"
        assert ConnectionPhase.OPTIMIZATION == "optimization"

    def test_project_scoping_constants(self) -> None:
        """Test project scoping constants."""
        assert MAX_PROJECT_TRAVERSAL_DEPTH == 100
        assert isinstance(MAX_PROJECT_TRAVERSAL_DEPTH, int)
        assert MAX_PROJECT_TRAVERSAL_DEPTH > 0


class TestLoadConfig:
    """Test the load_config function with various environment configurations."""

    def test_default_config(self) -> None:
        """Test load_config returns expected defaults when no env vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            config = load_config()

            # SQLite settings
            assert config["BROKER_BUSY_TIMEOUT"] == 5000
            assert config["BROKER_CACHE_MB"] == 10
            assert config["BROKER_SYNC_MODE"] == "FULL"
            assert config["BROKER_WAL_AUTOCHECKPOINT"] == 1000

            # Message processing
            assert config["BROKER_MAX_MESSAGE_SIZE"] == MAX_MESSAGE_SIZE
            assert config["BROKER_READ_COMMIT_INTERVAL"] == 1

            # Vacuum settings
            assert config["BROKER_AUTO_VACUUM"] == 1
            assert config["BROKER_AUTO_VACUUM_INTERVAL"] == 100
            assert config["BROKER_VACUUM_THRESHOLD"] == 0.1  # 10%
            assert config["BROKER_VACUUM_BATCH_SIZE"] == 1000
            assert config["BROKER_VACUUM_LOCK_TIMEOUT"] == 300

            # Watcher settings
            assert config["BROKER_SKIP_IDLE_CHECK"] is False
            assert config["BROKER_JITTER_FACTOR"] == 0.15
            assert config["SIMPLEBROKER_INITIAL_CHECKS"] == 100
            assert config["SIMPLEBROKER_MAX_INTERVAL"] == 0.1
            assert config["SIMPLEBROKER_BURST_SLEEP"] == 0.00001

            # Debug
            assert config["BROKER_DEBUG"] is False

            # Logging
            assert config["BROKER_LOGGING_ENABLED"] is False

            # Project scoping (new)
            assert config["BROKER_DEFAULT_DB_LOCATION"] == ""
            assert config["BROKER_DEFAULT_DB_NAME"] == DEFAULT_DB_NAME
            assert config["BROKER_PROJECT_SCOPE"] is False

    def test_custom_sqlite_settings(self) -> None:
        """Test SQLite-related environment variables."""
        env_vars = {
            "BROKER_BUSY_TIMEOUT": "10000",
            "BROKER_CACHE_MB": "50",
            "BROKER_SYNC_MODE": "NORMAL",
            "BROKER_WAL_AUTOCHECKPOINT": "2000",
        }

        with patch.dict(os.environ, env_vars):
            config = load_config()

            assert config["BROKER_BUSY_TIMEOUT"] == 10000
            assert config["BROKER_CACHE_MB"] == 50
            assert config["BROKER_SYNC_MODE"] == "NORMAL"
            assert config["BROKER_WAL_AUTOCHECKPOINT"] == 2000

    def test_sync_mode_validation(self) -> None:
        """Test BROKER_SYNC_MODE validation."""
        # Valid modes
        for mode in ["FULL", "NORMAL", "OFF"]:
            with patch.dict(os.environ, {"BROKER_SYNC_MODE": mode}):
                config = load_config()
                assert config["BROKER_SYNC_MODE"] == mode

        # Invalid mode should default to FULL
        with patch.dict(os.environ, {"BROKER_SYNC_MODE": "INVALID"}):
            config = load_config()
            assert config["BROKER_SYNC_MODE"] == "FULL"

        # Case sensitivity
        with patch.dict(os.environ, {"BROKER_SYNC_MODE": "full"}):
            config = load_config()
            assert config["BROKER_SYNC_MODE"] == "FULL"

    def test_message_settings(self) -> None:
        """Test message-related environment variables."""
        env_vars = {
            "BROKER_MAX_MESSAGE_SIZE": "5242880",  # 5MB
            "BROKER_READ_COMMIT_INTERVAL": "100",
        }

        with patch.dict(os.environ, env_vars):
            config = load_config()

            assert config["BROKER_MAX_MESSAGE_SIZE"] == 5242880
            assert config["BROKER_READ_COMMIT_INTERVAL"] == 100

    def test_vacuum_settings(self) -> None:
        """Test vacuum-related environment variables."""
        env_vars = {
            "BROKER_AUTO_VACUUM": "0",
            "BROKER_AUTO_VACUUM_INTERVAL": "50",
            "BROKER_VACUUM_THRESHOLD": "20",  # 20%
            "BROKER_VACUUM_BATCH_SIZE": "500",
            "BROKER_VACUUM_LOCK_TIMEOUT": "600",
        }

        with patch.dict(os.environ, env_vars):
            config = load_config()

            assert config["BROKER_AUTO_VACUUM"] == 0
            assert config["BROKER_AUTO_VACUUM_INTERVAL"] == 50
            assert config["BROKER_VACUUM_THRESHOLD"] == 0.2  # Converted to decimal
            assert config["BROKER_VACUUM_BATCH_SIZE"] == 500
            assert config["BROKER_VACUUM_LOCK_TIMEOUT"] == 600

    def test_watcher_settings(self) -> None:
        """Test watcher-related environment variables."""
        env_vars = {
            "BROKER_SKIP_IDLE_CHECK": "1",
            "BROKER_JITTER_FACTOR": "0.25",
            "SIMPLEBROKER_INITIAL_CHECKS": "200",
            "SIMPLEBROKER_MAX_INTERVAL": "0.5",
            "SIMPLEBROKER_BURST_SLEEP": "0.0001",
        }

        with patch.dict(os.environ, env_vars):
            config = load_config()

            assert config["BROKER_SKIP_IDLE_CHECK"] is True
            assert config["BROKER_JITTER_FACTOR"] == 0.25
            assert config["SIMPLEBROKER_INITIAL_CHECKS"] == 200
            assert config["SIMPLEBROKER_MAX_INTERVAL"] == 0.5
            assert config["SIMPLEBROKER_BURST_SLEEP"] == 0.0001

    def test_debug_setting(self) -> None:
        """Test debug environment variable."""
        # Any non-empty value should enable debug
        for value in ["1", "true", "yes", "debug"]:
            with patch.dict(os.environ, {"BROKER_DEBUG": value}):
                config = load_config()
                assert config["BROKER_DEBUG"] is True

        # Empty or missing should be False
        with patch.dict(os.environ, {"BROKER_DEBUG": ""}):
            config = load_config()
            assert config["BROKER_DEBUG"] is False

        with patch.dict(os.environ, {}, clear=True):
            config = load_config()
            assert config["BROKER_DEBUG"] is False

    def test_logging_setting(self) -> None:
        """Test logging environment variable."""
        # Only "1" should enable logging
        with patch.dict(os.environ, {"BROKER_LOGGING_ENABLED": "1"}):
            config = load_config()
            assert config["BROKER_LOGGING_ENABLED"] is True

        # Any other value should be False
        for value in ["0", "true", "yes", "enabled", ""]:
            with patch.dict(os.environ, {"BROKER_LOGGING_ENABLED": value}):
                config = load_config()
                assert config["BROKER_LOGGING_ENABLED"] is False

        # Missing should be False
        with patch.dict(os.environ, {}, clear=True):
            config = load_config()
            assert config["BROKER_LOGGING_ENABLED"] is False

    def test_boolean_conversion(self) -> None:
        """Test boolean environment variable conversion."""
        # BROKER_SKIP_IDLE_CHECK should only be True for "1"
        with patch.dict(os.environ, {"BROKER_SKIP_IDLE_CHECK": "1"}):
            config = load_config()
            assert config["BROKER_SKIP_IDLE_CHECK"] is True

        for value in ["0", "false", "no", ""]:
            with patch.dict(os.environ, {"BROKER_SKIP_IDLE_CHECK": value}):
                config = load_config()
                assert config["BROKER_SKIP_IDLE_CHECK"] is False

    def test_invalid_numeric_values(self) -> None:
        """Test handling of invalid numeric environment values."""
        # Invalid integers should raise ValueError
        with patch.dict(os.environ, {"BROKER_BUSY_TIMEOUT": "not_a_number"}):
            with pytest.raises(ValueError):
                load_config()

        # Invalid floats should raise ValueError
        with patch.dict(os.environ, {"BROKER_JITTER_FACTOR": "invalid"}):
            with pytest.raises(ValueError):
                load_config()

    def test_all_config_keys_present(self) -> None:
        """Test that all expected configuration keys are present."""
        config = load_config()

        expected_keys = {
            # SQLite settings
            "BROKER_BUSY_TIMEOUT",
            "BROKER_CACHE_MB",
            "BROKER_SYNC_MODE",
            "BROKER_WAL_AUTOCHECKPOINT",
            # Message processing
            "BROKER_MAX_MESSAGE_SIZE",
            "BROKER_READ_COMMIT_INTERVAL",
            "BROKER_GENERATOR_BATCH_SIZE",
            # Vacuum settings
            "BROKER_AUTO_VACUUM",
            "BROKER_AUTO_VACUUM_INTERVAL",
            "BROKER_VACUUM_THRESHOLD",
            "BROKER_VACUUM_BATCH_SIZE",
            "BROKER_VACUUM_LOCK_TIMEOUT",
            # Watcher settings
            "BROKER_SKIP_IDLE_CHECK",
            "BROKER_JITTER_FACTOR",
            "SIMPLEBROKER_INITIAL_CHECKS",
            "SIMPLEBROKER_MAX_INTERVAL",
            "SIMPLEBROKER_BURST_SLEEP",
            # Debug
            "BROKER_DEBUG",
            # Logging
            "BROKER_LOGGING_ENABLED",
            # Project scoping
            "BROKER_DEFAULT_DB_LOCATION",
            "BROKER_DEFAULT_DB_NAME",
            "BROKER_PROJECT_SCOPE",
        }

        assert set(config.keys()) == expected_keys

    def test_config_immutability(self) -> None:
        """Test that modifying returned config doesn't affect subsequent calls."""
        config1 = load_config()
        original_timeout = config1["BROKER_BUSY_TIMEOUT"]

        # Modify the returned config
        config1["BROKER_BUSY_TIMEOUT"] = 99999

        # Get a new config
        config2 = load_config()

        # Should have original value, not modified one
        assert config2["BROKER_BUSY_TIMEOUT"] == original_timeout
        assert config2["BROKER_BUSY_TIMEOUT"] != 99999

    def test_project_scoping_settings(self) -> None:
        """Test project scoping environment variables."""
        env_vars = {
            "BROKER_DEFAULT_DB_LOCATION": "/tmp/project",
            "BROKER_DEFAULT_DB_NAME": "custom.db",
            "BROKER_PROJECT_SCOPE": "1",
        }

        with patch.dict(os.environ, env_vars):
            config = load_config()

            assert config["BROKER_DEFAULT_DB_LOCATION"] == "/tmp/project"
            assert config["BROKER_DEFAULT_DB_NAME"] == "custom.db"
            assert config["BROKER_PROJECT_SCOPE"] is True

    def test_project_scope_boolean_parsing(self) -> None:
        """Test BROKER_PROJECT_SCOPE boolean parsing."""
        # Test true values
        for value in ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"]:
            with patch.dict(os.environ, {"BROKER_PROJECT_SCOPE": value}):
                config = load_config()
                assert config["BROKER_PROJECT_SCOPE"] is True, (
                    f"Failed for value: {value}"
                )

        # Test false values
        for value in ["0", "false", "FALSE", "no", "NO", "off", "OFF", "", "invalid"]:
            with patch.dict(os.environ, {"BROKER_PROJECT_SCOPE": value}):
                config = load_config()
                assert config["BROKER_PROJECT_SCOPE"] is False, (
                    f"Failed for value: {value}"
                )

    def test_relative_db_location_resolution(self) -> None:
        """Test that relative BROKER_DEFAULT_DB_LOCATION is converted to absolute."""
        from pathlib import Path

        with patch.dict(os.environ, {"BROKER_DEFAULT_DB_LOCATION": "relative/path"}):
            config = load_config()

            # Should be converted to absolute path
            assert Path(config["BROKER_DEFAULT_DB_LOCATION"]).is_absolute()
            assert config["BROKER_DEFAULT_DB_LOCATION"].endswith("relative/path")

        # Absolute paths should remain unchanged
        with patch.dict(os.environ, {"BROKER_DEFAULT_DB_LOCATION": "/absolute/path"}):
            config = load_config()
            assert config["BROKER_DEFAULT_DB_LOCATION"] == "/absolute/path"


class TestParseBool:
    """Test the _parse_bool helper function."""

    def test_true_values(self) -> None:
        """Test _parse_bool recognizes true values correctly."""
        true_values = ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"]
        for value in true_values:
            assert _parse_bool(value) is True, f"Failed for value: {value}"

    def test_false_values(self) -> None:
        """Test _parse_bool recognizes false values correctly."""
        false_values = ["0", "false", "FALSE", "no", "off", "OFF", "", "invalid"]
        for value in false_values:
            assert _parse_bool(value) is False, f"Failed for value: {value}"

    def test_whitespace_handling(self) -> None:
        """Test _parse_bool handles whitespace correctly."""
        assert _parse_bool(" 1 ") is True
        assert _parse_bool(" true ") is True
        assert _parse_bool("\ttrue\n") is True
        assert _parse_bool("  ") is False

    def test_empty_and_none_values(self) -> None:
        """Test _parse_bool handles empty and None-like values."""
        assert _parse_bool("") is False
        assert _parse_bool(" ") is False
