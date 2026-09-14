"""Tests for the _constants module."""

import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from simplebroker._constants import (
    # Database
    DEFAULT_DB_NAME,
    DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS,
    DEFAULT_PROJECT_CONFIG_NAME,
    EXIT_QUEUE_EMPTY,
    # Exit codes
    EXIT_SUCCESS,
    LOGICAL_COUNTER_BITS,
    LOGICAL_COUNTER_MASK,
    MAX_LOGICAL_COUNTER,
    # Message constraints
    MAX_MESSAGE_SIZE,
    # Project scoping constants
    MAX_QUEUE_NAME_LENGTH,
    # Watcher
    # Program
    SCHEMA_VERSION,
    SIMPLEBROKER_MAGIC,
    SQLITE_MAX_INT64,
    # Timestamp constants
    TIMESTAMP_EXACT_NUM_DIGITS,
    UNIX_NATIVE_BOUNDARY,
    # Version
    __version__,
    # Functions
    resolve_config,
)

from .helper_scripts import create_dangerous_path

pytestmark = [
    pytest.mark.shared,
    pytest.mark.filterwarnings("ignore:.*ignoring invalid"),
]


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

    def test_database_constants(self) -> None:
        """On-disk compatibility pins: changing any of these is a
        migration for existing databases and configs, not an edit —
        this test exists to make such a change loud and deliberate."""
        assert DEFAULT_DB_NAME == ".broker.db"
        assert DEFAULT_PROJECT_CONFIG_NAME == ".broker.toml"
        assert SIMPLEBROKER_MAGIC == "simplebroker-v1"
        assert SCHEMA_VERSION >= 1

    def test_exit_codes(self) -> None:
        """Public CLI contract pins ([SB-CLI-1]); doc sync is owned by
        test_documented_exit_codes."""
        assert EXIT_SUCCESS == 0
        assert EXIT_QUEUE_EMPTY == 2

    def test_message_constraints(self) -> None:
        """Documented limit pins; behavioral owners:
        test_message_size_contract (byte boundary) and
        test_queue_validation length-boundary test."""
        assert MAX_MESSAGE_SIZE == 10 * 1024 * 1024  # 10MB
        assert MAX_QUEUE_NAME_LENGTH == 512

    def test_timestamp_constants(self) -> None:
        """Bit-layout invariants of the 64-bit hybrid timestamp: these
        fail when one number changes without preserving the encoding
        relationships. 19 digits and the low counter bits are wire contract."""
        assert TIMESTAMP_EXACT_NUM_DIGITS == 19
        assert LOGICAL_COUNTER_BITS == 12
        assert LOGICAL_COUNTER_MASK == (1 << LOGICAL_COUNTER_BITS) - 1
        assert MAX_LOGICAL_COUNTER == 1 << LOGICAL_COUNTER_BITS
        assert UNIX_NATIVE_BOUNDARY == 2**44
        assert SQLITE_MAX_INT64 == 2**63


class TestLoadConfig:
    """Test explicit environment resolution with various environment configurations."""

    def test_default_config(self) -> None:
        """Test resolution returns expected defaults when no env vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_config(env=os.environ)

            # SQLite settings
            assert config["BUSY_TIMEOUT"] == 5000
            assert config["CACHE_MB"] == 10
            assert config["SYNC_MODE"] == "FULL"
            assert config["WAL_AUTOCHECKPOINT"] == 1000

            # Message processing
            assert config["MAX_MESSAGE_SIZE"] == MAX_MESSAGE_SIZE
            assert config["READ_COMMIT_INTERVAL"] == 1
            assert (
                config["LOAD_MAX_FUTURE_SKEW_SECONDS"]
                == DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS
            )

            # Vacuum settings
            assert config["AUTO_VACUUM"] == 1
            assert config["AUTO_VACUUM_INTERVAL"] == 100
            assert config["VACUUM_THRESHOLD"] == 10  # 10%
            assert config["VACUUM_BATCH_SIZE"] == 1000

            # Watcher settings
            assert config["SKIP_IDLE_CHECK"] is False
            assert config["JITTER_FACTOR"] == 0.15
            assert config["INITIAL_CHECKS"] == 100
            assert config["MAX_INTERVAL"] == 0.1
            assert config["BURST_SLEEP"] == 0.00001

            # Debug
            assert config["DEBUG"] is False

            # Logging
            assert config["LOGGING_ENABLED"] is False

            # Project scoping (new)
            assert config["DEFAULT_DB_LOCATION"] == ""
            assert config["DEFAULT_DB_NAME"] == DEFAULT_DB_NAME
            assert config["PROJECT_CONFIG_PATH"] == ""
            assert config["PROJECT_CONFIG_NAME"] == DEFAULT_PROJECT_CONFIG_NAME
            assert config["PROJECT_SCOPE"] is False

            # Backend selection
            assert config["BACKEND"] == "sqlite"
            assert config["BACKEND_HOST"] == "localhost"
            assert config["BACKEND_PORT"] == 5432
            assert config["BACKEND_USER"] == "postgres"
            assert config["BACKEND_PASSWORD"] == ""
            assert config["BACKEND_DATABASE"] == "simplebroker"
            assert config["BACKEND_SCHEMA"] == "simplebroker_pg_v1"
            assert config["BACKEND_TARGET"] == ""

    def test_backend_selection_reads_from_env(self) -> None:
        """Test backend-selection environment variables."""
        env_vars = {
            "BROKER_BACKEND": "postgres",
            "BROKER_BACKEND_HOST": "db.example.com",
            "BROKER_BACKEND_PORT": "5433",
            "BROKER_BACKEND_USER": "broker",
            "BROKER_BACKEND_PASSWORD": "secret",
            "BROKER_BACKEND_DATABASE": "simplebroker_app",
            "BROKER_BACKEND_SCHEMA": "broker_schema",
            "BROKER_BACKEND_TARGET": "postgresql://broker@db.example.com/simplebroker",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = resolve_config(env=os.environ)

            assert config["BACKEND"] == "postgres"
            assert config["BACKEND_HOST"] == "db.example.com"
            assert config["BACKEND_PORT"] == 5433
            assert config["BACKEND_USER"] == "broker"
            assert config["BACKEND_PASSWORD"] == "secret"
            assert config["BACKEND_DATABASE"] == "simplebroker_app"
            assert config["BACKEND_SCHEMA"] == "broker_schema"
            assert (
                config["BACKEND_TARGET"]
                == "postgresql://broker@db.example.com/simplebroker"
            )

    def test_custom_sqlite_settings(self) -> None:
        """Test SQLite-related environment variables."""
        env_vars = {
            "BROKER_BUSY_TIMEOUT": "10000",
            "BROKER_CACHE_MB": "50",
            "BROKER_SYNC_MODE": "NORMAL",
            "BROKER_WAL_AUTOCHECKPOINT": "2000",
        }

        with patch.dict(os.environ, env_vars):
            config = resolve_config(env=os.environ)

            assert config["BUSY_TIMEOUT"] == 10000
            assert config["CACHE_MB"] == 50
            assert config["SYNC_MODE"] == "NORMAL"
            assert config["WAL_AUTOCHECKPOINT"] == 2000

    def test_sync_mode_validation(self) -> None:
        """Test BROKER_SYNC_MODE validation."""
        # Valid modes
        for mode in ["FULL", "NORMAL", "OFF"]:
            with patch.dict(os.environ, {"BROKER_SYNC_MODE": mode}):
                config = resolve_config(env=os.environ)
                assert config["SYNC_MODE"] == mode

        # Invalid mode should default to FULL
        with patch.dict(os.environ, {"BROKER_SYNC_MODE": "INVALID"}):
            config = resolve_config(env=os.environ)
            assert config["SYNC_MODE"] == "FULL"

        # Case sensitivity
        with patch.dict(os.environ, {"BROKER_SYNC_MODE": "full"}):
            config = resolve_config(env=os.environ)
            assert config["SYNC_MODE"] == "FULL"

    def test_message_settings(self) -> None:
        """Test message-related environment variables."""
        env_vars = {
            "BROKER_MAX_MESSAGE_SIZE": "5242880",  # 5MB
            "BROKER_READ_COMMIT_INTERVAL": "100",
        }

        with patch.dict(os.environ, env_vars):
            config = resolve_config(env=os.environ)

            assert config["MAX_MESSAGE_SIZE"] == 5242880
            assert config["READ_COMMIT_INTERVAL"] == 100

    def test_vacuum_settings(self) -> None:
        """Test vacuum-related environment variables."""
        env_vars = {
            "BROKER_AUTO_VACUUM": "0",
            "BROKER_AUTO_VACUUM_INTERVAL": "50",
            "BROKER_VACUUM_THRESHOLD": "20",  # 20%
            "BROKER_VACUUM_BATCH_SIZE": "500",
        }

        with patch.dict(os.environ, env_vars):
            config = resolve_config(env=os.environ)

            assert config["AUTO_VACUUM"] == 0
            assert config["AUTO_VACUUM_INTERVAL"] == 50
            assert config["VACUUM_THRESHOLD"] == 20  # Declared percent
            assert config["VACUUM_BATCH_SIZE"] == 500

    @pytest.mark.parametrize(
        ("raw_value", "expected"),
        [
            ("0.5", 0.5),
            (0.5, 0.5),
            ("50", 50),
            (50, 50),
        ],
    )
    def test_vacuum_threshold_preserves_declared_percentage(
        self,
        raw_value: str | float,
        expected: float,
    ) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert (
                resolve_config(
                    env=os.environ, override={"BROKER_VACUUM_THRESHOLD": raw_value}
                )["VACUUM_THRESHOLD"]
                == expected
            )

    def test_configuration_guide_explains_vacuum_threshold_semantics(self) -> None:
        guide = " ".join(
            (Path(__file__).parent.parent / "docs" / "guides" / "configuration.md")
            .read_text(encoding="utf-8")
            .split()
        )

        required_phrases = ("VACUUM_THRESHOLD", "percentage", "0", "100")
        for phrase in required_phrases:
            assert phrase in guide

    def test_watcher_settings(self) -> None:
        """Test watcher-related environment variables."""
        env_vars = {
            "BROKER_SKIP_IDLE_CHECK": "1",
            "BROKER_JITTER_FACTOR": "0.25",
            "BROKER_INITIAL_CHECKS": "200",
            "BROKER_MAX_INTERVAL": "0.5",
            "BROKER_BURST_SLEEP": "0.0001",
        }

        with patch.dict(os.environ, env_vars):
            config = resolve_config(env=os.environ)

            assert config["SKIP_IDLE_CHECK"] is True
            assert config["JITTER_FACTOR"] == 0.25
            assert config["INITIAL_CHECKS"] == 200
            assert config["MAX_INTERVAL"] == 0.5
            assert config["BURST_SLEEP"] == 0.0001

    def test_debug_setting(self) -> None:
        """Test debug environment variable."""
        # Any non-empty value should enable debug
        for value in ["1", "true", "yes", "debug"]:
            with patch.dict(os.environ, {"BROKER_DEBUG": value}):
                config = resolve_config(env=os.environ)
                assert config["DEBUG"] is True

        # Empty or missing should be False
        with patch.dict(os.environ, {"BROKER_DEBUG": ""}):
            config = resolve_config(env=os.environ)
            assert config["DEBUG"] is False

        with patch.dict(os.environ, {}, clear=True):
            config = resolve_config(env=os.environ)
            assert config["DEBUG"] is False

    def test_logging_setting(self) -> None:
        """Test logging environment variable."""
        # Only "1" should enable logging
        with patch.dict(os.environ, {"BROKER_LOGGING_ENABLED": "1"}):
            config = resolve_config(env=os.environ)
            assert config["LOGGING_ENABLED"] is True

        # Any other value should be False
        for value in ["0", "true", "yes", "enabled", ""]:
            with patch.dict(os.environ, {"BROKER_LOGGING_ENABLED": value}):
                config = resolve_config(env=os.environ)
                assert config["LOGGING_ENABLED"] is False

        # Missing should be False
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_config(env=os.environ)
            assert config["LOGGING_ENABLED"] is False

    def test_boolean_conversion(self) -> None:
        """Test boolean environment variable conversion."""
        # BROKER_SKIP_IDLE_CHECK should only be True for "1"
        with patch.dict(os.environ, {"BROKER_SKIP_IDLE_CHECK": "1"}):
            config = resolve_config(env=os.environ)
            assert config["SKIP_IDLE_CHECK"] is True

        for value in ["0", "false", "no", ""]:
            with patch.dict(os.environ, {"BROKER_SKIP_IDLE_CHECK": value}):
                config = resolve_config(env=os.environ)
                assert config["SKIP_IDLE_CHECK"] is False

    def test_invalid_numeric_values(self) -> None:
        """Test handling of invalid numeric environment values."""
        # Invalid integers should raise ValueError
        with (
            patch.dict(os.environ, {"BROKER_BUSY_TIMEOUT": "not_a_number"}),
            pytest.raises(ValueError),
        ):
            resolve_config(env=os.environ)

    @pytest.mark.parametrize(
        ("key", "raw_value", "expected"),
        [
            ("BROKER_BUSY_TIMEOUT", "6000", 6000),
            ("BROKER_CACHE_MB", "20", 20),
            ("BROKER_SYNC_MODE", "normal", "NORMAL"),
            ("BROKER_WAL_AUTOCHECKPOINT", "2000", 2000),
            ("BROKER_MAX_MESSAGE_SIZE", "2048", 2048),
            ("BROKER_READ_COMMIT_INTERVAL", "2", 2),
            ("BROKER_GENERATOR_BATCH_SIZE", "50", 50),
            ("BROKER_AUTO_VACUUM", "0", 0),
            ("BROKER_AUTO_VACUUM_INTERVAL", "25", 25),
            ("BROKER_VACUUM_THRESHOLD", "25", 25),
            ("BROKER_VACUUM_BATCH_SIZE", "250", 250),
            ("BROKER_SKIP_IDLE_CHECK", "1", True),
            ("BROKER_JITTER_FACTOR", "0.2", 0.2),
            ("BROKER_INITIAL_CHECKS", "10", 10),
            ("BROKER_MAX_INTERVAL", "0.5", 0.5),
            ("BROKER_BURST_SLEEP", "0.001", 0.001),
            ("BROKER_DEBUG", "debug", True),
            ("BROKER_LOGGING_ENABLED", "1", True),
            (
                "BROKER_DEFAULT_DB_LOCATION",
                os.path.abspath("config-location"),
                os.path.abspath("config-location"),
            ),
            ("BROKER_DEFAULT_DB_NAME", "data/broker.db", "data/broker.db"),
            ("BROKER_PROJECT_CONFIG_PATH", ".weft", ".weft"),
            ("BROKER_PROJECT_CONFIG_NAME", "broker.toml", "broker.toml"),
            ("BROKER_PROJECT_SCOPE", "yes", True),
            ("BROKER_BACKEND", "postgres", "postgres"),
            ("BROKER_BACKEND_HOST", "db.example", "db.example"),
            ("BROKER_BACKEND_PORT", "5433", 5433),
            ("BROKER_BACKEND_USER", "broker", "broker"),
            ("BROKER_BACKEND_PASSWORD", "secret", "secret"),
            ("BROKER_BACKEND_DATABASE", "app", "app"),
            ("BROKER_BACKEND_SCHEMA", "broker_v1", "broker_v1"),
            ("BROKER_BACKEND_TARGET", "postgresql://db/app", "postgresql://db/app"),
        ],
    )
    def test_environment_and_override_use_the_same_field_schema(
        self,
        key: str,
        raw_value: str,
        expected: object,
    ) -> None:
        """Every declared key must use one coercion rule on both input paths."""
        with patch.dict(os.environ, {key: raw_value}, clear=True):
            environment_value = resolve_config(env=os.environ)[
                key.removeprefix("BROKER_")
            ]
        with patch.dict(os.environ, {}, clear=True):
            override_value = resolve_config(
                env=os.environ,
                override={
                    "BROKER_" + key: value
                    for key, value in ({key.removeprefix("BROKER_"): raw_value}).items()
                },
            )[key.removeprefix("BROKER_")]

        assert environment_value == expected
        assert override_value == expected

    @pytest.mark.parametrize(
        "key",
        [
            "BROKER_BUSY_TIMEOUT",
            "BROKER_CACHE_MB",
            "BROKER_WAL_AUTOCHECKPOINT",
            "BROKER_MAX_MESSAGE_SIZE",
            "BROKER_READ_COMMIT_INTERVAL",
            "BROKER_GENERATOR_BATCH_SIZE",
            "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS",
            "BROKER_AUTO_VACUUM",
            "BROKER_AUTO_VACUUM_INTERVAL",
            "BROKER_VACUUM_THRESHOLD",
            "BROKER_VACUUM_BATCH_SIZE",
            "BROKER_JITTER_FACTOR",
            "BROKER_INITIAL_CHECKS",
            "BROKER_MAX_INTERVAL",
            "BROKER_BURST_SLEEP",
            "BROKER_BACKEND_PORT",
        ],
    )
    def test_numeric_fields_reject_invalid_values_on_both_paths(
        self,
        key: str,
    ) -> None:
        """Invalid numeric text must fail consistently for env and overrides."""
        with (
            patch.dict(os.environ, {key: "invalid"}, clear=True),
            pytest.raises(ValueError),
        ):
            resolve_config(env=os.environ)
        with patch.dict(os.environ, {}, clear=True), pytest.raises(ValueError):
            resolve_config(
                env=os.environ,
                override={
                    "BROKER_" + key: value
                    for key, value in ({key.removeprefix("BROKER_"): "invalid"}).items()
                },
            )

        # Invalid floats should raise ValueError
        with (
            patch.dict(os.environ, {"BROKER_JITTER_FACTOR": "invalid"}),
            pytest.raises(ValueError),
        ):
            resolve_config(env=os.environ)

    def test_all_config_keys_present(self) -> None:
        """Test that all expected configuration keys are present."""
        config = resolve_config(env=os.environ)

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
            "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS",
            # Vacuum settings
            "BROKER_AUTO_VACUUM",
            "BROKER_AUTO_VACUUM_INTERVAL",
            "BROKER_VACUUM_THRESHOLD",
            "BROKER_VACUUM_BATCH_SIZE",
            # Watcher settings
            "BROKER_SKIP_IDLE_CHECK",
            "BROKER_JITTER_FACTOR",
            "BROKER_INITIAL_CHECKS",
            "BROKER_MAX_INTERVAL",
            "BROKER_BURST_SLEEP",
            # Debug
            "BROKER_DEBUG",
            # Logging
            "BROKER_LOGGING_ENABLED",
            # Project scoping
            "BROKER_DEFAULT_DB_LOCATION",
            "BROKER_DEFAULT_DB_NAME",
            "BROKER_PROJECT_CONFIG_PATH",
            "BROKER_PROJECT_CONFIG_NAME",
            "BROKER_PROJECT_SCOPE",
            # Backend selection
            "BROKER_BACKEND",
            "BROKER_BACKEND_HOST",
            "BROKER_BACKEND_PORT",
            "BROKER_BACKEND_USER",
            "BROKER_BACKEND_PASSWORD",
            "BROKER_BACKEND_DATABASE",
            "BROKER_BACKEND_SCHEMA",
            "BROKER_BACKEND_TARGET",
        }

        assert {key.removeprefix("BROKER_") for key in expected_keys} <= set(config)
        if "BROKER_TEST_BACKEND" in os.environ:
            assert config["TEST_BACKEND"] == os.environ["BROKER_TEST_BACKEND"]

    def test_config_immutability(self) -> None:
        """Test that modifying returned config doesn't affect subsequent calls."""
        config1 = resolve_config(env=os.environ)
        original_timeout = config1["BUSY_TIMEOUT"]

        # Modify the returned config
        with pytest.raises(TypeError):
            config1["BUSY_TIMEOUT"] = 99999  # type: ignore[index]

        # Get a new config
        config2 = resolve_config(env=os.environ)

        # Should have original value, not modified one
        assert config2["BUSY_TIMEOUT"] == original_timeout
        assert config2["BUSY_TIMEOUT"] != 99999

    def test_project_scoping_settings(self) -> None:
        """Test project scoping environment variables."""
        import os
        import tempfile
        from pathlib import Path

        # Create a platform-appropriate absolute path
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = str(Path(temp_dir) / "project")

            env_vars = {
                "BROKER_DEFAULT_DB_LOCATION": test_path,
                "BROKER_DEFAULT_DB_NAME": "custom.db",
                "BROKER_PROJECT_CONFIG_PATH": ".weft",
                "BROKER_PROJECT_CONFIG_NAME": "broker.toml",
                "BROKER_PROJECT_SCOPE": "1",
            }

            with patch.dict(os.environ, env_vars):
                config = resolve_config(env=os.environ)

                assert config["DEFAULT_DB_LOCATION"] == test_path
                assert config["DEFAULT_DB_NAME"] == "custom.db"
                assert config["PROJECT_CONFIG_PATH"] == ".weft"
                assert config["PROJECT_CONFIG_NAME"] == "broker.toml"
                assert config["PROJECT_SCOPE"] is True

    def test_project_scope_boolean_parsing(self) -> None:
        """Test BROKER_PROJECT_SCOPE boolean parsing."""
        # Test true values
        for value in ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"]:
            with patch.dict(os.environ, {"BROKER_PROJECT_SCOPE": value}):
                config = resolve_config(env=os.environ)
                assert config["PROJECT_SCOPE"] is True, f"Failed for value: {value}"

        # Test false values
        for value in ["0", "false", "FALSE", "no", "NO", "off", "OFF", "", "invalid"]:
            with patch.dict(os.environ, {"BROKER_PROJECT_SCOPE": value}):
                config = resolve_config(env=os.environ)
                assert config["PROJECT_SCOPE"] is False, f"Failed for value: {value}"

    def test_relative_db_location_is_invalid(self) -> None:
        """A relative BROKER_DEFAULT_DB_LOCATION is an invalid value."""
        import warnings

        with (
            patch.dict(os.environ, {"BROKER_DEFAULT_DB_LOCATION": "testdir"}),
            pytest.raises(ValueError, match="BROKER_DEFAULT_DB_LOCATION"),
        ):
            resolve_config(env=os.environ)

        # Absolute paths should remain unchanged
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp_dir:
            absolute_path = str(Path(temp_dir) / "absolute" / "path")
            with (
                patch.dict(os.environ, {"BROKER_DEFAULT_DB_LOCATION": absolute_path}),
                warnings.catch_warnings(record=True) as w,
            ):
                warnings.simplefilter("always")
                config = resolve_config(env=os.environ)

                # Should not issue a warning for absolute paths
                assert len(w) == 0
                assert config["DEFAULT_DB_LOCATION"] == absolute_path


class TestParseBool:
    """Test the project_scope helper function."""

    def test_true_values(self) -> None:
        """Test project_scope recognizes true values correctly."""
        true_values = ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"]
        for value in true_values:
            assert (
                resolve_config(override={"BROKER_PROJECT_SCOPE": value})[
                    "PROJECT_SCOPE"
                ]
                is True
            ), f"Failed for value: {value}"

    def test_false_values(self) -> None:
        """Test project_scope recognizes false values correctly."""
        false_values = ["0", "false", "FALSE", "no", "off", "OFF", "", "invalid"]
        for value in false_values:
            assert (
                resolve_config(override={"BROKER_PROJECT_SCOPE": value})[
                    "PROJECT_SCOPE"
                ]
                is False
            ), f"Failed for value: {value}"

    def test_whitespace_handling(self) -> None:
        """Test project_scope handles whitespace correctly."""
        assert (
            resolve_config(override={"BROKER_PROJECT_SCOPE": " 1 "})["PROJECT_SCOPE"]
            is True
        )
        assert (
            resolve_config(override={"BROKER_PROJECT_SCOPE": " true "})["PROJECT_SCOPE"]
            is True
        )
        assert (
            resolve_config(override={"BROKER_PROJECT_SCOPE": "\ttrue\n"})[
                "PROJECT_SCOPE"
            ]
            is True
        )
        assert (
            resolve_config(override={"BROKER_PROJECT_SCOPE": "  "})["PROJECT_SCOPE"]
            is False
        )

    def test_empty_and_none_values(self) -> None:
        """Test project_scope handles empty and None-like values."""
        assert (
            resolve_config(override={"BROKER_PROJECT_SCOPE": ""})["PROJECT_SCOPE"]
            is False
        )
        assert (
            resolve_config(override={"BROKER_PROJECT_SCOPE": " "})["PROJECT_SCOPE"]
            is False
        )


class TestConfigValidation:
    """Test configuration field validation."""

    def test_load_future_skew_reads_non_negative_env_value(self) -> None:
        with patch.dict(
            os.environ,
            {"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": "42"},
            clear=True,
        ):
            assert resolve_config(env=os.environ)["LOAD_MAX_FUTURE_SKEW_SECONDS"] == 42

    def test_load_future_skew_rejects_negative_value(self) -> None:
        with (
            patch.dict(
                os.environ,
                {"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": "-1"},
                clear=True,
            ),
            pytest.raises(
                ValueError,
                match="expected a non-negative integer number of seconds",
            ),
        ):
            resolve_config(env=os.environ)
        with pytest.raises(
            ValueError,
            match="expected a non-negative integer number of seconds",
        ):
            resolve_config(
                env=os.environ, override={"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": -1}
            )

    @pytest.mark.parametrize("value", [True, 1.0, 1.9])
    def test_load_future_skew_rejects_non_integer_type(self, value: object) -> None:
        with pytest.raises(
            ValueError,
            match="expected a non-negative integer number of seconds",
        ):
            resolve_config(
                env=os.environ, override={"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": value}
            )

    def test_load_future_skew_rejects_non_integer_string(self) -> None:
        with pytest.raises(
            ValueError,
            match="expected a non-negative integer number of seconds",
        ):
            resolve_config(
                env=os.environ, override={"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": "1.9"}
            )

    def test_broker_default_db_name_absolute_path_raises_error(self) -> None:
        """Test that absolute paths in BROKER_DEFAULT_DB_NAME raise an error."""
        import tempfile
        from pathlib import Path

        # Create a platform-appropriate absolute path
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = str(Path(temp_dir) / "broker.db")

            with (
                patch.dict(os.environ, {"BROKER_DEFAULT_DB_NAME": test_path}),
                pytest.raises(
                    ValueError,
                    match="expected a relative database path with at most one directory",
                ),
            ):
                resolve_config(env=os.environ)

    def test_broker_default_db_name_windows_absolute_path_raises_error(self) -> None:
        """Test that Windows absolute paths in BROKER_DEFAULT_DB_NAME raise an error."""
        import platform

        # Only test Windows absolute paths on Windows, or use a cross-platform absolute path
        if platform.system() == "Windows":
            test_path = "C:\\temp\\broker.db"
        else:
            # On Unix systems, test with a UNC path that would be absolute on Windows
            # but skip this test after os.path.isabs behavior is platform-specific
            pytest.skip("Windows absolute path test only relevant on Windows")

        with (
            patch.dict(os.environ, {"BROKER_DEFAULT_DB_NAME": test_path}),
            pytest.raises(
                ValueError,
                match="expected a relative database path with at most one directory",
            ),
        ):
            resolve_config(env=os.environ)

    def test_broker_default_db_name_nested_directories_raises_error(self) -> None:
        """Test that nested directories in BROKER_DEFAULT_DB_NAME raise an error."""
        with (
            patch.dict(os.environ, {"BROKER_DEFAULT_DB_NAME": ".config/app/broker.db"}),
            pytest.raises(
                ValueError,
                match="expected a relative database path with at most one directory",
            ),
        ):
            resolve_config(env=os.environ)

    def test_broker_default_db_name_valid_compound_path(self) -> None:
        """Test that valid compound paths are accepted."""
        with patch.dict(os.environ, {"BROKER_DEFAULT_DB_NAME": ".config/broker.db"}):
            config = resolve_config(env=os.environ)
            assert config["DEFAULT_DB_NAME"] == ".config/broker.db"

    def test_broker_default_db_name_simple_path(self) -> None:
        """Test that simple database names work correctly."""
        with patch.dict(os.environ, {"BROKER_DEFAULT_DB_NAME": "simple.db"}):
            config = resolve_config(env=os.environ)
            assert config["DEFAULT_DB_NAME"] == "simple.db"

    def test_broker_default_db_location_dangerous_characters_raises_error(self) -> None:
        """Test that dangerous characters in BROKER_DEFAULT_DB_LOCATION raise an error."""
        import tempfile

        # Create a platform-appropriate absolute path with dangerous characters
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = create_dangerous_path(temp_dir, "*")

            with (
                patch.dict(os.environ, {"BROKER_DEFAULT_DB_LOCATION": test_path}),
                pytest.raises(
                    ValueError,
                    match="expected an absolute directory path or empty string",
                ),
            ):
                resolve_config(env=os.environ)

    def test_broker_default_db_location_valid_absolute_path(self) -> None:
        """Test that valid absolute paths in BROKER_DEFAULT_DB_LOCATION are accepted."""
        import tempfile
        from pathlib import Path

        # Create a platform-appropriate absolute path
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = str(Path(temp_dir) / "valid_path")

            with patch.dict(os.environ, {"BROKER_DEFAULT_DB_LOCATION": test_path}):
                config = resolve_config(env=os.environ)
                assert config["DEFAULT_DB_LOCATION"] == test_path

    def test_broker_default_db_name_dangerous_characters_in_compound(self) -> None:
        """Test that dangerous characters are caught in compound database names at config load time."""
        # Since we now validate dangerous characters at config load time,
        # this should fail during resolve_config() itself
        with (
            patch.dict(os.environ, {"BROKER_DEFAULT_DB_NAME": "test*dir/broker.db"}),
            pytest.raises(
                ValueError,
                match="expected a relative database path with at most one directory",
            ),
        ):
            resolve_config(env=os.environ)

    def test_broker_project_config_name_valid_compound_path(self) -> None:
        """Test that valid project config compound paths are accepted."""
        with patch.dict(
            os.environ, {"BROKER_PROJECT_CONFIG_NAME": ".weft/broker.toml"}
        ):
            config = resolve_config(env=os.environ)
            assert config["PROJECT_CONFIG_NAME"] == ".weft/broker.toml"

    def test_broker_project_config_name_absolute_path_raises_error(self) -> None:
        """Test that absolute paths in BROKER_PROJECT_CONFIG_NAME raise an error."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = str(Path(temp_dir) / "broker.toml")

            with (
                patch.dict(os.environ, {"BROKER_PROJECT_CONFIG_NAME": test_path}),
                pytest.raises(
                    ValueError,
                    match="expected a relative config path with at most one directory",
                ),
            ):
                resolve_config(env=os.environ)

    def test_broker_project_config_path_accepts_relative_directory(self) -> None:
        """Test that project config path can namespace discovery under a project."""
        with patch.dict(os.environ, {"BROKER_PROJECT_CONFIG_PATH": ".weft"}):
            config = resolve_config(env=os.environ)
            assert config["PROJECT_CONFIG_PATH"] == ".weft"

    def test_broker_project_config_path_nested_relative_path_raises_error(
        self,
    ) -> None:
        """Test that relative config path prefixes are limited to one directory."""
        with (
            patch.dict(os.environ, {"BROKER_PROJECT_CONFIG_PATH": ".weft/config"}),
            pytest.raises(
                ValueError,
                match="expected an absolute directory or one relative directory",
            ),
        ):
            resolve_config(env=os.environ)

    def test_broker_project_config_combined_nested_path_raises_error(self) -> None:
        """Test that path and name cannot combine into nested directories."""
        with (
            patch.dict(
                os.environ,
                {
                    "BROKER_PROJECT_CONFIG_PATH": ".weft",
                    "BROKER_PROJECT_CONFIG_NAME": "config/broker.toml",
                },
            ),
            pytest.raises(
                ValueError,
                match="combine into nested directories",
            ),
        ):
            resolve_config(env=os.environ)


def test_every_bare_constant_declaration_carries_an_explanation() -> None:
    """No-magic-constants policy, explanation half (audit plan Task 6.6).

    Every module-level UPPER_CASE constant assignment in _constants.py
    must carry meaning or units: a comment directly above it (or above
    its contiguous constant block), an inline trailing comment, or the
    file's house-style docstring on the following line.
    DEFAULT_CONFIG entries are separately gated by their non-empty
    ``expected`` form in test_invalid_config_lifecycle.
    """
    import re
    from pathlib import Path

    source = (
        Path(__file__).parent.parent / "simplebroker" / "_constants.py"
    ).read_text(encoding="utf-8")
    lines = source.splitlines()
    assignment = re.compile(r"^(_?[A-Z][A-Z0-9_]+)(?::[^=]+)?\s*=")
    missing: list[str] = []
    for index, line in enumerate(lines):
        match = assignment.match(line)
        if not match:
            continue
        previous = lines[index - 1].strip() if index else ""
        if previous.startswith("#"):
            continue
        # Contiguous constant blocks share the comment above the block.
        if assignment.match(lines[index - 1]) if index else False:
            continue
        # An inline trailing comment also counts as an explanation.
        if "#" in line:
            continue
        # The file's house style: a docstring on the following line.
        following = lines[index + 1].strip() if index + 1 < len(lines) else ""
        if following.startswith(('"""', "'''")):
            continue
        missing.append(f"{index + 1}: {match.group(1)}")

    assert not missing, (
        "constants without an adjacent explanation (add a comment naming "
        f"meaning or units): {missing}"
    )


@pytest.mark.parametrize(
    "name",
    [
        "100%.db",
        "my dir/broker.db",
        "café.db",
        "bad!.db",
        "",
        ".",
        "..",
        "a/./b.db",
        "a//b.db",
        "a/b/c.db",
        "a\\b\\c.db",
    ],
)
@pytest.mark.parametrize("source", ["env", "override", "toml"])
def test_database_name_grammar_rejects_invalid_config_sources(name, source, tmp_path):
    import json

    values = {"BROKER_DEFAULT_DB_NAME": name}
    kwargs: dict[str, Any] = {"env": {}}
    if source == "toml":
        path = tmp_path / "config.toml"
        path.write_text(f"BROKER_DEFAULT_DB_NAME = {json.dumps(name)}\n")
        kwargs["toml"] = path
    else:
        kwargs[source] = values
    with pytest.raises(ValueError, match="ASCII"):
        resolve_config(**kwargs)


@pytest.mark.parametrize(
    "name",
    ["AZaz09._-.db", ".config/broker.db", "dir-name/broker_09.db", "dir\\broker.db"],
)
def test_database_name_grammar_accepts_ascii_components(name):
    assert (
        resolve_config(env={}, override={"BROKER_DEFAULT_DB_NAME": name})[
            "DEFAULT_DB_NAME"
        ]
        == name
    )


@pytest.mark.parametrize(
    "name",
    ["bad?.db", "bad\x00.db", "bad\n.db", "bad\x7f.db", "bad\u0085.db", "x" * 256],
)
def test_database_name_grammar_preserves_control_and_length_rejection(name):
    with pytest.raises(ValueError, match="ASCII"):
        resolve_config(env={}, override={"BROKER_DEFAULT_DB_NAME": name})


@pytest.mark.parametrize("name", ["CON.db", "nul", "COM1.db", "LPT9.db"])
def test_database_name_grammar_preserves_windows_reserved_names(monkeypatch, name):
    monkeypatch.setattr("simplebroker._constants.platform.system", lambda: "Windows")
    with pytest.raises(ValueError, match="ASCII"):
        resolve_config(env={}, override={"BROKER_DEFAULT_DB_NAME": name})
