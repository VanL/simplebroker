"""Tests for compound database name functionality."""

import pytest

from simplebroker.helpers import _is_compound_db_name


class TestIsCompoundDbName:
    """Test the _is_compound_db_name function."""

    def test_simple_db_name(self) -> None:
        """Test that simple database names are not compound."""
        is_compound, parts = _is_compound_db_name("broker.db")
        assert is_compound is False
        assert parts == []

    def test_compound_db_name(self) -> None:
        """Test that compound database names are detected."""
        is_compound, parts = _is_compound_db_name("some/name.db")
        assert is_compound is True
        assert parts == ["some", "name.db"]

    def test_deep_compound_db_name_raises_error(self) -> None:
        """Test that deeply nested compound database names raise errors."""
        with pytest.raises(
            ValueError, match="Database name must not contain nested directories"
        ):
            _is_compound_db_name(".weft/project/broker.db")

    def test_compound_with_dots(self) -> None:
        """Test compound names with dot directories."""
        is_compound, parts = _is_compound_db_name(".hidden/db.sqlite")
        assert is_compound is True
        assert parts == [".hidden", "db.sqlite"]

    def test_cross_platform_separators(self) -> None:
        """Test that compound detection works with different separators."""
        # On Unix, backslashes are treated as literal characters, not separators
        # So "some\\name.db" is a single filename, not a compound path
        is_compound, parts = _is_compound_db_name("some\\name.db")
        assert is_compound is False  # Not compound on Unix
        assert parts == []

        # Forward slashes work as separators on all platforms
        is_compound, parts = _is_compound_db_name("some/name.db")
        assert is_compound is True
        assert parts == ["some", "name.db"]


class TestCompoundDbNameIntegration:
    """Integration tests for compound database names with CLI."""

    def test_deep_compound_structure_raises_error(self) -> None:
        """Test that deeply nested compound database structure raises errors."""
        # Test the validation function directly since module-level config
        # caching makes environment variable testing difficult
        with pytest.raises(
            ValueError, match="Database name must not contain nested directories"
        ):
            _is_compound_db_name(".config/app/queues/main.db")
