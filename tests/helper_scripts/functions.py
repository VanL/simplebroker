"""Cross-platform test utilities for SimpleBroker tests."""

from pathlib import Path


def create_dangerous_path(base_path: str, dangerous_char: str) -> str:
    """Create a path with dangerous characters for security testing.

    Args:
        base_path: Base path to build upon
        dangerous_char: Dangerous character to include (e.g., "|", "&")

    Returns:
        Path with dangerous character embedded
    """
    base = Path(base_path)
    return str(base / f"test{dangerous_char}dir")
