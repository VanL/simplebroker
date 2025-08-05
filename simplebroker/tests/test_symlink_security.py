"""Test symlink-based security vulnerabilities."""

import tempfile
from pathlib import Path

import pytest

from .conftest import run_cli


def test_symlink_path_traversal_attack(workdir: Path):
    """Test that symlink-based path traversal attacks are prevented."""
    # Skip test if we can't create symlinks (e.g., on Windows without admin)
    try:
        test_link = workdir / "test_link"
        test_link.symlink_to(workdir)
        test_link.unlink()
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    # Create a temporary sensitive file outside the working directory
    with tempfile.NamedTemporaryFile(mode="w", suffix=".db", delete=False) as f:
        sensitive_file = Path(f.name)
        f.write("sensitive data")

    try:
        # Create a symlink pointing to the sensitive file
        evil_link = workdir / "evil.db"
        evil_link.symlink_to(sensitive_file)

        # Try to access the sensitive file through the symlink
        code, stdout, stderr = run_cli(
            "write", "-f", "evil.db", "test_queue", "message", cwd=workdir
        )

        # Should fail because resolved path is outside working directory
        assert code == 1, f"Symlink attack should have failed, got code {code}"
        assert "must be within the working directory" in stderr.lower(), (
            f"Expected containment error, got: {stderr}"
        )

        # Verify the sensitive file wasn't modified
        assert sensitive_file.read_text() == "sensitive data"

    finally:
        # Clean up
        if evil_link.exists():
            evil_link.unlink()
        sensitive_file.unlink()


def test_symlink_to_parent_directory(workdir: Path):
    """Test symlinks that point to parent directories are blocked."""
    # Skip test if we can't create symlinks
    try:
        test_link = workdir / "test_link"
        test_link.symlink_to(workdir)
        test_link.unlink()
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    # Create a symlink to parent directory
    parent_link = workdir / "parent_link.db"
    parent_link.symlink_to("..")

    try:
        # Try to use the symlink
        code, stdout, stderr = run_cli(
            "write", "-f", "parent_link.db", "test_queue", "message", cwd=workdir
        )

        # Should fail
        assert code == 1
        assert "must be within the working directory" in stderr.lower()

    finally:
        parent_link.unlink()


def test_legitimate_symlink_within_directory(workdir: Path):
    """Test that legitimate symlinks within the directory still work."""
    # Skip test if we can't create symlinks
    try:
        test_link = workdir / "test_link"
        test_link.symlink_to(workdir)
        test_link.unlink()
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    # Create a subdirectory
    subdir = workdir / "data"
    subdir.mkdir()

    # Create a legitimate database in the subdirectory
    real_db = subdir / "real.db"

    # Create a symlink to it within the working directory
    link_db = workdir / "link.db"
    link_db.symlink_to(real_db)

    try:
        # This should work because both the symlink and target are within workdir
        code, stdout, stderr = run_cli(
            "write", "-f", "link.db", "test_queue", "message", cwd=workdir
        )

        assert code == 0, f"Legitimate symlink should work: {stderr}"

        # Verify we can read it back
        code, stdout, stderr = run_cli(
            "read", "-f", "link.db", "test_queue", cwd=workdir
        )

        assert code == 0
        assert stdout.strip() == "message"

    finally:
        link_db.unlink()


def test_symlink_chain_attack(workdir: Path):
    """Test that chains of symlinks don't bypass security."""
    # Skip test if we can't create symlinks
    try:
        test_link = workdir / "test_link"
        test_link.symlink_to(workdir)
        test_link.unlink()
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    # Create a temporary directory outside workdir
    with tempfile.TemporaryDirectory() as tmpdir:
        external_dir = Path(tmpdir)

        # Create a chain: link1 -> link2 -> external_file
        external_file = external_dir / "external.db"
        link2 = workdir / "link2.db"
        link1 = workdir / "link1.db"

        link2.symlink_to(external_file)
        link1.symlink_to(link2)

        try:
            # Try to use the symlink chain
            code, stdout, stderr = run_cli(
                "write", "-f", "link1.db", "test_queue", "message", cwd=workdir
            )

            # Should fail because ultimate target is outside workdir
            assert code == 1
            assert "must be within the working directory" in stderr.lower()

        finally:
            link1.unlink()
            link2.unlink()


def test_absolute_path_with_symlink(workdir: Path):
    """Test that absolute paths with symlinks are handled correctly."""
    # Skip test if we can't create symlinks
    try:
        test_link = workdir / "test_link"
        test_link.symlink_to(workdir)
        test_link.unlink()
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    # Create a symlink with absolute path
    link_db = workdir / "abs_link.db"
    real_db = workdir / "real_abs.db"
    link_db.symlink_to(real_db.absolute())

    try:
        # Using absolute path to the symlink
        abs_link_path = str(link_db.absolute())
        code, stdout, stderr = run_cli(
            "write", "-f", abs_link_path, "test_queue", "message", cwd=workdir
        )

        # Should work because target is valid
        assert code == 0, f"Absolute path with symlink should work: {stderr}"

        # Verify data is in the real file
        code, stdout, stderr = run_cli(
            "read", "-f", str(real_db.absolute()), "test_queue", cwd=workdir
        )
        assert code == 0
        assert stdout.strip() == "message"

    finally:
        link_db.unlink()
        if real_db.exists():
            real_db.unlink()
