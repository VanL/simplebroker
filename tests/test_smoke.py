"""
Smoke tests for SimpleBroker.

T1 – Happy-path write → read → empty-code
T2 – Stdin path (echo foo | broker write q -)
T3 – --all returns FIFO order
"""

from .conftest import run_cli


def test_write_read_smoke(workdir):
    """T1: Basic write → read → empty queue."""
    # Write a message
    rc, _, _ = run_cli("write", "q1", "hello", cwd=workdir)
    assert rc == 0

    # Read it back
    rc, out, _ = run_cli("read", "q1", cwd=workdir)
    assert rc == 0
    assert out == "hello"

    # Read again - should be empty
    rc, _, _ = run_cli("read", "q1", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY


def test_write_from_stdin(workdir):
    """T2: Write from stdin using '-' argument."""
    # Write a simple message from stdin without embedded newlines
    rc, _, _ = run_cli("write", "q2", "-", cwd=workdir, stdin="hello from stdin")
    assert rc == 0

    # Read the message back
    rc, out, _ = run_cli("read", "q2", cwd=workdir)
    assert rc == 0
    assert out == "hello from stdin"
    
    # Test that stdin can handle special characters (but not newlines)
    rc, _, _ = run_cli("write", "q3", "-", cwd=workdir, stdin='special chars: !@#$%^&*()')
    assert rc == 0
    
    rc, out, _ = run_cli("read", "q3", cwd=workdir)
    assert rc == 0
    assert out == 'special chars: !@#$%^&*()'


def test_read_all_fifo(workdir):
    """T3: --all returns messages in FIFO order."""
    # Write multiple messages
    for val in ("1st", "2nd", "3rd"):
        rc, _, _ = run_cli("write", "fifo", val, cwd=workdir)
        assert rc == 0

    # Read all - should maintain order
    rc, out, _ = run_cli("read", "fifo", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["1st", "2nd", "3rd"]

    # Queue should now be empty
    rc, _, _ = run_cli("read", "fifo", cwd=workdir)
    assert rc == 2
