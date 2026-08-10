"""
Broadcast tests for SimpleBroker.

T4 – Broadcast fan-out to all existing queues
"""

from .conftest import run_cli


def test_broadcast(workdir):
    """T4: Broadcast sends message to all existing queues."""
    # Create two queues with initial messages
    rc, _, _ = run_cli("write", "alpha", "seed_a", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "beta", "seed_b", cwd=workdir)
    assert rc == 0

    # Broadcast a message to all queues
    rc, _, _ = run_cli("broadcast", "announcement", cwd=workdir)
    assert rc == 0

    # Check alpha queue has both messages
    rc, out, _ = run_cli("read", "alpha", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_a", "announcement"]

    # Check beta queue has both messages
    rc, out, _ = run_cli("read", "beta", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_b", "announcement"]


def test_broadcast_with_pattern(workdir):
    """Selective broadcast should only reach queues matching the glob."""
    rc, _, _ = run_cli("write", "alpha", "seed_a", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "beta", "seed_b", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("write", "alerts", "seed_alert", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("broadcast", "--pattern", "a*", "notice", cwd=workdir)
    assert rc == 0

    rc, out, _ = run_cli("read", "alpha", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_a", "notice"]

    rc, out, _ = run_cli("read", "alerts", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_alert", "notice"]

    rc, out, _ = run_cli("read", "beta", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_b"]


def test_broadcast_with_pattern_no_matches(workdir):
    """Selective broadcast with no matches should return EXIT_QUEUE_EMPTY."""
    rc, _, _ = run_cli("write", "alpha", "seed_a", cwd=workdir)
    assert rc == 0

    rc, _, _ = run_cli("broadcast", "--pattern", "z*", "notice", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY

    rc, out, _ = run_cli("read", "alpha", "--all", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["seed_a"]


def test_broadcast_to_repeated_exact_queues(workdir):
    """Repeated --queue flags target only named existing queues."""
    for queue in ("notify.alice", "notify.bob", "notify.carol"):
        assert run_cli("write", queue, "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli(
        "broadcast",
        "--queue",
        "notify.alice",
        "--queue",
        "notify.carol",
        "notice",
        cwd=workdir,
    )

    assert rc == 0, stderr
    assert run_cli("peek", "notify.alice", "--all", cwd=workdir)[1].splitlines() == [
        "seed",
        "notice",
    ]
    assert run_cli("peek", "notify.carol", "--all", cwd=workdir)[1].splitlines() == [
        "seed",
        "notice",
    ]
    assert run_cli("peek", "notify.bob", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_exact_queue_equals_form_and_missing_name(workdir):
    """--queue=value is literal and missing names do not create queues."""
    assert run_cli("write", "notify.alice", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli(
        "broadcast",
        "--queue=notify.alice",
        "--queue=notify.missing",
        "notice",
        cwd=workdir,
    )

    assert rc == 0, stderr
    assert run_cli("peek", "notify.alice", "--all", cwd=workdir)[1].splitlines() == [
        "seed",
        "notice",
    ]
    assert "notify.missing" not in run_cli("list", cwd=workdir)[1].splitlines()


def test_broadcast_exact_queue_does_not_split_commas(workdir):
    """A comma in --queue is part of one literal queue name."""
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0
    assert run_cli("write", "beta", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli(
        "broadcast",
        "--queue",
        "alpha,beta",
        "notice",
        cwd=workdir,
    )

    assert rc == 1
    assert "invalid queue name" in stderr.lower()
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1] == "seed"
    assert run_cli("peek", "beta", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_exact_queue_no_matches_returns_empty(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0

    rc, _, _ = run_cli(
        "broadcast",
        "--queue",
        "missing",
        "notice",
        cwd=workdir,
    )

    assert rc == 2
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_pattern_and_queue_are_mutually_exclusive(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli(
        "broadcast",
        "--pattern",
        "a*",
        "--queue",
        "alpha",
        "notice",
        cwd=workdir,
    )

    assert rc == 1
    assert "not allowed with argument" in stderr
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_empty_pattern_still_targets_all_queues(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0
    assert run_cli("write", "beta", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli(
        "broadcast",
        "--pattern",
        "",
        "notice",
        cwd=workdir,
    )

    assert rc == 0, stderr
    for queue in ("alpha", "beta"):
        assert run_cli("peek", queue, "--all", cwd=workdir)[1].splitlines() == [
            "seed",
            "notice",
        ]


def test_broadcast_empty_pattern_conflicts_with_queue(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli(
        "broadcast",
        "--pattern",
        "",
        "--queue",
        "alpha",
        "notice",
        cwd=workdir,
    )

    assert rc == 1
    assert "not allowed with argument" in stderr
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_queue_selector_reads_message_from_stdin(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0
    assert run_cli("write", "beta", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli(
        "broadcast",
        "--queue",
        "alpha",
        "-",
        cwd=workdir,
        stdin="notice",
    )

    assert rc == 0, stderr
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1].splitlines() == [
        "seed",
        "notice",
    ]
    assert run_cli("peek", "beta", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_queue_selector_requires_a_value(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0

    rc, stdout, stderr = run_cli("broadcast", "--queue", cwd=workdir)

    assert rc == 1
    assert stdout == ""
    assert "expected one argument" in stderr
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_help_documents_repeatable_queue_selector(workdir):
    rc, stdout, stderr = run_cli("broadcast", "--help", cwd=workdir)

    assert rc == 0, stderr
    assert "--queue QUEUE" in stdout
    assert "repeatable" in stdout


def test_broadcast_queue_prefix_is_rejected_before_mutation(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli("broadcast", "--qu", "alpha", "notice", cwd=workdir)

    assert rc == 1
    assert "unrecognized arguments: --qu" in stderr
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1] == "seed"


def test_broadcast_queue_prefix_can_be_literal_after_double_dash(workdir):
    assert run_cli("write", "alpha", "seed", cwd=workdir)[0] == 0

    rc, _, stderr = run_cli("broadcast", "--", "--qu", cwd=workdir)

    assert rc == 0, stderr
    assert run_cli("peek", "alpha", "--all", cwd=workdir)[1].splitlines() == [
        "seed",
        "--qu",
    ]


def test_broadcast_with_attached_short_pattern(workdir):
    run_cli("write", "queue-one", "original", cwd=workdir)
    run_cli("write", "other", "original", cwd=workdir)

    rc, _, stderr = run_cli("broadcast", "-pqueue*", "notice", cwd=workdir)

    assert rc == 0, stderr
    assert run_cli("peek", "queue-one", "--all", cwd=workdir)[1].splitlines() == [
        "original",
        "notice",
    ]
    assert run_cli("peek", "other", "--all", cwd=workdir)[1] == "original"


def test_broadcast_attached_short_pattern_requires_message(workdir):
    run_cli("write", "queue-one", "original", cwd=workdir)

    rc, stdout, stderr = run_cli("broadcast", "-pqueue*", cwd=workdir)

    assert rc == 1
    assert stdout == ""
    assert "required" in stderr
    assert run_cli("peek", "queue-one", "--all", cwd=workdir)[1] == "original"


def test_broadcast_no_queues(workdir):
    """Broadcast when no queues exist should return EXIT_QUEUE_EMPTY."""
    # Broadcast to no queues
    rc, _, _ = run_cli("broadcast", "hello", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY when no queues exist

    # Verify no queues were created
    rc, out, _ = run_cli("list", cwd=workdir)
    assert rc == 0
    assert out == ""  # No queues
