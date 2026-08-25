"""
Test suite for --after flag implementation.

Tests filtering messages by timestamp for read and peek commands.
"""

import datetime
import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from simplebroker import Queue, commands, target_for_directory
from simplebroker._constants import LOGICAL_COUNTER_MASK, load_config

from .conftest import _reset_pg_tables, run_cli
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

# Test data for explicit unit suffixes
UNIT_SUFFIX_TESTS = [
    # (input, description, should_work)
    ("1705329000s", "Unix seconds with suffix", True),
    ("1705329000.5s", "Fractional Unix seconds with suffix", False),
    ("1705329000000ms", "Unix milliseconds with suffix", True),
    ("1705329000000000000ns", "Unix nanoseconds with suffix", True),
    ("1837025672140161024", "Native hybrid", True),
    ("1837025672140161024hyb", "Native hybrid with hyb suffix", False),
    ("1.5hyb", "Float hybrid timestamp", False),
    ("1.532", "Fractional seconds", False),
    ("1e10s", "Scientific notation", False),
]


def _write_messages_direct(queue_name: str, messages) -> None:
    """Seed a queue without one CLI subprocess per message."""
    with Queue(queue_name, persistent=True) as queue:
        for message in messages:
            queue.write(message)


INVALID_TIMESTAMPS = [
    ("", "Invalid timestamp: empty string"),  # Empty string validation
    ("abc", "Invalid timestamp: abc"),
    ("-1", "timestamp bounds require integral seconds"),
    ("1e10", "Invalid timestamp: scientific notation not supported"),
    ("0x123", "Invalid timestamp: 0x123"),
    ("123abc", "Invalid timestamp: 123abc"),
    (str(2**64), "Invalid timestamp: exceeds maximum value"),
]


# ============================================================================
# Basic Functionality Tests
# ============================================================================


def test_after_empty_queue_after_postgres_schema_reset(
    workdir, pg_worker_runner, pg_worker_plugin
):
    """Postgres reset must recreate a schema dropped by a prior cleanup."""
    if pg_worker_runner is None or pg_worker_plugin is None:
        pytest.skip("Postgres-only schema reset regression")

    rc, _, err = run_cli("write", "schema_reset_queue", "message", cwd=workdir)
    assert rc == 0, err

    rc, _, err = run_cli("--cleanup", cwd=workdir)
    assert rc == 0, err

    _reset_pg_tables(pg_worker_runner, pg_worker_plugin)

    rc, out, err = run_cli("read", "empty_queue", "--after", "0", cwd=workdir)
    assert rc == 2, err
    assert out == ""


# ============================================================================
# Flag Combination Tests
# ============================================================================


def test_after_with_commit_interval(workdir):
    """Test batch processing respects --after filter."""
    # Write many messages
    for i in range(20):
        run_cli("write", "batch_queue", f"msg{i:02d}", cwd=workdir)

    # Get timestamp of 10th message
    rc, out, _ = run_cli("peek", "batch_queue", "--all", "--timestamps", cwd=workdir)
    lines = out.strip().split("\n")
    ts10 = int(lines[9].split("\t")[0])

    rc, out, err = run_cli(
        "read",
        "batch_queue",
        "--all",
        "--after",
        str(ts10),
        cwd=workdir,
        env={"BROKER_READ_COMMIT_INTERVAL": "5"},
    )
    assert rc == 0, err
    messages = out.strip().split("\n")
    assert len(messages) == 10  # msg10 through msg19
    assert messages[0] == "msg10"
    assert messages[-1] == "msg19"


def test_read_all_commit_interval_keeps_uncommitted_batch_on_output_failure(
    workdir, monkeypatch
):
    """CLI read --all should honor BROKER_READ_COMMIT_INTERVAL batching."""
    target = target_for_directory(workdir)
    queue = Queue("batch_queue", db_path=target)
    for i in range(20):
        queue.write(f"msg{i:02d}")

    emitted: list[str] = []

    def fail_during_second_batch(
        message: str,
        timestamp: int,
        json_output: bool,
        show_timestamps: bool,
        warned_newlines: bool,
    ) -> bool:
        del timestamp, json_output, show_timestamps
        emitted.append(message)
        if len(emitted) == 6:
            raise RuntimeError("output stopped")
        return warned_newlines

    config = load_config()
    config["BROKER_READ_COMMIT_INTERVAL"] = 5
    monkeypatch.setattr(commands, "_output_message", fail_during_second_batch)

    with pytest.raises(RuntimeError, match="output stopped"):
        commands.cmd_read(
            target,
            "batch_queue",
            all_messages=True,
            config=config,
        )

    remaining = Queue("batch_queue", db_path=target).peek_many(
        limit=100, with_timestamps=False
    )
    assert emitted == [f"msg{i:02d}" for i in range(6)]
    assert remaining == [f"msg{i:02d}" for i in range(5, 20)]


def test_after_with_peek(workdir):
    """Test peek command with --after (non-destructive)."""
    # Write messages
    for i in range(3):
        run_cli("write", "peek_queue", f"msg{i}", cwd=workdir)
        time.sleep(0.001)

    # Get first message timestamp
    rc, out, _ = run_cli("peek", "peek_queue", "--timestamps", cwd=workdir)
    ts1 = int(out.split("\t")[0])

    # Peek with --after multiple times
    for _ in range(3):
        rc, out, _ = run_cli(
            "peek", "peek_queue", "--all", "--after", str(ts1), cwd=workdir
        )
        assert rc == 0
        assert out == "msg1\nmsg2"

    # Verify all messages still exist
    rc, out, _ = run_cli("read", "peek_queue", "--all", cwd=workdir)
    assert rc == 0
    assert out == "msg0\nmsg1\nmsg2"


# ============================================================================
# Input Validation Tests
# ============================================================================


def test_after_iso_date_precise_boundary(workdir):
    """Test that date-only strings are interpreted as midnight UTC precisely."""
    queue_name = "iso_boundary_queue"
    test_date = "2024-01-15"
    days_since_epoch = (datetime.date(2024, 1, 15) - datetime.date(1970, 1, 1)).days
    midnight_ns = days_since_epoch * 86_400 * 1_000_000_000
    midnight = midnight_ns & ~LOGICAL_COUNTER_MASK
    target = target_for_directory(workdir)
    with Queue(queue_name, db_path=target) as queue:
        queue.insert_messages(
            [
                ("before", midnight - 1),
                ("at", midnight),
                ("after", midnight + 1),
            ]
        )

    rc, out, err = run_cli(
        "peek", queue_name, "--all", "--after", test_date, cwd=workdir
    )
    assert rc == 0, err
    assert out.splitlines() == ["after"]

    # Due to precision loss, might include messages from the same second as msg5


@pytest.mark.parametrize("ts_str,expected_error", INVALID_TIMESTAMPS)
def test_after_invalid_timestamps(workdir, ts_str, expected_error):
    """Test error handling for invalid timestamps."""
    rc, _out, err = run_cli("read", "invalid_queue", "--after", ts_str, cwd=workdir)
    assert rc == 1
    assert expected_error in err


def test_after_missing_value(workdir):
    """Test --after without value shows proper error."""
    # This should be caught by argparse
    rc, _out, err = run_cli("read", "test_queue", "--after", cwd=workdir)
    assert rc == 1
    assert "error" in err.lower()
    assert "argument --after: expected one argument" in err


# ============================================================================
# Concurrent Operations Tests
# ============================================================================


def test_after_during_concurrent_writes(workdir):
    """Test --after consistency during active writes."""
    queue_name = "concurrent_queue"

    # Write initial messages
    for i in range(5):
        run_cli("write", queue_name, f"initial_{i}", cwd=workdir)

    # Get timestamp after initial messages
    _rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    lines = out.strip().split("\n")
    checkpoint_ts = int(lines[-1].split("\t")[0])

    # Start concurrent writer
    def writer():
        for i in range(10):
            run_cli("write", queue_name, f"concurrent_{i}", cwd=workdir)
            time.sleep(0.001)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(writer)

        # --after must see concurrent messages while the writer is live.
        assert wait_for_condition(
            lambda: (
                run_cli(
                    "peek",
                    queue_name,
                    "--all",
                    "--after",
                    str(checkpoint_ts),
                    cwd=workdir,
                )[0]
                == 0
            ),
            timeout=scale_timeout_for_ci(10.0),
        ), "No concurrent messages found after waiting"

        # Two concurrent filtered readers drain competitively; their
        # union must conserve every concurrent message exactly once
        # (folded from the deleted test_after_multiple_readers, whose
        # dead expected-zero branch contradicted the rc==2 contract).
        collected: list[str] = []

        def drain_reader() -> list[str]:
            drained: list[str] = []
            empty_reads = 0
            while empty_reads < 3:
                rc, out, _ = run_cli(
                    "read",
                    queue_name,
                    "--all",
                    "--after",
                    str(checkpoint_ts),
                    cwd=workdir,
                )
                if rc == 0:
                    drained.extend(m for m in out.splitlines() if m)
                    empty_reads = 0
                else:
                    assert rc == 2
                    empty_reads += 1
            return drained

        future.result()  # Writer finished: all 10 messages durable.
        with ThreadPoolExecutor(max_workers=2) as readers:
            reader_futures = [readers.submit(drain_reader) for _ in range(2)]
            for reader_future in reader_futures:
                collected.extend(reader_future.result())

        assert sorted(collected) == sorted(f"concurrent_{i}" for i in range(10))
        for msg in collected:
            assert msg.startswith("concurrent_")


def test_after_checkpoint_pattern(workdir):
    """Test checkpoint-based consumption pattern."""
    queue_name = "checkpoint_queue"

    # Write messages in batches
    for batch in range(3):
        _write_messages_direct(
            queue_name,
            [f"batch{batch}_msg{i}" for i in range(5)],
        )
        time.sleep(0.002)  # Ensure timestamp difference between batches

    # Read first batch with timestamps
    rc, out, _ = run_cli("read", queue_name, "--timestamps", cwd=workdir)
    assert rc == 0
    last_ts = int(out.split("\t")[0])

    # Read subsequent messages in batches using checkpoint
    all_messages = ["batch0_msg0"]  # First message already read

    while True:
        rc, out, _ = run_cli(
            "read",
            queue_name,
            "--all",
            "--after",
            str(last_ts),
            "--timestamps",
            cwd=workdir,
        )
        if rc == 2:  # Queue is now empty (all messages consumed)
            break
        assert rc == 0  # Should succeed when messages exist

        lines = out.strip().split("\n")
        for line in lines:
            ts, msg = line.split("\t")
            all_messages.append(msg)
            last_ts = int(ts)

    # Verify we got all messages without duplicates
    assert len(all_messages) == 15
    assert len(set(all_messages)) == 15  # No duplicates
    for i, msg in enumerate(all_messages):
        expected = f"batch{i // 5}_msg{i % 5}"
        assert msg == expected


# ============================================================================
# Edge Cases and Boundary Tests
# ============================================================================


def test_after_timestamp_heuristic(workdir):
    """Documented timestamp forms select the same observable queue rows."""
    queue_name = "heuristic_queue"
    rc, out, err = run_cli("write", queue_name, "message", "--json", cwd=workdir)
    assert rc == 0, err
    native_id = json.loads(out)["timestamp"]

    documented_instant = (
        "1705329000",
        "1705329000s",
        "1705329000000ms",
        "1705329000000000000ns",
    )
    for timestamp in documented_instant:
        rc, out, err = run_cli("peek", queue_name, "--after", timestamp, cwd=workdir)
        assert rc == 0, (timestamp, err)
        assert out == "message"

    rc, out, err = run_cli("peek", queue_name, "--after", native_id, cwd=workdir)
    assert rc == 2, err
    assert out == ""


def test_after_hybrid_timestamp_ordering(workdir):
    """Test that hybrid timestamps maintain correct ordering."""
    queue_name = "hybrid_queue"

    # Write many messages rapidly to trigger same-millisecond timestamps
    messages = [f"msg{i:03d}" for i in range(100)]
    _write_messages_direct(queue_name, messages)

    # Get all timestamps
    rc, out, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    assert rc == 0

    lines = out.strip().split("\n")
    timestamps = []
    for line in lines:
        ts, _msg = line.split("\t")
        timestamps.append(int(ts))

    # Verify timestamps are strictly increasing
    for i in range(1, len(timestamps)):
        assert timestamps[i] > timestamps[i - 1]

    # Test --after with timestamps in the middle of same-millisecond group
    mid_point = len(timestamps) // 2
    mid_ts = timestamps[mid_point]

    rc, out, _ = run_cli(
        "peek", queue_name, "--all", "--after", str(mid_ts), cwd=workdir
    )
    assert rc == 0

    result_messages = out.strip().split("\n")
    expected_messages = messages[mid_point + 1 :]
    assert result_messages == expected_messages


def test_after_negative_timestamps(workdir):
    """Test handling of negative timestamps."""
    queue_name = "negative_queue"

    # Write a message
    run_cli("write", queue_name, "test", cwd=workdir)

    # Test various negative formats by passing as single string to avoid argparse issues
    negative_tests = [
        ("-1", "raw negative"),
        ("-1.5", "negative float"),
    ]

    for ts_str, desc in negative_tests:
        rc, _out, err = run_cli("peek", queue_name, f"--after={ts_str}", cwd=workdir)
        assert rc == 1, f"Expected error for {desc}"
        assert "Invalid timestamp" in err, f"Wrong error message for {desc}: {err}"


def test_after_scientific_notation_rejected(workdir):
    """Test that scientific notation is consistently rejected."""
    queue_name = "sci_queue"

    # Write a message
    run_cli("write", queue_name, "test", cwd=workdir)

    # Test various scientific notation formats
    sci_tests = ["1e10", "1E10", "1.5e9", "1e-5", "1.23E+10"]

    for ts_str in sci_tests:
        rc, _out, err = run_cli("peek", queue_name, "--after", ts_str, cwd=workdir)
        assert rc == 1, f"Expected error for {ts_str}"
        assert "Invalid timestamp" in err, f"Wrong error for {ts_str}: {err}"
        assert "scientific notation not supported" in err, (
            f"Wrong reason for {ts_str}: {err}"
        )


def test_read_after_plain_word_with_e_reports_invalid_timestamp(workdir):
    """Plain garbage containing e should not be mislabeled as scientific notation."""
    queue_name = "plain_word_queue"
    run_cli("write", queue_name, "test", cwd=workdir)

    rc, out, err = run_cli("read", queue_name, "--after", "tuesday", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "Invalid timestamp: tuesday" in err
    assert "scientific notation" not in err


def test_after_uses_persisted_cross_process_timestamp_order(workdir):
    """Test persisted timestamp ordering and --after across CLI processes."""
    queue_name = "cross_process_order_queue"

    run_cli("write", queue_name, "msg1", cwd=workdir)
    rc, out1, _ = run_cli("peek", queue_name, "--timestamps", cwd=workdir)
    ts1 = int(out1.split("\t")[0])

    run_cli("write", queue_name, "msg2", cwd=workdir)
    rc, out2, _ = run_cli("peek", queue_name, "--all", "--timestamps", cwd=workdir)
    lines = out2.strip().split("\n")
    ts2 = int(lines[1].split("\t")[0])

    assert ts2 > ts1, "Persisted timestamps should remain monotonic"

    rc, out, _ = run_cli("peek", queue_name, "--all", "--after", str(ts1), cwd=workdir)
    assert rc == 0
    assert out == "msg2"

    rc, out, _ = run_cli("read", queue_name, "--all", cwd=workdir)
    assert rc == 0
    assert out == "msg1\nmsg2"


def test_after_error_messages_are_helpful(workdir):
    """Test that error messages provide helpful information for common mistakes."""
    queue_name = "error_test_queue"
    run_cli("write", queue_name, "test", cwd=workdir)

    # Test 1: Scientific notation
    rc, _out, err = run_cli("peek", queue_name, "--after", "1.5e9", cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp: scientific notation not supported" in err

    # Test 2: Negative values
    rc, _out, err = run_cli("peek", queue_name, "--after=-1", cwd=workdir)
    assert rc == 1
    assert "timestamp bounds require integral seconds" in err

    # Test 3: Invalid ISO format
    rc, _out, err = run_cli("peek", queue_name, "--after", "2024-13-45", cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp" in err

    # Test 4: Overflow
    huge_value = str(2**64)
    rc, _out, err = run_cli("peek", queue_name, "--after", huge_value, cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp: exceeds maximum value" in err

    # Test 5: Empty string via equals syntax
    rc, _out, err = run_cli("peek", queue_name, "--after=", cwd=workdir)
    assert rc == 1
    assert "Invalid timestamp: empty string" in err


# ============================================================================
# Integration Test Patterns
# ============================================================================


# ---------------------------------------------------------------------------
# Parametrized owners rebuilt on the deterministic insert_messages pattern
# (audit plan Task 7.1; test_after_iso_date_precise_boundary is the model,
# test_before_flag.py the target shape). These absorb the deleted
# boundary/emptiness, flag-combination, and format families.
# ---------------------------------------------------------------------------

_ANCHOR_DATE = datetime.date(2024, 1, 15)
_ANCHOR_DAYS = (_ANCHOR_DATE - datetime.date(1970, 1, 1)).days
_ANCHOR_SECONDS = _ANCHOR_DAYS * 86_400
_ANCHOR_NS = _ANCHOR_SECONDS * 1_000_000_000
_ANCHOR_ID = _ANCHOR_NS & ~LOGICAL_COUNTER_MASK


def _seed_around_anchor(workdir, queue_name):
    """Three messages with exact IDs strictly before/at/after the anchor."""
    target = target_for_directory(workdir)
    with Queue(queue_name, db_path=target) as queue:
        queue.insert_messages(
            [
                ("before", _ANCHOR_ID - 4096),
                ("at", _ANCHOR_ID),
                ("after", _ANCHOR_ID + 4096),
            ]
        )


@pytest.mark.parametrize("command", ["read", "peek"])
@pytest.mark.parametrize(
    ("after_value", "expected", "expected_rc", "remaining_after_read"),
    [
        ("0", ["before", "at", "after"], 0, []),
        (
            str(_ANCHOR_ID - 4096),
            ["at", "after"],
            0,
            ["before"],
        ),  # strictly greater
        (str(_ANCHOR_ID), ["after"], 0, ["before", "at"]),
        (str(_ANCHOR_ID + 4096), [], 2, ["before", "at", "after"]),
        (str(2**63 - 1), [], 2, ["before", "at", "after"]),
    ],
)
def test_after_boundary_is_strictly_greater(
    workdir,
    command,
    after_value,
    expected,
    expected_rc,
    remaining_after_read,
):
    """Strict-> selection and destructive-read preservation at exact IDs."""
    queue_name = f"boundary_{command}_{after_value}"
    _seed_around_anchor(workdir, queue_name)

    rc, out, err = run_cli(
        command, queue_name, "--all", "--after", after_value, cwd=workdir
    )
    assert rc == expected_rc, err
    assert [line for line in out.splitlines() if line] == expected

    target = target_for_directory(workdir)
    with Queue(queue_name, db_path=target) as queue:
        remaining = queue.peek_many(10, with_timestamps=False)
    assert remaining == (
        ["before", "at", "after"] if command == "peek" else remaining_after_read
    )


@pytest.mark.parametrize("command", ["read", "peek"])
def test_after_empty_and_missing_queues_exit_two(workdir, command):
    rc, out, _ = run_cli(command, "missing_queue", "--after", "0", cwd=workdir)
    assert rc == 2
    assert out == ""


@pytest.mark.parametrize(
    "mode", ["all", "json", "timestamps", "json_timestamps", "single"]
)
def test_after_flag_combinations(workdir, mode):
    """--after composes with --all/--json/--timestamps and single mode."""
    queue_name = f"combo_{mode}"
    _seed_around_anchor(workdir, queue_name)
    args = ["read", queue_name, "--after", str(_ANCHOR_ID - 4096)]
    if mode != "single":
        args.append("--all")
    if "json" in mode:
        args.append("--json")
    if "timestamps" in mode:
        args.append("--timestamps")

    rc, out, err = run_cli(*args, cwd=workdir)
    assert rc == 0, err
    lines = [line for line in out.splitlines() if line]

    if mode == "single":
        assert lines == ["at"]
        return
    if mode == "all":
        assert lines == ["at", "after"]
    elif mode == "timestamps":
        rows = [line.split("\t") for line in lines]
        assert [body for _ts, body in rows] == ["at", "after"]
        assert [int(ts) for ts, _body in rows] == [_ANCHOR_ID, _ANCHOR_ID + 4096]
    else:
        payloads = [json.loads(line) for line in lines]
        assert [payload["message"] for payload in payloads] == ["at", "after"]
        assert [payload["timestamp"] for payload in payloads] == [
            str(_ANCHOR_ID),
            str(_ANCHOR_ID + 4096),
        ]


@pytest.mark.parametrize(
    "after_form",
    [
        "2024-01-15",  # ISO date (midnight UTC)
        "2024-01-15T00:00:00",  # naive ISO datetime, assumed UTC
        "2024-01-15T00:00:00Z",  # explicit UTC
        str(_ANCHOR_SECONDS),  # bare unix seconds
        f"{_ANCHOR_SECONDS}s",  # explicit seconds suffix
        f"{_ANCHOR_SECONDS * 1000}ms",  # milliseconds
        f"{_ANCHOR_NS}ns",  # nanoseconds
        str(_ANCHOR_ID),  # native hybrid ID
        f"  {_ANCHOR_ID}  ",  # deleted native-ID whitespace contract
        f"  {_ANCHOR_SECONDS}s  ",  # explicit whitespace stripping, not int() leniency
    ],
)
def test_after_documented_formats_select_the_same_boundary(workdir, after_form):
    """Every documented --after form resolves to the same anchor instant.

    Exact seeded IDs, no wall-clock coupling (the deleted format family
    span-waited on real second boundaries — the file's flake source).
    """
    queue_name = f"format_{abs(hash(after_form)) % 10_000}"
    _seed_around_anchor(workdir, queue_name)

    rc, out, err = run_cli(
        "peek", queue_name, "--all", "--after", after_form, cwd=workdir
    )
    assert rc == 0, err
    assert [line for line in out.splitlines() if line] == ["after"]
