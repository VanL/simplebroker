"""Contract tests for timestamp-bound string grammar [SB-CLI-5]/[SB-API-11]."""

import json

import pytest

from simplebroker.ext import TimestampError, TimestampGenerator

from .conftest import run_cli


def _assert_finer_grain_guidance(message: str) -> None:
    assert "integral seconds" in message
    assert "integer ms" in message
    assert "integer ns" in message
    assert "native hybrid message ID" in message


@pytest.mark.parametrize(
    "timestamp",
    ["1705329000.5", "123456789012.5", "1234567890.123456789"],
)
def test_public_validator_rejects_bare_fraction_with_finer_grain_guidance(
    timestamp: str,
) -> None:
    with pytest.raises(TimestampError) as exc_info:
        TimestampGenerator.validate(timestamp)

    _assert_finer_grain_guidance(str(exc_info.value))


@pytest.mark.parametrize(
    "timestamp",
    [
        "1705329000.5s",
        "1.5ms",
        "1.5ns",
        "1_705_329_000s",
        "+1705329000s",
        "-1705329000s",
    ],
)
def test_public_validator_rejects_invalid_suffixed_numeric_with_guidance(
    timestamp: str,
) -> None:
    with pytest.raises(TimestampError) as exc_info:
        TimestampGenerator.validate(timestamp)

    _assert_finer_grain_guidance(str(exc_info.value))


@pytest.mark.parametrize(
    "timestamp",
    [
        "2024-01-15T14:30:00.5Z",
        "2024-01-15T14:30:00,5+00:00",
        "20240115T143000.5+0000",
        "2024-01-15T00:00:00+00:00:30.5",
        "2024-01-15T00:00:00-00:00:30,5",
    ],
)
def test_public_validator_rejects_iso_fraction_with_guidance(timestamp: str) -> None:
    with pytest.raises(TimestampError) as exc_info:
        TimestampGenerator.validate(timestamp)

    _assert_finer_grain_guidance(str(exc_info.value))


@pytest.mark.parametrize("timestamp", ["1_705_329_000", "+99999999999", "-99999999999"])
def test_public_validator_rejects_sign_and_underscore_pseudonumerics_with_guidance(
    timestamp: str,
) -> None:
    with pytest.raises(TimestampError) as exc_info:
        TimestampGenerator.validate(timestamp)

    _assert_finer_grain_guidance(str(exc_info.value))


@pytest.mark.parametrize("timestamp", ["1e3", "1e3s", "1E+3ms"])
def test_public_validator_rejects_scientific_notation_with_guidance(
    timestamp: str,
) -> None:
    with pytest.raises(TimestampError) as exc_info:
        TimestampGenerator.validate(timestamp)

    assert "scientific notation not supported" in str(exc_info.value)
    _assert_finer_grain_guidance(str(exc_info.value))


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("1705329000", 1_705_329_000_000_000_000),
        ("1705329000s", 1_705_329_000_000_000_000),
        ("1705329000500ms", 1_705_329_000_499_998_720),
        ("1705329000500000000ns", 1_705_329_000_499_998_720),
        ("2024-01-15T14:30:00+05:00", 1_705_311_000_000_000_000),
        ("١٧٠٥٣٢٩٠٠٠s", 1_705_329_000_000_000_000),
    ],
)
def test_public_validator_preserves_integral_timestamp_forms(
    timestamp: str, expected: int
) -> None:
    assert TimestampGenerator.validate(timestamp) == expected


def test_public_validator_preserves_exact_hybrid_message_ids() -> None:
    expected = 1_837_025_672_140_161_024
    assert TimestampGenerator.validate("1837025672140161024") == expected
    assert TimestampGenerator.validate("١٨٣٧٠٢٥٦٧٢١٤٠١٦١٠٢٤", exact=True) == expected


@pytest.mark.parametrize(
    ("flag", "timestamp"),
    [
        ("--after", "1705329000.5"),
        ("--before", "2024-01-15T14:30:00.5Z"),
        ("--after", "2024-01-15T00:00:00+00:00:30.5"),
    ],
)
def test_cli_bound_flags_reject_fractions_on_stderr(
    workdir, flag: str, timestamp: str
) -> None:
    rc, out, err = run_cli("peek", "bound_queue", flag, timestamp, cwd=workdir)

    assert rc == 1
    assert out == ""
    assert err.startswith("simplebroker: error: Invalid timestamp:")
    _assert_finer_grain_guidance(err)
    assert "\n" not in err
    assert "Traceback" not in err


def test_cli_json_scientific_notation_error_has_actionable_guidance(workdir) -> None:
    rc, out, err = run_cli(
        "peek", "bound_queue", "--json", "--after", "1e3", cwd=workdir
    )

    assert rc == 1
    assert out == ""
    payload = json.loads(err)
    assert payload["error"] == "INVALID_TIMESTAMP"
    assert payload["retryable"] is False
    assert "scientific notation not supported" in payload["message"]
    _assert_finer_grain_guidance(payload["message"])


@pytest.mark.parametrize("command", ["peek", "move", "watch"])
def test_cli_bound_help_teaches_integral_limit_and_alternatives(
    workdir, command: str
) -> None:
    rc, out, err = run_cli(command, "--help", cwd=workdir)

    assert rc == 0
    assert err == ""
    normalized_help = " ".join(out.split())
    assert "fractional seconds unsupported" in normalized_help
    assert "integer ms" in normalized_help
    assert "integer ns" in normalized_help
    assert "native hybrid ID" in normalized_help
