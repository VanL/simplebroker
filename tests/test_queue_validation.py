"""Tests for queue name validation.

Layered per the test-suite audit plan (Task 3.6): parser-facing name
classes (leading ``-``, leading ``.``, empty operand, length boundary,
one delimiter representative, one invalid-character representative)
stay subprocess tests because they exercise argparse/preparse and CLI
dispatch, not just the validator. Exhaustive per-character and
per-spelling coverage runs in-process against the validator layer.
"""

from pathlib import Path

import pytest

from simplebroker.db import BrokerDB

from .conftest import run_cli

# ---------------------------------------------------------------------------
# Parser-facing subprocess classes (each exercises a distinct CLI layer)
# ---------------------------------------------------------------------------


def test_leading_hyphen_is_rejected_by_the_parser_layer(workdir):
    """A leading hyphen looks like an option and must fail at the parser."""
    rc, _, err = run_cli("write", "-myqueue", "message", cwd=workdir)
    assert rc == 1
    assert "error" in err.lower()


def test_leading_period_is_rejected_with_the_validator_diagnostic(workdir):
    """Leading-period names pass the parser and fail queue validation."""
    for name in (".", ".myqueue"):
        rc, _, err = run_cli("write", name, "message", cwd=workdir)
        assert rc == 1, f"expected rejection for {name!r}"
        assert "Invalid queue name" in err


def test_empty_queue_name_operand_is_rejected(workdir):
    rc, _, err = run_cli("write", "", "message", cwd=workdir)
    assert rc == 1
    assert "error" in err.lower()


def test_queue_name_length_boundary(workdir):
    """512 characters is accepted; 513 is rejected with the limit named."""
    rc, _, _ = run_cli("write", "a" * 512, "test", cwd=workdir)
    assert rc == 0

    rc, _, err = run_cli("write", "a" * 513, "test", cwd=workdir)
    assert rc == 1
    assert "Invalid queue name" in err
    assert "exceeds" in err


def test_allowed_delimiters_round_trip_through_the_cli(workdir):
    """One end-to-end representative for the accepted delimiter set."""
    rc, _, _ = run_cli("write", "my_queue-2.0", "test4", cwd=workdir)
    assert rc == 0
    rc, out, _ = run_cli("read", "my_queue-2.0", cwd=workdir)
    assert rc == 0
    assert out == "test4"


def test_invalid_character_is_rejected_through_the_cli(workdir):
    """One end-to-end representative for the rejected-character class."""
    rc, _, err = run_cli("write", "queue@name", "message", cwd=workdir)
    assert rc == 1
    assert "Invalid queue name" in err


def test_leading_period_rejected_across_read_peek_delete(workdir):
    """Each read-side command routes the name through its own dispatch."""
    for command in ("read", "peek", "delete"):
        rc, _, err = run_cli(command, ".invalid", cwd=workdir)
        assert rc == 1, f"{command} should reject '.invalid'"
        assert "Invalid queue name" in err


# ---------------------------------------------------------------------------
# Exhaustive validator-layer coverage (in-process)
# ---------------------------------------------------------------------------


@pytest.fixture
def validation_db(tmp_path: Path):
    with BrokerDB(str(tmp_path / "validation.db")) as db:
        yield db


@pytest.mark.parametrize("name", ["-", "--", "-myqueue"])
def test_hyphen_leading_names_rejected_by_validator(validation_db, name):
    with pytest.raises(ValueError, match="Invalid queue name"):
        validation_db.write(name, "message")


@pytest.mark.parametrize("name", ["-", "--", "-invalid"])
def test_read_side_operations_validate_names(validation_db, name):
    with pytest.raises(ValueError, match="Invalid queue name"):
        validation_db.peek_one(name)


def test_delete_validates_names(validation_db):
    with pytest.raises(ValueError, match="Invalid queue name"):
        validation_db.delete("-")


@pytest.mark.parametrize(
    "name",
    [
        "queue@name",
        "queue#name",
        "queue name",
        "queue/name",
        "queue\\name",
        "queue:name",
        "queue;name",
        "queue|name",
        "queue&name",
        "queue*name",
        "queue?name",
        "queue[name]",
        "queue{name}",
    ],
)
def test_special_characters_rejected_by_validator(validation_db, name):
    with pytest.raises(ValueError, match="Invalid queue name"):
        validation_db.write(name, "message")


@pytest.mark.parametrize("name", ["_myqueue", "my.queue", "my-queue"])
def test_valid_special_characters_accepted_by_validator(validation_db, name):
    validation_db.write(name, f"payload-{name}")
    assert validation_db.peek_one(name)[0] == f"payload-{name}"
