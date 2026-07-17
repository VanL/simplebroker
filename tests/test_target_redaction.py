import pytest

from simplebroker import BrokerTarget
from simplebroker._targets import redact_backend_target


def test_broker_target_is_the_only_target_type_name() -> None:
    import simplebroker._targets as targets

    assert BrokerTarget.__name__ == "BrokerTarget"
    assert targets.BrokerTarget is BrokerTarget


def test_redact_backend_target_removes_special_character_url_passwords() -> None:
    cases = [
        ("postgres://user:p@ss:word@host/db", "p@ss:word"),
        ("postgresql://user:secret/withslash@host/db", "secret/withslash"),
        ("postgresql://user:s#cret@host/db", "s#cret"),
        ("postgresql://user:s?cret@host/db", "s?cret"),
        ("postgresql://user:p%40ss%2Fword@host/db", "p%40ss%2Fword"),
    ]

    for target, password_fragment in cases:
        redacted = redact_backend_target(target)

        assert password_fragment not in redacted
        assert "***" in redacted


def test_redact_backend_target_redacts_uri_password() -> None:
    assert (
        redact_backend_target("postgresql://user:secret@db.example.com:5432/app")
        == "postgresql://user:***@db.example.com:5432/app"
    )


def test_redact_backend_target_redacts_uri_password_with_empty_username() -> None:
    assert (
        redact_backend_target("redis://:secret@127.0.0.1:6379/0")
        == "redis://:***@127.0.0.1:6379/0"
    )


def test_redact_backend_target_redacts_conninfo_password() -> None:
    assert (
        redact_backend_target("host=db.example.com user=app password='secret value'")
        == "host=db.example.com user=app password=***"
    )


@pytest.mark.parametrize(
    "target",
    [
        "\npostgresql://user:secret@db.example.com/app",
        "postgresql://user:secret@db.example.com/app\r",
        "postgresql://user:sec\tret@db.example.com/app",
        "\nhost=db.example.com password=secret",
        "host=db.example.com password=secret\r",
        "host=db.example.com password=secret\napplication_name=worker",
    ],
)
def test_redaction_neutralizes_all_ascii_control_characters(target: str) -> None:
    redacted = redact_backend_target(target)

    assert all(ord(character) >= 32 and ord(character) != 127 for character in redacted)
    assert "password=secret" not in redacted
    assert "user:secret" not in redacted


def test_resolved_target_display_target_leaves_sqlite_paths_unchanged() -> None:
    target = BrokerTarget("sqlite", "/tmp/password=literal.db")

    assert target.display_target == "/tmp/password=literal.db"


def test_resolved_target_repr_redacts_connection_and_backend_option_values() -> None:
    targets = [
        BrokerTarget(
            "postgres",
            "postgresql://user:uri-secret@db.example.com/app",
            backend_options={"schema": "ordinary-secret", "password": "option-secret"},
        ),
        BrokerTarget(
            "postgres",
            "host=db.example.com user=app password='conninfo secret'",
            backend_options={"password": "option-secret", "schema": "ordinary-secret"},
        ),
        BrokerTarget(
            "redis",
            "redis://:encoded%2Fsecret@127.0.0.1:6379/0",
            backend_options={"schema": "ordinary-secret", "password": "option-secret"},
        ),
    ]

    representations = [repr(target) for target in targets]

    for representation in representations:
        assert "uri-secret" not in representation
        assert "conninfo secret" not in representation
        assert "encoded%2Fsecret" not in representation
        assert "option-secret" not in representation
        assert "ordinary-secret" not in representation
        assert "***" in representation

    assert representations[0].index("password") < representations[0].index("schema")
    assert representations[1].index("password") < representations[1].index("schema")
