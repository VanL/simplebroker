from simplebroker._targets import ResolvedTarget, redact_backend_target


def test_redact_backend_target_redacts_uri_password() -> None:
    assert (
        redact_backend_target("postgresql://user:secret@db.example.com:5432/app")
        == "postgresql://user:***@db.example.com:5432/app"
    )


def test_redact_backend_target_redacts_conninfo_password() -> None:
    assert (
        redact_backend_target("host=db.example.com user=app password='secret value'")
        == "host=db.example.com user=app password=***"
    )


def test_resolved_target_display_target_leaves_sqlite_paths_unchanged() -> None:
    target = ResolvedTarget("sqlite", "/tmp/password=literal.db")

    assert target.display_target == "/tmp/password=literal.db"
