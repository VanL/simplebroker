"""Property-based tests for message-body fidelity and the size limit.

Contracts:
1. Any UTF-8-encodable Unicode body (minus NUL, see below) survives
   write -> read_one character-identical, on every backend.
2. The size limit counts UTF-8 BYTES, not characters (db.py:1081), accepts
   exactly the bodies within the limit, and rejects oversize ones without
   writing anything.

NUL ("\\x00") is excluded from generated bodies because the backends
genuinely diverge (finding F6, verified on live backends): SQLite and Redis
round-trip NUL, Postgres rejects it at write time. The explicit test at the
bottom pins that per-backend contract instead of letting the property suite
trip over the divergence at random.

Lone surrogates are likewise excluded (finding F8, pinned below): they are
not UTF-8 encodable, so write raises UnicodeEncodeError before any storage.
"""

from __future__ import annotations

import itertools

import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from simplebroker._exceptions import MessageError, OperationalError

from .helper_scripts.broker_factory import active_backend, make_queue

pytestmark = pytest.mark.shared

_uniq = itertools.count()

# exclude_categories=("Cs",): see finding F8 — explicit st.characters()
# includes surrogates unless told otherwise, and surrogates cannot be
# UTF-8 encoded.
_BODY_CHARS = st.characters(exclude_characters="\x00", exclude_categories=("Cs",))

BODIES = st.text(alphabet=_BODY_CHARS, max_size=300)

# Small override so the boundary is reachable with small generated bodies.
# The reject branch below doubles as proof the override is actually applied
# (the default limit is 10MB and would never reject these inputs).
SIZE_LIMIT_BYTES = 64


@given(body=BODIES)
@settings(
    # Function-scoped fixture reuse is intentional (isolation via unique
    # queue names); example counts come from the active profile.
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_bodies_round_trip_identically(queue_factory, body: str) -> None:
    q = queue_factory(f"body_{next(_uniq)}")
    q.write(body)
    # Empty string is a legal body; compare against None explicitly so an
    # empty-queue result can never masquerade as success.
    got = q.read_one()
    assert got is not None
    assert got == body


@given(body=st.text(alphabet=_BODY_CHARS, max_size=40))
@example("a" * SIZE_LIMIT_BYTES)  # exactly at the limit: accepted
@example("a" * (SIZE_LIMIT_BYTES + 1))  # one byte over: rejected
@example("é" * 32)  # 64 UTF-8 bytes in 32 chars: accepted
@example("é" * 33)  # 66 bytes in 33 chars: rejected
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_size_limit_counts_utf8_bytes(broker_target, body: str) -> None:
    q = make_queue(
        f"size_{next(_uniq)}",
        broker_target,
        config={"BROKER_MAX_MESSAGE_SIZE": SIZE_LIMIT_BYTES},
    )
    try:
        if len(body.encode("utf-8")) <= SIZE_LIMIT_BYTES:
            q.write(body)
            assert q.read_one() == body
        else:
            # MessageError subclasses ValueError, so older callers that
            # caught ValueError for oversize writes keep working.
            with pytest.raises(MessageError):
                q.write(body)
            assert q.read_one() is None  # the rejected write stored nothing
    finally:
        q.close()


def test_nul_byte_bodies_pinned_per_backend(queue_factory) -> None:
    """FINDING F6 (pre-verified 2026-06-11 on live Dockerized backends): NUL
    bodies round-trip on SQLite and Redis, but the Postgres backend rejects
    them at write time with OperationalError("PostgreSQL text fields cannot
    contain NUL (0x00) bytes") and stores nothing; the queue stays usable.
    Pinned per backend so any backend changing its NUL stance fails loudly."""
    q = queue_factory("nul_probe")
    if active_backend() == "postgres":
        with pytest.raises(OperationalError):
            q.write("a\x00b")
        assert q.read_one() is None
    else:
        q.write("a\x00b")
        assert q.read_one() == "a\x00b"


def test_lone_surrogate_bodies_raise_message_error(queue_factory) -> None:
    """Lone surrogates are not UTF-8 encodable; write() rejects them with
    MessageError, as its docstring promises (finding F8, resolved — the
    original UnicodeEncodeError is preserved as the cause). Backend
    independent: the error fires client-side, before any storage."""
    q = queue_factory("surrogate_probe")
    with pytest.raises(MessageError) as exc_info:
        q.write("\ud800")
    assert isinstance(exc_info.value.__cause__, UnicodeEncodeError)
    assert q.read_one() is None


def test_message_error_is_a_value_error() -> None:
    """Backward-compat contract for finding F8: MessageError must remain a
    ValueError subclass so callers that caught the old exception type keep
    working."""
    assert issubclass(MessageError, ValueError)
