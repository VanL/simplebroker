"""Property-based tests for queue-name validation and end-to-end usability.

The validator (db.py:_validate_queue_name_cached) accepts names matching
QUEUE_NAME_PATTERN (^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$ via .match()) up to
MAX_QUEUE_NAME_LENGTH chars. Two contracts:

1. Everything the grammar accepts actually WORKS, end to end, on every
   backend (a name that validates but breaks storage would be a
   cross-backend drift bug — exactly what this suite exists to catch).
2. Everything outside the accept set is rejected at first use.

Marked shared: name handling crosses into SQL text / Redis key territory,
where backend escaping differences would surface.
"""

from __future__ import annotations

import itertools

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from simplebroker._constants import MAX_QUEUE_NAME_LENGTH
from simplebroker._exceptions import QueueNameError
from simplebroker.db import QUEUE_NAME_PATTERN

pytestmark = pytest.mark.shared

# Unique-suffix counter: one broker serves ALL examples of a test item (the
# fixture is function-scoped, Hypothesis examples are not), so every example
# must use a queue name nobody else wrote to. See Part I section 4 caveat.
_uniq = itertools.count()

# st.from_regex deliberately exploits '$'-before-newline and can generate
# trailing-"\n" strings; the validator uses fullmatch (finding F4, resolved)
# and rejects those, so the strategy filters them out of the VALID set.
# Length cap leaves room for the "_<n>" suffix.
VALID_NAMES = st.from_regex(QUEUE_NAME_PATTERN).filter(
    lambda s: len(s) <= MAX_QUEUE_NAME_LENGTH - 24 and not s.endswith("\n")
)

# exclude_categories=("Cs",): lone surrogates cannot be UTF-8 encoded, and
# Queue.write raises UnicodeEncodeError for them — FINDING F8, pinned in
# test_property_message_roundtrip.py. NB: explicit st.characters() includes
# surrogates unless told otherwise (unlike st.text()'s default alphabet).
BODIES = st.text(
    alphabet=st.characters(exclude_characters="\x00", exclude_categories=("Cs",)),
    max_size=50,
)


def _validator_accepts(s: str) -> bool:
    """Mirror db._validate_queue_name_cached's accept logic exactly."""
    return (
        bool(s)
        and len(s) <= MAX_QUEUE_NAME_LENGTH
        and bool(QUEUE_NAME_PATTERN.fullmatch(s))
    )


@given(name=VALID_NAMES, body=BODIES)
@settings(
    # The function-scoped queue_factory is intentionally reused across
    # examples; isolation comes from the unique name suffix. too_slow must be
    # re-listed because suppress_health_check replaces, not appends. Example
    # counts come from the active profile (see tests/conftest.py).
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_grammar_valid_names_work_end_to_end(
    queue_factory, name: str, body: str
) -> None:
    q = queue_factory(f"{name}_{next(_uniq)}")
    q.write(body)
    assert q.read_one() == body


@given(name=st.text(max_size=600).filter(lambda s: not _validator_accepts(s)))
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_grammar_invalid_names_are_rejected_at_first_use(
    queue_factory, name: str
) -> None:
    """Invalid names raise QueueNameError, as the docstrings promise
    (finding F5, resolved: QueueNameError also subclasses ValueError so
    pre-existing `except ValueError` callers keep working)."""
    q = queue_factory(name)  # construction does not validate ...
    with pytest.raises(QueueNameError):
        q.write("x")  # ... first use does


def test_queue_name_error_is_a_value_error() -> None:
    """Backward-compat contract for finding F5: QueueNameError must remain a
    ValueError subclass so callers that caught the old exception type keep
    working."""
    assert issubclass(QueueNameError, ValueError)


def test_trailing_newline_name_rejected(queue_factory) -> None:
    """Names with a trailing newline are rejected (finding F4, resolved):
    validation uses fullmatch, so '$'-before-newline acceptance is closed and
    the validator matches its documented charset (and the prefix validator,
    which always used fullmatch)."""
    q = queue_factory("nl_quirk\n")
    with pytest.raises(QueueNameError):
        q.write("hello")
