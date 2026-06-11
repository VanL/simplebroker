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
from simplebroker.db import QUEUE_NAME_PATTERN

pytestmark = pytest.mark.shared

# Unique-suffix counter: one broker serves ALL examples of a test item (the
# fixture is function-scoped, Hypothesis examples are not), so every example
# must use a queue name nobody else wrote to. See Part I section 4 caveat.
_uniq = itertools.count()

# Trailing-"\n" names are accepted by the validator (finding F4, pinned in a
# dedicated test below) but would corrupt our "_<n>" suffixing, so the
# strategy filters them out. Length cap leaves room for the suffix.
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
        and bool(QUEUE_NAME_PATTERN.match(s))
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
    """FINDING F5 (pinned): rejection raises ValueError, although docstrings
    advertise QueueNameError (which is not a ValueError subclass). If this
    starts failing with QueueNameError, the implementation moved to match its
    docs — update the findings log and flip this assertion deliberately."""
    q = queue_factory(name)  # construction does not validate ...
    with pytest.raises(ValueError):
        q.write("x")  # ... first use does


def test_known_quirk_trailing_newline_name_accepted(queue_factory) -> None:
    """FINDING F4 (pinned, not endorsed): '$' with .match() matches before a
    trailing newline, so 'name\\n' validates AND functions on every backend."""
    q = queue_factory("nl_quirk\n")
    q.write("hello")
    assert q.read_one() == "hello"
