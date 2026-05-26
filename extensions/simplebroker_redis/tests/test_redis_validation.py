"""Redis namespace validation tests."""

from __future__ import annotations

import pytest
from simplebroker_redis.validation import (
    is_namespace_key,
    key_prefix,
    require_namespace,
)

from simplebroker._exceptions import DatabaseError


@pytest.mark.parametrize("namespace", ["parent:child", "parent:", ":child"])
def test_require_namespace_rejects_colons(namespace: str) -> None:
    with pytest.raises(DatabaseError, match="letters, numbers, _, -, or \\."):
        require_namespace({"namespace": namespace})


def test_require_namespace_allows_non_delimiter_punctuation() -> None:
    assert (
        require_namespace({"namespace": "tenant_1.jobs-prod"}) == "tenant_1.jobs-prod"
    )


def test_is_namespace_key_rejects_colon_extended_namespace_keys() -> None:
    prefix = key_prefix("parent")
    token = "0123456789abcdef0123456789abcdef"

    assert is_namespace_key(prefix, f"{prefix}:meta")
    assert is_namespace_key(prefix, f"{prefix}:q:jobs.pending:reserved")
    assert is_namespace_key(prefix, f"{prefix}:batches:{token}:meta")

    assert not is_namespace_key(prefix, f"{prefix}:child:meta")
    assert not is_namespace_key(prefix, f"{prefix}:q:q:jobs:reserved")
    assert not is_namespace_key(prefix, f"{prefix}:batches:batches:{token}:meta")
