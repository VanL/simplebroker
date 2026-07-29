"""Cross-backend contract tests for public Queue message-size validation."""

from typing import Any

import pytest

from simplebroker import Queue

pytestmark = [pytest.mark.shared]

_LIMIT_BYTES = 4


def _queue(broker_target: Any, name: str) -> Queue:
    return Queue(
        name,
        db_path=broker_target,
        persistent=True,
        config={"BROKER_MAX_MESSAGE_SIZE": _LIMIT_BYTES},
    )


def test_queue_write_accepts_exact_utf8_byte_limit(broker_target: Any) -> None:
    with _queue(broker_target, "message_size_exact") as queue:
        queue.write("🎉")

        assert queue.read() == "🎉"


def test_queue_write_rejects_one_byte_over_limit_with_stable_diagnostic(
    broker_target: Any,
) -> None:
    with _queue(broker_target, "message_size_overflow") as queue:
        with pytest.raises(
            ValueError,
            match=(
                r"Message size \(5 bytes\) exceeds maximum allowed size "
                r"\(4 bytes\)"
            ),
        ):
            queue.write("🎉x")

        assert queue.peek() is None


def test_queue_write_rejects_lone_surrogate(broker_target: Any) -> None:
    with _queue(broker_target, "message_size_surrogate") as queue:
        with pytest.raises(
            ValueError,
            match=r"Message must be UTF-8 encodable",
        ):
            queue.write("\ud800")

        assert queue.peek() is None
