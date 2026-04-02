"""Test message timestamp-based move functionality."""

import pytest

pytestmark = [pytest.mark.shared]


def test_move_by_message_timestamp(broker):
    """Test moving a specific message by timestamp."""
    broker.write("source", "msg1")
    broker.write("source", "msg2")
    broker.write("source", "msg3")

    # Get timestamps via public API
    messages = list(broker.peek_generator("source", with_timestamps=True))
    assert len(messages) == 3

    msg2_timestamp = messages[1][1]
    result = broker.move_one(
        "source", "dest", exact_timestamp=msg2_timestamp, with_timestamps=True
    )

    assert result is not None
    assert result[0] == "msg2"
    assert result[1] == msg2_timestamp

    remaining = broker.peek_many("source", with_timestamps=False)
    assert remaining == ["msg1", "msg3"]

    dest_messages = broker.peek_many("dest", with_timestamps=False)
    assert dest_messages == ["msg2"]


def test_move_by_timestamp_not_found(broker):
    """Test moving a non-existent message timestamp returns None."""
    broker.write("source", "msg1")

    result = broker.move_one(
        "source", "dest", exact_timestamp=99999999, with_timestamps=True
    )
    assert result is None

    remaining = broker.peek_many("source", with_timestamps=False)
    assert remaining == ["msg1"]


def test_move_by_id_wrong_queue(broker):
    """Test moving a message timestamp from wrong queue returns None."""
    broker.write("queue1", "msg1")

    messages = list(broker.peek_generator("queue1", with_timestamps=True))
    msg_ts = messages[0][1]

    result = broker.move_one("queue2", "dest", exact_timestamp=msg_ts)
    assert result is None

    remaining = broker.peek_many("queue1", with_timestamps=False)
    assert remaining == ["msg1"]


def test_move_claimed_message_with_require_unclaimed(broker):
    """Test that claimed messages are not moved when require_unclaimed=True."""
    broker.write("source", "msg1")
    broker.write("source", "msg2")

    # Get timestamps before claiming
    messages = list(broker.peek_generator("source", with_timestamps=True))
    msg1_ts = messages[0][1]
    msg2_ts = messages[1][1]

    # Claim msg1
    claimed = broker.claim_one("source", with_timestamps=False)
    assert claimed == "msg1"

    # Try to move claimed message with require_unclaimed=True (default)
    result = broker.move_one("source", "dest", exact_timestamp=msg1_ts)
    assert result is None

    # Move unclaimed message (msg2)
    result = broker.move_one("source", "dest", exact_timestamp=msg2_ts)
    assert result is not None
    assert result == "msg2" or (isinstance(result, tuple) and result[0] == "msg2")


def test_move_claimed_message_without_require_unclaimed(broker):
    """Test that claimed messages CAN be moved when require_unclaimed=False."""
    broker.write("source", "msg1")
    broker.write("source", "msg2")

    # Get timestamp before claiming
    messages = list(broker.peek_generator("source", with_timestamps=True))
    msg1_ts = messages[0][1]

    # Claim msg1
    claimed = broker.claim_one("source", with_timestamps=False)
    assert claimed == "msg1"

    # Move claimed message with require_unclaimed=False
    result = broker.move_one(
        "source", "dest", exact_timestamp=msg1_ts, require_unclaimed=False
    )
    assert result is not None
    assert result == "msg1" or (isinstance(result, tuple) and result[0] == "msg1")

    dest_messages = broker.peek_many("dest", with_timestamps=False)
    assert dest_messages == ["msg1"]

    source_messages = broker.peek_many("source", with_timestamps=False)
    assert source_messages == ["msg2"]


def test_move_by_id_preserves_timestamp(broker):
    """Test that move preserves the original timestamp."""
    broker.write("source", "msg1")

    messages = list(broker.peek_generator("source", with_timestamps=True))
    original_ts = messages[0][1]

    result = broker.move_one(
        "source", "dest", exact_timestamp=original_ts, with_timestamps=True
    )
    assert result is not None
    assert isinstance(result, tuple)
    assert result[1] == original_ts

    dest_messages = list(broker.peek_generator("dest", with_timestamps=True))
    assert dest_messages[0][1] == original_ts


def test_move_mixed_mode(broker):
    """Test mixing timestamp-based and bulk move modes."""
    for i in range(5):
        broker.write("source", f"msg{i}")

    messages = list(broker.peek_generator("source", with_timestamps=True))

    # Move specific message by timestamp (msg2)
    msg2_ts = messages[2][1]
    result = broker.move_one("source", "dest1", exact_timestamp=msg2_ts)
    assert result == "msg2" or (isinstance(result, tuple) and result[0] == "msg2")

    # Move oldest unclaimed (should be msg0)
    result = broker.move_one("source", "dest2")
    assert result == "msg0" or (isinstance(result, tuple) and result[0] == "msg0")

    # Move by timestamp again (msg4)
    msg4_ts = messages[4][1]
    result = broker.move_one("source", "dest1", exact_timestamp=msg4_ts)
    assert result == "msg4" or (isinstance(result, tuple) and result[0] == "msg4")

    remaining = broker.peek_many("source", with_timestamps=False)
    assert set(remaining) == {"msg1", "msg3"}

    dest1_msgs = broker.peek_many("dest1", with_timestamps=False)
    assert set(dest1_msgs) == {"msg2", "msg4"}

    dest2_msgs = broker.peek_many("dest2", with_timestamps=False)
    assert dest2_msgs == ["msg0"]
