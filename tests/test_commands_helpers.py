"""Unit tests for helper functions in simplebroker.commands."""

from __future__ import annotations

import json
import types
from collections.abc import Iterator

import pytest

import simplebroker.commands as commands
from simplebroker._constants import EXIT_ERROR, EXIT_QUEUE_EMPTY, EXIT_SUCCESS
from simplebroker.commands import (
    _get_message_content,
    _process_queue_fetch,
    _resolve_timestamp_filters,
)

pytestmark = [pytest.mark.shared]


class TestResolveTimestampFilters:
    def test_valid_filters(self, capsys: pytest.CaptureFixture[str]) -> None:
        error, since_ts, exact_ts = _resolve_timestamp_filters(
            "1700000000", "1234567890123456789"
        )

        assert error is None
        assert isinstance(since_ts, int) and since_ts > 0
        assert exact_ts == 1234567890123456789
        assert capsys.readouterr().err == ""

    def test_invalid_since_returns_exit_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error, since_ts, exact_ts = _resolve_timestamp_filters("invalid", None)

        assert error == EXIT_ERROR
        assert since_ts is None and exact_ts is None
        captured = capsys.readouterr()
        assert "simplebroker: error" in captured.err

    def test_invalid_message_id_returns_queue_empty(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error, since_ts, exact_ts = _resolve_timestamp_filters(None, "not-a-ts")

        assert error == EXIT_QUEUE_EMPTY
        assert since_ts is None and exact_ts is None
        assert capsys.readouterr().err == ""


class TestGetMessageContent:
    def test_reads_from_stdin_when_message_omitted_and_stdin_is_piped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            commands.sys, "stdin", types.SimpleNamespace(isatty=lambda: False)
        )
        monkeypatch.setattr(commands, "_read_from_stdin", lambda: "from stdin")

        assert _get_message_content(None) == "from stdin"

    def test_rejects_omitted_message_when_stdin_is_tty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            commands.sys, "stdin", types.SimpleNamespace(isatty=lambda: True)
        )

        with pytest.raises(ValueError, match="message is required"):
            _get_message_content(None)


class TestProcessQueueFetch:
    def test_exact_timestamp_path_json_output(self, capsys):
        def fetch_one(*, exact_timestamp, with_timestamps):
            assert exact_timestamp == 42
            assert with_timestamps is True
            return ("hello", 111)

        def fetch_generator(**_kwargs) -> Iterator[str]:  # pragma: no cover - unused
            return iter([])

        rc = _process_queue_fetch(
            fetch_one=fetch_one,
            fetch_generator=fetch_generator,
            exact_timestamp=42,
            all_messages=False,
            since_timestamp=None,
            json_output=True,
            show_timestamps=False,
        )

        assert rc == EXIT_SUCCESS
        payload = json.loads(capsys.readouterr().out)
        assert payload == {"message": "hello", "timestamp": 111}

    def test_all_messages_path(self, capsys):
        def fetch_one(**_kwargs):  # pragma: no cover - unused
            return None

        def fetch_generator(*, with_timestamps, since_timestamp):
            assert with_timestamps is True
            assert since_timestamp is None
            return iter([("a", 1), ("b", 2)])

        rc = _process_queue_fetch(
            fetch_one=fetch_one,
            fetch_generator=fetch_generator,
            exact_timestamp=None,
            all_messages=True,
            since_timestamp=None,
            json_output=False,
            show_timestamps=False,
        )

        captured = capsys.readouterr()
        assert rc == EXIT_SUCCESS
        assert captured.out.strip().splitlines() == ["a", "b"]

    def test_since_timestamp_path(self, capsys):
        def fetch_one(**_kwargs):  # pragma: no cover - unused
            return None

        def fetch_generator(*, with_timestamps, since_timestamp):
            assert since_timestamp == 99
            assert with_timestamps is True
            return iter([("c", 3)])

        rc = _process_queue_fetch(
            fetch_one=fetch_one,
            fetch_generator=fetch_generator,
            exact_timestamp=None,
            all_messages=False,
            since_timestamp=99,
            json_output=False,
            show_timestamps=True,
        )

        captured = capsys.readouterr()
        assert rc == EXIT_SUCCESS
        assert captured.out.strip().startswith("3\t")

    def test_single_fetch_plain_output(self, capsys):
        def fetch_one(*, exact_timestamp=None, with_timestamps=False):
            assert exact_timestamp is None
            assert with_timestamps is False
            return "plain"

        def fetch_generator(**_kwargs):  # pragma: no cover - unused
            return iter([])

        rc = _process_queue_fetch(
            fetch_one=fetch_one,
            fetch_generator=fetch_generator,
            exact_timestamp=None,
            all_messages=False,
            since_timestamp=None,
            json_output=False,
            show_timestamps=False,
        )

        assert rc == EXIT_SUCCESS
        assert capsys.readouterr().out.strip() == "plain"
