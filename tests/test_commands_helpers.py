"""Unit tests for helper functions in simplebroker.commands."""

from __future__ import annotations

import json
import types
from collections.abc import Iterator

import pytest

import simplebroker.commands as commands
from simplebroker._constants import EXIT_ERROR, EXIT_SUCCESS
from simplebroker.commands import (
    _get_message_content,
    _process_queue_fetch,
    _resolve_timestamp_filters,
)

pytestmark = [pytest.mark.shared]


class TestResolveTimestampFilters:
    def test_valid_filters(self, capsys: pytest.CaptureFixture[str]) -> None:
        error, after_ts, before_ts, exact_ts = _resolve_timestamp_filters(
            "1700000000", "1700000001", "1234567890123456789"
        )

        assert error is None
        assert isinstance(after_ts, int) and after_ts > 0
        assert isinstance(before_ts, int) and before_ts > after_ts
        assert exact_ts == 1234567890123456789
        assert capsys.readouterr().err == ""

    def test_invalid_after_returns_exit_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error, after_ts, before_ts, exact_ts = _resolve_timestamp_filters(
            "invalid", None, None
        )

        assert error == EXIT_ERROR
        assert after_ts is None and before_ts is None and exact_ts is None
        captured = capsys.readouterr()
        assert "simplebroker: error" in captured.err

    def test_invalid_before_returns_exit_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error, after_ts, before_ts, exact_ts = _resolve_timestamp_filters(
            None, "invalid", None
        )

        assert error == EXIT_ERROR
        assert after_ts is None and before_ts is None and exact_ts is None
        captured = capsys.readouterr()
        assert "simplebroker: error" in captured.err

    def test_invalid_message_id_returns_exit_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        error, after_ts, before_ts, exact_ts = _resolve_timestamp_filters(
            None, None, "not-a-ts"
        )

        assert error == EXIT_ERROR
        assert after_ts is None and before_ts is None and exact_ts is None
        assert capsys.readouterr().err == ""


class TestGetMessageContent:
    def test_reads_from_stdin_when_message_omitted_and_stdin_is_piped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            commands.sys, "stdin", types.SimpleNamespace(isatty=lambda: False)
        )
        monkeypatch.setattr(commands, "_read_from_stdin", lambda *_args: "from stdin")

        assert _get_message_content(None) == "from stdin"

    def test_rejects_omitted_message_when_stdin_is_tty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            commands.sys, "stdin", types.SimpleNamespace(isatty=lambda: True)
        )

        with pytest.raises(ValueError, match="message is required"):
            _get_message_content(None)

    def test_uses_configured_message_size_limit(self) -> None:
        with pytest.raises(ValueError, match="maximum size of 3 bytes"):
            _get_message_content("toolong", config={"BROKER_MAX_MESSAGE_SIZE": 3})


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
            after_timestamp=None,
            before_timestamp=None,
            json_output=True,
            show_timestamps=False,
        )

        assert rc == EXIT_SUCCESS
        payload = json.loads(capsys.readouterr().out)
        assert payload == {"message": "hello", "timestamp": 111}

    def test_all_messages_path(self, capsys):
        def fetch_one(**_kwargs):  # pragma: no cover - unused
            return None

        def fetch_generator(*, with_timestamps, after_timestamp, before_timestamp):
            assert with_timestamps is True
            assert after_timestamp is None
            assert before_timestamp is None
            return iter([("a", 1), ("b", 2)])

        rc = _process_queue_fetch(
            fetch_one=fetch_one,
            fetch_generator=fetch_generator,
            exact_timestamp=None,
            all_messages=True,
            after_timestamp=None,
            before_timestamp=None,
            json_output=False,
            show_timestamps=False,
        )

        captured = capsys.readouterr()
        assert rc == EXIT_SUCCESS
        assert captured.out.strip().splitlines() == ["a", "b"]

    def test_after_timestamp_path(self, capsys):
        def fetch_one(**_kwargs):  # pragma: no cover - unused
            return None

        def fetch_generator(*, with_timestamps, after_timestamp, before_timestamp):
            assert after_timestamp == 99
            assert before_timestamp is None
            assert with_timestamps is True
            return iter([("c", 3)])

        rc = _process_queue_fetch(
            fetch_one=fetch_one,
            fetch_generator=fetch_generator,
            exact_timestamp=None,
            all_messages=False,
            after_timestamp=99,
            before_timestamp=None,
            json_output=False,
            show_timestamps=True,
        )

        captured = capsys.readouterr()
        assert rc == EXIT_SUCCESS
        assert captured.out.strip().startswith("3\t")

    def test_before_timestamp_path(self, capsys):
        def fetch_one(**_kwargs):  # pragma: no cover - unused
            return None

        def fetch_generator(*, with_timestamps, after_timestamp, before_timestamp):
            assert after_timestamp is None
            assert before_timestamp == 123
            assert with_timestamps is True
            return iter([("d", 4)])

        rc = _process_queue_fetch(
            fetch_one=fetch_one,
            fetch_generator=fetch_generator,
            exact_timestamp=None,
            all_messages=False,
            after_timestamp=None,
            before_timestamp=123,
            json_output=False,
            show_timestamps=True,
        )

        captured = capsys.readouterr()
        assert rc == EXIT_SUCCESS
        assert captured.out.strip().startswith("4\t")

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
            after_timestamp=None,
            before_timestamp=None,
            json_output=False,
            show_timestamps=False,
        )

        assert rc == EXIT_SUCCESS
        assert capsys.readouterr().out.strip() == "plain"
