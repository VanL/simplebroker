"""Tests for the default message and error handlers."""

import json
import logging
from unittest.mock import patch

import pytest

from simplebroker.watcher import (
    default_error_handler,
    json_print_handler,
    logger_handler,
    simple_print_handler,
)


class TestDefaultHandlers:
    """Test the default message handlers."""

    def test_simple_print_handler(self, capsys):
        """Test simple_print_handler output format."""
        simple_print_handler("test message", 1234567890)

        captured = capsys.readouterr()
        assert captured.out == "[1234567890] test message\n"
        assert captured.err == ""

    def test_simple_print_handler_with_newlines(self, capsys):
        """Test simple_print_handler with message containing newlines."""
        simple_print_handler("line1\nline2\nline3", 9876543210)

        captured = capsys.readouterr()
        expected = "[9876543210] line1\nline2\nline3\n"
        assert captured.out == expected

    def test_simple_print_handler_with_special_chars(self, capsys):
        """Test simple_print_handler with special characters."""
        special_msg = "Special chars: <>&'\"[]{}()"
        simple_print_handler(special_msg, 1111111111)

        captured = capsys.readouterr()
        assert captured.out == f"[1111111111] {special_msg}\n"

    def test_json_print_handler(self, capsys):
        """Test json_print_handler output format."""
        json_print_handler("test message", 1234567890)

        captured = capsys.readouterr()
        output_data = json.loads(captured.out.strip())

        assert output_data["message"] == "test message"
        assert output_data["timestamp"] == 1234567890
        assert captured.err == ""

    def test_json_print_handler_with_newlines(self, capsys):
        """Test json_print_handler safely handles newlines."""
        json_print_handler("line1\nline2\nline3", 9876543210)

        captured = capsys.readouterr()
        output_data = json.loads(captured.out.strip())

        assert output_data["message"] == "line1\nline2\nline3"
        assert output_data["timestamp"] == 9876543210

    def test_json_print_handler_with_special_chars(self, capsys):
        """Test json_print_handler with special characters and unicode."""
        special_msg = "Special: <>&'\"[]{}() 🚀 中文"
        json_print_handler(special_msg, 2222222222)

        captured = capsys.readouterr()
        output_data = json.loads(captured.out.strip())

        assert output_data["message"] == special_msg
        assert output_data["timestamp"] == 2222222222
        # Verify ensure_ascii=False works
        assert "🚀" in captured.out
        assert "中文" in captured.out

    def test_logger_handler(self, caplog):
        """Test logger_handler uses correct logger and format."""
        with caplog.at_level(logging.INFO, logger="simplebroker.watcher"):
            logger_handler("test log message", 3333333333)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelno == logging.INFO
        assert record.name == "simplebroker.watcher"
        assert record.getMessage() == "Message 3333333333: test log message"

    def test_logger_handler_with_special_content(self, caplog):
        """Test logger_handler with various message content."""
        test_cases = [
            "Message with\nnewlines",
            "Unicode: 🎉 测试",
            "Special chars: <>&'\"",
            "",  # Empty message
            "A" * 1000,  # Long message
        ]

        with caplog.at_level(logging.INFO, logger="simplebroker.watcher"):
            for i, msg in enumerate(test_cases):
                logger_handler(msg, 4000000000 + i)

        assert len(caplog.records) == len(test_cases)
        for i, record in enumerate(caplog.records):
            expected_msg = f"Message {4000000000 + i}: {test_cases[i]}"
            assert record.getMessage() == expected_msg


class TestDefaultErrorHandler:
    """Test the default error handler."""

    def test_default_error_handler_returns_true(self):
        """Test that default_error_handler always returns True."""
        exc = ValueError("Test error")
        result = default_error_handler(exc, "test message", 1234567890)

        assert result is True

    def test_default_error_handler_logs_error(self, caplog):
        """Test that default_error_handler logs the error."""
        with caplog.at_level(logging.ERROR, logger="simplebroker.watcher"):
            exc = RuntimeError("Test runtime error")
            default_error_handler(exc, "failing message", 5555555555)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelno == logging.ERROR
        assert record.name == "simplebroker.watcher"
        assert "Handler error: Test runtime error" in record.getMessage()

    def test_default_error_handler_with_various_exceptions(self, caplog):
        """Test default_error_handler with different exception types."""
        exceptions = [
            ValueError("Value error"),
            TypeError("Type error"),
            RuntimeError("Runtime error"),
            Exception("Generic exception"),
        ]

        with caplog.at_level(logging.ERROR, logger="simplebroker.watcher"):
            for i, exc in enumerate(exceptions):
                result = default_error_handler(exc, f"message_{i}", 6000000000 + i)
                assert result is True

        assert len(caplog.records) == len(exceptions)
        for i, record in enumerate(caplog.records):
            assert f"Handler error: {exceptions[i]}" in record.getMessage()

    def test_default_error_handler_ignores_broker_config(self, caplog):
        """Test that default_error_handler always logs regardless of config."""
        # Test with BROKER_LOGGING_ENABLED = False
        with patch("simplebroker.watcher._config", {"BROKER_LOGGING_ENABLED": False}):
            with caplog.at_level(logging.ERROR, logger="simplebroker.watcher"):
                exc = ValueError("Should still be logged")
                result = default_error_handler(exc, "test", 7777777777)

                assert result is True
                assert len(caplog.records) == 1
                assert (
                    "Handler error: Should still be logged"
                    in caplog.records[0].getMessage()
                )


class TestHandlerIntegration:
    """Test handlers work with actual QueueWatcher instances."""

    def test_default_handlers_with_queuewatcher(self, temp_db):
        """Test that default handlers can be used with QueueWatcher."""
        from simplebroker.watcher import QueueWatcher

        # Test message handlers
        message_handlers = [simple_print_handler, logger_handler]

        for handler in message_handlers:
            # Should not raise any errors
            watcher = QueueWatcher("test_queue", handler, db=temp_db)
            assert callable(watcher._handler)
            assert watcher._handler == handler

        # Test error handler
        def dummy_msg_handler(msg, ts):
            pass

        watcher = QueueWatcher(
            "test_queue",
            dummy_msg_handler,
            db=temp_db,
            error_handler=default_error_handler,
        )
        assert callable(watcher._error_handler)
        assert watcher._error_handler == default_error_handler

    def test_json_handler_produces_valid_json(self, capsys):
        """Test that json_print_handler always produces valid JSON."""
        test_messages = [
            ("simple", 1),
            ("with\nnewlines", 2),
            ('with"quotes"and\\backslashes', 3),
            ("unicode: 🎉 中文 العربية", 4),
            ("", 5),  # Empty message
        ]

        for msg, ts in test_messages:
            json_print_handler(msg, ts)

        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")

        assert len(lines) == len(test_messages)

        for i, line in enumerate(lines):
            # Each line should be valid JSON
            data = json.loads(line)
            assert data["message"] == test_messages[i][0]
            assert data["timestamp"] == test_messages[i][1]


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database file."""
    return str(tmp_path / "test.db")
