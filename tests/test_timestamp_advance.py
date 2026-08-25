"""Focused contracts for monotone durable timestamp advancement."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

import simplebroker._timestamp as timestamp_module
from simplebroker import open_broker
from simplebroker._backends.sqlite.plugin import SQLiteBackendPlugin
from simplebroker._exceptions import OperationalError, TimestampError
from simplebroker._timestamp import TimestampGenerator


def _generator(monkeypatch: pytest.MonkeyPatch) -> tuple[TimestampGenerator, Any]:
    plugin = SQLiteBackendPlugin()
    runner = object()
    generator = TimestampGenerator(runner, backend_plugin=plugin)  # type: ignore[arg-type]
    return generator, plugin


def test_timestamp_error_outcome_ambiguity_defaults_false() -> None:
    known = TimestampError("known failure")
    ambiguous = TimestampError("unknown outcome", outcome_ambiguous=True)

    assert known.outcome_ambiguous is False
    assert ambiguous.outcome_ambiguous is True


@pytest.mark.shared
def test_advance_rejects_none_before_backend_work(broker: Any) -> None:
    assert broker.refresh_last_timestamp() == 0

    with pytest.raises(TypeError, match="timestamp must be an int"):
        broker.advance_last_timestamp(None)

    assert broker.refresh_last_timestamp() == 0


def test_advance_skips_initial_read_and_caches_one_final_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with open_broker(str(tmp_path / "order.db")) as broker:
        internal_broker = cast(Any, broker)
        generator = internal_broker._timestamp_gen
        plugin = generator._backend_plugin
        original_advance = plugin.advance_last_ts
        original_read = plugin.read_last_ts
        events: list[str] = []

        def advance(runner: object, *, new_ts: int) -> bool:
            events.append(f"advance:{new_ts}")
            return bool(original_advance(runner, new_ts=new_ts))

        def read(runner: object) -> int:
            events.append("read")
            return int(original_read(runner))

        monkeypatch.setattr(plugin, "advance_last_ts", advance)
        monkeypatch.setattr(plugin, "read_last_ts", read)

        assert broker.advance_last_timestamp(100) == 100
        assert broker.get_cached_last_timestamp() == 100
        assert events == ["advance:100", "read"]


def test_advance_rejects_final_observation_below_requested_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Sanctioned fault hook: a real SQLite CAS cannot return a lower final
    # observation, and this test owns the defensive postcondition itself.
    generator, plugin = _generator(monkeypatch)
    monkeypatch.setattr(
        plugin,
        "advance_last_ts",
        lambda _runner, *, new_ts: True,
    )
    monkeypatch.setattr(plugin, "read_last_ts", lambda _runner: 99)

    with pytest.raises(TimestampError, match="below requested floor") as caught:
        generator.advance_to_at_least(100)

    assert caught.value.outcome_ambiguous is False


def test_nonretryable_advance_error_is_outcome_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Sanctioned fault hook: transport ambiguity is not inducible through a
    # healthy local SQLite connection; real Redis EVAL coverage lives in the
    # extension suite.
    generator, plugin = _generator(monkeypatch)
    error = OperationalError("connection reset")
    error.retryable = False

    def fail(_runner: object, *, new_ts: int) -> bool:
        raise error

    monkeypatch.setattr(plugin, "advance_last_ts", fail)

    with pytest.raises(TimestampError, match="durable outcome is unknown") as caught:
        generator.advance_to_at_least(100)

    assert caught.value.outcome_ambiguous is True


def test_exhausted_retryable_advance_error_is_known_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generator, plugin = _generator(monkeypatch)
    error = OperationalError("database is locked")
    error.retryable = True

    def fail(_runner: object, *, new_ts: int) -> bool:
        raise error

    def execute_once(operation: Callable[[], bool], **_kwargs: object) -> bool:
        return operation()

    monkeypatch.setattr(plugin, "advance_last_ts", fail)
    monkeypatch.setattr(timestamp_module, "_execute_with_retry", execute_once)

    with pytest.raises(TimestampError, match="busy while writing timestamp") as caught:
        generator.advance_to_at_least(100)

    assert caught.value.outcome_ambiguous is False


def test_post_attempt_read_error_is_outcome_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generator, plugin = _generator(monkeypatch)
    monkeypatch.setattr(
        plugin,
        "advance_last_ts",
        lambda _runner, *, new_ts: True,
    )
    monkeypatch.setattr(
        plugin,
        "read_last_ts",
        lambda _runner: (_ for _ in ()).throw(OperationalError("read failed")),
    )

    with pytest.raises(TimestampError, match="durable outcome is unknown") as caught:
        generator.advance_to_at_least(100)

    assert caught.value.outcome_ambiguous is True


def test_sqlite_missing_last_ts_row_fails_loudly(tmp_path: Path) -> None:
    with open_broker(str(tmp_path / "missing-meta.db")) as broker:
        runner = cast(Any, broker)._runner
        runner.run("DELETE FROM meta WHERE key = 'last_ts'")
        runner.commit()

        with pytest.raises(TimestampError, match="below requested floor") as caught:
            broker.advance_last_timestamp(100)

    assert caught.value.outcome_ambiguous is False
