"""Property tests for the production CLI argv normalizer and parser."""

from __future__ import annotations

import contextlib
import io
from typing import Literal

from hypothesis import example, given
from hypothesis import strategies as st

from simplebroker._constants import resolve_isolated_config
from simplebroker.cli import (
    ArgumentParserError,
    ArgumentProcessor,
    _build_cli_parser,
)

_BUNDLE = _build_cli_parser(config=resolve_isolated_config({}))
_GRAMMAR = _BUNDLE.grammar
_REGISTERED_WITHOUT_HELP = sorted(_GRAMMAR.registered_options - {"-h", "--help"})
_REGISTERED_LONG = sorted(
    option for option in _REGISTERED_WITHOUT_HELP if option.startswith("--")
)
_VALUE_SHORT = sorted(
    option
    for option in _GRAMMAR.registered_value_options
    if option.startswith("-") and not option.startswith("--")
)
_SUBCOMMANDS = sorted(_GRAMMAR.subcommands)
_SURROGATE_CATEGORY: tuple[Literal["Cs"]] = ("Cs",)

_plain_tokens = st.text(
    alphabet=st.characters(
        blacklist_categories=_SURROGATE_CATEGORY,
        blacklist_characters="\x00\r\n",
    ),
    min_size=1,
    max_size=16,
).filter(lambda token: not token.startswith("-"))
_unknown_dash_tokens = st.sampled_from(
    ["--not-registered", "--queueish", "--=x", "-t-prefixed", "-q-combined"]
)
_registered_tokens = st.one_of(
    st.sampled_from(_REGISTERED_WITHOUT_HELP),
    st.tuples(st.sampled_from(_REGISTERED_LONG), _plain_tokens).map(
        lambda parts: f"{parts[0]}={parts[1]}"
    ),
    st.tuples(st.sampled_from(_VALUE_SHORT), _plain_tokens).map(
        lambda parts: f"{parts[0]}{parts[1]}"
    ),
)
_argv_tokens = st.one_of(
    _plain_tokens,
    _unknown_dash_tokens,
    _registered_tokens,
    st.sampled_from(_SUBCOMMANDS),
    st.sampled_from(["--", "-h", "--help", ""]),
)


def _process_and_parse(argv: list[str]) -> tuple[str, ...] | None:
    """Run one fresh production normalizer and the real parser."""
    processor = ArgumentProcessor(_GRAMMAR)
    try:
        processed = processor.process(argv)
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            try:
                _BUNDLE.parser.parse_args(processed.normalized_argv)
            except SystemExit as error:
                assert error.code == 0
        return processed.normalized_argv
    except ArgumentParserError:
        return None


@example(argv=["write", "q", "--json"])
@example(argv=["write", "q", "--cleanup"])
@example(argv=["write", "q", "--after=1s"])
@example(argv=["write", "q", "-m123"])
@example(argv=["write", "q", "-t-prefixed"])
@example(argv=["broadcast", "-pqueue*", "notice"])
@example(argv=["broadcast", "--queue=alpha", "notice"])
@example(argv=["write", "q", "--", "--cleanup"])
@given(argv=st.lists(_argv_tokens, max_size=12))
def test_cli_args_totality_property(argv: list[str]) -> None:
    """Arbitrary argv has only documented parser outcomes."""
    _process_and_parse(argv)


@given(command=st.sampled_from(_SUBCOMMANDS), tail=st.lists(_argv_tokens, max_size=8))
def test_root_actions_never_hoist_after_a_subcommand(
    command: str, tail: list[str]
) -> None:
    processor = ArgumentProcessor(_GRAMMAR)
    try:
        normalized = processor.process([command, *tail]).normalized_argv
    except ArgumentParserError:
        return
    assert normalized[0] == command


@given(
    command=st.sampled_from(["write", "broadcast"]),
    literal=st.lists(_argv_tokens, max_size=6),
)
def test_explicit_marker_preserves_following_data(
    command: str, literal: list[str]
) -> None:
    prefix = [command, "q"] if command == "write" else [command]
    normalized = (
        ArgumentProcessor(_GRAMMAR).process([*prefix, "--", *literal]).normalized_argv
    )
    marker = normalized.index("--")
    assert list(normalized[marker + 1 :]) == literal


@example(command="write", token="--json")
@example(command="write", token="--cleanup")
@example(command="write", token="--after=1s")
@example(command="write", token="-m123")
@example(command="broadcast", token="-pqueue*")
@example(command="broadcast", token="--queue=alpha")
@given(command=st.sampled_from(["write", "broadcast"]), token=_registered_tokens)
def test_registered_spelling_is_never_auto_protected_as_data(
    command: str, token: str
) -> None:
    argv = [command, "q", token] if command == "write" else [command, token]
    processor = ArgumentProcessor(_GRAMMAR)
    try:
        normalized = processor.process(argv).normalized_argv
    except ArgumentParserError:
        return
    if "--" in normalized:
        assert normalized.index("--") > normalized.index(token)


@example(command="write", token="-t-prefixed")
@example(command="broadcast", token="--queueish")
@example(command="broadcast", token="--=x")
@given(
    command=st.sampled_from(["write", "broadcast"]),
    token=_unknown_dash_tokens,
)
def test_unknown_dash_operand_remains_literal(command: str, token: str) -> None:
    argv = [command, "q", token] if command == "write" else [command, token]
    normalized = ArgumentProcessor(_GRAMMAR).process(argv).normalized_argv
    marker = normalized.index("--")
    assert normalized[marker + 1] == token
