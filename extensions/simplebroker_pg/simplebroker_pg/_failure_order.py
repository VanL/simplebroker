"""PostgreSQL cleanup and maintenance failure ordering."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

_T = TypeVar("_T")
PgFailurePhase = Literal[
    "lock",
    "body",
    "rollback",
    "unlock",
    "discard",
    "pre-release",
    "release",
    "connection discard",
]


@dataclass(frozen=True, slots=True)
class PgStepResult(Generic[_T]):
    """The exact value or failure produced by one PostgreSQL lifecycle step."""

    phase: PgFailurePhase
    value: _T | None = None
    failure: BaseException | None = None


def capture_pg_step(
    phase: PgFailurePhase,
    action: Callable[[], _T],
) -> PgStepResult[_T]:
    """Run one lifecycle step without skipping later required cleanup."""

    try:
        return PgStepResult(phase=phase, value=action())
    except BaseException as failure:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-006] exception
        return PgStepResult(phase=phase, failure=failure)


def add_pg_failure_note(
    primary: BaseException,
    phase: PgFailurePhase,
    failure: BaseException,
) -> None:
    """Record an ordinary secondary failure without replacing its primary."""

    primary.add_note(
        f"{phase} failure: {type(failure).__qualname__}: "
        f"{_stable_exception_message(failure)}"
    )


def _stable_exception_message(failure: BaseException) -> str:
    """Render built-in string arguments without invoking custom formatting."""

    string_args = [argument for argument in failure.args if type(argument) is str]
    return ": ".join(string_args) if string_args else "<message unavailable>"


def capture_ordinary_pg_cleanup(
    *,
    primary: BaseException,
    phase: PgFailurePhase,
    action: Callable[[], None],
) -> None:
    """Run best-effort ordinary cleanup while preserving interruption priority."""

    try:
        action()
    except Exception as cleanup_failure:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-005] exception
        add_pg_failure_note(primary, phase, cleanup_failure)


def chain_pg_failure(
    primary: BaseException,
    prior: BaseException | None,
    *,
    explicit_cause: bool = False,
) -> BaseException:
    """Make a prior phase inspectable behind the selected primary failure."""

    if prior is not None and prior is not primary:
        primary.__context__ = prior
        if explicit_cause:
            primary.__cause__ = prior
            primary.__suppress_context__ = True
    return primary


def resolve_pg_vacuum_acquire_failure(
    acquire_failure: BaseException,
    discard_failure: BaseException | None,
) -> BaseException:
    """Resolve uncertain advisory-lock acquisition and session discard."""

    if discard_failure is None:
        return acquire_failure
    if not isinstance(discard_failure, Exception):
        return chain_pg_failure(
            discard_failure,
            acquire_failure,
            explicit_cause=True,
        )
    add_pg_failure_note(acquire_failure, "discard", discard_failure)
    return acquire_failure


def resolve_pg_vacuum_rollback_failure(
    body_failure: BaseException,
    rollback_failure: BaseException | None,
) -> BaseException:
    """Resolve a failed batch body against its required rollback attempt."""

    if rollback_failure is None:
        return body_failure
    if isinstance(rollback_failure, Exception):
        if isinstance(body_failure, Exception):
            return chain_pg_failure(rollback_failure, body_failure)
        add_pg_failure_note(body_failure, "rollback", rollback_failure)
        return body_failure
    return chain_pg_failure(
        rollback_failure,
        body_failure,
        explicit_cause=True,
    )


def resolve_pg_vacuum_pre_release_failure(
    body_failure: BaseException | None,
    unlock_failure: BaseException | None,
    discard_failure: BaseException | None,
) -> BaseException | None:
    """Resolve body, unlock, and uncertain-session discard outcomes."""

    if unlock_failure is None:
        return body_failure

    if discard_failure is not None and not isinstance(discard_failure, Exception):
        chain_pg_failure(unlock_failure, body_failure)
        return chain_pg_failure(discard_failure, unlock_failure)

    if (
        body_failure is not None
        and not isinstance(body_failure, Exception)
        and isinstance(unlock_failure, Exception)
    ):
        primary = body_failure
        add_pg_failure_note(primary, "unlock", unlock_failure)
    else:
        primary = chain_pg_failure(unlock_failure, body_failure)

    if discard_failure is not None:
        add_pg_failure_note(primary, "discard", discard_failure)
    return primary


def resolve_pg_vacuum_release_failure(
    pre_release_failure: BaseException | None,
    release_failure: BaseException | None,
) -> BaseException | None:
    """Resolve logical lease release against the earlier vacuum outcome."""

    if release_failure is None:
        return pre_release_failure
    if (
        pre_release_failure is not None
        and not isinstance(pre_release_failure, Exception)
        and isinstance(release_failure, Exception)
    ):
        add_pg_failure_note(pre_release_failure, "release", release_failure)
        return pre_release_failure
    return chain_pg_failure(release_failure, pre_release_failure)
