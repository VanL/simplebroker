# Vendored Retry Core & Backon Consolidation — Implementation Plan

> **For agentic workers:** Implement task-by-task. Each task is red → green → commit
> unless a step says otherwise. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace SimpleBroker's scattered hand-rolled retry/backoff loops with one
vendorable stdlib-only module (`simplebroker/_retry.py`), then route **most** internal
retry paths through small adapters in `helpers.py`. Preserve observable behavior where
it matters; add bounded jitter only on lock-contention paths (helpers +
watcher operational). Connection and crash-recovery pacing stay deterministic.

**Architecture (three layers):**

| Layer | Module | Owns |
|---|---|---|
| **Engine** | `_retry.py` | Generic sync loop: sleep, wait generators, bounded jitter, stop composition, test hooks, hot-loop warning, `contextvars`, optional `before_sleep` callback |
| **Adapters** | `helpers.py` | Broker retry policy: which exceptions retry, knob mapping, `RetryInterrupted` → `StopException` |
| **Call sites** | `watcher.py`, `db.py`, … | Caller orchestration: stop semantics, logging, exception translation (`StopWatching`, `RuntimeError` wrap) |

Import rule: one-way only — `helpers` → `_retry`; call sites → `helpers`. `_retry.py`
must not import anything from `simplebroker.*` or know about queues, SQLite,
`OperationalError`, or `StopException`.

**What uses the engine vs what does not:**

| Path | Goes through `execute_retry`? | Via |
|---|---|---|
| Lock/busy DB ops (`_execute_with_retry`) | Yes | `helpers._execute_with_retry` |
| Watcher operational (`_process_with_retry`) | Yes | `helpers._execute_watcher_operational_retry` |
| DB connection open | Yes | `helpers._execute_connection_retry` |
| Watcher crash recovery (`_handle_retry`) | **No** — outer restart loop unchanged | `helpers.interruptible_sleep` only |

Crash recovery is intentionally **not** wrapped in `execute_retry` (different control
flow: restart whole watcher loop, not re-invoke one operation). Do not force it into
the engine to keep `_retry.py` vendorable and the semantics honest.

**Tech Stack:** Python ≥3.11, zero runtime dependencies, `uv`, pytest (+xdist), ruff,
strict mypy. Repo: `/Users/van/Developer/simplebroker`.

**Date:** 2026-07-05

**Status:** ready for execution (agent-1 through agent-5 review findings incorporated)

---

## Part I — Orientation (read this first)

### What SimpleBroker is

SimpleBroker is a message queue (SQLite by default; Postgres and Redis via extension
packages in `extensions/`). The **core package has zero runtime dependencies**. Public
API: top-level `simplebroker`. Extension/embed surface: `simplebroker.ext`.
Underscore-prefixed modules are **private implementation**.

This plan does **not** add a public retry API. `_retry.py` is internal infrastructure,
like `_phaselock.py`.

### What problem we are solving

Today, retry logic is duplicated in at least four places with **different backoff
math**:

| Location | Retries on | Backoff | Jitter |
|---|---|---|---|
| `helpers._execute_with_retry` | lock/busy `OperationalError` | `retry_delay * 2**attempt` | additive 0–25ms |
| `watcher._process_with_retry` | `OperationalError` | `0.05 * 2**attempt` | additive 0–25ms |
| `watcher._handle_retry` | any `Exception` (crash recovery) | `2**retry_count` seconds | **none** |
| `db.DBConnection.get_connection` | any `Exception` on connect | `2**(attempt+1)` seconds | **none** |

That fragmentation causes thundering-herd re-collisions and makes tests monkeypatch
`interruptible_sleep` ad hoc. Backon (a separate retry library in the monorepo) shows
the right shape: one engine, composable stops, test hooks. We are **not** adding
`backon` as a dependency — we vendor a ~400-line primitive.

### Why consolidation (not just a 2-edit jitter fix)

The **contention win** (bounded jitter) could ship in ~2 edits: change jitter in
`helpers._execute_with_retry` and `watcher._process_with_retry` only. Connection and
crash-recovery sleeps stay deterministic. That is a valid charter-minimal alternative.

This plan still chooses consolidation because:

1. **DRY on correctness-critical paths** — the duplicate loops already disagree on
   sleep count (see locked decision below); a one-line jitter swap leaves that bug in
   `db.py` until someone re-audits.
2. **Test hooks** (`remove_backoff`, sleep capture) become first-class instead of
   monkeypatching `helpers.interruptible_sleep`.
3. **Vendorable primitive** — same payoff as `_phaselock.py` for future embedders.

If scope must shrink **before Task 1**, use the 2-edit jitter path instead (no
`_retry.py`). If consolidation is already underway, either finish this plan or
**revert the branch** — there is no coherent hybrid “stop after Task 9 without
`_retry.py`” because Tasks 1–9 create and wire the module. The locked jitter formula
and no-sleep-on-final-attempt rule apply to both paths.

### Vocabulary

| Term | Meaning |
|---|---|
| **retry engine** | `execute_retry()` in `_retry.py` — generic sync loop; no broker types |
| **adapter** | A `helpers.py` wrapper that maps broker knobs/predicates to `execute_retry()` — not one function, a small family: `_execute_with_retry`, `_execute_watcher_operational_retry`, `_execute_connection_retry` |
| **call-site orchestration** | Code in `watcher.py` / `db.py` that stays above the adapter: `_check_stop()`, logging lambdas passed as `before_sleep`, `StopWatching` / `RuntimeError` translation |
| **bounded jitter** | `uniform(floor, wait)` — for **lock-contention** retries only; see jitter policy below |
| **stop condition** | Callable taking retry state; returns True when retrying must end |
| **wait generator** | Generator yielding successive base wait seconds before jitter |
| **vendorable** | Stdlib-only, no `simplebroker.*` imports, SPDX header, `__version__`, small `__all__` |

### Repository map (files this plan touches)

| Path | Role |
|---|---|
| `simplebroker/_retry.py` | **New.** Vendorable retry engine |
| `simplebroker/helpers.py` | Adapter family + setup retry; re-export `interruptible_sleep`; add `_execute_watcher_operational_retry`, `_execute_connection_retry`; **remove** `_retry_jitter` and inline `_execute_with_retry` loop |
| `simplebroker/watcher.py` | `_process_with_retry` → helpers adapter + local orchestration (`_check_stop`, logging, `StopWatching`); `_handle_retry` keeps outer loop, uses re-exported sleep only |
| `simplebroker/db.py` | Delete inline connection loops; call `_execute_connection_retry`; keep `RuntimeError` wrap + logging in `db.py` |
| `tests/test_retry.py` | **New.** Engine tests (real time, minimal mocking) |
| `tests/test_helpers_coverage.py` | Update jitter/backoff assertions; patch `_retry.time.monotonic` |
| `tests/test_timestamp_resilience.py` | Patch `_retry.time.monotonic` after elapsed logic moves |
| `tests/test_watcher_edge_cases.py` | Update if backoff timing assertions break |
| `tests/test_concurrency.py` | Gate — must stay green (real contention) |
| `CHANGELOG.md` | One `[Unreleased]` note (internal consolidation, bounded jitter) |

**Explicitly out of scope (YAGNI — do not touch):**

- `PollingStrategy` in `watcher.py` (idle polling, not error retry)
- `db.BrokerCore.write()` timestamp `IntegrityError` loop (different failure mode)
- Circuit breaker, rate limiter, hedging, decorators, async retry, public exports
- Postgres/Redis extension packages (no code changes)
- Adding `backon` to `pyproject.toml` dependencies
- Changing `BROKER_*` environment variables

### Toolset crash course

```bash
cd /Users/van/Developer/simplebroker

uv run pytest                                    # default suite (~25s)
uv run pytest tests/test_retry.py -v             # new engine tests
uv run pytest tests/test_helpers_coverage.py -v  # adapter tests
uv run ruff check . && uv run ruff format --check .
uv run mypy simplebroker
```

Style: ruff line length 88, `from __future__ import annotations` on new modules, full
type annotations (mypy strict), double quotes, trailing `# ~` on module end (match
neighbors). Commit messages: plain imperative ("Add vendored retry engine") — no
conventional-commit prefixes.

### Test design rules (hard requirement)

1. **`tests/test_retry.py` tests the engine in isolation** — no SQLite, no queues.
   Assert observable outcomes: return values, raised exceptions, elapsed time bands,
   attempt counts via `get_attempt_number()`.
2. **Adapter and integration tests use real databases** (`tmp_path`, real `Queue`,
   real threads) — same as existing concurrency tests.
3. **Do not mock `_execute_with_retry` or `execute_retry`** in integration tests.
   After Task 8 re-exports sleep, patch **`simplebroker.helpers.interruptible_sleep`**
   (not `simplebroker._retry.interruptible_sleep`) in adapter tests — see
   [tests/test_helpers_coverage.py:553](tests/test_helpers_coverage.py:553). Engine
   tests may inject a `sleep=` callable directly. Use `remove_backoff()` /
   `test_config()` to speed loops — but **not** in tests that assert sleep capture
   (`remove_backoff()` skips the sleep callback entirely).
4. **Elapsed-budget tests patch `_retry.time.monotonic`, not `helpers.time`.** After
   Task 6, elapsed measurement lives in `_retry.py`. The engine must import the
   `time` module and call `time.monotonic()` so tests can patch it. Update
   [tests/test_helpers_coverage.py](tests/test_helpers_coverage.py) (lines ~138,
   ~190, ~244, ~283) and
   [tests/test_timestamp_resilience.py:344](tests/test_timestamp_resilience.py:344) to
   `monkeypatch.setattr("simplebroker._retry.time.monotonic", fake_monotonic)` (Task 9).
5. **Jitter unit tests must be deterministic** — patch `random.uniform` or assert
   bounds only; never compare two independent `apply_jitter()` calls for equality.
6. **Do not assert "function X was called"** — assert behavior (message written,
   watcher recovered, sleep duration in range).
7. Prefer **one concurrent writer + one retrier** with real SQLite over inventing a
   fake `OperationalError` factory, except in pure engine unit tests.
8. Existing external contention / thundering-herd tests that use
   `tests.conftest.run_cli` are **black-box gates**. Do not edit, skip, xfail, reduce
   concurrency, loosen assertions, or alter their timeouts in this plan. If one fails,
   fix the retry implementation or add separate internal coverage.

### Reference implementations to read before coding

| File | Why |
|---|---|
| `simplebroker/_phaselock.py` | Vendorable module pattern (header, `__version__`, `__all__`, stdlib-only) |
| `simplebroker/helpers.py:37–271` | Current retry + setup code being replaced |
| `simplebroker/watcher.py:398–711` | Duplicate loops being deleted |
| `simplebroker/db.py:385–492` | Connection retry loops being deleted |
| `tests/test_helpers_coverage.py:535–605` | Jitter tests that must be rewritten |
| `tests/test_phaselock.py:1–60` | How we test a vendored module |

Optional context (do not import): `../backon/backon/_common.py` (hot loop),
`../backon/backon/_conditions.py` (stop composition), `../backon/backon/_testing.py`
(test hooks).

---

## Locked decisions (do not reopen without a red test proving failure)

### Bounded jitter, not full jitter

We **reject** backon's `full_jitter` (`uniform(0, wait)`). Empirically, a very short
guaranteed sleep helps the host clock and SQLite busy handlers advance.

**Locked formula:**

```text
DEFAULT_MIN_RETRY_SLEEP_S = 0.005   # 5 milliseconds

def apply_jitter(base_wait: float, *, floor: float = DEFAULT_MIN_RETRY_SLEEP_S) -> float:
    upper = max(floor, base_wait)
    return floor if upper <= floor else random.uniform(floor, upper)
```

Notes:

- When `base_wait < floor`, sleep exactly `floor`.
- When `base_wait >= floor`, sleep uniformly in `[floor, base_wait]`.
- Passing `jitter=None` to `execute_retry()` means **no jitter** (sleep exactly the
  base wait).

### Jitter policy by call site (locked)

| Call site | Jitter | Why |
|---|---|---|
| `_execute_with_retry` (lock/busy) | `bounded_jitter` | Thundering-herd decorrelation |
| Watcher operational (`_process_with_retry`) | `bounded_jitter` | Same contention domain |
| Watcher crash recovery (`_handle_retry`) | **none** — deterministic `2**retry_count` | Legacy pacing; not a lock storm |
| DB connection open | **none** — deterministic expo `[2, 4]` | Legacy pacing; setup/backoff semantics |

Do not apply `bounded_jitter` to connection or crash-recovery sleeps in this plan.

### `_retry.py` stays vendorable

Same rules as `_phaselock.py`:

- stdlib only
- no `from simplebroker...` imports
- no `OperationalError`, `StopException`, `BROKER_*`, queue/SQLite concepts
- MIT SPDX header + `__version__` + explicit `__all__`

### `_execute_with_retry()` signature is stable

Callers (`db.py`, `_timestamp.py`, `_runner.py` via setup, tests, examples) keep the
existing keyword parameters:

`max_retries`, `retry_delay`, `stop_event`, `max_elapsed`, `max_retry_delay`

Internal implementation delegates to `execute_retry()`. **Do not** add new parameters
to `_execute_with_retry` in this plan.

### Attempt counting semantics (must not change)

Current `_execute_with_retry` with `max_retries=10` performs **up to 10 attempts**
(first try + up to 9 backoff sleeps). Mirror this with `stop_after_attempt(10)`.

Formally: `stop_after_attempt(n)` returns True when `state.tries >= n`.

### No sleep after the final failed attempt (must not change)

Legacy checks exhaustion **after** the exception and **before** computing sleep:

- [helpers.py:136](simplebroker/helpers.py:136): `attempt < max_retries - 1`
- [db.py:428](simplebroker/db.py:428): `if attempt >= max_retries - 1: raise` before
  `wait_time`

So `max_retries=4` → **4 attempts, 3 sleeps**. The engine must **never** sleep and
then discover exhaustion on the next loop iteration. Check `stop(state)` after each
failed attempt, **before** sleep. Model this on backon `_sync.py` (`max_tries_exceeded`
before `sleep`), not a top-of-loop stop check.

### First attempt always runs (must not change)

Legacy does **not** consult `stop_event` before the first `operation()` call — only
`interruptible_sleep` observes it ([helpers.py:131–133](simplebroker/helpers.py:131)).
Do **not** check `stop()` at the top of the loop before the first attempt. A pre-set
`stop_event` must still allow one try; interruption raises `RetryInterrupted` only
during sleep.

### Watcher operational retry stays broad on `OperationalError` (but not `StopException`)

Current `_process_with_retry` retries **any** `OperationalError`, not only lock/busy
([watcher.py:427](simplebroker/watcher.py:427)). Legacy achieves this with **branch
ordering** — `except StopException` runs *before* `except OperationalError`
([watcher.py:425–427](simplebroker/watcher.py:425)). That matters because
`StopException` **subclasses** `OperationalError` ([_exceptions.py:34](simplebroker/_exceptions.py:34))
with `retryable = False` and the docstring *"A user-initiated stop must never be
retried."* A naive `isinstance(exc, OperationalError)` predicate would sleep and retry
stop signals up to 5 times before propagating — a shutdown regression.

The watcher path must **not** call `_execute_with_retry` directly (that helper is safe
only because `_is_locked_operational_error` honors `retryable=False`). Use
`execute_retry` with a predicate that retries broad `OperationalError` **except**
`retryable=False`:

```python
def _is_watcher_operational_retry(exc: Exception) -> bool:
    if not isinstance(exc, OperationalError):
        return False
    if getattr(exc, "retryable", None) is False:
        return False
    return True
```

Only the generic DB lock adapter keeps `_is_locked_operational_error` message-marker
filtering.

### Watcher `_check_stop()` stays inside the retried operation

Legacy calls `self._check_stop()` at the **start of each attempt** inside the retry
loop ([watcher.py:423](simplebroker/watcher.py:423)), *before* `process_func()`. That
raises `StopWatching` promptly when the watcher is told to stop. Do **not** rely on
first-attempt-always-runs alone — wrap the operation passed to
`_execute_watcher_operational_retry`:

```python
def _attempt() -> Any:
    self._check_stop()
    return process_func()

return _execute_watcher_operational_retry(_attempt, ...)
```

Map `StopException` from nested `_execute_with_retry` (stop during contended queue I/O)
and `RetryInterrupted` (stop during sleep) to `StopWatching` in `_process_with_retry`
as today.

### `execute_retry` catches `Exception`, not `BaseException`

Legacy connection retry catches `Exception` ([db.py:427](simplebroker/db.py:427)). The
engine must not retry `KeyboardInterrupt`, `SystemExit`, or `GeneratorExit`. Signature:
`retry_on: Callable[[Exception], bool]`, loop `except Exception as exc`.

### `before_sleep` callback for adapter logging (minimal backon seam)

`execute_retry` accepts optional `before_sleep(state, exc, sleep_seconds)` — invoked
only when `sleep_seconds > 0` (inside the sleep block, after multiplier/clamp). Watcher
and DB connection adapters use it to preserve legacy debug logs without importing broker
types into `_retry.py`. Lock-contention `_execute_with_retry` omits it (no per-attempt
debug log today). Not called under `remove_backoff()` (no phantom "Retrying in 0.000s"
lines).

### `remove_backoff()` skips sleeping entirely

When `sleep_multiplier == 0.0`, the engine **does not call** the sleep function (no
`sleep(0.0)` callback). Tests assert `sleeps == []`, not `[0.0]`. Implement
`remove_backoff()` in Task 1 (minimal working version); Task 7 extends `test_config`.

### Elapsed time measured after the failing attempt

Match [helpers.py:137](simplebroker/helpers.py:137): refresh `state.elapsed =
monotonic() - start` **after** `operation()` raises and **before** `stop(state)` and
`max_delay` clamping — not only at loop top before the attempt.

### `stop_event` is not a `Stop` condition

`stop_event` is passed only to `interruptible_sleep` (→ `RetryInterrupted`). Do **not**
add `stop_when_event_set(stop_event)` to `_build_retry_stop()` — that would skip the
final attempt or raise the wrong exception type.

### `StopException` on interrupted sleep (must not change)

When `stop_event` is set during sleep, raise `StopException("Retry interrupted by
stop event")` from the adapter — not a bare return, not a generic `RuntimeError`.

### Watcher crash recovery stays an outer restart loop

`BaseWatcher._run_with_retries` catches **any** `Exception` except `StopWatching`,
`StopException`, and `KeyboardInterrupt`, increments a counter, sleeps, and **restarts
the whole watcher loop** (initial drain + `_process_messages`). This is **not** the
same shape as `execute_retry()` (which re-invokes one operation). Do **not** wrap the
entire watcher body in `execute_retry`.

Crash recovery sleep stays **deterministic** (`wait_time = 2**retry_count`) — no
`bounded_jitter`. Keep `_handle_retry` returning `bool` and keep `_check_retry_timeout`
as-is.

### Connection open retry stays broad

`DBConnection.get_connection` retries **any** `Exception` from connection setup
(still excluding stop via `StopException`). Do not narrow without a red test proving
regression.

### `StopException` must propagate from connection open (not wrap as `RuntimeError`)

Legacy raises `StopException("Connection interrupted")` from the interrupted-sleep
branch ([db.py:442–443](simplebroker/db.py:442)) — **not** from the final-attempt
`RuntimeError` wrap. `StopException` is a shutdown signal; callers (including watcher
crash recovery) filter on it. The `db.py` wrapper around `_execute_connection_retry`
must re-raise `StopException` **before** `except Exception` — otherwise stop-during-connect
becomes `RuntimeError` and the watcher treats shutdown as a crash. Preserve
`_get_shared_connection`'s pre-loop `if self._stop_event.is_set(): raise StopException`
([db.py:460](simplebroker/db.py:460)).

---

## Invariants (each maps to a test)

| ID | Invariant | Test task |
|---|---|---|
| R1 | `_retry.py` imports only stdlib | Task 1 gate |
| R2 | `bounded_jitter(floor, wait)` always returns `>= floor` | Task 3 |
| R3 | `execute_retry` honors `stop_after_attempt` exactly | Task 6 |
| R4 | `execute_retry` clamps sleep to `max_delay` remaining budget | Task 6 |
| R5 | `stop_when_event_set` stops between attempts | Task 6 |
| R6 | `remove_backoff()` forces zero sleep in tests | Task 7 |
| R7 | `get_attempt_number()` reflects current attempt inside `operation` | Task 7 |
| R8 | Hot-loop warning logs when ≥5 retries occur within 100ms | Task 7 |
| R9 | `_execute_with_retry` retries only lock/busy `OperationalError` | Task 9 |
| R10 | `_execute_with_retry` preserves `max_retries` / `max_elapsed` semantics | Task 9 |
| R11 | Watcher operational path uses `_execute_watcher_operational_retry`; honors `retryable=False` | Task 10 |
| R17 | `StopException` raised by watcher operation is not retried (no sleeps) | Task 10 |
| R12 | Watcher crash recovery keeps deterministic `2**retry_count` sleeps (no jitter) | Task 10 |
| R13 | `DBConnection` uses adapter (no `2**(attempt+1)` loop) | Task 11 |
| R18 | Stop during connection open raises `StopException`, not `RuntimeError` | Task 11 |
| R14 | Real SQLite lock contention still succeeds via retry | Task 12 |
| R19 | Existing external contention/thundering-herd tests using `run_cli` are unchanged black-box gates | Task 13 |
| R15 | Full default pytest suite green | Task 13 |
| R16 | Pre-set `stop_event` still runs attempt 1; exhaustion does not sleep | Task 6 |

---

## Phase 0 — Baseline

### Task 0: Branch and baseline

**Files:** none modified.

- [ ] **Step 0.1: Read reference code** (30 min): skimming list in "Reference
  implementations" above.
- [ ] **Step 0.2: Create branch.**

```bash
cd /Users/van/Developer/simplebroker
git checkout -b vendored-retry main
```

- [ ] **Step 0.3: Record baseline — all must pass before edits.**

```bash
uv run pytest
uv run ruff check .
uv run mypy simplebroker
```

Record the pytest summary line (e.g. `XXXX passed`) in your PR description later.

---

## Phase 1 — Vendorable `_retry.py` skeleton

### Task 1: Module scaffold + import gate (R1)

**Files:**
- Create: `simplebroker/_retry.py`
- Create: `tests/test_retry.py`

- [ ] **Step 1.1: Write failing import test.** Create `tests/test_retry.py`:

```python
"""Tests for the vendorable retry engine (stdlib-only, no broker imports)."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import simplebroker._retry as retry_module
from simplebroker._retry import __version__

# Hardcoded in the test — do not trust attributes on the module under test.
_ALLOWED_STDLIB_ROOTS = frozenset(sys.stdlib_module_names) | {"__future__"}


def test_retry_module_is_stdlib_only() -> None:
    source = Path(retry_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                assert root in _ALLOWED_STDLIB_ROOTS, root
        elif isinstance(node, ast.ImportFrom):
            assert node.module is not None
            root = node.module.split(".")[0]
            assert root in _ALLOWED_STDLIB_ROOTS, node.module


def test_version_is_non_empty_string() -> None:
    assert isinstance(__version__, str)
    assert __version__
```

Add further imports in `tests/test_retry.py` **only when** a task adds tests that use
them (avoids ruff F401 on unused imports).

- [ ] **Step 1.2: Run — expect ImportError.**

```bash
uv run pytest tests/test_retry.py::test_retry_module_is_stdlib_only -v
```

- [ ] **Step 1.3: Create minimal `simplebroker/_retry.py`.** Full file for this task
  (stubs raise `NotImplementedError`; later tasks replace them):

```python
# Copyright (c) 2025 Van Lindberg
# SPDX-License-Identifier: MIT

"""Small sync retry engine with bounded jitter and composable stop conditions.

This module is intentionally standalone: it depends only on the Python standard
library and can be copied into another project. It does not know about databases,
queues, or application-specific exceptions.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
import threading
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from typing import Any, TypeVar

T = TypeVar("T")

__version__ = "1.0"

DEFAULT_MIN_RETRY_SLEEP_S = 0.005

_logger = logging.getLogger("simplebroker._retry")
_logger.addHandler(logging.NullHandler())

# --- public surface stubs (implemented in later tasks) ---


def interruptible_sleep(
    seconds: float,
    stop_event: threading.Event | None = None,
    *,
    chunk_size: float = 0.1,
) -> bool:
    raise NotImplementedError


def apply_jitter(
    base_wait: float,
    *,
    floor: float = DEFAULT_MIN_RETRY_SLEEP_S,
) -> float:
    raise NotImplementedError


def bounded_jitter(
    base_wait: float,
    *,
    floor: float = DEFAULT_MIN_RETRY_SLEEP_S,
) -> float:
    return apply_jitter(base_wait, floor=floor)


class Stop:
    def __call__(self, state: RetryState) -> bool:
        raise NotImplementedError

    def __or__(self, other: Stop) -> Stop:
        return stop_any(self, other)

    def __and__(self, other: Stop) -> Stop:
        return stop_all(self, other)


@dataclass
class RetryState:
    tries: int = 0
    start_time: float = 0.0
    elapsed: float = 0.0


class stop_after_attempt(Stop):
    def __init__(self, max_attempts: int) -> None:
        self.max_attempts = max_attempts

    def __call__(self, state: RetryState) -> bool:
        raise NotImplementedError


class stop_after_delay(Stop):
    def __init__(self, max_delay: float) -> None:
        self.max_delay = max_delay

    def __call__(self, state: RetryState) -> bool:
        raise NotImplementedError


class stop_when_event_set(Stop):
    def __init__(self, event: threading.Event) -> None:
        self.event = event

    def __call__(self, state: RetryState) -> bool:
        raise NotImplementedError


def stop_any(*stops: Stop) -> Stop:
    raise NotImplementedError


def stop_all(*stops: Stop) -> Stop:
    raise NotImplementedError


class stop_never(Stop):
    def __call__(self, state: RetryState) -> bool:
        return False


class Wait:
    def __init__(
        self, gen_func: Callable[..., Generator[float, Any, None]]
    ) -> None:
        self._gen_func = gen_func

    def __call__(self, **kwargs: Any) -> Generator[float, Any, None]:
        return self._gen_func(**kwargs)


def _expo(
    *,
    base: float = 2,
    factor: float = 1,
    max_value: float | None = None,
) -> Generator[float, Any, None]:
    raise NotImplementedError


expo = Wait(_expo)  # replaced in Task 4 if _expo stub remains


class RetryInterrupted(Exception):
    """Raised when a retry sleep is interrupted by stop_event."""


def execute_retry(
    operation: Callable[[], T],
    *,
    retry_on: Callable[[Exception], bool],
    wait_gen: Wait | None = None,
    wait_gen_kwargs: dict[str, Any] | None = None,
    jitter: Callable[[float], float] | None = bounded_jitter,
    stop: Stop | None = None,
    max_delay: float | None = None,
    sleep: Callable[[float, threading.Event | None], bool] | None = None,
    stop_event: threading.Event | None = None,
    before_sleep: Callable[[RetryState, Exception, float], None] | None = None,
) -> T:
    raise NotImplementedError


_retry_context: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_retry_context", default=None
)


def is_retrying() -> bool:
    return _retry_context.get() is not None


def get_attempt_number() -> int | None:
    return _retry_context.get()


_TEST_CONFIG: dict[str, float] = {"sleep_multiplier": 1.0}


@contextlib.contextmanager
def test_config(*, sleep_multiplier: float | None = None) -> Iterator[None]:
    old = _TEST_CONFIG["sleep_multiplier"]
    if sleep_multiplier is not None:
        _TEST_CONFIG["sleep_multiplier"] = sleep_multiplier
    try:
        yield
    finally:
        _TEST_CONFIG["sleep_multiplier"] = old


@contextlib.contextmanager
def remove_backoff() -> Iterator[None]:
    with test_config(sleep_multiplier=0.0):
        yield


__all__ = [
    "DEFAULT_MIN_RETRY_SLEEP_S",
    "RetryState",
    "Stop",
    "Wait",
    "__version__",
    "apply_jitter",
    "bounded_jitter",
    "execute_retry",
    "expo",
    "get_attempt_number",
    "interruptible_sleep",
    "is_retrying",
    "RetryInterrupted",
    "remove_backoff",
    "stop_after_attempt",
    "stop_after_delay",
    "stop_all",
    "stop_any",
    "stop_never",
    "stop_when_event_set",
    "test_config",
]

# ~
```

- [ ] **Step 1.4: Run import test — expect pass.**

```bash
uv run pytest tests/test_retry.py::test_retry_module_is_stdlib_only -v
uv run ruff check simplebroker/_retry.py tests/test_retry.py
uv run mypy simplebroker/_retry.py
```

- [ ] **Step 1.5: Commit.**

```bash
git add simplebroker/_retry.py tests/test_retry.py
git commit -m "Add vendored retry module scaffold"
```

---

## Phase 2 — Sleep + bounded jitter

### Task 2: `interruptible_sleep` (move implementation)

**Files:**
- Modify: `simplebroker/_retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 2.1: Red test.**

```python
import threading

def test_interruptible_sleep_returns_true_when_completed() -> None:
    assert interruptible_sleep(0.02) is True


def test_interruptible_sleep_returns_false_when_event_set() -> None:
    event = threading.Event()
    event.set()
    assert interruptible_sleep(1.0, event) is False
```

- [ ] **Step 2.2: Green — copy implementation from `helpers.py:37–87` into
  `_retry.py`**, replacing the stub. Add `import time` (omitted from Task 1 scaffold
  to avoid F401). Behavior must match exactly (including `chunk_size` default 0.1 and
  the `stop_event is None` nuance on early wake).

- [ ] **Step 2.3: Run tests + mypy.**

- [ ] **Step 2.4: Commit** ("Move interruptible_sleep into vendored retry module").

**Do not** wire `helpers.py` re-export yet — Task 8.

### Task 3: `apply_jitter` / `bounded_jitter` (R2)

**Files:**
- Modify: `simplebroker/_retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 3.1: Red tests.**

```python
def test_apply_jitter_enforces_floor() -> None:
    assert apply_jitter(0.0) == DEFAULT_MIN_RETRY_SLEEP_S
    assert apply_jitter(0.001) == DEFAULT_MIN_RETRY_SLEEP_S


def test_apply_jitter_spans_up_to_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "random.uniform",
        lambda low, high: (low + high) / 2,
    )
    assert apply_jitter(0.05) == pytest.approx(0.0275, rel=1e-9)


def test_bounded_jitter_delegates_to_apply_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[float, float]] = []

    def capture(base: float, *, floor: float = DEFAULT_MIN_RETRY_SLEEP_S) -> float:
        calls.append((base, floor))
        return 0.02

    monkeypatch.setattr("simplebroker._retry.apply_jitter", capture)
    assert bounded_jitter(0.05) == 0.02
    assert calls == [(0.05, DEFAULT_MIN_RETRY_SLEEP_S)]
```

- [ ] **Step 3.2: Green — implement `apply_jitter` per locked formula.** Add
  `import random` (omitted from Task 1 scaffold).

  The test patches stdlib `random.uniform`, not `simplebroker._retry.random.uniform`,
  so the red test fails on `apply_jitter` behavior rather than on a missing module
  attribute before Task 3 adds `import random`.

- [ ] **Step 3.3: Run + commit** ("Add bounded jitter for retry sleeps").

---

## Phase 3 — Wait generators + stop conditions

### Task 4: `expo` wait generator

**Files:**
- Modify: `simplebroker/_retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 4.1: Red test** — drive generator manually:

```python
def test_expo_yields_zero_then_exponential_values() -> None:
    gen = expo(base=2, factor=0.05, max_value=0.2)
    assert next(gen) == 0.0
    assert next(gen) == 0.05
    assert next(gen) == 0.10
    assert next(gen) == 0.20
    assert next(gen) == 0.20  # capped
```

- [ ] **Step 4.2: Green — implement `_expo` generator, then assign module-level
  `expo = Wait(_expo)`** (port minimal logic from backon `_wait_gen.py`, stdlib only).

**Locked `expo` priming model (one way only):**

1. `_init_wait_gen(gen)` calls `gen.send(None)` **once before the attempt loop** to
   consume the leading `0.0` yield. That value is **never** used as a sleep duration.
2. After each failed attempt (and only if `stop(state)` is False), the engine calls
   `base_wait = wait.send(None)` to get the next backoff base.
3. First real backoff after priming is `factor * base**0` (e.g. `retry_delay * 1` for
   `_execute_with_retry`), matching legacy `retry_delay * 2**attempt` with `attempt=0`.

Do not use the leading `0.0` as "skip first sleep" inside the loop — priming handles it.

- [ ] **Step 4.3: Commit.**

### Task 5: Stop conditions (R3–R5)

**Files:**
- Modify: `simplebroker/_retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 5.1: Implement `stop_never` (always returns False).**

- [ ] **Step 5.2: Red tests** for each stop type and composition:

```python
def test_stop_after_attempt_limits_tries() -> None:
    state = RetryState(tries=10, elapsed=0.0)
    assert stop_after_attempt(10)(state) is True
    state.tries = 9
    assert stop_after_attempt(10)(state) is False


def test_stop_after_delay() -> None:
    state = RetryState(tries=1, elapsed=31.0)
    assert stop_after_delay(30.0)(state) is True


def test_stop_when_event_set() -> None:
    event = threading.Event()
    assert stop_when_event_set(event)(RetryState()) is False
    event.set()
    assert stop_when_event_set(event)(RetryState()) is True


def test_stop_any_and_all() -> None:
    a = stop_after_attempt(2)
    b = stop_after_delay(0.01)
    state = RetryState(tries=2, elapsed=0.0)
    assert (a | b)(state) is True
    state.tries = 1
    assert (a & b)(state) is False
```

- [ ] **Step 5.3: Green — implement `stop_all`, `stop_any`, and concrete stops.**

- [ ] **Step 5.4: Commit.**

---

## Phase 4 — Core loop

### Task 6: `execute_retry` (R3–R5)

**Files:**
- Modify: `simplebroker/_retry.py`
- Modify: `tests/test_retry.py`

**Behavior spec (read carefully — matches legacy sleep counts):**

```text
if stop is None:
    stop = stop_never()
if sleep is None:
    sleep = interruptible_sleep
last_exc = None
wait = _init_wait_gen(wait_gen, wait_gen_kwargs)   # consumes leading 0.0
start = time.monotonic()
state = RetryState(start_time=start)

while True:
    state.tries += 1
    with _attempt_context(state.tries):            # contextvar; reset in finally
        try:
            return operation()
        except Exception as exc:
            last_exc = exc
            if not retry_on(exc):
                raise
            state.elapsed = time.monotonic() - start  # after failure, like helpers.py
            if stop(state):                        # BEFORE sleep — no final sleep
                raise last_exc
            base_wait = wait.send(None)
            sleep_seconds = jitter(base_wait) if jitter else base_wait
            if max_delay is not None:
                remaining = max_delay - state.elapsed
                if remaining <= 0:
                    raise last_exc
                sleep_seconds = min(sleep_seconds, remaining)
            sleep_seconds *= _TEST_CONFIG["sleep_multiplier"]
            if sleep_seconds > 0:
                if before_sleep is not None:
                    before_sleep(state, last_exc, sleep_seconds)
                _check_hot_loop()
                if not sleep_fn(sleep_seconds, stop_event):
                    raise RetryInterrupted
            continue
```

Optional `before_sleep(state, exc, sleep_seconds)` — invoked only when `sleep_seconds > 0`
(immediately before the sleep call). Minimal backon-style seam for adapter logging without
bloating the engine. Watcher and connection adapters pass retry debug logs here.

`retry_on(exc)` returns **bool only** in this plan (`True` → retry, `False` → re-raise).
Do not implement the `float` override path (YAGNI — defer to a future plan).

`RetryInterrupted` — raised when `interruptible_sleep` returns `False` because
`stop_event` was set. Adapter maps → `StopException`. When `stop(state)` fires, re-raise
`last_exc` (same type as legacy exhaustion).

`max_delay: float | None` — elapsed wall-clock budget in seconds from loop start (not
monotonic deadline). Adapter passes `max_elapsed` here. Engine clamps each sleep; also
raises `last_exc` if `remaining <= 0` before sleeping. Works alongside `stop_after_delay`
in composed `stop` — both must agree.

- [ ] **Step 6.1: Red tests** (`remove_backoff` already works from Task 1 scaffold):

```python
def test_execute_retry_succeeds_on_first_try() -> None:
    assert execute_retry(lambda: 42, retry_on=lambda e: False) == 42


def test_execute_retry_retries_then_succeeds() -> None:
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("fail")
        return "ok"

    with remove_backoff():
        assert (
            execute_retry(
                flaky,
                retry_on=lambda e: isinstance(e, ValueError),
                stop=stop_after_attempt(5),
            )
            == "ok"
        )
    assert calls["n"] == 3


def test_execute_retry_gives_up_after_max_attempts() -> None:
    def always_fail() -> None:
        raise RuntimeError("nope")

    with remove_backoff():
        with pytest.raises(RuntimeError, match="nope"):
            execute_retry(
                always_fail,
                retry_on=lambda e: True,
                stop=stop_after_attempt(3),
            )


def test_execute_retry_does_not_sleep_after_final_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise RuntimeError("nope")

    # Do NOT use remove_backoff() here — it skips the sleep callback entirely.
    with pytest.raises(RuntimeError):
        execute_retry(
            fail,
            retry_on=lambda e: True,
            stop=stop_after_attempt(4),
            sleep=capture,
        )
    assert len(sleeps) == 3  # 4 attempts, 3 sleeps — matches legacy


def test_execute_retry_runs_once_even_if_stop_event_preset() -> None:
    event = threading.Event()
    event.set()
    calls = {"n": 0}

    def once() -> int:
        calls["n"] += 1
        return 7

    assert execute_retry(once, retry_on=lambda e: False, stop_event=event) == 7
    assert calls["n"] == 1


def test_execute_retry_does_not_retry_base_exceptions() -> None:
    calls = {"n": 0}

    def exit_op() -> None:
        calls["n"] += 1
        raise SystemExit(1)

    with pytest.raises(SystemExit):
        execute_retry(
            exit_op,
            retry_on=lambda e: True,
            stop=stop_after_attempt(5),
        )
    assert calls["n"] == 1  # SystemExit is not an Exception — no retry


def test_execute_retry_coerces_stop_none_to_never() -> None:
    calls = {"n": 0}

    def flaky() -> int:
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("x")
        return 1

    with remove_backoff():
        assert execute_retry(flaky, retry_on=lambda e: True, stop=None) == 1
```

- [ ] **Step 6.2: Green — implement loop + `_init_wait_gen` + `_attempt_context`
  contextmanager** (prime generator before loop; no top-of-loop stop check). Use
  module import `import time` and call `time.monotonic()`; tests patch
  `simplebroker._retry.time.monotonic`.

- [ ] **Step 6.3: Add elapsed-budget tests (non-zero sleeps):**

```python
def test_execute_retry_clamps_sleep_to_max_delay_remaining(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise OSError("locked")

    # expo factor=1 → first backoff 1.0s; max_delay=0.3 forces clamp (must bind)
    with pytest.raises(OSError):
        execute_retry(
            fail,
            retry_on=lambda e: True,
            wait_gen_kwargs={"base": 2, "factor": 1.0},
            jitter=None,
            stop=stop_after_attempt(2),
            max_delay=0.3,
            sleep=capture,
        )
    assert sleeps == [0.3]  # min(1.0, remaining=0.3) — fails if clamp deleted


def test_execute_retry_honors_delay_budget_via_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    calls = 0

    def fake_monotonic() -> float:
        return monotonic_time

    def locked() -> None:
        nonlocal calls, monotonic_time
        calls += 1
        monotonic_time += 0.03
        raise OSError("locked")

    monkeypatch.setattr("simplebroker._retry.time.monotonic", fake_monotonic)

    with remove_backoff():
        with pytest.raises(OSError):
            execute_retry(
                locked,
                retry_on=lambda e: True,
                stop=stop_after_delay(0.05),
            )
    assert calls == 2
```

- [ ] **Step 6.4: Commit.**

### Task 7: Test hooks + observability (R6–R8)

**Files:**
- Modify: `simplebroker/_retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 7.1: Wire `test_config` / `remove_backoff` into the loop** (scaffold from
  Task 1 already defines both; Task 7 ensures `execute_retry` honors
  `sleep_multiplier` and skips sleep when multiplier is `0.0`):
  - `remove_backoff()` sets `sleep_multiplier=0.0` → **no sleep callback** (not
    `sleep(0.0)`)
  - `execute_retry` multiplies sleep seconds by `sleep_multiplier` (default 1.0)

- [ ] **Step 7.2: Implement `is_retrying` / `get_attempt_number`** via
  `_attempt_context` contextmanager:

```python
@contextlib.contextmanager
def _attempt_context(attempt_number: int) -> Iterator[None]:
    token = _retry_context.set(attempt_number)
    try:
        yield
    finally:
        _retry_context.reset(token)
```

Call it around `operation()` only (see Task 6 spec). **Must** reset in `finally` so
`get_attempt_number()` is `None` after `execute_retry` returns.

- [ ] **Step 7.3: Port hot-loop detection** from backon `_common.py:_check_hot_loop`
  (≥5 retries within 100ms → warning log). Call before each sleep when
  `sleep_seconds > 0`. **Note:** like backon, the detector uses **process-global**
  state (lock-guarded) — unrelated retries across threads within 100ms can trip one
  warning. Log-only; acceptable for this plan.

- [ ] **Step 7.4: Red/green tests:**

```python
def test_get_attempt_number_inside_operation() -> None:
    seen: list[int | None] = []

    def record() -> None:
        seen.append(get_attempt_number())
        if len(seen) < 2:
            raise ValueError("again")

    with remove_backoff():
        execute_retry(
            record,
            retry_on=lambda e: isinstance(e, ValueError),
            stop=stop_after_attempt(5),
        )
    assert seen == [1, 2]


def test_remove_backoff_zeroes_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def fake_sleep(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise ValueError("x")

    with remove_backoff():
        with pytest.raises(ValueError):
            execute_retry(
                fail,
                retry_on=lambda e: True,
                stop=stop_after_attempt(2),
                sleep=fake_sleep,
            )
    assert sleeps == []  # multiplier 0 → skip sleep entirely


def test_hot_loop_warning_logs_after_rapid_retries(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    caplog.set_level(logging.WARNING, logger="simplebroker._retry")
    sleeps: list[float] = []

    def fast_sleep(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise ValueError("again")

    with pytest.raises(ValueError):
        execute_retry(
            fail,
            retry_on=lambda e: True,
            stop=stop_after_attempt(7),
            sleep=fast_sleep,  # no wall delay — monotonic gaps < 100ms
            jitter=None,
            wait_gen_kwargs={"base": 2, "factor": 0.01},
        )
    assert len(sleeps) >= 5
    # Timing-coupled: assumes no-delay sleeps complete within 100ms window.
    # Match the literal substring from ported _check_hot_loop (backon: "Hot loop detected").
    assert any("Hot loop" in record.message for record in caplog.records)
```

- [ ] **Step 7.5: Commit.**

---

## Phase 5 — Adapter in `helpers.py`

### Task 8: Re-export sleep + build stop builder

**Files:**
- Modify: `simplebroker/helpers.py`
- Modify: `tests/test_retry.py` (optional import check)

- [ ] **Step 8.1: In `helpers.py`, replace `interruptible_sleep` body with:**

```python
from ._retry import interruptible_sleep as interruptible_sleep
```

Remove the old implementation (delete lines, do not duplicate).

- [ ] **Step 8.2: Add private helper `_build_retry_stop(...)`** in `helpers.py`:

```python
def _build_retry_stop(
    *,
    max_retries: int | None,
    max_elapsed: float | None,
) -> Stop:
    stops: list[Stop] = []
    if max_retries is not None:
        stops.append(stop_after_attempt(max_retries))
    if max_elapsed is not None:
        stops.append(stop_after_delay(max_elapsed))
    if not stops:
        return stop_never()
    if len(stops) == 1:
        return stops[0]
    return stop_any(*stops)
```

Import from `_retry` (illustrative — adapters need the full set; mypy will enforce):

```python
from ._retry import (
    RetryInterrupted,
    RetryState,
    Stop,
    bounded_jitter,
    execute_retry,
    expo,
    interruptible_sleep,
    stop_after_attempt,
    stop_after_delay,
    stop_any,
    stop_never,
)
```

**`stop_event` is not a `Stop`** — pass it only to `execute_retry(stop_event=...)`.

- [ ] **Step 8.3: Run** `uv run pytest tests/test_helpers_coverage.py -k interruptible`
  (if any) **and** `uv run mypy simplebroker/helpers.py`.

- [ ] **Step 8.4: Commit.**

### Task 9: Rewrite `_execute_with_retry` (R9–R10)

**Files:**
- Modify: `simplebroker/helpers.py`
- Modify: `tests/test_helpers_coverage.py`
- Modify: `tests/test_timestamp_resilience.py` (monotonic patch target)

- [ ] **Step 9.1: Update red tests** — replace `test_retry_jitter_is_random_per_call`
  and `test_execute_with_retry_jitter_decorrelates_rapid_retries`:

**Delete** `_retry_jitter` tests. **Add:**

```python
def test_execute_with_retry_uses_bounded_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture_sleep(wait: float, stop_event=None) -> bool:
        sleeps.append(wait)
        return True

    monkeypatch.setattr("simplebroker.helpers.interruptible_sleep", capture_sleep)
    jitter_values = iter([0.006, 0.015, 0.03])

    def fake_uniform(low: float, high: float) -> float:
        value = next(jitter_values)
        assert low == DEFAULT_MIN_RETRY_SLEEP_S
        assert low <= value <= high
        return value

    monkeypatch.setattr("random.uniform", fake_uniform)

    def locked() -> None:
        raise OperationalError("database is locked")

    with pytest.raises(OperationalError):
        _execute_with_retry(locked, max_retries=4, retry_delay=0.01)

    assert len(sleeps) == 3
    assert sleeps == pytest.approx([0.006, 0.015, 0.03], rel=1e-9)
```

Import `DEFAULT_MIN_RETRY_SLEEP_S` from `simplebroker._retry`.

- [ ] **Step 9.2: Green — rewrite `_execute_with_retry`:**

```python
def _execute_with_retry(operation, *, max_retries=10, retry_delay=0.05, ...):
    if max_retries is None and max_elapsed is None:
        raise ValueError("max_retries=None requires max_elapsed")

    def retry_on(exc: Exception) -> bool:
        if not isinstance(exc, OperationalError):
            return False
        return _is_locked_operational_error(exc)

    try:
        return execute_retry(
            operation,
            retry_on=retry_on,
            wait_gen=expo,
            wait_gen_kwargs={"base": 2, "factor": retry_delay, "max_value": max_retry_delay},
            jitter=bounded_jitter,
            stop=_build_retry_stop(
                max_retries=max_retries,
                max_elapsed=max_elapsed,
            ),
            max_delay=max_elapsed,
            sleep=interruptible_sleep,
            stop_event=stop_event,
        )
    except RetryInterrupted:  # engine-local; map below
        raise StopException("Retry interrupted by stop event") from None
```

**Implementation note:** expose a `RetryInterrupted` exception in `_retry.py` when
`interruptible_sleep` returns `False` and `stop_event` was provided. The adapter maps
it to `StopException`.

- [ ] **Step 9.3: Delete `_retry_jitter`** entirely.

- [ ] **Step 9.4: Update elapsed-budget monkeypatch targets** in
  `tests/test_helpers_coverage.py` and `tests/test_timestamp_resilience.py`: change
  `monkeypatch.setattr(helpers.time, "monotonic", ...)` →
  `monkeypatch.setattr("simplebroker._retry.time.monotonic", ...)`.

- [ ] **Step 9.5: Run adapter tests:**

```bash
uv run pytest tests/test_helpers_coverage.py tests/test_timestamp_resilience.py -v
```

All existing `_execute_with_retry` tests must pass (update assertions only where they
encoded additive 0–25ms jitter).

- [ ] **Step 9.6: Commit.**


---

## Phase 6 — Collapse duplicate call sites

### Task 10: Watcher retries (R11–R12)

**Files:**
- Modify: `simplebroker/watcher.py`
- Modify: `tests/test_watcher_edge_cases.py`

Two separate changes — do not conflate them:

| Path | Mechanism |
|---|---|
| Operational DB errors (`_process_with_retry`) | `_execute_watcher_operational_retry` (broad `OperationalError`, `retryable=False` excluded) |
| Crash recovery (`_handle_retry`) | Keep outer-loop structure; **no jitter** on sleep |

- [ ] **Step 10.1: Add `helpers._is_watcher_operational_retry` and
  `_execute_watcher_operational_retry`**, then refactor `_process_with_retry` to call
  it. Delete `_calculate_retry_wait_time`. Remove import of `_retry_jitter`. **Do not**
  call `_execute_with_retry` here — that helper narrows to lock/busy via message markers.
  Remove `tests/test_watcher_edge_cases.py::test_calculate_retry_wait_time_jitter_decorrelates`
  at the same time; it targets the deleted private method. Jitter coverage now lives in
  `tests/test_retry.py` and the `_execute_with_retry` adapter test.

```python
def _is_watcher_operational_retry(exc: Exception) -> bool:
    if not isinstance(exc, OperationalError):
        return False
    if getattr(exc, "retryable", None) is False:
        return False
    return True


def _execute_watcher_operational_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 5,
    retry_delay: float = 0.05,
    stop_event: threading.Event | None = None,
    before_sleep: Callable[[RetryState, Exception, float], None] | None = None,
) -> T:
    try:
        return execute_retry(
            operation,
            retry_on=_is_watcher_operational_retry,
            wait_gen=expo,
            wait_gen_kwargs={"base": 2, "factor": retry_delay},
            jitter=bounded_jitter,
            stop=stop_after_attempt(max_retries),
            sleep=interruptible_sleep,
            stop_event=stop_event,
            before_sleep=before_sleep,
        )
    except RetryInterrupted:
        raise StopException("Retry interrupted by stop event") from None
```

In `_process_with_retry`, wrap the operation so `_check_stop()` runs each attempt
(locked decision above). Preserve retry debug logging and final failure logging via
`before_sleep` + outer `except OperationalError` ([watcher.py:428–433](simplebroker/watcher.py:428)):

```python
def _attempt() -> Any:
    self._check_stop()
    return process_func()

def _log_retry(_state: RetryState, exc: Exception, wait: float) -> None:
    if config["BROKER_LOGGING_ENABLED"]:
        logger.debug(
            f"OperationalError during {operation_name} "
            f"(retry {_state.tries}/{max_retries}): {exc}. "
            f"Retrying in {wait:.3f} seconds...",
        )

try:
    return _execute_watcher_operational_retry(
        _attempt,
        max_retries=max_retries,
        retry_delay=0.05,
        stop_event=self._stop_event,
        before_sleep=_log_retry,
    )
except StopException:
    raise StopWatching from None
except OperationalError as e:
    if config["BROKER_LOGGING_ENABLED"]:
        logger.exception(
            f"Failed after {max_retries} operational errors: {e}",
        )
    raise
```

Preserve:
  - `max_retries=5`, `retry_delay=0.05`
  - `StopException` → `StopWatching` (including from nested queue I/O)
  - Per-attempt debug log + final `logger.exception` on exhaustion

- [ ] **Step 10.1b: Red test — `StopException` is not retried:**

```python
def test_watcher_operational_retry_does_not_retry_stop_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture(wait: float, stop_event=None) -> bool:
        sleeps.append(wait)
        return True

    monkeypatch.setattr("simplebroker.helpers.interruptible_sleep", capture)

    def fail() -> None:
        raise StopException("stop during operation")

    with pytest.raises(StopException):
        _execute_watcher_operational_retry(fail, max_retries=5)

    assert sleeps == []
```

- [ ] **Step 10.2: `_handle_retry` — no sleep math change.** Keep
  `wait_time = 2**retry_count` (deterministic, no `bounded_jitter`). Keep
  `interruptible_sleep(wait_time, self._stop_event)` and the `bool` return. Keep
  `_check_retry_timeout` in `_run_with_retries` unchanged.

- [ ] **Step 10.3: Red test** (real watcher, no mock) — extend
  `tests/test_watcher_edge_cases.py::test_watcher_retry_with_exponential_backoff`:
  assert recovery still happens after injected failures. Do **not** assert
  `DEFAULT_MIN_RETRY_SLEEP_S` on crash-recovery sleeps — those remain `2**n` seconds.
  Keep the existing crash-recovery assertion `retry_sleeps[:2] == [2, 4]`.

- [ ] **Step 10.4: Run:**

```bash
uv run pytest tests/test_watcher_edge_cases.py tests/test_watcher.py -v
```

- [ ] **Step 10.5: Commit.**

### Task 11: `DBConnection` retries (R13)

**Files:**
- Modify: `simplebroker/db.py`
- Modify: `tests/test_queue_connection_manager.py` (or nearest existing test file)

- [ ] **Step 11.1: Add `helpers._execute_connection_retry`:**

```python
def _execute_connection_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 3,
    stop_event: threading.Event | None = None,
    before_sleep: Callable[[RetryState, Exception, float], None] | None = None,
) -> T:
    def retry_on(exc: Exception) -> bool:
        # StopException only — KeyboardInterrupt is BaseException; engine never
        # delivers it to retry_on (see locked Exception-only catch).
        return not isinstance(exc, StopException)

    try:
        return execute_retry(
            operation,
            retry_on=retry_on,
            wait_gen=expo,
            wait_gen_kwargs={"base": 2, "factor": 2.0, "max_value": None},
            jitter=None,  # deterministic [2, 4] — no bounded jitter on connection open
            stop=stop_after_attempt(max_retries),
            sleep=interruptible_sleep,
            stop_event=stop_event,
            before_sleep=before_sleep,
        )
    except RetryInterrupted:
        raise StopException("Connection interrupted") from None
```

**Locked backoff note (verify against [db.py:414–445](simplebroker/db.py:414)):**

| Legacy `attempt` index | Outcome | Sleep before next try |
|---|---|---|
| 0 | fail | `2**(0+1)` = **2s** |
| 1 | fail | `2**(1+1)` = **4s** |
| 2 | fail | **raise** — no sleep |

Three attempts, **two sleeps** (`[2.0, 4.0]` seconds exactly — no jitter). There is
**no** 8s sleep in legacy — attempt 2 raises at line 428 before `wait_time` is computed.

With `expo(base=2, factor=2)` after priming and `jitter=None`: `wait.send` yields
`2.0`, then `4.0`. `stop_after_attempt(3)` + pre-sleep stop check → 3 attempts, 2
sleeps. **Add test:**

```python
def test_connection_retry_sleep_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture(wait: float, stop_event=None) -> bool:
        sleeps.append(wait)
        return True

    monkeypatch.setattr("simplebroker.helpers.interruptible_sleep", capture)

    def fail() -> None:
        raise RuntimeError("connection failed")

    with pytest.raises(RuntimeError):
        _execute_connection_retry(fail, max_retries=3)

    assert sleeps == [2.0, 4.0]


def test_connection_stop_during_sleep_raises_stop_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()

    def interrupt_sleep(_wait: float, _ev: threading.Event | None) -> bool:
        return False  # simulates stop_event set during backoff

    monkeypatch.setattr("simplebroker.helpers.interruptible_sleep", interrupt_sleep)

    def fail() -> None:
        raise RuntimeError("connection failed")

    with pytest.raises(StopException, match="Connection interrupted"):
        _execute_connection_retry(fail, max_retries=3, stop_event=stop_event)
```

- [ ] **Step 11.2: Replace both loops in `DBConnection.get_connection` and
  `_get_shared_connection`**. Keep logging and `RuntimeError` wrapping in `db.py` —
  the helper only owns the retry loop. **`StopException` must re-raise before the
  generic handler** (locked decision above). Preserve `_get_shared_connection`'s
  pre-loop stop check ([db.py:460](simplebroker/db.py:460)):

```python
def _open() -> BrokerConnection:
    connection = self._create_managed_connection()  # or shared-session body
    with self._registry_lock:
        self._connection_registry.add(connection)
    self._thread_local.db = connection
    return connection

max_retries = 3

def _log_connection_retry(state: RetryState, exc: Exception, wait: float) -> None:
    if config["BROKER_LOGGING_ENABLED"]:
        logger.debug(
            f"Database connection error (retry {state.tries}/{max_retries}): {exc}. "
            f"Retrying in {wait} seconds..."
        )

try:
    return _execute_connection_retry(
        _open,
        stop_event=self._stop_event,
        before_sleep=_log_connection_retry,
    )
except StopException:
    raise  # shutdown — must not become RuntimeError
except Exception as e:
    if config["BROKER_LOGGING_ENABLED"]:
        logger.exception(
            f"Failed to get database connection after {max_retries} retries: {e}"
        )
    raise RuntimeError(f"Failed to get database connection: {e}") from e
```

For `_get_shared_connection`, keep `if self._stop_event.is_set(): raise StopException(...)`
before the same try/except pattern.

- [ ] **Step 11.3: Run existing connection tests:**

```bash
uv run pytest tests/test_queue_connection_manager.py -v
```

- [ ] **Step 11.4: Commit.**

---

## Phase 7 — Integration gates

### Task 12: Real contention test (R14)

**Files:**
- Modify: `tests/test_retry.py` **or** `tests/test_concurrency.py` (prefer extending an
  existing concurrency test if one already covers lock retry)

- [ ] **Step 12.1: Add test** `test_execute_with_retry_survives_real_sqlite_lock`:

Pattern (no mocks) — exercise the **public** `Queue.write` path, not a raw
`sqlite3` helper:

```python
def test_execute_with_retry_survives_real_sqlite_lock(tmp_path: Path) -> None:
    from simplebroker import Queue

    db_path = tmp_path / ".broker.db"
    lock_ready = threading.Event()
    holder_errors: list[BaseException] = []

    def hold_lock() -> None:
        try:
            # Create and use the SQLite connection in the same thread
            # (sqlite3 defaults check_same_thread=True).
            holder = sqlite3.connect(db_path, timeout=5.0)
            try:
                holder.execute("BEGIN IMMEDIATE")
                lock_ready.set()
                time.sleep(0.5)  # keep transaction open while retrier starts
                holder.commit()
            finally:
                holder.close()
        except BaseException as exc:
            holder_errors.append(exc)
            lock_ready.set()

    holder_thread = threading.Thread(target=hold_lock)
    holder_thread.start()
    assert lock_ready.wait(timeout=2.0)
    assert holder_errors == []

    # Retrier: busy_timeout=0 so SQLite surfaces "database is locked" to Python
    queue = Queue(
        "lock-test",
        db_path=str(db_path),
        config={"BROKER_BUSY_TIMEOUT": 0},
    )
    try:
        queue.write("payload")  # should succeed via _execute_with_retry
    finally:
        queue.close()
        holder_thread.join(timeout=2.0)

    assert not holder_thread.is_alive()
    assert holder_errors == []
```

Use `threading.Event` so the holder has `BEGIN IMMEDIATE` before the retrier starts.
Create, use, commit, and close the holder's SQLite connection inside the holder thread;
do not pass a default `sqlite3.Connection` across threads.
The **holder** keeps a normal busy timeout (`sqlite3.connect(..., timeout=5.0)`); only
the retrier uses `BROKER_BUSY_TIMEOUT=0` via `Queue(..., config=...)`.

Assert the write succeeds within a reasonable timeout. Use `remove_backoff()` only if
the test would otherwise take >2s; document why in a one-line comment.

- [ ] **Step 12.2: Commit.**

### Task 13: Full suite + lint + changelog (R15)

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 13.1: Run full gates:**

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy simplebroker
```

Before the final commit, confirm `git diff -- tests` does **not** modify any
pre-existing external contention / thundering-herd test that uses
`tests.conftest.run_cli`. Those tests are R19 black-box gates; do not "stabilize" this
work by weakening them.

- [ ] **Step 13.2: Add CHANGELOG `[Unreleased]` entry:**

```markdown
### Changed
- Consolidated internal retry/backoff logic into vendorable `simplebroker._retry`
  module. Lock-contention retries (helpers + watcher operational) now use bounded
  jitter (uniform sleep between 5ms and the computed backoff cap) instead of additive
  0–25ms jitter. Connection open and watcher crash-recovery pacing unchanged
  (deterministic expo sleeps).
```

- [ ] **Step 13.3: Final commit** ("Consolidate retry loops behind vendored engine").

---

## Appendix A — `execute_retry` public contract (locked)

```python
def execute_retry(
    operation: Callable[[], T],
    *,
    retry_on: Callable[[Exception], bool],
    wait_gen: Wait | None = None,
    wait_gen_kwargs: dict[str, Any] | None = None,
    jitter: Callable[[float], float] | None = bounded_jitter,
    stop: Stop | None = None,
    max_delay: float | None = None,
    sleep: Callable[[float, threading.Event | None], bool] | None = None,
    stop_event: threading.Event | None = None,
    before_sleep: Callable[[RetryState, Exception, float], None] | None = None,
) -> T:
    ...
```

Defaults when omitted (coerced at loop entry — do not call `stop(state)` on `None`):

- `wait_gen`: `expo` (`base=2`, `factor=1.0`)
- `stop`: `None` → `stop_never()` inside `execute_retry`
- `max_delay`: `None` (no wall-clock clamp beyond `stop`)
- `sleep`: `None` → `interruptible_sleep`

**Give-up behavior:** re-raise the last caught exception. There is no `RetryError` type.

---

## Appendix B — Stop / adapter mapping

| Caller | `max_retries` | `max_elapsed` | `stop_event` | `retry_on` |
|---|---|---|---|---|
| `_execute_with_retry` | kwarg (default 10) | kwarg | kwarg | lock/busy `OperationalError` |
| `execute_setup_with_retry` | `None` | `SETUP_RETRY_MAX_ELAPSED` or budget | kwarg | via `_execute_with_retry` |
| Watcher operational | 5 | — | watcher event | broad `OperationalError`; `retryable=False` excluded (`StopException`) |
| Watcher recovery (`_handle_retry`) | 3 | `MAX_TOTAL_RETRY_TIME` via `_check_retry_timeout` | watcher event | **not `execute_retry`** — outer loop in `watcher.py`; deterministic `2**n` sleeps |
| DB connection | 3 | — | conn event (sleep only) | any `Exception` except `StopException`; **2 sleeps** `[2.0, 4.0]` |

Rows above the recovery line go through `helpers` adapters → `execute_retry`. Recovery
orchestration stays in `watcher.py`; only `interruptible_sleep` is shared from helpers.

---

## Appendix C — Common mistakes (read before marking done)

1. **Import cycle:** `_retry.py` must not import `helpers.py`. One-way: `helpers` →
   `_retry` only.
2. **Exporting `_retry` publicly:** do not add to `simplebroker.__init__` or `ext.py`.
3. **Mocking in concurrency tests:** use real SQLite + threads.
4. **Narrowing connection retry** to `OperationalError` only — breaks transient setup
   races covered today.
5. **Reintroducing `_retry_jitter`** anywhere — delete, don't wrap.
6. **Full jitter (`uniform(0, wait)`)** — explicitly rejected by this plan.
7. **Growing `_retry.py` past ~650 lines** — stop and split (e.g. circuit breaker is a
   separate future plan).
8. **Top-of-loop `stop()` check** — skips the first attempt when `stop_event` is
   pre-set; legacy always tries once.
9. **Sleep after final failed attempt** — adds spurious delay (e.g. 25s on last
   `_execute_with_retry` give-up).
10. **`stop_when_event_set` in `_build_retry_stop`** — wrong exception type; use
    `stop_event` → `interruptible_sleep` only.
11. **Routing watcher operational through `_execute_with_retry`** — narrows retry to
    lock/busy only; use `_execute_watcher_operational_retry` instead.
12. **`bounded_jitter` on connection or crash-recovery sleeps** — locked policy says
    `jitter=None` / deterministic `2**n`; contention jitter is helpers + watcher
    operational only.
13. **`isinstance(exc, OperationalError)` for watcher retry** — `StopException`
    subclasses `OperationalError`; use `_is_watcher_operational_retry` (honors
    `retryable=False`) or branch ordering is lost.
14. **Omitting `_check_stop()` inside the retried operation** — watcher would process
    one batch after stop is requested; keep `_check_stop()` at the start of each
    attempt wrapper.
15. **`stop=None` without coercion** — `stop(state)` raises `TypeError`; coerce to
    `stop_never()` at loop entry.
16. **`remove_backoff()` in sleep-count tests** — skips the sleep callback; use an
    explicit `sleep=capture` without `remove_backoff()`.
17. **Comparing two `apply_jitter()` calls for equality** — independent random draws;
    patch `random.uniform` or delegate via monkeypatch.
18. **`RetryInterrupted` leaking from connection helper** — map to
    `StopException("Connection interrupted")` per [db.py:442](simplebroker/db.py:442).
19. **`except Exception` swallowing `StopException` in `db.py`** — re-raise
    `StopException` before the `RuntimeError` wrap; shutdown is not a connection failure.
20. **Wrapping watcher crash recovery in `execute_retry`** — wrong shape (restarts whole
    loop, not one operation); keep outer loop in `watcher.py`, sleep via helpers
    re-export only.
21. **Putting broker logging inside `_retry.py`** — pass `before_sleep` from watcher/db;
    engine stays vendorable.
22. **Changing external `run_cli` contention/thundering-herd tests** — those are
    black-box compatibility gates. Do not weaken, skip, xfail, reduce concurrency, or
    change timeouts to make this refactor pass.

---

## Plan review log (editorial)

| Review pass | Issue found | Fix applied |
|---|---|---|
| 1 | `StopException` in engine would break vendorable rule | Added engine-local `RetryInterrupted`; adapter maps to `StopException` |
| 1 | Attempt-count semantics ambiguous | Locked `stop_after_attempt(n)` == n tries total |
| 1 | Connection backoff `2**(attempt+1)` mapping unclear | Locked `expo(base=2, factor=2)` with explicit attempt table in Task 11 |
| 1 | `interruptible_sleep` move breaks imports | Task 8 re-exports from `helpers`; watcher keeps `from .helpers import interruptible_sleep` |
| 2 | Watcher recovery wrongly specified as `execute_retry` | Rewrote Task 10: outer loop unchanged (agent-2: no jitter on crash recovery) |
| 2 | Engine test importing `_retry_jitter` | Removed; tests target `apply_jitter` / bounded jitter |
| 2 | `expo` API shape ambiguous | Documented generator priming + `Wait` wrapper in Task 4/6 |
| 3 | `stop_never()` needed for generic engine default | Added to Task 5/8 imports |
| 3 | `expo` callable vs generator ambiguity | Locked as `expo = Wait(_expo)` module singleton |
| 3 | `RetryInterrupted` missing from scaffold | Added to `_retry.py` + `__all__` in Task 6 |
| 3 | Timestamp write loop looked in-scope | Listed explicitly in YAGNI out-of-scope |
| 4 (agent-1) | Loop slept after final attempt; contradicted tests | Rewrote Task 6: post-attempt/pre-sleep `stop()` only |
| 4 (agent-1) | Task 11 claimed `[2,4,8]` sleeps; legacy is `[2,4]` | Fixed table + `sleeps == [2.0, 4.0]` test |
| 4 (agent-1) | R4 clamping had no mechanism | Added `max_delay` param + non-zero clamp test |
| 4 (agent-1) | `RetryError` undefined; top-of-loop stop wrong | Removed `RetryError`; first attempt always runs |
| 4 (agent-1) | `stop_event` in `_build_retry_stop` | Removed; sleep-only via `RetryInterrupted` |
| 4 (agent-1) | `expo` priming ambiguous | Single priming model in Task 4/6 |
| 4 (agent-1) | contextvar leak | `_attempt_context` with `reset` in `finally` |
| 4 (agent-1) | Task 12 vacuous with busy handler | Retrier uses `busy_timeout=0` |
| 4 (agent-1) | `retry_on` float under-specified | Deferred (bool only this plan) |
| 4 (agent-1) | Charter: consolidation vs jitter-only | Added "Why consolidation" + fallback path |
| 5 (agent-2) | Task 1 ruff — unused imports in scaffold/tests | Minimal Task 1 test; add imports per task |
| 5 (agent-2) | `remove_backoff` used before Task 7 | Working `remove_backoff`/`test_config` in Task 1 scaffold |
| 5 (agent-2) | `remove_backoff` vs `sleeps == [0.0]` | Locked: multiplier 0 skips sleep; Task 7.4 asserts `[]` |
| 5 (agent-2) | Wrong monkeypatch target after re-export | Patch `helpers.interruptible_sleep`, not `_retry` |
| 5 (agent-2) | Elapsed measured before operation | Refresh `state.elapsed` after exception, before stop/clamp |
| 5 (agent-2) | `BaseException` vs `Exception` | Engine and adapters catch `Exception` only |
| 5 (agent-2) | Watcher operational via `_execute_with_retry` narrows retry | `_execute_watcher_operational_retry` — any `OperationalError` |
| 5 (agent-2) | Jitter on connection/recovery | `jitter=None` for connection; crash recovery keeps `2**n` |
| 5 (agent-2) | Fallback "stop after Task 9 without _retry.py" | Fixed: revert branch or finish; 2-edit only before Task 1 |
| 5 (agent-2) | `max_attempts_override` poor fit | Dropped from `_TEST_CONFIG` |
| 5 (agent-2) | Stdlib test trusts `__stdlib_roots__` | Hardcode `sys.stdlib_module_names` in test |
| 6 (agent-3) | `StopException` retried by watcher `isinstance(OperationalError)` | `_is_watcher_operational_retry` honors `retryable=False` + test |
| 6 (agent-3) | R4 clamp test non-binding (`1.0 < 1.5`) | `jitter=None`, `max_delay=0.3`, assert `sleeps == [0.3]` |
| 6 (agent-3) | `stop=None` hits `TypeError` | Coerce `stop = stop or stop_never()` + test |
| 6 (agent-3) | `_check_stop()` placement unspecified | Locked: inside per-attempt wrapper in Task 10 |
| 6 (agent-3) | Hot-loop detector process-global | Documented in Task 7.3 (log-only) |
| 7 (agent-4) | `remove_backoff()` in sleep-count test | Removed from `test_execute_retry_does_not_sleep_after_final_attempt` |
| 7 (agent-4) | Jitter alias compares two random draws | Deterministic tests via `random.uniform` patch + delegate monkeypatch |
| 7 (agent-4) | Connection helper leaks `RetryInterrupted` | Map to `StopException("Connection interrupted")` |
| 7 (agent-4) | DB wrapping/logging underspecified | Task 11.2 shows outer try/except + `before_sleep` logging |
| 7 (agent-4) | Watcher logging dropped | `before_sleep` callback + outer `except OperationalError` |
| 7 (agent-4) | Elapsed tests patch `helpers.time.monotonic` | Task 9.4 → patch `_retry.time.monotonic` |
| 7 (agent-4) | R8 hot-loop untested | Added `test_hot_loop_warning_logs_after_rapid_retries` in Task 7.4 |
| 7 (agent-4) | Task 12 `busy_timeout=0` unspecified | `Queue(..., config={"BROKER_BUSY_TIMEOUT": 0})` on retrier |
| 8 (agent-5) | `db.py` `except Exception` wraps `StopException` | `except StopException: raise` before RuntimeError wrap; R18 test |
| 8 (agent-5) | `KeyboardInterrupt` in connection `retry_on` | Dropped — dead branch under Exception-only engine |
| 8 (agent-5) | Task 8.2 import list incomplete | Full adapter import block listed |
| 8 (agent-5) | Stale "extend before_sleep" in Task 11.2 | Removed — already in Step 11.1 |
| 8 (agent-5) | `before_sleep` on zero-second path | Moved inside `sleep_seconds > 0` guard |

**Direction check:** Three-layer split unchanged — vendorable `_retry.py` engine,
`helpers.py` adapter family, call-site orchestration in watcher/db. Bounded jitter on
lock-contention paths only; crash recovery outside the engine. Agent-1 through agent-5
reviews fixed correctness semantics, not the strategic goal. Fallback to 2-edit jitter
path remains documented if consolidation scope must shrink before Task 1.
