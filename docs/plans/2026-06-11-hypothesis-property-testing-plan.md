# Hypothesis Property-Based Testing Implementation Plan

> **For the implementer (human or agent):** work the checklist top to bottom; each `- [ ]` step is one action, tracked by its checkbox. No special tooling is assumed — if your environment provides plan-execution skills, you may use them, but following the steps literally is sufficient.

**Goal:** Add Hypothesis property-based tests to SimpleBroker in three landable stages: pure-function properties for the timestamp parser, cross-backend round-trip properties for queue names and message bodies, and a stateful model-based test that differentially checks queue semantics on SQLite, Postgres, and Redis against one plain-Python reference model.

**Architecture:** All new tests live in `tests/` as ordinary pytest modules and exercise the **public API only** (`simplebroker.Queue` via the existing backend-agnostic fixtures, plus the documented `TimestampGenerator.validate` entry point). No production code changes except one sanctioned exception-type fix (Task 4). Hypothesis profiles are registered centrally in `tests/conftest.py`. Cross-backend coverage reuses the repo's existing `shared` marker + `bin/pytest-pg` / `bin/pytest-redis` wrappers — we write each property once and the existing infrastructure runs it on all three backends.

**Tech Stack:** Python ≥3.11, pytest 7+ with pytest-xdist, Hypothesis ≥6.100 (new dev dependency), uv for env/lock management, ruff for lint/format.

---

# Part I — Required context (read this before Task 1)

You are assumed to be a competent developer who has never seen this codebase,
does not know Hypothesis well, and does not know this project's testing
philosophy. Everything you need is below. **Do not skip this part.**

## 1. What SimpleBroker is

SimpleBroker is a lightweight message queue (PyPI package `simplebroker`,
version 4.6.0) with **zero runtime dependencies**. The default backend is a
single SQLite file; optional extension packages (`simplebroker-pg`,
`simplebroker-redis`, both developed in this repo under `extensions/`) provide
Postgres and Redis/Valkey backends behind the same API. Users interact through:

- A CLI: `broker write myqueue "hello"`, `broker read myqueue`, etc.
  (entry point `simplebroker/cli.py`).
- A Python API: `simplebroker.Queue` (defined in `simplebroker/sbqueue.py`),
  which wraps the core engine `BrokerCore` (`simplebroker/db.py`).

**The project is in maintenance mode.** The maintainer's charter: correctness
fixes and consumer-driven API additions only. For this plan that means: **your
job is to add tests, not to "improve" production code.** Exactly one
production change is sanctioned by this plan (Task 4, a small exception-type
fix with a pre-verified failing input). Anything else surprising you find goes
in the findings log (section 9), not into the code.

## 2. Domain crash course

You must understand four concepts to write these tests:

**Timestamps are message IDs.** Every message gets a 64-bit "hybrid
timestamp": the top bits are nanoseconds-since-epoch with the bottom 12 bits
zeroed, and those bottom 12 bits hold a logical counter so that two messages
written in the same instant still get distinct, ordered IDs
([_timestamp.py:80-113](../../simplebroker/_timestamp.py)). Timestamps are
unique per database and strictly increase with every write. The API calls the
same value `timestamp`, `message_id`, and `ts` in different places — they are
the same 64-bit integer. Real timestamps generated today are ≈ 1.8 × 10^18
(19 decimal digits).

**Reading is claiming.** `Queue.read_one()` does not physically delete the
row. It marks it `claimed = 1` and commits, which guarantees exactly-once
delivery. Claimed rows linger invisibly until a **vacuum** physically deletes
them. By default an automatic vacuum can trigger during writes
([db.py:1242](../../simplebroker/db.py) — gated on
`config["BROKER_AUTO_VACUUM"] == 1`). This matters because
`peek(include_claimed=True)` and `stats()` can see claimed rows: with
auto-vacuum **on**, when claimed rows disappear is nondeterministic; with
auto-vacuum **off** (config `{"BROKER_AUTO_VACUUM": 0}`), claimed rows persist
until explicitly deleted, which lets a test model predict them exactly. The
Redis backend has no automatic vacuum at all (its `vacuum()` is only ever
explicit), and the Postgres backend reuses the generic `BrokerCore` write path
and therefore the same config gate — so `{"BROKER_AUTO_VACUUM": 0}` gives
deterministic claimed rows on **all three** backends.

**FIFO is timestamp order.** Within a queue, messages are always delivered
in ascending timestamp order. A message *moved* into a queue keeps its
original timestamp (the move SQL is `SET queue = ?, claimed = 0` —
[_sql/sqlite.py:166](../../simplebroker/_sql/sqlite.py)), so an old message
moved into a queue with newer messages will be read **first**, and a claimed
message moved by ID becomes **unclaimed (pending) again** at the destination.

**Queue names are validated by a regex.**
`QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")` at
[db.py:81](../../simplebroker/db.py), max length 512
(`MAX_QUEUE_NAME_LENGTH`, [_constants.py:91](../../simplebroker/_constants.py)),
checked with `.match()` (not `.fullmatch()`) at
[db.py:185](../../simplebroker/db.py). Invalid names raise **`ValueError`**
([db.py:949](../../simplebroker/db.py)) at first use (e.g. first `write()`),
*not* at `Queue()` construction time, and *not* the `QueueNameError` the
docstrings advertise — see finding F5 in section 8.

## 3. The Queue API surface you will test

All in [sbqueue.py](../../simplebroker/sbqueue.py). Signatures verified
2026-06-11; trust these over your guesses:

| Method | Behavior you rely on |
|---|---|
| `Queue(name, *, db_path=None, persistent=False, runner=None, config=None)` | Handle to one queue. `config` is a partial override dict merged over defaults (e.g. `{"BROKER_AUTO_VACUUM": 0}`). Process-local persistent sessions are keyed by `(target, config)` — [_broker_session.py:101-110](../../simplebroker/_broker_session.py) — so a custom config never bleeds into other queues. |
| `write(message: str) -> None` | Appends. Validates queue name, then UTF-8 **byte** size against `config["BROKER_MAX_MESSAGE_SIZE"]` (default 10 MB) — [db.py:1080-1086](../../simplebroker/db.py); oversize raises `ValueError`. |
| `refresh_last_ts() -> int` | Reads `meta.last_ts` from the database — the most recently generated timestamp. Called immediately after a `write()` in a single-threaded test, it returns exactly that write's timestamp. |
| `read_one(*, exact_timestamp=None, with_timestamps=False)` | Claims and returns the oldest pending message; `(body, ts)` tuple when `with_timestamps=True`; `None` when nothing matches. |
| `read_many(limit, *, with_timestamps=False, ...)` | Claims up to `limit` oldest pending, in order. `limit < 1` raises `ValueError`. Returns a list. |
| `read(*, all_messages=False, with_timestamps=False, after_timestamp=None, before_timestamp=None, message_id=None)` | CLI-mirroring wrapper. With `after_timestamp=b` it claims the oldest pending with `ts > b` (strict) or returns `None`. |
| `peek_many(limit=1000, *, with_timestamps=False, after_timestamp=None, before_timestamp=None, include_claimed=False)` | Non-destructive. Returns pending rows in ascending ts order; with `include_claimed=True`, claimed rows are merged in ts order. Never changes claim state. |
| `move_one(destination, *, exact_timestamp=None, require_unclaimed=True, with_timestamps=False)` | Atomically moves the oldest pending message (or the exact message by ID, which with `require_unclaimed=False` may be a claimed one). The message keeps its timestamp and arrives **unclaimed**. Returns `(body, ts)` or `None`. Destination is a queue **name** (str) or a `Queue`. |
| `delete(*, message_id=None) -> bool` | With no `message_id`: purges every row in the queue, claimed included; returns whether anything was deleted. |
| `delete_many(message_ids) -> int` | Physically deletes exact IDs (claimed or pending); silently ignores missing IDs; returns the count actually deleted. |
| `stats() -> QueueStats` | Dataclass with `.pending`, `.claimed`, `.total` ints ([metadata.py:9-15](../../simplebroker/metadata.py)). Counts are exact when auto-vacuum is off. |
| `has_pending(after_timestamp=None) -> bool` | True iff an unclaimed message exists (`ts > bound` when given). |
| `exists() -> bool` | True iff any row exists, claimed included. |
| `close() -> None` | Releases connections. Always call it on queues you create outside fixtures. |

`TimestampGenerator.validate(timestamp_str, exact=False)`
([_timestamp.py:259](../../simplebroker/_timestamp.py)) is a **static** method
and the canonical timestamp parser: it accepts native 19-digit IDs, ISO-8601
dates/datetimes, unix seconds/milliseconds/nanoseconds (bare numbers,
heuristically by digit count, or with explicit `s`/`ms`/`ns` suffixes), strips
surrounding whitespace, rejects scientific notation, and raises
`TimestampError` ([_exceptions.py](../../simplebroker/_exceptions.py),
subclass of `BrokerError`, **not** of `ValueError`) for invalid input. With
`exact=True` it accepts only 19-digit numeric strings below `2**63`.

## 4. The test infrastructure you must reuse (do not reinvent)

Read [tests/conftest.py](../../tests/conftest.py) once before starting. The
parts you will use:

- **`queue_factory` fixture** — function-scoped. `queue_factory("name")`
  returns a `Queue` bound to the active backend; all queues it makes are
  closed at teardown. This is the default way to get a queue in a test.
- **`broker_target` fixture** — function-scoped `ResolvedTarget` describing
  the active backend (a temp SQLite file, a per-worker Postgres schema, or a
  per-worker Redis namespace). Pass it to
  `make_queue(name, broker_target, config=...)` (from
  `tests/helper_scripts/broker_factory.py`) when you need a queue with a
  custom config — `queue_factory` does not take `config`.
- **Backend selection** is by environment variable `BROKER_TEST_BACKEND`
  (default `sqlite`). You never set it manually; the wrappers do:
  `uv run ./bin/pytest-pg …` and `uv run ./bin/pytest-redis …` start Docker
  containers automatically and run the suite against Postgres/Redis.
- **Markers** ([pyproject.toml:88-94](../../pyproject.toml)):
  `shared` = backend-agnostic test that must pass on all backends;
  `sqlite_only`, `pg_only`, `redis_only` = scoped. A hook in conftest
  ([tests/conftest.py:837-853](../../tests/conftest.py)) auto-marks unmarked
  tests: modules calling `run_cli` become `shared`, **everything else becomes
  `sqlite_only`**. Consequence: any property test that should run on Postgres
  and Redis **must carry an explicit `pytest.mark.shared`** (we use
  module-level `pytestmark`), or it will silently never run there.
- **Parallelism**: pytest runs with `-n auto --dist loadgroup`
  ([pyproject.toml:87](../../pyproject.toml)) and `-m "not slow"` by default.
- **Postgres/Redis isolation caveat**: for SQLite each *test* gets a fresh
  temp database, but Postgres tables are truncated / Redis namespaces reset
  only **between test items**, and one Hypothesis test item internally runs
  *many* examples. Therefore each example (and each state-machine execution)
  must isolate itself by using **unique queue names** — you'll see a
  `itertools.count()` suffix pattern in every fixture-using property below.
  This is not optional; without it, examples poison each other and you get
  unreproducible failures.

Commands you will run constantly:

```bash
uv sync --all-extras                  # install/refresh the dev environment
uv run pytest tests/test_property_timestamp_validate.py -v    # one file
uv run pytest -q                      # full default (SQLite) suite, fast lane
uv run ./bin/pytest-pg -q tests/test_property_queue_model.py  # same tests on Postgres (auto-Docker)
uv run ./bin/pytest-redis -q tests/test_property_queue_model.py  # same on Redis (auto-Docker)
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
```

## 5. Test design rules for this plan (read twice)

1. **No mocks. None.** Every test in this plan runs against a real broker on
   a real backend through the fixtures above, or calls a real pure function.
   If you find yourself reaching for `unittest.mock`, `MagicMock`, or a
   hand-rolled fake `Queue`, stop — you are about to test your mock instead
   of SimpleBroker. The whole value of this suite is that the *same
   assertions* run against three real storage engines. The only "test double"
   in this plan is the plain-Python *reference model* inside the state
   machine, and it is the **expected-value oracle**, not a replacement for
   the system under test.
2. **Public API only.** Tests import `simplebroker.Queue`,
   `TimestampGenerator.validate`, exceptions, and constants. Do not call
   `BrokerCore` internals, do not open the SQLite file directly, do not poke
   `_private` attributes. (Importing constants like `SQLITE_MAX_INT64` and
   `QUEUE_NAME_PATTERN` for *strategy construction* is fine and matches
   existing test practice.)
3. **Properties state contracts, not implementations.** Each property's
   docstring must say what user-visible promise it checks. If you can't
   phrase the promise, the property is wrong.
4. **Red before green, even for tests.** A test you have never seen fail is
   unverified. For each property file there is an explicit "sabotage" step:
   deliberately break the expectation, watch Hypothesis fail and shrink,
   revert. Do not skip these steps — they are how you verify the harness
   actually bites.
5. **Pin reality, log surprises.** When current behavior contradicts
   documentation (several pre-verified cases below), the test pins the
   *actual* behavior with a `FINDING` comment and you add a row to the
   findings log. You do not change production behavior (except Task 4).
6. **DRY within reason.** Shared strategies live at module top. But do not
   build a "property test framework" — three small files plus one machine
   file is the whole deliverable. YAGNI: no abstract base classes, no helper
   packages, no parametrized meta-strategies.

## 6. Hypothesis crash course (just enough for this plan)

- `@given(x=some_strategy)` runs the test many times with generated inputs.
  On failure Hypothesis **shrinks** to a minimal failing input and prints it,
  plus a `@reproduce_failure(...)` blob (we enable `print_blob`) you can
  paste onto the test to replay exactly.
- Strategies used here: `st.integers(min_value=, max_value=)`,
  `st.text(alphabet=..., max_size=...)`, `st.characters(exclude_characters=...)`
  (full Unicode minus surrogates by default), `st.sampled_from(seq)`,
  `st.booleans()`, `st.lists(...)`, `st.dates(...)`,
  `st.from_regex(compiled_pattern)`, `.filter(pred)`.
- `@example(value)` forces specific inputs to be tried first, deterministically
  — we use it to bake in pre-verified edge cases so failures are not left to
  random search.
- **Settings & profiles**: we register `dev` (default, 50 examples) and `ci`
  (200 examples) profiles in `tests/conftest.py`, selected by the
  `HYPOTHESIS_PROFILE` env var. `deadline=None` globally: deadlines measure
  per-example wall time and DB-backed examples would flake on busy CI.
  Per-test `@settings(...)` overrides individual fields; note
  `suppress_health_check` **replaces** rather than appends.
- **Health check you will hit**: `function_scoped_fixture`. Hypothesis warns
  when a `@given` test uses a function-scoped pytest fixture, because the
  fixture is set up **once per test item, not once per example**. For us that
  is intentional (one broker per item, isolation via unique queue names), so
  those tests suppress exactly that health check and say why in a comment.
- **Stateful testing**: `RuleBasedStateMachine` defines `@rule()` methods
  (actions Hypothesis sequences randomly) and `@invariant()` methods (checked
  after every step). `run_state_machine_as_test(MachineClass, settings=...)`
  runs whole random action sequences and shrinks failing sequences to a
  minimal script. `@precondition(lambda self: ...)` gates rules without
  failing them. A machine's `__init__`/`teardown` run once per *execution*
  (one generated sequence), many executions per test item.
- The example database lives in `.hypothesis/` at the repo root —
  git-ignored in Task 1, never committed.

## 7. Files you will create/modify (whole plan at a glance)

| Path | Action | Stage |
|---|---|---|
| `pyproject.toml` | add `hypothesis>=6.100` to `[project.optional-dependencies] dev` | 0 |
| `uv.lock` | regenerated by `uv sync --all-extras` | 0 |
| `.gitignore` | add `.hypothesis/` | 0 |
| `tests/conftest.py` | register Hypothesis profiles (one small block) | 0 |
| `docs/plans/2026-06-11-hypothesis-property-testing-findings.md` | create findings log (stays local — `docs/` is gitignored) | 0 |
| `tests/test_property_timestamp_validate.py` | create — parser properties | 1 |
| `simplebroker/_timestamp.py` | sanctioned exception-type fix in `validate()` | 1 |
| `tests/test_property_queue_names.py` | create — name grammar properties | 2 |
| `tests/test_property_message_roundtrip.py` | create — body fidelity + size limit | 2 |
| `tests/test_property_queue_model.py` | create — stateful model machine | 3 |
| `README.md` | dev-testing section: new lines + one stale-comment fix | 4 |
| `CHANGELOG.md` | `[Unreleased]` entry | 4 |

Naming: `test_property_*.py` keeps the flat-file convention of `tests/` and
makes the property suite greppable as a unit.

## 8. Pre-verified findings (probed on 2026-06-11 — do not re-litigate)

These six behaviors were verified by probing the current code (F6 on live
Dockerized backends). The tasks below cite them as F1–F6. Only F1 gets fixed
(Task 4); the rest get pinned and logged.

- **F1** `TimestampGenerator.validate("9999-01-01")` raises **`ValueError`**
  ("Invalid timestamp: too far in future") leaking from
  [_timestamp.py:465](../../simplebroker/_timestamp.py), violating the
  documented contract (docstring: raises `TimestampError`). The CLI is
  unaffected either way (its `_validate_timestamp` wrapper in
  [commands.py](../../simplebroker/commands.py) converts `TimestampError` →
  `ValueError`, and a leaked
  `ValueError` carries the same message), so fixing the exception type is
  contract-restoring with zero CLI behavior change. **Sanctioned fix: Task 4.**
- **F2** `validate("0001-01-01")` returns a **negative** int
  (−62135596800000000000): pre-epoch ISO dates parse "successfully" into
  values every downstream bound check rejects
  (`validate_timestamp_bound` requires ≥ 0,
  [_timestamp.py:38](../../simplebroker/_timestamp.py)). Pin, log, don't fix.
- **F3** `validate("١٢٣s")` (Eastern Arabic digits) returns 122999996416 —
  Python's `int()`/`str.isdigit()` accept any Unicode decimal digits. Pin,
  log, don't fix.
- **F4** `QUEUE_NAME_PATTERN.match("abc\n")` **matches** (`$` matches before a
  trailing newline when used with `.match()`), so `"abc\n"` is a valid,
  working queue name despite the documented charset. Verified end-to-end:
  writes and reads fine on SQLite. Pin, log, don't fix (rejecting it now
  would break any existing queue named that way — maintainer's call).
- **F5** Invalid queue names raise **`ValueError`**, while `Queue` docstrings
  promise `QueueNameError`. Pin `ValueError`, log the docs mismatch.
- **F6** NUL (`\x00`) bodies round-trip on SQLite and Redis, but the Postgres
  backend rejects them at write time with
  `OperationalError("PostgreSQL text fields cannot contain NUL (0x00) bytes")`
  — a deliberate, translated error in `simplebroker_pg/runner.py`; nothing is
  stored and the queue stays usable. Task 8 pins this per backend. Documented
  divergence, not a bug to fix here.

## 9. Findings protocol

`docs/plans/2026-06-11-hypothesis-property-testing-findings.md` (created in
Task 2) is the single place new discoveries go. When a property fails and the
failure is *the code being wrong or surprising* (rather than your property
being wrong):

1. Reproduce it with the printed `@reproduce_failure` blob or a minimal
   plain-pytest example.
2. Add a row to the findings table (ID, one-line description, repro, file:line,
   suggested disposition).
3. Make the property pass against **current** behavior: either narrow the
   strategy with a comment naming the finding ID, or pin the behavior with an
   explicit `@example`/plain test marked `# FINDING Fn`.
4. Do **not** fix production code. The maintainer triages the log.

The log lives under `docs/`, which this repo deliberately gitignores
(`.gitignore` line 12 — plan documents are local working state by
convention). Never `git add` it (git refuses ignored paths); keep it
accurate anyway — it is part of the handoff.

The one exception is F1, whose fix is explicitly Task 4 of this plan.

---

# Part II — Tasks

Work top to bottom. Every task that *changes files* ends in a commit, with
two deliberate exceptions called out inline: Task 3 defers its commit to
Task 4 (the red test and its fix land together), and Tasks 6, 13, and 15 are
verification-only unless they say otherwise. Beyond the Task 3+4 pairing,
never batch two tasks' changes into one commit. Commit messages follow the
repo's style: imperative mood, no `feat:`/`fix:` prefixes (look at
`git log --oneline -5` to see it).

**Global rules for every commit in this plan:**
- Stage only the files the task names (`git add <paths>`). The working tree
  may contain unrelated in-flight changes — run `git status` before you
  start, treat anything already modified or untracked as off-limits, and
  **never** use `git add -A`. (Known example at plan-writing time:
  `extensions/simplebroker_pg/uv.lock` was already modified — do not stage
  it, do not revert it.) If unrelated *test failures* exist before you
  begin, record the baseline so your gates compare against it.
- Before committing, run
  `uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests`
  and include any resulting fixes.
- Never commit `.hypothesis/`, `*.db`, or scratch files.

## Stage 0 — Infrastructure

### Task 1: Add the Hypothesis dependency and ignore its cache

**Files:**
- Modify: `pyproject.toml` (dev extras, around line 35)
- Modify: `.gitignore` (near the `htmlcov/` line, around line 48)
- Modify (generated): `uv.lock`

- [ ] **Step 1: Add the dependency.** In `pyproject.toml`, inside
  `[project.optional-dependencies]` → `dev = [`, add one line directly after
  `"build>=1.2",`:

```toml
    "hypothesis>=6.100",
```

- [ ] **Step 2: Add the cache dir to .gitignore.** In `.gitignore`, directly
  after the `htmlcov/` line, add:

```gitignore
.hypothesis/
```

- [ ] **Step 3: Sync the environment (regenerates `uv.lock`).**

Run: `uv sync --all-extras`
Expected: output ends with installed/updated packages including
`+ hypothesis==6.1xx.x` (exact version varies); `uv.lock` is now modified.
If the sync also touches extension lockfiles (`extensions/*/uv.lock`), leave
them alone and unstaged — only the root `uv.lock` belongs to this commit.

- [ ] **Step 4: Verify the install.**

Run: `uv run python -c "import hypothesis; print(hypothesis.__version__)"`
Expected: prints a version ≥ 6.100.

- [ ] **Step 5: Verify nothing broke.**

Run: `uv run pytest tests/test_smoke.py -q`
Expected: all tests pass, exit code 0.

- [ ] **Step 6: Commit.**

```bash
git add pyproject.toml uv.lock .gitignore
git commit -m "Add Hypothesis as a dev dependency"
```

### Task 2: Register Hypothesis profiles and create the findings log

**Files:**
- Modify: `tests/conftest.py` (two small insertions: one import, one
  configuration block)
- Create: `docs/plans/2026-06-11-hypothesis-property-testing-findings.md`

- [ ] **Step 1: Register profiles.** Two insertions in `tests/conftest.py`
  (two so that ruff's import sorting stays happy — the import goes in the
  import section, the calls go below it):

  (a) Directly after the existing `import pytest` line (around line 21), add:

```python
from hypothesis import settings as hypothesis_settings
```

  (b) Directly after the last `from .helper_scripts...` import (the
  `patch_watchers` one, around line 49), add:

```python
# --------------------------------------------------------------------------- #
# Hypothesis configuration (used by tests/test_property_*.py)
# --------------------------------------------------------------------------- #
# deadline=None: per-example deadlines measure wall time and would flake for
# DB-backed properties on slow CI; correctness, not latency, is what these
# tests check. print_blob=True makes every failure replayable via the printed
# @reproduce_failure decorator.
hypothesis_settings.register_profile(
    "dev", max_examples=50, deadline=None, print_blob=True
)
hypothesis_settings.register_profile(
    "ci", max_examples=200, deadline=None, print_blob=True
)
hypothesis_settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
```

(`import os` already exists at the top of conftest.)

- [ ] **Step 2: Verify collection still works.**

Run: `uv run pytest --collect-only -q 2>&1 | tail -3`
Expected: a count of collected tests, no errors.

- [ ] **Step 3: Verify the profile actually loads.**

Run: `HYPOTHESIS_PROFILE=ci uv run python -c "import tests.conftest; from hypothesis import settings; print(settings.default.max_examples)"`
Expected: `200`.

- [ ] **Step 4: Create the findings log.** Create
  `docs/plans/2026-06-11-hypothesis-property-testing-findings.md` with exactly:

```markdown
# Property-testing findings log

Discovered-by-Hypothesis behaviors that contradict documentation or look
unintended. Tests PIN current behavior; nothing here changes production code
without maintainer sign-off. F1 is fixed by the implementation plan (Task 4);
all others are open for triage.

| ID | Behavior (current, verified) | Repro | Where | Suggested disposition |
|----|------------------------------|-------|-------|----------------------|
| F1 | `validate("9999-01-01")` leaks `ValueError` instead of documented `TimestampError` | `TimestampGenerator.validate("9999-01-01")` | `simplebroker/_timestamp.py:465` + uncaught at `:300` | Plan Task 4 fixes this (contract-restoring, CLI-neutral) |
| F2 | Pre-epoch ISO dates parse to negative ints that all bound checks then reject | `validate("0001-01-01")` → −62135596800000000000 | `simplebroker/_timestamp.py:418-466` | Decide: reject pre-epoch ISO with `TimestampError`, or document |
| F3 | Non-ASCII Unicode digits accepted everywhere `int()`/`isdigit()` are used | `validate("١٢٣s")` → 122999996416 | `simplebroker/_timestamp.py:339-383,323-336` | Probably document as harmless superset |
| F4 | Trailing-newline queue names accepted and functional (`$` + `.match()` quirk) | `Queue("abc\n").write(...)` works | `simplebroker/db.py:81,185` | Decide: switch to `fullmatch` (breaking) or document |
| F5 | Invalid queue names raise `ValueError`; docstrings promise `QueueNameError` | `Queue("bad name!").write("x")` | `simplebroker/db.py:949` vs `sbqueue.py` docstrings | Decide: fix docstrings, or raise `QueueNameError` (breaking) |
| F6 | NUL bodies diverge: round-trip on SQLite/Redis; Postgres rejects at write with `OperationalError: PostgreSQL text fields cannot contain NUL (0x00) bytes` | `Queue.write("a\x00b")` under pytest-pg | `extensions/simplebroker_pg/simplebroker_pg/runner.py` (deliberate error translation) | Documented divergence, pinned per-backend in tests; consider a README note |

## New findings

(append rows here as F7, F8, ... per the protocol in the plan, Part I §9)
```

- [ ] **Step 5: Commit.** The findings log is deliberately NOT staged —
  `docs/` is gitignored in this repo (Part I §9), and `git add` on an
  ignored path errors out.

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/conftest.py
git commit -m "Register Hypothesis test profiles"
```

## Stage 1 — Timestamp parser properties (pure functions, SQLite-suite only)

These tests are pure-Python (no broker, no fixtures), so they intentionally
carry **no** `shared` marker: the conftest hook will mark them `sqlite_only`,
and that is correct — re-running pure parsing logic under Postgres/Redis
wrappers would add runtime and zero information.

### Task 3: Totality property (this is the RED test for the Task 4 fix)

**Files:**
- Create: `tests/test_property_timestamp_validate.py`

- [ ] **Step 1: Create the file** with this exact content:

```python
"""Property-based tests for TimestampGenerator.validate().

validate() is the canonical multi-format timestamp parser used by the CLI
(-m / --since flags) and extensions. Formats: native 19-digit hybrid IDs,
ISO-8601 dates/datetimes, unix seconds/ms/ns (bare, by digit-count heuristic,
or with explicit s/ms/ns suffixes). These properties pin its contract:

1. Totality: any string either parses to an int or raises TimestampError.
2. Exact mode accepts exactly the in-range 19-digit strings.
3. Native IDs round-trip through str().
4. Equivalent representations of the same instant parse equal.

Pure functions only — no broker, no fixtures. The conftest hook will mark
this module sqlite_only, which is intended: nothing here touches a backend.
"""

from __future__ import annotations

from hypothesis import example, given
from hypothesis import strategies as st

from simplebroker._constants import LOGICAL_COUNTER_MASK
from simplebroker._exceptions import TimestampError
from simplebroker._timestamp import TimestampGenerator

# Imports stay minimal here on purpose: ruff --fix strips unused imports at
# commit time, so each task adds imports only when its code first uses them
# (Task 5 extends this block).

NS_PER_S = 1_000_000_000
# Hybrid timestamps zero their bottom 12 bits for a logical counter; parsed
# wall-clock inputs are quantized to this granularity (4096 ns).
QUANTUM = LOGICAL_COUNTER_MASK + 1


@given(st.text(max_size=64))
@example("9999-01-01")  # F1: leaked ValueError before the Task 4 fix
@example("0001-01-01")  # F2: parses to a negative int (pinned below)
@example("١٢٣s")  # F3: non-ASCII digits accepted (pinned below)
@example("  5s  ")  # whitespace is stripped first (documented behavior)
@example("1e9")  # scientific notation is rejected
@example("2024-01-15T14:30:00Z")
def test_validate_is_total(s: str) -> None:
    """Contract: validate() returns an int or raises TimestampError — never
    any other exception type (sbqueue/CLI callers catch TimestampError only).
    """
    try:
        result = TimestampGenerator.validate(s)
    except TimestampError:
        return
    assert isinstance(result, int)
```

- [ ] **Step 2: Run it and watch it fail (RED).**

Run: `uv run pytest tests/test_property_timestamp_validate.py -q`
Expected: **FAIL** on the `@example("9999-01-01")` case with
`ValueError: Invalid timestamp: too far in future` (an exception that is
neither a return nor a `TimestampError` — exactly finding F1). If it
*passes*, stop: either the bug was already fixed (check `git log
simplebroker/_timestamp.py`) or your file differs from the listing.

- [ ] **Step 3: Do NOT commit yet.** The suite is deliberately red; work
  lands on the main branch here, so the failing test and its fix go into one
  commit at the end of Task 4. Leave the file in your working tree and move
  straight on.

### Task 4: The sanctioned fix — wrap ISO parser errors (GREEN)

**Files:**
- Modify: `simplebroker/_timestamp.py:298-302` (inside `validate()`)

This is the **only** production change in this plan. Scope check before you
start: you are changing the *type* of an exception for inputs that already
fail, nothing else. `_parse_message_id` (exact mode) never reaches the ISO
branch, and the CLI's `_validate_timestamp` wrapper in
[commands.py](../../simplebroker/commands.py) converts
`TimestampError` to `ValueError` with the same message, so user-visible CLI
behavior is identical before and after.

- [ ] **Step 1: Apply the fix.** In `simplebroker/_timestamp.py`, inside
  `validate()`, find:

```python
        # Try formats in order of precedence
        # 1. ISO format (unambiguous)
        ts = TimestampGenerator._parse_iso8601(timestamp_str)
        if ts is not None:
            return ts
```

Replace with:

```python
        # Try formats in order of precedence
        # 1. ISO format (unambiguous)
        try:
            ts = TimestampGenerator._parse_iso8601(timestamp_str)
        except ValueError as e:
            # _parse_iso8601 raises bare ValueError for out-of-range dates;
            # validate()'s contract is TimestampError for any invalid input.
            raise TimestampError(str(e)) from None
        if ts is not None:
            return ts
```

- [ ] **Step 2: Verify GREEN.**

Run: `uv run pytest tests/test_property_timestamp_validate.py -q`
Expected: PASS.

- [ ] **Step 3: Verify no regressions in the existing timestamp and CLI tests.**

Run: `uv run pytest tests/test_timestamp_edge_cases.py tests/test_timestamp_helpers.py tests/test_timestamp_resilience.py tests/test_cli_validation.py tests/test_message_by_timestamp.py -q`
Expected: all pass.

- [ ] **Step 4: Run mypy on the touched production file.**

Run: `uv run mypy simplebroker`
Expected: no new errors (`TimestampError` is already imported in the module).

- [ ] **Step 5: Update the findings log.** In
  `docs/plans/2026-06-11-hypothesis-property-testing-findings.md`, change
  F1's disposition cell to `FIXED by Task 4; changelog entry pending (Task 14)`.

- [ ] **Step 6: Commit the test and the fix together** (red was observed in
  Task 3, green in Step 2 — that is the TDD evidence; the single commit keeps
  the main branch green at every commit):

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_timestamp_validate.py simplebroker/_timestamp.py
git commit -m "Pin validate() totality and raise TimestampError for far-future ISO dates"
```

(The findings-log update from Step 5 stays uncommitted — `docs/` is
gitignored; see Part I §9.)

### Task 5: Exact-mode, round-trip, and equivalence properties

**Files:**
- Modify: `tests/test_property_timestamp_validate.py` (append)

- [ ] **Step 1: Extend the imports, then append the exact-mode and native
  round-trip properties.** First replace the import block at the top of
  `tests/test_property_timestamp_validate.py` with its final form (the rest
  of this task uses `pytest`, the `datetime` types, and two more constants):

```python
from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from simplebroker._constants import (
    LOGICAL_COUNTER_MASK,
    SQLITE_MAX_INT64,
    UNIX_NATIVE_BOUNDARY,
)
from simplebroker._exceptions import TimestampError
from simplebroker._timestamp import TimestampGenerator
```

  (Drop the "imports stay minimal" comment — it has served its purpose.)
  Then append to the end of the file:

```python
@given(st.integers(min_value=10**18, max_value=SQLITE_MAX_INT64 - 1))
def test_exact_mode_round_trips_in_range_ids(ts: int) -> None:
    """Contract: every real message ID (19 digits, < 2**63) survives
    str() -> validate(exact=True) unchanged. This is the -m flag's path."""
    assert TimestampGenerator.validate(str(ts), exact=True) == ts


@given(st.integers(min_value=SQLITE_MAX_INT64, max_value=10**19 - 1))
def test_exact_mode_rejects_out_of_range_19_digit_ids(ts: int) -> None:
    """19-digit strings at or above 2**63 are not valid IDs."""
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(str(ts), exact=True)


@given(
    st.text(max_size=30).filter(
        # Mirror the implementation gate (len==19 and str.isdigit() after
        # strip) exactly, so this property is the complement of acceptance.
        # str.isdigit (not an ASCII check) is intentional — see finding F3.
        lambda s: not (len(s.strip()) == 19 and s.strip().isdigit())
    )
)
def test_exact_mode_rejects_everything_else(s: str) -> None:
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(s, exact=True)


@given(st.integers(min_value=UNIX_NATIVE_BOUNDARY, max_value=SQLITE_MAX_INT64 - 1))
def test_native_ids_round_trip_in_default_mode(ts: int) -> None:
    """Bare integers at or above 2**44 are treated as native IDs and returned
    verbatim (the digit-count heuristic only applies below that boundary)."""
    assert TimestampGenerator.validate(str(ts)) == ts
```

- [ ] **Step 2: Run them.**

Run: `uv run pytest tests/test_property_timestamp_validate.py -q`
Expected: PASS.

- [ ] **Step 3: Sabotage check (verify the properties can fail).** Temporarily
  change `== ts` to `== ts + 1` in `test_exact_mode_round_trips_in_range_ids`,
  run the file, and confirm it FAILS on the first generated example with a
  clean Hypothesis falsifying-example report. Revert the sabotage and re-run
  to confirm PASS again.

- [ ] **Step 4: Append the cross-format equivalence properties:**

```python
@given(st.integers(min_value=0, max_value=9_000_000_000))
def test_unit_suffixes_and_bare_seconds_agree(n: int) -> None:
    """The same instant expressed as Ns / N*1000ms / N*1e9ns / bare N parses
    to one identical hybrid timestamp (integer math end-to-end). Bare numbers
    of <= 11 digits are read as seconds by the documented heuristic; 9e9 stays
    inside both that heuristic and the 2**63 ns range (~year 2255)."""
    expected = (n * NS_PER_S) & ~LOGICAL_COUNTER_MASK
    assert TimestampGenerator.validate(f"{n}s") == expected
    assert TimestampGenerator.validate(f"{n * 1000}ms") == expected
    assert TimestampGenerator.validate(f"{n * NS_PER_S}ns") == expected
    assert TimestampGenerator.validate(str(n)) == expected


@given(st.integers(min_value=0, max_value=4_102_444_800))  # 1970 .. 2100-01-01
def test_iso_datetimes_agree_with_unix_seconds(n: int) -> None:
    """An ISO datetime and its unix-seconds form agree to within one logical
    quantum. NOT exact equality: the ISO path multiplies a float (datetime
    .timestamp() * 1e9, _timestamp.py:459) whose rounding can land one 4096ns
    quantum away from the integer-math suffix path. Asserting exact equality
    here WILL flake — do not 'fix' a failure by tightening this."""
    iso = datetime.fromtimestamp(n, tz=UTC).isoformat()
    assert abs(TimestampGenerator.validate(iso) - TimestampGenerator.validate(f"{n}s")) <= QUANTUM


@given(st.dates(min_value=date(1970, 1, 1), max_value=date(2200, 12, 31)))
def test_date_only_iso_means_midnight_utc(d: date) -> None:
    """A bare YYYY-MM-DD parses identically to its explicit midnight-UTC
    datetime (both run through the same float path, so equality is exact)."""
    assert TimestampGenerator.validate(d.isoformat()) == TimestampGenerator.validate(
        f"{d.isoformat()}T00:00:00Z"
    )


@given(st.dates(min_value=date(2263, 1, 1), max_value=date(9999, 12, 31)))
def test_far_future_iso_raises_timestamp_error(d: date) -> None:
    """Dates beyond the 2**63-ns horizon (April 2262) are invalid. Guards the
    Task 4 fix; the boundary year 2262 itself is deliberately excluded."""
    with pytest.raises(TimestampError):
        TimestampGenerator.validate(d.isoformat())
```

- [ ] **Step 5: Append the pinned-quirk regression tests:**

```python
def test_known_quirk_pre_epoch_iso_returns_negative_int() -> None:
    """FINDING F2 (pinned, not endorsed): pre-epoch ISO dates parse to
    negative ints, which every downstream bound check then rejects
    (validate_timestamp_bound requires >= 0). If this starts failing, the
    parser's pre-epoch behavior changed — update the findings log."""
    result = TimestampGenerator.validate("0001-01-01")
    assert isinstance(result, int)
    assert result < 0


def test_known_quirk_non_ascii_digits_accepted() -> None:
    """FINDING F3 (pinned, not endorsed): int() accepts any Unicode decimal
    digits, so Eastern Arabic numerals parse like ASCII ones."""
    assert TimestampGenerator.validate("١٢٣s") == (123 * NS_PER_S) & ~LOGICAL_COUNTER_MASK
```

- [ ] **Step 6: Run the whole file.**

Run: `uv run pytest tests/test_property_timestamp_validate.py -q`
Expected: PASS — 11 tests (9 properties + 2 pinned quirks).

- [ ] **Step 7: Lint, format, commit.**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_timestamp_validate.py
git commit -m "Add round-trip and equivalence properties for timestamp parsing"
```

### Task 6: Stage 1 gate

- [ ] **Step 1: Full default suite.**

Run: `uv run pytest -q`
Expected: everything passes (same count as pre-plan plus the new tests; no
new failures, no new warnings about unknown marks).

- [ ] **Step 2: Deeper Hypothesis run of the new file.**

Run: `HYPOTHESIS_PROFILE=ci uv run pytest tests/test_property_timestamp_validate.py -q`
Expected: PASS in well under a minute. If Hypothesis finds a counterexample
the dev profile missed, follow the findings protocol (Part I §9) before
proceeding.

No commit (nothing changed); this gate just must be green before Stage 2.

## Stage 2 — Cross-backend grammar and round-trip properties

These DO run on all three backends, so both files carry a module-level
`pytestmark = pytest.mark.shared`.

### Task 7: Queue-name grammar properties

**Files:**
- Create: `tests/test_property_queue_names.py`

- [ ] **Step 1: Create the file** with this exact content:

```python
"""Property-based tests for queue-name validation and end-to-end usability.

The validator (db.py:_validate_queue_name_cached) accepts names matching
QUEUE_NAME_PATTERN (^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$ via .match()) up to
MAX_QUEUE_NAME_LENGTH chars. Two contracts:

1. Everything the grammar accepts actually WORKS, end to end, on every
   backend (a name that validates but breaks storage would be a
   cross-backend drift bug — exactly what this suite exists to catch).
2. Everything outside the accept set is rejected at first use.

Marked shared: name handling crosses into SQL text / Redis key territory,
where backend escaping differences would surface.
"""

from __future__ import annotations

import itertools

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from simplebroker._constants import MAX_QUEUE_NAME_LENGTH
from simplebroker.db import QUEUE_NAME_PATTERN

pytestmark = pytest.mark.shared

# Unique-suffix counter: one broker serves ALL examples of a test item (the
# fixture is function-scoped, Hypothesis examples are not), so every example
# must use a queue name nobody else wrote to. See Part I section 4 caveat.
_uniq = itertools.count()

# Trailing-"\n" names are accepted by the validator (finding F4, pinned in a
# dedicated test below) but would corrupt our "_<n>" suffixing, so the
# strategy filters them out. Length cap leaves room for the suffix.
VALID_NAMES = st.from_regex(QUEUE_NAME_PATTERN).filter(
    lambda s: len(s) <= MAX_QUEUE_NAME_LENGTH - 24 and not s.endswith("\n")
)

BODIES = st.text(
    alphabet=st.characters(exclude_characters="\x00"), max_size=50
)


def _validator_accepts(s: str) -> bool:
    """Mirror db._validate_queue_name_cached's accept logic exactly."""
    return bool(s) and len(s) <= MAX_QUEUE_NAME_LENGTH and bool(QUEUE_NAME_PATTERN.match(s))


@given(name=VALID_NAMES, body=BODIES)
@settings(
    # The function-scoped queue_factory is intentionally reused across
    # examples; isolation comes from the unique name suffix. too_slow must be
    # re-listed because suppress_health_check replaces, not appends. Example
    # counts come from the active profile (see tests/conftest.py).
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_grammar_valid_names_work_end_to_end(queue_factory, name: str, body: str) -> None:
    q = queue_factory(f"{name}_{next(_uniq)}")
    q.write(body)
    assert q.read_one() == body


@given(name=st.text(max_size=600).filter(lambda s: not _validator_accepts(s)))
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_grammar_invalid_names_are_rejected_at_first_use(queue_factory, name: str) -> None:
    """FINDING F5 (pinned): rejection raises ValueError, although docstrings
    advertise QueueNameError (which is not a ValueError subclass). If this
    starts failing with QueueNameError, the implementation moved to match its
    docs — update the findings log and flip this assertion deliberately."""
    q = queue_factory(name)  # construction does not validate ...
    with pytest.raises(ValueError):
        q.write("x")  # ... first use does


def test_known_quirk_trailing_newline_name_accepted(queue_factory) -> None:
    """FINDING F4 (pinned, not endorsed): '$' with .match() matches before a
    trailing newline, so 'name\\n' validates AND functions on every backend."""
    q = queue_factory("nl_quirk\n")
    q.write("hello")
    assert q.read_one() == "hello"
```

- [ ] **Step 2: Run on SQLite.**

Run: `uv run pytest tests/test_property_queue_names.py -q`
Expected: PASS (3 tests).

- [ ] **Step 3: Sabotage check.** In
  `test_grammar_valid_names_work_end_to_end`, temporarily change
  `q.write(body)` to `q.write(body + "!")`. Run the file; expect a FAIL where
  Hypothesis shrinks `body` to `''` (the minimal counterexample). Revert,
  re-run, confirm PASS. You have now seen shrinking work; remember what a
  minimal counterexample looks like.

- [ ] **Step 4: Run on Postgres and Redis.**

Run: `uv run ./bin/pytest-pg -q tests/test_property_queue_names.py`
Run: `uv run ./bin/pytest-redis -q tests/test_property_queue_names.py`
Expected: PASS on both (Docker startup may take ~a minute the first time).
If either backend fails where SQLite passed, that is a cross-backend drift
discovery: follow the findings protocol — capture the shrunk example, log it
as F6+, and constrain the strategy with a comment referencing the finding so
the suite is green on all backends before you commit.

- [ ] **Step 5: Lint, format, commit.**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_queue_names.py
git commit -m "Add cross-backend queue-name grammar properties"
```

### Task 8: Message-body fidelity and size-limit properties

**Files:**
- Create: `tests/test_property_message_roundtrip.py`

- [ ] **Step 1: Create the file** with this exact content:

```python
"""Property-based tests for message-body fidelity and the size limit.

Contracts:
1. Any Unicode body (minus NUL, see below) survives write -> read_one
   character-identical, on every backend.
2. The size limit counts UTF-8 BYTES, not characters (db.py:1081), accepts
   exactly the bodies within the limit, and rejects oversize ones without
   writing anything.

NUL ("\\x00") is excluded from generated bodies because the backends
genuinely diverge (finding F6, verified on live backends): SQLite and Redis
round-trip NUL, Postgres rejects it at write time. The explicit test at the
bottom pins that per-backend contract instead of letting the property suite
trip over the divergence at random.
"""

from __future__ import annotations

import itertools

import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from simplebroker._exceptions import OperationalError

from .helper_scripts.broker_factory import active_backend, make_queue

pytestmark = pytest.mark.shared

_uniq = itertools.count()

BODIES = st.text(alphabet=st.characters(exclude_characters="\x00"), max_size=300)

# Small override so the boundary is reachable with small generated bodies.
# The reject branch below doubles as proof the override is actually applied
# (the default limit is 10MB and would never reject these inputs).
SIZE_LIMIT_BYTES = 64


@given(body=BODIES)
@settings(
    # Function-scoped fixture reuse is intentional (isolation via unique
    # queue names); example counts come from the active profile.
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_bodies_round_trip_identically(queue_factory, body: str) -> None:
    q = queue_factory(f"body_{next(_uniq)}")
    q.write(body)
    # Empty string is a legal body; compare against None explicitly so an
    # empty-queue result can never masquerade as success.
    got = q.read_one()
    assert got is not None
    assert got == body


@given(body=st.text(alphabet=st.characters(exclude_characters="\x00"), max_size=40))
@example("a" * SIZE_LIMIT_BYTES)      # exactly at the limit: accepted
@example("a" * (SIZE_LIMIT_BYTES + 1))  # one byte over: rejected
@example("é" * 32)                    # 64 UTF-8 bytes in 32 chars: accepted
@example("é" * 33)                    # 66 bytes in 33 chars: rejected
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
def test_size_limit_counts_utf8_bytes(broker_target, body: str) -> None:
    q = make_queue(
        f"size_{next(_uniq)}",
        broker_target,
        config={"BROKER_MAX_MESSAGE_SIZE": SIZE_LIMIT_BYTES},
    )
    try:
        if len(body.encode("utf-8")) <= SIZE_LIMIT_BYTES:
            q.write(body)
            assert q.read_one() == body
        else:
            with pytest.raises(ValueError):
                q.write(body)
            assert q.read_one() is None  # the rejected write stored nothing
    finally:
        q.close()


def test_nul_byte_bodies_pinned_per_backend(queue_factory) -> None:
    """FINDING F6 (pre-verified 2026-06-11 on live Dockerized backends): NUL
    bodies round-trip on SQLite and Redis, but the Postgres backend rejects
    them at write time with OperationalError("PostgreSQL text fields cannot
    contain NUL (0x00) bytes") and stores nothing; the queue stays usable.
    Pinned per backend so any backend changing its NUL stance fails loudly."""
    q = queue_factory("nul_probe")
    if active_backend() == "postgres":
        with pytest.raises(OperationalError):
            q.write("a\x00b")
        assert q.read_one() is None
    else:
        q.write("a\x00b")
        assert q.read_one() == "a\x00b"
```

- [ ] **Step 2: Run on SQLite.**

Run: `uv run pytest tests/test_property_message_roundtrip.py -q`
Expected: PASS (3 tests).

- [ ] **Step 3: Sabotage check.** Temporarily change the config line to
  `config={"BROKER_MAX_MESSAGE_SIZE": SIZE_LIMIT_BYTES + 1}` and run the
  file. Expected: FAIL on the `@example("a" * (SIZE_LIMIT_BYTES + 1))` case —
  the broker now accepts a 65-byte body the test still expects to be
  rejected. Revert, re-run, confirm PASS.

- [ ] **Step 4: Run on Postgres and Redis.**

Run: `uv run ./bin/pytest-pg -q tests/test_property_message_roundtrip.py`
Run: `uv run ./bin/pytest-redis -q tests/test_property_message_roundtrip.py`
Expected: PASS on both, including both branches of the NUL test — this exact
divergence was verified on live backends while writing this plan (the PG
error comes from deliberate error translation in
`extensions/simplebroker_pg/simplebroker_pg/runner.py`). If a backend
disagrees with its pinned branch, that is *new* drift since 2026-06-11:
findings protocol, stop for maintainer triage before committing.

- [ ] **Step 5: Lint, format, commit.**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_message_roundtrip.py
git commit -m "Add cross-backend body fidelity and size-limit properties"
```

## Stage 3 — Stateful model-based test

This is the centerpiece: Hypothesis generates random *sequences* of queue
operations, a ~40-line plain-Python model predicts every result, and
invariants re-check counts after every step. The same machine runs unchanged
against SQLite, Postgres, and Redis — one model, three implementations, any
divergence is a bug in somebody.

Design decisions you must not casually change (each prevents a specific
failure mode):

- **Auto-vacuum off** (`{"BROKER_AUTO_VACUUM": 0}`): with it on, claimed rows
  vanish at implementation-chosen moments and `include_claimed` peeks /
  `stats().claimed` become unpredictable. The config-keyed session registry
  guarantees this override can't leak into, or be polluted by, other tests.
- **Unique queue-name prefix per machine execution** (`prop<N>_`): Postgres
  and Redis reset state per test *item*, but Hypothesis runs many machine
  executions inside one item. Prefixing isolates them; `teardown()` purges
  and closes so executions don't bloat the shared schema.
- **The model stores `(ts, body)` tuples** and relies on global timestamp
  monotonicity (asserted in the `write` rule), so list/`insort`/`sorted`
  comparisons never tie on `ts` and never compare bodies.

### Task 9: Machine skeleton — write, read_one, stats invariant

**Files:**
- Create: `tests/test_property_queue_model.py`

- [ ] **Step 1: Create the file** with this exact content:

```python
"""Stateful model-based test: real queues vs a plain-Python reference model.

Hypothesis drives random operation sequences against real Queue objects on
the active backend while a dict-of-lists model predicts every return value;
@invariant() re-derives stats/has_pending/exists from the model after every
step. Run it on all three backends (it is marked shared) and any semantic
divergence between SQLite, Postgres, and Redis shows up as a shrunk,
replayable operation script.

Read Part I of docs/plans/2026-06-11-hypothesis-property-testing-plan.md
(domain crash course + design decisions) before editing anything here.
"""

from __future__ import annotations

import itertools
from bisect import insort

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    rule,
    run_state_machine_as_test,
)

from simplebroker._targets import ResolvedTarget

from .helper_scripts.broker_factory import make_queue

pytestmark = pytest.mark.shared

QUEUE_KEYS = ("alpha", "bravo", "charlie")

# Printable-ASCII bodies keep shrunk failure scripts readable; full-Unicode
# body fidelity is already covered by test_property_message_roundtrip.py.
BODIES = st.text(alphabet=st.characters(min_codepoint=32, max_codepoint=126), max_size=20)

# Claimed rows must be deterministic for exact include_claimed/stats
# predictions; see the Stage 3 design notes in the plan.
MACHINE_CONFIG = {"BROKER_AUTO_VACUUM": 0}

_EXECUTIONS = itertools.count()


class QueueModelMachine(RuleBasedStateMachine):
    """One execution = one isolated trio of real queues + a reference model.

    Model representation: pending[key] and claimed[key] are lists of
    (ts, body) kept in ascending-ts order. FIFO == ascending ts, so
    "oldest pending" is always pending[key][0].
    """

    # Injected by the test wrapper below (the active backend's target).
    target: ResolvedTarget

    def __init__(self) -> None:
        super().__init__()
        prefix = f"prop{next(_EXECUTIONS)}"
        self._queues = {
            key: make_queue(f"{prefix}_{key}", self.target, config=MACHINE_CONFIG)
            for key in QUEUE_KEYS
        }
        self.pending: dict[str, list[tuple[int, str]]] = {k: [] for k in QUEUE_KEYS}
        self.claimed: dict[str, list[tuple[int, str]]] = {k: [] for k in QUEUE_KEYS}

    def teardown(self) -> None:
        # Purge (claimed rows included) so executions never see each other's
        # rows on backends whose state outlives a single execution.
        for q in self._queues.values():
            try:
                q.delete()
            finally:
                q.close()

    # ---------- model helpers ----------

    def _entries(self, key: str) -> list[tuple[int, str]]:
        return self.pending[key] + self.claimed[key]

    def _max_known_ts(self) -> int:
        return max((ts for k in QUEUE_KEYS for ts, _ in self._entries(k)), default=0)

    # ---------- rules ----------

    @rule(key=st.sampled_from(QUEUE_KEYS), body=BODIES)
    def write(self, key: str, body: str) -> None:
        q = self._queues[key]
        q.write(body)
        # Single-threaded, so meta.last_ts after our write IS our write's id.
        ts = q.refresh_last_ts()
        assert ts > self._max_known_ts(), "timestamps must be globally strictly increasing"
        self.pending[key].append((ts, body))

    @rule(key=st.sampled_from(QUEUE_KEYS))
    def read_one(self, key: str) -> None:
        got = self._queues[key].read_one(with_timestamps=True)
        if not self.pending[key]:
            assert got is None
        else:
            ts, body = self.pending[key].pop(0)
            assert got == (body, ts)
            insort(self.claimed[key], (ts, body))

    # ---------- invariants ----------

    @invariant()
    def counts_match_the_model(self) -> None:
        for key in QUEUE_KEYS:
            stats = self._queues[key].stats()
            n_pending = len(self.pending[key])
            n_claimed = len(self.claimed[key])
            assert (stats.pending, stats.claimed, stats.total) == (
                n_pending,
                n_claimed,
                n_pending + n_claimed,
            ), f"stats mismatch on {key!r}"
            assert self._queues[key].has_pending() == bool(self.pending[key])
            assert self._queues[key].exists() == bool(n_pending + n_claimed)


def test_queue_semantics_match_reference_model(broker_target) -> None:
    """Run the machine against the active backend (sqlite by default;
    bin/pytest-pg and bin/pytest-redis run the identical machine on
    Postgres and Redis)."""

    class Machine(QueueModelMachine):
        target = broker_target

    # Budget pinned regardless of HYPOTHESIS_PROFILE: executions x steps x
    # per-step invariants is the cost driver here, on three backends. Tune
    # these two numbers if a backend is slow (Task 13 step 3) — never delete
    # rules to save time.
    run_state_machine_as_test(
        Machine,
        settings=settings(
            max_examples=15,
            stateful_step_count=25,
            deadline=None,
            suppress_health_check=[HealthCheck.too_slow],
        ),
    )
```

- [ ] **Step 2: Run on SQLite.**

Run: `uv run pytest tests/test_property_queue_model.py -q`
Expected: PASS in a few seconds on SQLite (the full machine was measured at
~1s here; Postgres/Redis in Task 13 take minutes, mostly Docker startup).

- [ ] **Step 3: Sabotage check (mandatory — this validates the whole
  harness).** In `read_one`, temporarily change `self.pending[key].pop(0)` to
  `self.pending[key].pop()` (LIFO instead of FIFO). Run the file. Expected:
  FAIL, with Hypothesis printing a minimal falsifying script of roughly:

```text
state.write(key='alpha', body='')
state.write(key='alpha', body='')
state.read_one(key='alpha')
```

  (Bodies shrink to empty strings; the mismatch shows up in the *timestamps*
  — the broker returned the older message's id, your broken model predicted
  the newer one's.) That is the machine proving real queues are FIFO while
  the model predicted LIFO. Revert the sabotage, re-run, confirm PASS.

- [ ] **Step 4: Commit.**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_queue_model.py
git commit -m "Add stateful queue model machine with write/read/stats coverage"
```

### Task 10: Batch reads and (include_claimed) peeks

**Files:**
- Modify: `tests/test_property_queue_model.py` (add rules inside
  `QueueModelMachine`, after `read_one`)

- [ ] **Step 1: Add the two rules:**

```python
    @rule(key=st.sampled_from(QUEUE_KEYS), limit=st.integers(min_value=1, max_value=5))
    def read_many(self, key: str, limit: int) -> None:
        got = self._queues[key].read_many(limit, with_timestamps=True)
        expected = self.pending[key][:limit]
        assert got == [(body, ts) for ts, body in expected]
        del self.pending[key][: len(expected)]
        for entry in expected:
            insort(self.claimed[key], entry)

    @rule(key=st.sampled_from(QUEUE_KEYS), include_claimed=st.booleans())
    def peek_is_nondestructive_and_exact(self, key: str, include_claimed: bool) -> None:
        got = self._queues[key].peek_many(
            limit=1000, with_timestamps=True, include_claimed=include_claimed
        )
        rows = self.pending[key] + (self.claimed[key] if include_claimed else [])
        # Merged in ascending message-ID order; sorted() never ties because
        # timestamps are globally unique.
        assert got == [(body, ts) for ts, body in sorted(rows)]
        # Deliberately no model mutation: if a peek ever claimed or deleted
        # anything, the very next counts_match_the_model invariant fails.
```

- [ ] **Step 2: Run on SQLite.**

Run: `uv run pytest tests/test_property_queue_model.py -q`
Expected: PASS. (When Task 13 later runs this same rule set on Postgres and
Redis, that run becomes a mechanical re-verification of the 4.6.0
`include_claimed` surface across all backends.)

- [ ] **Step 3: Commit.**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_queue_model.py
git commit -m "Cover batch reads and include_claimed peeks in the queue model"
```

### Task 11: Moves — oldest-pending and claimed-by-ID

**Files:**
- Modify: `tests/test_property_queue_model.py` (add inside the machine, after
  the peek rule)

- [ ] **Step 1: Add a claimed-entry helper and the two move rules.** First
  extend the `hypothesis.stateful` import at the top of the file with
  `precondition` (it becomes used now):

```python
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)
```

  Then add, inside the machine after the peek rule:

```python
    def _claimed_triples(self) -> list[tuple[str, int, str]]:
        return [
            (key, ts, body)
            for key in QUEUE_KEYS
            for ts, body in self.claimed[key]
        ]

    @rule(data=st.data())
    def move_oldest_pending(self, data: st.DataObject) -> None:
        src = data.draw(st.sampled_from(QUEUE_KEYS), label="src")
        dst = data.draw(
            st.sampled_from([k for k in QUEUE_KEYS if k != src]), label="dst"
        )
        got = self._queues[src].move_one(self._queues[dst].name, with_timestamps=True)
        if not self.pending[src]:
            assert got is None
        else:
            ts, body = self.pending[src].pop(0)
            assert got == (body, ts)
            # The message keeps its original timestamp, so an old message can
            # jump ahead of newer ones at the destination — insort, not append.
            insort(self.pending[dst], (ts, body))

    @precondition(lambda self: self._claimed_triples())
    @rule(data=st.data())
    def move_claimed_by_id_redelivers(self, data: st.DataObject) -> None:
        src, ts, body = data.draw(
            st.sampled_from(self._claimed_triples()), label="claimed message"
        )
        dst = data.draw(
            st.sampled_from([k for k in QUEUE_KEYS if k != src]), label="dst"
        )
        got = self._queues[src].move_one(
            self._queues[dst].name,
            exact_timestamp=ts,
            require_unclaimed=False,
            with_timestamps=True,
        )
        assert got == (body, ts)
        self.claimed[src].remove((ts, body))
        # Moving resets the claim (SQL: SET queue = ?, claimed = 0), i.e. a
        # consumed message becomes deliverable again at the destination.
        insort(self.pending[dst], (ts, body))
```

- [ ] **Step 2: Run on SQLite.**

Run: `uv run pytest tests/test_property_queue_model.py -q`
Expected: PASS.

- [ ] **Step 3: Sabotage check.** In `move_claimed_by_id_redelivers`,
  temporarily change the final line to
  `insort(self.claimed[dst], (ts, body))` (modeling "stays claimed"). Run;
  expected FAIL — the invariant (or a subsequent peek) catches that the real
  message arrived *pending*. The shrunk script should be approximately:
  write → read_one → move_claimed_by_id_redelivers. Revert, re-run, PASS.

- [ ] **Step 4: Commit.**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_queue_model.py
git commit -m "Cover pending and claimed-by-id moves in the queue model"
```

### Task 12: Deletes and bounded reads

**Files:**
- Modify: `tests/test_property_queue_model.py` (add inside the machine, after
  the move rules)

- [ ] **Step 1: Add the purge, exact-ID delete, and bounded-read rules:**

```python
    @rule(key=st.sampled_from(QUEUE_KEYS))
    def purge(self, key: str) -> None:
        had_rows = bool(self._entries(key))
        assert self._queues[key].delete() == had_rows
        self.pending[key].clear()
        self.claimed[key].clear()

    @precondition(lambda self: any(self._entries(k) for k in QUEUE_KEYS))
    @rule(data=st.data())
    def delete_exact_ids(self, data: st.DataObject) -> None:
        key = data.draw(
            st.sampled_from([k for k in QUEUE_KEYS if self._entries(k)]),
            label="queue",
        )
        chosen = data.draw(
            st.lists(st.sampled_from(self._entries(key)), min_size=1, unique=True),
            label="targets",
        )
        ids = [ts for ts, _ in chosen]
        if data.draw(st.booleans(), label="also pass a missing id"):
            # 1 is never a real hybrid timestamp (real ones are ~1.8e18);
            # missing ids must be ignored, not counted.
            ids.append(1)
        assert self._queues[key].delete_many(ids) == len(chosen)
        for entry in chosen:
            bucket = self.pending[key] if entry in self.pending[key] else self.claimed[key]
            bucket.remove(entry)

    @precondition(lambda self: any(self._entries(k) for k in QUEUE_KEYS))
    @rule(data=st.data())
    def read_after_bound(self, data: st.DataObject) -> None:
        key = data.draw(st.sampled_from(QUEUE_KEYS), label="queue")
        known_ts = [ts for k in QUEUE_KEYS for ts, _ in self._entries(k)]
        bound = data.draw(st.sampled_from(known_ts), label="bound")
        got = self._queues[key].read(after_timestamp=bound, with_timestamps=True)
        matches = [(ts, body) for ts, body in self.pending[key] if ts > bound]
        if not matches:
            assert got is None
        else:
            ts, body = matches[0]
            assert got == (body, ts)
            self.pending[key].remove((ts, body))
            insort(self.claimed[key], (ts, body))
```

- [ ] **Step 2: Run on SQLite, twice.**

Run: `uv run pytest tests/test_property_queue_model.py -q` (then run it again)
Expected: PASS both times (the second run replays the example database plus
fresh examples — a cheap flakiness probe before involving Docker).

- [ ] **Step 3: Sabotage check (strictness of the bound).** In
  `read_after_bound`, temporarily change `if ts > bound` to `if ts >= bound`.
  Run; expected FAIL (the API's `after_timestamp` is strictly greater-than;
  your model now wrongly predicts the bound message itself is eligible).
  Revert, re-run, PASS.

- [ ] **Step 4: Commit.**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_queue_model.py
git commit -m "Cover purges, exact-id deletes, and bounded reads in the queue model"
```

### Task 13: Cross-backend gate for the machine

- [ ] **Step 1: Postgres.**

Run: `uv run ./bin/pytest-pg -q tests/test_property_queue_model.py`
Expected: PASS in ~1–3 minutes. Failure modes and what they mean:
  - A shrunk operation script with a value mismatch → genuine SQLite/Postgres
    semantic drift. Findings protocol; this is the suite doing its job. Stop
    and triage with the maintainer before any strategy-narrowing.
  - `stats.claimed` mismatches that mention vacuum, or claimed rows
    disappearing → `MACHINE_CONFIG` is not reaching the backend; verify the
    `make_queue(..., config=MACHINE_CONFIG)` line survived your edits before
    suspecting the backend.

- [ ] **Step 2: Redis.**

Run: `uv run ./bin/pytest-redis -q tests/test_property_queue_model.py`
Expected: PASS. (Redis has no auto-vacuum at all — its `vacuum()` is explicit
only — so claimed-row determinism holds there regardless of config.)

- [ ] **Step 3: Wall-clock sanity.** Note the runtime of each backend run. If
  Postgres or Redis exceeded ~5 minutes, lower the budget in the test wrapper
  to `max_examples=10, stateful_step_count=20` and re-run all three backends.
  Record what you chose in the commit message. Do not go below 10×20 —
  shrink the budget, never the rule set.

- [ ] **Step 4: Commit (only if Step 3 changed the budget; otherwise nothing
  changed in this task).**

```bash
uv run ruff check --fix simplebroker tests && uv run ruff format simplebroker tests
git add tests/test_property_queue_model.py
git commit -m "Tune queue-model example budget for backend runtimes"
```

## Stage 4 — Documentation and final gates

### Task 14: README and CHANGELOG

**Files:**
- Modify: `README.md` (development/testing command list, around line 1854)
- Modify: `CHANGELOG.md` (top, after the intro paragraphs)

- [ ] **Step 1: README.** Find the development testing command block (search
  for `uv run ./bin/pytest-redis`). Directly after that line, add:

```bash
HYPOTHESIS_PROFILE=ci uv run pytest tests/test_property_*.py  # deeper property-test run (50 -> 200 examples per property)
```

Second, fix a pre-existing stale comment in the same command block (flagged
during plan review): `bin/pytest-redis` starts a Valkey container itself —
verified live while preparing this plan — but its comment predates that.
Replace the line

```bash
uv run ./bin/pytest-redis  # Redis/Valkey extension tests; requires a local test server
```

with

```bash
uv run ./bin/pytest-redis  # All Redis-backed tests with automatic Docker setup/teardown (Valkey)
```

Third, after the command block, add this sentence as a new paragraph:

> Property-based tests (`tests/test_property_*.py`, powered by Hypothesis)
> check parser totality/round-trips and run a stateful model of queue
> semantics against every backend; failures print a `@reproduce_failure`
> blob that replays the exact case.

- [ ] **Step 2: CHANGELOG.** Directly above the `## [4.6.0] - 2026-06-10`
  line, insert:

```markdown
## [Unreleased]
### Added
- Property-based test suite (Hypothesis): timestamp-parser totality and
  round-trip properties, cross-backend queue-name/body round-trip properties,
  and a stateful reference-model test that runs identical operation sequences
  against the SQLite, Postgres, and Redis backends.

### Fixed
- `TimestampGenerator.validate()` now raises `TimestampError` (as documented)
  instead of leaking `ValueError` for ISO dates beyond the year-2262
  timestamp horizon. CLI behavior is unchanged.
```

- [ ] **Step 3: Commit.**

```bash
git add README.md CHANGELOG.md
git commit -m "Document the property-based test suite"
```

### Task 15: Final gate (all backends, full suite)

- [ ] **Step 1: Lint/format/type final pass.**

Run: `uv run ruff check simplebroker tests && uv run ruff format --check simplebroker tests && uv run mypy simplebroker`
Expected: all clean (fix and amend the relevant commit if not).

- [ ] **Step 2: Full default suite.**

Run: `uv run pytest -q`
Expected: PASS, no new warnings.

- [ ] **Step 3: Full backend wrappers (property files plus their stage-2/3
  shared siblings are the new load; the rest is regression assurance).**

Run: `uv run ./bin/pytest-pg -q`
Run: `uv run ./bin/pytest-redis -q`
Expected: PASS. These run the entire shared suite and take a while; this is
the once-per-plan full check.

- [ ] **Step 4: CI-profile pass over just the property files.**

Run: `HYPOTHESIS_PROFILE=ci uv run pytest tests/test_property_timestamp_validate.py tests/test_property_queue_names.py tests/test_property_message_roundtrip.py tests/test_property_queue_model.py -q`
Expected: PASS. (The CI profile deepens the `@given` properties to 200
examples; the state machine deliberately pins its own budget and is
unaffected.) Any counterexample found only at this depth: findings protocol,
then green before finishing.

- [ ] **Step 5: Review the findings log.** Confirm every `FINDING` comment in
  the four test files has a corresponding row, F1 is marked fixed, and F6
  still matches the per-backend behavior you observed in Task 8. The log is
  the deliverable the maintainer triages; leave it accurate.

- [ ] **Step 6: Hand off.** Summarize for the maintainer: tests added, the
  one production fix, findings F2–F6 awaiting disposition, and the chosen
  machine budget. Do not start fixing F2–F6.

---

# Appendix — Decisions already made (do not relitigate during implementation)

- **No concurrency properties.** Hypothesis is deterministic and
  single-threaded; `tests/test_concurrency.py`, `test_thread_safety.py`, and
  the watcher race tests already cover that ground with purpose-built
  machinery.
- **No CLI-level property tests.** Subprocess-per-example is ~100× slower for
  near-zero extra coverage: the CLI shares the exact code paths the API
  properties already pin (e.g. `commands._validate_timestamp` is a thin
  wrapper over the `validate()` we test directly).
- **No private codec tests.** `_encode/_decode_hybrid_timestamp` laws are
  exercised through `validate()` round-trips and the machine's monotonicity
  assertion; reaching into instance privates would couple tests to
  implementation for no added contract coverage.
- **No `vacuum()` rule in the machine.** With auto-vacuum disabled, an
  explicit-vacuum rule would only assert "claimed rows disappear", which the
  purge rule already proves deletable; modeling vacuum's threshold behavior
  is implementation detail, not public contract.
- **No Hypothesis tests inside `extensions/*/tests/`.** Cross-backend
  coverage flows through the `shared` marker + wrappers; duplicating property
  files per extension would violate DRY and drift.
