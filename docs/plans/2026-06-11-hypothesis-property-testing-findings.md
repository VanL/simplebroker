# Property-testing findings log

Discovered-by-Hypothesis behaviors that contradict documentation or look
unintended. Tests PIN current behavior; nothing here changes production code
without maintainer sign-off.

**Triage complete 2026-06-11:** F1/F2/F4/F5/F8/F9 fixed, F3/F6/F7 documented
(dispositions per row below). Nothing remains open.

| ID | Behavior (current, verified) | Repro | Where | Suggested disposition |
|----|------------------------------|-------|-------|----------------------|
| F1 | `validate("9999-01-01")` leaks `ValueError` instead of documented `TimestampError` | `TimestampGenerator.validate("9999-01-01")` | `simplebroker/_timestamp.py:465` + uncaught at `:300` | FIXED — validate() wraps ISO parser errors as TimestampError (CHANGELOG [Unreleased]) |
| F2 | Pre-epoch ISO dates parse to negative ints that all bound checks then reject | `validate("0001-01-01")` → −62135596800000000000 | `simplebroker/_timestamp.py:418-466` | FIXED — pre-epoch ISO clamps to the Unix epoch, unifying CLI (accepted by accident) and API (rejected) behavior (commit 154c393) |
| F3 | Non-ASCII Unicode digits accepted everywhere `int()`/`isdigit()` are used | `validate("١٢٣s")` → 122999996416 | `simplebroker/_timestamp.py:339-383,323-336` | DOCUMENTED — validate() docstring notes digits may be any Unicode decimals accepted by int() |
| F4 | Trailing-newline queue names accepted and functional (`$` + `.match()` quirk) | `Queue("abc\n").write(...)` works | `simplebroker/db.py:81,185` | FIXED — validation uses `fullmatch`, matching the prefix validator's existing strictness (commit 6808672) |
| F5 | Invalid queue names raise `ValueError`; docstrings promise `QueueNameError` | `Queue("bad name!").write("x")` | `simplebroker/db.py:949` vs `sbqueue.py` docstrings | FIXED — `QueueNameError(BrokerError, ValueError)` now raised; dual inheritance keeps `except ValueError` callers working (commit 99aedd5) |
| F6 | NUL bodies diverge: round-trip on SQLite/Redis; Postgres rejects at write with `OperationalError: PostgreSQL text fields cannot contain NUL (0x00) bytes` | `Queue.write("a\x00b")` under pytest-pg | `extensions/simplebroker_pg/simplebroker_pg/runner.py` (deliberate error translation) | DOCUMENTED — README limits list (JSON payloads immune: serializers escape NUL; jsonb-cast caveat noted); pinned per-backend in tests |

## New findings

(append rows here as F7, F8, ... per the protocol in the plan, Part I §9)

| ID | Behavior (current, verified) | Repro | Where | Suggested disposition |
|----|------------------------------|-------|-------|----------------------|
| F7 | 8-digit bare integers forming a valid YYYYMMDD date are parsed as ISO dates, not the documented unix seconds: `validate("10550401")` → −28866844800000000000 (year 1055, see F2) and `validate("25980801")` → `TimestampError` (year 2598 > 2262 horizon), while `validate("10550401s")` → 10550400999997440. Discovered by `test_unit_suffixes_and_bare_seconds_agree` at 200 examples (`@reproduce_failure('6.155.2', b'AEQAoPyB')`). | `TimestampGenerator.validate("10550401")` | `simplebroker/_timestamp.py` `_parse_iso8601` — the `len == 8 and isdigit` YYYYMMDD heuristic runs before the numeric heuristic; docstring calls ISO "unambiguous" | DOCUMENTED — validate() docstring states the precedence (8-digit bare numerals read as YYYYMMDD; use the `s` suffix for second counts); pinned in tests. Code left unchanged: every reinterpretation silently moves inputs between meanings |
| F8 | Message bodies containing lone surrogates raise `UnicodeEncodeError` at write time (from `len(message.encode("utf-8"))` in the size check) — an exception type no docstring mentions (write promises QueueNameError/MessageError/OperationalError). Discovered when property shrinking surfaced `body='\ud800'`. | `Queue("q", db_path=...).write("\ud800")` | `simplebroker/db.py:1081` `_validate_message_size` | FIXED — `MessageError(BrokerError, ValueError)` raised with the UnicodeEncodeError as `__cause__`; oversize bodies also raise MessageError now (commit 99aedd5) |
| F9 | Dev tooling: flag-only wrapper invocations (`bin/pytest-pg -q`, no paths) misroute the flag into the extension phase, so `extension_pytest_args or [default-path]` skips the default path; the phase then runs `-m pg_only` against `tests/`, collects nothing, and the wrapper exits 5. Path-ful and no-arg invocations work. | `uv run ./bin/pytest-pg -q` → exit 5, "no tests collected" phase | `simplebroker/_scripts.py` `_route_pytest_args` + the `or [...default path...]` fallbacks (~:476-492) | FIXED — `_with_default_suite_path` applies suite defaults when no target classifies, also covering `-k value` flag values (commit b026733). `bin/pytest-redis` already had its own helper for the flag-only case (its simpler check still treats `-k` values as targets — residual cosmetic edge) |
