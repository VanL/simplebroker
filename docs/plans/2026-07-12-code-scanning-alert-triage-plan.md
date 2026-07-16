# Code Scanning Alert Remediation and Accepted-Risk Plan

## Purpose

This plan resolves the ten open GitHub code-scanning alerts without treating the
OpenSSF Scorecard as a grade to maximize. It follows the repository's existing
remediation pattern: verify each finding against the code, add regression evidence
before changing behavior, fix concrete defects at the narrowest chokepoint, and
record false positives or disproportionate controls as explicit exceptions.

There are four outcomes:

1. Fix the two real credential-output findings: CodeQL alerts `#28` and `#35`.
2. Remove the four unlocked CI install paths: Scorecard alerts `#19`, `#25`,
   `#32`, and `#33`.
3. Dismiss CodeQL alert `#29` as a verified sanitizer false positive.
4. Dismiss Scorecard alerts `#1`, `#20`, and `#22` as accepted solo-maintainer
   policy choices, while adding a narrow green-CI gate for automated Dependabot
   merges.

The plan also restores trustworthy scanning before using a new scan as closure
evidence. At planning time, the most recent successful alert-producing scan is
for commit `9e48762415ddc4333120093dab18d6640b5dbda1`, while GitHub's `main` is
`d0056c5cc937449aaac08e3fcd7cecb4da679139`. The live CodeQL workflow mixes
`init` v4.36.3 with `analyze` v4.37.0 and fails before analysis. Open PR `#47`
does not currently repair the repository: its CodeQL and Windows 3.14 checks are
red.

Assume the implementer is a skilled developer with no prior alert-triage context.
Follow the tasks in order. Do not dismiss an alert before the evidence named here
exists on the default branch.

## Alert Disposition

| Alert | Finding | Disposition | Reason |
|---|---|---|---|
| `#28` | `_run()` prints a command containing `POSTGRES_PASSWORD` | Fix | A test password can be supplied through the environment and is echoed into terminals or CI logs. |
| `#35` | Backend benchmark prints a raw Postgres DSN | Fix | The benchmark is test-only, but its Docker password is configurable and should not be printed. |
| `#29` | Redacted PG test DSN is reported as clear text | Dismiss: false positive | `redact_backend_target()` is a deliberate sanitizer with focused adversarial tests; CodeQL does not model it. |
| `#19` | Packaging workflow runs `pip install build` | Fix | The install is redundant; packaging smoke already controls its build invocation. |
| `#25`, `#32`, `#33` | Fuzz workflow upgrades pip and installs unlocked packages/project | Fix | Fuzz dependencies should come from the checked-in `uv.lock`, including hashes. |
| `#1` | Branch protection is not maximal | Dismiss: won't fix | The active ruleset intentionally prevents deletion and non-fast-forward updates. Mandatory PRs and reviews are disproportionate for one maintainer. Automated merges receive a separate green-CI gate in this plan. |
| `#20` | No approved changes among the last 30 | Dismiss: won't fix | Independent human approval cannot be required when the sole active maintainer authors the change. Revisit if a second active maintainer joins. |
| `#22` | No OpenSSF Best Practices badge | Dismiss: won't fix | The badge measures formal multi-person OSS governance. Its low-risk maturity signal is not proportional to this project. |

## Repository Precedents To Preserve

- `docs/plans/2026-07-05-independent-review-fixes-plan.md` distinguishes real
  findings from false positives and treats `_targets.py` as the single redaction
  chokepoint.
- `tests/test_target_redaction.py` uses adversarial observable-output assertions:
  the secret must be absent and the useful target must remain.
- `tests/test_dev_scripts.py` tests developer entry points through captured output
  and small fakes at subprocess boundaries.
- `.github/scripts/require_green_workflows.py` and
  `tests/test_release_workflow_gate.py` already implement and test a fail-closed
  sibling-workflow gate. Reuse them instead of adding a second GitHub API client.
- `.github/dependabot.yml` and all workflow `uses:` references favor automated
  updates, full SHA pins, and minimal job permissions.
- `SECURITY.md` defines package, release, and supply-chain integrity as the
  security scope. Alert dispositions should be evaluated against that scope.

## Locked Decisions

### Do not pursue a Scorecard 10/10

The Scorecard remains useful as a generic control catalog, but its project model
is not authoritative for this repository. Keep collecting and publishing the
full Scorecard result. Do not filter SARIF, hide checks, add an OpenSSF badge, add
nominal reviewers, or manufacture review evidence solely to improve the score.

### Keep the current destructive-update ruleset

Do not weaken or remove the active repository ruleset that blocks deletion and
non-fast-forward updates to the default branch. Those controls are cheap and
useful for a solo project. Do not add required PRs, required human approvals,
code-owner review, or administrator review enforcement in this slice.

### Gate automated merges without changing the human workflow

The practical problem is not the absence of team-style review. It is that the
current Dependabot job enables a merge without requiring sibling workflows to
pass. Reuse `require_green_workflows.py` to require the `Test`, `Test Postgres
Extension`, `Test Redis Extension`, and `CodeQL` workflows for the Dependabot PR
head SHA, then merge. Human-authored updates remain governed by the current
solo-maintainer workflow.

### Keep credentials out of command arguments and output

Do not teach generic `_run()` logging to guess which arbitrary arguments are
secret. For the Docker password, pass `--env POSTGRES_PASSWORD` and place the
value only in the subprocess environment. Keep using `redact_backend_target()`
where connection details are intentionally useful. The benchmark does not need
connection details, so print a constant `[redacted]` marker there instead of
passing sensitive data through another analyzer-visible sink.

### Use the repository lock, not hand-maintained requirements hashes

Add Atheris to a root `uv` dependency group and regenerate `uv.lock`. Run fuzzing
from a frozen sync. Do not add a parallel `requirements-fuzz.txt`, duplicate
transitive hashes, or exact version pins in workflow YAML. Dependabot already
maintains the root `uv` lock.

## Task 0: Establish A Current Baseline

### Goal

Start implementation from the current GitHub default branch and preserve an
auditable before-state.

### Steps

1. Fetch `origin`, confirm the working tree is clean, and create an implementation
   branch from the current `origin/main`, not the planning checkout's stale
   tracking ref.
2. Record:

   ```bash
   git rev-parse HEAD
   gh api --paginate \
     '/repos/VanL/simplebroker/code-scanning/alerts?state=open&per_page=100' \
     --jq '.[] | [.number, .tool.name, .rule.id, .most_recent_instance.commit_sha] | @tsv'
   gh run list --repo VanL/simplebroker --branch main --limit 20
   ```

3. Run the focused baseline:

   ```bash
   uv run pytest -q -n 0 \
     tests/test_target_redaction.py \
     tests/test_dev_scripts.py \
     tests/test_backend_benchmark_smoke.py \
     tests/test_release_workflow.py \
     tests/test_release_workflow_gate.py
   uv run ruff check simplebroker/_scripts.py tests/backend_benchmark.py \
     tests/test_dev_scripts.py tests/test_backend_benchmark_smoke.py \
     tests/test_release_workflow.py .github/scripts/require_green_workflows.py
   ```

4. Save the exact messages and locations for alerts `#1`, `#19`, `#20`, `#22`,
   `#25`, `#28`, `#29`, `#32`, `#33`, and `#35` in the implementation PR body.

### Gate

The implementer can explain which alert-producing scan is current, why the live
CodeQL workflow fails, and why no alert has yet been dismissed.

## Task 1: Restore Trustworthy CodeQL And Scorecard Runs

### Primary files

- `.github/workflows/codeql.yml`
- `.github/workflows/scorecard.yml`
- `.github/dependabot.yml`
- `tests/test_release_workflow.py`

### Tests first

Add workflow-structure tests that fail on the current default branch:

1. Every `github/codeql-action/*` reference in `codeql.yml` and `scorecard.yml`
   uses one full SHA and one documented version.
2. `codeql.yml` still grants only `contents: read` and job-local
   `security-events: write`.
3. `scorecard.yml` has `workflow_dispatch` in addition to push and schedule, so
   the fixed/default-branch state can be verified immediately.
4. Dependabot groups `github/codeql-action/*` updates into one PR so `init`,
   `analyze`, and `upload-sarif` cannot be partially upgraded again.

### Implementation

1. Align `init`, `analyze`, and `upload-sarif` on the same current v4.37.0 full
   SHA. Do not use a tag.
2. Add a GitHub Actions dependency group for `github/codeql-action/*`.
3. Add `workflow_dispatch` to the Scorecard workflow without changing its
   schedule, permissions, artifact, or `publish_results` behavior.
4. Supersede rather than merge PR `#47` once the replacement change is open.
   Record the superseding PR in the close comment.

### Gate

```bash
uv run pytest -q -n 0 tests/test_release_workflow.py
uv run ruff check tests/test_release_workflow.py
```

On the implementation PR, `Analyze Python` must execute the query suite and
upload results successfully. A configuration-error SARIF upload is not a pass.

## Task 2: Remove Real Credential Output (`#28`, `#35`)

### Primary files

- `simplebroker/_scripts.py`
- `tests/backend_benchmark.py`
- `tests/test_dev_scripts.py`
- `tests/test_backend_benchmark_smoke.py`

### Red tests first

1. In `tests/test_dev_scripts.py`, set a distinctive password such as
   `command-output-secret`, invoke `_start_postgres_container()` through a small
   `_run` fake, and assert:
   - no command argument contains the secret;
   - Docker receives `--env POSTGRES_PASSWORD` without `=<value>`;
   - the subprocess environment contains the exact password;
   - user/database variables remain present and container cleanup behavior is
     unchanged.
2. In `tests/test_backend_benchmark_smoke.py`, provision a fake Docker DSN with a
   distinctive password, enter `_postgres_dsn_for_benchmark()`, and assert the
   captured output is the constant `[redacted]` message and never the secret.
3. Keep `test_pytest_pg_main_redacts_dsn_password` green. It is the regression
   evidence for the sink reported by false-positive alert `#29`.

### Implementation

1. In `_start_postgres_container()`, copy the process environment and set
   `POSTGRES_PASSWORD`, `POSTGRES_USER`, and `POSTGRES_DB` in that copy.
2. Change the Docker argument list from `--env NAME=value` to `--env NAME` and
   pass the copied environment to `_run()`.
3. In the benchmark harness, print a constant `[redacted]` marker rather than
   passing the DSN through the output sink. CodeQL does not model the custom
   sanitizer, and the benchmark does not need host or database details.
4. Do not change the DSN returned to tests or the environment passed to backend
   subprocesses. Only the display and Docker command boundary change.

### Gates

```bash
uv run pytest -q -n 0 \
  tests/test_target_redaction.py \
  tests/test_dev_scripts.py \
  tests/test_backend_benchmark_smoke.py
uv run ruff check simplebroker/_scripts.py tests/backend_benchmark.py \
  tests/test_dev_scripts.py tests/test_backend_benchmark_smoke.py
uv run mypy simplebroker
```

Then run a Docker-backed smoke with a non-default test password and capture the
log. The secret must not appear in the command echo, DSN display, or pytest
output.

## Task 3: Put Fuzzing And Packaging On The Lock (`#19`, `#25`, `#32`, `#33`)

### Primary files

- `pyproject.toml`
- `uv.lock`
- `.github/workflows/fuzz.yml`
- `.github/workflows/test.yml`
- `fuzz/fuzz_timestamp_validate.py`
- `fuzz/fuzz_dump_load.py`
- `tests/test_release_workflow.py`

### Tests first

Add workflow assertions that fail on the current files:

1. `fuzz.yml` contains no `pip install`, `python -m pip`, or pip self-upgrade.
2. The fuzz install is a frozen `uv` sync using the named fuzz dependency group.
3. The fuzz command uses the already-synced environment and cannot update the
   lock.
4. The packaging job contains no standalone `pip install build` step.
5. The root project declares Atheris in a fuzz-only dependency group; it is not a
   runtime dependency or a normal developer requirement on unsupported systems.

### Implementation

1. Add a root `[dependency-groups]` entry named `fuzz` containing the supported
   Atheris floor. Continue using the existing `dev` extra for Hypothesis and
   pytest.
2. Regenerate `uv.lock` and confirm Atheris artifacts and hashes are present.
3. Replace the three pip commands in `fuzz.yml` with:
   - `uv sync --frozen --extra dev --group fuzz` during installation;
   - `uv run --frozen --no-sync ...` for the selected harness.
4. Update both fuzz module usage comments to show the same locked `uv` commands.
5. Delete the redundant `python -m pip install build` workflow step. Do not add a
   new network install in its place. The existing packaging-smoke entry point is
   responsible for its build tool invocation.
6. If `packaging_smoke_main()` cannot run from a clean checkout after that
   deletion, change its build invocation to consume the repository lock. Do not
   reintroduce an unlocked pip install merely to make CI green.

### Gates

```bash
uv lock --check
uv sync --frozen --extra dev --group fuzz
uv run --frozen --no-sync python -c \
  'import atheris, hypothesis, pytest, simplebroker'
uv run pytest -q -n 0 tests/test_release_workflow.py tests/test_dev_scripts.py
uv run ./bin/packaging-smoke --python 3.11
uv run ruff check fuzz tests/test_release_workflow.py simplebroker/_scripts.py
```

Run each fuzz harness with a short bounded duration, not the full scheduled
15-minute budget, to prove startup, corpus loading, and imports.

## Task 4: Gate Dependabot Without Adding Team-Scale Branch Rules

### Primary files

- `.github/workflows/dependabot.yml`
- `.github/scripts/require_green_workflows.py` only if a real reuse gap is found
- `tests/test_release_workflow.py`
- `tests/test_release_workflow_gate.py` only if the shared gate changes

### Tests first

Add workflow assertions proving that the Dependabot job:

1. has `actions: read`, `contents: write`, and `pull-requests: write`, with no
   broader permission;
2. checks out the repository before invoking the existing gate script;
3. passes `${{ github.event.pull_request.head.sha }}` explicitly rather than the
   synthetic pull-request merge SHA;
4. requires `Test`, `Test Postgres Extension`, `Test Redis Extension`, and
   `CodeQL`;
5. merges only after the gate step and no longer calls `gh pr merge --auto`.

### Implementation

1. Reuse `.github/scripts/require_green_workflows.py` in the Dependabot job.
   Do not copy its API/polling logic into YAML.
2. Add `actions: read` to the job permissions.
3. Gate only supported minor and patch updates, preserving the existing metadata
   condition.
4. After the gate returns success, run `gh pr merge --merge "$PR_URL"`.
5. If any required workflow is missing, pending past the timeout, cancelled, or
   red, fail without merging.

The gate workflow itself is not in the required list, so there is no cycle.
This control applies only to automated dependency changes and does not alter the
default-branch ruleset or require human review.

### Gates

```bash
uv run pytest -q -n 0 \
  tests/test_release_workflow.py \
  tests/test_release_workflow_gate.py
uv run ruff check .github/scripts/require_green_workflows.py \
  tests/test_release_workflow.py tests/test_release_workflow_gate.py
```

The implementation PR must also demonstrate one negative case: a test fixture or
controlled run with a failed required workflow refuses to merge.

## Task 5: Merge Fixes And Produce Fresh Analyzer Evidence

### Steps

1. Run all final local gates below.
2. Open one focused PR. In its body, map every alert number to `fixed`, `false
   positive`, or `accepted policy`, and link the exact evidence.
3. Require the implementation PR's tests and CodeQL analysis to be green before
   merge. Do not use the currently unsafe Dependabot auto-merge path.
4. After merge, manually dispatch both workflows if a default-branch run was not
   created automatically:

   ```bash
   gh workflow run CodeQL --repo VanL/simplebroker --ref main
   gh workflow run 'OSSF Scorecard' --repo VanL/simplebroker --ref main
   ```

5. Wait for successful default-branch runs and confirm they analyzed the current
   `main` SHA.
6. Verify that `#28`, `#35`, `#19`, `#25`, `#32`, and `#33` moved to `fixed`.
   Do not dismiss these to make the dashboard clean; a fix must close them.

### Gate

The current default-branch SHA has successful `CodeQL` and `OSSF Scorecard`
runs, and all six implementation findings are closed as fixed.

## Task 6: Record Deliberate Dismissals

Dismiss only after Tasks 1 through 5 are complete, so the comments can cite
evidence that exists on `main`.

### Alert `#29`: false positive

Use reason `false positive` and this substance in the comment:

> The DSN is passed through `redact_backend_target()` before this print sink.
> `tests/test_target_redaction.py` covers standard, reserved-character,
> percent-encoded, malformed URL, and conninfo passwords, and
> `test_pytest_pg_main_redacts_dsn_password` asserts the raw secret is absent from
> observable output. The separate raw benchmark and Docker-command sinks were
> fixed under alerts #35 and #28.

### Alert `#1`: won't fix

Use reason `won't fix` and this substance in the comment:

> Accepted solo-maintainer policy. The active default-branch ruleset blocks
> deletion and non-fast-forward updates without bypass. Mandatory PR-only
> updates, independent approvals, and maximal branch protection are not
> proportional for a one-maintainer repository. Automated Dependabot merges are
> separately fail-closed on Test, Test Postgres Extension, Test Redis Extension,
> and CodeQL in `.github/workflows/dependabot.yml`.

### Alert `#20`: won't fix

Use reason `won't fix` and this substance in the comment:

> Accepted solo-maintainer constraint. Independent human approval cannot be
> required when the sole active maintainer authors a change. CI, CodeQL, fuzzing,
> release gates, and automated dependency updates remain in place. Revisit this
> decision if a second active maintainer joins the project.

### Alert `#22`: won't fix

Use reason `won't fix` and this substance in the comment:

> Accepted scope mismatch. The OpenSSF Best Practices badge measures formal OSS
> governance and includes multi-person project assumptions that are not
> proportional to this solo-maintained library. The repository retains a security
> policy, least-privilege CI, CodeQL, fuzzing, lockfile updates, release gates,
> trusted publishing, and artifact attestations without pursuing the badge.

### API form

Apply one alert at a time and read it back immediately:

```bash
gh api --method PATCH \
  /repos/VanL/simplebroker/code-scanning/alerts/ALERT_NUMBER \
  -f state=dismissed \
  -f dismissed_reason='REASON' \
  -f dismissed_comment='COMMENT'
```

Use `false positive` only for `#29`; use `won't fix` for the three policy
exceptions. Do not label the Scorecard results false positives: their
observations are accurate, but the prescribed controls do not fit this project.

The API caps `dismissed_comment` at 280 characters. The comments above are the
substance to convey, not literal text; condense to fit and keep the reason and
the revisit condition.

### Policy dismissals re-mint when the SARIF location changes

GitHub fingerprints a Scorecard alert partly by its SARIF location, so changing
where a repository-level finding is anchored retires the old alert number and
creates a new one carrying no dismissal. The three policy checks
(`BranchProtectionID`, `CodeReviewID`, `CIIBestPracticesID`) have no real file
location, so they are the ones exposed to this.

This has happened once. Commit `f7bc7c7` moved repository-level findings from
`no file associated with this alert` to `.github/workflows/scorecard.yml` for
SARIF compatibility, and alerts `#1`, `#20`, and `#22` reappeared on
2026-07-14 as `#38`, `#39`, and `#40`. They were re-dismissed on 2026-07-16 with
the same reasons and a `Supersedes #N` marker. Nothing about the repository's
posture had changed.

If these three surface again as new numbers, check whether the anchoring in
`.github/scripts/normalize_scorecard_sarif.jq` changed before re-triaging. A
re-mint is a fingerprint artifact, not a new finding: re-dismiss with the same
substance and cite the alert it supersedes. This is the accepted cost of
anchoring — it buys SARIF compatibility at the price of dismissal stability.
Do not resolve it by weakening the anchoring or by adopting the controls.

## Final Verification

### Local gates

```bash
uv lock --check
uv run pytest -q -n 0
uv run ruff check .
uv run ruff format --check .
uv run mypy simplebroker \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis
uv run ./bin/packaging-smoke --python 3.11
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
```

Then run the default parallel core suite:

```bash
uv run pytest -q
```

### GitHub gates

```bash
gh api --paginate \
  '/repos/VanL/simplebroker/code-scanning/alerts?state=open&per_page=100' \
  --jq '.[] | [.number, .tool.name, .rule.id, .state] | @tsv'
gh api --paginate \
  '/repos/VanL/simplebroker/code-scanning/alerts?state=dismissed&per_page=100' \
  --jq '.[] | [.number, .dismissed_reason, .dismissed_comment] | @tsv'
gh api --paginate \
  '/repos/VanL/simplebroker/code-scanning/alerts?state=fixed&per_page=100' \
  --jq '.[] | [.number, .fixed_at, .most_recent_instance.commit_sha] | @tsv'
```

Expected final state:

- Fixed: `#19`, `#25`, `#28`, `#32`, `#33`, `#35`.
- Dismissed false positive: `#29`.
- Dismissed won't fix: `#1`, `#20`, `#22`.
- Open from this set: none.

## Completion Evidence

Fill this table during implementation. Do not mark a row complete from local
tests alone when it requires GitHub state.

| Slice | Status | Evidence |
|---|---|---|
| Current baseline captured | Complete | Starting SHA [`d0056c5c`](https://github.com/VanL/simplebroker/commit/d0056c5c), alerts `#1/#19/#20/#22/#25/#28/#29/#32/#33/#35`, and the incompatible CodeQL action failure on [PR `#47`](https://github.com/VanL/simplebroker/actions/runs/29181076673/job/86618661652). |
| CodeQL versions aligned | Complete | [PR `#50` CodeQL](https://github.com/VanL/simplebroker/actions/runs/29213748080/job/86705784446) and [`main` CodeQL](https://github.com/VanL/simplebroker/actions/runs/29214304312) passed. |
| Credential sinks fixed | Complete | Focused regressions passed; alerts [`#28`](https://github.com/VanL/simplebroker/security/code-scanning/28) and [`#35`](https://github.com/VanL/simplebroker/security/code-scanning/35) are fixed. |
| Locked CI installs | Complete | Packaging passed in [PR `#50`](https://github.com/VanL/simplebroker/actions/runs/29213748082); both 15-minute harnesses passed in the [fuzz run](https://github.com/VanL/simplebroker/actions/runs/29213671555); alerts [`#19`](https://github.com/VanL/simplebroker/security/code-scanning/19), [`#25`](https://github.com/VanL/simplebroker/security/code-scanning/25), [`#32`](https://github.com/VanL/simplebroker/security/code-scanning/32), and [`#33`](https://github.com/VanL/simplebroker/security/code-scanning/33) are fixed. |
| Dependabot gate | Complete | Gate unit tests, including failed/missing sibling workflow cases, passed; [PR `#50`](https://github.com/VanL/simplebroker/pull/50) records the fail-closed required workflow set. |
| False-positive dismissal | Complete | Alert [`#29`](https://github.com/VanL/simplebroker/security/code-scanning/29) is dismissed as `false positive` with sanitizer and regression-test evidence. |
| Policy dismissals | Complete | Alerts [`#1`](https://github.com/VanL/simplebroker/security/code-scanning/1), [`#20`](https://github.com/VanL/simplebroker/security/code-scanning/20), and [`#22`](https://github.com/VanL/simplebroker/security/code-scanning/22) are dismissed as `won't fix` with solo-maintainer scope comments. Re-minted as [`#38`](https://github.com/VanL/simplebroker/security/code-scanning/38), [`#39`](https://github.com/VanL/simplebroker/security/code-scanning/39), and [`#40`](https://github.com/VanL/simplebroker/security/code-scanning/40) by the `f7bc7c7` SARIF anchoring change and re-dismissed on 2026-07-16; see the re-mint note above. |
| Final analyzer state | Complete | Merge SHA [`bbb6293c`](https://github.com/VanL/simplebroker/commit/bbb6293ce62e3c8afebe31ee8f3e6d3f6fed7c91); [`main` CodeQL](https://github.com/VanL/simplebroker/actions/runs/29214304312) and [`main` Scorecard](https://github.com/VanL/simplebroker/actions/runs/29214304313) passed; the open-alert query returned no results. |

## Fresh-Eyes Review Checklist

- Does any printed command, DSN, exception, or captured test output still contain
  the configured Postgres password?
- Was the password removed from the Docker argument vector, not merely replaced
  in the visible echo?
- Can the fuzz workflow run without resolving or changing `uv.lock`?
- Did deleting `pip install build` leave packaging smoke reproducible from a
  clean runner?
- Can Dependabot merge while any one of the four required workflows is red,
  cancelled, missing, or timed out?
- Are CodeQL action updates grouped so a future partial version upgrade cannot
  recreate the configuration error?
- Do dismissal comments distinguish inaccurate analysis (`#29`) from accurate
  observations with an intentionally rejected control (`#1/#20/#22`)?
- Did any change weaken the existing no-delete/no-force-push ruleset?
- Did anyone add badge work, nominal reviewers, SARIF filtering, or branch rules
  solely to improve a score? If so, remove that work.

## Non-Goals

- No OpenSSF Best Practices badge application.
- No attempt to reach a Scorecard 10/10.
- No required human review, CODEOWNERS approval, or second nominal maintainer.
- No mandatory PR workflow for human-authored changes.
- No removal or weakening of the current deletion/non-fast-forward protection.
- No custom CodeQL model for `redact_backend_target()` in this slice.
- No separate requirements file or manually maintained transitive hash list.
- No refactor of the general subprocess runner beyond the password-bearing Docker
  call site.
- No dismissal of a concrete finding that can be closed by a code or workflow
  fix.
