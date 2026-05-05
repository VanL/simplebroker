#!/usr/bin/env python3
"""Require sibling workflow runs for the current SHA to be green before release."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass

SUCCESS_CONCLUSION = "success"


@dataclass(frozen=True)
class WorkflowRun:
    """The workflow-run fields the release gate cares about."""

    id: int
    name: str
    status: str
    conclusion: str | None
    url: str
    created_at: str
    run_attempt: int

    @classmethod
    def from_api(cls, raw: Mapping[str, object]) -> WorkflowRun:
        return cls(
            id=_int_value(raw.get("id")),
            name=str(raw.get("name") or ""),
            status=str(raw.get("status") or ""),
            conclusion=_optional_str(raw.get("conclusion")),
            url=str(raw.get("html_url") or ""),
            created_at=str(raw.get("created_at") or ""),
            run_attempt=_int_value(raw.get("run_attempt"), default=1),
        )


@dataclass(frozen=True)
class GateCheck:
    """Result of evaluating the required workflow runs for one poll."""

    passed: tuple[WorkflowRun, ...]
    missing: tuple[str, ...]
    pending: tuple[WorkflowRun, ...]
    failed: tuple[WorkflowRun, ...]

    @property
    def ready(self) -> bool:
        return not self.missing and not self.pending and not self.failed


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _int_value(value: object, *, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def latest_runs_by_name(
    runs: Iterable[WorkflowRun],
    *,
    exclude_run_id: int | str | None = None,
) -> dict[str, WorkflowRun]:
    """Return the latest observed run for each workflow name."""

    excluded = str(exclude_run_id) if exclude_run_id is not None else None
    latest: dict[str, WorkflowRun] = {}
    latest_keys: dict[str, tuple[str, int, int]] = {}

    for run in runs:
        if excluded is not None and str(run.id) == excluded:
            continue
        if not run.name:
            continue

        sort_key = (run.created_at, run.run_attempt, run.id)
        if sort_key >= latest_keys.get(run.name, ("", 0, 0)):
            latest[run.name] = run
            latest_keys[run.name] = sort_key

    return latest


def evaluate_required_workflows(
    runs: Iterable[WorkflowRun],
    required_workflows: Sequence[str],
    *,
    exclude_run_id: int | str | None = None,
) -> GateCheck:
    """Check whether all required workflow runs are completed successfully."""

    latest = latest_runs_by_name(runs, exclude_run_id=exclude_run_id)
    passed: list[WorkflowRun] = []
    missing: list[str] = []
    pending: list[WorkflowRun] = []
    failed: list[WorkflowRun] = []

    for name in required_workflows:
        run = latest.get(name)
        if run is None:
            missing.append(name)
            continue
        if run.status != "completed":
            pending.append(run)
            continue
        if run.conclusion != SUCCESS_CONCLUSION:
            failed.append(run)
            continue
        passed.append(run)

    return GateCheck(
        passed=tuple(passed),
        missing=tuple(missing),
        pending=tuple(pending),
        failed=tuple(failed),
    )


def describe_gate_check(check: GateCheck) -> str:
    """Return a concise status line for logs."""

    parts: list[str] = []
    if check.passed:
        parts.append(
            "green: " + ", ".join(f"{run.name} ({run.url})" for run in check.passed)
        )
    if check.pending:
        parts.append(
            "pending: "
            + ", ".join(f"{run.name} [{run.status}]" for run in check.pending)
        )
    if check.missing:
        parts.append("missing: " + ", ".join(check.missing))
    if check.failed:
        parts.append(
            "failed: "
            + ", ".join(
                f"{run.name} [{run.conclusion or run.status}] {run.url}"
                for run in check.failed
            )
        )
    return "; ".join(parts) if parts else "no required workflow runs found"


def fetch_workflow_runs(
    *,
    repo: str,
    sha: str,
    token: str,
    api_url: str,
) -> tuple[WorkflowRun, ...]:
    """Fetch all GitHub Actions workflow runs for a commit SHA."""

    runs: list[WorkflowRun] = []
    page = 1
    repo_path = urllib.parse.quote(repo, safe="/")

    while True:
        query = urllib.parse.urlencode(
            {
                "head_sha": sha,
                "per_page": "100",
                "page": str(page),
            }
        )
        url = f"{api_url.rstrip('/')}/repos/{repo_path}/actions/runs?{query}"
        payload = github_api_get(url, token=token)
        raw_runs = payload.get("workflow_runs", [])
        if not isinstance(raw_runs, list):
            raise RuntimeError("GitHub API response did not include workflow_runs")

        runs.extend(
            WorkflowRun.from_api(run) for run in raw_runs if isinstance(run, Mapping)
        )

        if len(raw_runs) < 100:
            return tuple(runs)
        page += 1


def github_api_get(url: str, *, token: str) -> dict[str, object]:
    """Return a JSON object from the GitHub API."""

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "simplebroker-release-gate",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API request failed: {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"GitHub API request failed: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("GitHub API response was not a JSON object")
    return payload


def wait_for_required_workflows(
    *,
    fetch_runs: Callable[[], tuple[WorkflowRun, ...]],
    required_workflows: Sequence[str],
    exclude_run_id: int | str | None,
    timeout_seconds: int,
    missing_timeout_seconds: int,
    poll_interval_seconds: int,
) -> GateCheck:
    """Poll until required workflow runs are green or a failure is known."""

    start = time.monotonic()
    missing_since: float | None = None
    last_check: GateCheck | None = None

    while True:
        check = evaluate_required_workflows(
            fetch_runs(),
            required_workflows,
            exclude_run_id=exclude_run_id,
        )
        last_check = check
        print(describe_gate_check(check), flush=True)

        if check.ready:
            return check

        if check.failed:
            raise RuntimeError(
                "required workflow run failed; refusing to publish release"
            )

        now = time.monotonic()
        if check.missing:
            if missing_since is None:
                missing_since = now
            if now - missing_since >= missing_timeout_seconds:
                raise RuntimeError(
                    "required workflow run was not found before the missing-run "
                    f"timeout: {', '.join(check.missing)}"
                )
        else:
            missing_since = None

        if now - start >= timeout_seconds:
            detail = describe_gate_check(last_check)
            raise RuntimeError(
                f"timed out waiting for required workflow runs to be green: {detail}"
            )

        time.sleep(poll_interval_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Wait until required GitHub Actions workflow runs for a commit SHA "
            "have completed successfully."
        )
    )
    parser.add_argument(
        "--workflow",
        action="append",
        dest="required_workflows",
        required=True,
        help="Workflow name that must complete with conclusion=success.",
    )
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument(
        "--exclude-run-id",
        default=os.environ.get("GITHUB_RUN_ID"),
        help="Workflow run id to ignore, usually the release workflow itself.",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--missing-timeout-seconds", type=int, default=300)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        parser.error("GITHUB_TOKEN is required")
    if not args.repo:
        parser.error("--repo or GITHUB_REPOSITORY is required")
    if not args.sha:
        parser.error("--sha or GITHUB_SHA is required")

    try:
        wait_for_required_workflows(
            fetch_runs=lambda: fetch_workflow_runs(
                repo=args.repo,
                sha=args.sha,
                token=token,
                api_url=args.api_url,
            ),
            required_workflows=tuple(args.required_workflows),
            exclude_run_id=args.exclude_run_id,
            timeout_seconds=args.timeout_seconds,
            missing_timeout_seconds=args.missing_timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
