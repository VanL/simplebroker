#!/usr/bin/env python3
"""Manage draft-first, immutable-compatible GitHub Release publication."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Final

GITHUB_API_BASE: Final[str] = "https://api.github.com"
GITHUB_API_VERSION: Final[str] = "2026-03-10"
PYPI_API_BASE: Final[str] = "https://pypi.org/pypi"
HTTP_TIMEOUT_SECONDS: Final[float] = 30.0
PYPI_RETRY_DELAYS: Final[tuple[int, ...]] = (15, 30, 60, 120)


def _mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{label} was not a JSON object")
    return value


def github_api_request(
    method: str,
    path: str,
    *,
    token: str,
    body: object = None,
) -> object:
    """Send one authenticated GitHub API request and decode its JSON response."""

    if not path.startswith("/"):
        raise RuntimeError("GitHub API path must start with /")
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"{GITHUB_API_BASE}{path}",
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "simplebroker-release-publication",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"GitHub API {method} {path} failed: HTTP {exc.code}: {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"GitHub API {method} {path} failed: {exc.reason}") from exc

    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"GitHub API {method} {path} returned invalid JSON") from exc


def list_releases(repo: str, token: str) -> tuple[Mapping[str, object], ...]:
    """List releases, including drafts visible to the authenticated maintainer."""

    releases: list[Mapping[str, object]] = []
    page = 1
    encoded_repo = urllib.parse.quote(repo, safe="/")
    while True:
        path = f"/repos/{encoded_repo}/releases?per_page=100&page={page}"
        payload = github_api_request("GET", path, token=token)
        if not isinstance(payload, list):
            raise RuntimeError("GitHub releases response was not a JSON list")
        page_releases = [
            _mapping(release, label="GitHub release") for release in payload
        ]
        releases.extend(page_releases)
        if len(payload) < 100:
            return tuple(releases)
        page += 1


def matching_releases(
    releases: Sequence[Mapping[str, object]],
    tag: str,
) -> tuple[Mapping[str, object], ...]:
    """Return releases whose tag_name exactly matches ``tag``."""

    return tuple(release for release in releases if release.get("tag_name") == tag)


def resolve_tag_commit(repo: str, tag: str, token: str) -> str:
    """Resolve a remote lightweight or annotated tag to its commit SHA."""

    encoded_repo = urllib.parse.quote(repo, safe="/")
    encoded_tag = urllib.parse.quote(tag, safe="")
    payload = github_api_request(
        "GET",
        f"/repos/{encoded_repo}/git/ref/tags/{encoded_tag}",
        token=token,
    )
    current = _mapping(payload, label="Git tag reference").get("object")

    for _ in range(8):
        tag_object = _mapping(current, label="Git tag object")
        object_type = tag_object.get("type")
        sha = tag_object.get("sha")
        if not isinstance(sha, str) or not sha:
            raise RuntimeError(f"Git tag {tag} did not contain an object SHA")
        if object_type == "commit":
            return sha
        if object_type != "tag":
            raise RuntimeError(
                f"Git tag {tag} resolved to unsupported object type {object_type!r}"
            )
        annotated = github_api_request(
            "GET",
            f"/repos/{encoded_repo}/git/tags/{sha}",
            token=token,
        )
        current = _mapping(annotated, label="Annotated Git tag").get("object")

    raise RuntimeError(f"Git tag {tag} exceeded the annotated-tag resolution limit")


def _require_expected_tag_sha(
    *,
    repo: str,
    tag: str,
    expected_sha: str,
    token: str,
) -> None:
    actual_sha = resolve_tag_commit(repo, tag, token)
    if actual_sha != expected_sha:
        raise RuntimeError(
            f"Remote tag {tag} at {actual_sha} does not match expected release SHA "
            f"{expected_sha}"
        )


def _release_id(release: Mapping[str, object]) -> int:
    release_id = release.get("id")
    if not isinstance(release_id, int) or isinstance(release_id, bool):
        raise RuntimeError("GitHub Release did not contain a numeric id")
    return release_id


def require_exact_assets(
    release: Mapping[str, object],
    expected_assets: Sequence[str],
) -> None:
    """Require exactly one uploaded release asset for every expected name."""

    expected = tuple(expected_assets)
    wheel_count = sum(name.endswith(".whl") for name in expected)
    sdist_count = sum(name.endswith(".tar.gz") for name in expected)
    sigstore_count = sum(name.endswith(".sigstore.json") for name in expected)
    if (wheel_count, sdist_count, sigstore_count) != (1, 1, 1):
        raise RuntimeError(
            "Expected release assets must contain one wheel, one sdist, and one "
            "Sigstore bundle; "
            f"observed wheel={wheel_count}, sdist={sdist_count}, "
            f"sigstore={sigstore_count}"
        )

    raw_assets = release.get("assets")
    if not isinstance(raw_assets, list):
        raise RuntimeError("GitHub Release asset set was not a JSON list")

    names: list[str] = []
    incomplete: list[str] = []
    for raw_asset in raw_assets:
        asset = _mapping(raw_asset, label="GitHub Release asset")
        name = asset.get("name")
        if not isinstance(name, str) or not name:
            raise RuntimeError("GitHub Release asset set contained an unnamed asset")
        names.append(name)
        if asset.get("state") != "uploaded":
            incomplete.append(name)

    if len(set(expected)) != len(expected):
        raise RuntimeError("Expected release asset set contains duplicate names")
    if len(set(names)) != len(names):
        raise RuntimeError("GitHub Release asset set contains duplicate names")
    if set(names) != set(expected) or incomplete:
        missing = sorted(set(expected) - set(names))
        extra = sorted(set(names) - set(expected))
        raise RuntimeError(
            "GitHub Release asset set does not match expected assets; "
            f"missing={missing}, extra={extra}, incomplete={sorted(incomplete)}"
        )


def _one_matching_release(
    *,
    repo: str,
    tag: str,
    token: str,
) -> Mapping[str, object]:
    matches = matching_releases(list_releases(repo, token), tag)
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one GitHub Release for tag {tag}, found {len(matches)}"
        )
    return matches[0]


def replace_draft(
    *,
    repo: str,
    tag: str,
    expected_sha: str,
    token: str,
) -> None:
    """Delete a stale matching draft after independently verifying the tag SHA."""

    _require_expected_tag_sha(
        repo=repo,
        tag=tag,
        expected_sha=expected_sha,
        token=token,
    )
    matches = matching_releases(list_releases(repo, token), tag)
    if not matches:
        print(f"No existing draft for {tag}; staging a new draft")
        return
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected at most one GitHub Release for tag {tag}, found {len(matches)}"
        )

    release = matches[0]
    if release.get("draft") is not True:
        raise RuntimeError(
            f"Refusing to delete published release for tag {tag}; published releases "
            "are permanent"
        )
    release_id = _release_id(release)
    encoded_repo = urllib.parse.quote(repo, safe="/")
    github_api_request(
        "DELETE",
        f"/repos/{encoded_repo}/releases/{release_id}",
        token=token,
    )
    print(f"Deleted stale draft release for {tag}")


def _normalize_package_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def pypi_release_state(package: str, version: str) -> tuple[bool, str]:
    """Return whether PyPI has the exact package/version and a status description."""

    encoded_package = urllib.parse.quote(package, safe="")
    encoded_version = urllib.parse.quote(version, safe="")
    url = f"{PYPI_API_BASE}/{encoded_package}/{encoded_version}/json"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "simplebroker-release"},
    )
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code} from {url}"
    except urllib.error.URLError as exc:
        return False, f"network error from {url}: {exc.reason}"
    except json.JSONDecodeError:
        return False, f"invalid JSON from {url}"

    if not isinstance(payload, Mapping):
        return False, f"non-object JSON from {url}"
    info = payload.get("info")
    if not isinstance(info, Mapping):
        return False, f"missing project info from {url}"
    observed_name = info.get("name")
    observed_version = info.get("version")
    if not isinstance(observed_name, str) or not isinstance(observed_version, str):
        return False, f"incomplete project info from {url}"
    if _normalize_package_name(observed_name) != _normalize_package_name(package):
        return False, f"observed package {observed_name} instead of {package}"
    if observed_version != version:
        return False, f"observed version {observed_version} instead of {version}"
    return True, f"{observed_name} {observed_version}"


def wait_for_pypi(package: str, version: str) -> None:
    """Wait a bounded few minutes for an already-published rerun to reach PyPI."""

    last_state = "not checked"
    for delay in PYPI_RETRY_DELAYS:
        exists, last_state = pypi_release_state(package, version)
        if exists:
            return
        time.sleep(delay)

    exists, last_state = pypi_release_state(package, version)
    if exists:
        return
    raise RuntimeError(
        f"PyPI did not report {package} {version} after five attempts; "
        f"last observed state: {last_state}"
    )


def publish_draft(
    *,
    repo: str,
    tag: str,
    expected_sha: str,
    package: str,
    version: str,
    expected_assets: Sequence[str],
    token: str,
) -> None:
    """Verify and publish the exact matching draft, or validate an exact rerun."""

    _require_expected_tag_sha(
        repo=repo,
        tag=tag,
        expected_sha=expected_sha,
        token=token,
    )
    release = _one_matching_release(repo=repo, tag=tag, token=token)
    require_exact_assets(release, expected_assets)

    if release.get("draft") is False:
        if release.get("immutable") is not True:
            raise RuntimeError(
                f"Published release for tag {tag} is not immutable; refusing to "
                "treat it as exact rerun success"
            )
        wait_for_pypi(package, version)
        print(f"Immutable release {tag} and PyPI {package} {version} already match")
        return
    if release.get("draft") is not True:
        raise RuntimeError(f"GitHub Release for tag {tag} has invalid draft state")

    release_id = _release_id(release)
    encoded_repo = urllib.parse.quote(repo, safe="/")
    updated = github_api_request(
        "PATCH",
        f"/repos/{encoded_repo}/releases/{release_id}",
        token=token,
        body={"draft": False},
    )
    updated_release = _mapping(updated, label="Published GitHub Release")
    if updated_release.get("draft") is not False:
        raise RuntimeError(f"GitHub Release for tag {tag} remained a draft")
    if updated_release.get("immutable") is not True:
        raise RuntimeError(f"Published GitHub Release for tag {tag} is not immutable")
    print(f"Published immutable GitHub Release {tag}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage draft-first GitHub Release publication state"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    replace_parser = subparsers.add_parser("replace-draft")
    replace_parser.add_argument("--repo", required=True)
    replace_parser.add_argument("--tag", required=True)
    replace_parser.add_argument("--expected-sha", required=True)

    publish_parser = subparsers.add_parser("publish-draft")
    publish_parser.add_argument("--repo", required=True)
    publish_parser.add_argument("--tag", required=True)
    publish_parser.add_argument("--expected-sha", required=True)
    publish_parser.add_argument("--package", required=True)
    publish_parser.add_argument("--version", required=True)
    publish_parser.add_argument(
        "--expected-asset",
        action="append",
        dest="expected_assets",
        required=True,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        parser.error("GITHUB_TOKEN is required")

    try:
        if args.command == "replace-draft":
            replace_draft(
                repo=args.repo,
                tag=args.tag,
                expected_sha=args.expected_sha,
                token=token,
            )
        else:
            publish_draft(
                repo=args.repo,
                tag=args.tag,
                expected_sha=args.expected_sha,
                package=args.package,
                version=args.version,
                expected_assets=tuple(args.expected_assets),
                token=token,
            )
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
