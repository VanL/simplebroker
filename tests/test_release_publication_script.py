from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _load_publication_module() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "scripts"
        / "release_publication.py"
    )
    spec = importlib.util.spec_from_file_location(
        "simplebroker_release_publication",
        path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


publication = _load_publication_module()


def _release(
    *,
    draft: bool = True,
    immutable: bool = False,
    assets: tuple[str, ...] = (
        "package.whl",
        "package.tar.gz",
        "bundle.sigstore.json",
    ),
) -> dict[str, object]:
    return {
        "id": 17,
        "tag_name": "v1.2.3",
        "target_commitish": "main",
        "draft": draft,
        "immutable": immutable,
        "assets": [{"name": name, "state": "uploaded"} for name in assets],
    }


def test_release_listing_discovers_drafts_by_tag_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def request(
        method: str,
        path: str,
        *,
        token: str,
        body: object = None,
    ) -> object:
        calls.append((method, path))
        return [_release(), {**_release(), "id": 18, "tag_name": "v9.9.9"}]

    monkeypatch.setattr(publication, "github_api_request", request)

    releases = publication.list_releases("VanL/simplebroker", "token")
    matches = publication.matching_releases(releases, "v1.2.3")

    assert [release["id"] for release in matches] == [17]
    assert calls == [("GET", "/repos/VanL/simplebroker/releases?per_page=100&page=1")]
    assert all("/releases/tags/" not in path for _, path in calls)


def test_replace_draft_deletes_only_the_matching_draft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, object]] = []
    monkeypatch.setattr(
        publication,
        "resolve_tag_commit",
        lambda repo, tag, token: "a" * 40,
    )
    monkeypatch.setattr(publication, "list_releases", lambda repo, token: (_release(),))

    def request(
        method: str,
        path: str,
        *,
        token: str,
        body: object = None,
    ) -> None:
        calls.append((method, path, body))

    monkeypatch.setattr(publication, "github_api_request", request)

    publication.replace_draft(
        repo="VanL/simplebroker",
        tag="v1.2.3",
        expected_sha="a" * 40,
        token="token",
    )

    assert calls == [("DELETE", "/repos/VanL/simplebroker/releases/17", None)]


def test_replace_draft_never_deletes_a_published_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        publication,
        "resolve_tag_commit",
        lambda repo, tag, token: "a" * 40,
    )
    monkeypatch.setattr(
        publication,
        "list_releases",
        lambda repo, token: (_release(draft=False, immutable=True),),
    )
    monkeypatch.setattr(
        publication,
        "github_api_request",
        lambda *args, **kwargs: pytest.fail("published release must not be deleted"),
    )

    with pytest.raises(RuntimeError, match="published release"):
        publication.replace_draft(
            repo="VanL/simplebroker",
            tag="v1.2.3",
            expected_sha="a" * 40,
            token="token",
        )


def test_tag_resolution_peels_annotated_tags_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def request(
        method: str,
        path: str,
        *,
        token: str,
        body: object = None,
    ) -> object:
        calls.append(path)
        if "/git/ref/tags/" in path:
            return {"object": {"type": "tag", "sha": "b" * 40}}
        return {"object": {"type": "commit", "sha": "a" * 40}}

    monkeypatch.setattr(publication, "github_api_request", request)

    assert (
        publication.resolve_tag_commit("VanL/simplebroker", "v1.2.3", "token")
        == "a" * 40
    )
    assert calls == [
        "/repos/VanL/simplebroker/git/ref/tags/v1.2.3",
        f"/repos/VanL/simplebroker/git/tags/{'b' * 40}",
    ]


def test_wrong_remote_tag_sha_is_always_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        publication,
        "resolve_tag_commit",
        lambda repo, tag, token: "b" * 40,
    )

    with pytest.raises(RuntimeError, match="does not match expected release SHA"):
        publication.replace_draft(
            repo="VanL/simplebroker",
            tag="v1.2.3",
            expected_sha="a" * 40,
            token="token",
        )


def test_exact_asset_set_rejects_missing_extra_and_duplicate_assets() -> None:
    expected = ("package.whl", "package.tar.gz", "bundle.sigstore.json")

    publication.require_exact_assets(_release(), expected)

    for assets in (
        ("package.whl", "package.tar.gz"),
        (
            "package.whl",
            "package.tar.gz",
            "bundle.sigstore.json",
            "extra.txt",
        ),
        (
            "package.whl",
            "package.whl",
            "package.tar.gz",
            "bundle.sigstore.json",
        ),
    ):
        with pytest.raises(RuntimeError, match="asset set"):
            publication.require_exact_assets(_release(assets=assets), expected)


@pytest.mark.parametrize(
    "expected",
    (
        ("package.whl", "bundle.json"),
        ("package.tar.gz", "bundle.json"),
        ("package.whl", "package.tar.gz"),
        (
            "package.whl",
            "other.whl",
            "package.tar.gz",
            "bundle.sigstore.json",
        ),
    ),
)
def test_expected_assets_require_one_wheel_sdist_and_sigstore_bundle(
    expected: tuple[str, ...],
) -> None:
    release = _release(assets=expected)

    with pytest.raises(RuntimeError, match="one wheel, one sdist, and one Sigstore"):
        publication.require_exact_assets(release, expected)


def test_publish_draft_verifies_assets_then_publishes_without_pypi_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, object]] = []
    monkeypatch.setattr(
        publication,
        "resolve_tag_commit",
        lambda repo, tag, token: "a" * 40,
    )
    monkeypatch.setattr(publication, "list_releases", lambda repo, token: (_release(),))
    monkeypatch.setattr(
        publication,
        "wait_for_pypi",
        lambda *args, **kwargs: pytest.fail("a draft follows a successful PyPI job"),
    )

    def request(
        method: str,
        path: str,
        *,
        token: str,
        body: object = None,
    ) -> dict[str, object]:
        calls.append((method, path, body))
        return {**_release(), "draft": False, "immutable": True}

    monkeypatch.setattr(publication, "github_api_request", request)

    publication.publish_draft(
        repo="VanL/simplebroker",
        tag="v1.2.3",
        expected_sha="a" * 40,
        package="simplebroker",
        version="1.2.3",
        expected_assets=(
            "package.whl",
            "package.tar.gz",
            "bundle.sigstore.json",
        ),
        token="token",
    )

    assert calls == [
        (
            "PATCH",
            "/repos/VanL/simplebroker/releases/17",
            {"draft": False},
        )
    ]


@pytest.mark.parametrize("releases", ((), (_release(), _release())))
def test_publish_draft_fails_closed_on_missing_or_duplicate_state(
    monkeypatch: pytest.MonkeyPatch,
    releases: tuple[dict[str, object], ...],
) -> None:
    monkeypatch.setattr(
        publication,
        "resolve_tag_commit",
        lambda repo, tag, token: "a" * 40,
    )
    monkeypatch.setattr(publication, "list_releases", lambda repo, token: releases)

    with pytest.raises(RuntimeError, match="exactly one GitHub Release"):
        publication.publish_draft(
            repo="VanL/simplebroker",
            tag="v1.2.3",
            expected_sha="a" * 40,
            package="simplebroker",
            version="1.2.3",
            expected_assets=(
                "package.whl",
                "package.tar.gz",
                "bundle.sigstore.json",
            ),
            token="token",
        )


def test_already_published_immutable_release_is_exact_rerun_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pypi_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        publication,
        "resolve_tag_commit",
        lambda repo, tag, token: "a" * 40,
    )
    monkeypatch.setattr(
        publication,
        "list_releases",
        lambda repo, token: (_release(draft=False, immutable=True),),
    )
    monkeypatch.setattr(
        publication,
        "wait_for_pypi",
        lambda package, version: pypi_calls.append((package, version)),
    )
    monkeypatch.setattr(
        publication,
        "github_api_request",
        lambda *args, **kwargs: pytest.fail("published release must not be edited"),
    )

    publication.publish_draft(
        repo="VanL/simplebroker",
        tag="v1.2.3",
        expected_sha="a" * 40,
        package="simplebroker",
        version="1.2.3",
        expected_assets=(
            "package.whl",
            "package.tar.gz",
            "bundle.sigstore.json",
        ),
        token="token",
    )

    assert pypi_calls == [("simplebroker", "1.2.3")]


def test_already_published_mutable_release_is_not_rerun_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        publication,
        "resolve_tag_commit",
        lambda repo, tag, token: "a" * 40,
    )
    monkeypatch.setattr(
        publication,
        "list_releases",
        lambda repo, token: (_release(draft=False, immutable=False),),
    )

    with pytest.raises(RuntimeError, match="not immutable"):
        publication.publish_draft(
            repo="VanL/simplebroker",
            tag="v1.2.3",
            expected_sha="a" * 40,
            package="simplebroker",
            version="1.2.3",
            expected_assets=(
                "package.whl",
                "package.tar.gz",
                "bundle.sigstore.json",
            ),
            token="token",
        )


def test_pypi_poll_is_bounded_to_five_attempts_over_a_few_minutes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    states = iter(
        (
            (False, "HTTP 404"),
            (False, "HTTP 404"),
            (False, "HTTP 404"),
            (False, "HTTP 404"),
            (True, "simplebroker 1.2.3"),
        )
    )
    sleeps: list[int] = []
    monkeypatch.setattr(
        publication,
        "pypi_release_state",
        lambda package, version: next(states),
    )
    monkeypatch.setattr(publication.time, "sleep", sleeps.append)

    publication.wait_for_pypi("simplebroker", "1.2.3")

    assert sleeps == list(publication.PYPI_RETRY_DELAYS)
    assert len(sleeps) == 4
    assert 120 <= sum(sleeps) <= 300


def test_pypi_poll_failure_reports_last_observed_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    states = iter((False, f"attempt {attempt}") for attempt in range(1, 6))
    monkeypatch.setattr(
        publication,
        "pypi_release_state",
        lambda package, version: next(states),
    )
    monkeypatch.setattr(publication.time, "sleep", lambda delay: None)

    with pytest.raises(RuntimeError, match="attempt 5"):
        publication.wait_for_pypi("simplebroker", "1.2.3")


def test_cli_never_accepts_a_token_argument() -> None:
    parser = publication.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "replace-draft",
                "--repo",
                "VanL/simplebroker",
                "--tag",
                "v1.2.3",
                "--expected-sha",
                "a" * 40,
                "--token",
                "secret",
            ]
        )
