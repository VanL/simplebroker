from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_product_spec_fully_qualified_test_citations_resolve() -> None:
    """Traceability references must resolve without duplicating each spec's list."""
    citation = re.compile(
        r"(?<![\w/.-])((?:tests|extensions|examples)/[\w/.-]+\.py)"
        r"((?:::[A-Za-z_]\w*)+)"
    )
    checked = 0
    for spec in sorted((REPO_ROOT / "docs/specs").glob("*.md")):
        for match in citation.finditer(spec.read_text(encoding="utf-8")):
            module = REPO_ROOT / match.group(1)
            assert module.is_file(), (spec.name, match.group(0))
            body = ast.parse(module.read_text(encoding="utf-8")).body
            for name in match.group(2).split("::")[1:]:
                declarations = [
                    node
                    for node in body
                    if isinstance(
                        node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                    )
                    and node.name == name
                ]
                assert len(declarations) == 1, (spec.name, match.group(0), name)
                body = declarations[0].body
            checked += 1
    assert checked, "expected firing test references in product specs"


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _result_detail(result: subprocess.CompletedProcess[str]) -> str:
    return f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"


@pytest.mark.parametrize("script_name", ["check-dom15-fixtures", "check-doc-paths"])
def test_history_independent_doc_gate(script_name: str) -> None:
    result = _run([sys.executable, str(REPO_ROOT / "bin" / script_name)])

    assert result.returncode == 0, _result_detail(result)
    assert result.stderr == ""


def test_coalesce_check_on_full_history_checkout() -> None:
    shallow = _run(["git", "rev-parse", "--is-shallow-repository"])
    assert shallow.returncode == 0, _result_detail(shallow)

    result = _run([sys.executable, str(REPO_ROOT / "bin" / "coalesce-check")])

    assert result.returncode == 0, _result_detail(result)
    assert result.stderr == ""
    if shallow.stdout.strip() == "true":
        # The tool now skips its history-dependent legs loudly on
        # shallow clones instead of reporting false BROKEN (adopted
        # from agent-theory @ 0423923).
        assert "shallow clone detected" in result.stdout
        assert "retrieval cues found (syntax only, unverified here):" in result.stdout
        assert "lessons dated entries:" in result.stdout


def test_dom15_fixture_checker_self_test() -> None:
    result = _run(
        [sys.executable, str(REPO_ROOT / "bin" / "check-dom15-fixtures"), "--self-test"]
    )

    assert result.returncode == 0, _result_detail(result)
    assert "all mutation cases and probes pass" in result.stdout


def test_check_doc_paths_self_test() -> None:
    result = _run(
        [sys.executable, str(REPO_ROOT / "bin" / "check-doc-paths"), "--self-test"]
    )

    assert result.returncode == 0, _result_detail(result)
    assert "all claim, fence, and allowlist probes pass" in result.stdout


def test_coalesce_check_self_test() -> None:
    result = _run(
        [sys.executable, str(REPO_ROOT / "bin" / "coalesce-check"), "--self-test"]
    )

    assert result.returncode == 0, _result_detail(result)
    assert "all reachability and attribution probes pass" in result.stdout


def test_coalesce_check_rejects_unreachable_loose_sha_and_simplebroker_marker(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "bin").mkdir()
    script_copy = repo / "bin" / "coalesce-check"
    script_copy.write_text(
        (REPO_ROOT / "bin" / "coalesce-check").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (repo / "docs" / "plans").mkdir(parents=True)
    (repo / "docs" / "plans" / "README.md").write_text(
        "## Status Index\n\n| Plan | Status |\n|------|--------|\n\n## Retired Plans\n",
        encoding="utf-8",
    )
    (repo / "docs" / "lessons.md").write_text("# Lessons\n", encoding="utf-8")

    git_env = {
        "GIT_AUTHOR_NAME": "Test",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "Test",
        "GIT_COMMITTER_EMAIL": "test@example.com",
        "PATH": os.environ.get("PATH", ""),
    }

    def git(*args: str) -> str:
        proc = subprocess.run(
            ["git", "-C", str(repo), *args],
            env=git_env,
            capture_output=True,
            text=True,
            check=True,
        )
        return proc.stdout.strip()

    git("init", "-b", "main")
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    git("add", "README.md", "docs/plans/README.md", "docs/lessons.md")
    git("commit", "-m", "base")
    base_sha = git("rev-parse", "--short", "HEAD")

    (repo / "README.md").write_text("dangling\n", encoding="utf-8")
    git("add", "README.md")
    git("commit", "-m", "dangling")
    dangling_sha = git("rev-parse", "--short", "HEAD")
    git("reset", "--hard", "HEAD~1")

    # 1. Dangling loose object fails by default, passes only with --allow-local-only.
    (repo / "docs" / "coalescing.md").write_text(
        f"Base `{base_sha}` and rebased local loose `{dangling_sha}`.\n",
        encoding="utf-8",
    )
    res_default = subprocess.run(
        [sys.executable, str(script_copy)],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_default.returncode == 1, _result_detail(res_default)
    assert dangling_sha in res_default.stdout

    res_allowed = subprocess.run(
        [sys.executable, str(script_copy), "--allow-local-only"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_allowed.returncode == 0, _result_detail(res_allowed)

    # 1b. Unpublished SHA held by a local branch or tag (while origin/main is at base)
    # must ALSO fail by default — local refs are not published history.
    git("update-ref", "refs/remotes/origin/main", base_sha)
    git("symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
    git("branch", "local-side-branch", dangling_sha)
    git("tag", "local-hold-tag", dangling_sha)

    res_local_refs = subprocess.run(
        [sys.executable, str(script_copy)],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_local_refs.returncode == 1, _result_detail(res_local_refs)
    assert dangling_sha in res_local_refs.stdout

    res_local_refs_allowed = subprocess.run(
        [sys.executable, str(script_copy), "--allow-local-only"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_local_refs_allowed.returncode == 0, _result_detail(
        res_local_refs_allowed
    )

    # 2. Broken SHA near the word "simplebroker" must fail, never skip as foreign.
    (repo / "docs" / "coalescing.md").write_text(
        "In simplebroker commit `deadbeef1` we changed behavior.\n",
        encoding="utf-8",
    )
    res_self_name = subprocess.run(
        [sys.executable, str(script_copy)],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_self_name.returncode == 1, _result_detail(res_self_name)
    assert "deadbeef1" in res_self_name.stdout


def test_coalesce_check_passes_in_no_local_clean_clone(tmp_path: Path) -> None:
    shallow = _run(["git", "rev-parse", "--is-shallow-repository"])
    if shallow.stdout.strip() == "true":
        pytest.skip("clean-clone ancestry proof requires full history")

    clone_dir = tmp_path / "clean-clone"
    clone_res = subprocess.run(
        ["git", "clone", "--no-local", str(REPO_ROOT), str(clone_dir)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert clone_res.returncode == 0, _result_detail(clone_res)
    # Copy current working-tree docs/coalescing.md and bin/coalesce-check into clone
    (clone_dir / "docs" / "coalescing.md").write_text(
        (REPO_ROOT / "docs" / "coalescing.md").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (clone_dir / "bin" / "coalesce-check").write_text(
        (REPO_ROOT / "bin" / "coalesce-check").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    res = subprocess.run(
        [sys.executable, str(clone_dir / "bin" / "coalesce-check")],
        cwd=clone_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode == 0, _result_detail(res)


def test_check_doc_paths_rejects_dangling_symlinks_new_surfaces_and_suffixes(
    tmp_path: Path,
) -> None:
    checker = REPO_ROOT / "bin" / "check-doc-paths"

    # Dangling symlink fails (both outside SCAN_DIRS and inside SCAN_DIRS)
    dangling_root = tmp_path / "dangling"
    (dangling_root / "docs" / "specs").mkdir(parents=True)
    (dangling_root / "bin").mkdir(parents=True)
    (dangling_root / "docs" / "specs" / "01.md").write_text(
        "See `bin/broken-tool`.\n",
        encoding="utf-8",
    )
    (dangling_root / "bin" / "broken-tool").symlink_to("nonexistent-tool")
    res_symlink = _run([sys.executable, str(checker), "--root", str(dangling_root)])
    assert res_symlink.returncode == 1, _result_detail(res_symlink)
    assert "Traceback" not in res_symlink.stdout + res_symlink.stderr

    # docs/program-theory.md and docs/implementation/*.md are scanned
    for rel_doc in ("docs/program-theory.md", "docs/implementation/01-arch.md"):
        surface_root = tmp_path / rel_doc.replace("/", "_")
        doc_path = surface_root / rel_doc
        doc_path.parent.mkdir(parents=True)
        doc_path.write_text("See `docs/specs/missing-target.md`.\n", encoding="utf-8")
        res_surface = _run([sys.executable, str(checker), "--root", str(surface_root)])
        assert res_surface.returncode == 1, _result_detail(res_surface)

    # #anchor, :line, and ::symbol suffixes validate the underlying repo path
    for suffixed in (
        "README.md#overview",
        "bin/missing-tool:12",
        "docs/specs/missing.md::Section",
    ):
        suffix_root = tmp_path / f"suffix_{abs(hash(suffixed))}"
        (suffix_root / "docs" / "specs").mkdir(parents=True)
        (suffix_root / "docs" / "specs" / "01.md").write_text(
            f"Ref `{suffixed}`.\n",
            encoding="utf-8",
        )
        res_suffix = _run([sys.executable, str(checker), "--root", str(suffix_root)])
        assert res_suffix.returncode == 1, _result_detail(res_suffix)


def test_check_plan_context_requires_review_loop_and_validates_all_source_paths(
    tmp_path: Path,
) -> None:
    checker = REPO_ROOT / "bin" / "check-plan-context"
    plan_dir = tmp_path / "docs" / "plans"
    spec_dir = tmp_path / "docs" / "specs"
    plan_dir.mkdir(parents=True)
    spec_dir.mkdir(parents=True)
    (spec_dir / "valid.md").write_text("# Valid\n", encoding="utf-8")

    # 1. Active class-3 plan without ## Independent Review Loop fails; draft succeeds.
    (plan_dir / "README.md").write_text(
        """## Status Index

| Plan | Status |
|------|--------|
| active-no-review.md | active — class 3 |
| draft-no-review.md | draft — class 4 |

## Retired Plans
""",
        encoding="utf-8",
    )
    body_without_review = (
        "# Plan\nClass: 3\n\n## Source Documents\n\n- `docs/specs/valid.md`\n"
    )
    (plan_dir / "active-no-review.md").write_text(body_without_review, encoding="utf-8")
    (plan_dir / "draft-no-review.md").write_text(body_without_review, encoding="utf-8")

    res_missing_review = _run([sys.executable, str(checker), "--root", str(tmp_path)])
    assert res_missing_review.returncode == 1, _result_detail(res_missing_review)
    assert "active-no-review.md" in res_missing_review.stdout
    assert "Independent Review Loop" in res_missing_review.stdout
    assert "draft-no-review.md" not in res_missing_review.stdout

    # 2. Source Documents with one valid path + one broken path fails (no short-circuit).
    (plan_dir / "README.md").write_text(
        """## Status Index

| Plan | Status |
|------|--------|
| mixed-paths.md | draft — class 3 |

## Retired Plans
""",
        encoding="utf-8",
    )
    (plan_dir / "mixed-paths.md").write_text(
        """# Plan
Class: 3

## Source Documents

- `docs/specs/valid.md` (`[SB-CLI-1]`)
- `docs/specs/broken-second-path.md`
""",
        encoding="utf-8",
    )
    res_mixed = _run([sys.executable, str(checker), "--root", str(tmp_path)])
    assert res_mixed.returncode == 1, _result_detail(res_mixed)
    assert "broken-second-path.md" in res_mixed.stdout

    # 3. House style in active plans: bare live/retired sibling plan filenames,
    # convention tokens, and git branch names pass; typoed dated plan filename fails.
    (plan_dir / "2026-11-08-sibling-plan.md").write_text(
        """# Sibling
Class: 3

## Source Documents

- `docs/specs/valid.md`
""",
        encoding="utf-8",
    )
    (plan_dir / "README.md").write_text(
        """## Status Index

| Plan | Status |
|------|--------|
| 2026-11-08-sibling-plan.md | completed — class 3 |
| 2026-11-10-house-style-plan.md | active — class 4 |

## Retired Plans

- `2026-10-01-retired-plan.md` — `abc1234` — retired 2026-11-01
""",
        encoding="utf-8",
    )
    (plan_dir / "2026-11-10-house-style-plan.md").write_text(
        """# House Style
Class: 4

## Source Documents

- `docs/specs/valid.md`
- `2026-11-08-sibling-plan.md` (live sibling plan by bare filename)
- `2026-10-01-retired-plan.md` (retired sibling plan by bare filename)
- Conventions like `MODULE-THEORY.md`, `.broker.toml`, `writing-plans.md`, and branch `perf/sb-listener-recovery`

## Independent Review Loop

- Author family: Claude
- Reviewer family: OpenAI Codex
- Review invocation: `codex exec -s read-only`
""",
        encoding="utf-8",
    )
    res_house = _run([sys.executable, str(checker), "--root", str(tmp_path)])
    assert res_house.returncode == 0, _result_detail(res_house)

    # Typoed dated sibling plan filename still fails.
    (plan_dir / "2026-11-10-house-style-plan.md").write_text(
        """# House Style Broken
Class: 4

## Source Documents

- `docs/specs/valid.md`
- `2026-11-09-typo-missing-plan.md`

## Independent Review Loop

- Author family: Claude
- Reviewer family: OpenAI Codex
- Review invocation: `codex exec -s read-only`
""",
        encoding="utf-8",
    )
    res_typo_plan = _run([sys.executable, str(checker), "--root", str(tmp_path)])
    assert res_typo_plan.returncode == 1, _result_detail(res_typo_plan)
    assert "2026-11-09-typo-missing-plan.md" in res_typo_plan.stdout

    # 3b. Missing retired plan path under docs/plans/ passes, while an escaping symlink at that path is rejected.
    (plan_dir / "2026-11-10-house-style-plan.md").write_text(
        """# Retired Slash Path Probe
Class: 4

## Source Documents

- `docs/specs/valid.md`
- `docs/plans/2026-10-01-retired-plan.md`

## Independent Review Loop

- Author family: Claude
- Reviewer family: OpenAI Codex
- Review invocation: `codex exec -s read-only`
""",
        encoding="utf-8",
    )
    res_missing_retired = _run([sys.executable, str(checker), "--root", str(tmp_path)])
    assert res_missing_retired.returncode == 0, _result_detail(res_missing_retired)

    escaping_target = tmp_path.parent / "outside-repo-plan.md"
    escaping_target.write_text("# outside\n", encoding="utf-8")
    retired_symlink = plan_dir / "2026-10-01-retired-plan.md"
    retired_symlink.symlink_to(escaping_target)
    try:
        res_escape = _run([sys.executable, str(checker), "--root", str(tmp_path)])
        assert res_escape.returncode == 1, _result_detail(res_escape)
        assert "docs/plans/2026-10-01-retired-plan.md" in res_escape.stdout
    finally:
        retired_symlink.unlink(missing_ok=True)
        escaping_target.unlink(missing_ok=True)

    # 4. Real repository sibling plan marked active passes without house-style false positives.
    import shutil

    real_plan_root = tmp_path / "real-repo-plan-probe"
    shutil.copytree(REPO_ROOT / "docs", real_plan_root / "docs")
    ownership_plan_path = (
        real_plan_root
        / "docs"
        / "plans"
        / "2026-11-10-backend-thread-ownership-plan.md"
    )
    ownership_txt = ownership_plan_path.read_text(encoding="utf-8")
    # Materialize stub files inside real_plan_root for non-docs code/test paths
    # cited in Source Documents. Bound the fixture to that section: later
    # command examples can contain path separators and are not source claims.
    source_documents = re.search(
        r"^## Source Documents[^\n]*\n(?P<body>.*?)(?=^## |\Z)",
        ownership_txt,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert source_documents is not None
    source_documents_txt = source_documents.group("body")
    assert "uv run ./bin/pytest-pg" in ownership_txt
    assert "uv run ./bin/pytest-pg" not in source_documents_txt
    assert "`2026-11-08-pg-notify-fanout-plan.md`" in source_documents_txt
    for match in re.finditer(r"`([^`\r\n]+)`", source_documents_txt):
        candidate = match.group(1).split("#", 1)[0].split(":", 1)[0]
        if "/" in candidate and not candidate.startswith("docs/"):
            stub = real_plan_root / candidate
            stub.parent.mkdir(parents=True, exist_ok=True)
            if not stub.exists():
                stub.write_text("# stub\n", encoding="utf-8")
    if "## Independent Review Loop" not in ownership_txt:
        ownership_txt += (
            "\n## Independent Review Loop\n\n"
            "- Author family: Claude\n"
            "- Reviewer family: OpenAI Codex\n"
            "- Review invocation: `codex exec -s read-only`\n"
        )
        ownership_plan_path.write_text(ownership_txt, encoding="utf-8")
    probe_readme_path = real_plan_root / "docs" / "plans" / "README.md"
    original_probe_readme = probe_readme_path.read_text(encoding="utf-8")
    # This probe checks one real plan. Rows the live index currently holds
    # in flight are not its subject, and their sources are not stubbed here.
    original_probe_readme = re.sub(
        r"^(\|\s*[^|\s]+\.md\s*\|\s*)(?:draft|active|status-review)\b[^|]*(\|)",
        r"\1completed \2",
        original_probe_readme,
        flags=re.MULTILINE,
    )
    updated_probe_readme, count = re.subn(
        r"^(\|\s*2026-11-10-backend-thread-ownership-plan\.md\s*\|\s*)[^|]+(\|)",
        r"\1active — class 4 \2",
        original_probe_readme,
        count=1,
        flags=re.MULTILINE,
    )
    assert count == 1
    probe_readme_path.write_text(updated_probe_readme, encoding="utf-8")
    res_real_plan = _run([sys.executable, str(checker), "--root", str(real_plan_root)])
    assert res_real_plan.returncode == 0, _result_detail(res_real_plan)
    assert "1 in-flight plan source declarations resolve" in res_real_plan.stdout


@pytest.mark.parametrize(
    "bad_args",
    [
        ["--spec=/nonexistent/dom15.md"],
        ["--spec"],
        ["--unknown-flag"],
    ],
)
def test_check_dom15_fixtures_rejects_malformed_cli_flags(bad_args: list[str]) -> None:
    result = _run(
        [sys.executable, str(REPO_ROOT / "bin" / "check-dom15-fixtures"), *bad_args]
    )
    assert result.returncode == 2, _result_detail(result)
    assert "Traceback" not in result.stdout + result.stderr


def test_lint_workflow_runs_doc_and_governance_gates_with_full_history() -> None:
    workflow_text = (REPO_ROOT / ".github" / "workflows" / "test.yml").read_text(
        encoding="utf-8"
    )
    lint_section = workflow_text.split("  lint:", 1)[1].split("  packaging:", 1)[0]

    assert "fetch-depth: 0" in lint_section
    for gate_cmd in (
        "bin/coalesce-check",
        "bin/check-doc-paths",
        "bin/check-plan-context",
        "bin/check-dom15-fixtures",
    ):
        assert gate_cmd in lint_section
