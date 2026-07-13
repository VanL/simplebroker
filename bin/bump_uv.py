#!/usr/bin/env python3
"""Update and verify the repository-wide uv version policy."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

WORKFLOWS = (
    "fuzz.yml",
    "release-gate.yml",
    "release-gate-pg.yml",
    "release-gate-redis.yml",
    "test.yml",
    "test-pg-extension.yml",
    "test-redis-extension.yml",
)
LOCK_DIRECTORIES = (
    Path("."),
    Path("extensions/simplebroker_pg"),
    Path("extensions/simplebroker_redis"),
)
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
REQUIRED_VERSION_RE = re.compile(
    r"^>=(?P<minimum>\d+\.\d+\.\d+),<(?P<maximum>\d+\.\d+(?:\.\d+)?)$"
)


class PolicyError(RuntimeError):
    """Raised when the uv policy is incomplete or inconsistent."""


def _version_tuple(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def _validate_versions(ci_version: str, required_version: str) -> None:
    if not VERSION_RE.fullmatch(ci_version):
        raise PolicyError(f"invalid CI uv version: {ci_version!r}")
    match = REQUIRED_VERSION_RE.fullmatch(required_version)
    if match is None:
        raise PolicyError("required uv version must look like '>=0.11.28,<0.12'")
    pinned = _version_tuple(ci_version)
    minimum = _version_tuple(match.group("minimum"))
    maximum = _version_tuple(match.group("maximum"))
    if not minimum <= pinned < maximum:
        raise PolicyError(
            f"CI uv {ci_version} is outside required range {required_version}"
        )


def _workflow_paths(root: Path) -> tuple[Path, ...]:
    directory = root / ".github" / "workflows"
    paths = tuple(directory / name for name in WORKFLOWS)
    missing = [path.relative_to(root) for path in paths if not path.is_file()]
    if missing:
        raise PolicyError(
            f"missing managed workflow(s): {', '.join(map(str, missing))}"
        )

    managed = set(paths)
    workflow_files = (*directory.glob("*.yml"), *directory.glob("*.yaml"))
    unmanaged = sorted(
        path.relative_to(root)
        for path in workflow_files
        if "astral-sh/setup-uv@" in path.read_text() and path not in managed
    )
    if unmanaged:
        raise PolicyError(
            "workflow(s) use setup-uv but are not managed: "
            + ", ".join(map(str, unmanaged))
        )
    return paths


def _updated_pyproject(text: str, required_version: str) -> str:
    section = re.search(r"(?ms)^\[tool\.uv\]\n(?P<body>.*?)(?=^\[|\Z)", text)
    if section is None:
        raise PolicyError("pyproject.toml has no [tool.uv] section")
    body = section.group("body")
    matches = re.findall(r'(?m)^required-version[ \t]*=[ \t]*"[^"]+"[ \t]*$', body)
    setting = f'required-version = "{required_version}"'
    if len(matches) > 1:
        raise PolicyError("pyproject.toml has more than one uv required-version")
    if matches:
        new_body = re.sub(
            r'(?m)^required-version[ \t]*=[ \t]*"[^"]+"[ \t]*$',
            setting,
            body,
        )
    else:
        new_body = f"{setting}\n{body}"
    return text[: section.start("body")] + new_body + text[section.end("body") :]


def _updated_workflow(text: str, ci_version: str, path: Path) -> str:
    matches = re.findall(r'(?m)^  UV_VERSION:[ \t]*"[^"]+"[ \t]*$', text)
    setting = f'  UV_VERSION: "{ci_version}"'
    if len(matches) > 1:
        raise PolicyError(f"{path.name} has more than one UV_VERSION")
    if matches:
        return re.sub(r'(?m)^  UV_VERSION:[ \t]*"[^"]+"[ \t]*$', setting, text)
    if not re.search(r"(?m)^jobs:[^\n]*(?:\n|$)", text):
        raise PolicyError(f"{path.name} has no top-level jobs key")
    return re.sub(
        r"(?m)^jobs:(?P<suffix>[^\n]*)(?P<newline>\n|$)",
        rf"env:\n{setting}\n\njobs:\g<suffix>\g<newline>",
        text,
        count=1,
    )


def _configured_versions(root: Path, workflows: tuple[Path, ...]) -> tuple[str, str]:
    pyproject = (root / "pyproject.toml").read_text()
    match = re.search(
        r'(?ms)^\[tool\.uv\]\n.*?^required-version[ \t]*=[ \t]*"([^"]+)"[ \t]*$',
        pyproject,
    )
    if match is None:
        raise PolicyError("pyproject.toml has no uv required-version")
    required_version = match.group(1)

    pins: dict[Path, str] = {}
    for path in workflows:
        matches = re.findall(
            r'(?m)^  UV_VERSION:[ \t]*"([^"]+)"[ \t]*$', path.read_text()
        )
        if len(matches) != 1:
            raise PolicyError(f"{path.name} must have exactly one UV_VERSION")
        pins[path] = matches[0]
    versions = set(pins.values())
    if len(versions) != 1:
        details = ", ".join(f"{path.name}={version}" for path, version in pins.items())
        raise PolicyError(f"workflow uv versions differ: {details}")
    ci_version = versions.pop()
    _validate_versions(ci_version, required_version)
    return ci_version, required_version


def _run_lock_commands(root: Path, *, check: bool) -> None:
    for directory in LOCK_DIRECTORIES:
        command = ["uv", "lock"]
        if directory != Path("."):
            command.extend(["--directory", str(directory)])
        if check:
            command.append("--check")
        subprocess.run(command, cwd=root, check=True)


def _restore(originals: dict[Path, bytes | None]) -> None:
    for path, contents in originals.items():
        if contents is None:
            path.unlink(missing_ok=True)
        else:
            path.write_bytes(contents)


def _update(
    root: Path, ci_version: str, required_version: str, *, dry_run: bool
) -> None:
    _validate_versions(ci_version, required_version)
    workflows = _workflow_paths(root)
    pyproject = root / "pyproject.toml"
    updates = {
        pyproject: _updated_pyproject(pyproject.read_text(), required_version),
        **{
            path: _updated_workflow(path.read_text(), ci_version, path)
            for path in workflows
        },
    }
    changed = [path for path, text in updates.items() if path.read_text() != text]
    if dry_run:
        for path, text in updates.items():
            status = "would update" if path.read_text() != text else "unchanged"
            print(f"{path.relative_to(root)}: {status}")
        return
    for path in changed:
        print(path.relative_to(root))

    lock_paths = tuple(
        root / directory / "uv.lock" if directory != Path(".") else root / "uv.lock"
        for directory in LOCK_DIRECTORIES
    )
    originals = {
        path: path.read_bytes() if path.exists() else None
        for path in (*updates, *lock_paths)
    }
    try:
        for path, text in updates.items():
            path.write_text(text)
        _run_lock_commands(root, check=False)
        _configured_versions(root, workflows)
        _run_lock_commands(root, check=True)
    except BaseException:
        _restore(originals)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci-version", help="exact uv version used by CI")
    parser.add_argument("--required-version", help="allowed local uv version range")
    parser.add_argument("--dry-run", action="store_true", help="report changes only")
    parser.add_argument("--check", action="store_true", help="verify policy and locks")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.root.resolve()
    try:
        workflows = _workflow_paths(root)
        if args.check:
            if args.dry_run or args.ci_version or args.required_version:
                raise PolicyError("--check cannot be combined with update options")
            ci_version, required_version = _configured_versions(root, workflows)
            _run_lock_commands(root, check=True)
            print(f"uv policy is current: CI {ci_version}; local {required_version}")
        else:
            if args.ci_version is None or args.required_version is None:
                raise PolicyError(
                    "updates require both --ci-version and --required-version"
                )
            _update(
                root,
                args.ci_version,
                args.required_version,
                dry_run=args.dry_run,
            )
    except (OSError, PolicyError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
