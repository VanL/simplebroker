"""Internal developer scripts exposed through project entry points."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from email.message import Message
from email.parser import BytesParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POSTGRES_IMAGE = os.environ.get("SIMPLEBROKER_PG_TEST_IMAGE", "postgres:17")
POSTGRES_DB = os.environ.get("SIMPLEBROKER_PG_TEST_DB", "simplebroker_test")
POSTGRES_USER = os.environ.get("SIMPLEBROKER_PG_TEST_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("SIMPLEBROKER_PG_TEST_PASSWORD", "postgres")


def _run(
    cmd: list[str],
    *,
    cwd: Path = ROOT,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess from the repository root and echo the command."""

    print(f"+ {shlex.join(cmd)}", flush=True)
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        check=True,
        capture_output=capture_output,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _docker_port(container_name: str) -> str | None:
    """Return the published host port for Postgres or None if not ready yet."""

    result = subprocess.run(
        ["docker", "port", container_name, "5432/tcp"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        return None
    output = result.stdout.strip()
    if not output:
        return None
    return output.rsplit(":", 1)[1]


def _cleanup_container(container_name: str) -> None:
    """Remove the temporary Docker container if it still exists."""

    subprocess.run(
        ["docker", "rm", "-f", container_name],
        cwd=ROOT,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for_postgres(container_name: str, *, timeout_seconds: float = 60.0) -> str:
    """Wait for the Postgres container to accept connections and return its host port."""

    deadline = time.time() + timeout_seconds
    last_error = "container did not start"

    while time.time() < deadline:
        port = _docker_port(container_name)
        if port is None:
            last_error = "waiting for published port"
            time.sleep(1.0)
            continue

        result = subprocess.run(
            [
                "docker",
                "exec",
                container_name,
                "pg_isready",
                "-U",
                POSTGRES_USER,
                "-d",
                POSTGRES_DB,
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            return port

        last_error = (
            result.stderr.strip() or result.stdout.strip() or "pg_isready failed"
        )
        time.sleep(1.0)

    raise RuntimeError(f"Postgres did not become ready: {last_error}")


def _start_postgres_container() -> tuple[str, str]:
    """Start the temporary Postgres container and return its name and DSN."""

    container_name = f"simplebroker-pg-test-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    _run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "--env",
            f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
            "--env",
            f"POSTGRES_USER={POSTGRES_USER}",
            "--env",
            f"POSTGRES_DB={POSTGRES_DB}",
            "--publish-all",
            POSTGRES_IMAGE,
            "-c",
            "max_connections=300",
        ],
        capture_output=True,
    )
    port = _wait_for_postgres(container_name)
    dsn = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@127.0.0.1:{port}/{POSTGRES_DB}"
    )
    return container_name, dsn


def _build_test_env(*, dsn: str, include_backend_marker: bool) -> dict[str, str]:
    """Build the environment used for PG-backed test runs."""

    env = os.environ.copy()
    env["SIMPLEBROKER_PG_TEST_DSN"] = dsn
    if include_backend_marker:
        env["BROKER_TEST_BACKEND"] = "postgres"
    return env


def _merge_marker_expressions(base: str, extra: str | None) -> str:
    """Combine marker expressions while preserving the base filter."""

    if not extra:
        return base
    return f"({base}) and ({extra})"


def _append_marker_expression(
    current: str | None,
    extra: str,
) -> str:
    """Accumulate multiple user-supplied marker expressions."""

    if not current:
        return extra
    return f"({current}) and ({extra})"


def _classify_pytest_target(arg: str) -> str | None:
    """Map a pytest path or node id to the shared or extension suite."""

    if arg.startswith("-"):
        return None

    path_part = arg.split("::", 1)[0]
    if not path_part:
        return None

    candidate = Path(path_part)
    if not candidate.is_absolute():
        candidate = (ROOT / candidate).resolve()

    try:
        relative = candidate.relative_to(ROOT).as_posix()
    except ValueError:
        return None

    if relative == "tests" or relative.startswith("tests/"):
        return "shared"
    if relative == "extensions/simplebroker_pg/tests" or relative.startswith(
        "extensions/simplebroker_pg/tests/"
    ):
        return "extension"
    return None


def _extract_pytest_runner_overrides(
    pytest_args: list[str],
) -> tuple[list[str], str | None, str | None, str | None]:
    """Extract pytest args that need to be merged with runner defaults."""

    remaining: list[str] = []
    marker_expr: str | None = None
    numprocesses: str | None = None
    dist: str | None = None

    index = 0
    while index < len(pytest_args):
        arg = pytest_args[index]

        if arg == "--":
            index += 1
            continue
        if arg == "-m":
            if index + 1 >= len(pytest_args):
                raise SystemExit("pytest-pg: -m requires an argument")
            marker_expr = _append_marker_expression(marker_expr, pytest_args[index + 1])
            index += 2
            continue
        if arg.startswith("-m") and arg != "-m":
            value = arg[2:]
            marker_expr = _append_marker_expression(marker_expr, value)
            index += 1
            continue
        if arg == "-n":
            if index + 1 >= len(pytest_args):
                raise SystemExit("pytest-pg: -n requires an argument")
            numprocesses = pytest_args[index + 1]
            index += 2
            continue
        if arg.startswith("-n") and arg != "-n":
            numprocesses = arg[2:]
            index += 1
            continue
        if arg == "--dist":
            if index + 1 >= len(pytest_args):
                raise SystemExit("pytest-pg: --dist requires an argument")
            dist = pytest_args[index + 1]
            index += 2
            continue
        if arg.startswith("--dist="):
            dist = arg.split("=", 1)[1]
            index += 1
            continue

        remaining.append(arg)
        index += 1

    return remaining, marker_expr, numprocesses, dist


def _route_pytest_args(
    pytest_args: list[str],
) -> tuple[list[str], list[str], bool, bool, str | None, str | None, str | None]:
    """Split passthrough pytest args between core and extension suites."""

    filtered_args, marker_expr, numprocesses, dist = _extract_pytest_runner_overrides(
        pytest_args
    )

    shared_args: list[str] = []
    extension_args: list[str] = []
    shared_selected = False
    extension_selected = False

    for arg in filtered_args:
        target = _classify_pytest_target(arg)
        if target == "shared":
            shared_selected = True
            shared_args.append(arg)
            continue
        if target == "extension":
            extension_selected = True
            extension_args.append(arg)
            continue

        shared_args.append(arg)
        extension_args.append(arg)

    has_explicit_targets = shared_selected or extension_selected
    return (
        shared_args,
        extension_args,
        not has_explicit_targets or shared_selected,
        not has_explicit_targets or extension_selected,
        marker_expr,
        numprocesses,
        dist,
    )


def pytest_pg_main() -> int:
    """Run the Postgres-backed SimpleBroker test suites with Docker setup."""

    parser = argparse.ArgumentParser(
        description="Run PG-backed SimpleBroker tests with automatic Docker setup."
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Run the release-gate subset (shared and not slow) instead of all "
            "shared tests."
        ),
    )
    parser.add_argument(
        "--keep-container",
        action="store_true",
        help="Leave the temporary Postgres container running for debugging.",
    )
    args, pytest_args = parser.parse_known_args()

    if shutil.which("docker") is None:
        print("docker is required to run PG-backed tests", file=sys.stderr)
        return 1
    if shutil.which("uv") is None:
        print("uv is required to run PG-backed tests", file=sys.stderr)
        return 1

    shared_marker = "shared and not slow" if args.fast else "shared"
    (
        shared_pytest_args,
        extension_pytest_args,
        run_shared_suite,
        run_extension_suite,
        extra_marker_expr,
        numprocesses,
        dist_mode,
    ) = _route_pytest_args(pytest_args)
    shared_marker = _merge_marker_expressions(shared_marker, extra_marker_expr)
    extension_marker = _merge_marker_expressions("pg_only", extra_marker_expr)
    numprocesses = numprocesses or "auto"
    dist_mode = dist_mode or "loadgroup"
    container_name: str | None = None

    try:
        container_name, dsn = _start_postgres_container()
        print(f"Postgres test DSN: {dsn}", flush=True)

        shared_env = _build_test_env(dsn=dsn, include_backend_marker=True)
        extension_env = _build_test_env(dsn=dsn, include_backend_marker=False)

        if run_shared_suite:
            _run(
                [
                    "uv",
                    "run",
                    "--extra",
                    "dev",
                    "--with-editable",
                    ".",
                    "--with-editable",
                    "./extensions/simplebroker_pg[dev]",
                    "pytest",
                    *(shared_pytest_args or ["tests"]),
                    "-m",
                    shared_marker,
                    "-n",
                    numprocesses,
                    "--dist",
                    dist_mode,
                ],
                env=shared_env,
            )

        if run_extension_suite:
            _run(
                [
                    "uv",
                    "run",
                    "--extra",
                    "dev",
                    "--with-editable",
                    ".",
                    "--with-editable",
                    "./extensions/simplebroker_pg[dev]",
                    "pytest",
                    *(extension_pytest_args or ["extensions/simplebroker_pg/tests"]),
                    "-m",
                    extension_marker,
                    "-n",
                    numprocesses,
                    "--dist",
                    dist_mode,
                ],
                env=extension_env,
            )
        return 0
    except subprocess.CalledProcessError as exc:
        return exc.returncode or 1
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    except Exception as exc:  # pragma: no cover - defensive CLI wrapper
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        if container_name and not args.keep_container:
            _cleanup_container(container_name)


def _require_single_wheel(dist_dir: Path, pattern: str) -> Path:
    wheels = sorted(dist_dir.glob(pattern))
    if len(wheels) != 1:
        raise RuntimeError(
            f"Expected exactly one wheel matching {pattern!r} in {dist_dir}, "
            f"found {len(wheels)}"
        )
    return wheels[0]


def _read_wheel_metadata(wheel_path: Path) -> Message:
    with zipfile.ZipFile(wheel_path) as wheel:
        metadata_name = next(
            (name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")),
            None,
        )
        if metadata_name is None:
            raise RuntimeError(f"Wheel {wheel_path} is missing .dist-info/METADATA")
        metadata_bytes = wheel.read(metadata_name)
    return BytesParser().parsebytes(metadata_bytes)


def _assert_metadata_contains(values: list[str], *, needle: str, context: str) -> None:
    if not any(needle in value for value in values):
        raise RuntimeError(f"Expected {context} to contain {needle!r}, got {values!r}")


def _venv_python(env_dir: Path) -> Path:
    if os.name == "nt":
        return env_dir / "Scripts" / "python.exe"
    return env_dir / "bin" / "python"


def _remove_build_outputs() -> None:
    for path in (ROOT / "dist", ROOT / "extensions" / "simplebroker_pg" / "dist"):
        shutil.rmtree(path, ignore_errors=True)


def _build_distribution(project_dir: Path) -> None:
    _run(
        ["uv", "run", "--with", "build", "python", "-m", "build"],
        cwd=project_dir,
    )


def packaging_smoke_main() -> int:
    """Build wheels, inspect metadata, and smoke-install the pg extra."""

    parser = argparse.ArgumentParser(
        description="Build and smoke-test SimpleBroker packaging artifacts."
    )
    parser.add_argument(
        "--python",
        default="3.10",
        help="Python version or interpreter to use for the install smoke env.",
    )
    args = parser.parse_args()

    if shutil.which("uv") is None:
        print("uv is required to run packaging smoke tests", file=sys.stderr)
        return 1

    try:
        _remove_build_outputs()

        _build_distribution(ROOT)
        _build_distribution(ROOT / "extensions" / "simplebroker_pg")

        root_dist = ROOT / "dist"
        extension_dist = ROOT / "extensions" / "simplebroker_pg" / "dist"

        root_wheel = _require_single_wheel(root_dist, "simplebroker-*.whl")
        extension_wheel = _require_single_wheel(extension_dist, "simplebroker_pg-*.whl")

        root_metadata = _read_wheel_metadata(root_wheel)
        extension_metadata = _read_wheel_metadata(extension_wheel)

        provides_extra = root_metadata.get_all("Provides-Extra", [])
        requires_dist = root_metadata.get_all("Requires-Dist", [])
        requires_python = extension_metadata.get("Requires-Python", "")
        root_version = root_metadata.get("Version")

        if "pg" not in provides_extra:
            raise RuntimeError(
                f"Expected root wheel to provide extra 'pg', got {provides_extra!r}"
            )
        _assert_metadata_contains(
            requires_dist,
            needle="simplebroker-pg",
            context="root wheel Requires-Dist",
        )
        _assert_metadata_contains(
            requires_dist,
            needle="extra == 'pg'",
            context="root wheel Requires-Dist",
        )
        if requires_python != ">=3.10":
            raise RuntimeError(
                "Expected extension wheel Requires-Python to be '>=3.10', got "
                f"{requires_python!r}"
            )
        if not root_version:
            raise RuntimeError(f"Wheel {root_wheel} is missing a Version header")

        with tempfile.TemporaryDirectory(prefix="simplebroker-packaging-smoke-") as tmp:
            env_dir = Path(tmp) / "venv"
            _run(["uv", "venv", "--python", args.python, str(env_dir)])
            venv_python = _venv_python(env_dir)

            _run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(venv_python),
                    "--find-links",
                    str(root_dist),
                    "--find-links",
                    str(extension_dist),
                    f"simplebroker[pg] @ {root_wheel.resolve().as_uri()}",
                ]
            )

            _run(
                [
                    str(venv_python),
                    "-c",
                    (
                        "import simplebroker_pg; "
                        "from simplebroker.ext import get_backend_plugin; "
                        "plugin = get_backend_plugin('postgres'); "
                        "assert plugin.name == 'postgres'"
                    ),
                ]
            )

        print(
            "Packaging smoke passed for "
            f"simplebroker {root_version} on Python {args.python}",
            flush=True,
        )
        return 0
    except subprocess.CalledProcessError as exc:
        return exc.returncode or 1
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    except Exception as exc:  # pragma: no cover - defensive CLI wrapper
        print(str(exc), file=sys.stderr)
        return 1


__all__ = ["packaging_smoke_main", "pytest_pg_main"]
