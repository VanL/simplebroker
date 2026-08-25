"""Internal developer scripts exposed through project entry points."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import importlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
import zipfile
from dataclasses import dataclass
from email.message import Message
from email.parser import BytesParser
from pathlib import Path
from typing import Any, cast
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from ._targets import redact_backend_target

# Module-owned clock/rng seam: tests patch these aliases instead of the
# shared stdlib attributes, which background threads, destructors, and
# concurrent tests can observe. Default binding is the real stdlib
# function; production behavior is identical.
_monotonic = time.monotonic
_sleep = time.sleep

ROOT = Path(__file__).resolve().parents[1]
POSTGRES_IMAGE = os.environ.get("SIMPLEBROKER_PG_TEST_IMAGE", "postgres:18")
POSTGRES_DB = os.environ.get("SIMPLEBROKER_PG_TEST_DB", "simplebroker_test")
POSTGRES_USER = os.environ.get("SIMPLEBROKER_PG_TEST_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("SIMPLEBROKER_PG_TEST_PASSWORD", "postgres")
VALKEY_IMAGE = os.environ.get("SIMPLEBROKER_VALKEY_TEST_IMAGE", "valkey/valkey:7.2")
_SHARED_BACKEND_MARKER = "shared and not sqlite_only"
_PUBLISHED_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
_PYPI_ROOT_RELEASE_URL = "https://pypi.org/pypi/simplebroker/{version}/json"

_ARTIFACT_ORIGIN_CHECK = (
    "package_file = Path(simplebroker.__file__ or '').resolve()\n"
    "environment_root = Path(sys.prefix).resolve()\n"
    "try:\n"
    "    package_file.relative_to(environment_root)\n"
    "except ValueError as exc:\n"
    "    raise RuntimeError(\n"
    '        f"simplebroker imported from {package_file}, outside the smoke "\n'
    '        f"virtual environment {environment_root}"\n'
    "    ) from exc\n"
)

_ROOT_ARTIFACT_PROBE = (
    "import sys\n"
    "from pathlib import Path\n"
    "from tempfile import TemporaryDirectory\n"
    "import simplebroker\n"
    + _ARTIFACT_ORIGIN_CHECK
    + "from simplebroker import CloseableIterator, Queue\n"
    "if CloseableIterator.__name__ != 'CloseableIterator':\n"
    "    raise RuntimeError('CloseableIterator root export is unavailable')\n"
    "with TemporaryDirectory(prefix='simplebroker-root-probe-') as tmp:\n"
    "    for persistent in (False, True):\n"
    "        db_path = str(Path(tmp) / f'broker-{persistent}.db')\n"
    "        queue = Queue('artifact_probe', db_path=db_path, persistent=persistent)\n"
    "        try:\n"
    "            queue.write('first')\n"
    "            queue.write('second')\n"
    "            iterator = queue.peek_generator()\n"
    "            if next(iterator) != 'first':\n"
    "                raise RuntimeError('peek generator returned an unexpected row')\n"
    "            iterator.close()\n"
    "            if queue.peek_one() != 'first':\n"
    "                raise RuntimeError('Queue was not reusable after early close')\n"
    "            queue.write('third')\n"
    "        finally:\n"
    "            queue.close()\n"
    "print('simplebroker-root-artifact-smoke-passed')\n"
)

_EXTENSION_ARTIFACT_PROBE = (
    "import sys\n"
    "from pathlib import Path\n"
    "import simplebroker\n" + _ARTIFACT_ORIGIN_CHECK + "import simplebroker_pg\n"
    "import simplebroker_redis\n"
    "from simplebroker.ext import get_backend_plugin\n"
    "pg_plugin = get_backend_plugin('postgres')\n"
    "redis_plugin = get_backend_plugin('redis')\n"
    "if pg_plugin.name != 'postgres':\n"
    "    raise RuntimeError(\n"
    "        f\"Packaging smoke expected backend 'postgres', "
    'got {pg_plugin.name!r}"\n'
    "    )\n"
    "if redis_plugin.name != 'redis':\n"
    "    raise RuntimeError(\n"
    "        f\"Packaging smoke expected backend 'redis', "
    'got {redis_plugin.name!r}"\n'
    "    )\n"
)


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


def _host_port_accepts_connections(
    port: str,
    *,
    timeout_seconds: float = 1.0,
) -> tuple[bool, str]:
    """Return whether the host can connect to Docker's published Postgres port."""

    try:
        port_number = int(port)
    except ValueError as exc:
        return False, f"invalid published port {port!r}: {exc}"

    try:
        with socket.create_connection(
            ("127.0.0.1", port_number),
            timeout=timeout_seconds,
        ):
            return True, ""
    except OSError as exc:
        return False, str(exc)


def _wait_for_postgres(container_name: str, *, timeout_seconds: float = 60.0) -> str:
    """Wait for the Postgres container to accept connections and return its host port."""

    deadline = _monotonic() + timeout_seconds
    last_error = "container did not start"

    while _monotonic() < deadline:
        port = _docker_port(container_name)
        if port is None:
            last_error = "waiting for published port"
            _sleep(1.0)
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
            host_ready, host_error = _host_port_accepts_connections(port)
            if host_ready:
                return port
            last_error = (
                f"waiting for host connection to 127.0.0.1:{port}: {host_error}"
            )
            _sleep(1.0)
            continue

        last_error = (
            result.stderr.strip() or result.stdout.strip() or "pg_isready failed"
        )
        _sleep(1.0)

    raise RuntimeError(f"Postgres did not become ready: {last_error}")


def _start_postgres_container() -> tuple[str, str]:
    """Start the temporary Postgres container and return its name and DSN."""

    container_name = f"simplebroker-pg-test-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    container_env = os.environ.copy()
    container_env.update(
        {
            "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
            "POSTGRES_USER": POSTGRES_USER,
            "POSTGRES_DB": POSTGRES_DB,
        }
    )
    _run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "--env",
            "POSTGRES_PASSWORD",
            "--env",
            "POSTGRES_USER",
            "--env",
            "POSTGRES_DB",
            "--publish-all",
            POSTGRES_IMAGE,
            "-c",
            "max_connections=300",
        ],
        env=container_env,
        capture_output=True,
    )
    try:
        port = _wait_for_postgres(container_name)
    except BaseException:
        _cleanup_container(container_name)
        raise
    dsn = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@127.0.0.1:{port}/{POSTGRES_DB}"
    )
    return container_name, dsn


def _valkey_docker_port(container_name: str) -> str | None:
    """Return the published host port for Valkey or None if not ready yet."""

    result = subprocess.run(
        ["docker", "port", container_name, "6379/tcp"],
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


def _wait_for_valkey(container_name: str, *, timeout_seconds: float = 30.0) -> str:
    """Wait for the Valkey container to accept connections and return its host port."""

    deadline = _monotonic() + timeout_seconds
    last_error = "container did not start"
    while _monotonic() < deadline:
        port = _valkey_docker_port(container_name)
        if port is None:
            last_error = "waiting for published port"
            _sleep(0.5)
            continue
        try:
            with socket.create_connection(("127.0.0.1", int(port)), timeout=1.0):
                return port
        except OSError as exc:
            last_error = str(exc)
            _sleep(0.5)
    raise RuntimeError(f"Valkey did not become ready: {last_error}")


def _start_valkey_container() -> tuple[str, str]:
    """Start the temporary Valkey container and return its name and URL."""

    container_name = f"simplebroker-valkey-test-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    _run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "--publish-all",
            VALKEY_IMAGE,
        ]
    )
    try:
        port = _wait_for_valkey(container_name)
    except BaseException:
        _cleanup_container(container_name)
        raise
    return container_name, f"redis://127.0.0.1:{port}/15"


def _build_test_env(*, dsn: str, include_backend_marker: bool) -> dict[str, str]:
    """Build the environment used for PG-backed test runs."""

    env = os.environ.copy()
    env["SIMPLEBROKER_PG_TEST_DSN"] = dsn
    if include_backend_marker:
        env["BROKER_TEST_BACKEND"] = "postgres"
    return env


def _pg_test_uv_command(*args: str) -> list[str]:
    """Build a uv command with the dependencies used by PG-backed tests."""

    return [
        "uv",
        "run",
        "--project",
        str(ROOT),
        "--locked",
        "--extra",
        "dev",
        "--extra",
        "pg",
        *args,
    ]


def _redis_test_uv_command(*args: str) -> list[str]:
    """Build a uv command with the dependencies used by Redis-backed tests."""

    return [
        "uv",
        "run",
        "--project",
        str(ROOT),
        "--locked",
        "--extra",
        "dev",
        "--extra",
        "redis",
        *args,
    ]


_POSTGRES_DSN_VERIFY_COMMAND = (
    "from simplebroker._scripts import _verify_postgres_test_dsn_from_env; "
    "_verify_postgres_test_dsn_from_env()"
)


def _verify_postgres_test_dsn_from_env() -> None:
    """Verify the PG test DSN from the current process environment."""

    psycopg = cast(Any, importlib.import_module("psycopg"))

    dsn = os.environ["SIMPLEBROKER_PG_TEST_DSN"]
    deadline = _monotonic() + float(
        os.environ.get("SIMPLEBROKER_PG_TEST_DSN_READY_TIMEOUT", "60")
    )
    retry_interval = float(
        os.environ.get("SIMPLEBROKER_PG_TEST_DSN_RETRY_INTERVAL", "0.5")
    )
    last_error = "connection not attempted"

    while True:
        try:
            with (
                psycopg.connect(dsn, connect_timeout=5) as conn,
                conn.cursor() as cur,
            ):
                cur.execute("SELECT 1")
                row = cur.fetchone()
                if row != (1,):
                    raise RuntimeError(
                        f"Postgres verification query returned unexpected row: {row!r}"
                    )
            return
        except psycopg.OperationalError as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if _monotonic() >= deadline:
                print(
                    f"Postgres test DSN was not ready: {last_error}",
                    file=sys.stderr,
                )
                raise
            _sleep(retry_interval)


def _verify_postgres_test_dsn(dsn: str, *, timeout_seconds: float = 60.0) -> None:
    """Verify the test runner can connect to the exact host DSN before pytest."""

    env = _build_test_env(dsn=dsn, include_backend_marker=False)
    env["SIMPLEBROKER_PG_TEST_DSN_READY_TIMEOUT"] = f"{timeout_seconds:.6f}"
    _run(
        _pg_test_uv_command(
            "python",
            "-c",
            _POSTGRES_DSN_VERIFY_COMMAND,
        ),
        env=env,
    )


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


def _classify_pytest_target(
    arg: str,
    *,
    extension_suite_path: str = "extensions/simplebroker_pg/tests",
) -> str | None:
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
    if relative == extension_suite_path or relative.startswith(
        f"{extension_suite_path}/"
    ):
        return "extension"
    return None


# Per-test hang bound applied by the wrapper when the caller does not
# choose their own timeout discipline. Job-level workflow ceilings still
# bound the Docker/setup phases that run before pytest starts.
_DEFAULT_PYTEST_TIMEOUT_ARGS = ("--timeout=180", "--timeout-method=thread")


def _with_default_timeout_bounds(args: list[str]) -> list[str]:
    """Append the default per-test timeout unless the caller set one.

    Add-if-missing only (audit plan Task 8.1): an explicit --timeout,
    --timeout-method, or worker-restart control from the caller is never
    overridden by an appended default.
    """
    explicit = ("--timeout", "--timeout-method", "--max-worker-restart")
    for arg in args:
        if arg in explicit or arg.startswith(tuple(f"{name}=" for name in explicit)):
            return args
    return [*args, *_DEFAULT_PYTEST_TIMEOUT_ARGS]


def _with_default_suite_path(args: list[str], default_path: str) -> list[str]:
    """Append the suite's default test path unless a target was routed.

    Flag-only invocations (e.g. ``pytest-pg -q``) route the flags into both
    suites' argument lists; a bare ``args or [default]`` then skips the
    default path and the suite collects nothing. Flag values (``-k foo``)
    do not classify as targets, so they do not suppress the default either.
    """

    extension_suite_path = (
        default_path if default_path != "tests" else "extensions/simplebroker_pg/tests"
    )
    if any(
        _classify_pytest_target(
            arg,
            extension_suite_path=extension_suite_path,
        )
        is not None
        for arg in args
    ):
        return list(args)
    return [*args, default_path]


def _extract_pytest_runner_overrides(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-012] exception
    pytest_args: list[str],
    *,
    runner_name: str = "pytest-pg",
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
                raise SystemExit(f"{runner_name}: -m requires an argument")
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
                raise SystemExit(f"{runner_name}: -n requires an argument")
            numprocesses = pytest_args[index + 1]
            index += 2
            continue
        if arg.startswith("-n") and arg != "-n":
            numprocesses = arg[2:]
            index += 1
            continue
        if arg == "--dist":
            if index + 1 >= len(pytest_args):
                raise SystemExit(f"{runner_name}: --dist requires an argument")
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
    *,
    extension_suite_path: str = "extensions/simplebroker_pg/tests",
    runner_name: str = "pytest-pg",
) -> tuple[list[str], list[str], bool, bool, str | None, str | None, str | None]:
    """Split passthrough pytest args between core and extension suites."""

    filtered_args, marker_expr, numprocesses, dist = _extract_pytest_runner_overrides(
        pytest_args,
        runner_name=runner_name,
    )

    shared_args: list[str] = []
    extension_args: list[str] = []
    shared_selected = False
    extension_selected = False

    for arg in filtered_args:
        target = _classify_pytest_target(
            arg,
            extension_suite_path=extension_suite_path,
        )
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
            "Run the release-gate subset (shared, not SQLite-only, and not "
            "benchmark) instead of all shared backend tests."
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

    shared_marker = _SHARED_BACKEND_MARKER
    if args.fast:
        shared_marker += " and not benchmark"
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
        print(f"Postgres test DSN: {redact_backend_target(dsn)}", flush=True)
        _verify_postgres_test_dsn(dsn)

        shared_env = _build_test_env(dsn=dsn, include_backend_marker=True)
        extension_env = _build_test_env(dsn=dsn, include_backend_marker=False)

        if run_shared_suite:
            _run(
                _pg_test_uv_command(
                    "pytest",
                    *_with_default_timeout_bounds(
                        _with_default_suite_path(shared_pytest_args, "tests")
                    ),
                    "-m",
                    shared_marker,
                    "-n",
                    numprocesses,
                    "--dist",
                    dist_mode,
                ),
                env=shared_env,
            )

        if run_extension_suite:
            _run(
                _pg_test_uv_command(
                    "pytest",
                    *_with_default_timeout_bounds(
                        _with_default_suite_path(
                            extension_pytest_args, "extensions/simplebroker_pg/tests"
                        )
                    ),
                    "-m",
                    extension_marker,
                    "-n",
                    numprocesses,
                    "--dist",
                    dist_mode,
                ),
                env=extension_env,
            )
        return 0
    except subprocess.CalledProcessError as exc:
        return exc.returncode or 1
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    except Exception as exc:  # pragma: no cover - defensive CLI wrapper  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        if container_name and not args.keep_container:
            _cleanup_container(container_name)


def pytest_redis_main() -> int:
    """Run Redis-backed SimpleBroker test suites with Docker setup."""

    parser = argparse.ArgumentParser(
        description="Run Redis-backed tests with automatic Docker setup."
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Run the release-gate subset (shared, not SQLite-only, and not "
            "benchmark) instead of all shared backend tests."
        ),
    )
    parser.add_argument(
        "--keep-container",
        action="store_true",
        help="Leave the temporary Valkey container running for debugging.",
    )
    args, pytest_args = parser.parse_known_args()

    if shutil.which("docker") is None:
        print("docker is required to run Redis-backed tests", file=sys.stderr)
        return 1
    if shutil.which("uv") is None:
        print("uv is required to run Redis-backed tests", file=sys.stderr)
        return 1

    shared_marker = _SHARED_BACKEND_MARKER
    if args.fast:
        shared_marker += " and not benchmark"
    (
        shared_pytest_args,
        extension_pytest_args,
        run_shared_suite,
        run_extension_suite,
        extra_marker_expr,
        numprocesses,
        dist_mode,
    ) = _route_pytest_args(
        pytest_args,
        extension_suite_path="extensions/simplebroker_redis/tests",
        runner_name="pytest-redis",
    )
    shared_marker = _merge_marker_expressions(shared_marker, extra_marker_expr)
    extension_marker = _merge_marker_expressions("redis_only", extra_marker_expr)
    numprocesses = numprocesses or "auto"
    dist_mode = dist_mode or "loadgroup"
    container_name: str | None = None

    try:
        container_name, url = _start_valkey_container()
        print(f"Valkey test URL: {redact_backend_target(url)}", flush=True)

        shared_env = os.environ.copy()
        shared_env["SIMPLEBROKER_VALKEY_TEST_URL"] = url
        shared_env["BROKER_TEST_BACKEND"] = "redis"
        extension_env = os.environ.copy()
        extension_env["SIMPLEBROKER_VALKEY_TEST_URL"] = url

        if run_shared_suite:
            _run(
                _redis_test_uv_command(
                    "pytest",
                    *_with_default_timeout_bounds(
                        _with_default_suite_path(shared_pytest_args, "tests")
                    ),
                    "-m",
                    shared_marker,
                    "-n",
                    numprocesses,
                    "--dist",
                    dist_mode,
                ),
                env=shared_env,
            )

        if run_extension_suite:
            _run(
                _redis_test_uv_command(
                    "pytest",
                    *_with_default_timeout_bounds(
                        _with_default_suite_path(
                            extension_pytest_args,
                            "extensions/simplebroker_redis/tests",
                        )
                    ),
                    "-m",
                    extension_marker,
                    "-n",
                    numprocesses,
                    "--dist",
                    dist_mode,
                ),
                env=extension_env,
            )
        return 0
    except subprocess.CalledProcessError as exc:
        return exc.returncode or 1
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    except Exception as exc:  # pragma: no cover - defensive CLI wrapper  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
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


def _distribution_member_names(archive_path: Path) -> list[str]:
    if archive_path.suffix == ".whl":
        with zipfile.ZipFile(archive_path) as archive:
            return archive.namelist()
    if archive_path.name.endswith(".tar.gz"):
        with tarfile.open(archive_path, "r:gz") as archive:
            return archive.getnames()
    raise RuntimeError(f"Unsupported distribution archive: {archive_path}")


def _assert_distribution_clean(archive_path: Path) -> None:
    banned_parts = {
        ".agents",
        ".claude",
        ".github",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        "node_modules",
        "tests",
    }
    banned_suffixes = (".pyc", ".pyo", ".db", ".db-shm", ".db-wal")
    offenders: list[str] = []

    for name in _distribution_member_names(archive_path):
        parts = set(Path(name).parts)
        if parts & banned_parts or name.endswith(banned_suffixes):
            offenders.append(name)

    if offenders:
        sample = ", ".join(offenders[:5])
        raise RuntimeError(
            f"Distribution {archive_path} contains excluded paths: {sample}"
        )


def _assert_wheel_contains_license(wheel_path: Path) -> None:
    license_names = {
        Path(name).name
        for name in _distribution_member_names(wheel_path)
        if ".dist-info/licenses" in Path(name).parts or Path(name).name == "LICENSE"
    }
    if "LICENSE" not in license_names:
        raise RuntimeError(f"Wheel {wheel_path} is missing bundled LICENSE")


def _assert_metadata_contains(values: list[str], *, needle: str, context: str) -> None:
    if not any(needle in value for value in values):
        raise RuntimeError(f"Expected {context} to contain {needle!r}, got {values!r}")


def _venv_python(env_dir: Path) -> Path:
    if os.name == "nt":
        return env_dir / "Scripts" / "python.exe"
    return env_dir / "bin" / "python"


def _remove_build_outputs() -> None:
    for path in (
        ROOT / "dist",
        ROOT / "extensions" / "simplebroker_pg" / "dist",
        ROOT / "extensions" / "simplebroker_redis" / "dist",
    ):
        shutil.rmtree(path, ignore_errors=True)


def _build_distribution(project_dir: Path) -> None:
    _run(
        [
            "uv",
            "run",
            "--project",
            str(ROOT),
            "--locked",
            "--group",
            "release",
            "python",
            "-m",
            "build",
            "--no-isolation",
            str(project_dir),
        ],
        cwd=ROOT,
    )


@dataclass(frozen=True)
class _PackagingArtifacts:
    root_wheel: Path
    root_sdist: Path
    pg_wheel: Path
    pg_sdist: Path
    redis_wheel: Path
    redis_sdist: Path


@dataclass(frozen=True)
class _RootPackagingArtifacts:
    root_wheel: Path
    root_sdist: Path


def _validate_published_version(version: str) -> str:
    """Require the exact release-version shape owned by the release driver."""

    normalized = version.strip()
    if not _PUBLISHED_VERSION_PATTERN.fullmatch(normalized):
        raise RuntimeError(
            "--published-version requires an exact X.Y.Z release version"
        )
    return normalized


def _read_url_bytes(url: str) -> bytes:
    """Fetch one release resource with a bounded network timeout."""

    request = urllib_request.Request(
        url,
        headers={"User-Agent": "simplebroker-packaging-smoke"},
    )
    with urllib_request.urlopen(request, timeout=30) as response:
        return cast(bytes, response.read())


def _select_published_root_file(
    release: dict[str, Any],
    *,
    version: str,
    package_type: str,
) -> dict[str, Any]:
    """Select exactly one root wheel or sdist from PyPI release metadata."""

    urls = release.get("urls")
    if not isinstance(urls, list):
        raise TypeError("PyPI release metadata is missing its artifact list")

    if package_type == "bdist_wheel":
        expected_prefix = f"simplebroker-{version}-"

        def matches(filename: str) -> bool:
            return filename.startswith(expected_prefix) and filename.endswith(".whl")

        label = "wheel"
    elif package_type == "sdist":

        def matches(filename: str) -> bool:
            return filename == f"simplebroker-{version}.tar.gz"

        label = "sdist"
    else:  # pragma: no cover - internal caller invariant
        raise RuntimeError(f"Unsupported PyPI artifact type {package_type!r}")

    candidates: list[dict[str, Any]] = []
    for value in urls:
        if not isinstance(value, dict) or value.get("packagetype") != package_type:
            continue
        filename = value.get("filename")
        if isinstance(filename, str) and matches(filename):
            candidates.append(cast(dict[str, Any], value))

    if len(candidates) != 1:
        filenames = [candidate.get("filename") for candidate in candidates]
        raise RuntimeError(
            f"Expected exactly one simplebroker {version} {label}, got {filenames!r}"
        )
    return candidates[0]


def _download_verified_published_file(
    entry: dict[str, Any],
    *,
    destination: Path,
    label: str,
) -> Path:
    """Download one PyPI artifact and require its indexed SHA-256."""

    filename = entry.get("filename")
    url = entry.get("url")
    digests = entry.get("digests")
    if not isinstance(filename, str) or Path(filename).name != filename:
        raise RuntimeError(f"PyPI {label} metadata has an unsafe filename")
    if not isinstance(url, str) or urllib_parse.urlparse(url).scheme != "https":
        raise RuntimeError(f"PyPI {label} metadata has a non-HTTPS URL")
    if not isinstance(digests, dict):
        raise TypeError(f"PyPI {label} metadata is missing digests")
    expected_digest = digests.get("sha256")
    if not isinstance(expected_digest, str) or not re.fullmatch(
        r"[0-9a-fA-F]{64}", expected_digest
    ):
        raise RuntimeError(f"PyPI {label} metadata has an invalid SHA-256")

    content = _read_url_bytes(url)
    actual_digest = hashlib.sha256(content).hexdigest()
    if not hmac.compare_digest(actual_digest, expected_digest.lower()):
        raise RuntimeError(
            f"SHA-256 mismatch for {filename}: expected "
            f"{expected_digest.lower()}, got {actual_digest}"
        )

    artifact_path = destination / filename
    artifact_path.write_bytes(content)
    print(
        f"Verified published {label} {filename} sha256={actual_digest}",
        flush=True,
    )
    return artifact_path


def _download_published_root_artifacts(
    version: str,
    destination: Path,
) -> _RootPackagingArtifacts:
    """Download an exact published root wheel and sdist with digest checks."""

    normalized_version = _validate_published_version(version)
    metadata_url = _PYPI_ROOT_RELEASE_URL.format(
        version=urllib_parse.quote(normalized_version, safe="")
    )
    try:
        raw_release = json.loads(_read_url_bytes(metadata_url))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RuntimeError("PyPI returned invalid release metadata") from exc
    if not isinstance(raw_release, dict):
        raise TypeError("PyPI returned invalid release metadata")
    release = cast(dict[str, Any], raw_release)

    info = release.get("info")
    if not isinstance(info, dict):
        raise TypeError("PyPI release metadata is missing package information")
    if info.get("name") != "simplebroker" or info.get("version") != normalized_version:
        raise RuntimeError(
            f"PyPI release metadata did not match simplebroker=={normalized_version}"
        )

    destination.mkdir(parents=True, exist_ok=True)
    wheel_entry = _select_published_root_file(
        release,
        version=normalized_version,
        package_type="bdist_wheel",
    )
    sdist_entry = _select_published_root_file(
        release,
        version=normalized_version,
        package_type="sdist",
    )
    return _RootPackagingArtifacts(
        root_wheel=_download_verified_published_file(
            wheel_entry,
            destination=destination,
            label="wheel",
        ),
        root_sdist=_download_verified_published_file(
            sdist_entry,
            destination=destination,
            label="sdist",
        ),
    )


def _build_packaging_artifacts() -> _PackagingArtifacts:
    """Build and locate every released distribution artifact."""
    _remove_build_outputs()
    _build_distribution(ROOT)
    _build_distribution(ROOT / "extensions" / "simplebroker_pg")
    _build_distribution(ROOT / "extensions" / "simplebroker_redis")

    root_dist = ROOT / "dist"
    pg_dist = ROOT / "extensions" / "simplebroker_pg" / "dist"
    redis_dist = ROOT / "extensions" / "simplebroker_redis" / "dist"
    return _PackagingArtifacts(
        root_wheel=_require_single_wheel(root_dist, "simplebroker-*.whl"),
        root_sdist=_require_single_wheel(root_dist, "simplebroker-*.tar.gz"),
        pg_wheel=_require_single_wheel(pg_dist, "simplebroker_pg-*.whl"),
        pg_sdist=_require_single_wheel(pg_dist, "simplebroker_pg-*.tar.gz"),
        redis_wheel=_require_single_wheel(redis_dist, "simplebroker_redis-*.whl"),
        redis_sdist=_require_single_wheel(redis_dist, "simplebroker_redis-*.tar.gz"),
    )


def _inspect_packaging_artifacts(artifacts: _PackagingArtifacts) -> str:
    """Validate artifact contents and return the root package version."""
    for archive_path in (
        artifacts.root_wheel,
        artifacts.root_sdist,
        artifacts.pg_wheel,
        artifacts.pg_sdist,
        artifacts.redis_wheel,
        artifacts.redis_sdist,
    ):
        _assert_distribution_clean(archive_path)
    for wheel_path in (
        artifacts.root_wheel,
        artifacts.pg_wheel,
        artifacts.redis_wheel,
    ):
        _assert_wheel_contains_license(wheel_path)

    root_metadata = _read_wheel_metadata(artifacts.root_wheel)
    pg_metadata = _read_wheel_metadata(artifacts.pg_wheel)
    redis_metadata = _read_wheel_metadata(artifacts.redis_wheel)

    provides_extra = root_metadata.get_all("Provides-Extra", [])
    requires_dist = root_metadata.get_all("Requires-Dist", [])
    if "pg" not in provides_extra:
        raise RuntimeError(
            f"Expected root wheel to provide extra 'pg', got {provides_extra!r}"
        )
    if "redis" not in provides_extra:
        raise RuntimeError(
            f"Expected root wheel to provide extra 'redis', got {provides_extra!r}"
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
    _assert_metadata_contains(
        requires_dist,
        needle="simplebroker-redis",
        context="root wheel Requires-Dist",
    )
    _assert_metadata_contains(
        requires_dist,
        needle="extra == 'redis'",
        context="root wheel Requires-Dist",
    )

    pg_requires_python = pg_metadata.get("Requires-Python", "")
    if pg_requires_python != ">=3.11":
        raise RuntimeError(
            "Expected PG extension wheel Requires-Python to be '>=3.11', got "
            f"{pg_requires_python!r}"
        )
    redis_requires_python = redis_metadata.get("Requires-Python", "")
    if redis_requires_python != ">=3.11":
        raise RuntimeError(
            "Expected Redis extension wheel Requires-Python to be '>=3.11', got "
            f"{redis_requires_python!r}"
        )

    root_version = root_metadata.get("Version")
    if not root_version:
        raise RuntimeError(f"Wheel {artifacts.root_wheel} is missing a Version header")
    return root_version


def _artifact_probe_environment() -> dict[str, str]:
    """Return an inherited environment without source-path injection."""

    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    return environment


def _run_clean_artifact_probe(
    install_requirements: list[str],
    *,
    python: str,
    probe_source: str,
    temp_prefix: str,
) -> None:
    """Install artifacts and run a probe wholly outside the source checkout."""

    with tempfile.TemporaryDirectory(prefix=temp_prefix) as tmp:
        probe_root = Path(tmp)
        work_dir = probe_root / "work"
        work_dir.mkdir()
        env_dir = probe_root / "venv"
        environment = _artifact_probe_environment()
        _run(
            ["uv", "venv", "--python", python, str(env_dir)],
            cwd=work_dir,
            env=environment,
        )
        venv_python = _venv_python(env_dir)
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(venv_python),
                *install_requirements,
            ],
            cwd=work_dir,
            env=environment,
        )
        _run(
            [str(venv_python), "-c", probe_source],
            cwd=work_dir,
            env=environment,
        )


def _smoke_install_extension_wheels(
    artifacts: _PackagingArtifacts,
    *,
    python: str,
) -> None:
    """Install the three built wheels together and verify backend discovery."""

    _run_clean_artifact_probe(
        [
            f"simplebroker[pg,redis] @ {artifacts.root_wheel.resolve().as_uri()}",
            f"simplebroker-pg @ {artifacts.pg_wheel.resolve().as_uri()}",
            f"simplebroker-redis @ {artifacts.redis_wheel.resolve().as_uri()}",
        ],
        python=python,
        probe_source=_EXTENSION_ARTIFACT_PROBE,
        temp_prefix="simplebroker-extension-wheel-smoke-",
    )


def _smoke_install_root_artifact(
    artifact: Path,
    *,
    artifact_kind: str,
    python: str,
) -> None:
    """Install and exercise one root distribution in a clean environment."""

    _run_clean_artifact_probe(
        [f"simplebroker @ {artifact.resolve().as_uri()}"],
        python=python,
        probe_source=_ROOT_ARTIFACT_PROBE,
        temp_prefix=f"simplebroker-root-{artifact_kind}-smoke-",
    )
    print(f"Root {artifact_kind} artifact smoke passed", flush=True)


def _smoke_install_root_artifacts(
    artifacts: _RootPackagingArtifacts,
    *,
    python: str,
) -> None:
    """Probe the root wheel and sdist in separate clean environments."""

    _smoke_install_root_artifact(
        artifacts.root_wheel,
        artifact_kind="wheel",
        python=python,
    )
    _smoke_install_root_artifact(
        artifacts.root_sdist,
        artifact_kind="sdist",
        python=python,
    )


def _smoke_install_artifacts(
    artifacts: _PackagingArtifacts,
    *,
    python: str,
) -> None:
    """Probe extension wheels plus separate root wheel and sdist installs."""

    _smoke_install_extension_wheels(artifacts, python=python)
    _smoke_install_root_artifacts(
        _RootPackagingArtifacts(
            root_wheel=artifacts.root_wheel,
            root_sdist=artifacts.root_sdist,
        ),
        python=python,
    )


def packaging_smoke_main() -> int:
    """Smoke-test built artifacts or one exact published root release."""

    parser = argparse.ArgumentParser(
        description="Build and smoke-test SimpleBroker packaging artifacts."
    )
    parser.add_argument(
        "--python",
        default="3.11",
        help="Python version or interpreter to use for the install smoke env.",
    )
    parser.add_argument(
        "--published-version",
        help=(
            "Download and probe the exact published simplebroker X.Y.Z root "
            "wheel and sdist instead of building checkout artifacts."
        ),
    )
    args = parser.parse_args()

    if shutil.which("uv") is None:
        print("uv is required to run packaging smoke tests", file=sys.stderr)
        return 1

    try:
        if args.published_version is not None:
            published_version = _validate_published_version(args.published_version)
            with tempfile.TemporaryDirectory(
                prefix="simplebroker-published-artifacts-"
            ) as tmp:
                root_artifacts = _download_published_root_artifacts(
                    published_version,
                    Path(tmp),
                )
                _smoke_install_root_artifacts(root_artifacts, python=args.python)
            print(
                "Published packaging smoke passed for "
                f"simplebroker {published_version} on Python {args.python}",
                flush=True,
            )
            return 0

        artifacts = _build_packaging_artifacts()
        root_version = _inspect_packaging_artifacts(artifacts)
        _smoke_install_artifacts(artifacts, python=args.python)

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
    except Exception as exc:  # pragma: no cover - defensive CLI wrapper  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
        print(str(exc), file=sys.stderr)
        return 1


__all__ = [
    "_assert_distribution_clean",
    "_assert_wheel_contains_license",
    "packaging_smoke_main",
    "pytest_pg_main",
    "pytest_redis_main",
]
