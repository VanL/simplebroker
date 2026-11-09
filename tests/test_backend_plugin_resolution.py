"""Tests for public backend plugin resolution."""

from __future__ import annotations

import ast
import os
import select
import signal
import sys
import threading
import weakref
from collections.abc import Callable
from pathlib import Path
from typing import Any, Self

import pytest

import simplebroker._sql as sqlite_sql
from simplebroker import Queue, create_activity_waiter_for_queues
from simplebroker._backend_plugins import BACKEND_API_VERSION
from simplebroker._constants import __version__ as SIMPLEBROKER_VERSION
from simplebroker._exceptions import UnknownBackendPluginError
from simplebroker._runner import SetupPhase
from simplebroker._targets import BrokerTarget
from simplebroker.db import DBConnection
from simplebroker.ext import get_backend_plugin

pytestmark = [pytest.mark.shared]

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class EntryPointStub:
    def __init__(
        self, name: str, loaded: Any = None, *, error: BaseException | None = None
    ):
        self.name = name
        self._loaded = loaded
        self._error = error
        self.load_calls = 0

    def load(self) -> Any:
        self.load_calls += 1
        if self._error is not None:
            raise self._error
        return self._loaded


class EntryPointsMock(list[EntryPointStub]):
    def select(self, *, group: str, name: str) -> EntryPointsMock:
        if group != "simplebroker.backends":
            return EntryPointsMock()
        return EntryPointsMock(
            entry_point for entry_point in self if entry_point.name == name
        )


class CountingEntryPoints:
    """Installed-metadata stand-in that counts scans and can be reinstalled."""

    def __init__(self, *installed: EntryPointStub) -> None:
        self.installed = list(installed)
        self.scans = 0

    def __call__(self) -> EntryPointsMock:
        self.scans += 1
        return EntryPointsMock(self.installed)


def _use_entry_points(
    monkeypatch: pytest.MonkeyPatch,
    entry_points: Callable[[], EntryPointsMock],
) -> None:
    """Substitute installed entry points behind an empty resolution memo.

    Successful resolutions are memoized per process [SB-API-11]. A private
    memo keeps stub plugins from seeing, or leaking into, real resolutions;
    monkeypatch restores the process memo afterwards.
    """
    monkeypatch.setattr(
        "simplebroker._backend_plugins.metadata.entry_points", entry_points
    )
    monkeypatch.setattr(
        "simplebroker._backend_plugins._RESOLVED_ENTRY_POINT_PLUGINS", {}
    )


def _install_entry_point(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    loaded: Any = None,
    *,
    error: BaseException | None = None,
) -> None:
    _use_entry_points(
        monkeypatch,
        lambda: EntryPointsMock([EntryPointStub(name, loaded, error=error)]),
    )


class ValidDummyPlugin:
    name = "dummy"
    sql: Any = sqlite_sql
    backend_api_version: Any = BACKEND_API_VERSION
    schema_version = 1


@pytest.mark.sqlite_only
def test_builtin_sqlite_backend_plugin_resolves() -> None:
    """The built-in sqlite backend should resolve without entry points."""
    from simplebroker._runner import SQLiteRunner

    plugin = get_backend_plugin("sqlite")

    assert plugin.name == "sqlite"
    assert plugin.backend_api_version == BACKEND_API_VERSION
    assert plugin.sql is not None
    assert isinstance(plugin.create_runner(":memory:"), SQLiteRunner)


@pytest.mark.sqlite_only
def test_queue_backend_name_reports_sqlite_without_opening_target(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "not-created.db"
    queue = Queue("tasks", db_path=str(db_path))

    assert queue.backend_name == "sqlite"
    assert not db_path.exists()


@pytest.mark.sqlite_only
def test_sqlite_initialize_target_passes_config_snapshot_to_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import simplebroker.db as db_module
    from simplebroker import resolve_config

    marker = resolve_config(
        env=os.environ, override={"BROKER_EXTENSION_RECEIPT": "kept"}
    )
    received: list[object] = []

    class Broker:
        def __init__(self, target: str, *, config: object) -> None:
            received.append(config)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(db_module, "BrokerDB", Broker)

    get_backend_plugin("sqlite").initialize_target(":memory:", config=marker)

    assert received == [marker]


@pytest.mark.sqlite_only
def test_sqlite_plugin_reports_missing_magic_metadata() -> None:
    """An empty metadata table must not be mistaken for a managed database."""

    plugin = get_backend_plugin("sqlite")
    runner = plugin.create_runner(":memory:")
    try:
        runner.run("CREATE TABLE meta (key TEXT PRIMARY KEY, value)")

        assert plugin.read_magic(runner) is None
    finally:
        runner.close()


def test_unknown_backend_plugin_raises_clear_error() -> None:
    """Unknown backends use the internal typed RuntimeError-compatible seam."""
    with pytest.raises(
        UnknownBackendPluginError,
        match="Unknown backend plugin: missing",
    ) as exc_info:
        get_backend_plugin("missing")

    assert isinstance(exc_info.value, RuntimeError)


def test_external_backend_plugin_resolves_via_entry_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entry-point plugins should be loaded by name."""

    _install_entry_point(monkeypatch, "dummy", ValidDummyPlugin)

    plugin = get_backend_plugin("dummy")
    assert plugin.name == "dummy"


def test_queue_backend_name_preserves_third_party_plugin_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_entry_point(monkeypatch, "dummy", ValidDummyPlugin)
    queue = Queue(
        "tasks",
        db_path=BrokerTarget("dummy", "dummy-target", {}),
    )

    assert queue.backend_name == "dummy"


@pytest.mark.parametrize("backend_name", ["redis", "postgres"])
def test_queue_backend_name_preserves_first_party_extension_name_without_io(
    monkeypatch: pytest.MonkeyPatch,
    backend_name: str,
) -> None:
    class InitializablePlugin(ValidDummyPlugin):
        def init_backend(self, config: Any, **kwargs: Any) -> dict[str, Any]:
            assert self.name == "redis", "other plugin initializers must stay lazy"
            return {"target": kwargs["toml_target"], "backend_options": {}}

    plugin = InitializablePlugin()
    plugin.name = backend_name
    _install_entry_point(monkeypatch, backend_name, plugin)
    queue = Queue(
        "tasks",
        db_path=BrokerTarget(backend_name, f"{backend_name}-target", {}),
    )

    assert queue.backend_name == backend_name


@pytest.mark.parametrize("reverse_discovery", [False, True])
def test_duplicate_entry_points_fail_before_loading_either_candidate(
    monkeypatch: pytest.MonkeyPatch,
    reverse_discovery: bool,
) -> None:
    """Ambiguous registrations must not make discovery order authoritative."""
    first = EntryPointStub("dummy", ValidDummyPlugin)
    second = EntryPointStub("dummy", ValidDummyPlugin)
    discovered = [first, second]
    if reverse_discovery:
        discovered.reverse()
    _use_entry_points(monkeypatch, lambda: EntryPointsMock(discovered))

    with pytest.raises(
        RuntimeError,
        match="Ambiguous backend plugin 'dummy': 2 matching entry points",
    ):
        get_backend_plugin("dummy")

    assert first.load_calls == 0
    assert second.load_calls == 0


def test_successful_entry_point_resolution_is_memoized_per_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Later lookups reuse one validated plugin without rescanning [SB-API-11]."""
    entry_point = EntryPointStub("dummy", ValidDummyPlugin)
    installed = CountingEntryPoints(entry_point)
    _use_entry_points(monkeypatch, installed)

    first = get_backend_plugin("dummy")
    later = [get_backend_plugin("dummy") for _ in range(3)]

    assert installed.scans == 1
    assert entry_point.load_calls == 1
    assert all(plugin is first for plugin in later)


def test_queue_backend_identity_paths_reuse_the_memoized_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue construction and waiter grouping do not rescan per queue."""
    installed = CountingEntryPoints(EntryPointStub("dummy", ValidDummyPlugin))
    _use_entry_points(monkeypatch, installed)
    target = BrokerTarget("dummy", "dummy-target", {})

    queues = [Queue(name, db_path=target) for name in ("first", "second", "third")]
    for queue in queues:
        assert queue.backend_name == "dummy"
    # The stub backend has no multi-queue hook; grouping still resolves the
    # backend identity of every queue.
    waiter = create_activity_waiter_for_queues(queues, stop_event=threading.Event())

    assert waiter is None
    assert installed.scans == 1


class StaleVersionDummyPlugin(ValidDummyPlugin):
    backend_api_version = BACKEND_API_VERSION - 1


# Each failed-lookup kind named in [SB-API-11], with the error that proves the
# lookup failed for that reason rather than another.
_FAILED_REGISTRATIONS: dict[str, tuple[Callable[[], list[EntryPointStub]], str]] = {
    "unknown": (list, "Unknown backend plugin: dummy"),
    "ambiguous": (
        lambda: [
            EntryPointStub("dummy", ValidDummyPlugin),
            EntryPointStub("dummy", ValidDummyPlugin),
        ],
        "Ambiguous backend plugin 'dummy'",
    ),
    "load-failure": (
        lambda: [EntryPointStub("dummy", error=ImportError("partially installed"))],
        "could not be loaded .*partially installed",
    ),
    "validation-failure": (
        lambda: [EntryPointStub("dummy", StaleVersionDummyPlugin)],
        f"built against backend API v{BACKEND_API_VERSION - 1}",
    ),
}


@pytest.mark.parametrize("failure", sorted(_FAILED_REGISTRATIONS))
def test_failed_entry_point_resolution_is_not_memoized(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """The next lookup after a failure observes an install or repair."""
    registration, expected_error = _FAILED_REGISTRATIONS[failure]
    installed = CountingEntryPoints(*registration())
    _use_entry_points(monkeypatch, installed)

    with pytest.raises(RuntimeError, match=expected_error):
        get_backend_plugin("dummy")

    installed.installed = [EntryPointStub("dummy", ValidDummyPlugin)]
    plugin = get_backend_plugin("dummy")

    assert installed.scans == 2
    assert plugin.name == "dummy"
    assert plugin.backend_api_version == BACKEND_API_VERSION


def test_concurrent_first_lookups_share_one_plugin_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Racing first lookups may each load, but every caller gets one plugin."""
    both_loading = threading.Barrier(2)

    class RendezvousEntryPoint(EntryPointStub):
        def load(self) -> Any:
            try:
                both_loading.wait(timeout=1.0)
            except threading.BrokenBarrierError:
                pass  # A serialized resolver never has two loaders at once.
            return super().load()

    _use_entry_points(
        monkeypatch,
        CountingEntryPoints(RendezvousEntryPoint("dummy", ValidDummyPlugin)),
    )
    results: list[object] = []

    def resolve() -> None:
        # A resolver exception surfaces as pytest's unhandled-thread warning.
        results.append(get_backend_plugin("dummy"))

    threads = [threading.Thread(target=resolve) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert len(results) == 2
    assert results[0] is results[1]
    assert get_backend_plugin("dummy") is results[0]


def _assert_fork_child_succeeds(child: Callable[[], bool]) -> None:
    """Run ``child`` in a forked process and require it to return True promptly.

    A child blocked on state inherited from its parent is killed and reaped.
    """
    read_fd, write_fd = os.pipe()
    child_pid: int | None = None
    try:
        child_pid = os.fork()
        if child_pid == 0:
            status = 1
            try:
                os.close(read_fd)
                if child():
                    os.write(write_fd, b"ok")
                    status = 0
            finally:
                os._exit(status)

        os.close(write_fd)
        write_fd = -1
        ready, _, _ = select.select([read_fd], [], [], 5.0)
        assert ready, "forked child blocked on state inherited from its parent"
        assert os.read(read_fd, 16) == b"ok"
        _, status = os.waitpid(child_pid, 0)
        child_pid = None
        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == 0
    finally:
        if child_pid is not None:
            os.kill(child_pid, signal.SIGKILL)
            os.waitpid(child_pid, 0)
        os.close(read_fd)
        if write_fd != -1:
            os.close(write_fd)


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_forked_child_queue_does_not_reuse_parent_plugin_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A child resolves afresh, so a parent-held plugin lock cannot block it.

    The plugin's own lock is held by a parent thread at fork time. A child
    that reused the parent's plugin object would inherit that lock held.
    """

    class LockingPlugin(ValidDummyPlugin):
        name = "locking"

        def __init__(self) -> None:
            self.lock = threading.Lock()

        def create_activity_waiter(
            self,
            *,
            target: str | None,
            backend_options: dict[str, Any] | None = None,
            runner: Any = None,
            queue_name: str,
            stop_event: threading.Event,
        ) -> None:
            del target, backend_options, runner, queue_name, stop_event
            # A backend hook that serializes on plugin-owned state.
            with self.lock:
                pass

    _use_entry_points(
        monkeypatch,
        CountingEntryPoints(EntryPointStub("locking", LockingPlugin)),
    )
    parent_plugin = get_backend_plugin("locking")
    assert isinstance(parent_plugin, LockingPlugin)
    target = BrokerTarget("locking", str(tmp_path / "child.db"), {})
    held = threading.Event()
    release = threading.Event()

    def hold_plugin_lock() -> None:
        with parent_plugin.lock:
            held.set()
            release.wait(15.0)

    def child_queue_creates_waiter() -> bool:
        queue = Queue("child", db_path=target)
        return queue.create_activity_waiter(stop_event=threading.Event()) is None

    holder = threading.Thread(target=hold_plugin_lock)
    holder.start()
    try:
        assert held.wait(5.0)
        _assert_fork_child_succeeds(child_queue_creates_waiter)
    finally:
        release.set()
        holder.join(timeout=5.0)
    assert not holder.is_alive()


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_forked_child_never_finalizes_inherited_plugins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropping the memo in a fork child must not release inherited plugins.

    The memo holds the plugin's only reference, and the plugin's finalizer
    takes a lock that a parent thread holds at fork time. Releasing that
    reference in the child's fork hook would run the finalizer there, before
    ``os.fork()`` returns in the child, and block on the inherited lock.
    """
    finalizer_lock = threading.Lock()
    finalized = threading.Event()

    class FinalizingPlugin(ValidDummyPlugin):
        name = "finalizing"

        def __del__(self) -> None:
            with finalizer_lock:
                finalized.set()

    _use_entry_points(
        monkeypatch,
        CountingEntryPoints(EntryPointStub("finalizing", FinalizingPlugin)),
    )
    retained = weakref.ref(get_backend_plugin("finalizing"))
    # Only the memo keeps the plugin alive.
    assert retained() is not None
    assert not finalized.is_set()
    held = threading.Event()
    release = threading.Event()

    def hold_finalizer_lock() -> None:
        with finalizer_lock:
            held.set()
            release.wait(15.0)

    holder = threading.Thread(target=hold_finalizer_lock)
    holder.start()
    try:
        assert held.wait(5.0)
        _assert_fork_child_succeeds(
            lambda: get_backend_plugin("finalizing").name == "finalizing"
        )
    finally:
        release.set()
        holder.join(timeout=5.0)
    assert not holder.is_alive()


@pytest.mark.skipif(sys.platform == "win32", reason="fork() not available on Windows")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_forked_child_resolves_while_parent_resolution_is_in_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolution takes no lock that a forked child could inherit held."""
    parent_pid = os.getpid()
    loading = threading.Event()
    release = threading.Event()

    class InFlightPlugin(ValidDummyPlugin):
        name = "inflight"

    class ParentBlockingEntryPoint(EntryPointStub):
        def load(self) -> Any:
            if os.getpid() == parent_pid:
                loading.set()
                release.wait(15.0)
            return super().load()

    _use_entry_points(
        monkeypatch,
        CountingEntryPoints(ParentBlockingEntryPoint("inflight", InFlightPlugin)),
    )
    resolver = threading.Thread(target=get_backend_plugin, args=("inflight",))
    resolver.start()
    try:
        assert loading.wait(5.0)
        _assert_fork_child_succeeds(
            lambda: get_backend_plugin("inflight").name == "inflight"
        )
    finally:
        release.set()
        resolver.join(timeout=5.0)
    assert not resolver.is_alive()


def test_entry_point_plugin_name_must_match_the_requested_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A misregistered entry point must not impersonate another backend."""

    class MismatchedPlugin(ValidDummyPlugin):
        name = "other"

    _install_entry_point(monkeypatch, "dummy", MismatchedPlugin)

    with pytest.raises(
        RuntimeError,
        match="Backend plugin 'dummy' resolved to object with mismatched name 'other'",
    ):
        get_backend_plugin("dummy")


def test_external_backend_plugin_with_invalid_sql_namespace_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entry-point plugins should fail fast when their SQL namespace is incomplete."""

    class DummyPlugin:
        name = "dummy"
        sql = object()
        backend_api_version = BACKEND_API_VERSION
        schema_version = 1

        def init_backend(self, config: Any, **kwargs: Any) -> None:
            raise NotImplementedError

    _install_entry_point(monkeypatch, "dummy", DummyPlugin)

    with pytest.raises(RuntimeError, match="Backend SQL namespace is missing"):
        get_backend_plugin("dummy")


def test_plugin_without_sql_must_be_declared_as_a_direct_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing SQL hooks must not be accepted as an implicit direct backend."""

    class NeitherSqlNorDirectPlugin(ValidDummyPlugin):
        sql = None
        is_direct_backend = False

    _install_entry_point(monkeypatch, "dummy", NeitherSqlNorDirectPlugin)

    with pytest.raises(RuntimeError, match="has no SQL namespace"):
        get_backend_plugin("dummy")


def test_direct_backend_must_expose_a_callable_core_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Declaring direct mode is insufficient without the direct core interface."""

    class DirectPluginWithoutCoreFactory(ValidDummyPlugin):
        sql = None
        is_direct_backend = True

    _install_entry_point(monkeypatch, "dummy", DirectPluginWithoutCoreFactory)

    with pytest.raises(RuntimeError, match=r"must expose create_core\(\)"):
        get_backend_plugin("dummy")


def test_external_backend_plugin_missing_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingVersionPlugin:
        name = "dummy"
        sql = sqlite_sql
        schema_version = 1

    _install_entry_point(monkeypatch, "dummy", MissingVersionPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert "backend_api_version" in message


def test_external_backend_plugin_with_stale_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StaleVersionPlugin(ValidDummyPlugin):
        backend_api_version = BACKEND_API_VERSION - 1

    _install_entry_point(monkeypatch, "dummy", StaleVersionPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert f"backend API v{BACKEND_API_VERSION - 1}" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_external_backend_plugin_with_future_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    future_version = BACKEND_API_VERSION + 1

    class FutureVersionPlugin(ValidDummyPlugin):
        backend_api_version = future_version

    _install_entry_point(monkeypatch, "dummy", FutureVersionPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert f"backend API v{future_version}" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_first_party_backend_api_mismatch_mentions_package_and_core_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StalePostgresPlugin(ValidDummyPlugin):
        name = "postgres"
        backend_api_version = 0

    _install_entry_point(monkeypatch, "postgres", StalePostgresPlugin)

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("postgres")

    message = str(excinfo.value)
    assert "postgres" in message
    assert "simplebroker-pg" in message
    assert SIMPLEBROKER_VERSION in message
    assert "backend API v0" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_external_backend_plugin_non_integer_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StringVersionPlugin(ValidDummyPlugin):
        backend_api_version = "1"

    _install_entry_point(monkeypatch, "dummy", StringVersionPlugin)

    with pytest.raises(RuntimeError, match="integer backend_api_version"):
        get_backend_plugin("dummy")


def test_external_backend_plugin_bool_backend_api_version_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BoolVersionPlugin(ValidDummyPlugin):
        backend_api_version = True

    _install_entry_point(monkeypatch, "dummy", BoolVersionPlugin)

    with pytest.raises(RuntimeError, match="integer backend_api_version"):
        get_backend_plugin("dummy")


def test_entry_point_load_failure_gets_actionable_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_entry_point(
        monkeypatch,
        "dummy",
        error=ImportError("cannot import name 'BackendPlugin'"),
    )

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("dummy")

    message = str(excinfo.value)
    assert "dummy" in message
    assert "simplebroker" in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "cannot import name 'BackendPlugin'" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_first_party_entry_point_load_failure_mentions_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_entry_point(
        monkeypatch,
        "postgres",
        error=ImportError("cannot import name 'BackendPlugin'"),
    )

    with pytest.raises(RuntimeError) as excinfo:
        get_backend_plugin("postgres")

    message = str(excinfo.value)
    assert "postgres" in message
    assert "simplebroker-pg" in message
    assert SIMPLEBROKER_VERSION in message
    assert f"backend API v{BACKEND_API_VERSION}" in message
    assert "cannot import name 'BackendPlugin'" in message
    assert "upgrade" in message.lower()
    assert "pin simplebroker" in message


def test_non_aware_runner_with_resolved_target_uses_target_plugin(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-027] exception
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BrokerTarget context should identify wrapped runners that are not backend-aware."""

    class LegacyRunner:
        def __init__(self) -> None:
            self.completed_phases: set[SetupPhase] = set()

        def run(
            self,
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> list[tuple[Any, ...]]:
            del sql, params, fetch
            return []

        def begin_immediate(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def close(self) -> None:
            pass

        def setup(self, phase: SetupPhase) -> None:
            self.completed_phases.add(phase)

        def is_setup_complete(self, phase: SetupPhase) -> bool:
            return phase in self.completed_phases

    class DummyWaiter:
        def wait(self, timeout: float) -> bool:
            del timeout
            return False

        def close(self) -> None:
            pass

    class RecordingPlugin:
        sql = sqlite_sql
        backend_api_version = BACKEND_API_VERSION
        schema_version = 1

        def __init__(self, name: str) -> None:
            self.name = name
            self.waiter = DummyWaiter()

        def initialize_database(self, runner: Any, **kwargs: Any) -> None:
            del runner, kwargs

        def meta_table_exists(self, runner: Any) -> bool:
            del runner
            return False

        def read_schema_version(self, runner: Any) -> int:
            del runner
            return 1

        def write_schema_version(self, runner: Any, version: int) -> None:
            del runner, version

        def migrate_schema(self, runner: Any, **kwargs: Any) -> None:
            del runner, kwargs

        def read_last_ts(self, runner: Any) -> int:
            del runner
            return 0

        def create_activity_waiter(
            self,
            *,
            target: str | None,
            backend_options: dict[str, Any] | None = None,
            runner: Any = None,
            queue_name: str,
            stop_event: threading.Event,
        ) -> DummyWaiter:
            del target, backend_options, runner, queue_name, stop_event
            return self.waiter

    default_plugin = RecordingPlugin("sqlite")
    target_plugin = RecordingPlugin("dummy")

    def get_plugin(name: str = "sqlite") -> RecordingPlugin:
        if name == "dummy":
            return target_plugin
        if name == "sqlite":
            return default_plugin
        raise RuntimeError(f"unexpected plugin {name}")

    monkeypatch.setattr("simplebroker._backend_plugins.get_backend_plugin", get_plugin)

    runner = LegacyRunner()
    target = BrokerTarget("dummy", "dummy-target", {"schema": "test_schema"})

    connection = DBConnection(target, runner)
    try:
        assert connection._backend_plugin is target_plugin
    finally:
        connection.close()

    queue = Queue("jobs", db_path=target, runner=runner, persistent=True)
    try:
        assert queue.backend_name == "dummy"
        assert queue.create_activity_waiter(stop_event=threading.Event()) is (
            target_plugin.waiter
        )
    finally:
        queue.close()


def test_sqlite_plugin_has_init_backend() -> None:
    """The built-in sqlite plugin should expose init_backend()."""
    from simplebroker._constants import resolve_config

    plugin = get_backend_plugin("sqlite")
    result = plugin.init_backend(resolve_config(env=os.environ))

    assert "target" in result
    assert "backend_options" in result


@pytest.mark.parametrize(
    "relative_path",
    [
        "extensions/simplebroker_pg/simplebroker_pg/plugin.py",
        "extensions/simplebroker_redis/simplebroker_redis/plugin.py",
    ],
)
def test_first_party_extension_plugins_declare_literal_backend_api_version(
    relative_path: str,
) -> None:
    plugin_source = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")

    declarations = []
    for node in ast.walk(ast.parse(plugin_source)):
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        if any(
            isinstance(target, ast.Name) and target.id == "backend_api_version"
            for target in targets
        ):
            declarations.append(node.value)
    assert len(declarations) == 1
    value = declarations[0]
    # A literal remains meaningful in an independently installed extension;
    # aliasing the host core version would disguise incompatibility.
    assert isinstance(value, ast.Constant)
    assert type(value.value) is int
    assert value.value == BACKEND_API_VERSION
