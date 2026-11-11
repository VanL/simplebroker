import pytest

from simplebroker._exceptions import QueueNameError
from simplebroker._targets import BrokerTarget

from .helper_scripts.broker_factory import active_backend, make_broker

pytestmark = [pytest.mark.shared]


def _insert_legacy_invalid_alias(broker, alias: str, target: str) -> None:
    if active_backend() == "redis":
        broker._client.hset(broker._key("aliases"), alias, target)
        return
    broker._runner.run(broker._sql.INSERT_ALIAS, (alias, target))
    broker._runner.commit()


def test_alias_store_and_resolve(broker) -> None:
    assert broker.resolve_alias("nonexistent") is None

    broker.add_alias("task1", "shared")
    assert broker.resolve_alias("task1") == "shared"

    broker.add_alias("task2", "shared")
    assert broker.resolve_alias("task2") == "shared"

    aliases = dict(broker.list_aliases())
    assert aliases == {"task1": "shared", "task2": "shared"}

    broker.remove_alias("task1")
    assert broker.resolve_alias("task1") is None
    assert dict(broker.list_aliases()) == {"task2": "shared"}


def test_alias_cache_miss_refresh(broker_target: BrokerTarget) -> None:
    creator = make_broker(broker_target)
    try:
        creator.add_alias("alpha", "shared")
    finally:
        creator.shutdown()

    consumer = make_broker(broker_target)
    try:
        assert consumer.resolve_alias("alpha") == "shared"

        remover = make_broker(broker_target)
        try:
            remover.remove_alias("alpha")
        finally:
            remover.shutdown()

        assert consumer.resolve_alias("alpha") is None
    finally:
        consumer.shutdown()


def test_aliases_for_target(broker) -> None:
    broker.add_alias("alpha", "shared")
    broker.add_alias("beta", "shared")
    assert broker.aliases_for_target("shared") == ["alpha", "beta"]
    assert broker.aliases_for_target("missing") == []


def test_get_meta_includes_alias_version(broker) -> None:
    meta_before = broker.get_meta()
    assert "alias_version" in meta_before
    broker.add_alias("alpha", "shared")
    meta_after = broker.get_meta()
    assert meta_after["alias_version"] > meta_before["alias_version"]


def test_alias_reject_self_reference(broker) -> None:
    with pytest.raises(ValueError):
        broker.add_alias("queue", "queue")


@pytest.mark.parametrize(
    ("alias", "target"),
    (("bad alias", "target"), ("alias", "bad target")),
)
def test_alias_and_target_use_queue_name_grammar(broker, alias, target) -> None:
    with pytest.raises(QueueNameError):
        broker.add_alias(alias, target)
    assert broker.list_aliases() == []


def test_alias_rejects_chain_in_creation_order_without_mutation(broker) -> None:
    broker.add_alias("a", "b")
    version = broker.get_alias_version()
    with pytest.raises(ValueError):
        broker.add_alias("b", "c")
    assert dict(broker.list_aliases()) == {"a": "b"}
    assert broker.get_alias_version() == version


def test_alias_version_bumps(broker) -> None:
    version1 = broker.get_alias_version()
    broker.add_alias("x", "y")
    version2 = broker.get_alias_version()
    assert version2 > version1
    broker.remove_alias("x")
    version3 = broker.get_alias_version()
    assert version3 > version2


def test_alias_persistent_cache_refresh(broker_target: BrokerTarget) -> None:
    db1 = make_broker(broker_target)
    try:
        db1.add_alias("alpha", "beta")

        db2 = make_broker(broker_target)
        try:
            assert db2.resolve_alias("alpha") == "beta"
        finally:
            db2.shutdown()

        db1.remove_alias("alpha")

        db3 = make_broker(broker_target)
        try:
            assert db3.resolve_alias("alpha") is None
        finally:
            db3.shutdown()
    finally:
        db1.shutdown()


def test_alias_add_revalidates_against_live_state(
    broker_target: BrokerTarget,
) -> None:
    db1 = make_broker(broker_target)
    db2 = make_broker(broker_target)
    try:
        assert db1.resolve_alias("a") is None

        db2.add_alias("b", "a")

        with pytest.raises(ValueError):
            db1.add_alias("a", "b")

        assert dict(db1.list_aliases()) == {"b": "a"}
    finally:
        db1.shutdown()
        db2.shutdown()


def test_legacy_invalid_alias_remains_visible_resolvable_and_removable(broker) -> None:
    _insert_legacy_invalid_alias(broker, "legacy", "bad target")

    assert ("legacy", "bad target") in broker.list_aliases()
    assert broker.resolve_alias("legacy") == "bad target"
    broker.remove_alias("legacy")
    assert broker.resolve_alias("legacy") is None


def test_legacy_alias_chain_remains_one_hop_visible_and_removable(broker) -> None:
    _insert_legacy_invalid_alias(broker, "legacy-a", "legacy-b")
    _insert_legacy_invalid_alias(broker, "legacy-b", "canonical")

    assert dict(broker.list_aliases()) == {
        "legacy-a": "legacy-b",
        "legacy-b": "canonical",
    }
    assert broker.resolve_alias("legacy-a") == "legacy-b"
    broker.remove_alias("legacy-a")
    broker.remove_alias("legacy-b")
    assert broker.list_aliases() == []


def test_canonicalize_queue_resolves_only_behind_the_sigil(broker) -> None:
    """A plain name is always the literal queue; only "@name" resolves.

    Guards the collision case: a queue and an alias may share a name, and
    without the sigil rule `canonicalize_queue("ali")` would silently redirect
    writes intended for the literal queue `ali`.
    """
    broker.add_alias("ali", "real")

    assert broker.canonicalize_queue("ali") == "ali"
    assert broker.canonicalize_queue("@ali") == "real"
    assert broker.canonicalize_queue("plain") == "plain"

    with pytest.raises(ValueError, match="not defined"):
        broker.canonicalize_queue("@nope")

    with pytest.raises(ValueError, match="cannot be empty"):
        broker.canonicalize_queue("@")


def test_alias_add_warns_on_existing_queue(broker) -> None:
    broker.write("existing", "message")
    with pytest.warns(RuntimeWarning, match=r"Queue 'existing' already exists"):
        broker.add_alias("existing", "redirect")


def test_sql_alias_seqlock_retries_interleaved_update_without_caching_stale_map(
    tmp_path,
) -> None:
    from simplebroker.db import BrokerDB

    db_file = str(tmp_path / "seqlock.db")
    reader = BrokerDB(db_file)
    writer = BrokerDB(db_file)
    try:
        writer.add_alias("alpha", "v1_target")
        assert reader.resolve_alias("alpha") == "v1_target"

        # Now writer adds a second alias so version advances
        writer.add_alias("beta", "v2_target")

        # Simulate writer committing a third alias right between SELECT_ALIASES
        # and the trailing read_alias_version check inside reader._load_aliases_locked().
        real_run = reader._runner.run
        interleaved_once = False

        def interleaving_run(sql: str, params=(), *, fetch: bool = False):
            nonlocal interleaved_once
            result = (
                list(real_run(sql, params, fetch=fetch))
                if fetch
                else real_run(sql, params, fetch=fetch)
            )
            if fetch and sql == reader._sql.SELECT_ALIASES and not interleaved_once:
                interleaved_once = True
                writer.add_alias("gamma", "v3_target")
            return result

        reader._runner.run = interleaving_run  # type: ignore[method-assign]
        try:
            assert reader.resolve_alias("gamma") == "v3_target"
        finally:
            reader._runner.run = real_run  # type: ignore[method-assign]

        assert reader.resolve_alias("gamma") == "v3_target"
        assert reader.get_alias_version() == writer.get_alias_version()
    finally:
        reader.shutdown()
        writer.shutdown()


def test_sql_alias_monotonic_version_commit_rollback_and_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import simplebroker.db as db_mod
    from simplebroker._exceptions import OperationalError
    from simplebroker.db import BrokerDB

    db_file = str(tmp_path / "alias-monotonic.db")
    db1 = BrokerDB(db_file)
    db2 = BrokerDB(db_file)
    try:
        # 1. Frozen time.time_ns() still advances alias_version monotonically (+1)
        monkeypatch.setattr(db_mod.time, "time_ns", lambda: 1_700_000_000_000_000_000)
        v0 = db1.get_alias_version()
        db1.add_alias("a1", "target1")
        v1 = db1.get_alias_version()
        assert v1 == v0 + 1
        assert db2.resolve_alias("a1") == "target1"

        db1.remove_alias("a1")
        v2 = db1.get_alias_version()
        assert v2 == v1 + 1
        assert db2.resolve_alias("a1") is None

        # 2. Simulated commit failure invalidates _alias_cache_version so uncommitted alias is never cached
        real_commit = db1._runner.commit

        def failing_commit() -> None:
            raise RuntimeError("simulated commit failure")

        monkeypatch.setattr(db1._runner, "commit", failing_commit)
        with pytest.raises(RuntimeError, match="simulated commit failure"):
            db1.add_alias("ghost", "target1")
        monkeypatch.setattr(db1._runner, "commit", real_commit)
        assert db1._alias_cache_version == -1
        assert db1.resolve_alias("ghost") is None

        # 3. Transient retryable OperationalError on add_alias / remove_alias retries via _run_with_retry
        real_begin = db1._runner.begin_immediate
        attempts = 0

        def flaky_begin() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                err = OperationalError("database is locked")
                err.retryable = True
                raise err
            real_begin()

        monkeypatch.setattr(db1._runner, "begin_immediate", flaky_begin)
        db1.add_alias("retried", "target1")
        assert attempts == 2
        assert db1.resolve_alias("retried") == "target1"

        attempts = 0
        db1.remove_alias("retried")
        assert attempts == 2
        assert db1.resolve_alias("retried") is None
        monkeypatch.setattr(db1._runner, "begin_immediate", real_begin)
    finally:
        db1.shutdown()
        db2.shutdown()


def test_get_alias_version_never_exposes_negative_sentinel_after_three_torn_passes(
    tmp_path,
) -> None:
    from simplebroker.db import BrokerDB

    db_file = str(tmp_path / "alias-torn-seqlock.db")
    writer = BrokerDB(db_file)
    reader = BrokerDB(db_file)
    try:
        writer.add_alias("alpha", "target_a")
        real_read_version = reader._current_alias_version_locked
        version_reads = 0

        def torn_read_version() -> int:
            nonlocal version_reads
            version_reads += 1
            return version_reads

        reader._current_alias_version_locked = torn_read_version  # type: ignore[method-assign]
        try:
            version = reader.get_alias_version()
            assert version >= 1
            assert reader._alias_cache_version == -1
        finally:
            reader._current_alias_version_locked = real_read_version  # type: ignore[method-assign]
    finally:
        reader.shutdown()
        writer.shutdown()
