import pytest

from simplebroker._targets import ResolvedTarget

from .helper_scripts.broker_factory import make_broker

pytestmark = [pytest.mark.shared]


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


def test_alias_cache_miss_refresh(broker_target: ResolvedTarget) -> None:
    creator = make_broker(broker_target)
    try:
        creator.add_alias("alpha", "shared")
    finally:
        creator.close()

    consumer = make_broker(broker_target)
    try:
        assert consumer.resolve_alias("alpha") == "shared"

        remover = make_broker(broker_target)
        try:
            remover.remove_alias("alpha")
        finally:
            remover.close()

        assert consumer.resolve_alias("alpha") is None
    finally:
        consumer.close()


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


def test_alias_detect_cycle(broker) -> None:
    broker.add_alias("a", "b")
    broker.add_alias("b", "c")
    with pytest.raises(ValueError):
        broker.add_alias("c", "a")


def test_alias_version_bumps(broker) -> None:
    version1 = broker.get_alias_version()
    broker.add_alias("x", "y")
    version2 = broker.get_alias_version()
    assert version2 > version1
    broker.remove_alias("x")
    version3 = broker.get_alias_version()
    assert version3 > version2


def test_alias_persistent_cache_refresh(broker_target: ResolvedTarget) -> None:
    db1 = make_broker(broker_target)
    try:
        db1.add_alias("alpha", "beta")

        db2 = make_broker(broker_target)
        try:
            assert db2.resolve_alias("alpha") == "beta"
        finally:
            db2.close()

        db1.remove_alias("alpha")

        db3 = make_broker(broker_target)
        try:
            assert db3.resolve_alias("alpha") is None
        finally:
            db3.close()
    finally:
        db1.close()


def test_alias_add_warns_on_existing_queue(broker) -> None:
    broker.write("existing", "message")
    with pytest.warns(RuntimeWarning, match=r"Queue 'existing' already exists"):
        broker.add_alias("existing", "redirect")
