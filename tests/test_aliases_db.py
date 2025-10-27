from pathlib import Path

import pytest

from simplebroker.db import BrokerDB


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "aliases.db"


def test_alias_store_and_resolve(db_path: Path) -> None:
    with BrokerDB(str(db_path)) as db:
        assert db.resolve_alias("nonexistent") is None

        db.add_alias("task1", "shared")
        assert db.resolve_alias("task1") == "shared"

        db.add_alias("task2", "shared")
        assert db.resolve_alias("task2") == "shared"

        aliases = dict(db.list_aliases())
        assert aliases == {"task1": "shared", "task2": "shared"}

        db.remove_alias("task1")
        assert db.resolve_alias("task1") is None
        assert dict(db.list_aliases()) == {"task2": "shared"}


def test_alias_cache_miss_refresh(db_path: Path) -> None:
    path = str(db_path)
    with BrokerDB(path) as creator:
        creator.add_alias("alpha", "shared")

    with BrokerDB(path) as consumer:
        # Cache is empty initially; resolving should trigger refresh
        assert consumer.resolve_alias("alpha") == "shared"

        # Remove alias externally, ensure subsequent resolve sees removal
        with BrokerDB(path) as remover:
            remover.remove_alias("alpha")

        assert consumer.resolve_alias("alpha") is None


def test_aliases_for_target(db_path: Path) -> None:
    with BrokerDB(str(db_path)) as db:
        db.add_alias("alpha", "shared")
        db.add_alias("beta", "shared")
        assert db.aliases_for_target("shared") == ["alpha", "beta"]
        assert db.aliases_for_target("missing") == []


def test_get_meta_includes_alias_version(db_path: Path) -> None:
    with BrokerDB(str(db_path)) as db:
        meta_before = db.get_meta()
        assert "alias_version" in meta_before
        db.add_alias("alpha", "shared")
        meta_after = db.get_meta()
        assert meta_after["alias_version"] > meta_before["alias_version"]


def test_alias_reject_self_reference(db_path: Path) -> None:
    with BrokerDB(str(db_path)) as db:
        with pytest.raises(ValueError):
            db.add_alias("queue", "queue")


def test_alias_detect_cycle(db_path: Path) -> None:
    with BrokerDB(str(db_path)) as db:
        db.add_alias("a", "b")
        db.add_alias("b", "c")
        with pytest.raises(ValueError):
            db.add_alias("c", "a")


def test_alias_version_bumps(db_path: Path) -> None:
    with BrokerDB(str(db_path)) as db:
        version1 = db.get_alias_version()
        db.add_alias("x", "y")
        version2 = db.get_alias_version()
        assert version2 > version1
        db.remove_alias("x")
        version3 = db.get_alias_version()
        assert version3 > version2


def test_alias_persistent_cache_refresh(tmp_path: Path) -> None:
    db_path = tmp_path / "aliases.db"
    with BrokerDB(str(db_path)) as db1:
        db1.add_alias("alpha", "beta")

        with BrokerDB(str(db_path)) as db2:
            assert db2.resolve_alias("alpha") == "beta"

        db1.remove_alias("alpha")

        with BrokerDB(str(db_path)) as db2:
            assert db2.resolve_alias("alpha") is None


def test_alias_add_warns_on_existing_queue(db_path: Path) -> None:
    with BrokerDB(str(db_path)) as db:
        db.write("existing", "message")
        with pytest.warns(RuntimeWarning, match=r"Queue 'existing' already exists"):
            db.add_alias("existing", "redirect")
