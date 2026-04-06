from __future__ import annotations

from simplebroker._scripts import _route_pytest_args


def test_route_pytest_args_limits_run_to_shared_suite() -> None:
    (
        shared_args,
        extension_args,
        run_shared,
        run_extension,
        marker_expr,
        numprocesses,
        dist,
    ) = _route_pytest_args(
        [
            "-q",
            "-k",
            "test_metrics_collection_basic",
            "tests/test_watcher_metrics.py::test_metrics_collection_basic",
        ]
    )

    assert run_shared is True
    assert run_extension is False
    assert shared_args == [
        "-q",
        "-k",
        "test_metrics_collection_basic",
        "tests/test_watcher_metrics.py::test_metrics_collection_basic",
    ]
    assert extension_args == ["-q", "-k", "test_metrics_collection_basic"]
    assert marker_expr is None
    assert numprocesses is None
    assert dist is None


def test_route_pytest_args_limits_run_to_extension_suite() -> None:
    (
        shared_args,
        extension_args,
        run_shared,
        run_extension,
        marker_expr,
        numprocesses,
        dist,
    ) = _route_pytest_args(
        [
            "extensions/simplebroker_pg/tests/test_smoke.py::test_pg_backend",
            "-m",
            "smoke",
            "-n",
            "1",
            "--dist=loadscope",
            "-q",
        ]
    )

    assert run_shared is False
    assert run_extension is True
    assert shared_args == ["-q"]
    assert extension_args == [
        "extensions/simplebroker_pg/tests/test_smoke.py::test_pg_backend",
        "-q",
    ]
    assert marker_expr == "smoke"
    assert numprocesses == "1"
    assert dist == "loadscope"


def test_route_pytest_args_combines_multiple_marker_filters() -> None:
    _, _, _, _, marker_expr, _, _ = _route_pytest_args(
        ["-m", "smoke", "-m", "not slow"]
    )

    assert marker_expr == "(smoke) and (not slow)"
