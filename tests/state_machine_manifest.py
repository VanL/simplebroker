"""Registry for executable state-machine transition contracts."""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from enum import StrEnum


class ManifestCoverage(StrEnum):
    """Whether the registry claims complete repository coverage."""

    COMPLETE = "complete"


class ManifestComponent(StrEnum):
    """Installable component that owns a state-machine contract."""

    CORE = "core"
    POSTGRES = "postgres"
    REDIS = "redis"


@dataclass(frozen=True, slots=True)
class StateMachineEntry:
    """Import references that bind a machine to its executable table."""

    machine_id: str
    owner_module: str
    owner_name: str
    test_module: str
    table_name: str
    firing_test_name: str
    component: ManifestComponent = ManifestComponent.CORE


def entries_for_components(
    entries: Sequence[StateMachineEntry],
    components: Collection[ManifestComponent],
) -> tuple[StateMachineEntry, ...]:
    """Select contracts whose owning components exist in this environment."""

    return tuple(entry for entry in entries if entry.component in components)


INVENTORY_STATE_MACHINE_IDS = (
    "SM-SQLITE-SCHEMA",
    "SM-DUMP-LOAD",
    "SM-TIMESTAMP-GENERATOR",
    "SM-DARWIN-XATTR",
    "SM-PHASE-LOCK",
    "SM-CONNECTION",
    "SM-PROCESS-SESSION",
    "SM-SETUP-BUDGET",
    "SM-DELIVERY-POISON",
    "SM-POLLING",
    "SM-WATCHER-LIFECYCLE",
    "SM-CLI-WATCH",
    "SM-PG-LISTENER",
    "SM-PG-VACUUM",
    "SM-REDIS-BROADCAST",
    "SM-REDIS-ACTIVITY-LISTENER",
    "SM-SQLITE-RUNNER",
    "SM-REDIS-RUNNER",
    "SM-COVERAGE-SETTLEMENT",
    "SM-CLI-COVERAGE",
    "SM-RELEASE",
    "SM-ASYNC-STREAM",
    "SM-REACTOR",
    "SM-REACTOR-OUTPUT",
    "SM-PRIORITY-WATCHER",
    "SM-MONITORING-WATCHER",
    "SM-SUBPROCESS",
    "SM-CROSS-THREAD-PROBE",
    "SM-MULTIPROCESS-WATCHER",
    "SM-SIGINT-PROBE",
)

STATE_MACHINE_MANIFEST_COVERAGE = ManifestCoverage.COMPLETE
STATE_MACHINE_MANIFEST_COVERAGE_NOTE = (
    "Complete repository inventory with one registered executable transition "
    "contract for every named machine."
)

STATE_MACHINE_MANIFEST = (
    StateMachineEntry(
        machine_id="SM-SQLITE-SCHEMA",
        owner_module="simplebroker._backends.sqlite.schema",
        owner_name="initialize_database",
        test_module="tests.test_core_persistence_transition_tables",
        table_name="SQLITE_SCHEMA_TRANSITIONS",
        firing_test_name="test_sqlite_schema_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-DUMP-LOAD",
        owner_module="simplebroker._dump",
        owner_name="load_lines",
        test_module="tests.test_core_persistence_transition_tables",
        table_name="DUMP_LOAD_TRANSITIONS",
        firing_test_name="test_dump_load_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-TIMESTAMP-GENERATOR",
        owner_module="simplebroker._timestamp",
        owner_name="TimestampGenerator",
        test_module="tests.test_core_persistence_transition_tables",
        table_name="TIMESTAMP_GENERATOR_TRANSITIONS",
        firing_test_name="test_timestamp_generator_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-DARWIN-XATTR",
        owner_module="simplebroker._phaselock",
        owner_name="_darwin_xattr_provider",
        test_module="tests.test_phaselock_transition_tables",
        table_name="DARWIN_XATTR_TRANSITIONS",
        firing_test_name="test_darwin_xattr_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-PHASE-LOCK",
        owner_module="simplebroker._phaselock",
        owner_name="PhaseLockService",
        test_module="tests.test_phaselock_transition_tables",
        table_name="PHASE_LOCK_TRANSITIONS",
        firing_test_name="test_phase_lock_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-CONNECTION",
        owner_module="simplebroker.db",
        owner_name="DBConnection",
        test_module="tests.test_connection_transition_tables",
        table_name="CONNECTION_TRANSITIONS",
        firing_test_name="test_connection_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-PROCESS-SESSION",
        owner_module="simplebroker._broker_session",
        owner_name="_ProcessBrokerSession",
        test_module="tests.test_connection_transition_tables",
        table_name="PROCESS_SESSION_TRANSITIONS",
        firing_test_name="test_process_session_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-SETUP-BUDGET",
        owner_module="simplebroker.helpers",
        owner_name="SetupProgressBudget",
        test_module="tests.test_helpers_coverage",
        table_name="SETUP_PROGRESS_BUDGET_TRANSITIONS",
        firing_test_name="test_setup_progress_budget_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-DELIVERY-POISON",
        owner_module="simplebroker.db",
        owner_name="BrokerCore",
        test_module="tests.test_connection_transition_tables",
        table_name="DELIVERY_POISON_TRANSITIONS",
        firing_test_name="test_delivery_poison_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-POLLING",
        owner_module="simplebroker.watcher",
        owner_name="PollingStrategy",
        test_module="tests.test_watcher_transition_tables",
        table_name="POLLING_TRANSITIONS",
        firing_test_name="test_polling_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-WATCHER-LIFECYCLE",
        owner_module="simplebroker.watcher",
        owner_name="BaseWatcher",
        test_module="tests.test_watcher_transition_tables",
        table_name="WATCHER_LIFECYCLE_TRANSITIONS",
        firing_test_name="test_watcher_lifecycle_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-CLI-WATCH",
        owner_module="simplebroker.commands",
        owner_name="cmd_watch",
        test_module="tests.test_watcher_transition_tables",
        table_name="CLI_WATCH_TRANSITIONS",
        firing_test_name="test_cli_watch_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-SQLITE-RUNNER",
        owner_module="simplebroker._runner",
        owner_name="SQLiteRunner",
        test_module="tests.test_core_persistence_transition_tables",
        table_name="SQLITE_RUNNER_TRANSITIONS",
        firing_test_name="test_sqlite_runner_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-PG-LISTENER",
        owner_module="simplebroker_pg.runner",
        owner_name="_SharedActivityListener",
        test_module="extensions.simplebroker_pg.tests.test_pg_state_machine_transitions",
        table_name="PG_LISTENER_TRANSITIONS",
        firing_test_name="test_pg_listener_fires_transition_table",
        component=ManifestComponent.POSTGRES,
    ),
    StateMachineEntry(
        machine_id="SM-PG-VACUUM",
        owner_module="simplebroker_pg.plugin",
        owner_name="PostgresBackendPlugin",
        test_module="extensions.simplebroker_pg.tests.test_pg_state_machine_transitions",
        table_name="PG_VACUUM_TRANSITIONS",
        firing_test_name="test_pg_vacuum_fires_transition_table",
        component=ManifestComponent.POSTGRES,
    ),
    StateMachineEntry(
        machine_id="SM-REDIS-BROADCAST",
        owner_module="simplebroker_redis.core",
        owner_name="RedisBrokerCore",
        test_module=(
            "extensions.simplebroker_redis.tests.test_redis_state_machine_transitions"
        ),
        table_name="REDIS_BROADCAST_TRANSITIONS",
        firing_test_name="test_redis_broadcast_fires_transition_table",
        component=ManifestComponent.REDIS,
    ),
    StateMachineEntry(
        machine_id="SM-REDIS-ACTIVITY-LISTENER",
        owner_module="simplebroker_redis.plugin",
        owner_name="_SharedRedisActivityListener",
        test_module=(
            "extensions.simplebroker_redis.tests.test_redis_state_machine_transitions"
        ),
        table_name="REDIS_ACTIVITY_LISTENER_TRANSITIONS",
        firing_test_name="test_redis_activity_listener_fires_transition_table",
        component=ManifestComponent.REDIS,
    ),
    StateMachineEntry(
        machine_id="SM-REDIS-RUNNER",
        owner_module="simplebroker_redis.runner",
        owner_name="RedisRunner",
        test_module=(
            "extensions.simplebroker_redis.tests.test_redis_state_machine_transitions"
        ),
        table_name="REDIS_RUNNER_TRANSITIONS",
        firing_test_name="test_redis_runner_fires_transition_table",
        component=ManifestComponent.REDIS,
    ),
    StateMachineEntry(
        machine_id="SM-COVERAGE-SETTLEMENT",
        owner_module="bin.coverage_combine",
        owner_name="_wait_for_stable_sources",
        test_module="tests.test_dev_scripts",
        table_name="COVERAGE_SETTLEMENT_TRANSITIONS",
        firing_test_name="test_coverage_settlement_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-CLI-COVERAGE",
        owner_module="tests.conftest",
        owner_name="run_cli",
        test_module="tests.test_dev_scripts",
        table_name="CLI_COVERAGE_TRANSITIONS",
        firing_test_name="test_cli_coverage_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-RELEASE",
        owner_module="bin.release",
        owner_name="main",
        test_module="tests.test_release_script",
        table_name="RELEASE_TRANSITIONS",
        firing_test_name="test_release_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-ASYNC-STREAM",
        owner_module="examples.async_pooled_broker",
        owner_name="AsyncBrokerCore",
        test_module="tests.test_example_async_stream_transitions",
        table_name="ASYNC_STREAM_TRANSITIONS",
        firing_test_name="test_async_stream_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-REACTOR",
        owner_module="examples.reference_reactor",
        owner_name="BaseReactor",
        test_module="examples.tests.test_reference_reactor_transitions",
        table_name="REACTOR_TRANSITIONS",
        firing_test_name="test_reference_reactor_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-REACTOR-OUTPUT",
        owner_module="examples.reference_reactor",
        owner_name="Reactor",
        test_module="examples.tests.test_reference_reactor_transitions",
        table_name="REACTOR_OUTPUT_TRANSITIONS",
        firing_test_name="test_reference_reactor_output_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-PRIORITY-WATCHER",
        owner_module="examples.multi_queue_patterns",
        owner_name="pattern_2_priority_simulation",
        test_module="examples.tests.test_multi_queue_pattern_transitions",
        table_name="PRIORITY_WATCHER_TRANSITIONS",
        firing_test_name="test_priority_watcher_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-MONITORING-WATCHER",
        owner_module="examples.multi_queue_patterns",
        owner_name="pattern_5_monitoring",
        test_module="examples.tests.test_multi_queue_pattern_transitions",
        table_name="MONITORING_WATCHER_TRANSITIONS",
        firing_test_name="test_monitoring_watcher_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-SUBPROCESS",
        owner_module="tests.helper_scripts.managed_subprocess",
        owner_name="managed_subprocess",
        test_module="tests.test_managed_subprocess_transitions",
        table_name="SUBPROCESS_TRANSITIONS",
        firing_test_name="test_managed_subprocess_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-CROSS-THREAD-PROBE",
        owner_module="tests.helper_scripts.cross_thread_generator_probe",
        owner_name="run_cross_thread_generator_probe",
        test_module="tests.test_cross_thread_probe_transitions",
        table_name="CROSS_THREAD_PROBE_TRANSITIONS",
        firing_test_name="test_cross_thread_probe_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-MULTIPROCESS-WATCHER",
        owner_module="tests.test_watcher_multiprocess",
        owner_name="watcher_process",
        test_module="tests.test_watcher_multiprocess_transitions",
        table_name="MULTIPROCESS_WATCHER_TRANSITIONS",
        firing_test_name="test_multiprocess_watcher_fires_transition_table",
    ),
    StateMachineEntry(
        machine_id="SM-SIGINT-PROBE",
        owner_module="tests.helper_scripts.watcher_sigint_script_improved",
        owner_name="main",
        test_module="tests.test_watcher_sigint_probe_transitions",
        table_name="SIGINT_PROBE_TRANSITIONS",
        firing_test_name="test_watcher_sigint_probe_fires_transition_table",
    ),
)
