"""Constants for the Valkey/Redis SimpleBroker backend."""

REDIS_SCHEMA_VERSION = 1
DEFAULT_NAMESPACE = "simplebroker_redis_v1"
DEFAULT_TARGET = "redis://127.0.0.1:6379/0"
DEFAULT_STALE_BATCH_SECONDS = 300

# Longest wait in RedisRunner.close() for checked-out connections to come back.
# The pool sets no socket timeout, so a stuck command must not hang close (or
# interpreter exit); a connection still out afterwards is disconnected by its
# owner when released. Plan: 2026-11-10-backend-thread-ownership-plan.md at 4cea0cb.
RUNNER_CLOSE_IN_FLIGHT_WAIT_SECONDS = 5.0
