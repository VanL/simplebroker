"""Constants for the Valkey/Redis SimpleBroker backend."""

REDIS_SCHEMA_VERSION = 1
DEFAULT_NAMESPACE = "simplebroker_redis_v1"
DEFAULT_TARGET = "redis://127.0.0.1:6379/0"
DEFAULT_STALE_BATCH_SECONDS = 300
