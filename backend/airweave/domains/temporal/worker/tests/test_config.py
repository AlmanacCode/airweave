"""Tests for WorkerConfig defaults and from_settings() wiring."""

from datetime import timedelta
from unittest.mock import patch

from airweave.domains.temporal.worker.config import WorkerConfig


def test_sdk_metrics_port_default():
    """sdk_metrics_port defaults to 9090 when omitted."""
    config = WorkerConfig(
        task_queue="q",
        metrics_port=8080,
        graceful_shutdown_timeout_seconds=30,
    )
    assert config.sdk_metrics_port == 9090
    assert config.bind_host == "0.0.0.0"


def test_sdk_metrics_port_override():
    """sdk_metrics_port can be set explicitly."""
    config = WorkerConfig(
        task_queue="q",
        metrics_port=8080,
        graceful_shutdown_timeout_seconds=30,
        sdk_metrics_port=9999,
    )
    assert config.sdk_metrics_port == 9999


def test_from_settings_wires_sdk_metrics_port():
    """from_settings() reads TEMPORAL_SDK_METRICS_PORT."""
    with patch("airweave.domains.temporal.worker.config.settings") as mock:
        mock.TEMPORAL_MAX_CONCURRENT_ACTIVITIES = 4
        mock.TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASKS = 8
        mock.TEMPORAL_TASK_QUEUE = "q"
        mock.WORKER_METRICS_PORT = 8080
        mock.WORKER_BIND_HOST = "0.0.0.0"
        mock.TEMPORAL_GRACEFUL_SHUTDOWN_TIMEOUT = 30
        mock.TEMPORAL_DISABLE_SANDBOX = False
        mock.TEMPORAL_SDK_METRICS_PORT = 7777

        config = WorkerConfig.from_settings()

    assert config.sdk_metrics_port == 7777


def test_from_settings_wires_all_fields():
    """from_settings() maps every settings field correctly."""
    with patch("airweave.domains.temporal.worker.config.settings") as mock:
        mock.TEMPORAL_MAX_CONCURRENT_ACTIVITIES = 4
        mock.TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASKS = 8
        mock.TEMPORAL_TASK_QUEUE = "my-queue"
        mock.WORKER_METRICS_PORT = 9091
        mock.WORKER_BIND_HOST = "127.0.0.1"
        mock.TEMPORAL_GRACEFUL_SHUTDOWN_TIMEOUT = 60
        mock.TEMPORAL_DISABLE_SANDBOX = True
        mock.TEMPORAL_SDK_METRICS_PORT = 9999

        config = WorkerConfig.from_settings()

    assert config.task_queue == "my-queue"
    assert config.metrics_port == 9091
    assert config.bind_host == "127.0.0.1"
    assert config.graceful_shutdown_timeout_seconds == 60
    assert config.disable_sandbox is True
    assert config.sdk_metrics_port == 9999

    # Defaults for fields not covered by from_settings()
    assert config.max_concurrent_workflow_polls == 8
    assert config.max_concurrent_activity_polls == 16
    assert config.sticky_queue_schedule_to_start_timeout == timedelta(seconds=0.5)
    assert config.nonsticky_to_sticky_poll_ratio == 0.5
    assert config.default_heartbeat_throttle_interval == timedelta(seconds=2)
    assert config.max_heartbeat_throttle_interval == timedelta(seconds=2)


def test_capacity_is_explicit_positive_and_independent_of_pollers():
    """Slot capacity is not inferred from long-poll counts or record workers."""
    import pytest
    from pydantic import ValidationError

    config = WorkerConfig(task_queue="q", metrics_port=8080, graceful_shutdown_timeout_seconds=30)
    assert config.max_concurrent_activities == 4
    assert config.max_concurrent_workflow_tasks == 8
    with pytest.raises(ValidationError):
        WorkerConfig(
            task_queue="q",
            metrics_port=8080,
            graceful_shutdown_timeout_seconds=30,
            max_concurrent_activities=0,
        )
    with pytest.raises(ValidationError):
        WorkerConfig(
            task_queue="q",
            metrics_port=8080,
            graceful_shutdown_timeout_seconds=30,
            max_concurrent_workflow_tasks=1,
        )


def test_sql_budget_is_not_derived_from_record_workers():
    """Finite pool settings permit tuning without changing record concurrency."""
    import pytest
    from pydantic import ValidationError

    from airweave.core.config import Settings, settings

    values = settings.model_dump()
    values.update(
        ENCRYPTION_KEY="synthetic-capacity-key",
        STATE_SECRET="synthetic-state-key-for-capacity-test-32",
        TENANT_DATABASE_URI=None,
        SYNC_MAX_WORKERS=2,
    )
    configured = Settings(**values)
    assert configured.db_pool_size == 8
    assert configured.db_pool_max_overflow == 0
    values.update(DB_POOL_SIZE=5, DB_POOL_MAX_OVERFLOW=2, SYNC_MAX_WORKERS=100)
    configured = Settings(**values)
    assert configured.db_pool_size + configured.db_pool_max_overflow == 7
    values.update(DB_POOL_MAX_OVERFLOW=-1)
    with pytest.raises(ValidationError):
        Settings(**values)
    values.update(
        DB_POOL_MAX_OVERFLOW=0,
        DB_POOL_SIZE=1,
        TENANT_DATABASE_URI="postgresql+asyncpg://tenant@localhost/fixture",
    )
    with pytest.raises(ValidationError, match="DB_POOL_SIZE"):
        Settings(**values)


async def test_real_tenant_control_health_pools_share_the_configured_ceiling(monkeypatch):
    """Construct real SQLAlchemy pools without opening a database connection."""
    import runpy

    from airweave.core.config import settings

    monkeypatch.setattr(
        settings, "TENANT_DATABASE_URI", "postgresql+asyncpg://tenant@localhost/fixture"
    )
    monkeypatch.setattr(settings, "DB_POOL_SIZE", 8)
    monkeypatch.setattr(settings, "DB_POOL_MAX_OVERFLOW", 0)
    module = runpy.run_module("airweave.db.session", run_name="capacity_fixture")
    engines = [module[name] for name in ("async_engine", "tenant_engine", "health_check_engine")]
    try:
        assert [engine.pool.size() for engine in engines] == [1, 7, 1]
        assert [engine.pool._max_overflow for engine in engines] == [0, 0, 0]
    finally:
        for engine in engines:
            await engine.dispose()
