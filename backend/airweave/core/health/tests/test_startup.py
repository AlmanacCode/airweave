"""Critical Temporal startup failures must be recoverable through process restart."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI

from airweave import main
from airweave.core import container, metrics_service
from airweave.db import session


@pytest.mark.asyncio
@pytest.mark.parametrize("critical", [True, False])
async def test_schedule_startup_failure_respects_critical_dependency(monkeypatch, critical):
    """A required dependency cannot leave an API alive but permanently skipped."""
    services = MagicMock()
    services.temporal_schedule_service.ensure_system_schedules = AsyncMock(
        side_effect=[ConnectionError("Temporal unavailable"), None]
    )
    monkeypatch.setattr(container, "container", services)
    monkeypatch.setattr(container, "initialize_container", MagicMock())
    monkeypatch.setattr(main, "validate_embedding_config", AsyncMock())
    monkeypatch.setattr(main, "AsyncSessionLocal", MagicMock(return_value=AsyncMock()))
    monkeypatch.setattr(
        main.settings,
        "HEALTH_CRITICAL_PROBES",
        "postgres,temporal" if critical else "postgres",
    )
    monkeypatch.setattr(session, "health_check_engine", AsyncMock())

    @asynccontextmanager
    async def metrics(*args):
        yield

    monkeypatch.setattr(metrics_service, "metrics_lifespan", metrics)

    if critical:
        with pytest.raises(ConnectionError, match="Temporal unavailable"):
            async with main.lifespan(FastAPI()):
                pytest.fail("Critical startup failure must not enter serving lifespan")
    else:
        async with main.lifespan(FastAPI()):
            pass

    # A subsequent startup can succeed; no sticky health flag hides recovery.
    async with main.lifespan(FastAPI()):
        pass
    assert services.temporal_schedule_service.ensure_system_schedules.await_count == 2
