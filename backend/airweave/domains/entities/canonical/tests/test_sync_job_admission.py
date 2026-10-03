"""Actual PostgreSQL admission, replay and generation fences in an isolated schema."""

import asyncio
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select, update

from airweave.core.context import BaseContext
from airweave.crud.crud_sync_job import SyncJobBusy, sync_job
from airweave.models import Organization, Sync, SyncJob
from airweave.schemas.organization import Organization as OrganizationSchema
from airweave.schemas.sync_job import SyncJobCreate

pytestmark = pytest.mark.integration


async def test_locked_admission_lost_ack_race_and_current_authority(database, source):
    _, fence = source
    async with database() as db:
        await db.execute(update(SyncJob).values(status="completed"))
        await db.execute(
            update(Sync).values(
                status="active", provisioning_generation=1, provisioning_ready_generation=1
            )
        )
        org = await db.get(Organization, fence.organization_id)
        ctx = BaseContext(OrganizationSchema.model_validate(org))
        await db.commit()

    operation = uuid4()

    async def admit(job_id):
        async with database() as db:
            return await sync_job.create(
                db, obj_in=SyncJobCreate(id=job_id, sync_id=fence.sync_id), ctx=ctx
            )

    first = await admit(operation)  # Commit survives a lost reply.
    replay = await admit(operation)
    assert first.id == replay.id == operation
    assert replay.provisioning_generation == 1
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(SyncJob)) == 2
        await db.execute(update(SyncJob).where(SyncJob.id == operation).values(status="completed"))
        await db.commit()
    terminal = await admit(operation)
    assert terminal.status == "completed"  # Never resurrect an exact operation.

    results = await asyncio.gather(admit(uuid4()), admit(uuid4()), return_exceptions=True)
    assert sum(isinstance(result, SyncJobBusy) for result in results) == 1
    assert sum(isinstance(result, SyncJob) for result in results) == 1

    async with database() as db:
        await db.execute(
            update(Sync).values(provisioning_generation=2, provisioning_ready_generation=2)
        )
        await db.commit()
    with pytest.raises(HTTPException) as error:
        await admit(operation)
    assert error.value.status_code == 409
    async with database() as db:
        await db.execute(update(Sync).values(status="paused"))
        await db.commit()
    with pytest.raises(HTTPException) as error:
        await admit(operation)
    assert error.value.status_code == 409

    foreign = BaseContext(ctx.organization.model_copy(update={"id": uuid4()}))
    async with database() as db:
        with pytest.raises(HTTPException) as error:
            await sync_job.create(
                db, obj_in=SyncJobCreate(id=operation, sync_id=fence.sync_id), ctx=foreign
            )
        assert error.value.status_code == 404

    async with database() as db:
        other_sync = Sync(id=uuid4(), organization_id=ctx.organization.id, name="Other owned sync")
        db.add(other_sync)
        await db.commit()
        with pytest.raises(HTTPException) as error:
            await sync_job.create(
                db, obj_in=SyncJobCreate(id=operation, sync_id=other_sync.id), ctx=ctx
            )
        assert error.value.status_code == 409


async def test_existing_cleanup_releases_stale_scheduled_pending_but_keeps_recent_running(
    database, source, worker_discovery
):
    """Use existing discovery and state machine; external workflow cancellation is absent."""
    from contextlib import asynccontextmanager
    from datetime import timedelta
    from unittest.mock import AsyncMock, patch
    from uuid import NAMESPACE_URL, uuid5

    from airweave.adapters.event_bus.fake import FakeEventBus
    from airweave.core.datetime_utils import utc_now_naive
    from airweave.domains.organizations.repository import OrganizationRepository
    from airweave.domains.syncs.jobs.repository import SyncJobRepository
    from airweave.domains.syncs.jobs.state_machine import SyncJobStateMachine
    from airweave.domains.temporal.activities.cleanup_stuck_sync_jobs import (
        CleanupStuckSyncJobsActivity,
    )

    _, fence = source
    now = utc_now_naive()
    operation = uuid5(
        NAMESPACE_URL, f"sync-job:{fence.organization_id}:{fence.sync_id}:test:run:admission"
    )
    async with database() as db:
        await db.execute(update(SyncJob).values(status="completed"))
        org = await db.get(Organization, fence.organization_id)
        ctx = BaseContext(OrganizationSchema.model_validate(org))
        await db.commit()
        await sync_job.create(
            db, obj_in=SyncJobCreate(id=operation, sync_id=fence.sync_id), ctx=ctx
        )
        await db.execute(
            update(SyncJob)
            .where(SyncJob.id == operation)
            .values(modified_at=now - timedelta(minutes=4))
        )
        running_sync = Sync(id=uuid4(), organization_id=ctx.organization.id, name="Recent capture")
        db.add(running_sync)
        await db.flush()
        running_id = uuid4()
        db.add(
            SyncJob(
                id=running_id,
                sync_id=running_sync.id,
                organization_id=ctx.organization.id,
                status="running",
                started_at=now - timedelta(seconds=30),
                modified_at=now - timedelta(minutes=4),
            )
        )
        await db.commit()

    @asynccontextmanager
    async def scoped(_organization=None):
        async with database() as db:
            yield db

    temporal = AsyncMock()
    temporal.cancel_sync_job_workflow.return_value = {"success": False, "workflow_found": False}
    cleanup = CleanupStuckSyncJobsActivity(
        temporal,
        SyncJobStateMachine(SyncJobRepository(), FakeEventBus()),
        AsyncMock(),
        OrganizationRepository(),
    )
    module = "airweave.domains.temporal.activities.cleanup_stuck_sync_jobs"
    with (
        patch(f"{module}.get_db_context", scoped),
        patch(f"{module}.get_tenant_db_context", scoped),
        patch("airweave.domains.syncs.jobs.state_machine.get_tenant_db_context", scoped),
    ):
        await cleanup.run()
    async with database() as db:
        assert (await db.get(SyncJob, operation)).status == "cancelled"
        assert (await db.get(SyncJob, running_id)).status == "running"
        recovered = await sync_job.create(
            db, obj_in=SyncJobCreate(id=uuid4(), sync_id=fence.sync_id), ctx=ctx
        )
        assert recovered.status == "pending"
    assert temporal.cancel_sync_job_workflow.await_count == 1
