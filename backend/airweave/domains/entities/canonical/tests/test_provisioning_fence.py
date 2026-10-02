"""Connection changes invalidate capture independently of worker cancellation."""

from uuid import uuid4

import pytest
from sqlalchemy import select, update

from airweave.domains.entities.canonical.store import StaleWriter
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.models import Sync, SyncJob

pytestmark = pytest.mark.integration


async def test_reconnect_rejects_existing_writer_without_losing_history(database, source):
    service, fence = source
    original = await capture(database, service, fence, observation())
    async with database() as db:
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(provisioning_generation=1)
        )
        await db.commit()

    with pytest.raises(StaleWriter):
        await capture(database, service, fence, observation("stale-credential-result"))
    async with database() as db:
        with pytest.raises(StaleWriter):
            await service.store.admit_job(db, fence.organization_id, fence.sync_id, fence.job_id)
        page = await service.store.changes(db, fence.organization_id, fence.sync_id)
        assert page.high_watermark == original.sequence
        assert [change.record.identity.native_id for change in page.changes] == ["one"]


async def test_managed_job_requires_verified_active_generation_even_on_identical_retry(
    database, source
):
    service, fence = source
    async with database() as db:
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(
                provisioning_generation=1, status="active"
            )
        )
        await db.execute(
            update(SyncJob).where(SyncJob.id == fence.job_id).values(provisioning_generation=1)
        )
        await db.commit()

    async with database() as db:
        with pytest.raises(StaleWriter):
            await service.store.admit_job(db, fence.organization_id, fence.sync_id, fence.job_id)
        with pytest.raises(StaleWriter):
            await service.store.activate_writer(
                db, fence.organization_id, fence.sync_id, fence.job_id,
                attempt_id=fence.attempt_id, attempt_number=fence.attempt_number,
            )
        await db.rollback()
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(provisioning_ready_generation=1)
        )
        await db.commit()
        assert await service.store.admit_job(
            db, fence.organization_id, fence.sync_id, fence.job_id
        ) == 1
        with pytest.raises(StaleWriter):
            await service.store.admit_job(db, uuid4(), fence.sync_id, fence.job_id)

    await capture(database, service, fence, observation("verified"))
    async with database() as db:
        await db.execute(update(Sync).where(Sync.id == fence.sync_id).values(status="paused"))
        await db.commit()
    with pytest.raises(StaleWriter):
        await capture(database, service, fence, observation("after-disconnect"))
    async with database() as db:
        assert await db.scalar(select(Sync.observed_change_sequence)) == 1
