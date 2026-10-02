"""Stop a locked source writer while preserving the existing read-authority fact."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob


async def stop_source_writer(
    db: AsyncSession, sync: Sync, source: SourceConnection, *, retain_read_authority: bool
) -> tuple[UUID, ...]:
    """Caller holds the source/sync locks; external workflow cancellation follows commit."""
    if source.sync_id != sync.id or source.organization_id != sync.organization_id:
        raise ValueError("Source writer transition belongs to another sync")
    sync.status = "paused"
    sync.writer_epoch += 1
    jobs = (
        await db.scalars(
            select(SyncJob)
            .where(
                SyncJob.sync_id == sync.id,
                SyncJob.status.in_(("pending", "running", "cancelling")),
            )
            .with_for_update()
        )
    ).all()
    for job in jobs:
        job.status = "cancelled"
    source.is_authenticated = source.is_authenticated and retain_read_authority
    return tuple(job.id for job in jobs)
