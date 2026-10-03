"""CRUD operations for sync jobs."""

from typing import Optional
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.context import BaseContext
from airweave.crud._base_organization import CRUDBaseOrganization
from airweave.db.unit_of_work import UnitOfWork
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob, SyncJobStatus
from airweave.schemas.sync_job import SyncJobCreate, SyncJobUpdate


async def lock_sync_for_job(db: AsyncSession, organization: UUID, sync_id: UUID) -> Sync:
    """Serialize admission with provisioning changes, including privileged admin runs."""
    sync = await db.scalar(
        select(Sync)
        .where(Sync.id == sync_id, Sync.organization_id == organization)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if sync is None:
        raise HTTPException(status_code=404, detail="Sync not found")
    if sync.status != "active" or (
        sync.provisioning_generation != sync.provisioning_ready_generation
    ):
        raise HTTPException(status_code=409, detail="Owned source is not ready for capture")
    return sync


class SyncJobBusy(HTTPException):
    """An admitted job already owns this sync; callers may defer without holding a slot."""

    def __init__(self) -> None:
        """Expose the existing HTTP conflict contract to manual callers."""
        super().__init__(status_code=409, detail="A sync job is already active")


class CRUDSyncJob(CRUDBaseOrganization[SyncJob, SyncJobCreate, SyncJobUpdate]):
    """CRUD operations for sync jobs."""

    async def create(
        self,
        db: AsyncSession,
        *,
        obj_in: SyncJobCreate,
        ctx: BaseContext,
        uow: Optional[UnitOfWork] = None,
        skip_validation: bool = False,
    ) -> SyncJob:
        """Stamp job admission under the same lock used by account generation changes."""
        if not skip_validation:
            await self._validate_organization_access(ctx, ctx.organization.id)
        sync = await lock_sync_for_job(db, ctx.organization.id, obj_in.sync_id)
        if obj_in.id is not None:
            existing = await db.scalar(
                select(SyncJob)
                .where(SyncJob.id == obj_in.id, SyncJob.organization_id == ctx.organization.id)
                .execution_options(populate_existing=True)
            )
            if existing is not None:
                if (
                    existing.organization_id != ctx.organization.id
                    or existing.sync_id != sync.id
                    or existing.provisioning_generation != sync.provisioning_generation
                ):
                    raise HTTPException(status_code=409, detail="Job admission identity changed")
                return existing
        active = await db.scalar(
            select(SyncJob.id)
            .where(
                SyncJob.sync_id == sync.id,
                SyncJob.organization_id == ctx.organization.id,
                SyncJob.status.in_(
                    [SyncJobStatus.PENDING, SyncJobStatus.RUNNING, SyncJobStatus.CANCELLING]
                ),
            )
            .limit(1)
        )
        if active is not None:
            raise SyncJobBusy()
        values = obj_in.model_dump(exclude_unset=True, exclude_none=True)
        values["provisioning_generation"] = sync.provisioning_generation
        return await super().create(
            db, obj_in=values, ctx=ctx, uow=uow, skip_validation=True
        )

    async def get(self, db: AsyncSession, id: UUID, ctx: BaseContext) -> SyncJob | None:
        """Get a sync job by ID."""
        stmt = (
            select(SyncJob, Sync.name.label("sync_name"))
            .join(Sync, SyncJob.sync_id == Sync.id)
            .where(SyncJob.id == id, SyncJob.organization_id == ctx.organization.id)
        )
        result = await db.execute(stmt)
        row = result.first()
        if not row:
            return None

        job, sync_name = row
        # Add the sync name to the job object
        job.sync_name = sync_name
        return job

    async def get_all_by_sync_id(
        self,
        db: AsyncSession,
        sync_id: UUID,
        status: Optional[list[str]] = None,
        limit: Optional[int] = None,
    ) -> list[SyncJob]:
        """Get jobs for a sync; optional status filter; newest first; optional row limit."""
        stmt = (
            select(SyncJob, Sync.name.label("sync_name"))
            .join(Sync, SyncJob.sync_id == Sync.id)
            .where(SyncJob.sync_id == sync_id)
        )

        # Add status filter if provided
        if status:
            # Database enum already uses uppercase values
            stmt = stmt.where(SyncJob.status.in_(status))

        stmt = stmt.order_by(SyncJob.created_at.desc())
        if limit is not None:
            stmt = stmt.limit(limit)

        result = await db.execute(stmt)
        jobs = []
        for job, sync_name in result:
            job.sync_name = sync_name
            jobs.append(job)
        return jobs

    async def get_all_jobs(
        self,
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100,
        status: Optional[list[str]] = None,
    ) -> list[SyncJob]:
        """Get all sync jobs across all syncs, optionally filtered by status."""
        stmt = select(SyncJob, Sync.name.label("sync_name")).join(Sync, SyncJob.sync_id == Sync.id)

        # Add status filter if provided
        if status:
            stmt = stmt.where(SyncJob.status.in_(status))

        stmt = stmt.order_by(SyncJob.created_at.desc()).offset(skip).limit(limit)

        result = await db.execute(stmt)
        jobs = []
        for job, sync_name in result:
            job.sync_name = sync_name
            jobs.append(job)
        return jobs

    async def get_latest_by_sync_id(
        self,
        db: AsyncSession,
        sync_id: UUID,
    ) -> SyncJob | None:
        """Get the most recent job for a specific sync."""
        stmt = (
            select(SyncJob, Sync.name.label("sync_name"))
            .join(Sync, SyncJob.sync_id == Sync.id)
            .where(SyncJob.sync_id == sync_id)
            .order_by(SyncJob.created_at.desc())
            .limit(1)
        )
        result = await db.execute(stmt)
        row = result.first()
        if not row:
            return None

        job, sync_name = row
        # Add the sync name to the job object
        job.sync_name = sync_name
        return job


sync_job = CRUDSyncJob(SyncJob)
