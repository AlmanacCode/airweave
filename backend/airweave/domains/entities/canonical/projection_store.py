"""Canonical pending work and atomic publication. No network I/O under row locks."""

from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.projection_models import ProjectionLocator, ProjectionWork
from airweave.domains.entities.canonical.store import content_is_available, source_record
from airweave.models.entity import Entity
from airweave.models.sync import Sync


def publication_matches(locator: ProjectionLocator):
    """Add request organization/source scope separately; gate before text leaves retrieval."""
    return and_(
        Entity.id == locator.record_id,
        Entity.record_revision == locator.revision,
        Entity.indexed_revision == locator.revision,
        Entity.indexed_pipeline_version == locator.pipeline_version,
        Sync.index_pipeline_version == locator.pipeline_version,
        Entity.indexed_generation == locator.generation,
        Entity.deleted_at.is_(None),
        content_is_available(),
    )


class CanonicalProjectionStore:
    """Optimistic projection computation, serialized CAS against source capture."""

    async def pending(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        *,
        after_id: UUID | None = None,
        limit: int = 25,
    ) -> tuple[ProjectionWork, ...]:
        """One bounded UUID page; deleted records publish an empty generation."""
        if not 1 <= limit <= 100:
            raise ValueError("Projection batch size must be between 1 and 100")
        statement = (
            select(Entity, Sync.index_pipeline_version)
            .join(Sync, Sync.id == Entity.sync_id)
            .where(
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Sync.organization_id == organization_id,
                Entity.record_revision > 0,
                or_(
                    Entity.indexed_revision.is_distinct_from(Entity.record_revision),
                    Entity.indexed_pipeline_version.is_distinct_from(Sync.index_pipeline_version),
                    Entity.indexed_generation.is_(None),
                ),
                or_(Entity.deleted_at.is_not(None), content_is_available()),
            )
        )
        if after_id is not None:
            statement = statement.where(Entity.id > after_id)
        rows = await db.execute(statement.order_by(Entity.id).limit(limit + 1))
        return tuple(
            ProjectionWork(
                organization_id=organization_id,
                record=source_record(row),
                pipeline_version=version,
                previous_generation=row.indexed_generation,
            )
            for row, version in rows
        )

    async def _current(self, db: AsyncSession, work: ProjectionWork) -> Entity | None:
        # Capture locks Sync first too: parent visibility and revision cannot change
        # between validation and publication. This transaction contains no provider I/O.
        sync = await db.scalar(
            select(Sync)
            .where(
                Sync.id == work.record.sync_id,
                Sync.organization_id == work.organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if sync is None or sync.index_pipeline_version != work.pipeline_version:
            return None
        entity = await db.scalar(
            select(Entity)
            .where(
                Entity.id == work.record.id,
                Entity.sync_id == work.record.sync_id,
                Entity.organization_id == work.organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            entity is None
            or entity.record_revision != work.record.revision
            or entity.indexed_generation != work.previous_generation
        ):
            return None
        if entity.deleted_at is None:
            visible = await db.scalar(select(content_is_available()).where(Entity.id == entity.id))
            if not visible:
                return None
        return entity

    async def publish(
        self,
        db: AsyncSession,
        work: ProjectionWork,
        generation: UUID,
        chunk_count: int,
    ) -> bool:
        """Publish only a complete current generation; late writers cannot replace it."""
        if chunk_count < 0 or (work.record.deleted_at is None and chunk_count == 0):
            raise ValueError("Active records require a nonempty complete projection")
        if work.record.deleted_at is not None and chunk_count != 0:
            raise ValueError("Deleted records must publish no content")
        async with UnitOfWork(db):
            entity = await self._current(db, work)
            if entity is None:
                return False
            entity.indexed_revision = work.record.revision
            entity.indexed_pipeline_version = work.pipeline_version
            entity.indexed_generation = generation
            entity.indexed_chunk_count = chunk_count
            entity.projection_error = None
            await db.flush()
            return True

    async def fail(self, db: AsyncSession, work: ProjectionWork, message: str) -> None:
        """Record bounded diagnostics only while the same input remains current."""
        async with UnitOfWork(db):
            entity = await self._current(db, work)
            if entity is not None:
                entity.projection_error = message[:1000]
                await db.flush()
