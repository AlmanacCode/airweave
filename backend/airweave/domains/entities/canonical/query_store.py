"""Read-only SQL for exact canonical record listing."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query_models import RecordFilters
from airweave.domains.entities.canonical.store import SourceNotFound, source_record
from airweave.models.entity import Entity
from airweave.models.sync import Sync


class CanonicalQueryStore:
    """No provider or index calls; reads only committed canonical rows."""

    async def list_records(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        filters: RecordFilters,
        *,
        after_id: UUID | None,
        limit: int,
    ) -> tuple[SourceRecord, ...]:
        """Fetch one extra row so continuation does not require a count query."""
        scope = await db.scalar(
            select(Sync.id).where(Sync.id == sync_id, Sync.organization_id == organization_id)
        )
        if scope is None:
            raise SourceNotFound("Source does not exist in this organization")
        statement = select(Entity).where(
            Entity.organization_id == organization_id,
            Entity.sync_id == sync_id,
            Entity.record_revision > 0,
        )
        if filters.record_type is not None:
            statement = statement.where(Entity.entity_definition_short_name == filters.record_type)
        if filters.container_id is not None:
            statement = statement.where(Entity.container_id == filters.container_id)
        if filters.state == "active":
            statement = statement.where(Entity.deleted_at.is_(None))
        elif filters.state == "deleted":
            statement = statement.where(Entity.deleted_at.is_not(None))
        if after_id is not None:
            statement = statement.where(Entity.id > after_id)
        rows = await db.scalars(statement.order_by(Entity.id).limit(limit + 1))
        return tuple(source_record(row) for row in rows)
