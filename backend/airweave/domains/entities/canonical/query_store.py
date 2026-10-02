"""Read-only SQL for exact canonical record listing."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, func, literal_column, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query_models import RecordFilters
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import (
    SourceNotFound,
    content_is_available,
    parent_is_visible,
    source_record,
    with_content_access,
)
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class CanonicalQueryStore:
    """No provider or index calls; reads only committed canonical rows."""

    async def source_readable(self, db: AsyncSession, organization_id: UUID, sync_id: UUID) -> bool:
        """One batched operation-level check, never a separate check for each hit."""
        return bool(await db.scalar(select(source_is_readable(organization_id, sync_id))))

    async def read(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, record_id: UUID
    ) -> SourceRecord | None:
        """Read citation identity while gating content against current source authority."""
        row = (
            await db.execute(
                select(
                    Entity,
                    content_is_available() & source_is_readable(organization_id, sync_id),
                ).where(
                    Entity.id == record_id,
                    Entity.organization_id == organization_id,
                    Entity.sync_id == sync_id,
                    Entity.record_revision > 0,
                )
            )
        ).one_or_none()
        return with_content_access(source_record(row[0]), bool(row[1])) if row else None

    async def message_id(
        self, db: AsyncSession, organization_id: UUID, sync_id: UUID, native_id: str
    ) -> UUID | None:
        """Exact visible message identity, never a mailbox scan or index lookup."""
        return (
            await db.execute(
                select(Entity.id).where(
                    Entity.organization_id == organization_id,
                    Entity.sync_id == sync_id,
                    Entity.entity_definition_short_name == "message",
                    Entity.native_id == native_id,
                    select(SourceConnection.id)
                    .where(
                        SourceConnection.organization_id == organization_id,
                        SourceConnection.sync_id == sync_id,
                        SourceConnection.short_name == "gmail",
                        SourceConnection.is_authenticated.is_(True),
                    )
                    .exists(),
                    Entity.record_revision > 0,
                    Entity.deleted_at.is_(None),
                    content_is_available(),
                    parent_is_visible(),
                    source_is_readable(organization_id, sync_id),
                )
            )
        ).scalar_one_or_none()

    async def captured_counts(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
    ) -> dict[str, int]:
        """Count currently visible captured records, independent of index publication."""
        rows = await db.execute(
            select(Entity.entity_definition_short_name, func.count(Entity.id))
            .where(
                Entity.organization_id == organization_id,
                Entity.sync_id == sync_id,
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                content_is_available(),
                source_is_readable(organization_id, sync_id),
            )
            .group_by(Entity.entity_definition_short_name)
        )
        return dict(rows.all())

    async def list_records(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        filters: RecordFilters,
        *,
        after_id: UUID | None,
        limit: int,
        parent: RecordIdentity | None = None,
    ) -> tuple[SourceRecord, ...]:
        """Fetch one extra row so continuation does not require a count query."""
        scope = await db.scalar(
            select(Sync.id).where(
                Sync.id == sync_id,
                Sync.organization_id == organization_id,
                source_is_readable(organization_id, sync_id),
            )
        )
        if scope is None:
            raise SourceNotFound("Source is unavailable in this organization")
        statement = select(Entity).where(
            Entity.organization_id == organization_id,
            Entity.sync_id == sync_id,
            Entity.record_revision > 0,
            parent_is_visible(),
            source_is_readable(organization_id, sync_id),
        )
        if filters.parent_record_id is not None and parent is None:
            raise ValueError("Parent filter requires an authorized parent identity")
        if parent is not None:
            statement = statement.where(
                Entity.parent_record_type == parent.record_type,
                Entity.parent_native_id == parent.native_id,
                Entity.parent_container_id.is_not_distinct_from(parent.container_id),
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
        return tuple(
            with_content_access(
                source_record(row), row.removal_reason not in ("access_revoked", "scope_removed")
            )
            for row in rows
        )

    async def mail_thread(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        thread_id: str,
        *,
        after_id: UUID | None,
        after_created_at: datetime | None,
        limit: int,
    ) -> tuple[SourceRecord, ...]:
        """Use native Gmail thread identity, keeping mailbox capture containers unchanged."""
        scope = await db.scalar(
            select(Sync.id).where(
                Sync.id == sync_id,
                Sync.organization_id == organization_id,
                source_is_readable(organization_id, sync_id),
            )
        )
        if scope is None:
            raise SourceNotFound("Source is unavailable in this organization")
        statement = select(Entity).where(
            Entity.organization_id == organization_id,
            Entity.sync_id == sync_id,
            Entity.entity_definition_short_name == "message",
            Entity.record_revision > 0,
            Entity.deleted_at.is_(None),
            content_is_available(),
            source_is_readable(organization_id, sync_id),
            Entity.source_payload.op("->>")(literal_column("'threadId'")) == thread_id,
        )
        if after_id is not None:
            if after_created_at is None:
                statement = statement.where(
                    Entity.source_created_at.is_(None), Entity.id > after_id
                )
            else:
                statement = statement.where(
                    or_(
                        Entity.source_created_at > after_created_at,
                        Entity.source_created_at.is_(None),
                        and_(Entity.source_created_at == after_created_at, Entity.id > after_id),
                    )
                )
        rows = await db.scalars(
            statement.order_by(Entity.source_created_at.asc().nulls_last(), Entity.id).limit(
                limit + 1
            )
        )
        return tuple(source_record(row) for row in rows)
