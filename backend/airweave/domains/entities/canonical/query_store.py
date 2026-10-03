"""Read-only SQL for exact canonical record listing."""

from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import and_, case, func, literal_column, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.query_models import (
    RecordBrowseCursor,
    RecordBrowseItem,
    RecordBrowseQuery,
    RecordBrowseSource,
    RecordFilters,
)
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    SourceNotFound,
    content_is_available,
    parent_is_visible,
    source_record,
    with_content_access,
)
from airweave.domains.native_ingestion.models import NativeSnapshotMetadata
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class RecordMetadataUnavailable(CanonicalStoreError):
    """Native provenance is not safe to expose as an ordinary provider original."""

    code = "record_metadata_unavailable"


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

    async def browse_sources(
        self, db: AsyncSession, organization_id: UUID, sync_ids: tuple[UUID, ...]
    ) -> tuple[RecordBrowseSource, ...]:
        """Require every requested source; unavailable inventory is not an empty page."""
        rows = await db.execute(
            select(
                SourceConnection.sync_id,
                SourceConnection.id.label("source_connection_id"),
                SourceConnection.short_name.label("provider"),
            )
            .join(Sync, Sync.id == SourceConnection.sync_id)
            .where(
                SourceConnection.organization_id == organization_id,
                Sync.organization_id == organization_id,
                SourceConnection.sync_id.in_(sync_ids),
                SourceConnection.is_authenticated.is_(True),
                source_is_readable(organization_id, SourceConnection.sync_id),
            )
            .order_by(SourceConnection.sync_id, SourceConnection.id)
        )
        sources = tuple(RecordBrowseSource.model_validate(row) for row in rows.mappings())
        if len(sources) != len(sync_ids) or {s.sync_id for s in sources} != set(sync_ids):
            raise SourceNotFound("Selected sources are unavailable in this organization")
        return sources

    def browse_statement(
        self,
        organization_id: UUID,
        query: RecordBrowseQuery,
        sources: tuple[RecordBrowseSource, ...],
        cursor: RecordBrowseCursor | None,
    ):
        """Select only bounded metadata, applying all authority before keyset and LIMIT."""
        filters = query.filters
        native = Entity.sync_id.in_(tuple(s.sync_id for s in sources if s.provider == "almanac"))
        payload = Entity.source_payload
        native_type = case(
            (
                native,
                case(
                    (
                        Entity.entity_definition_short_name == "knowledge",
                        payload["original"]["type"].astext,
                    ),
                    else_=Entity.entity_definition_short_name,
                ),
            ),
            else_=None,
        )
        native_metadata = case(
            (
                native,
                func.jsonb_build_object(
                    "authority",
                    payload["authority"],
                    "representation",
                    payload["representation"],
                    "schema_version",
                    payload["schema_version"],
                    "owner_id",
                    payload["owner_id"],
                    "identity",
                    payload["identity"],
                    "parent",
                    payload["parent"],
                    "version",
                    payload["version"],
                    "operation",
                    payload["operation"],
                ),
            ),
            else_=None,
        )
        title = case(
            (
                native,
                func.coalesce(
                    payload["original"]["title"].astext, payload["original"]["name"].astext
                ),
            ),
            else_=func.coalesce(
                Entity.gmail_metadata["subject"].astext,
                payload["name"].astext,
                payload["title"].astext,
                payload["summary"].astext,
                payload["responses"][0]["response"]["title"].astext,
                payload["listing"]["title"].astext,
            ),
        )
        clock = (
            Entity.source_created_at
            if filters.basis == "source_created"
            else Entity.source_updated_at
        )
        eligible_sources = (
            select(Sync.id)
            .where(
                Sync.organization_id == organization_id,
                Sync.id.in_(filters.sync_ids),
                source_is_readable(organization_id, Sync.id),
            )
            .cte("browse_sources")
            .prefix_with("MATERIALIZED")
        )
        statement = (
            select(
                Entity.id.label("record_id"),
                Entity.sync_id,
                Entity.record_revision.label("revision"),
                Entity.entity_definition_short_name.label("record_type"),
                Entity.native_id,
                Entity.container_id,
                Entity.parent_record_type,
                Entity.parent_native_id,
                Entity.parent_container_id,
                func.left(func.coalesce(title, Entity.native_id), 512).label("title"),
                Entity.source_created_at,
                Entity.source_updated_at,
                Entity.observed_at,
                Entity.completeness,
                native_metadata.label("native_metadata"),
                native_type.label("native_type"),
                case(
                    (
                        func.jsonb_typeof(payload["threadId"]) == "string",
                        payload["threadId"].astext,
                    ),
                    else_=None,
                ).label("email_thread_id"),
            )
            .join(eligible_sources, eligible_sources.c.id == Entity.sync_id)
            .where(
                Entity.organization_id == organization_id,
                Entity.sync_id.in_(filters.sync_ids),
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                content_is_available(),
                clock.is_not(None),
            )
        )
        if filters.record_types:
            statement = statement.where(
                Entity.entity_definition_short_name.in_(filters.record_types)
            )
        if filters.native_types:
            statement = statement.where(native_type.in_(filters.native_types))
        for field, after, before in (
            (Entity.source_created_at, filters.created_after, filters.created_before),
            (Entity.source_updated_at, filters.updated_after, filters.updated_before),
        ):
            if after is not None:
                statement = statement.where(field >= after)
            if before is not None:
                statement = statement.where(field < before)
        if cursor is not None:
            key, position = tuple_(clock, Entity.id), tuple_(cursor.after_time, cursor.after_id)
            statement = statement.where(
                key > position if filters.order == "asc" else key < position
            )
        ordering = (
            (clock.asc(), Entity.id.asc())
            if filters.order == "asc"
            else (clock.desc(), Entity.id.desc())
        )
        return statement.order_by(*ordering).limit(query.limit + 1)

    async def browse(
        self,
        db: AsyncSession,
        organization_id: UUID,
        query: RecordBrowseQuery,
        sources: tuple[RecordBrowseSource, ...],
        cursor: RecordBrowseCursor | None,
    ) -> tuple[RecordBrowseItem, ...]:
        """Decode native provenance without fetching original bodies or index artifacts."""
        rows = await db.execute(self.browse_statement(organization_id, query, sources, cursor))
        by_sync = {source.sync_id: source for source in sources}
        result = []
        for row in rows.mappings():
            values = dict(row)
            source = by_sync[values["sync_id"]]
            identity = RecordIdentity(
                record_type=values.pop("record_type"),
                native_id=values.pop("native_id"),
                container_id=values.pop("container_id"),
            )
            parent_type = values.pop("parent_record_type")
            parent_native = values.pop("parent_native_id")
            parent_container = values.pop("parent_container_id")
            parent = (
                RecordIdentity(
                    record_type=parent_type, native_id=parent_native, container_id=parent_container
                )
                if parent_type
                else None
            )
            metadata = values.pop("native_metadata")
            version = None
            if source.provider == "almanac":
                try:
                    snapshot = NativeSnapshotMetadata.model_validate(metadata)
                    if (
                        snapshot.identity != identity
                        or snapshot.parent != parent
                        or snapshot.operation != "upsert"
                    ):
                        raise ValueError("Native metadata differs from canonical identity")
                    if not values["native_type"]:
                        raise ValueError("Native type is unavailable")
                    version = snapshot.version
                except (ValidationError, ValueError):
                    raise RecordMetadataUnavailable(
                        "Retained native metadata needs a fresh export"
                    ) from None
            result.append(
                RecordBrowseItem(
                    **values,
                    source_connection_id=source.source_connection_id,
                    provider=source.provider,
                    identity=identity,
                    parent=parent,
                    native_version=version,
                )
            )
        return tuple(result)
