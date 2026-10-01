"""Primary-key serialized native binding; no JSON discovery or external side effects."""

from uuid import UUID

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.native_ingestion.models import NativeSourceBinding
from airweave.domains.native_ingestion.source_models import (
    EnsureNativeSource,
    NativeSource,
    native_source_id,
    native_sync_id,
)
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync


class LockedNativeSource(BaseModel):
    """Internal ORM state held under Sync then SourceConnection row locks."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)
    sync: Sync
    source: SourceConnection
    binding: NativeSourceBinding

    def response(self) -> NativeSource:
        """Expose only stable identifiers and typed binding, not ORM/auth data."""
        return NativeSource(
            source_connection_id=self.source.id,
            sync_id=self.sync.id,
            organization_id=self.sync.organization_id,
            binding=self.binding,
            collection=self.source.readable_collection_id,
            available=self.source.is_authenticated,
        )


class NativeSourceStore:
    """All methods run within their caller's transaction; locks live through commit."""

    async def ensure(
        self, db: AsyncSession, organization_id: UUID, request: EnsureNativeSource
    ) -> LockedNativeSource:
        """Concurrent exact ensures serialize on the deterministic Sync primary key."""
        collection = await db.scalar(
            select(Collection.id).where(
                Collection.organization_id == organization_id,
                Collection.readable_id == request.collection,
            )
        )
        if collection is None:
            raise HTTPException(404, "Native collection not found")
        binding = request.binding()
        source_id = native_source_id(organization_id, binding)
        sync_id = native_sync_id(source_id)
        inserted = await db.scalar(
            insert(Sync)
            .values(
                id=sync_id,
                organization_id=organization_id,
                name=f"Almanac {binding.dataset}",
                status="ACTIVE",
                sync_type="full",
            )
            .on_conflict_do_nothing(index_elements=[Sync.id])
            .returning(Sync.id)
        )
        # The unique insert waits for any concurrent ensure before this lock/read.
        sync = await db.scalar(
            select(Sync)
            .where(
                Sync.id == sync_id,
                Sync.organization_id == organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if sync is None:
            raise HTTPException(409, "Native source identity conflicts with existing state")
        if inserted is not None:
            existing = await db.scalar(
                select(SourceConnection.id).where(SourceConnection.id == source_id)
            )
            if existing is not None:
                raise HTTPException(409, "Native source identity conflicts with existing state")
            db.add(
                SourceConnection(
                    id=source_id,
                    organization_id=organization_id,
                    sync_id=sync_id,
                    name=f"Almanac {binding.dataset}",
                    short_name="almanac",
                    config_fields=binding.model_dump(mode="json"),
                    readable_collection_id=request.collection,
                    is_authenticated=True,
                )
            )
            await db.flush()
        locked = await self.require(db, organization_id, source_id, missing_status=409)
        if locked.binding != binding or locked.source.readable_collection_id != request.collection:
            raise HTTPException(409, "Native source binding is immutable")
        return locked

    async def require(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        *,
        missing_status: int = 404,
    ) -> LockedNativeSource:
        """Lock Sync then source and fail closed on foreign or unexplained partial state."""
        sync = await db.scalar(
            select(Sync)
            .where(
                Sync.id == native_sync_id(source_id),
                Sync.organization_id == organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        source = await db.scalar(
            select(SourceConnection)
            .where(
                SourceConnection.id == source_id,
                SourceConnection.organization_id == organization_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if sync is None or source is None:
            raise HTTPException(missing_status, "Native source not found or incomplete")
        try:
            binding = NativeSourceBinding.model_validate(source.config_fields)
        except ValidationError as error:
            raise HTTPException(409, "Native source binding is malformed") from error
        if (
            source.short_name != "almanac"
            or source.sync_id != sync.id
            or native_source_id(organization_id, binding) != source_id
            or source.readable_auth_provider_id is not None
            or source.auth_provider_config is not None
            or source.connection_id is not None
            or source.connection_init_session_id is not None
        ):
            raise HTTPException(409, "Native source identity conflicts with existing state")
        collection = await db.scalar(
            select(Collection.id).where(
                Collection.readable_id == source.readable_collection_id,
                Collection.organization_id == organization_id,
            )
        )
        if collection is None:
            raise HTTPException(409, "Native source collection is unavailable")
        return LockedNativeSource(sync=sync, source=source, binding=binding)
