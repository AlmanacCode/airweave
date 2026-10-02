"""Own atomic native binding transactions independently of provider provisioning."""

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.source_lifecycle import stop_source_writer
from airweave.domains.native_ingestion.publication_models import (
    NativeInventoryPage,
    NativePublication,
)
from airweave.domains.native_ingestion.publication_store import NativePublicationStore
from airweave.domains.native_ingestion.source_models import EnsureNativeSource, NativeSource
from airweave.domains.native_ingestion.source_store import NativeSourceStore


class NativeSources:
    """Ensure/read only; source availability is not proof of imported content."""

    def __init__(self, store: NativeSourceStore):
        """Inject the single persistence boundary."""
        self.store = store
        self.publications = NativePublicationStore(store)

    async def ensure(
        self, db: AsyncSession, organization_id: UUID, request: EnsureNativeSource
    ) -> NativeSource:
        """Publish the complete source/sync binding or roll back both."""
        async with UnitOfWork(db):
            return (await self.store.ensure(db, organization_id, request)).response()

    async def get(self, db: AsyncSession, organization_id: UUID, source_id: UUID) -> NativeSource:
        """Read a currently valid binding without opening a provider connection."""
        async with UnitOfWork(db):
            return (await self.store.require(db, organization_id, source_id)).response()

    async def publication(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, owner_id: str
    ) -> NativePublication:
        """Recover the current import receipt without activating a writer."""
        async with UnitOfWork(db):
            return await self.publications.current(db, organization_id, source_id, owner_id)

    async def inventory(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        owner_id: str,
        *,
        limit: int = 100,
        after: UUID | None = None,
        roots_only: bool = False,
    ) -> NativeInventoryPage:
        """Enumerate metadata for exact owner-authorized native DB reconciliation."""
        async with UnitOfWork(db):
            return await self.publications.inventory(
                db,
                organization_id,
                source_id,
                owner_id,
                limit=limit,
                after=after,
                roots_only=roots_only,
            )

    async def withdraw(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, owner_id: str
    ) -> NativeSource:
        """Explicit owner withdrawal uses the same writer and availability transition."""
        async with UnitOfWork(db):
            bound = await self.store.require(db, organization_id, source_id)
            if bound.binding.owner_id != owner_id:
                raise HTTPException(404, "Native source is unavailable for this owner")
            await stop_source_writer(db, bound.sync, bound.source, retain_read_authority=False)
            return bound.response()
