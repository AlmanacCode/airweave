"""Own atomic native binding transactions independently of provider provisioning."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.native_ingestion.source_models import EnsureNativeSource, NativeSource
from airweave.domains.native_ingestion.source_store import NativeSourceStore


class NativeSources:
    """Ensure/read only; source availability is not proof of imported content."""

    def __init__(self, store: NativeSourceStore):
        """Inject the single persistence boundary."""
        self.store = store

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
