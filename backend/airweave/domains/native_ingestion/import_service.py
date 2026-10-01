"""Atomic native import start/read application boundary."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.native_ingestion.import_models import NativeImportState, StartNativeImport
from airweave.domains.native_ingestion.import_store import NativeImportStore


class NativeImports:
    """Own the transaction; never enqueue provider execution or expose writer fences."""

    def __init__(self, store: NativeImportStore):
        """Inject the native import persistence boundary."""
        self.store = store

    async def start(
        self,
        db: AsyncSession,
        organization_id: UUID,
        source_id: UUID,
        request_key: str,
        request: StartNativeImport,
    ) -> NativeImportState:
        """Create or recover a durable import and commit its writer/cycle atomically."""
        async with UnitOfWork(db):
            return await self.store.start(db, organization_id, source_id, request_key, request)

    async def read(
        self, db: AsyncSession, organization_id: UUID, source_id: UUID, request_key: str
    ) -> NativeImportState:
        """Read durable progress under source authorization without reactivation."""
        async with UnitOfWork(db):
            return await self.store.read(db, organization_id, source_id, request_key)
