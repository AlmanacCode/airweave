"""Atomic internal native ingestion; no provider I/O or public credential handling."""

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.models import CaptureResult
from airweave.domains.entities.canonical.scan_models import ScanResult
from airweave.domains.native_ingestion.models import IngestNativeBatch, IngestNativePage
from airweave.domains.native_ingestion.store import NativeIngestionStore


class NativeIngestionService:
    """Commit admission and canonical capture together, or neither."""

    def __init__(self, store: NativeIngestionStore):
        """Inject persistence; callers supply a fresh session without pending writes."""
        self.store = store

    async def ingest(self, db: AsyncSession, request: IngestNativeBatch) -> CaptureResult:
        """Admit one bounded snapshot batch under its current writer fence."""
        async with UnitOfWork(db):
            return await self.store.ingest(db, request)

    async def page(self, db: AsyncSession, request: IngestNativePage) -> ScanResult:
        """Commit admitted originals, sweep sightings and continuation together."""
        async with UnitOfWork(db):
            return await self.store.page(db, request)
