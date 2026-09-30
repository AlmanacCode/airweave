"""Transaction boundary for source capture; external I/O happens before entry."""

from uuid import UUID

from pydantic import JsonValue
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.models import CaptureResult, ReconcileResult
from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    ReconcileScope,
    RemovedScope,
    StartedScope,
    WriterFence,
)
from airweave.domains.entities.canonical.store import CanonicalRecordStore


class CanonicalCaptureService:
    """Own capture transactions; callers supply a fresh session without pending writes."""

    def __init__(self, store: CanonicalRecordStore):
        """Inject the persistence owner without hidden global sessions."""
        self.store = store

    async def activate_writer(
        self,
        db: AsyncSession,
        organization_id: UUID,
        sync_id: UUID,
        job_id: UUID,
        *,
        attempt_id: UUID,
        attempt_number: int,
    ) -> WriterFence:
        """Activate one valid source run under the same lock used for capture."""
        async with UnitOfWork(db):
            return await self.store.activate_writer(
                db,
                organization_id,
                sync_id,
                job_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
            )

    async def capture(self, db: AsyncSession, batch: CaptureBatch) -> CaptureResult:
        """Commit record state and observed changes atomically."""
        async with UnitOfWork(db):
            return await self.store.capture(db, batch)

    async def reconcile_scope(self, db: AsyncSession, request: ReconcileScope) -> ReconcileResult:
        """Commit one bounded exact-scope absence reconciliation batch."""
        async with UnitOfWork(db):
            return await self.store.reconcile_scope(db, request)

    async def start_scope(self, db: AsyncSession, fence: WriterFence, scope: StartedScope) -> None:
        """Commit a new exact-scope enumeration boundary."""
        async with UnitOfWork(db):
            await self.store.start_scope(db, fence, scope)

    async def remove_scope(
        self,
        db: AsyncSession,
        fence: WriterFence,
        scope: RemovedScope,
        *,
        limit: int = 250,
    ) -> ReconcileResult:
        """Commit a bounded batch of confirmed inaccessible/deselected records."""
        async with UnitOfWork(db):
            return await self.store.remove_scope(db, fence, scope, limit=limit)

    async def reconcile_parents(
        self,
        db: AsyncSession,
        fence: WriterFence,
        *,
        limit: int = 250,
    ) -> ReconcileResult:
        """Commit bounded durable removal of children with inaccessible/missing parents."""
        async with UnitOfWork(db):
            return await self.store.reconcile_parents(db, fence, limit=limit)

    async def save_checkpoint(
        self, db: AsyncSession, fence: WriterFence, cursor_data: dict[str, JsonValue]
    ) -> None:
        """Advance only after the caller's full capture/reconciliation completion barrier."""
        async with UnitOfWork(db):
            await self.store.save_checkpoint(db, fence, cursor_data)
