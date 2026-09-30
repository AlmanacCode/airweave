"""Transaction boundary for source capture; external I/O happens before entry."""

from uuid import UUID

from pydantic import JsonValue
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CaptureCycle,
    CompleteCycle,
    RestartCycle,
    ScopeWork,
)
from airweave.domains.entities.canonical.cycle_store import (
    begin_cycle,
    complete_cycle,
    cursor_row,
    cycle_state,
    next_scope_work,
    restart_cycle,
)
from airweave.domains.entities.canonical.models import CaptureResult, ReconcileResult, SourceRecord
from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CompletedScope,
    ReconcileScope,
    RemovedScope,
    ScopeRemovalReason,
    StartedScope,
    WriterFence,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitOmission,
    CommitScanPage,
    ReconcileScan,
    ScanResult,
    ScanState,
)
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore


class CanonicalCaptureService:
    """Own capture transactions; callers supply a fresh session without pending writes."""

    def __init__(self, store: CanonicalRecordStore):
        """Inject the persistence owner without hidden global sessions."""
        self.store = store
        self.scans = CanonicalScanStore(store)

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

    async def next_scope_work(
        self, db: AsyncSession, fence: WriterFence, cycle_id: UUID
    ) -> ScopeWork | None:
        """Read the next eligible incomplete scope under the existing writer fence."""
        async with UnitOfWork(db):
            await self.store._fenced_sync(db, fence)
            return await next_scope_work(db, fence, cycle_id)

    async def withdraw_scan_parent(
        self,
        db: AsyncSession,
        fence: WriterFence,
        parent: SourceRecord,
        *,
        expected_epoch: int,
        removal_reason: ScopeRemovalReason,
    ) -> CaptureResult:
        """Attest exact fetched routing revision and owner epoch in the withdrawal transaction."""
        async with UnitOfWork(db):
            return await self.scans.withdraw_parent(
                db, fence, parent, expected_epoch=expected_epoch, removal_reason=removal_reason
            )

    async def scan_missing(
        self, db: AsyncSession, fence: WriterFence, state: ScanState, *, after: UUID | None = None
    ) -> tuple[SourceRecord, ...]:
        """Read exact inventory omissions under the same attempt and scan revision."""
        async with UnitOfWork(db):
            return await self.scans.missing(db, fence, state, after=after)

    async def read_scan(
        self, db: AsyncSession, fence: WriterFence, scope: CompletedScope
    ) -> ScanState | None:
        """Reload an exact scope after restart or uncertain acknowledgement."""
        async with UnitOfWork(db):
            return await self.scans.read(db, fence, scope)

    async def begin_scan(self, db: AsyncSession, request: BeginScan) -> ScanState:
        """Begin/resume under the same fence as captured records."""
        async with UnitOfWork(db):
            return await self.scans.begin(db, request)

    async def commit_scan_page(self, db: AsyncSession, request: CommitScanPage) -> ScanResult:
        """Commit original records and their continuation atomically."""
        async with UnitOfWork(db):
            return await self.scans.page(db, request)

    async def commit_omission(self, db: AsyncSession, request: CommitOmission) -> ScanResult:
        """Accept exact provider validation without network I/O in the transaction."""
        async with UnitOfWork(db):
            return await self.scans.omission(db, request)

    async def reconcile_scan(self, db: AsyncSession, request: ReconcileScan) -> ScanResult:
        """Commit one bounded whole-scope absence reconciliation step."""
        async with UnitOfWork(db):
            return await self.scans.reconcile(db, request)

    async def read_cycle(self, db: AsyncSession, fence: WriterFence) -> CaptureCycle | None:
        """Read the current durable boundary without creating a new cycle."""
        async with UnitOfWork(db):
            await self.store._fenced_sync(db, fence)
            return cycle_state(await cursor_row(db, fence))

    async def begin_cycle(self, db: AsyncSession, request: BeginCycle) -> CaptureCycle:
        """Resume or explicitly advance the existing checkpoint's cycle."""
        async with UnitOfWork(db):
            await self.store._fenced_sync(db, request.fence)
            return await begin_cycle(db, request)

    async def complete_cycle(self, db: AsyncSession, request: CompleteCycle) -> CaptureCycle:
        """Atomically verify scope completion and publish the source checkpoint."""
        async with UnitOfWork(db):
            sync = await self.store._fenced_sync(db, request.fence)
            return await complete_cycle(db, sync, request)

    async def restart_cycle(self, db: AsyncSession, request: RestartCycle) -> CaptureCycle:
        """Explicitly replace active cycle state under the writer fence and exact CAS."""
        async with UnitOfWork(db):
            await self.store._fenced_sync(db, request.fence)
            return await restart_cycle(db, request)
