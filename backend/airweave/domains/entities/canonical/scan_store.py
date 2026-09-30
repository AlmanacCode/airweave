"""Scan persistence inside the existing capture transaction; no provider I/O or commits."""

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import CaptureCycle, CycleVersion
from airweave.domains.entities.canonical.cycle_store import (
    CycleConflict,
    attest_cycle,
    attest_scope,
    persist,
)
from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CompletedScope,
    ReconcileScope,
    RecordIdentity,
    WriterFence,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
    ScanResult,
    ScanState,
    ScanVersion,
)
from airweave.domains.entities.canonical.store import (
    CanonicalRecordStore,
    CanonicalStoreError,
    content_is_available,
)
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity


class ScanConflict(CanonicalStoreError):
    """Reload durable progress before performing more provider I/O."""

    code = "scan_conflict"


def scope_key(scope: CompletedScope) -> str:
    """Distinguish null containers from empty strings without a nullable unique key."""
    return json.dumps(
        [scope.record_type, scope.container_id], ensure_ascii=False, separators=(",", ":")
    )


def scan_state(row: CaptureScan) -> ScanState:
    """Do not leak ORM state outside its transaction."""
    return ScanState(
        scope=CompletedScope(record_type=row.record_type, container_id=row.container_id),
        cycle_id=row.cycle_id,
        version=ScanVersion(sweep_id=row.sweep_id, revision=row.revision),
        phase=row.phase,
        fingerprint=row.fingerprint,
        continuation=ScanContinuation(value=row.continuation),
        started_at=row.started_at,
        completed_at=row.completed_at,
    )


class CanonicalScanStore:
    """Reuse record capture, lock ordering and journal semantics for recoverable scans."""

    def __init__(self, records: CanonicalRecordStore):
        """The existing record store remains the only authority for record mutations."""
        self.records = records

    async def _row(
        self, db: AsyncSession, fence: WriterFence, scope: CompletedScope
    ) -> CaptureScan | None:
        return await db.scalar(
            select(CaptureScan)
            .where(
                CaptureScan.organization_id == fence.organization_id,
                CaptureScan.sync_id == fence.sync_id,
                CaptureScan.scope_key == scope_key(scope),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def read(
        self, db: AsyncSession, fence: WriterFence, scope: CompletedScope
    ) -> ScanState | None:
        """Read only under the active writer fence, including after an uncertain commit."""
        await self.records._fenced_sync(db, fence)
        row = await self._row(db, fence, scope)
        return scan_state(row) if row else None

    @staticmethod
    def _expect(row: CaptureScan | None, expected: ScanVersion, cycle_id: UUID) -> CaptureScan:
        if (
            row is None
            or row.cycle_id != cycle_id
            or row.sweep_id != expected.sweep_id
            or row.revision != expected.revision
        ):
            raise ScanConflict("Scan changed; reload its durable state")
        return row

    async def begin(self, db: AsyncSession, request: BeginScan) -> ScanState:
        """Resume unchanged scans; restart and next-cycle transitions require exact CAS."""
        await self.records._fenced_sync(db, request.fence)
        cursor, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        is_root = await attest_scope(db, request.fence, cycle, request.scope)
        if request.fingerprint != cycle.configuration.fingerprint:
            raise CycleConflict("Scan configuration differs from the active cycle")
        row = await self._row(db, request.fence, request.scope)
        if (
            row is not None
            and row.cycle_id == request.cycle_id
            and is_root
            and cycle.configuration.child_record_types
            and cycle.root_writer_attempt_id != request.fence.attempt_id
            and not request.restart
        ):
            raise CycleConflict("Restart root membership for this writer attempt")
        if row is None:
            if request.expected is not None or request.restart:
                raise ScanConflict("Cannot restart a missing scan")
            row = CaptureScan(
                organization_id=request.fence.organization_id,
                sync_id=request.fence.sync_id,
                scope_key=scope_key(request.scope),
                record_type=request.scope.record_type,
                container_id=request.scope.container_id,
                revision=1,
            )
            db.add(row)
        else:
            if request.expected is not None:
                self._expect(row, request.expected, row.cycle_id)
            if (
                row.cycle_id == request.cycle_id
                and row.fingerprint == request.fingerprint
                and not request.restart
            ):
                return scan_state(row)
            if request.expected is None:
                raise ScanConflict("Replacing a scan requires its current version")
            if row.cycle_id == request.cycle_id and not request.restart:
                raise ScanConflict("Changed scope configuration requires an explicit restart")
            row.revision += 1
        row.cycle_id = request.cycle_id
        row.sweep_id = uuid4()
        row.fingerprint = request.fingerprint
        row.phase = "collecting"
        row.continuation = request.continuation.value
        row.started_at = datetime.now(timezone.utc)
        row.completed_at = None
        if is_root:
            cycle = cycle.model_copy(
                update={
                    "root_writer_attempt_id": request.fence.attempt_id,
                    "version": CycleVersion(
                        cycle_id=cycle.version.cycle_id, revision=cycle.version.revision + 1
                    ),
                }
            )
            persist(cursor, cycle)
        await db.flush()
        return scan_state(row)

    async def page(self, db: AsyncSession, request: CommitScanPage) -> ScanResult:
        """Capture and advance the page as one transaction, never a partial acknowledgement."""
        sync = await self.records._fenced_sync(db, request.fence)
        _, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        is_root = await attest_scope(db, request.fence, cycle, request.scope)
        if (
            is_root
            and cycle.configuration.child_record_types
            and cycle.root_writer_attempt_id != request.fence.attempt_id
        ):
            raise CycleConflict("Root membership belongs to an earlier writer attempt")
        row = self._expect(
            await self._row(db, request.fence, request.scope), request.expected, request.cycle_id
        )
        if row.phase != "collecting":
            raise ScanConflict("Scan is not collecting pages")
        if any(
            record.identity.record_type != row.record_type
            or record.identity.container_id != row.container_id
            for record in request.records
        ):
            raise ScanConflict("Page contains records outside its exact scope")
        if not is_root:
            parent = RecordIdentity(
                record_type=cycle.configuration.root_record_type,
                native_id=request.scope.container_id,
            )
            if any(record.parent != parent for record in request.records):
                raise ScanConflict("Child records must retain their declared parent identity")
        if is_root and cycle.configuration.child_record_types:
            await self._invalidate_revived_children(db, request, cycle)
        captured = await self.records._capture_locked(
            db,
            sync,
            CaptureBatch(fence=request.fence, records=request.records),
            seen_id=row.sweep_id,
        )
        row.continuation = request.continuation.value
        row.revision += 1
        if request.final:
            row.phase = "reconciling"
        await db.flush()
        return ScanResult(state=scan_state(row), capture=captured)

    async def _invalidate_revived_children(
        self,
        db: AsyncSession,
        request: CommitScanPage,
        cycle: CaptureCycle,
    ) -> None:
        """Reviving an unavailable parent requires a fresh child scan in this same transaction."""
        upserts = tuple(
            record.identity.native_id for record in request.records if record.kind == "upsert"
        )
        if not upserts:
            return
        revived = tuple(
            (
                await db.scalars(
                    select(Entity.native_id).where(
                        Entity.organization_id == request.fence.organization_id,
                        Entity.sync_id == request.fence.sync_id,
                        Entity.entity_definition_short_name == cycle.configuration.root_record_type,
                        Entity.container_id.is_(None),
                        Entity.native_id.in_(upserts),
                        or_(Entity.deleted_at.is_not(None), ~content_is_available()),
                    )
                )
            ).all()
        )
        if not revived:
            return
        rows = (
            await db.scalars(
                select(CaptureScan)
                .where(
                    CaptureScan.organization_id == request.fence.organization_id,
                    CaptureScan.sync_id == request.fence.sync_id,
                    CaptureScan.cycle_id == request.cycle_id,
                    CaptureScan.record_type.in_(cycle.configuration.child_record_types),
                    CaptureScan.container_id.in_(revived),
                )
                .with_for_update()
            )
        ).all()
        for child in rows:
            child.sweep_id = uuid4()
            child.revision += 1
            child.phase = "collecting"
            child.continuation = {}
            child.started_at = datetime.now(timezone.utc)
            child.completed_at = None

    async def reconcile(self, db: AsyncSession, request: ReconcileScan) -> ScanResult:
        """A final page is necessary; absence completion is durable and bounded."""
        sync = await self.records._fenced_sync(db, request.fence)
        _, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        is_root = await attest_scope(db, request.fence, cycle, request.scope)
        if (
            is_root
            and cycle.configuration.child_record_types
            and cycle.root_writer_attempt_id != request.fence.attempt_id
        ):
            raise CycleConflict("Root membership belongs to an earlier writer attempt")
        row = self._expect(
            await self._row(db, request.fence, request.scope), request.expected, request.cycle_id
        )
        if row.phase != "reconciling":
            raise ScanConflict("Only fully collected scans can reconcile absence")
        result = await self.records._reconcile_scope_locked(
            db,
            sync,
            ReconcileScope(
                fence=request.fence,
                scope=request.scope,
                removal_reason=(
                    "scope_removed"
                    if is_root and cycle.configuration.child_record_types
                    else request.removal_reason
                ),
                observed_at=request.observed_at,
                limit=request.limit,
            ),
            seen_id=row.sweep_id,
        )
        row.revision += 1
        if not result.has_more:
            row.phase = "complete"
            row.completed_at = datetime.now(timezone.utc)
        await db.flush()
        return ScanResult(state=scan_state(row), capture=result.capture)
