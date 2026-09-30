"""Scan persistence inside the existing capture transaction; no provider I/O or commits."""

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import CycleVersion
from airweave.domains.entities.canonical.cycle_store import (
    CycleConflict,
    attest_cycle,
    attest_scope,
    cursor_row,
    cycle_state,
    persist,
    scope_owner,
)
from airweave.domains.entities.canonical.models import CaptureResult, SourceRecord
from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CaptureRecord,
    CompletedScope,
    ReconcileScope,
    ScopeRemovalReason,
    WriterFence,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitOmission,
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
    source_record,
)
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor


class ScanConflict(CanonicalStoreError):
    """Reload durable progress before performing more provider I/O."""

    code = "scan_conflict"


def scope_key(scope: CompletedScope, parent_id: UUID | None = None) -> str:
    """Distinguish null containers from empty strings without a nullable unique key."""
    key = json.dumps(
        [scope.record_type, scope.container_id], ensure_ascii=False, separators=(",", ":")
    )
    return key if parent_id is None else key + "|" + str(parent_id)


def scan_state(row: CaptureScan, parent: Entity | None = None) -> ScanState:
    """Do not leak ORM state outside its transaction."""
    return ScanState(
        scope=CompletedScope(
            record_type=row.record_type,
            container_id=row.container_id,
            parent=source_record(parent).identity if parent is not None else None,
        ),
        cycle_id=row.cycle_id,
        version=ScanVersion(sweep_id=row.sweep_id, revision=row.revision),
        phase=row.phase,
        fingerprint=row.fingerprint,
        continuation=ScanContinuation(value=row.continuation),
        started_at=row.started_at,
        completed_at=row.completed_at,
        parent_visibility_epoch=row.parent_visibility_epoch,
        membership_attempt_id=row.membership_attempt_id,
    )


class CanonicalScanStore:
    """Reuse record capture, lock ordering and journal semantics for recoverable scans."""

    def __init__(self, records: CanonicalRecordStore):
        """The existing record store remains the only authority for record mutations."""
        self.records = records

    async def _row(
        self, db: AsyncSession, fence: WriterFence, scope: CompletedScope
    ) -> CaptureScan | None:
        cycle = cycle_state(await cursor_row(db, fence))
        if cycle is None:
            raise CycleConflict("Scan requires a persisted cycle")
        parent = await scope_owner(db, fence, cycle, scope)
        predicates = [
            CaptureScan.organization_id == fence.organization_id,
            CaptureScan.sync_id == fence.sync_id,
        ]
        if parent is None:
            predicates.append(CaptureScan.scope_key == scope_key(scope))
        else:
            predicates.extend(
                (
                    CaptureScan.parent_record_id == parent.id,
                    CaptureScan.record_type == scope.record_type,
                )
            )
        row = await db.scalar(
            select(CaptureScan)
            .where(*predicates)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if row is not None and row.container_id != scope.container_id:
            raise CycleConflict(
                "Established scope identity mapping changed; explicit migration required"
            )
        return row

    async def _state(self, db: AsyncSession, row: CaptureScan) -> ScanState:
        parent = await db.get(Entity, row.parent_record_id) if row.parent_record_id else None
        if row.parent_record_id is not None and parent is None:
            raise CycleConflict("Scope parent disappeared; explicitly restart the cycle")
        cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == row.sync_id))
        cycle = cycle_state(cursor) if cursor else None
        return scan_state(row, parent).model_copy(
            update={
                "completion_policy": cycle.configuration.policy(row.record_type)
                if cycle
                else "exhaustive"
            }
        )

    async def read(
        self, db: AsyncSession, fence: WriterFence, scope: CompletedScope
    ) -> ScanState | None:
        """Read only under the active writer fence, including after an uncertain commit."""
        await self.records._fenced_sync(db, fence)
        row = await self._row(db, fence, scope)
        return await self._state(db, row) if row else None

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

    @staticmethod
    def _check_parent_epoch(row: CaptureScan, parent: Entity | None) -> None:
        if row.parent_visibility_epoch != (parent.visibility_epoch if parent else None):
            raise ScanConflict("Scope owner changed; restart its sweep with current CAS")

    async def begin(self, db: AsyncSession, request: BeginScan) -> ScanState:
        """Resume unchanged scans; restart and next-cycle transitions require exact CAS."""
        await self.records._fenced_sync(db, request.fence)
        cursor, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        await attest_scope(db, request.fence, cycle, request.scope)
        parent = await scope_owner(db, request.fence, cycle, request.scope)
        inventory = bool(cycle.configuration.children_of(request.scope.record_type))
        if request.fingerprint != cycle.configuration.fingerprint:
            raise CycleConflict("Scan configuration differs from the active cycle")
        row = await self._row(db, request.fence, request.scope)
        if (
            row is not None
            and row.cycle_id == request.cycle_id
            and inventory
            and row.membership_attempt_id != request.fence.attempt_id
            and not request.restart
        ):
            raise CycleConflict("Restart root or ancestor membership for this writer attempt")
        if row is None:
            if request.expected is not None or request.restart:
                raise ScanConflict("Cannot restart a missing scan")
            row = CaptureScan(
                organization_id=request.fence.organization_id,
                sync_id=request.fence.sync_id,
                scope_key=scope_key(request.scope, parent.id if parent is not None else None),
                parent_record_id=parent.id if parent is not None else None,
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
                self._check_parent_epoch(row, parent)
                return await self._state(db, row)
            if request.expected is None:
                raise ScanConflict("Replacing a scan requires its current version")
            if row.cycle_id == request.cycle_id and not request.restart:
                raise ScanConflict("Changed scope configuration requires an explicit restart")
            row.revision += 1
        row.parent_visibility_epoch = parent.visibility_epoch if parent else None
        row.membership_attempt_id = request.fence.attempt_id if inventory else None
        row.cycle_id = request.cycle_id
        row.sweep_id = uuid4()
        row.fingerprint = request.fingerprint
        row.phase = "collecting"
        row.continuation = request.continuation.value
        row.started_at = datetime.now(timezone.utc)
        row.completed_at = None
        persist(
            cursor,
            cycle.model_copy(
                update={
                    "version": CycleVersion(
                        cycle_id=cycle.version.cycle_id,
                        revision=cycle.version.revision + 1,
                    )
                }
            ),
        )
        await db.flush()
        return await self._state(db, row)

    async def page(self, db: AsyncSession, request: CommitScanPage) -> ScanResult:
        """Capture and advance the page as one transaction, never a partial acknowledgement."""
        sync = await self.records._fenced_sync(db, request.fence)
        _, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        await attest_scope(db, request.fence, cycle, request.scope)
        row = self._expect(
            await self._row(db, request.fence, request.scope), request.expected, request.cycle_id
        )
        parent = await scope_owner(db, request.fence, cycle, request.scope)
        if row.parent_visibility_epoch != (parent.visibility_epoch if parent else None):
            raise ScanConflict("Scope owner changed; restart from its current epoch")
        if (
            cycle.configuration.children_of(row.record_type)
            and row.membership_attempt_id != request.fence.attempt_id
        ):
            raise CycleConflict("Membership belongs to an earlier writer attempt")
        if row.phase != "collecting":
            raise ScanConflict("Scan is not collecting pages")
        if any(
            record.identity.record_type != row.record_type
            or record.identity.container_id != row.container_id
            for record in request.records
        ):
            raise ScanConflict("Page contains records outside its exact scope")
        expected_parent = source_record(parent).identity if parent is not None else None
        if any(record.parent != expected_parent for record in request.records):
            raise ScanConflict("Page records must retain their exact declared parent identity")
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
        return ScanResult(state=await self._state(db, row), capture=captured)

    async def withdraw_parent(
        self,
        db: AsyncSession,
        fence: WriterFence,
        parent: SourceRecord,
        *,
        expected_epoch: int,
        removal_reason: ScopeRemovalReason,
    ) -> CaptureResult:
        """A failed old provider route cannot withdraw a renamed or revived current owner."""
        sync = await self.records._fenced_sync(db, fence)
        current = await db.scalar(
            select(Entity)
            .where(
                Entity.id == parent.id,
                Entity.organization_id == fence.organization_id,
                Entity.sync_id == fence.sync_id,
            )
            .execution_options(populate_existing=True)
        )
        if (
            parent.sync_id != fence.sync_id
            or current is None
            or current.record_revision != parent.revision
            or current.visibility_epoch != expected_epoch
            or source_record(current).identity != parent.identity
        ):
            raise ScanConflict("Scope owner changed during provider access check; reload work")
        observation = CaptureRecord(
            identity=parent.identity,
            parent=parent.parent,
            payload=parent.payload,
            payload_schema_version=parent.payload_schema_version,
            completeness=parent.completeness,
            content_hash=parent.content_hash,
            source_created_at=parent.source_created_at,
            source_updated_at=parent.source_updated_at,
            blobs=parent.blobs,
            kind="delete",
            removal_reason=removal_reason,
            observed_at=datetime.now(timezone.utc),
        )
        return await self.records._capture_locked(
            db, sync, CaptureBatch(fence=fence, records=(observation,))
        )

    async def missing(
        self, db: AsyncSession, fence: WriterFence, state: ScanState, *, after: UUID | None = None
    ) -> tuple[SourceRecord, ...]:
        """Bounded exact inventory omissions, only after current-attempt collection."""
        await self.records._fenced_sync(db, fence)
        _, cycle = await attest_cycle(db, fence, state.cycle_id)
        await attest_scope(db, fence, cycle, state.scope)
        row = self._expect(await self._row(db, fence, state.scope), state.version, state.cycle_id)
        parent = await scope_owner(db, fence, cycle, state.scope)
        if (
            row.phase != "reconciling"
            or (
                cycle.configuration.children_of(row.record_type)
                and row.membership_attempt_id != fence.attempt_id
            )
            or row.parent_visibility_epoch != (parent.visibility_epoch if parent else None)
        ):
            raise CycleConflict("Omission checks require completed current-attempt inventory")
        identity = source_record(parent).identity if parent else None
        predicates = [
            Entity.organization_id == fence.organization_id,
            Entity.sync_id == fence.sync_id,
            Entity.entity_definition_short_name == row.record_type,
            Entity.container_id.is_not_distinct_from(row.container_id),
            Entity.parent_record_type.is_not_distinct_from(
                identity.record_type if identity else None
            ),
            Entity.parent_native_id.is_not_distinct_from(identity.native_id if identity else None),
            Entity.parent_container_id.is_not_distinct_from(
                identity.container_id if identity else None
            ),
            Entity.record_revision > 0,
            Entity.last_seen_run_id.is_distinct_from(row.sweep_id),
        ]
        if cycle.configuration.policy(row.record_type) != "discovery_with_validation":
            predicates.append(Entity.deleted_at.is_(None))
        if after is not None:
            predicates.append(Entity.id > after)
        records = (
            await db.scalars(select(Entity).where(*predicates).order_by(Entity.id).limit(100))
        ).all()
        return tuple(source_record(record) for record in records)

    async def omission(self, db: AsyncSession, request: CommitOmission) -> ScanResult:
        """Commit exact known-object validation with its sweep acknowledgement."""
        sync = await self.records._fenced_sync(db, request.fence)
        _, cycle = await attest_cycle(db, request.fence, request.state.cycle_id)
        if (
            cycle.configuration.policy(request.state.scope.record_type)
            != "discovery_with_validation"
        ):
            raise ScanConflict("This scope does not accept known-object validation")
        candidates = await self.missing(db, request.fence, request.state)
        previous = request.expected_record
        current = next((record for record in candidates if record.id == previous.id), None)
        if (
            current is None
            or current.revision != previous.revision
            or current.identity != previous.identity
        ):
            raise ScanConflict("Known object changed or is outside the next validation batch")
        observation = request.observation
        if observation.removal_reason == "absent":
            raise ScanConflict("Discovery requires explicit unavailability, never absence")
        if observation.identity != current.identity or observation.parent != current.parent:
            raise ScanConflict("Known-object validation changed identity or owner")
        row = self._expect(
            await self._row(db, request.fence, request.state.scope),
            request.state.version,
            request.state.cycle_id,
        )
        captured = await self.records._capture_locked(
            db,
            sync,
            CaptureBatch(fence=request.fence, records=(observation,)),
            seen_id=row.sweep_id,
        )
        row.revision += 1
        await db.flush()
        return ScanResult(state=await self._state(db, row), capture=captured)

    async def reconcile(self, db: AsyncSession, request: ReconcileScan) -> ScanResult:
        """A final page is necessary; absence completion is durable and bounded."""
        sync = await self.records._fenced_sync(db, request.fence)
        _, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        await attest_scope(db, request.fence, cycle, request.scope)
        row = self._expect(
            await self._row(db, request.fence, request.scope), request.expected, request.cycle_id
        )
        parent = await scope_owner(db, request.fence, cycle, request.scope)
        if row.parent_visibility_epoch != (parent.visibility_epoch if parent else None):
            raise ScanConflict("Scope owner changed; restart from its current epoch")
        if (
            cycle.configuration.children_of(row.record_type)
            and row.membership_attempt_id != request.fence.attempt_id
        ):
            raise CycleConflict("Membership belongs to an earlier writer attempt")
        if row.phase != "reconciling":
            raise ScanConflict("Only fully collected scans can reconcile absence")
        policy = cycle.configuration.policy(row.record_type)
        if policy != "exhaustive":
            if policy == "discovery_with_validation" and await self.missing(
                db, request.fence, await self._state(db, row)
            ):
                raise ScanConflict("Known objects still require exact validation")
            row.phase = "complete"
            row.completed_at = datetime.now(timezone.utc)
            row.revision += 1
            await db.flush()
            return ScanResult(
                state=await self._state(db, row),
                capture=CaptureResult(
                    changes=(), sequence=sync.observed_change_sequence, unchanged=0
                ),
            )
        result = await self.records._reconcile_scope_locked(
            db,
            sync,
            ReconcileScope(
                fence=request.fence,
                scope=(await self._state(db, row)).scope,
                removal_reason=(
                    "scope_removed"
                    if cycle.configuration.children_of(row.record_type)
                    else request.removal_reason
                ),
                observed_at=request.observed_at,
                limit=request.limit,
            ),
            seen_id=row.sweep_id,
            parent_scoped=True,
        )
        row.revision += 1
        if not result.has_more:
            row.phase = "complete"
            row.completed_at = datetime.now(timezone.utc)
        await db.flush()
        return ScanResult(state=await self._state(db, row), capture=result.capture)
