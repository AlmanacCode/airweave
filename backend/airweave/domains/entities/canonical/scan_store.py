"""Scan persistence inside the existing capture transaction; no provider I/O or commits."""

import json
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.cycle_models import (
    TERMINAL_CHECKPOINT_KEY,
    CaptureCycle,
    CaptureMode,
    CycleVersion,
    ProviderCheckpoint,
)
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
    ScanAdmission,
    ScanContinuation,
    ScanResult,
    ScanState,
    ScanVersion,
)
from airweave.domains.entities.canonical.scope_execution import (
    ScopeEvidence,
    ScopeExecution,
    ScopePlan,
)
from airweave.domains.entities.canonical.store import (
    CanonicalRecordStore,
    CanonicalStoreError,
    content_is_available,
    source_record,
)
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync import Sync
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
        continuation=ScanContinuation(
            value={
                key: value
                for key, value in row.continuation.items()
                if key != TERMINAL_CHECKPOINT_KEY
            }
        ),
        provider_checkpoint=ProviderCheckpoint.model_validate(
            row.continuation[TERMINAL_CHECKPOINT_KEY]
        )
        if TERMINAL_CHECKPOINT_KEY in row.continuation
        else None,
        started_at=row.started_at,
        completed_at=row.completed_at,
        parent_visibility_epoch=row.parent_visibility_epoch,
        membership_attempt_id=row.membership_attempt_id,
        parent_verified_attempt_id=row.parent_verified_attempt_id,
        parent_verified_revision=row.parent_verified_revision,
        execution=ScopeExecution.model_validate(row.execution_state)
        if row.execution_state
        else None,
    )


def page_continuation(request: CommitScanPage, mode: CaptureMode) -> dict[str, JsonValue]:
    """The terminal boundary commits with the records it certifies, never in a later call."""
    if request.provider_checkpoint is not None and not request.final:
        raise ScanConflict("Only a terminal page may certify a provider checkpoint")
    if mode == "changes" and request.final and request.provider_checkpoint is None:
        raise ScanConflict("Changes terminal page requires a provider checkpoint")
    if mode == "changes" and any(record.removal_reason == "absent" for record in request.records):
        raise ScanConflict("Changes require explicit deletion evidence, never absence")
    continuation = dict(request.continuation.value)
    if request.provider_checkpoint is not None:
        continuation[TERMINAL_CHECKPOINT_KEY] = request.provider_checkpoint.model_dump(mode="json")
    return continuation


def effective_mode(row: CaptureScan, cycle: CaptureCycle) -> str:
    """A mixed cycle never supplies a default cleanup mode for its scopes."""
    if cycle.mode != "mixed":
        return cycle.mode
    if row.execution_state is None and row.cycle_id != cycle.version.cycle_id:
        return "full"  # Legacy evidence is unknown; prepare a new full scope.
    if row.execution_state is None:
        raise ScanConflict("Mixed scope is missing its immutable execution plan")
    return ScopeExecution.model_validate(row.execution_state).plan.mode


def publish_scope(row: CaptureScan, cycle: CaptureCycle, sequence: int, attempt_id: UUID) -> None:
    """Completed evidence is derived only from this row's committed terminal state."""
    if row.execution_state is None or row.phase != "complete":
        return
    execution = ScopeExecution.model_validate(row.execution_state)
    checkpoint = row.continuation.get(TERMINAL_CHECKPOINT_KEY)
    evidence = ScopeEvidence(
        cycle_id=row.cycle_id,
        sweep_id=row.sweep_id,
        completed_at=row.completed_at,
        parent_visibility_epoch=row.parent_visibility_epoch,
        request_context=execution.plan.request_context,
        policy=cycle.configuration.policy(row.record_type),
        observed_change_sequence=sequence,
        writer_attempt_id=attempt_id,
        checkpoint=ProviderCheckpoint.model_validate(checkpoint) if checkpoint else None,
    )
    row.execution_state = execution.model_copy(
        update={
            "published": evidence,
            "last_full": evidence if execution.plan.mode == "full" else execution.last_full,
        }
    ).model_dump(mode="json")


def begin_execution(
    row: CaptureScan, cycle: CaptureCycle, request: BeginScan, epoch: int | None
) -> None:
    """Previous completion survives replacement; mismatching owner/request invalidates it."""
    if cycle.mode != "mixed":
        if request.plan is None:
            row.execution_state = None
            return
        if (
            cycle.mode != "full"
            or request.plan.mode != "full"
            or request.plan.starting_checkpoint is not None
        ):
            raise ScanConflict("Full capture scope plans cannot request incremental execution")
    plan = request.plan
    if plan is None:
        raise ScanConflict("Mixed capture requires an explicit scope plan")
    previous = (
        ScopeExecution.model_validate(row.execution_state)
        if row.execution_state and row.fingerprint == cycle.configuration.fingerprint
        else None
    )
    full = previous.last_full if previous else None
    published = previous.published if previous else None
    policy = cycle.configuration.policy(request.scope.record_type)
    if full and (not full.matches(plan, epoch) or full.policy != policy):
        full = None
    if published and (not published.matches(plan, epoch) or published.policy != policy):
        published = None
    validate_scope_changes(cycle, request.scope.record_type, plan, full, published)
    row.execution_state = ScopeExecution(plan=plan, last_full=full, published=published).model_dump(
        mode="json"
    )


def validate_scope_changes(
    cycle: CaptureCycle,
    kind: str,
    plan: ScopePlan,
    full: ScopeEvidence | None,
    published: ScopeEvidence | None,
) -> None:
    """Only compatible completed evidence authorizes an incremental scope."""
    if plan.mode == "changes":
        if cycle.force_full_scopes:
            raise ScanConflict("This cycle requires full scope scans")
        if kind not in cycle.configuration.scope_changes:
            raise ScanConflict("Scope kind does not permit native changes")
        if full is None or published is None or published.checkpoint is None:
            raise ScanConflict("Scope changes require compatible full evidence and checkpoint")
        if plan.starting_checkpoint != published.checkpoint:
            raise ScanConflict("Scope changes must start at its last published checkpoint")


def attest_planned_owner(request: BeginScan, parent: Entity | None) -> None:
    """The post-I/O plan must still describe the captured owner used to select it."""
    if request.expected_parent_epoch != (parent.visibility_epoch if parent else None) or (
        (request.plan is not None or request.exact_parent_observation is not None)
        and request.expected_parent_revision != (parent.record_revision if parent else None)
    ):
        raise ScanConflict("Scope owner changed during plan selection")


def preserve_owner_verification(
    row: CaptureScan, request: BeginScan, parent: Entity | None
) -> None:
    """A cursor restart preserves only a receipt still bound to this exact owner."""
    keep_verification = bool(
        parent
        and row.cycle_id == request.cycle_id
        and row.parent_visibility_epoch == parent.visibility_epoch
        and row.parent_verified_attempt_id == request.fence.attempt_id
        and row.parent_verified_revision == parent.record_revision
    )
    if not keep_verification:
        row.parent_verified_attempt_id = None
        row.parent_verified_revision = None


class LockedPageCapture(Protocol):
    """Internal composition: capture the entire page under the caller's writer lock.

    Implementations must not commit. This is code-owned behavior, never HTTP data.
    The surrounding scan transaction owns scope checks and continuation advancement.
    """

    async def __call__(
        self, db: AsyncSession, sync: Sync, batch: CaptureBatch, *, seen_id: UUID
    ) -> CaptureResult:
        """Capture changes and sightings atomically using the existing sweep identity."""
        ...


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
                else "exhaustive",
                "mode": effective_mode(row, cycle) if cycle else "full",
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

    async def admit(self, db: AsyncSession, request: BeginScan) -> ScanAdmission:
        """Commit fresh owner state and its child receipt under the existing writer fence."""
        sync = await self.records._fenced_sync(db, request.fence)
        cursor, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        await attest_scope(
            db,
            request.fence,
            cycle,
            request.scope,
            require_owner_receipt=request.exact_parent_observation is None,
        )
        parent = await scope_owner(db, request.fence, cycle, request.scope)
        observation = request.exact_parent_observation
        empty = CaptureResult(changes=(), sequence=sync.observed_change_sequence, unchanged=0)
        if observation is None:
            state = await self._begin(db, request, context=(cursor, cycle, parent))
            return ScanAdmission(
                state=state, parent=source_record(parent) if parent else None, capture=empty
            )
        if (
            parent is None
            or parent.entity_definition_short_name
            not in cycle.configuration.exact_parent_validation
        ):
            raise ScanConflict("Scope does not accept exact parent verification")
        attest_planned_owner(request, parent)
        if request.fingerprint != cycle.configuration.fingerprint:
            raise CycleConflict("Scan configuration differs from the active cycle")
        row = await self._row(db, request.fence, request.scope)
        if row is None:
            if request.expected is not None:
                raise ScanConflict("Verification expected an existing scope")
        elif request.expected is None:
            raise ScanConflict("Verification requires the current scope version")
        else:
            self._expect(row, request.expected, row.cycle_id)
        previous = source_record(parent)
        if (
            observation.identity != previous.identity
            or observation.parent != previous.parent
            or observation.allow_reparent
            or observation.removal_reason == "absent"
        ):
            raise ScanConflict("Exact parent observation changed identity or claimed absence")
        captured = await self.records._capture_locked(
            db,
            sync,
            CaptureBatch(fence=request.fence, records=(observation,)),
            mark_seen=False,
        )
        await db.refresh(parent)
        available = await db.scalar(select(content_is_available()).where(Entity.id == parent.id))
        if parent.deleted_at is not None or not available:
            return ScanAdmission(state=None, parent=source_record(parent), capture=captured)
        state = await self._begin_verified(db, request, context=(cursor, cycle, parent))
        return ScanAdmission(state=state, parent=source_record(parent), capture=captured)

    async def _begin_verified(
        self,
        db: AsyncSession,
        request: BeginScan,
        *,
        context: tuple[SyncCursor, CaptureCycle, Entity],
    ) -> ScanState:
        """Share child admission after a fenced native owner observation is captured."""
        cursor, cycle, parent = context
        row = await self._row(db, request.fence, request.scope)
        updated = request.model_copy(
            update={
                "expected_parent_epoch": parent.visibility_epoch,
                "expected_parent_revision": parent.record_revision,
                "restart": request.restart
                or bool(row and row.parent_visibility_epoch != parent.visibility_epoch),
            }
        )
        await self._begin(db, updated, context=(cursor, cycle, parent))
        row = await self._row(db, request.fence, request.scope)
        row.parent_verified_attempt_id = request.fence.attempt_id
        row.parent_verified_revision = parent.record_revision
        row.revision += 1
        await db.flush()
        return await self._state(db, row)

    async def begin(self, db: AsyncSession, request: BeginScan) -> ScanState:
        """Ordinary admission cannot accept uncommitted owner observations."""
        if request.exact_parent_observation is not None:
            raise ScanConflict("Exact parent observations require typed scan admission")
        return await self._begin(db, request)

    async def _begin_context(
        self,
        db: AsyncSession,
        request: BeginScan,
    ) -> tuple[SyncCursor, CaptureCycle, Entity | None]:
        """Ordinary entry obtains the same authority checks as exact admission."""
        await self.records._fenced_sync(db, request.fence)
        cursor, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        await attest_scope(db, request.fence, cycle, request.scope)
        parent = await scope_owner(db, request.fence, cycle, request.scope)
        return cursor, cycle, parent

    async def _begin(
        self,
        db: AsyncSession,
        request: BeginScan,
        *,
        context: tuple[SyncCursor, CaptureCycle, Entity | None] | None = None,
    ) -> ScanState:
        """Resume unchanged scans; reuse admission checks held under the same writer lock."""
        cursor, cycle, parent = context or await self._begin_context(db, request)
        attest_planned_owner(request, parent)
        inventory = cycle.configuration.fresh_inventory(request.scope.record_type)
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
                if request.plan is not None and (
                    not row.execution_state
                    or ScopeExecution.model_validate(row.execution_state).plan != request.plan
                ):
                    raise ScanConflict("Active scope plan changed; explicit restart required")
                return await self._state(db, row)
            if request.expected is None:
                raise ScanConflict("Replacing a scan requires its current version")
            if row.cycle_id == request.cycle_id and not request.restart:
                raise ScanConflict("Changed scope configuration requires an explicit restart")
            row.revision += 1
        begin_execution(row, cycle, request, parent.visibility_epoch if parent else None)
        preserve_owner_verification(row, request, parent)
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

    @staticmethod
    def _validate_child_observations(request: CommitScanPage, cycle: CaptureCycle) -> None:
        """Only exact active owners in this native page can attest child inventories."""
        if not request.child_scope_observations:
            return
        if cycle.mode != "full":
            raise ScanConflict("Page child observations require a full capture cycle")
        records = {record.identity: record for record in request.records}
        seen = set()
        for observation in request.child_scope_observations:
            scope = observation.scope
            parent = records.get(scope.parent)
            if (
                parent is None
                or parent.kind != "upsert"
                or parent.allow_reparent
                or parent.removal_reason is not None
                or parent.identity.record_type not in cycle.configuration.exact_parent_validation
                or parent.identity.record_type
                not in cycle.configuration.parents.get(scope.record_type, ())
            ):
                raise ScanConflict(
                    "Child observation requires an allowed current-page upsert owner"
                )
            key = (scope.parent, scope.record_type)
            if key in seen:
                raise ScanConflict("Duplicate child observation for the same owner and kind")
            seen.add(key)

    async def _observe_child_scopes(self, db: AsyncSession, request: CommitScanPage) -> None:
        """Derive existing child receipts inside the page's atomic writer transaction."""
        for observation in request.child_scope_observations:
            # _begin may advance the cycle version; never reuse an earlier snapshot.
            cursor, cycle = await attest_cycle(db, request.fence, request.cycle_id)
            scope = observation.scope
            await attest_scope(db, request.fence, cycle, scope, require_owner_receipt=False)
            parent = await scope_owner(db, request.fence, cycle, scope)
            if parent is None or parent.deleted_at is not None:
                raise ScanConflict("Child observation owner is unavailable")
            previous = await self._row(db, request.fence, scope)
            request_begin = BeginScan(
                fence=request.fence,
                scope=scope,
                cycle_id=request.cycle_id,
                fingerprint=cycle.configuration.fingerprint,
                expected=(
                    ScanVersion(sweep_id=previous.sweep_id, revision=previous.revision)
                    if previous is not None
                    else None
                ),
                restart=bool(
                    previous
                    and previous.cycle_id == request.cycle_id
                    and cycle.configuration.fresh_inventory(scope.record_type)
                    and previous.membership_attempt_id != request.fence.attempt_id
                ),
                expected_parent_epoch=parent.visibility_epoch,
                expected_parent_revision=parent.record_revision,
                continuation=observation.continuation,
            )
            await self._begin_verified(db, request_begin, context=(cursor, cycle, parent))

    async def page(
        self,
        db: AsyncSession,
        request: CommitScanPage,
        *,
        capture_page: LockedPageCapture | None = None,
    ) -> ScanResult:
        """Capture and advance the page as one transaction, never a partial acknowledgement."""
        sync = await self.records._fenced_sync(db, request.fence)
        _, cycle = await attest_cycle(db, request.fence, request.cycle_id)
        await attest_scope(db, request.fence, cycle, request.scope)
        row = self._expect(
            await self._row(db, request.fence, request.scope), request.expected, request.cycle_id
        )
        mode = effective_mode(row, cycle)
        continuation = page_continuation(request, mode)
        parent = await scope_owner(db, request.fence, cycle, request.scope)
        if row.parent_visibility_epoch != (parent.visibility_epoch if parent else None):
            raise ScanConflict("Scope owner changed; restart from its current epoch")
        if (
            cycle.configuration.fresh_inventory(row.record_type)
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
        combined = (*request.records, *request.discovered_records)
        identities = {
            (record.identity.record_type, record.identity.entity_key) for record in combined
        }
        if len(combined) > 500 or len(identities) != len(combined):
            raise ScanConflict("A page must contain at most 500 distinct original identities")
        if any(
            record.identity.record_type not in cycle.configuration.root_record_types
            or cycle.configuration.policy(record.identity.record_type) == "exhaustive"
            or record.parent is not None
            or record.identity.container_id is not None
            or record.kind != "upsert"
            or record.allow_reparent
            or not record.payload
            for record in request.discovered_records
        ):
            raise ScanConflict("Discovered originals must be verified independent declared roots")
        self._validate_child_observations(request, cycle)
        capture = capture_page or self.records._capture_locked
        captured = await capture(
            db,
            sync,
            CaptureBatch(fence=request.fence, records=request.records),
            seen_id=row.sweep_id,
        )
        discovered = await self.records._capture_locked(
            db,
            sync,
            CaptureBatch(fence=request.fence, records=request.discovered_records),
            mark_seen=False,
        )
        captured = CaptureResult(
            changes=(*captured.changes, *discovered.changes),
            sequence=discovered.sequence,
            unchanged=captured.unchanged + discovered.unchanged,
        )
        await self._observe_child_scopes(db, request)
        row.continuation = continuation
        row.revision += 1
        if request.final:
            row.phase = "complete" if mode == "changes" else "reconciling"
            row.completed_at = datetime.now(timezone.utc) if mode == "changes" else None
            publish_scope(row, cycle, sync.observed_change_sequence, request.fence.attempt_id)
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
        if cycle.mode == "changes":
            raise ScanConflict("Changes scans cannot reconcile or validate enumeration absence")
        await attest_scope(db, fence, cycle, state.scope)
        row = self._expect(await self._row(db, fence, state.scope), state.version, state.cycle_id)
        if effective_mode(row, cycle) == "changes":
            raise ScanConflict("Changes scans cannot validate enumeration absence")
        parent = await scope_owner(db, fence, cycle, state.scope)
        if (
            row.phase != "reconciling"
            or (
                cycle.configuration.fresh_inventory(row.record_type)
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
        if (
            cycle.configuration.policy(row.record_type) != "discovery_with_validation"
            and row.record_type not in cycle.configuration.known_object_validation
        ):
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
        if cycle.mode == "changes":
            raise ScanConflict("Changes scans cannot reconcile or validate enumeration absence")
        if (
            cycle.configuration.policy(request.state.scope.record_type)
            != "discovery_with_validation"
            and request.state.scope.record_type not in cycle.configuration.known_object_validation
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
        if cycle.mode == "changes":
            raise ScanConflict("Changes scans cannot reconcile or validate enumeration absence")
        await attest_scope(db, request.fence, cycle, request.scope)
        row = self._expect(
            await self._row(db, request.fence, request.scope), request.expected, request.cycle_id
        )
        if effective_mode(row, cycle) == "changes":
            raise ScanConflict("Changes scans cannot reconcile enumeration absence")
        parent = await scope_owner(db, request.fence, cycle, request.scope)
        if row.parent_visibility_epoch != (parent.visibility_epoch if parent else None):
            raise ScanConflict("Scope owner changed; restart from its current epoch")
        if (
            cycle.configuration.fresh_inventory(row.record_type)
            and row.membership_attempt_id != request.fence.attempt_id
        ):
            raise CycleConflict("Membership belongs to an earlier writer attempt")
        if row.phase != "reconciling":
            raise ScanConflict("Only fully collected scans can reconcile absence")
        policy = cycle.configuration.policy(row.record_type)
        if (
            policy == "discovery_with_validation"
            or row.record_type in cycle.configuration.known_object_validation
        ) and await self.missing(db, request.fence, await self._state(db, row)):
            raise ScanConflict("Known objects still require exact validation")
        if policy != "exhaustive":
            row.phase = "complete"
            row.completed_at = datetime.now(timezone.utc)
            row.revision += 1
            publish_scope(row, cycle, sync.observed_change_sequence, request.fence.attempt_id)
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
                    if cycle.configuration.fresh_inventory(row.record_type)
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
            publish_scope(row, cycle, sync.observed_change_sequence, request.fence.attempt_id)
        await db.flush()
        return ScanResult(state=await self._state(db, row), capture=result.capture)
