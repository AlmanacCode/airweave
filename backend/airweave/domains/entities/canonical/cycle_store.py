"""Cycle ownership on the existing sync cursor; callers already hold the Sync fence."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from airweave.domains.entities.canonical.checkpoint import CanonicalCheckpoint
from airweave.domains.entities.canonical.cycle_models import (
    CYCLE_KEY,
    TERMINAL_CHECKPOINT_KEY,
    BeginCycle,
    CaptureCycle,
    CompleteCycle,
    CycleVersion,
    FullCaptureEvidence,
    PromotedCheckpoint,
    ProviderCheckpoint,
    RestartCycle,
    ScopeWork,
)
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity, WriterFence
from airweave.domains.entities.canonical.store import (
    CanonicalStoreError,
    ancestor_chain,
    content_is_available,
    source_record,
)
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor


class CycleConflict(CanonicalStoreError):
    """Reload the active cycle; do not guess recovery or erase engine state."""

    code = "cycle_conflict"


async def cursor_row(db: AsyncSession, fence: WriterFence) -> SyncCursor | None:
    """The caller holds the same sync lock as all capture mutations."""
    return await db.scalar(
        select(SyncCursor)
        .where(
            SyncCursor.sync_id == fence.sync_id,
            SyncCursor.organization_id == fence.organization_id,
        )
        .execution_options(populate_existing=True)
    )


def cycle_state(cursor: SyncCursor | None) -> CaptureCycle | None:
    """Reject malformed state rather than falling back to a new cycle."""
    if cursor is None or CYCLE_KEY not in cursor.cursor_data:
        return None
    return CaptureCycle.model_validate(cursor.cursor_data[CYCLE_KEY])


def persist(cursor: SyncCursor, state: CaptureCycle) -> None:
    """Preserve provider checkpoint fields until finalization."""
    cursor.cursor_data = {**cursor.cursor_data, CYCLE_KEY: state.model_dump(mode="json")}
    cursor.last_updated = datetime.now(timezone.utc)


def next_cycle(previous: CaptureCycle | None, request: BeginCycle | RestartCycle) -> CaptureCycle:
    """One immutable execution plan; incompatible history never becomes a delta baseline."""
    digest = request.configuration.digest()
    evidence = previous.last_full_capture if previous else None
    promoted = previous.promoted_checkpoint if previous else None
    if evidence is not None and evidence.configuration_digest != digest:
        evidence = None
    if promoted is not None and promoted.configuration_digest != digest:
        promoted = None
    if request.mode == "mixed":
        if not request.configuration.scope_changes or request.starting_checkpoint is not None:
            raise CycleConflict("Mixed capture requires declared scope changes and no global token")
        evidence = promoted = None
    elif request.configuration.scope_changes:
        raise CycleConflict("Scoped changes require explicit mixed cycle execution")
    starting = request.starting_checkpoint
    if request.mode == "changes":
        if len(request.configuration.parents) != 1 or next(
            iter(request.configuration.parents.values())
        ) != (None,):
            raise CycleConflict("Changes currently require one independent root kind")
        if evidence is None or promoted is None:
            raise CycleConflict(
                "Changes require a completed compatible full capture and checkpoint"
            )
        if starting is not None and starting != promoted.checkpoint:
            raise CycleConflict("Changes must start at the last promoted provider checkpoint")
        starting = promoted.checkpoint
    return CaptureCycle(
        version=CycleVersion(
            cycle_id=uuid4(), revision=1 if previous is None else previous.version.revision + 1
        ),
        configuration=request.configuration,
        mode=request.mode,
        starting_checkpoint=starting,
        source_plan=request.source_plan,
        force_full_scopes=request.force_full_scopes,
        promoted_checkpoint=promoted,
        last_full_capture=evidence,
    )


def attest_active_plan(state: CaptureCycle, request: BeginCycle) -> None:
    """Resume never changes the already persisted execution intent."""
    if state.configuration != request.configuration:
        raise CycleConflict("Active cycle configuration changed; explicit abandonment required")
    if state.force_full_scopes != request.force_full_scopes:
        raise CycleConflict("Active cycle force-full intent changed")
    if state.source_plan != request.source_plan:
        raise CycleConflict("Active cycle source plan changed; explicit restart required")
    if state.mode != request.mode or (
        request.starting_checkpoint is not None
        and request.starting_checkpoint != state.starting_checkpoint
    ):
        raise CycleConflict("Active cycle execution plan changed; explicit restart required")


async def begin_cycle(db: AsyncSession, request: BeginCycle) -> CaptureCycle:
    """An explicit completed-version CAS starts the next cycle."""
    cursor = await cursor_row(db, request.fence)
    state = cycle_state(cursor)
    if state:
        if request.expected is not None and request.expected != state.version:
            raise CycleConflict("Cycle changed; reload durable progress")
        if state.phase == "active":
            attest_active_plan(state, request)
            return state
        if request.expected is None:
            return state
    elif request.expected is not None:
        raise CycleConflict("Expected cycle does not exist")
    if cursor is None:
        cursor = SyncCursor(
            organization_id=request.fence.organization_id,
            sync_id=request.fence.sync_id,
            cursor_data={},
        )
        db.add(cursor)
    state = next_cycle(state, request)
    persist(cursor, state)
    await db.flush()
    return state


async def attest_cycle(
    db: AsyncSession, fence: WriterFence, cycle_id: UUID
) -> tuple[SyncCursor, CaptureCycle]:
    """Reject obsolete or foreign cycle identities before any scope mutation."""
    cursor = await cursor_row(db, fence)
    state = cycle_state(cursor)
    if state is None or state.phase != "active" or state.version.cycle_id != cycle_id:
        raise CycleConflict("Scan does not belong to the active cycle")
    return cursor, state


def inventory_complete(subject, fence: WriterFence, state: CaptureCycle):
    """Exact inventory that captured a parent must be fresh for this writer attempt."""
    owner = aliased(Entity)
    parent_id = (
        select(owner.id)
        .where(
            owner.organization_id == subject.organization_id,
            owner.sync_id == subject.sync_id,
            owner.entity_definition_short_name == subject.parent_record_type,
            owner.native_id == subject.parent_native_id,
            owner.container_id.is_not_distinct_from(subject.parent_container_id),
        )
        .correlate(subject)
        .scalar_subquery()
    )
    sightings = (
        (subject.last_seen_run_id == CaptureScan.sweep_id,)
        if state.configuration.membership == "observed"
        else ()
    )
    return exists(
        select(CaptureScan.id).where(
            *sightings,
            CaptureScan.organization_id == fence.organization_id,
            CaptureScan.sync_id == fence.sync_id,
            CaptureScan.cycle_id == state.version.cycle_id,
            CaptureScan.record_type == subject.entity_definition_short_name,
            CaptureScan.container_id.is_not_distinct_from(subject.container_id),
            CaptureScan.parent_record_id.is_not_distinct_from(parent_id),
            CaptureScan.parent_visibility_epoch.is_not_distinct_from(
                subject.parent_visibility_epoch
            ),
            or_(
                subject.entity_definition_short_name.in_(
                    state.configuration.exact_parent_validation
                ),
                CaptureScan.membership_attempt_id == fence.attempt_id,
            ),
            or_(
                subject.parent_record_type.is_(None),
                subject.parent_record_type.not_in(state.configuration.exact_parent_validation),
                and_(
                    CaptureScan.parent_verified_attempt_id == fence.attempt_id,
                    CaptureScan.parent_verified_revision
                    == select(owner.record_revision)
                    .where(owner.id == parent_id)
                    .correlate(subject)
                    .scalar_subquery(),
                ),
            ),
            CaptureScan.phase == "complete",
        )
    )


def membership_ready(fence: WriterFence, state: CaptureCycle):
    """Current parent and every ancestor need completed current-attempt inventories."""
    chain = ancestor_chain()
    ancestor = aliased(Entity)
    stale_ancestor = exists(
        select(chain.c.id)
        .join(ancestor, ancestor.id == chain.c.id)
        .where(~inventory_complete(ancestor, fence, state))
    )
    return and_(inventory_complete(Entity, fence, state), ~stale_ancestor)


async def root_ready(db: AsyncSession, fence: WriterFence, state: CaptureCycle) -> bool:
    """Every declared root inventory completes before descendant work."""
    for kind in state.configuration.root_record_types:
        predicates = [
            CaptureScan.organization_id == fence.organization_id,
            CaptureScan.sync_id == fence.sync_id,
            CaptureScan.record_type == kind,
            CaptureScan.container_id.is_(None),
            CaptureScan.parent_record_id.is_(None),
            CaptureScan.cycle_id == state.version.cycle_id,
            CaptureScan.phase == "complete",
        ]
        if state.configuration.fresh_inventory(kind):
            predicates.append(CaptureScan.membership_attempt_id == fence.attempt_id)
        if await db.scalar(select(CaptureScan.id).where(*predicates).limit(1)) is None:
            return False
    return True


async def scope_owner(
    db: AsyncSession, fence: WriterFence, state: CaptureCycle, scope: CompletedScope
) -> Entity | None:
    """Resolve full parent identity; only old unambiguous flat requests are normalized."""
    allowed = state.configuration.parents.get(scope.record_type)
    if allowed is None:
        raise CycleConflict("Scope kind is not declared by the active cycle")
    identity = scope.parent
    if identity is None:
        if scope.container_id is None and None in allowed:
            return None
        flat = tuple(parent for parent in allowed if parent is not None)
        if (
            len(flat) != 1
            or state.configuration.parents[flat[0]] != (None,)
            or scope.container_id is None
        ):
            raise CycleConflict("Nested scope requires its exact parent identity")
        identity = RecordIdentity(record_type=flat[0], native_id=scope.container_id)
    if identity.record_type not in allowed:
        raise CycleConflict("Scope parent kind is not declared by the active cycle")
    parent = await db.scalar(
        select(Entity)
        .where(
            Entity.organization_id == fence.organization_id,
            Entity.sync_id == fence.sync_id,
            Entity.entity_definition_short_name == identity.record_type,
            Entity.entity_id == identity.entity_key,
            Entity.record_revision > 0,
        )
        .execution_options(populate_existing=True)
    )
    if parent is None:
        raise CycleConflict("Scope parent is missing; refresh membership before resuming")
    return parent


async def attest_scope(
    db: AsyncSession,
    fence: WriterFence,
    state: CaptureCycle,
    scope: CompletedScope,
    *,
    require_owner_receipt: bool = True,
) -> bool:
    """A scope's full owner chain must be visible and freshly enumerated."""
    parent = await scope_owner(db, fence, state, scope)
    if parent is None:
        return True
    if (
        await db.scalar(select(Entity.id).where(Entity.id == parent.id, content_is_available()))
        is None
    ):
        raise CycleConflict("Child scope no longer has a visible parent")
    ready = await db.scalar(
        select(Entity.id).where(
            Entity.id == parent.id, content_is_available(), membership_ready(fence, state)
        )
    )
    if ready is None or not await root_ready(db, fence, state):
        raise CycleConflict("Refresh and complete ancestor membership before child work")
    if (
        require_owner_receipt
        and parent.entity_definition_short_name in state.configuration.exact_parent_validation
    ):
        verified = await db.scalar(
            select(CaptureScan.id).where(
                CaptureScan.organization_id == fence.organization_id,
                CaptureScan.sync_id == fence.sync_id,
                CaptureScan.parent_record_id == parent.id,
                CaptureScan.record_type == scope.record_type,
                CaptureScan.cycle_id == state.version.cycle_id,
                CaptureScan.parent_visibility_epoch == parent.visibility_epoch,
                CaptureScan.parent_verified_attempt_id == fence.attempt_id,
                CaptureScan.parent_verified_revision == parent.record_revision,
            )
        )
        if verified is None:
            raise CycleConflict("Scope requires fresh exact parent verification")
    return False


def child_scope_complete(fence: WriterFence, state: CaptureCycle, record_type: str):
    """A completed child scope belongs to this exact owner generation."""
    predicates = [
        CaptureScan.organization_id == fence.organization_id,
        CaptureScan.sync_id == fence.sync_id,
        CaptureScan.record_type == record_type,
        CaptureScan.parent_record_id == Entity.id,
        CaptureScan.parent_visibility_epoch == Entity.visibility_epoch,
        CaptureScan.cycle_id == state.version.cycle_id,
        CaptureScan.phase == "complete",
    ]
    if state.configuration.fresh_inventory(record_type):
        predicates.append(CaptureScan.membership_attempt_id == fence.attempt_id)
    predicates.append(
        or_(
            Entity.entity_definition_short_name.not_in(state.configuration.exact_parent_validation),
            and_(
                CaptureScan.parent_verified_attempt_id == fence.attempt_id,
                CaptureScan.parent_verified_revision == Entity.record_revision,
            ),
        )
    )
    return exists(select(CaptureScan.id).where(*predicates))


async def next_scope_work(db: AsyncSession, fence: WriterFence, cycle_id: UUID) -> ScopeWork | None:
    """Reevaluate SQL frontier each time; newly discovered earlier UUIDs cannot be skipped."""
    _, state = await attest_cycle(db, fence, cycle_id)
    for kind in state.configuration.root_record_types:
        predicates = [
            CaptureScan.organization_id == fence.organization_id,
            CaptureScan.sync_id == fence.sync_id,
            CaptureScan.record_type == kind,
            CaptureScan.container_id.is_(None),
            CaptureScan.parent_record_id.is_(None),
            CaptureScan.cycle_id == cycle_id,
            CaptureScan.phase == "complete",
        ]
        if state.configuration.fresh_inventory(kind):
            predicates.append(CaptureScan.membership_attempt_id == fence.attempt_id)
        if await db.scalar(select(CaptureScan.id).where(*predicates).limit(1)) is None:
            return ScopeWork(record_type=kind)
    for child_type, allowed in state.configuration.parents.items():
        parent_types = tuple(kind for kind in allowed if kind is not None)
        if not parent_types:
            continue
        parent = await db.scalar(
            select(Entity)
            .where(
                Entity.organization_id == fence.organization_id,
                Entity.sync_id == fence.sync_id,
                Entity.entity_definition_short_name.in_(parent_types),
                content_is_available(),
                membership_ready(fence, state),
                ~child_scope_complete(fence, state, child_type),
            )
            .order_by(Entity.id)
            .limit(1)
            .execution_options(populate_existing=True)
        )
        if parent is not None:
            return ScopeWork(
                record_type=child_type,
                parent=source_record(parent),
                parent_visibility_epoch=parent.visibility_epoch,
            )
    return None


async def terminal_checkpoint(
    db: AsyncSession, state: CaptureCycle, request: CompleteCycle, now: datetime
) -> PromotedCheckpoint | None:
    """Attest the source's terminal scan before publishing its provider boundary."""
    terminal = request.terminal_checkpoint
    if state.mode == "mixed":
        if terminal is not None:
            raise CycleConflict("Mixed capture has no global provider checkpoint")
        return None
    if (state.starting_checkpoint is not None or state.mode == "changes") and terminal is None:
        raise CycleConflict("Checkpoint-bearing capture requires its terminal scan boundary")
    promoted = state.promoted_checkpoint if state.mode == "changes" else None
    if terminal is not None:
        if terminal.record_type not in state.configuration.root_record_types:
            raise CycleConflict("Terminal checkpoint must belong to a declared root scope")
        scan = await db.scalar(
            select(CaptureScan).where(
                CaptureScan.organization_id == request.fence.organization_id,
                CaptureScan.sync_id == request.fence.sync_id,
                CaptureScan.cycle_id == state.version.cycle_id,
                CaptureScan.record_type == terminal.record_type,
                CaptureScan.container_id.is_(None),
                CaptureScan.parent_record_id.is_(None),
                CaptureScan.phase == "complete",
                CaptureScan.sweep_id == terminal.expected.sweep_id,
                CaptureScan.revision == terminal.expected.revision,
            )
        )
        if scan is None:
            raise CycleConflict("Terminal scan changed or is not complete")
        committed = scan.continuation.get(TERMINAL_CHECKPOINT_KEY)
        if committed is None or ProviderCheckpoint.model_validate(committed) != terminal.checkpoint:
            raise CycleConflict("Provider checkpoint does not match committed terminal page")
        promoted = PromotedCheckpoint(
            checkpoint=terminal.checkpoint,
            configuration_digest=state.configuration.digest(),
            promoted_at=now,
        )
    return promoted


async def complete_cycle(db: AsyncSession, sync: Sync, request: CompleteCycle) -> CaptureCycle:
    """Publish only when every parent eligible under the declared membership is complete."""
    cursor, state = await attest_cycle(db, request.fence, request.expected.cycle_id)
    if state.version != request.expected:
        raise CycleConflict("Cycle changed before completion")
    if not await root_ready(db, request.fence, state):
        raise CycleConflict("Root enumeration is incomplete or stale")
    for child_type, parents in state.configuration.parents.items():
        parent_types = tuple(kind for kind in parents if kind is not None)
        if not parent_types:
            continue
        missing = await db.scalar(
            select(Entity.id)
            .where(
                Entity.organization_id == request.fence.organization_id,
                Entity.sync_id == request.fence.sync_id,
                Entity.entity_definition_short_name.in_(parent_types),
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                content_is_available(),
                # The same current-sweep parent/ancestor predicate governs admission
                # and frontier selection. Retained mode preserves provider semantics.
                *(
                    (membership_ready(request.fence, state),)
                    if state.configuration.membership == "observed"
                    else ()
                ),
                ~child_scope_complete(request.fence, state, child_type),
            )
            .limit(1)
        )
        if missing is not None:
            raise CycleConflict("A visible parent still has an incomplete child scope")
    now = datetime.now(timezone.utc)
    promoted = await terminal_checkpoint(db, state, request, now)
    evidence = state.last_full_capture
    if state.mode == "full":
        evidence = FullCaptureEvidence(
            cycle_id=state.version.cycle_id,
            completed_at=now,
            configuration_digest=state.configuration.digest(),
            discovery="scope_enumeration_complete"
            if state.configuration.membership == "retained"
            and all(
                state.configuration.policy(kind) == "exhaustive"
                for kind in state.configuration.parents
            )
            else "incomplete",
        )
    state = state.model_copy(
        update={
            "phase": "complete",
            "promoted_checkpoint": promoted,
            "last_full_capture": evidence,
            "completed_job_id": request.fence.job_id,
            "version": CycleVersion(
                cycle_id=state.version.cycle_id, revision=state.version.revision + 1
            ),
        }
    )
    persist(cursor, state)
    cursor.cursor_data = {
        **cursor.cursor_data,
        "canonical_checkpoint": CanonicalCheckpoint(
            writer_attempt_id=request.fence.attempt_id,
            observed_change_sequence=sync.observed_change_sequence,
        ).model_dump(mode="json"),
    }
    await db.flush()
    return state


async def restart_cycle(db: AsyncSession, request: RestartCycle) -> CaptureCycle:
    """Retain captured records/checkpoint; invalidate old scans with a fresh cycle UUID."""
    cursor, previous = await attest_cycle(db, request.fence, request.expected.cycle_id)
    if previous.version != request.expected:
        raise CycleConflict("Cycle changed before explicit restart")
    state = next_cycle(previous, request)
    persist(cursor, state)
    await db.flush()
    return state
