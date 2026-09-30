"""Cycle ownership on the existing sync cursor; callers already hold the Sync fence."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from airweave.domains.entities.canonical.checkpoint import CanonicalCheckpoint
from airweave.domains.entities.canonical.cycle_models import (
    CYCLE_KEY,
    BeginCycle,
    CaptureCycle,
    CompleteCycle,
    CycleVersion,
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


async def begin_cycle(db: AsyncSession, request: BeginCycle) -> CaptureCycle:
    """An explicit completed-version CAS starts the next cycle."""
    cursor = await cursor_row(db, request.fence)
    state = cycle_state(cursor)
    if state:
        if request.expected is not None and request.expected != state.version:
            raise CycleConflict("Cycle changed; reload durable progress")
        if state.phase == "active":
            if state.configuration != request.configuration:
                raise CycleConflict(
                    "Active cycle configuration changed; explicit abandonment required"
                )
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
    state = CaptureCycle(
        version=CycleVersion(
            cycle_id=uuid4(), revision=1 if state is None else state.version.revision + 1
        ),
        configuration=request.configuration,
    )
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
    return exists(
        select(CaptureScan.id).where(
            CaptureScan.organization_id == fence.organization_id,
            CaptureScan.sync_id == fence.sync_id,
            CaptureScan.cycle_id == state.version.cycle_id,
            CaptureScan.record_type == subject.entity_definition_short_name,
            CaptureScan.container_id.is_not_distinct_from(subject.container_id),
            CaptureScan.parent_record_id.is_not_distinct_from(parent_id),
            CaptureScan.parent_visibility_epoch.is_not_distinct_from(
                subject.parent_visibility_epoch
            ),
            CaptureScan.membership_attempt_id == fence.attempt_id,
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
        if state.configuration.children_of(kind):
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
    db: AsyncSession, fence: WriterFence, state: CaptureCycle, scope: CompletedScope
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
    if state.configuration.children_of(record_type):
        predicates.append(CaptureScan.membership_attempt_id == fence.attempt_id)
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
        if state.configuration.children_of(kind):
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


async def complete_cycle(db: AsyncSession, sync: Sync, request: CompleteCycle) -> CaptureCycle:
    """Publish the checkpoint only when SQL proves all currently visible scopes complete."""
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
                ~child_scope_complete(request.fence, state, child_type),
            )
            .limit(1)
        )
        if missing is not None:
            raise CycleConflict("A visible parent still has an incomplete child scope")
    state = state.model_copy(
        update={
            "phase": "complete",
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
    state = CaptureCycle(
        version=CycleVersion(cycle_id=uuid4(), revision=previous.version.revision + 1),
        configuration=request.configuration,
    )
    persist(cursor, state)
    await db.flush()
    return state
