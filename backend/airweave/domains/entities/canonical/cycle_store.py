"""Cycle ownership on the existing sync cursor; callers already hold the Sync fence."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.domains.entities.canonical.checkpoint import CanonicalCheckpoint
from airweave.domains.entities.canonical.cycle_models import (
    CYCLE_KEY,
    BeginCycle,
    CaptureCycle,
    CompleteCycle,
    CycleVersion,
)
from airweave.domains.entities.canonical.requests import CompletedScope, WriterFence
from airweave.domains.entities.canonical.store import CanonicalStoreError, content_is_available
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


async def root_ready(db: AsyncSession, fence: WriterFence, state: CaptureCycle) -> bool:
    """A retry must refresh membership before touching or finishing child scopes."""
    root = await db.scalar(
        select(CaptureScan).where(
            CaptureScan.organization_id == fence.organization_id,
            CaptureScan.sync_id == fence.sync_id,
            CaptureScan.record_type == state.configuration.root_record_type,
            CaptureScan.container_id.is_(None),
            CaptureScan.cycle_id == state.version.cycle_id,
            CaptureScan.phase == "complete",
        )
    )
    return root is not None and (
        not state.configuration.child_record_types
        or state.root_writer_attempt_id == fence.attempt_id
    )


async def attest_scope(
    db: AsyncSession, fence: WriterFence, state: CaptureCycle, scope: CompletedScope
) -> bool:
    """Only whole root or visible declared child scopes belong to this cycle."""
    if scope.record_type == state.configuration.root_record_type and scope.container_id is None:
        return True
    if (
        scope.record_type not in state.configuration.child_record_types
        or scope.container_id is None
    ):
        raise CycleConflict("Scope is not declared by the active cycle")
    if not await root_ready(db, fence, state):
        raise CycleConflict("Refresh and complete root membership before child work")
    parent = await db.scalar(
        select(Entity).where(
            Entity.organization_id == fence.organization_id,
            Entity.sync_id == fence.sync_id,
            Entity.entity_definition_short_name == state.configuration.root_record_type,
            Entity.container_id.is_(None),
            Entity.native_id == scope.container_id,
            Entity.record_revision > 0,
            Entity.deleted_at.is_(None),
            content_is_available(),
        )
    )
    if parent is None:
        raise CycleConflict("Child scope no longer has a visible parent")
    return False


async def complete_cycle(db: AsyncSession, sync: Sync, request: CompleteCycle) -> CaptureCycle:
    """Publish the checkpoint only when SQL proves all currently visible scopes complete."""
    cursor, state = await attest_cycle(db, request.fence, request.expected.cycle_id)
    if state.version != request.expected:
        raise CycleConflict("Cycle changed before completion")
    if not await root_ready(db, request.fence, state):
        raise CycleConflict("Root enumeration is incomplete or stale")
    for child_type in state.configuration.child_record_types:
        complete_child = exists(
            select(CaptureScan.id).where(
                CaptureScan.organization_id == request.fence.organization_id,
                CaptureScan.sync_id == request.fence.sync_id,
                CaptureScan.record_type == child_type,
                CaptureScan.container_id == Entity.native_id,
                CaptureScan.cycle_id == state.version.cycle_id,
                CaptureScan.phase == "complete",
            )
        )
        missing = await db.scalar(
            select(Entity.id)
            .where(
                Entity.organization_id == request.fence.organization_id,
                Entity.sync_id == request.fence.sync_id,
                Entity.entity_definition_short_name == state.configuration.root_record_type,
                Entity.container_id.is_(None),
                Entity.record_revision > 0,
                Entity.deleted_at.is_(None),
                content_is_available(),
                ~complete_child,
            )
            .limit(1)
        )
        if missing is not None:
            raise CycleConflict("A visible parent still has an incomplete child scope")
    state = state.model_copy(
        update={
            "phase": "complete",
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
