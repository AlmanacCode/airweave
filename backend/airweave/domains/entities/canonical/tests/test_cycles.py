"""Real PostgreSQL proofs for the single durable cycle boundary and scoped recovery."""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CompleteCycle,
    CycleConfiguration,
)
from airweave.domains.entities.canonical.cycle_store import CycleConflict, complete_cycle
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.store import CanonicalStoreError, StaleWriter
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor

CONFIG = CycleConfiguration(
    fingerprint="a" * 64, root_record_type="channel", child_record_types=("message",)
)
ROOT = CompletedScope(record_type="channel")
CHILD = CompletedScope(record_type="message", container_id="C1")


def record(kind="channel", native_id="C1"):
    parent = RecordIdentity(record_type="channel", native_id="C1") if kind == "message" else None
    return CaptureRecord(
        identity=RecordIdentity(
            record_type=kind, native_id=native_id, container_id="C1" if parent else None
        ),
        parent=parent,
        payload={"id": native_id},
        observed_at=datetime.now(timezone.utc),
    )


async def cycle(database, service, fence, **kwargs):
    async with database() as db:
        return await service.begin_cycle(
            db, BeginCycle(fence=fence, configuration=CONFIG, **kwargs)
        )


async def scan(database, service, fence, active, scope=ROOT, **kwargs):
    async with database() as db:
        return await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=scope,
                cycle_id=active.version.cycle_id,
                fingerprint=CONFIG.fingerprint,
                **kwargs,
            ),
        )


async def page(database, service, fence, state, *records, final=False):
    async with database() as db:
        result = await service.commit_scan_page(
            db,
            CommitScanPage(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                records=records,
                continuation=ScanContinuation(value={"next": "synthetic"}),
                final=final,
            ),
        )
    return result.state


async def finish(database, service, fence, state):
    async with database() as db:
        result = await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                observed_at=datetime.now(timezone.utc),
                removal_reason="scope_removed" if state.scope == ROOT else "absent",
            ),
        )
    assert result.state.phase == "complete"
    return result.state


async def refresh_cycle(database, service, fence):
    async with database() as db:
        return await service.read_cycle(db, fence)


async def complete(database, service, fence):
    state = await refresh_cycle(database, service, fence)
    async with database() as db:
        return await service.complete_cycle(db, CompleteCycle(fence=fence, expected=state.version))


async def fixture_scopes(database, service, fence, *, child_complete=True):
    active = await cycle(database, service, fence)
    root = await scan(database, service, fence, active)
    root = await finish(
        database, service, fence, await page(database, service, fence, root, record(), final=True)
    )
    child = await scan(database, service, fence, active, CHILD)
    child = await page(
        database, service, fence, child, record("message", "one"), final=child_complete
    )
    if child_complete:
        child = await finish(database, service, fence, child)
    return active, root, child


async def test_lost_final_ack_boundary_and_explicit_next_cycle(database, source):
    service, fence = source
    active, root, child = await fixture_scopes(database, service, fence)
    async with database() as db:
        assert "canonical_checkpoint" not in (await db.scalar(select(SyncCursor))).cursor_data
    assert (await cycle(database, service, fence)).version.cycle_id == active.version.cycle_id
    finished = await complete(database, service, fence)
    assert finished.phase == "complete"
    assert await cycle(database, service, fence) == finished
    async with database() as db:
        stored = (await db.scalar(select(SyncCursor))).cursor_data
        assert stored["canonical_checkpoint"]["observed_change_sequence"] == 2
    following = await cycle(database, service, fence, expected=finished.version)
    assert following.version.cycle_id != finished.version.cycle_id
    with pytest.raises(CycleConflict, match="Root enumeration"):
        await complete(database, service, fence)
    refreshed = await scan(database, service, fence, following, expected=root.version)
    assert refreshed.phase == "collecting" and refreshed.version.sweep_id != root.version.sweep_id
    with pytest.raises(CycleConflict):
        await page(database, service, fence, child, final=True)


async def test_retry_refreshes_root_without_resetting_child_pages(database, source):
    service, fence = source
    active, root, child = await fixture_scopes(database, service, fence, child_complete=False)
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    with pytest.raises(StaleWriter):
        await page(database, service, fence, child, record("message", "late"))
    with pytest.raises(CycleConflict, match="Refresh"):
        await page(database, service, newer, child, record("message", "early"))
    with pytest.raises(CycleConflict, match="Restart root"):
        await scan(database, service, newer, active)
    root = await scan(database, service, newer, active, expected=root.version, restart=True)
    await finish(
        database, service, newer, await page(database, service, newer, root, record(), final=True)
    )
    resumed = await scan(database, service, newer, active, CHILD)
    assert resumed == child
    await finish(
        database,
        service,
        newer,
        await page(database, service, newer, resumed, record("message", "two"), final=True),
    )
    assert (await complete(database, service, newer)).phase == "complete"
    async with database() as db:
        messages = (
            await db.scalars(select(Entity).where(Entity.entity_definition_short_name == "message"))
        ).all()
        assert {row.native_id for row in messages} == {"one", "two"}
        assert all(row.deleted_at is None for row in messages)


async def test_removed_parent_is_not_pending_and_reappearance_gets_new_scan(database, source):
    service, fence = source
    active, root, child = await fixture_scopes(database, service, fence, child_complete=False)
    with pytest.raises(CycleConflict, match="incomplete child"):
        await complete(database, service, fence)
    root = await scan(database, service, fence, active, expected=root.version, restart=True)
    root = await finish(
        database, service, fence, await page(database, service, fence, root, final=True)
    )
    with pytest.raises(CycleConflict, match="visible parent"):
        await page(database, service, fence, child, final=True)
    finished = await complete(database, service, fence)
    following = await cycle(database, service, fence, expected=finished.version)
    root = await scan(database, service, fence, following, expected=root.version)
    await finish(
        database, service, fence, await page(database, service, fence, root, record(), final=True)
    )
    replacement = await scan(database, service, fence, following, CHILD, expected=child.version)
    assert replacement.phase == "collecting"
    assert replacement.version.sweep_id != child.version.sweep_id
    with pytest.raises(CycleConflict):
        await scan(
            database, service, fence, active, CHILD, expected=replacement.version, restart=True
        )


async def test_configuration_unknown_scope_and_foreign_cycle_are_rejected(database, source):
    service, fence = source
    active = await cycle(database, service, fence)
    async with database() as db:
        with pytest.raises(CycleConflict, match="configuration changed"):
            await service.begin_cycle(
                db,
                BeginCycle(
                    fence=fence, configuration=CONFIG.model_copy(update={"fingerprint": "b" * 64})
                ),
            )
    with pytest.raises(CycleConflict, match="not declared"):
        await scan(database, service, fence, active, CompletedScope(record_type="arbitrary"))
    foreign = active.model_copy(
        update={"version": active.version.model_copy(update={"cycle_id": uuid4()})}
    )
    with pytest.raises(CycleConflict, match="active cycle"):
        await scan(database, service, fence, foreign)


async def test_finalization_rollback_and_concurrent_cas(database, source):
    service, fence = source
    await fixture_scopes(database, service, fence)
    active = await refresh_cycle(database, service, fence)
    request = CompleteCycle(fence=fence, expected=active.version)
    async with database() as db:
        with pytest.raises(RuntimeError, match="after flush"):
            async with UnitOfWork(db):
                sync = await service.store._fenced_sync(db, fence)
                await complete_cycle(db, sync, request)
                raise RuntimeError("synthetic failure after flush")
    assert await refresh_cycle(database, service, fence) == active
    async with database() as db:
        assert "canonical_checkpoint" not in (await db.scalar(select(SyncCursor))).cursor_data

    async def publish():
        async with database() as db:
            return await service.complete_cycle(db, request)

    results = await asyncio.gather(publish(), publish(), return_exceptions=True)
    assert sum(isinstance(result, CycleConflict) for result in results) == 1
    assert (await refresh_cycle(database, service, fence)).phase == "complete"


@pytest.mark.parametrize("finished", [False, True])
async def test_legacy_checkpoint_cannot_erase_cycle(database, source, finished):
    service, fence = source
    await fixture_scopes(database, service, fence)
    if finished:
        await complete(database, service, fence)
    before = await refresh_cycle(database, service, fence)
    async with database() as db:
        with pytest.raises(CanonicalStoreError, match="explicit cycle finalization"):
            await service.save_checkpoint(db, fence, {"channel_ids": []})
    assert await refresh_cycle(database, service, fence) == before


@pytest.mark.parametrize("finished", [False, True])
@pytest.mark.parametrize("operation", ["create", "update", "delete"])
async def test_legacy_crud_cannot_erase_active_or_completed_cycle(
    database, source, finished, operation
):
    from airweave.core.context import BaseContext
    from airweave.crud.crud_sync_cursor import sync_cursor
    from airweave.schemas.organization import Organization
    from airweave.schemas.sync_cursor import SyncCursorCreate

    service, fence = source
    await fixture_scopes(database, service, fence)
    if finished:
        await complete(database, service, fence)
    before = await refresh_cycle(database, service, fence)
    now = datetime.now(timezone.utc)
    ctx = BaseContext(
        organization=Organization(
            id=fence.organization_id, name="Synthetic owner", created_at=now, modified_at=now
        )
    )
    async with database() as db:
        with pytest.raises(ValueError, match="Cycle-owned"):
            if operation == "create":
                await sync_cursor.create_or_update(
                    db, sync_id=fence.sync_id, ctx=ctx, obj_in=SyncCursorCreate(cursor_data={})
                )
            elif operation == "update":
                await sync_cursor.update_cursor_data(
                    db, sync_id=fence.sync_id, ctx=ctx, cursor_data={}
                )
            else:
                await sync_cursor.delete_by_sync_id(db, sync_id=fence.sync_id, ctx=ctx)
    assert await refresh_cycle(database, service, fence) == before


async def test_child_pages_cannot_omit_visibility_parent_and_root_removal_is_private(
    database, source
):
    from airweave.domains.entities.canonical.scan_store import ScanConflict

    service, fence = source
    active, root, child = await fixture_scopes(database, service, fence, child_complete=False)
    with pytest.raises(ScanConflict, match="parent identity"):
        await page(
            database,
            service,
            fence,
            child,
            record("message", "unscoped").model_copy(update={"parent": None}),
        )
    root = await scan(database, service, fence, active, expected=root.version, restart=True)
    root = await page(database, service, fence, root, final=True)
    async with database() as db:
        result = await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=ROOT,
                cycle_id=root.cycle_id,
                expected=root.version,
                observed_at=datetime.now(timezone.utc),
            ),
        )
    assert result.capture.changes[0].record.removal_reason == "scope_removed"


@pytest.mark.parametrize("completeness", ["partial", "metadata_only"])
async def test_visible_metadata_parent_still_requires_child_scan(database, source, completeness):
    service, fence = source
    active = await cycle(database, service, fence)
    root = await scan(database, service, fence, active)
    root = await page(
        database,
        service,
        fence,
        root,
        record().model_copy(update={"completeness": completeness}),
        final=True,
    )
    await finish(database, service, fence, root)
    with pytest.raises(CycleConflict, match="incomplete child"):
        await complete(database, service, fence)
