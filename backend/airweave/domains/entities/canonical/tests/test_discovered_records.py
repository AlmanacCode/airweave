"""Side discovery shares the page transaction, never another scope's sightings."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.store import StaleWriter
from airweave.domains.entities.canonical.tests.helpers import observation
from airweave.models.entity import Entity
from airweave.models.entity_change import EntityChange


def original(kind, native):
    return observation(native, identity=RecordIdentity(record_type=kind, native_id=native))


async def begin(database, service, fence, cycle, scope):
    async with database() as db:
        return await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                cycle_id=cycle.version.cycle_id,
                scope=scope,
                fingerprint=cycle.configuration.fingerprint,
            ),
        )


async def page(database, service, fence, state, records=(), discovered=()):
    async with database() as db:
        return await service.commit_scan_page(
            db,
            CommitScanPage(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                records=records,
                discovered_records=discovered,
                continuation=ScanContinuation(value={"cursor": "after"}),
                final=True,
            ),
        )


async def finish(database, service, fence, state):
    async with database() as db:
        return await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                observed_at=datetime.now(timezone.utc),
            ),
        )


async def setup(database, service, fence):
    config = CycleConfiguration(
        fingerprint="f" * 64,
        parents={"page": (None,), "database": (None,), "block": ("page",)},
        completion_policies={"page": "discovery_only", "database": "discovery_only"},
    )
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
    state = await begin(database, service, fence, cycle, CompletedScope(record_type="page"))
    captured = await page(database, service, fence, state, (original("page", "old"),))
    await finish(database, service, fence, captured.state)
    old = captured.capture.changes[0].record
    state = await begin(database, service, fence, cycle, CompletedScope(record_type="database"))
    return cycle, state, old


async def test_completed_root_inventory_admits_new_frontier_without_changing_sightings(
    database, source
):
    service, fence = source
    cycle, state, old = await setup(database, service, fence)
    async with database() as db:
        prior = (await db.get(Entity, old.id)).last_seen_run_id
    result = await page(
        database,
        service,
        fence,
        state,
        (original("database", "db"),),
        (original("page", "old"), original("page", "new")),
    )
    assert len(result.capture.changes) == 2 and result.capture.unchanged == 1
    await finish(database, service, fence, result.state)
    child = await begin(
        database, service, fence, cycle, CompletedScope(record_type="block", parent=old.identity)
    )
    child = await page(database, service, fence, child)
    await finish(database, service, fence, child.state)
    async with database() as db:
        assert (await db.get(Entity, old.id)).last_seen_run_id == prior
        new = await db.scalar(select(Entity).where(Entity.native_id == "new"))
        assert new.last_seen_run_id is None
        work = await service.next_scope_work(db, fence, cycle.version.cycle_id)
        assert work.record_type == "block" and work.parent.identity.native_id == "new"
    with pytest.raises(ScanConflict):
        await page(database, service, fence, state, discovered=(original("page", "again"),))


@pytest.mark.parametrize(
    "invalid", ["duplicate", "parent", "undeclared", "empty", "oversized", "promotion"]
)
async def test_rejected_side_discovery_preserves_page_cursor_and_journal(database, source, invalid):
    service, fence = source
    _, state, old = await setup(database, service, fence)
    main = original("database", "db")
    discovered = original("page", "new")
    if invalid == "duplicate":
        discovered = main
    if invalid == "parent":
        discovered = discovered.model_copy(update={"parent": old.identity})
    if invalid == "undeclared":
        discovered = original("block", "new")
    if invalid == "empty":
        discovered = discovered.model_copy(update={"payload": {}})
    if invalid == "promotion":
        discovered = discovered.model_copy(update={"allow_reparent": True})
    side = (
        tuple(original("page", str(i)) for i in range(500))
        if invalid == "oversized"
        else (discovered,)
    )
    with pytest.raises(ScanConflict):
        await page(database, service, fence, state, (main,), side)
    async with database() as db:
        current = await service.read_scan(db, fence, state.scope)
        assert current.version == state.version and current.continuation == state.continuation
        assert await db.scalar(select(Entity).where(Entity.native_id == "db")) is None
        assert await db.scalar(select(EntityChange).where(EntityChange.sequence > 1)) is None


async def test_new_writer_fences_entire_discovery_commit(database, source):
    service, fence = source
    _, state, _ = await setup(database, service, fence)
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
        await page(
            database,
            service,
            fence,
            state,
            (original("database", "db"),),
            (original("page", "new"),),
        )
    async with database() as db:
        assert await db.scalar(select(Entity).where(Entity.native_id == "new")) is None
        assert (await service.read_scan(db, newer, state.scope)).version == state.version


async def test_existing_child_cannot_be_promoted_and_rolls_back_scoped_write(database, source):
    from airweave.domains.entities.canonical.store import CanonicalStoreError
    from airweave.domains.entities.canonical.tests.helpers import capture

    service, fence = source
    _, state, old = await setup(database, service, fence)
    # A prior connector mapping stored this native object beneath another page.
    child = original("page", "child").model_copy(update={"parent": old.identity})
    await capture(database, service, fence, child)
    with pytest.raises(CanonicalStoreError, match="parent"):
        await page(
            database,
            service,
            fence,
            state,
            (original("database", "db"),),
            (original("page", "child"),),
        )
    async with database() as db:
        assert await db.scalar(select(Entity).where(Entity.native_id == "db")) is None
        assert (await service.read_scan(db, fence, state.scope)).version == state.version
        assert await db.scalar(select(EntityChange).where(EntityChange.sequence > 2)) is None


async def test_parent_withdrawn_after_io_blocks_scoped_and_discovered_records(database, source):
    from airweave.domains.entities.canonical.cycle_store import CycleConflict
    from airweave.domains.entities.canonical.tests.helpers import capture

    service, fence = source
    cycle, root, old = await setup(database, service, fence)
    final = await page(database, service, fence, root)
    await finish(database, service, fence, final.state)
    state = await begin(
        database, service, fence, cycle, CompletedScope(record_type="block", parent=old.identity)
    )
    await capture(
        database,
        service,
        fence,
        original("page", "old").model_copy(
            update={"kind": "delete", "removal_reason": "access_revoked"}
        ),
    )
    body = original("block", "body").model_copy(update={"parent": old.identity})
    with pytest.raises(CycleConflict):
        await page(database, service, fence, state, (body,), (original("page", "new"),))
    async with database() as db:
        assert await db.scalar(select(Entity).where(Entity.native_id.in_(["new", "body"]))) is None
        assert (await service.read_scan(db, fence, state.scope)).version == state.version


async def test_exhaustive_target_root_rejects_side_admission(database, source):
    service, fence = source
    config = CycleConfiguration(
        fingerprint="f" * 64,
        parents={"page": (None,), "database": (None,)},
        completion_policies={"database": "discovery_only"},
    )
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
    state = await begin(database, service, fence, cycle, CompletedScope(record_type="database"))
    with pytest.raises(ScanConflict, match="independent declared roots"):
        await page(database, service, fence, state, discovered=(original("page", "new"),))


async def test_side_revival_invalidates_previously_completed_child_scope(database, source):
    from airweave.domains.entities.canonical.tests.helpers import capture

    service, fence = source
    cycle, root, old = await setup(database, service, fence)
    final = await page(database, service, fence, root)
    final = await finish(database, service, fence, final.state)
    child = await begin(
        database, service, fence, cycle, CompletedScope(record_type="block", parent=old.identity)
    )
    child = await page(database, service, fence, child)
    await finish(database, service, fence, child.state)
    await capture(
        database,
        service,
        fence,
        original("page", "old").model_copy(
            update={"kind": "delete", "removal_reason": "scope_removed"}
        ),
    )
    async with database() as db:
        restarted = await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=root.scope,
                cycle_id=root.cycle_id,
                fingerprint=root.fingerprint,
                expected=final.state.version,
                restart=True,
            ),
        )
    revived = await page(database, service, fence, restarted, discovered=(original("page", "old"),))
    await finish(database, service, fence, revived.state)
    async with database() as db:
        work = await service.next_scope_work(db, fence, cycle.version.cycle_id)
        assert work.parent.id == old.id
        assert work.parent_visibility_epoch > child.state.parent_visibility_epoch
        assert work.record_type == "block"
