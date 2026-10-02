"""Early exact-owner work is not completed inventory or a replacement discovery queue."""

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CompleteCycle
from airweave.domains.entities.canonical.cycle_store import CycleConflict, next_scope_work
from airweave.domains.entities.canonical.requests import CompletedScope, parent_container_key
from airweave.domains.entities.canonical.scan_models import BeginScan
from airweave.domains.entities.canonical.tests.helpers import capture
from airweave.domains.entities.canonical.tests.test_cycles import finish, page, scan
from airweave.domains.entities.canonical.tests.test_exact_parent_validation import CONFIG
from airweave.domains.entities.canonical.tests.test_forest_scans import collect, item
from airweave.models.entity import Entity


async def setup(database, source):
    service, fence = source
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=CONFIG))
    channel = item("channel", "C")
    await collect(database, service, fence, cycle, CompletedScope(record_type="channel"), channel)
    message = item("message", "M", channel.identity)
    scope = CompletedScope(
        record_type="message", container_id=message.identity.container_id, parent=channel.identity
    )
    state = await scan(database, service, fence, cycle, scope)
    state = await page(database, service, fence, state, message)
    async with database() as db:
        work = await next_scope_work(db, fence, cycle.version.cycle_id, within=scope)
    assert work.record_type == "file" and work.parent.identity == message.identity
    request = BeginScan(
        fence=fence,
        scope=CompletedScope(
            record_type="file",
            container_id=parent_container_key(message.identity),
            parent=message.identity,
        ),
        cycle_id=cycle.version.cycle_id,
        fingerprint=CONFIG.fingerprint,
        expected_parent_epoch=work.parent_visibility_epoch,
        expected_parent_revision=work.parent.revision,
        exact_parent_observation=message,
    )
    return cycle, channel, message, scope, state, request


async def test_early_file_commits_without_completing_history(database, source):
    service, fence = source
    cycle, _, message, scope, history, request = await setup(database, source)
    with pytest.raises(CycleConflict, match="fresh exact"):
        async with database() as db:
            await service.begin_scan(
                db, request.model_copy(update={"exact_parent_observation": None})
            )
    async with database() as db:
        admitted = await service.admit_scan(db, request)
    state = await page(
        database, service, fence, admitted.state, item("file", "F", message.identity), final=True
    )
    await finish(database, service, fence, state)
    async with database() as db:
        assert await next_scope_work(db, fence, cycle.version.cycle_id, within=scope) is None
        current = await service.read_scan(db, fence, scope)
        assert current.phase == "collecting" and current.version == history.version
        refreshed = await service.read_cycle(db, fence)
    with pytest.raises(CycleConflict, match="incomplete child"):
        async with database() as db:
            await service.complete_cycle(db, CompleteCycle(fence=fence, expected=refreshed.version))


async def test_unseen_and_other_inventory_owners_cannot_enter_early_work(database, source):
    service, fence = source
    cycle, channel, _, scope, _, request = await setup(database, source)
    unseen = item("message", "unseen", channel.identity)
    await capture(database, service, fence, unseen)
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.native_id == "unseen"))
        forged = request.model_copy(
            update={
                "scope": CompletedScope(
                    record_type="file",
                    container_id=parent_container_key(unseen.identity),
                    parent=unseen.identity,
                ),
                "expected_parent_revision": row.record_revision,
                "expected_parent_epoch": row.visibility_epoch,
                "exact_parent_observation": unseen,
            }
        )
    with pytest.raises(CycleConflict, match="ancestor membership"):
        async with database() as db:
            await service.admit_scan(db, forged)
    # Same kind/container with a different owner is not a broad frontier query.
    wrong = scope.model_copy(
        update={"parent": channel.identity.model_copy(update={"native_id": "other"})}
    )
    with pytest.raises(CycleConflict, match="parent is missing"):
        async with database() as db:
            await next_scope_work(db, fence, cycle.version.cycle_id, within=wrong)


async def test_early_receipt_invalidated_by_edit_and_inventory_restart(database, source):
    service, fence = source
    cycle, _, message, scope, history, request = await setup(database, source)
    async with database() as db:
        admitted = await service.admit_scan(db, request)
    history = await page(
        database,
        service,
        fence,
        history,
        message.model_copy(update={"payload": {"id": "M", "edited": True}}),
    )
    with pytest.raises(CycleConflict, match="fresh exact"):
        await page(database, service, fence, admitted.state, final=True)
    await scan(database, service, fence, cycle, scope, expected=history.version, restart=True)
    async with database() as db:
        assert await next_scope_work(db, fence, cycle.version.cycle_id, within=scope) is None
    with pytest.raises(CycleConflict, match="ancestor membership"):
        await page(database, service, fence, admitted.state, final=True)


async def test_new_writer_cannot_use_early_parent_before_root_refresh(database, source):
    from uuid import uuid4

    service, fence = source
    cycle, _, _, scope, _, request = await setup(database, source)
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    with pytest.raises(CycleConflict, match="ancestor membership"):
        async with database() as db:
            await next_scope_work(db, newer, cycle.version.cycle_id, within=scope)
    with pytest.raises(CycleConflict, match="ancestor membership"):
        async with database() as db:
            await service.admit_scan(db, request.model_copy(update={"fence": newer}))
