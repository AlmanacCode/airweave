"""Native page observations atomically attest existing child scans without another queue."""

from uuid import uuid4

import pytest
from sqlalchemy import func, select

from airweave.domains.entities.canonical.cycle_models import BeginCycle
from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.requests import CompletedScope, parent_container_key
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    ChildScopeObservation,
    CommitScanPage,
    ScanContinuation,
)
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.store import StaleWriter
from airweave.domains.entities.canonical.tests.helpers import capture
from airweave.domains.entities.canonical.tests.test_cycles import finish, page, scan
from airweave.domains.entities.canonical.tests.test_exact_parent_validation import CONFIG
from airweave.domains.entities.canonical.tests.test_forest_scans import collect, item
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity


async def history(database, source, config=CONFIG):
    service, fence = source
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
    channel = item("channel", "C")
    await collect(database, service, fence, cycle, CompletedScope(record_type="channel"), channel)
    messages = tuple(item("message", name, channel.identity) for name in ("one", "two"))
    state = await scan(
        database,
        service,
        fence,
        cycle,
        CompletedScope(
            record_type="message",
            container_id=messages[0].identity.container_id,
            parent=channel.identity,
        ),
    )
    return cycle, channel, messages, state


def declaration(message):
    return ChildScopeObservation(
        scope=CompletedScope(
            record_type="file",
            container_id=parent_container_key(message.identity),
            parent=message.identity,
        )
    )


def request(fence, state, messages, declarations=None):
    return CommitScanPage(
        fence=fence,
        scope=state.scope,
        cycle_id=state.cycle_id,
        expected=state.version,
        records=messages,
        continuation=ScanContinuation(value={"next": str(state.version.revision)}),
        child_scope_observations=(
            tuple(declaration(m) for m in messages) if declarations is None else declarations
        ),
    )


async def commit(database, service, request):
    async with database() as db:
        return await service.commit_scan_page(db, request)


@pytest.mark.parametrize("completed", [False, True])
async def test_page_receipts_include_unchanged_owners_and_preserve_child_progress(
    database, source, completed
):
    service, fence = source
    _, _, messages, state = await history(database, source)
    # Complete child inventory does not imply complete unrelated body coverage.
    messages = (messages[0].model_copy(update={"completeness": "partial"}), messages[1])
    async with database() as db:
        before_cycle = await service.read_cycle(db, fence)
    result = await commit(database, service, request(fence, state, messages))
    async with database() as db:
        cycle = await service.read_cycle(db, fence)
        children = [await service.read_scan(db, fence, declaration(m).scope) for m in messages]
    assert cycle.version.revision == before_cycle.version.revision + len(messages)
    assert all(c.parent_verified_attempt_id == fence.attempt_id for c in children)
    child = await page(
        database,
        service,
        fence,
        children[0],
        item("file", "F", messages[0].identity),
        final=completed,
    )
    if completed:
        child = await finish(database, service, fence, child)
    repeated = await commit(database, service, request(fence, result.state, messages))
    assert repeated.capture.unchanged == 2 and repeated.capture.changes == ()
    async with database() as db:
        current = await service.read_scan(db, fence, child.scope)
        owner = await db.scalar(select(Entity).where(Entity.native_id == "one"))
    assert current.version.sweep_id == child.version.sweep_id
    assert current.continuation == child.continuation and current.phase == child.phase
    assert current.parent_verified_revision == owner.record_revision == 1
    assert current.parent_visibility_epoch == owner.visibility_epoch


async def test_changed_inventory_restarts_child_and_hides_removed_attachment(database, source):
    service, fence = source
    _, _, messages, state = await history(database, source)
    message = messages[0].model_copy(
        update={"payload": {"files": ["F"]}, "descendant_visibility_fields": ("files",)}
    )
    result = await commit(database, service, request(fence, state, (message,)))
    async with database() as db:
        child = await service.read_scan(db, fence, declaration(message).scope)
    child = await page(
        database, service, fence, child, item("file", "F", message.identity), final=True
    )
    child = await finish(database, service, fence, child)
    changed = message.model_copy(update={"payload": {"files": []}})
    await commit(database, service, request(fence, result.state, (changed,)))
    async with database() as db:
        current = await service.read_scan(db, fence, child.scope)
        file = await db.scalar(select(Entity).where(Entity.native_id == "F"))
        retained = await service.store.read(db, fence.organization_id, fence.sync_id, file.id)
    assert retained.content_access == "unavailable"
    assert current.phase == "collecting" and current.continuation == ScanContinuation()
    assert current.version.sweep_id != child.version.sweep_id
    assert current.parent_verified_revision == 2
    empty = await page(database, service, fence, current, final=True)
    await finish(database, service, fence, empty)
    async with database() as db:
        assert (await db.get(Entity, file.id)).deleted_at is not None


@pytest.mark.parametrize(
    "invalid",
    [
        "outside_page",
        "duplicate",
        "duplicate_namespace",
        "delete",
        "reparent",
        "relationship",
        "not_opted_in",
    ],
)
async def test_page_rejects_unqualified_child_receipts_without_advancing(database, source, invalid):
    service, fence = source
    config = (
        CONFIG.model_copy(update={"exact_parent_validation": ()})
        if invalid == "not_opted_in"
        else CONFIG
    )
    _, _, messages, state = await history(database, source, config)
    message = messages[0]
    declared = declaration(message)
    if invalid == "outside_page":
        await capture(database, service, fence, messages[1])
        declared = declaration(messages[1])
    elif invalid == "delete":
        message = message.model_copy(
            update={"kind": "delete", "removal_reason": "provider_deleted"}
        )
    elif invalid == "reparent":
        message = message.model_copy(update={"allow_reparent": True})
    elif invalid == "relationship":
        declared = declared.model_copy(
            update={"scope": declared.scope.model_copy(update={"record_type": "channel"})}
        )
    declarations = (declared, declared) if invalid == "duplicate" else (declared,)
    if invalid == "duplicate_namespace":
        declarations = (
            declared,
            declared.model_copy(
                update={"scope": declared.scope.model_copy(update={"container_id": "other"})}
            ),
        )
    with pytest.raises(ScanConflict):
        await commit(database, service, request(fence, state, (message,), declarations))
    async with database() as db:
        assert (await service.read_scan(db, fence, state.scope)).version == state.version
        assert await db.scalar(
            select(func.count())
            .select_from(Entity)
            .where(Entity.entity_definition_short_name == "message")
        ) == (1 if invalid == "outside_page" else 0)
        assert (
            await db.scalar(
                select(func.count())
                .select_from(CaptureScan)
                .where(CaptureScan.record_type == "file")
            )
            == 0
        )


async def test_receipt_failure_rolls_back_originals_scans_and_page_cursor(
    database, source, monkeypatch
):
    service, fence = source
    _, _, messages, state = await history(database, source)
    original = service.scans._begin_verified
    calls = 0

    async def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        value = await original(*args, **kwargs)
        if calls == 2:
            raise RuntimeError("synthetic crash after second receipt")
        return value

    monkeypatch.setattr(service.scans, "_begin_verified", fail_second)
    with pytest.raises(RuntimeError, match="synthetic crash"):
        await commit(database, service, request(fence, state, messages))
    async with database() as db:
        assert (await service.read_scan(db, fence, state.scope)).version == state.version
        assert (
            await db.scalar(
                select(func.count())
                .select_from(Entity)
                .where(Entity.entity_definition_short_name == "message")
            )
            == 0
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(CaptureScan)
                .where(CaptureScan.record_type == "file")
            )
            == 0
        )


async def test_new_writer_needs_new_observation_and_rejects_old_writer(database, source):
    service, fence = source
    cycle, channel, messages, state = await history(database, source)
    result = await commit(database, service, request(fence, state, messages))
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
        root = await service.read_scan(db, newer, CompletedScope(record_type="channel"))
    with pytest.raises(StaleWriter):
        await commit(database, service, request(fence, result.state, messages))
    root = await scan(
        database, service, newer, cycle, root.scope, expected=root.version, restart=True
    )
    root = await page(database, service, newer, root, channel, final=True)
    await finish(database, service, newer, root)
    async with database() as db:
        old_child = await service.read_scan(db, newer, declaration(messages[1]).scope)
    old_admission = BeginScan(
        fence=newer,
        scope=old_child.scope,
        cycle_id=cycle.version.cycle_id,
        fingerprint=CONFIG.fingerprint,
        expected=old_child.version,
        expected_parent_epoch=old_child.parent_visibility_epoch,
    )
    with pytest.raises(CycleConflict, match="fresh exact"):
        async with database() as db:
            await service.begin_scan(db, old_admission)
    await commit(database, service, request(newer, result.state, (messages[0],)))
    async with database() as db:
        fresh = await service.read_scan(db, newer, declaration(messages[0]).scope)
        old = await service.read_scan(db, newer, old_child.scope)
    assert fresh.parent_verified_attempt_id == newer.attempt_id
    assert old.parent_verified_attempt_id == fence.attempt_id


async def test_withdrawn_ancestor_rejects_page_receipts(database, source):
    service, fence = source
    _, channel, messages, state = await history(database, source)
    await capture(
        database,
        service,
        fence,
        channel.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    with pytest.raises(CycleConflict, match="visible parent"):
        await commit(database, service, request(fence, state, messages))
    async with database() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(CaptureScan)
                .where(CaptureScan.record_type == "file")
            )
            == 0
        )


async def test_established_child_namespace_change_rolls_back_page(database, source):
    service, fence = source
    _, _, messages, state = await history(database, source)
    result = await commit(database, service, request(fence, state, messages))
    declared = declaration(messages[0])
    wrong = declared.model_copy(
        update={"scope": declared.scope.model_copy(update={"container_id": "other"})}
    )
    with pytest.raises(CycleConflict, match="identity mapping changed"):
        await commit(database, service, request(fence, result.state, (messages[0],), (wrong,)))
    async with database() as db:
        assert (await service.read_scan(db, fence, state.scope)).version == result.state.version
        assert (await service.read_scan(db, fence, declared.scope)).scope == declared.scope
