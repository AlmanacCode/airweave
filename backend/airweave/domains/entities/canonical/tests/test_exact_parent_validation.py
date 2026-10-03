"""Fenced exact owner reads authorize child work without resetting discovery."""

from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.requests import CompletedScope, parent_container_key
from airweave.domains.entities.canonical.scan_models import BeginScan
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.tests.helpers import capture
from airweave.domains.entities.canonical.tests.test_cycles import complete, finish, page
from airweave.domains.entities.canonical.tests.test_forest_scans import collect, item, work
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity

CONFIG = CycleConfiguration(
    fingerprint="a" * 64,
    parents={"channel": (None,), "message": ("channel",), "file": ("message",)},
    exact_parent_validation=("message",),
)


async def setup(database, source):
    service, fence = source
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=CONFIG))
    channel = item("channel", "C")
    message = item("message", "M", channel.identity)
    await collect(database, service, fence, cycle, CompletedScope(record_type="channel"), channel)
    await collect(
        database,
        service,
        fence,
        cycle,
        CompletedScope(
            record_type="message",
            container_id=message.identity.container_id,
            parent=channel.identity,
        ),
        message,
    )
    selected = await work(database, service, fence, cycle)
    owner = selected.parent
    scope = CompletedScope(
        record_type="file",
        container_id=parent_container_key(message.identity),
        parent=message.identity,
    )
    request = BeginScan(
        fence=fence,
        scope=scope,
        cycle_id=cycle.version.cycle_id,
        fingerprint=CONFIG.fingerprint,
        expected_parent_epoch=selected.parent_visibility_epoch,
        expected_parent_revision=owner.revision,
        exact_parent_observation=message,
    )
    return cycle, message, owner, request


async def test_receipt_preserves_sighting_and_completed_children(database, source):
    service, fence = source
    cycle, message, owner, request = await setup(database, source)
    async with database() as db:
        seen = await db.scalar(select(Entity.last_seen_run_id).where(Entity.id == owner.id))
        admitted = await service.admit_scan(db, request)
    assert admitted.parent == owner
    assert admitted.state.parent_verified_attempt_id == fence.attempt_id
    state = await page(database, service, fence, admitted.state, final=True)
    state = await finish(database, service, fence, state)
    async with database() as db:
        assert await db.scalar(select(Entity.last_seen_run_id).where(Entity.id == owner.id)) == seen
        repeated = await service.admit_scan(
            db,
            request.model_copy(
                update={
                    "expected": state.version,
                    "exact_parent_observation": None,
                }
            ),
        )
    assert repeated.state.phase == "complete"
    assert await work(database, service, fence, cycle) is None


async def test_missing_receipt_and_stale_post_io_revision_fail(database, source):
    service, fence = source
    _, message, _, request = await setup(database, source)
    with pytest.raises(CycleConflict, match="fresh exact"):
        async with database() as db:
            await service.begin_scan(
                db, request.model_copy(update={"exact_parent_observation": None})
            )
    await capture(
        database, service, fence, message.model_copy(update={"payload": {"id": "M", "new": True}})
    )
    with pytest.raises(ScanConflict, match="owner changed"):
        async with database() as db:
            await service.admit_scan(db, request)


async def test_unavailable_observation_commits_without_child_admission(database, source):
    service, fence = source
    _, message, owner, request = await setup(database, source)
    unavailable = message.model_copy(update={"kind": "delete", "removal_reason": "scope_removed"})
    async with database() as db:
        result = await service.admit_scan(
            db, request.model_copy(update={"exact_parent_observation": unavailable})
        )
    assert result.state is None
    assert result.parent.deleted_at is not None
    async with database() as db:
        assert (await db.get(Entity, owner.id)).deleted_at is not None


async def test_new_writer_needs_root_refresh_before_exact_admission(database, source):
    service, fence = source
    _, _, _, request = await setup(database, source)
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    with pytest.raises(CycleConflict):
        async with database() as db:
            await service.admit_scan(db, request.model_copy(update={"fence": newer}))


async def test_owner_edit_invalidates_receipt_and_restarts_inventory(database, source):
    service, fence = source
    _, message, owner, request = await setup(database, source)
    async with database() as db:
        admitted = await service.admit_scan(db, request)
    state = admitted.state
    changed = message.model_copy(
        update={
            "payload": {"id": "M", "files": ["new"]},
            "descendant_visibility_fields": ("files",),
        }
    )
    await capture(database, service, fence, changed)
    with pytest.raises(CycleConflict, match="fresh exact"):
        await page(database, service, fence, state, final=True)
    async with database() as db:
        current = await db.get(Entity, owner.id)
        refreshed = await service.admit_scan(
            db,
            request.model_copy(
                update={
                    "expected": state.version,
                    "expected_parent_epoch": current.visibility_epoch,
                    "expected_parent_revision": current.record_revision,
                    "exact_parent_observation": changed,
                }
            ),
        )
    assert refreshed.state.version.sweep_id != state.version.sweep_id
    assert refreshed.parent.payload["files"] == ["new"]
    assert refreshed.state.parent_verified_revision == refreshed.parent.revision
    # Native cursor expiry may restart the sweep, retaining this still-valid receipt.
    async with database() as db:
        restarted = await service.begin_scan(
            db,
            request.model_copy(
                update={
                    "expected": refreshed.state.version,
                    "restart": True,
                    "expected_parent_epoch": refreshed.state.parent_visibility_epoch,
                    "expected_parent_revision": refreshed.parent.revision,
                    "exact_parent_observation": None,
                }
            ),
        )
    await page(database, service, fence, restarted, final=True)


async def test_stale_writer_cannot_apply_observation(database, source):
    from airweave.domains.entities.canonical.store import StaleWriter

    service, fence = source
    _, message, owner, request = await setup(database, source)
    async with database() as db:
        await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    with pytest.raises(StaleWriter):
        async with database() as db:
            await service.admit_scan(
                db,
                request.model_copy(
                    update={
                        "exact_parent_observation": message.model_copy(
                            update={"payload": {"bad": True}}
                        ),
                    }
                ),
            )
    async with database() as db:
        current = await db.get(Entity, owner.id)
        assert current.source_payload == message.payload
        assert current.record_revision == owner.revision


async def test_new_writer_reuses_completed_children_after_fresh_root(database, source):
    service, fence = source
    cycle, message, _, request = await setup(database, source)
    async with database() as db:
        admitted = await service.admit_scan(db, request)
    original = item("file", "F", message.identity)
    state = await page(database, service, fence, admitted.state, original, final=True)
    state = await finish(database, service, fence, state)
    async with database() as db:
        file_before = await db.scalar(select(Entity).where(Entity.native_id == "F"))
        revision, payload = file_before.record_revision, file_before.source_payload
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    assert (await work(database, service, newer, cycle)).record_type == "channel"
    root_scope = CompletedScope(record_type="channel")
    async with database() as db:
        old_root = await service.read_scan(db, newer, root_scope)
        root = await service.begin_scan(
            db,
            BeginScan(
                fence=newer,
                scope=root_scope,
                cycle_id=cycle.version.cycle_id,
                fingerprint=CONFIG.fingerprint,
                expected=old_root.version,
                restart=True,
            ),
        )
    root = await page(database, service, newer, root, item("channel", "C"), final=True)
    await finish(database, service, newer, root)
    assert await work(database, service, newer, cycle) is None
    terminal = await complete(database, service, newer)
    assert terminal.phase == "complete"
    async with database() as db:
        current = await db.scalar(select(Entity).where(Entity.native_id == "F"))
        assert (current.record_revision, current.source_payload) == (revision, payload)


@pytest.mark.parametrize("invalidated", ["receipt", "revision", "visibility"])
async def test_completed_child_proof_requires_valid_owner_generation(database, source, invalidated):
    service, fence = source
    cycle, _, owner, request = await setup(database, source)
    async with database() as db:
        admitted = await service.admit_scan(db, request)
    state = await page(database, service, fence, admitted.state, final=True)
    state = await finish(database, service, fence, state)
    async with database() as db:
        if invalidated == "receipt":
            scan = await db.scalar(
                select(CaptureScan).where(CaptureScan.parent_record_id == owner.id)
            )
            scan.parent_verified_attempt_id = None
        else:
            parent = await db.get(Entity, owner.id)
            if invalidated == "revision":
                parent.record_revision += 1
            else:
                parent.visibility_epoch += 1
        await db.commit()
    assert (await work(database, service, fence, cycle)).record_type == "file"
    with pytest.raises(CycleConflict, match="incomplete child"):
        await complete(database, service, fence)
