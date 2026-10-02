"""Real database proof of immediate visibility and retryable descendant removals."""

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.query_models import RecordFilters
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.models import Entity, EntityChange

pytestmark = pytest.mark.integration


async def test_parent_deletion_hides_content_before_sweep_and_replay_recovers(database, source):
    service, fence = source
    await bind_projection(database, fence)
    parent_id = RecordIdentity(record_type="calendar", native_id="cal")
    parent = observation(identity=parent_id)
    child = observation(
        "event",
        "cal",
        parent=parent_id,
        blobs=(BlobReference(key="immutable", sha256="a" * 64, size_bytes=7),),
    )
    initial = await capture(database, service, fence, parent, child)
    child_id = initial.changes[1].record.id
    tombstone = parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"})
    await capture(database, service, fence, tombstone)

    # Simulate a crash after the parent transaction, before any child cleanup.
    async with database() as db:
        persisted = await db.get(Entity, child_id)
        assert persisted.deleted_at is None
        exact = await service.store.read(db, fence.organization_id, fence.sync_id, child_id)
        assert exact.content_access == "unavailable"
        assert exact.payload == {} and exact.blobs == ()
        visible = await CanonicalQueryStore().list_records(
            db, fence.organization_id, fence.sync_id, RecordFilters(), after_id=None, limit=100
        )
        assert not visible
        history = await service.store.changes(db, fence.organization_id, fence.sync_id)
        assert all(change.record.content_access == "unavailable" for change in history.changes)
        assert all(change.record.payload == {} for change in history.changes)
        snapshot = await db.scalar(select(EntityChange.snapshot).where(EntityChange.sequence == 2))
        assert snapshot["payload"]["summary"] == "Synthetic event"
        assert snapshot["blobs"][0]["key"] == "immutable"

    replay = await capture(database, service, fence, tombstone)
    assert replay.unchanged == 1
    async with database() as db:
        swept = await service.reconcile_parents(db, fence)
    assert len(swept.capture.changes) == 1
    assert swept.capture.changes[0].record.id == child_id
    assert swept.capture.changes[0].record.removal_reason == "access_revoked"
    async with database() as db:
        repeated = await service.reconcile_parents(db, fence)
    assert not repeated.capture.changes
    async with database() as db:
        history = await service.store.changes(db, fence.organization_id, fence.sync_id)
        assert [item.sequence for item in history.changes] == [1, 2, 3, 4]
        assert history.changes[-1].kind == "delete"
        assert history.changes[-1].record.content_access == "unavailable"


@pytest.mark.parametrize("reason", ["access_revoked", "scope_removed"])
async def test_revoked_root_tombstones_redact_retained_payload(database, source, reason):
    service, fence = source
    await bind_projection(database, fence)
    record = observation(kind="delete", removal_reason=reason)
    result = await capture(database, service, fence, record)
    async with database() as db:
        exact = await service.store.read(
            db, fence.organization_id, fence.sync_id, result.changes[0].record.id
        )
        assert exact.content_access == "unavailable" and exact.payload == {}
        listed = await CanonicalQueryStore().list_records(
            db,
            fence.organization_id,
            fence.sync_id,
            RecordFilters(state="deleted"),
            after_id=None,
            limit=100,
        )
        assert len(listed) == 1
        assert listed[0].content_access == "unavailable" and listed[0].payload == {}
