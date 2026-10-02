"""Real transactions prove version admission, rollback and retained access state."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CaptureRecord,
    RecordIdentity,
    RemovedScope,
)
from airweave.domains.entities.canonical.store import (
    CanonicalRecordStore,
    CanonicalStoreError,
    StaleWriter,
    content_is_available,
)
from airweave.domains.entities.canonical.tests.helpers import bind_projection
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.models import (
    IngestNativeBatch,
    NativeSnapshot,
    RecordVersion,
    SessionVersion,
)
from airweave.domains.native_ingestion.service import NativeIngestionService
from airweave.domains.native_ingestion.store import NativeIngestionStore
from airweave.models.entity import Entity
from airweave.models.entity_change import EntityChange
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync

NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def snapshot(native_id="one", revision=1, **changes):
    """Keep original Unicode JSON distinct from index text."""
    fields = {
        "owner_id": "owner-one",
        "identity": RecordIdentity(record_type="knowledge", native_id=native_id),
        "version": RecordVersion(revision=revision),
        "original": {"body": "नमस्ते — original", "references": ["people/sam"]},
    }
    return NativeSnapshot(**(fields | changes))


async def bind(database, fence, *, dataset="knowledge", provider="almanac"):
    binding = await bind_projection(database, fence, provider)
    async with database() as db:
        source = await db.get(SourceConnection, binding.source_connection_id)
        source.config_fields = {"owner_id": "owner-one", "dataset": dataset}
        await db.commit()


async def ingest(database, fence, *snapshots):
    service = NativeIngestionService(NativeIngestionStore(CanonicalRecordStore()))
    async with database() as db:
        return await service.ingest(
            db, IngestNativeBatch(fence=fence, observed_at=NOW, snapshots=snapshots)
        )


async def retained(database, fence):
    async with database() as db:
        return list((await db.scalars(select(Entity).where(Entity.sync_id == fence.sync_id))).all())


async def test_atomic_versions_retry_tombstone_and_stale_resurrection(database, source):
    _, fence = source
    await bind(database, fence)
    first = snapshot(revision=3)
    result = await ingest(database, fence, first)
    assert result.sequence == 1
    assert (await ingest(database, fence, first)).unchanged == 1
    for invalid in (snapshot(revision=2), snapshot(revision=3, original={"body": "conflict"})):
        with pytest.raises(NativeAdmissionError):
            await ingest(database, fence, snapshot("second"), invalid)
        rows = await retained(database, fence)
        assert len(rows) == 1 and rows[0].record_revision == 1
    deleted = snapshot(revision=4, operation="delete", original={})
    result = await ingest(database, fence, deleted)
    assert result.sequence == 2 and result.changes[0].kind == "delete"
    with pytest.raises(NativeAdmissionError, match="stale"):
        await ingest(database, fence, first)
    assert (await ingest(database, fence, deleted)).unchanged == 1
    result = await ingest(database, fence, snapshot(revision=5))
    assert result.sequence == 3 and result.changes[0].record.deleted_at is None


async def test_session_versions_are_partial_order_and_original_payload_retained(database, source):
    _, fence = source
    await bind(database, fence, dataset="sessions")
    item = snapshot(
        identity=RecordIdentity(record_type="session", native_id="session-one"),
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=3, content_revision=7),
    )
    await ingest(database, fence, item)
    with pytest.raises(ValidationError):
        SessionVersion.model_validate({"kind": "session", "revision": 3, "content_revision": 7})
    recreated = item.model_copy(
        update={"version": item.version.model_copy(update={"created_at": NOW + timedelta(days=1)})}
    )
    with pytest.raises(NativeAdmissionError, match="incarnation"):
        await ingest(database, fence, recreated)
    with pytest.raises(NativeAdmissionError, match="incomparable"):
        await ingest(
            database,
            fence,
            item.model_copy(update={"version": SessionVersion(created_at="2026-10-01T00:00:00Z", revision=4, content_revision=6)}),
        )
    higher = item.model_copy(update={"version": SessionVersion(created_at="2026-10-01T00:00:00Z", revision=3, content_revision=8)})
    result = await ingest(database, fence, higher)
    assert result.changes[0].record.payload["original"] == item.original
    assert result.changes[0].record.payload["representation"] == "snapshot"


async def test_visibility_withdrawal_preserves_version_and_retry_does_not_restore(database, source):
    canonical, fence = source
    await bind(database, fence)
    item = snapshot(revision=3)
    await ingest(database, fence, item)
    async with database() as db:
        await canonical.remove_scope(
            db,
            fence,
            RemovedScope(record_type="knowledge", removal_reason="access_revoked", observed_at=NOW),
        )
    assert (await ingest(database, fence, item)).unchanged == 1
    row = (await retained(database, fence))[0]
    assert row.removal_reason == "access_revoked"
    assert row.source_payload["version"]["revision"] == 3
    with pytest.raises(NativeAdmissionError, match="renewal"):
        await ingest(database, fence, snapshot(revision=4))


async def test_bound_source_owner_malformed_state_and_superseded_writer(database, source):
    canonical, fence = source
    with pytest.raises(NativeAdmissionError, match="not bound"):
        await ingest(database, fence, snapshot())
    await bind(database, fence)
    with pytest.raises(NativeAdmissionError, match="bound native source"):
        await ingest(database, fence, snapshot(owner_id="other-owner"))
    async with database() as db:
        await canonical.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    CaptureRecord(
                        identity=snapshot().identity, payload={"original": {}}, observed_at=NOW
                    ),
                ),
            ),
        )
    with pytest.raises(NativeAdmissionError, match="malformed"):
        await ingest(database, fence, snapshot())
    async with database() as db:
        await canonical.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    with pytest.raises(StaleWriter):
        await ingest(database, fence, snapshot("second"))
    assert len(await retained(database, fence)) == 1


async def test_provider_binding_cannot_be_used_for_native_ingestion(database, source):
    _, fence = source
    await bind(database, fence, provider="gmail")
    with pytest.raises(NativeAdmissionError, match="not bound"):
        await ingest(database, fence, snapshot())


async def test_capture_failure_rolls_back_batch_and_parent_withdrawal_hides_children(
    database, source
):
    canonical, fence = source
    await bind(database, fence, dataset="sessions")
    parent = snapshot(
        identity=RecordIdentity(record_type="session", native_id="session-one"),
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=1, content_revision=1),
    )
    missing = RecordIdentity(record_type="session", native_id="missing")
    late_parent = parent.model_copy(update={"identity": missing})
    child = snapshot(
        identity=RecordIdentity(
            record_type="message", native_id="message-one", container_id="missing"
        ),
        parent=missing,
        version=parent.version,
        original={"role": "user", "content": "original message"},
    )
    with pytest.raises(CanonicalStoreError):
        # All pass admission; the child arrives before its parent in capture order.
        await ingest(database, fence, parent, child, late_parent)
    assert await retained(database, fence) == []
    async with database() as db:
        assert (
            await db.scalar(select(Sync.observed_change_sequence).where(Sync.id == fence.sync_id))
            == 0
        )
        assert (
            await db.scalar(select(EntityChange.id).where(EntityChange.sync_id == fence.sync_id))
            is None
        )
    child = child.model_copy(
        update={
            "parent": parent.identity,
            "identity": RecordIdentity(
                record_type="message", native_id="message-one", container_id="session-one"
            ),
        }
    )
    result = await ingest(database, fence, parent, child)
    assert result.sequence == 2 and [change.sequence for change in result.changes] == [1, 2]
    async with database() as db:
        await canonical.remove_scope(
            db,
            fence,
            RemovedScope(record_type="session", removal_reason="access_revoked", observed_at=NOW),
        )
    assert (await ingest(database, fence, child)).unchanged == 1
    async with database() as db:
        visible = await db.scalar(
            select(content_is_available()).where(
                Entity.sync_id == fence.sync_id, Entity.native_id == "message-one"
            )
        )
        assert visible is False
    higher = child.model_copy(update={"version": SessionVersion(created_at="2026-10-01T00:00:00Z", revision=1, content_revision=2)})
    with pytest.raises(CanonicalStoreError):
        await ingest(database, fence, higher)
    rows = await retained(database, fence)
    message = next(row for row in rows if row.native_id == "message-one")
    assert message.record_revision == 1


async def test_late_unseen_message_cannot_join_newer_session(database, source):
    _, fence = source
    await bind(database, fence, dataset="sessions")
    parent = snapshot(
        identity=RecordIdentity(record_type="session", native_id="session-one"),
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=3, content_revision=10),
    )
    await ingest(database, fence, parent)
    child = snapshot(
        identity=RecordIdentity(
            record_type="message", native_id="late", container_id="session-one"
        ),
        parent=parent.identity,
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=2, content_revision=8),
    )
    with pytest.raises(NativeAdmissionError, match="attested native session"):
        await ingest(database, fence, child)
    assert len(await retained(database, fence)) == 1
    current = child.model_copy(update={"version": parent.version})
    assert (await ingest(database, fence, current)).sequence == 2
    advanced = parent.model_copy(
        update={"version": SessionVersion(created_at="2026-10-01T00:00:00Z", revision=3, content_revision=11)}
    )
    assert (await ingest(database, fence, advanced)).sequence == 3
    assert (await ingest(database, fence, current)).unchanged == 1
    unseen = current.model_copy(
        update={
            "identity": RecordIdentity(
                record_type="message", native_id="unseen", container_id="session-one"
            )
        }
    )
    with pytest.raises(NativeAdmissionError, match="attested native session"):
        await ingest(database, fence, unseen)
    stale_delete = current.model_copy(update={"operation": "delete", "original": {}})
    with pytest.raises(NativeAdmissionError, match="conflicting content"):
        await ingest(database, fence, stale_delete)
    # A later child version can still be stale relative to the parent.
    newest = advanced.model_copy(
        update={"version": SessionVersion(created_at="2026-10-01T00:00:00Z", revision=3, content_revision=12)}
    )
    await ingest(database, fence, newest)
    stale_delete = stale_delete.model_copy(update={"version": advanced.version})
    with pytest.raises(NativeAdmissionError, match="attested native session"):
        await ingest(database, fence, stale_delete)
    current_delete = stale_delete.model_copy(update={"version": newest.version})
    result = await ingest(database, fence, current_delete)
    assert result.changes[0].kind == "delete"
    # Parent tombstones permit matching child tombstones, but never child upserts.
    parent_delete = newest.model_copy(
        update={
            "version": SessionVersion(created_at="2026-10-01T00:00:00Z", revision=3, content_revision=13),
            "operation": "delete",
            "original": {},
        }
    )
    child_delete = current_delete.model_copy(update={"version": parent_delete.version})
    result = await ingest(database, fence, parent_delete, child_delete)
    assert [change.kind for change in result.changes] == ["delete", "delete"]
