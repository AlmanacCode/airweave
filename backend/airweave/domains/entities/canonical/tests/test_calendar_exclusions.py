"""Cancelled instances remain recurrence facts, without becoming searchable meetings."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_models import RecordFilters, RecordListQuery
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.helpers import bind_projection
from airweave.models import Entity, ProjectionGeneration
from airweave.platform.sources.records.google_calendar import record


async def test_cancelled_exception_read_project_reinstate_and_revoke(database, source):
    capture, fence = source
    binding = await bind_projection(database, fence, "google_calendar")
    parent = record("calendar", {"id": "cal"})
    native = {
        "id": "instance",
        "status": "cancelled",
        "recurringEventId": "series",
        "originalStartTime": {"date": "2026-10-01"},
    }
    excluded = record("event", native, "cal").model_copy(update={"parent": parent.identity})
    async with database() as db:
        result = await capture.capture(db, CaptureBatch(fence=fence, records=(parent, excluded)))
    row = result.changes[-1].record
    queries = CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test-key")
    projections = CanonicalProjectionStore()
    async with database() as db:
        current = await queries.read(db, fence.organization_id, fence.sync_id, row.id)
        assert current.deleted_at is None and current.payload == native
        page = await queries.list_records(
            db,
            fence.organization_id,
            fence.sync_id,
            RecordListQuery(filters=RecordFilters(record_type="event")),
        )
        assert len(page.records) == 1 and page.records[0].payload["status"] == "cancelled"
        work = next(
            w
            for w in await projections.pending(db, fence.organization_id, fence.sync_id)
            if w.record.id == row.id
        )
    async with map_record(current, "google_calendar", AsyncMock()) as mapped:
        mapped = mapped.entities
        assert mapped == ()
    processor = MagicMock()
    processor.build_text = AsyncMock()
    destination = MagicMock()
    destination.collection_id = binding.collection_id
    destination.feed_prepared = AsyncMock()
    projector = CanonicalProjector(projections, database, processor, AsyncMock())
    assert await projector.project_one(work, "google_calendar", destination, MagicMock())
    processor.build_text.assert_not_awaited()
    destination.feed_prepared.assert_not_awaited()
    async with database() as db:
        assert all(
            w.record.id != row.id
            for w in await projections.pending(db, fence.organization_id, fence.sync_id)
        )
        manifest = await db.scalar(
            select(ProjectionGeneration).where(ProjectionGeneration.record_id == row.id)
        )
        assert manifest.documents == []
        assert (await db.get(Entity, row.id)).indexed_chunk_count == 0
    restored = record(
        "event",
        {
            **native,
            "status": "confirmed",
            "summary": "Restored",
            "start": {"date": "2026-10-01"},
            "end": {"date": "2026-10-02"},
        },
        "cal",
    ).model_copy(update={"parent": parent.identity})
    async with database() as db:
        result = await capture.capture(db, CaptureBatch(fence=fence, records=(restored,)))
        assert result.changes[0].record.id == row.id and result.changes[0].record.revision == 2
        assert any(
            w.record.id == row.id
            for w in await projections.pending(db, fence.organization_id, fence.sync_id)
        )
        current = await queries.read(db, fence.organization_id, fence.sync_id, row.id)
    async with map_record(current, "google_calendar", AsyncMock()) as mapped:
        mapped = mapped.entities
        assert len(mapped) == 1 and mapped[0].status == "confirmed"
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=(excluded,)))
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    parent.model_copy(
                        update={"kind": "delete", "removal_reason": "access_revoked"}
                    ),
                ),
            ),
        )
        hidden = await queries.read(db, fence.organization_id, fence.sync_id, row.id)
        assert hidden.content_access == "unavailable" and hidden.payload == {} and not hidden.blobs
        page = await queries.list_records(
            db,
            fence.organization_id,
            fence.sync_id,
            RecordListQuery(filters=RecordFilters(record_type="event")),
        )
        assert not page.records
        assert all(
            w.record.id != row.id
            for w in await projections.pending(db, fence.organization_id, fence.sync_id)
        )


async def test_live_event_cannot_publish_empty_or_exclusion_nonempty(database, source):
    capture, fence = source
    binding = await bind_projection(database, fence, "google_calendar")
    fact = record(
        "event",
        {
            "id": "cancelled",
            "status": "cancelled",
            "recurringEventId": "series",
            "originalStartTime": {"date": "2026-10-01"},
        },
        "cal",
    )
    async with database() as db:
        await capture.capture(db, CaptureBatch(fence=fence, records=(fact,)))
        store = CanonicalProjectionStore()
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
        with pytest.raises(ValueError, match="exclusions"):
            await store.publish(db, work, uuid4(), 1)
        active = work.model_copy(
            update={
                "record": work.record.model_copy(
                    update={"payload": {"id": "cancelled", "status": "confirmed"}}
                )
            }
        )
        generation = uuid4()
        assert await store.prepare(db, active, generation, binding.collection_id, ())
        with pytest.raises(ValueError, match="indexed content"):
            await store.publish(db, active, generation, 0)


async def test_operational_occurrences_publish_zero_documents_without_retry(database, source):
    capture, fence = source
    binding = await bind_projection(database, fence, "google_calendar")
    parent = record("calendar", {"id": "cal"})
    occurrence = record(
        "event_occurrence",
        {
            "id": "real-instance",
            "status": "confirmed",
            "start": {"date": "2026-10-01"},
            "end": {"date": "2026-10-02"},
        },
        "cal",
    ).model_copy(update={"parent": parent.identity})
    async with database() as db:
        result = await capture.capture(db, CaptureBatch(fence=fence, records=(parent, occurrence)))
    row = result.changes[-1].record
    projections = CanonicalProjectionStore()
    async with database() as db:
        work = next(
            w
            for w in await projections.pending(db, fence.organization_id, fence.sync_id)
            if w.record.id == row.id
        )
    processor = MagicMock()
    processor.build_text = AsyncMock()
    destination = MagicMock()
    destination.collection_id = binding.collection_id
    destination.feed_prepared = AsyncMock()
    projector = CanonicalProjector(projections, database, processor, AsyncMock())
    assert await projector.project_one(work, "google_calendar", destination, MagicMock())
    processor.build_text.assert_not_awaited()
    destination.feed_prepared.assert_not_awaited()
    async with database() as db:
        assert all(
            w.record.id != row.id
            for w in await projections.pending(db, fence.organization_id, fence.sync_id)
        )
        assert (await db.get(Entity, row.id)).indexed_chunk_count == 0
