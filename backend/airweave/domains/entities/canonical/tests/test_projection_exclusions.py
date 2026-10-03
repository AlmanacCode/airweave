"""Deliberate search exclusions publish normally without losing retained originals."""

from contextlib import asynccontextmanager
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_inputs import ProjectionInputs
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.native_ingestion.tests.test_ingestion import NOW, bind, ingest
from airweave.domains.native_ingestion.tests.test_projection import knowledge, message, session
from airweave.models import Entity


@pytest.mark.parametrize(
    "kind",
    [
        "meeting_listing",
        "scratchpad_listing",
        "knowledge",
        "message",
        "event_occurrence",
        "event",
    ],
)
async def test_excluded_originals_publish_zero_documents(database, source, kind):
    service, fence = source
    if kind in {"knowledge", "message"}:
        provider = "almanac"
        await bind(database, fence, dataset="knowledge" if kind == "knowledge" else "sessions")
        snapshots = (
            (knowledge(archived_at=NOW.isoformat()),)
            if kind == "knowledge"
            else (
                session(),
                message(
                    "Original retained",
                    import_provenance={
                        "legacy_row_id": 1,
                        "active": False,
                        "compacted": True,
                    },
                ),
            )
        )
        await ingest(database, fence, *snapshots)
    else:
        provider = "wispr" if kind.endswith("listing") else "google_calendar"
        await bind_projection(database, fence, provider)
        payload = {"id": "original", "title": "Original retained"}
        if kind == "event":
            payload.update(
                status="cancelled",
                recurringEventId="series",
                originalStartTime={"date": "2026-10-01"},
            )
        await capture(
            database,
            service,
            fence,
            observation(
                identity=RecordIdentity(record_type=kind, native_id="original"),
                payload=payload,
                completeness="metadata_only",
            ),
        )
    store = CanonicalProjectionStore()
    async with database() as db:
        work = next(
            item
            for item in await store.pending(db, fence.organization_id, fence.sync_id)
            if item.record.identity.record_type == kind
        )
    processor, storage = MagicMock(), MagicMock()
    destination = MagicMock(collection_id=work.binding.collection_id)
    projector = CanonicalProjector(store, lambda _organization: database(), processor, storage)
    assert (await projector.project_one(work, provider, destination, MagicMock())).published
    processor.build_text.assert_not_called()
    destination.feed_prepared.assert_not_called()
    reader = CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test")
    async with database() as db:
        retained = await reader.read(db, fence.organization_id, fence.sync_id, work.record.id)
        assert retained.payload == work.record.payload
        row = await db.scalar(select(Entity).where(Entity.id == work.record.id))
        assert row.indexed_generation is not None and row.indexed_chunk_count == 0
        assert row.projection_error is None
        assert work.record.id not in {
            item.record.id for item in await store.pending(db, fence.organization_id, fence.sync_id)
        }


@pytest.mark.parametrize("record_type", ["unknown", "event_occurrence", "event"])
async def test_unknown_empty_projection_remains_an_error(
    database, source, monkeypatch, record_type
):
    service, fence = source
    binding = await bind_projection(database, fence, "wispr")
    await capture(
        database,
        service,
        fence,
        observation(
            identity=RecordIdentity(record_type=record_type, native_id="original"),
            payload={
                "status": "cancelled",
                "recurringEventId": "series",
                "originalStartTime": {"date": "2026-10-01"},
            },
            completeness="metadata_only",
        ),
    )
    store = CanonicalProjectionStore()
    async with database() as db:
        (work,) = await store.pending(db, fence.organization_id, fence.sync_id)

    @asynccontextmanager
    async def empty_mapper(*args):
        yield ProjectionInputs(parts=())

    monkeypatch.setattr(
        "airweave.domains.entities.canonical.projection_mappers.map_record", empty_mapper
    )
    projector = CanonicalProjector(
        store, lambda _organization: database(), MagicMock(), MagicMock()
    )
    with pytest.raises(ValueError, match="no required content"):
        await projector.project_one(
            work, "wispr", MagicMock(collection_id=binding.collection_id), MagicMock()
        )
    generation = uuid4()
    async with database() as db:
        assert await store.prepare(
            db,
            work,
            generation,
            binding.collection_id,
            (),
            coverage=ExtractionCoverage(parts=()),
            text_representations=(),
        )
    async with database() as db:
        with pytest.raises(ValueError, match="indexed content or explicit unavailable extraction"):
            await store.publish(db, work, generation, 0)
