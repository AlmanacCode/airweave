"""Synthetic real SQL/HTTP Wispr meeting starts and native retained text."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update

from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.domains.entities.canonical.wispr_models import MeetingFilters, MeetingListQuery
from airweave.domains.entities.canonical.wispr_query import CanonicalWisprQuery
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def meeting(native_id, start=NOW, *, body_changes=None, listing_changes=None):
    listing = {"id": native_id, "title": "Meeting " + native_id, "start": start.isoformat()}
    listing.update(listing_changes or {})
    body = {
        **listing,
        "modified_at": (NOW + timedelta(days=10)).isoformat(),
        "has_transcript": True,
        "content": "Native notes",
        "transcript": "Native transcript",
    }
    body.update(body_changes or {})
    parent = observation(
        identity=RecordIdentity(record_type="meeting_listing", native_id=native_id),
        payload=listing,
        completeness="metadata_only",
    )
    child = observation(
        identity=RecordIdentity(record_type="meeting", native_id=native_id),
        parent=parent.identity,
        payload={
            "listing": listing,
            "responses": [
                {
                    "requested_ranges": {
                        "view_content": {"start_char": 0, "char_limit": 40000},
                        "view_transcript": {"start_char": 0, "char_limit": 40000},
                    },
                    "response": body,
                }
            ],
        },
        completeness="partial",
        source_updated_at=NOW + timedelta(days=10),
    )
    return parent, child


async def read(database, fence, **options):
    async with database() as db:
        return await CanonicalWisprQuery("meeting-test-key").meetings(
            db, fence.organization_id, fence.sync_id, MeetingListQuery(**options)
        )


async def test_newest_meetings_over_200_native_start_gaps_and_ancestor_gate(database, source):
    service, fence = source
    await bind_projection(database, fence, "wispr")
    records = [
        record
        for index in range(260)
        for record in meeting(str(index), NOW + timedelta(minutes=index))
    ]
    records.extend(
        meeting("missing", body_changes={"start": None}, listing_changes={"start": None})
    )
    records.extend(
        meeting("mismatch", body_changes={"start": (NOW + timedelta(days=1)).isoformat()})
    )
    records.extend(meeting("malformed-title", body_changes={"title": ["not a title"]}))
    for start in range(0, len(records), 400):
        await capture(database, service, fence, *records[start : start + 400])
    page = await read(database, fence)
    assert [item.native_id for item in page.meetings] == [
        str(index) for index in range(259, 254, -1)
    ]
    assert page.metadata_missing == 3
    seen, cursor = [], None
    while True:
        page = await read(database, fence, limit=43, cursor=cursor)
        seen.extend(page.meetings)
        if not page.has_more:
            break
        assert page.next_cursor and page.next_cursor != cursor
        cursor = page.next_cursor
    assert len(seen) == len({item.id for item in seen}) == 260
    assert [item.native_id for item in seen] == [str(index) for index in range(259, -1, -1)]
    bounded = await read(
        database,
        fence,
        filters=MeetingFilters(
            after=NOW + timedelta(minutes=5), before=NOW + timedelta(minutes=10)
        ),
    )
    assert [item.native_id for item in bounded.meetings] == ["9", "8", "7", "6", "5"]
    async with database() as db:
        assert all(item is None for item in await db.scalars(select(Entity.source_created_at)))
        await db.execute(update(Sync).where(Sync.id == fence.sync_id).values(status="paused"))
        await db.commit()
    assert (await read(database, fence)).meetings  # Scheduling pause retains authorized originals.
    parent, _ = meeting("259", NOW + timedelta(minutes=259))
    await capture(
        database,
        service,
        fence,
        parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    assert [item.native_id for item in (await read(database, fence)).meetings] == [
        "258",
        "257",
        "256",
        "255",
        "254",
    ]


async def test_meetings_http_cursor_scope_sequence_and_post_read_revocation(
    database, source, monkeypatch
):
    service, fence = source
    await bind_projection(database, fence, "wispr")
    await capture(database, service, fence, *meeting("a"), *meeting("b", NOW + timedelta(hours=1)))
    owner = [fence.organization_id]
    app = query_app(database, lambda: SimpleNamespace(organization=SimpleNamespace(id=owner[0])))
    base = f"/sync/{fence.sync_id}/wispr/meetings"
    async with AsyncClient(transport=ASGITransport(app), base_url="http://fixture") as client:
        first = await client.get(base, params={"limit": 1})
        assert first.status_code == 200
        body = first.json()
        assert first.headers["cache-control"] == "private, no-store"
        assert body["meetings"][0]["native_id"] == "b" and "capture" in body
        assert not any(key in body["meetings"][0] for key in ("content", "transcript", "payload"))
        cursor = body["next_cursor"]
        assert (await client.get(base, params={"limit": 1, "cursor": cursor})).json()["meetings"][
            0
        ]["native_id"] == "a"
        assert (await client.get(base, params={"cursor": "bad"})).status_code == 400
        assert (
            await client.get(base, params={"cursor": cursor, "after": NOW.isoformat()})
        ).status_code == 400
        assert (await client.get(base, params={"after": "2026-01-01T00:00:00"})).status_code == 422
        assert (
            await client.get(base, params={"after": NOW.isoformat(), "before": NOW.isoformat()})
        ).status_code == 422
        owner[0] = uuid4()
        assert (await client.get(base)).status_code == 404
        owner[0] = fence.organization_id
        await capture(database, service, fence, *meeting("new"))
        assert (await client.get(base, params={"cursor": cursor})).status_code == 409
        import airweave.domains.entities.canonical.wispr_query as module

        original = module.capture_coverage

        async def revoke(db, organization, syncs):
            result = await original(db, organization, syncs)
            async with database() as separate:
                await separate.execute(
                    update(SourceConnection)
                    .where(SourceConnection.sync_id.in_(syncs))
                    .values(is_authenticated=False)
                )
                await separate.commit()
            return result

        monkeypatch.setattr(module, "capture_coverage", revoke)
        assert (await client.get(base)).status_code == 404


async def test_wispr_backfill_preserves_originals_and_creation_dates(database, source):
    import importlib.util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    service, fence = source
    await bind_projection(database, fence, "wispr")
    await capture(
        database,
        service,
        fence,
        *meeting("legacy"),
        *meeting("bad", body_changes={"start": "naive"}),
    )
    path = Path(__file__).resolve().parents[5] / "alembic/versions/0015_retained_wispr_meetings.py"
    spec = importlib.util.spec_from_file_location("wispr_backfill_test", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def migrate(connection):
        with Operations.context(MigrationContext.configure(connection)):
            migration.downgrade()
            migration.upgrade()

    async with database() as db:
        original = [
            (row.id, row.source_payload, row.record_revision, row.source_created_at)
            for row in await db.scalars(select(Entity).order_by(Entity.id))
        ]
        sequence = (await db.get(Sync, fence.sync_id)).observed_change_sequence
        await (await db.connection()).run_sync(migrate)
        await db.commit()
    page = await read(database, fence)
    assert [item.native_id for item in page.meetings] == ["legacy"] and page.metadata_missing == 1
    async with database() as db:
        current = [
            (row.id, row.source_payload, row.record_revision, row.source_created_at)
            for row in await db.scalars(select(Entity).order_by(Entity.id))
        ]
        assert current == original
        assert (await db.get(Sync, fence.sync_id)).observed_change_sequence == sequence


async def test_native_notes_transcript_multirange_exact_content_reads(database, source, tmp_path):
    from unittest.mock import AsyncMock, MagicMock

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.converters.registry import ConverterRegistry
    from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
    from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
    from airweave.domains.entities.canonical.projector import CanonicalProjector
    from airweave.domains.entities.canonical.query import CanonicalQueryService
    from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
    from airweave.domains.entities.canonical.store import CanonicalRecordStore
    from airweave.domains.entities.canonical.text_query import CanonicalTextReader
    from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
    from airweave.platform.destinations.vespa.transformer import EntityTransformer

    service, fence = source
    binding = await bind_projection(database, fence, "wispr")
    notes = "Notes: " + "n" * 40000 + "\nUnicode résumé ending."
    transcript = "Speaker: " + "t" * 40000 + "\nFull transcript ending."
    summary = "Provider Flow Summary: स्वीकृत résumé\n"
    parent, original = meeting("ranged")
    first = original.payload["responses"][0]["response"]
    responses = []
    for offset in (0, 40000):
        body = dict(first)
        body["summary"] = summary
        for field, value in (("content", notes), ("transcript", transcript)):
            fragment = value[offset : offset + 40000]
            if offset == 0:
                fragment += (
                    f"\n\n(...truncated, {len(value) - 40000} chars remaining; "
                    f"continue with view_{field}.start_char=40000...)"
                )
            body[field] = fragment
        responses.append(
            {
                "requested_ranges": {
                    "view_content": {"start_char": offset, "char_limit": 40000},
                    "view_transcript": {"start_char": offset, "char_limit": 40000},
                },
                "response": body,
            }
        )
    original = original.model_copy(
        update={"payload": {"listing": parent.payload, "responses": responses}}
    )
    await capture(database, service, fence, parent, original)
    store = CanonicalProjectionStore()
    async with database() as db:
        work = next(
            item
            for item in await store.pending(db, fence.organization_id, fence.sync_id)
            if item.record.identity.record_type == "meeting"
        )
    storage = FilesystemBackend(tmp_path)
    processor = ChunkEmbedProcessor(ConverterRegistry(), FakeDenseEmbedder(), FakeSparseEmbedder())

    async def fixed_chunks(entities, context, runtime):
        return processor._multiply_entities(
            entities,
            [
                [
                    {
                        "text": entity.textual_representation,
                        "start_index": 0,
                        "end_index": len(entity.textual_representation),
                    }
                ]
                for entity in entities
            ],
            context,
        )

    processor._chunk_entities = fixed_chunks
    destination = MagicMock(collection_id=binding.collection_id, feed_prepared=AsyncMock())
    destination.prepare_documents = lambda chunks: {
        "base_entity": [
            EntityTransformer(collection_id=binding.collection_id).transform(chunk)
            for chunk in chunks
        ]
    }
    projector = CanonicalProjector(store, lambda _organization: database(), processor, storage)
    assert (await projector.project_one(work, "wispr", destination, MagicMock())).published
    records = CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "meeting-test-key"
    )
    reader = CanonicalTextReader(records, storage)
    async with database() as db:
        assert (
            await records.read(db, fence.organization_id, fence.sync_id, work.record.id)
        ).payload == original.payload
        listed = await reader.list(db, fence.organization_id, fence.sync_id, work.record.id, 1)
        assert listed.status == "available"
        expected = {"notes": notes, "transcript": transcript, "summary": summary}
        assert {part.part_key for part in listed.representations} == set(expected)
        for part in listed.representations:
            assert part.kind == "native_text"
            segments, offset = [], 0
            while True:
                page = await reader.read(
                    db,
                    fence.organization_id,
                    fence.sync_id,
                    work.record.id,
                    1,
                    part.generation,
                    part.id,
                    offset=offset,
                    limit=12000,
                )
                segments.append(page.text)
                if page.next_offset is None:
                    break
                assert page.next_offset > offset
                offset = page.next_offset
            assert "".join(segments) == expected[part.part_key]
            assert not "".join(segments).startswith("# Metadata")


async def test_transcript_absent_unknown_and_missing_text(database, source, tmp_path):
    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.entities.canonical.projection_mappers import (
        ProjectionMappingError,
        map_record,
    )
    from airweave.domains.entities.canonical.query import CanonicalQueryService
    from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
    from airweave.domains.entities.canonical.store import CanonicalRecordStore

    service, fence = source
    await bind_projection(database, fence, "wispr")
    await capture(
        database,
        service,
        fence,
        *meeting("absent", body_changes={"has_transcript": False, "transcript": None}),
        *meeting(
            "unknown",
            body_changes={"has_transcript": None, "transcript": "Captured despite unknown flag"},
        ),
        *meeting("missing-text", body_changes={"has_transcript": None, "transcript": None}),
    )
    page = await read(database, fence, limit=100)
    assert {item.native_id: item.has_transcript for item in page.meetings} == {
        "absent": False,
        "unknown": None,
        "missing-text": None,
    }
    records = CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "meeting-test-key"
    )
    for item in page.meetings:
        async with database() as db:
            record = await records.read(db, fence.organization_id, fence.sync_id, item.id)
        if item.native_id == "missing-text":
            with pytest.raises(ProjectionMappingError, match="unavailable or malformed"):
                async with map_record(record, "wispr", FilesystemBackend(tmp_path)):
                    pass
        else:
            async with map_record(record, "wispr", FilesystemBackend(tmp_path)) as mapped:
                assert [part.part.key for part in mapped.parts] == (
                    ["notes"] if item.native_id == "absent" else ["notes", "transcript"]
                )
                assert mapped.parts[0].native_body.text == "Native notes"


def test_native_start_identity_preview_types_and_offsets():
    from airweave.domains.entities.canonical.wispr_facts_v1 import meeting_started_at_v1

    _, item = meeting("one", body_changes={"start": "2026-01-01T05:30:00+05:30"})
    assert meeting_started_at_v1(item.payload, "one") == NOW
    for changes in (
        {"start": "2026-01-01T00:00:00"},
        {"start": "2026"},
        {"start": 2026},
        {"id": "different"},
        {"has_transcript": "false"},
    ):
        _, malformed = meeting("one", body_changes=changes)
        assert meeting_started_at_v1(malformed.payload, "one") is None
    conflicting = {
        **item.payload,
        "responses": [
            *item.payload["responses"],
            {"response": {"id": "one", "has_transcript": False}},
        ],
    }
    assert meeting_started_at_v1(conflicting, "one") is None
