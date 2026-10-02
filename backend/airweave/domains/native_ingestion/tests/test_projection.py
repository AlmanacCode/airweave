"""Native model-shaped fixtures; real SQL publication, synthetic embedding/destination."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.projection_policy import excluded_from_search
from airweave.domains.entities.canonical.projection_store import (
    CanonicalProjectionStore,
    current_extraction,
)
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.test_extraction_coverage import (
    destination,
    projector,
)
from airweave.domains.entities.canonical.text_query import CanonicalTextReader
from airweave.domains.native_ingestion.models import RecordVersion, SessionVersion
from airweave.domains.native_ingestion.tests.test_ingestion import NOW, bind, ingest, snapshot


def knowledge(**changes):
    original = {
        "id": "one",
        "type": "page",
        "revision": 1,
        "title": "Fundraising",
        "description": "A useful original page",
        "path": "notes/fundraising",
        "body": "नमस्ते 👋\n# Original body",
        "created_at": NOW.isoformat(),
        "updated_at": NOW.isoformat(),
        "archived_at": None,
        "citations": {},
        "field_citations": {},
        "aliases": [],
        "image": None,
        "about": [],
        "user_notes": "A note",
    }
    return snapshot(original=original | changes, source_created_at=NOW, source_updated_at=NOW)


def session():
    return snapshot(
        identity=RecordIdentity(record_type="session", native_id="s1"),
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=1, content_revision=1),
        source_created_at=NOW,
        source_updated_at=NOW,
        original={
            "id": "s1",
            "title": "Session title",
            "title_source": "user",
            "description": "Original session description",
            "source": "chat",
            "metadata": {},
            "archived": True,
            "pinned": False,
            "revision": 1,
            "content_revision": 1,
            "replay_epoch": 4,
            "message_count": 1,
            "earlier_message_count": 0,
            "project": None,
            "effective_project": None,
            "work": None,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
        },
    )


def message(content, **changes):
    parent = session()
    original = {
        "id": "m1",
        "input_id": None,
        "ordinal": 0,
        "import_provenance": None,
        "payload": {
            "role": "user",
            "content": content,
            "api_content": "DO NOT INDEX",
            "reasoning_content": "PRIVATE REPLAY TEXT",
        },
        "created_at": NOW.isoformat(),
    }
    return snapshot(
        identity=RecordIdentity(record_type="message", native_id="m1", container_id="s1"),
        parent=parent.identity,
        version=parent.version,
        original=original | changes,
        source_created_at=NOW,
    )


async def test_knowledge_identity_rename_archive_and_malformed_original(database, source):
    _, fence = source
    await bind(database, fence)
    result = await ingest(database, fence, knowledge())
    record = result.changes[0].record
    async with map_record(record, "almanac", AsyncMock()) as mapped:
        assert mapped.parts[0].entity.entity_id == "one"
        assert mapped.parts[0].native_body.text == knowledge().original["body"]
    renamed = knowledge(path="notes/renamed", revision=2).model_copy(
        update={"version": RecordVersion(revision=2)}
    )
    record = (await ingest(database, fence, renamed)).changes[0].record
    async with map_record(record, "almanac", AsyncMock()) as mapped:
        assert mapped.parts[0].entity.entity_id == "one"
        assert mapped.parts[0].entity.path == "notes/renamed"
    bad = record.model_copy(
        update={
            "payload": renamed.model_copy(
                update={"original": renamed.original | {"id": "wrong"}}
            ).model_dump(mode="json")
        }
    )
    with pytest.raises(ValueError, match="ID differs"):
        async with map_record(bad, "almanac", AsyncMock()):
            pass
    archived = renamed.model_copy(
        update={"original": renamed.original | {"archived_at": NOW.isoformat()}}
    )
    assert excluded_from_search(
        record.model_copy(update={"payload": archived.model_dump(mode="json")}), "almanac"
    )


async def test_mixed_message_publishes_partial_coverage_and_exact_native_text(
    database, source, tmp_path
):
    _, fence = source
    await bind(database, fence, dataset="sessions")
    text = "नमस्ते 👋\n# Content\nOriginal user words"
    item = message(
        [
            {"type": "text", "text": text},
            {"type": "image_url", "image_url": {"url": "https://example.org/image"}},
        ]
    )
    result = await ingest(database, fence, session(), item)
    assert not excluded_from_search(
        result.changes[0].record, "almanac"
    )  # Archived sessions included today.
    storage = FilesystemBackend(tmp_path)
    projection = projector(database, storage)
    indexed = []

    async def fixed_chunks(entities, context, runtime):
        indexed.extend(entity.textual_representation for entity in entities)
        return projection._processor._multiply_entities(
            entities, [[{"text": entity.textual_representation}] for entity in entities], context
        )

    projection._processor._chunk_entities = fixed_chunks
    async with database() as db:
        pending = await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id)
    work = next(work for work in pending if work.record.identity.record_type == "message")
    async with map_record(work.record, "almanac", storage) as mapped:
        assert mapped.parts[0].entity.message_id == "m1"
        assert mapped.parts[0].entity.session_id == "s1"
        assert mapped.parts[0].entity.block_index == 0
        assert mapped.parts[0].native_body.text == text
        assert mapped.parts[1].omission == "unsupported_format"
    assert await projection.project_one(
        work, "almanac", destination(work.binding.collection_id), MagicMock()
    )
    assert indexed[0].endswith(text)
    assert "DO NOT INDEX" not in indexed[0] and "PRIVATE REPLAY TEXT" not in indexed[0]
    async with database() as db:
        coverage = await current_extraction(
            db, fence.organization_id, fence.sync_id, work.record.id, work.record.revision
        )
    assert coverage.status == "partial"
    assert [part.outcome for part in coverage.parts] == ["indexed", "unsupported"]
    reader = CanonicalTextReader(
        CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test-key"), storage
    )
    async with database() as db:
        representations = await reader.list(
            db, fence.organization_id, fence.sync_id, work.record.id, work.record.revision
        )
        ref = representations.representations[0]
        native = await reader.read(
            db,
            fence.organization_id,
            fence.sync_id,
            work.record.id,
            work.record.revision,
            ref.generation,
            ref.id,
        )
    assert ref.kind == "native_text" and native.text == text


async def test_inactive_import_and_invalid_text_are_not_successful_empty_indexing(database, source):
    _, fence = source
    await bind(database, fence, dataset="sessions")
    inactive = message(
        "old", import_provenance={"legacy_row_id": 1, "active": False, "compacted": True}
    )
    result = await ingest(database, fence, session(), inactive)
    assert excluded_from_search(result.changes[1].record, "almanac")
    malformed = message([{"type": "text", "text": {"unexpected": "object"}}])
    record = result.changes[1].record.model_copy(
        update={"payload": malformed.model_dump(mode="json")}
    )
    with pytest.raises(ValueError):
        async with map_record(record, "almanac", AsyncMock()):
            pass


async def test_person_fields_enter_index_without_rewriting_native_body(database, source, tmp_path):
    _, fence = source
    await bind(database, fence)
    item = knowledge(
        type="person",
        emails=["sam@example.org"],
        roles=[
            {
                "organisation": {"type": "organisation", "id": "opaque-id"},
                "title": "Research engineer",
                "start": "2024",
                "end": None,
            }
        ],
        links=[{"label": "Profile", "url": "https://example.org/sam", "handle": "samdev"}],
        metadata={"secret": "UNSELECTED_METADATA"},
    )
    captured = await ingest(database, fence, item)
    storage = FilesystemBackend(tmp_path)
    projection = projector(database, storage)
    indexed = []

    async def fixed_chunks(entities, context, runtime):
        indexed.extend(entity.textual_representation for entity in entities)
        return projection._processor._multiply_entities(
            entities, [[{"text": entity.textual_representation}] for entity in entities], context
        )

    projection._processor._chunk_entities = fixed_chunks
    async with database() as db:
        work = (await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id))[
            0
        ]
    assert await projection.project_one(
        work, "almanac", destination(work.binding.collection_id), MagicMock()
    )
    assert "sam@example.org" in indexed[0] and "Research engineer" in indexed[0]
    assert "samdev" in indexed[0] and "2024" in indexed[0]
    assert "UNSELECTED_METADATA" not in indexed[0] and "opaque-id" not in indexed[0]
    assert captured.changes[0].record.payload["original"] == item.original
    reader = CanonicalTextReader(
        CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test-key"), storage
    )
    async with database() as db:
        representations = await reader.list(
            db, fence.organization_id, fence.sync_id, work.record.id, 1
        )
        ref = representations.representations[0]
        content = await reader.read(
            db,
            fence.organization_id,
            fence.sync_id,
            work.record.id,
            1,
            ref.generation,
            ref.id,
            view="content",
        )
        prepared = await reader.read(
            db,
            fence.organization_id,
            fence.sync_id,
            work.record.id,
            1,
            ref.generation,
            ref.id,
            view="index",
        )
    assert content.text == item.original["body"]
    assert "sam@example.org" in prepared.text


@pytest.mark.parametrize(
    "kind,fields,expected",
    [
        (
            "organisation",
            {"domains": ["example.org"], "industry": ["Robotics"], "founded": "2020"},
            "Robotics",
        ),
        (
            "place",
            {"place_kind": "Office", "address": {"locality": "मुंबई", "country": "India"}},
            "मुंबई",
        ),
        ("creative_work", {"work_kind": "Book", "published_on": "2025-01-01"}, "2025-01-01"),
    ],
)
def test_selected_knowledge_fields_preserve_native_text(kind, fields, expected):
    from airweave.domains.native_ingestion.knowledge_fields import knowledge_details

    text = knowledge_details(kind, fields | {"metadata": {"private": "UNSELECTED"}})
    assert expected in text and "UNSELECTED" not in text


def test_invalid_selected_field_fails_instead_of_stringifying_json():
    from pydantic import ValidationError

    from airweave.domains.native_ingestion.knowledge_fields import knowledge_details

    with pytest.raises(ValidationError):
        knowledge_details("person", {"emails": [{"unexpected": "value"}]})


@pytest.mark.parametrize(
    "kind,fields,expected",
    [
        (
            "task",
            {
                "state": "waiting",
                "priority": "urgent",
                "due_at": "2026-10-20T12:00:00Z",
                "available_on": "2026-10-03",
                "session": {"id": "OPAQUE_SESSION"},
            },
            ("waiting", "urgent", "2026-10-20", "2026-10-03"),
        ),
        (
            "project",
            {
                "state": "in_progress",
                "target_on": "2026-11-01",
                "people": [{"id": "OPAQUE_PERSON"}],
            },
            ("in_progress", "2026-11-01"),
        ),
    ],
)
async def test_authored_work_projects_original_body_and_selected_details(
    database, source, kind, fields, expected
):
    _, fence = source
    await bind(database, fence)
    item = knowledge(type=kind, **fields)
    record = (await ingest(database, fence, item)).changes[0].record
    assert record.payload["original"] == item.original
    async with map_record(record, "almanac", AsyncMock()) as mapped:
        entity = mapped.parts[0].entity
        assert entity.native_type == kind
        assert mapped.parts[0].native_body.text == item.original["body"]
        assert all(value in entity.details for value in expected)
        assert "OPAQUE" not in entity.details
    archived = item.model_copy(
        update={"original": item.original | {"archived_at": NOW.isoformat()}}
    )
    assert excluded_from_search(
        record.model_copy(update={"payload": archived.model_dump(mode="json")}), "almanac"
    )


@pytest.mark.parametrize(
    "fields", [{"state": "invented"}, {"state": "open", "due_at": "2026-10-20T12:00:00"}]
)
def test_malformed_work_details_fail_closed(fields):
    from pydantic import ValidationError

    from airweave.domains.native_ingestion.knowledge_fields import knowledge_details

    with pytest.raises(ValidationError):
        knowledge_details("task", fields)


def test_event_and_relationship_values_are_searchable_without_resolving_references():
    from airweave.domains.native_ingestion.knowledge_fields import knowledge_details

    assert "2026-10-01" in knowledge_details(
        "event",
        {
            "schedule": {
                "kind": "all_day",
                "start_on": "2026-10-01",
                "end_on_exclusive": "2026-10-02",
            }
        },
    )
    details = knowledge_details(
        "person",
        {
            "related_people": [
                {"person": {"type": "person", "id": "hidden-id"}, "relationship": "Co-founder"}
            ],
            "education": [{"institution": {"type": "organisation", "id": "hidden-school"}}],
        },
    )
    assert details == "related people: relationship: Co-founder"
    coordinates = knowledge_details(
        "place", {"coordinates": {"latitude": 19.07, "longitude": 72.87}}
    )
    assert "19.07" in coordinates and "72.87" in coordinates
