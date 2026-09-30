"""Retained native Notion text, explicit limits, and existing publication authority."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import (
    ProjectionMappingError,
    map_record,
)
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import (
    CanonicalProjectionStore,
    publication_matches,
)
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import capture, observation, publish_prepared
from airweave.models import Entity, Sync

PAGE, BLOCK = str(UUID(int=1)), str(UUID(int=2))
STAMP = "2026-09-30T00:00:00Z"


def rich(text, kind="text"):
    return {"type": kind, "plain_text": text}


def original(kind="page", **fields):
    native = BLOCK if kind == "block" else PAGE
    payload = {
        "object": kind,
        "id": native,
        "created_time": STAMP,
        "last_edited_time": STAMP,
        "in_trash": False,
        **fields,
    }
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(record_type=kind, native_id=native),
        parent=RecordIdentity(record_type="page", native_id=PAGE) if kind == "block" else None,
        revision=1,
        payload=payload,
        payload_schema_version=1,
        capture_hash="hash",
        content_hash=None,
        completeness="partial",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


def block(kind, content, **fields):
    return original(
        "block",
        type=kind,
        has_children=False,
        parent={"type": "page_id", "page_id": PAGE},
        **{kind: content},
        **fields,
    )


async def mapped(record):
    storage = AsyncMock()
    async with map_record(record, "notion", storage) as entities:
        entities = entities.entities
        assert len(entities) == 1
        entity = entities[0]
    assert not storage.mock_calls
    assert record.completeness == "partial"
    assert entity.native_id == record.identity.native_id
    return entity


@pytest.mark.asyncio
async def test_root_metadata_is_not_an_assembled_page_body():
    record = original(
        properties={
            "Name": {"type": "title", "title": [rich("Launch "), rich("@Mira", "mention")]}
        },
        url=f"https://app.notion.com/p/{PAGE}",
        unknown={"private": "not prose"},
    )
    entity = await mapped(record)
    assert entity.title == "Launch @Mira" and entity.text == ""
    assert entity.web_url == record.payload["url"]
    assert entity.created_at.tzinfo is not None
    assert "Metadata only" in entity.content_coverage
    for kind in ("database", "data_source"):
        fields = {"title": [rich("Plans")], "description": [rich("Roadmap")]}
        if kind == "data_source":
            fields["properties"] = {
                "Name": {"type": "title", "title": {}},
                "Stage": {"type": "status", "status": {}},
            }
        entity = await mapped(original(kind, **fields))
        assert entity.title == "Plans" and "Roadmap" in entity.text
        assert ("Stage (status)" in entity.text) == (kind == "data_source")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,content,expected",
    [
        (
            "paragraph",
            {"rich_text": [rich("Hi "), rich("@Mira", "mention"), rich(" x²", "equation")]},
            "Hi @Mira x²",
        ),
        ("heading_2", {"rich_text": [rich("Milestones")]}, "Milestones"),
        ("to_do", {"checked": True, "rich_text": [rich("Ship")]}, "[x] Ship"),
        (
            "code",
            {
                "language": "python",
                "rich_text": [rich("print('hello')\n")],
                "caption": [rich("Example")],
            },
            "python\nprint('hello')\n\nExample",
        ),
        ("table_row", {"cells": [[rich("Owner")], [rich("Mira")]]}, "Owner | Mira"),
        ("equation", {"expression": "E=mc^2"}, "E=mc^2"),
    ],
)
async def test_supported_native_block_text(kind, content, expected):
    entity = await mapped(block(kind, content))
    assert entity.text == expected
    assert entity.web_url == ""
    assert "This block only" in entity.content_coverage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,content",
    [
        ("table", {"table_width": 2, "has_column_header": True, "has_row_header": False}),
        ("column", {}),
        ("divider", {}),
        ("synced_block", {"synced_from": None}),
    ],
)
async def test_known_structure_has_explicit_empty_body(kind, content):
    entity = await mapped(block(kind, content))
    assert entity.title and entity.text == ""
    assert any(word in entity.content_coverage for word in ("only", "separate"))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["child_page", "child_database"])
async def test_native_reference_label_does_not_duplicate_target_body(kind):
    entity = await mapped(block(kind, {"title": "Budget"}))
    assert entity.title == "Budget" and entity.text == ""
    assert entity.web_url == "" and "Reference label only" in entity.content_coverage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "record",
    [
        block("future_type", {"secret": "never serialize me"}),
        block("synced_block", {"synced_from": {"type": "block_id", "block_id": PAGE}}),
        block("paragraph", {"rich_text": [{"type": "future", "plain_text": "unknown"}]}),
        block(
            "paragraph",
            {"rich_text": [{"type": "text", "text": {"content": "not retained plain text"}}]},
        ),
    ],
)
async def test_unsupported_or_malformed_content_never_becomes_empty_success(record):
    with pytest.raises(ValueError):
        await mapped(record)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)",
        "https://evil.example/page",
        "https://app.notion.com.evil.example/page",
        "https://user@app.notion.com/page",
        "http://notion.so/page",
    ],
)
async def test_untrusted_navigation_remains_pending(url):
    with pytest.raises(ValueError):
        await mapped(original(properties={"title": {"type": "title", "title": []}}, url=url))


@pytest.mark.asyncio
async def test_identity_and_authority_mismatches_are_rejected():
    record = block("paragraph", {"rich_text": [rich("private")]})
    cases = [
        record.model_copy(update={"parent": None}),
        record.model_copy(
            update={"identity": record.identity.model_copy(update={"native_id": PAGE})}
        ),
        record.model_copy(update={"content_access": "parent_unavailable"}),
        record.model_copy(update={"deleted_at": datetime.now(timezone.utc)}),
        record.model_copy(update={"payload": {**record.payload, "in_trash": True}}),
    ]
    for case in cases:
        with pytest.raises(ProjectionMappingError):
            await mapped(case)


def test_original_search_view_is_in_existing_entity_registry():
    from airweave.domains.entities.registry import EntityDefinitionRegistry
    from airweave.platform.entities.notion import NotionOriginalEntity

    registry = EntityDefinitionRegistry()
    registry.build()
    assert registry.get_short_name_by_class(NotionOriginalEntity) == "notion_original_entity"
    assert (NotionOriginalEntity.model_fields["content_coverage"].json_schema_extra or {}).get(
        "embeddable"
    ) is not True


@pytest.mark.integration
async def test_notion_publication_uses_current_revision_and_ancestor_authority(database, source):
    service, fence = source
    root = original(properties={"title": {"type": "title", "title": [rich("Page")]}})
    child = block("paragraph", {"rich_text": [rich("First version")]})
    root_observation = observation(
        identity=root.identity, payload=root.payload, completeness="partial"
    )
    child_observation = observation(
        identity=child.identity, parent=root.identity, payload=child.payload, completeness="partial"
    )
    await capture(database, service, fence, root_observation, child_observation)
    store = CanonicalProjectionStore()
    async with database() as db:
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
    work = next(item for item in pending if item.record.identity.record_type == "block")
    assert (await mapped(work.record)).text == "First version"
    generation = uuid4()
    async with database() as db:
        assert await publish_prepared(store, db, work, generation, 1)
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=work.record.revision,
        pipeline_version=work.pipeline_version,
        generation=generation,
        part_index=0,
    )
    updated = child_observation.model_copy(
        update={"payload": {**child.payload, "paragraph": {"rich_text": [rich("Changed")]}}}
    )
    await capture(database, service, fence, updated)
    async with database() as db:
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(locator)))
            is None
        )
        assert not await publish_prepared(store, db, work, uuid4(), 1)
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
    fresh = next(item for item in pending if item.record.identity.record_type == "block")
    await capture(
        database,
        service,
        fence,
        root_observation.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    async with database() as db:
        assert not await publish_prepared(store, db, fresh, uuid4(), 1)


@pytest.mark.integration
async def test_unknown_notion_block_stays_pending_in_actual_projector(database, source):
    service, fence = source
    root = original(properties={"title": {"type": "title", "title": []}})
    child = block("future_type", {"body": "not raw JSON prose"})
    await capture(
        database,
        service,
        fence,
        observation(identity=root.identity, payload=root.payload, completeness="partial"),
        observation(
            identity=child.identity,
            parent=root.identity,
            payload=child.payload,
            completeness="partial",
        ),
    )
    store = CanonicalProjectionStore()
    async with database() as db:
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
        root_work = next(item for item in pending if item.record.identity.record_type == "page")
        assert await publish_prepared(store, db, root_work, uuid4(), 1)
    processor, destination = MagicMock(), MagicMock()
    projector = CanonicalProjector(store, database, processor, AsyncMock())
    result = await projector.batch(
        fence.organization_id, fence.sync_id, "notion", destination, MagicMock()
    )
    assert result.failed == 1 and result.published == 0
    processor.process.assert_not_called()
    destination.feed_prepared.assert_not_called()
    async with database() as db:
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
        assert len(pending) == 1
        row = await db.get(Entity, pending[0].record.id)
        assert row.projection_error == "ProjectionMappingError" and row.indexed_revision is None
