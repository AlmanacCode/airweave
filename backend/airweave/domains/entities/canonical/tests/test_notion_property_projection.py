"""Offline native properties, bounded reads and retained identity."""

import hashlib
import json
from unittest.mock import AsyncMock

import pytest

from airweave.domains.entities.canonical.projection_mappers import (
    ProjectionMappingError,
    map_record,
)
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import bind_projection
from airweave.domains.entities.canonical.tests.test_notion_projection import PAGE, STAMP, original


def item(kind, value):
    return {"object": "property_item", "id": "a%3Ab", "type": kind, kind: value}


def page(kind, values, more=False, calculation=None):
    metadata = {"id": "a%3Ab", "type": kind, "next_url": "next" if more else None}
    if calculation is not None:
        metadata[kind] = calculation
    return {
        "object": "list",
        "type": "property_item",
        "results": values,
        "property_item": metadata,
        "has_more": more,
        "next_cursor": "next" if more else None,
    }


def fixture(kind, responses, status="available"):
    record = original()
    header = {
        "format_version": 1,
        "page_id": PAGE,
        "property_id": "a%3Ab",
        "notion_version": "2026-03-11",
        "page_last_edited_time": STAMP,
    }
    content = json.dumps({**header, "responses": responses}, ensure_ascii=False).encode()
    digest = hashlib.sha256(content).hexdigest()
    blob = BlobReference(
        key=f"canonical/{record.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="application/json",
    )
    record = record.model_copy(
        update={
            "identity": RecordIdentity(
                record_type="page_property", native_id="a%3Ab", container_id=PAGE
            ),
            "parent": RecordIdentity(record_type="page", native_id=PAGE),
            "payload_schema_version": 2,
            "payload": {
                **header,
                "name": "Résumé",
                "property": {"id": "a%3Ab", "type": kind},
                "response_count": len(responses),
                "value_status": status,
            },
            "blobs": (blob,),
        }
    )
    return record, content


async def project(record, content):
    storage = AsyncMock()
    storage.read_file.return_value = content
    async with map_record(record, "notion", storage) as result:
        return result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,value,expected",
    [
        ("number", 12, "12"),
        ("number", 9007199254740993, "9007199254740993"),
        ("checkbox", False, "false"),
        ("select", {"name": "準備"}, "準備"),
        ("date", {"start": "2026-10-01"}, "2026-10-01"),
        ("people", {"id": PAGE, "name": "Mira"}, f"Mira ({PAGE})"),
        ("relation", {"id": PAGE}, PAGE),
        (
            "files",
            [{"name": "Report.pdf", "file": {"url": "https://signed.example/secret"}}],
            "Report.pdf",
        ),
        ("formula", {"type": "string", "string": "計画"}, "計画"),
    ],
)
async def test_native_values(kind, value, expected):
    responses = (
        [page(kind, [item(kind, value)])] if kind in {"people", "relation"} else [item(kind, value)]
    )
    record, content = fixture(kind, responses)
    result = await project(record, content)
    entity = result.entities[0]
    assert entity.text == expected and entity.native_id == "a%3Ab"
    assert entity.created_at is None and entity.updated_at is None
    assert result.parts[0].native_body is None
    assert "exclude linked bodies" in entity.content_coverage


@pytest.mark.asyncio
async def test_paginated_text_and_terminal_rollup():
    record, content = fixture(
        "rich_text",
        [
            page("rich_text", [item("rich_text", {"type": "text", "plain_text": "你好 "})], True),
            page("rich_text", [item("rich_text", {"type": "mention", "plain_text": "Mira"})]),
        ],
    )
    assert (await project(record, content)).entities[0].text == "你好 Mira"
    record, content = fixture(
        "rollup",
        [
            page("rollup", [], True, {"type": "incomplete", "function": "sum"}),
            page("rollup", [], calculation={"type": "number", "number": 9, "function": "sum"}),
        ],
    )
    assert (await project(record, content)).entities[0].text == "9"


@pytest.mark.asyncio
async def test_unsupported_and_incomplete_distinct():
    record, content = fixture("formula", [item("formula", {"type": "unsupported"})], "unsupported")
    result = await project(record, content)
    assert not result.entities and result.parts[0].omission == "unsupported_format"
    record, content = fixture("rollup", [page("rollup", [], calculation={"type": "incomplete"})])
    with pytest.raises(ProjectionMappingError, match="incomplete"):
        await project(record, content)


@pytest.mark.asyncio
async def test_authority_hash_and_pagination_fail_closed():
    record, content = fixture("number", [item("number", 1)])
    for change in [
        {"parent": None},
        {"payload_schema_version": 1},
        {"payload": {**record.payload, "response_count": 2}},
        {"payload": {**record.payload, "page_last_edited_time": "2026-10-02T00:00:00Z"}},
    ]:
        with pytest.raises(ValueError):
            await project(record.model_copy(update=change), content)
    with pytest.raises(ValueError):
        await project(record, content + b" ")
    record, content = fixture("rich_text", [page("rich_text", [], True)])
    with pytest.raises(ProjectionMappingError, match="pagination"):
        await project(record, content)


@pytest.mark.asyncio
async def test_budget_before_storage():
    record, _ = fixture("number", [item("number", 1)])
    record = record.model_copy(
        update={"blobs": (record.blobs[0].model_copy(update={"size_bytes": 16 * 1024 * 1024 + 1}),)}
    )
    storage = AsyncMock()
    with pytest.raises(ProjectionMappingError, match="projection budget"):
        async with map_record(record, "notion", storage):
            pass
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,responses",
    [
        ("future_type", [item("future_type", None)]),
        ("number", [page("number", [])]),
        ("people", [item("people", {"id": PAGE})]),
        ("checkbox", [item("checkbox", None)]),
    ],
)
async def test_unknown_or_invalid_native_shape_fails(kind, responses):
    record, content = fixture(kind, responses)
    with pytest.raises(ValueError):
        await project(record, content)


@pytest.mark.integration
async def test_property_publication_obeys_current_parent(database, source):
    from uuid import uuid4

    from sqlalchemy import select

    from airweave.domains.entities.canonical.projection_models import ProjectionLocator
    from airweave.domains.entities.canonical.projection_store import (
        CanonicalProjectionStore,
        publication_matches,
    )
    from airweave.domains.entities.canonical.tests.helpers import (
        capture,
        observation,
        publish_prepared,
    )
    from airweave.models import Entity, Sync

    service, fence = source
    await bind_projection(database, fence, "notion")
    record, content = fixture("number", [item("number", 1)])
    blob = record.blobs[0].model_copy(
        update={"key": f"canonical/{fence.sync_id}/blobs/sha256/{record.blobs[0].sha256}"}
    )
    root = observation(identity=record.parent, payload={"id": PAGE})
    await capture(
        database,
        service,
        fence,
        root,
        observation(
            identity=record.identity,
            parent=record.parent,
            payload=record.payload,
            payload_schema_version=2,
            blobs=(blob,),
            completeness="partial",
        ),
    )
    store = CanonicalProjectionStore()
    async with database() as db:
        work = next(
            w
            for w in await store.pending(db, fence.organization_id, fence.sync_id)
            if w.record.identity.record_type == "page_property"
        )
    assert (await project(work.record, content)).entities[0].text == "1"
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
    await capture(
        database,
        service,
        fence,
        root.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    async with database() as db:
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(locator)))
            is None
        )
        assert not await publish_prepared(store, db, work, uuid4(), 1)


@pytest.mark.integration
async def test_unsupported_property_publishes_explicit_extraction(database, source, tmp_path):
    from sqlalchemy import select

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.entities.canonical.projection_store import (
        CanonicalProjectionStore,
        current_extraction,
    )
    from airweave.domains.entities.canonical.tests.helpers import capture, observation
    from airweave.domains.entities.canonical.tests.test_extraction_coverage import (
        destination,
        logger,
        projector,
    )
    from airweave.models import Entity

    service, fence = source
    binding = await bind_projection(database, fence, "notion")
    record, content = fixture("formula", [item("formula", {"type": "unsupported"})], "unsupported")
    blob = record.blobs[0].model_copy(
        update={"key": f"canonical/{fence.sync_id}/blobs/sha256/{record.blobs[0].sha256}"}
    )
    storage = FilesystemBackend(tmp_path)
    await storage.write_file(blob.key, content)
    await capture(
        database,
        service,
        fence,
        observation(identity=record.parent, payload={"id": PAGE}),
        observation(
            identity=record.identity,
            parent=record.parent,
            payload=record.payload,
            payload_schema_version=2,
            blobs=(blob,),
            completeness="partial",
        ),
    )
    async with database() as db:
        work = next(
            w
            for w in await CanonicalProjectionStore().pending(
                db, fence.organization_id, fence.sync_id
            )
            if w.record.identity.record_type == "page_property"
        )
    assert await projector(database, storage).project_one(
        work, "notion", destination(binding.collection_id), logger
    )
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.id == work.record.id))
        coverage = await current_extraction(
            db, fence.organization_id, fence.sync_id, row.id, row.record_revision
        )
        assert row.indexed_chunk_count == 0
        assert coverage.parts[0].outcome == "unsupported"
        assert coverage.status == "unavailable"
