"""Source-selected native bodies share the retained and indexed construction."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.tests.test_slack_projection import message
from airweave.domains.entities.canonical.text_models import TextArtifact
from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder


@pytest.mark.parametrize("schema", [1, 2])
@pytest.mark.parametrize(
    "text", ["", "  \n", "Hello <@U1> 👋\n*bold* <https://example.com|link>\n# Content\nend"]
)
async def test_native_body_exact_and_excluded_from_metadata(schema, text):
    record = message([]).model_copy(
        update={
            "payload": {"ts": "1.000001", "text": text, "blocks": [{"type": "unrendered"}]},
            "payload_schema_version": schema,
        }
    )
    async with map_record(record, "slack", AsyncMock()) as mapped:
        item = mapped.parts[0]
        assert item.entity.text == text
        batch = await TextualRepresentationBuilder(MagicMock()).build_with_text(
            [item.entity],
            SimpleNamespace(source_short_name="slack", logger=MagicMock()),
            SimpleNamespace(entity_tracker=SimpleNamespace(record_skipped=AsyncMock())),
            native_bodies={item.entity.entity_id: item.native_body},
        )
        built = batch.representations[0]
        assert built.kind == "native_text"
        assert built.text[built.content_start :] == text
        assert "**Text**:" not in built.text[: built.content_start]
        assert built.text == batch.entities[0].textual_representation
        assert "unrendered" not in built.text


async def test_missing_text_does_not_invent_native_body_from_blocks_or_title():
    record = message([]).model_copy(
        update={
            "payload": {"ts": "1.000001", "blocks": [{"type": "rich_text"}]},
        }
    )
    async with map_record(record, "slack", AsyncMock()) as mapped:
        assert mapped.parts[0].native_body is None
        assert mapped.parts[0].entity.text == ""


@pytest.mark.parametrize("text", [None, 123, {}, []])
async def test_malformed_native_body_fails(text):
    record = message([]).model_copy(update={"payload": {"ts": "1.000001", "text": text}})
    with pytest.raises(ValueError, match="Slack message text must be a string"):
        async with map_record(record, "slack", AsyncMock()):
            pass


@pytest.mark.parametrize("start,kind", [(None, "generated_text"), (0, "extracted_text")])
def test_old_manifest_provenance_is_preserved(start, kind):
    values = {
        "id": uuid4(),
        "part_index": 0,
        "sha256": "0" * 64,
        "size_bytes": 0,
        "characters": 0,
        "content_start": start,
    }
    assert TextArtifact.model_validate(values).kind == kind
    for incorrect in ["native_text", "extracted_text"] if start is None else ["generated_text"]:
        with pytest.raises(ValidationError, match="provenance"):
            TextArtifact.model_validate({**values, "kind": incorrect})


@pytest.mark.parametrize("body", [None, "", "Native 👋 text\n# Content\n<@U1>"])
async def test_slack_native_retained_read_matches_text_used_for_index(
    database, source, tmp_path, body
):
    from sqlalchemy import update

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
    from airweave.domains.entities.canonical.query import CanonicalQueryService
    from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
    from airweave.domains.entities.canonical.requests import RecordIdentity
    from airweave.domains.entities.canonical.store import CanonicalRecordStore
    from airweave.domains.entities.canonical.tests.helpers import (
        bind_projection,
        capture,
        observation,
    )
    from airweave.domains.entities.canonical.tests.test_extraction_coverage import (
        destination,
        projector,
    )
    from airweave.domains.entities.canonical.text_query import CanonicalTextReader, TextUnavailable
    from airweave.models.sync import Sync

    service, fence = source
    binding = await bind_projection(database, fence, "slack")
    original = observation(
        identity=RecordIdentity(record_type="message", native_id="1.000001", container_id="C1"),
        payload={"ts": "1.000001", **({"text": body} if body is not None else {})},
        payload_schema_version=2,
    )
    await capture(database, service, fence, original)
    storage = FilesystemBackend(tmp_path)
    projection = projector(database, storage)
    indexed = []

    async def fixed_chunks(entities, context, runtime):
        indexed.extend(entity.textual_representation for entity in entities)
        return projection._processor._multiply_entities(
            entities,
            [[{"text": entity.textual_representation}] for entity in entities],
            context,
        )

    projection._processor._chunk_entities = fixed_chunks
    async with database() as db:
        work = (await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id))[
            0
        ]
    assert (
        await projection.project_one(work, "slack", destination(binding.collection_id), MagicMock())
    ).published
    reader = CanonicalTextReader(
        CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "key"),
        storage,
    )
    async with database() as db:
        ref = (
            await reader.list(db, fence.organization_id, fence.sync_id, work.record.id, 1)
        ).representations[0]
        if body is None:
            assert ref.kind == "generated_text" and ref.content_characters is None
        else:
            assert ref.kind == "native_text" and ref.content_characters == len(body)
        args = (fence.organization_id, fence.sync_id, work.record.id, 1, ref.generation, ref.id)
        if body is None:
            with pytest.raises(TextUnavailable, match="generated search text"):
                await reader.read(db, *args)
        else:
            content = await reader.read(db, *args)
            assert content.text == body and content.total_characters == len(body)
        index = await reader.read(db, *args, view="index")
        assert index.text == indexed[0]
        if body is not None:
            assert index.text.endswith(body)
        if body:
            first = await reader.read(db, *args, limit=8)
            rest = await reader.read(db, *args, offset=first.next_offset)
            assert first.text + rest.text == body
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=2)
        )
        await db.commit()
        with pytest.raises(TextUnavailable):
            await reader.read(db, *args)
