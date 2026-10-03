"""Real PostgreSQL publication races: late feeds never regain search eligibility."""

from uuid import uuid4

import pytest
from sqlalchemy import select, update

from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import (
    CanonicalProjectionStore,
    publication_matches,
)
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import (
    bind_projection,
    capture,
    observation,
    publish_prepared,
)
from airweave.models import Entity, Sync

pytestmark = pytest.mark.integration


async def pending(database, fence):
    async with database() as db:
        return await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id)


async def test_publication_compare_and_swap_revision_generation_and_pipeline(database, source):
    service, fence = source
    await bind_projection(database, fence, "slack")
    await capture(database, service, fence, observation())
    work = (await pending(database, fence))[0]
    store = CanonicalProjectionStore()
    generation = uuid4()
    async with database() as db:
        assert await publish_prepared(store, db, work, generation, 3)
    async with database() as db:
        assert not await publish_prepared(store, db, work, uuid4(), 3)  # timed-out prior feed
    assert not await pending(database, fence)
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=1,
        pipeline_version=1,
        generation=generation,
        part_index=0,
    )
    assert ProjectionLocator.parse(locator.encode()) == locator
    async with database() as db:
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(locator)))
            == work.record.id
        )
    await capture(database, service, fence, observation(payload={"summary": "Changed"}))
    async with database() as db:
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(locator)))
            is None
        )
        assert not await publish_prepared(store, db, work, uuid4(), 3)
    changed = (await pending(database, fence))[0]
    async with database() as db:
        await db.execute(update(Sync).values(index_pipeline_version=2))
        await db.commit()
    async with database() as db:
        assert not await publish_prepared(store, db, changed, uuid4(), 3)
    assert (await pending(database, fence))[0].pipeline_version == 2


async def test_parent_access_loss_fences_publication_and_tombstone_is_empty(database, source):
    service, fence = source
    await bind_projection(database, fence, "slack")
    parent_id = RecordIdentity(record_type="calendar", native_id="cal")
    parent = observation(identity=parent_id)
    child = observation("event", "cal", parent=parent_id)
    await capture(database, service, fence, parent, child)
    rows = await pending(database, fence)
    child_work = next(row for row in rows if row.record.identity.record_type == "event")
    await capture(
        database,
        service,
        fence,
        parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    store = CanonicalProjectionStore()
    async with database() as db:
        assert not await publish_prepared(store, db, child_work, uuid4(), 2)
    rows = await pending(database, fence)
    assert len(rows) == 1 and rows[0].record.deleted_at is not None
    async with database() as db:
        assert await publish_prepared(store, db, rows[0], uuid4(), 0)
    assert not await pending(database, fence)


async def test_failed_work_retries_and_stale_failure_preserves_publication(database, source):
    service, fence = source
    await bind_projection(database, fence, "slack")
    await capture(database, service, fence, observation())
    work = (await pending(database, fence))[0]
    store = CanonicalProjectionStore()
    async with database() as db:
        await store.fail(db, work, "conversion failed")
    assert len(await pending(database, fence)) == 1
    async with database() as db:
        assert await publish_prepared(store, db, work, uuid4(), 1)
    async with database() as db:
        await store.fail(db, work, "late failure")
    async with database() as db:
        assert (await db.get(Entity, work.record.id)).projection_error is None


def test_locator_rejects_malformed_canonical_values():
    assert ProjectionLocator.parse("legacy-native-id") is None
    for value in ("canonical:v2:bad", "canonical:v1:bad", "canonical:"):
        with pytest.raises(ValueError):
            ProjectionLocator.parse(value)


async def test_owned_payload_mapper_to_publication_pipeline(database, source):
    from unittest.mock import AsyncMock, MagicMock

    from airweave.domains.entities.canonical.projector import CanonicalProjector
    from airweave.platform.destinations.vespa.transformer import EntityTransformer

    service, fence = source
    binding = await bind_projection(database, fence, "slack")
    await capture(
        database,
        service,
        fence,
        observation(
            identity=RecordIdentity(record_type="channel", native_id="C1"),
            payload={"id": "C1", "name": "general", "purpose": {"value": "Project work"}},
        ),
    )
    work = (await pending(database, fence))[0]
    processor = MagicMock()

    async def process(entities, context, runtime, *, strict, expected_ids):
        assert strict and context.source_short_name == "slack"
        assert entities[0].name == "general"
        assert entities[0].airweave_system_metadata.sync_id == fence.sync_id
        locator = ProjectionLocator.parse(entities[0].entity_id)
        assert locator.record_id == work.record.id
        chunk = entities[0].model_copy(deep=True)
        chunk.airweave_system_metadata.original_entity_id = chunk.entity_id
        chunk.entity_id += "__chunk_0"
        return [chunk]

    from airweave.domains.sync_pipeline.pipeline.text_models import BuiltText, BuiltTextBatch

    async def build_text(entities, context, runtime, *, native_bodies=None):
        for entity in entities:
            entity.textual_representation = "Synthetic complete text"
        return BuiltTextBatch(
            entities=entities,
            representations=tuple(
                BuiltText(entity_id=e.entity_id, text="Synthetic complete text") for e in entities
            ),
        )

    processor.build_text = build_text
    processor.process_built_text = process
    destination = MagicMock()
    destination.collection_id = binding.collection_id
    destination.prepare_documents = lambda chunks: {
        "base_entity": [
            EntityTransformer(collection_id=destination.collection_id).transform(chunk)
            for chunk in chunks
        ]
    }

    async def feed_prepared(documents):
        from airweave.models.projection_generation import ProjectionGeneration

        async with database() as db:
            manifest = await db.scalar(select(ProjectionGeneration))
            assert manifest is not None
            assert manifest.documents[0]["document_id"] == documents["base_entity"][0].id
            assert (await db.get(Entity, work.record.id)).indexed_generation is None
            document = documents["base_entity"][0]
            assert document.id.startswith(f"{fence.sync_id}_{destination.collection_id}_")
            # Direct reads/navigation query entity fields, not the remote feed key.
            original = document.fields["airweave_system_metadata_original_entity_id"]
            assert ProjectionLocator.parse(original).record_id == work.record.id
            assert document.fields["entity_id"] == original + "__chunk_0"
            assert document.fields["airweave_system_metadata_sync_id"] == str(fence.sync_id)

    destination.feed_prepared = AsyncMock(side_effect=feed_prepared)
    storage = MagicMock(write_file=AsyncMock())
    projector = CanonicalProjector(
        CanonicalProjectionStore(), lambda _organization: database(), processor, storage
    )
    assert (await projector.project_one(work, "slack", destination, MagicMock())).published
    destination.feed_prepared.assert_awaited_once()
    assert not await pending(database, fence)
