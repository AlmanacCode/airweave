"""Stale index content cannot survive authoritative changes or cross-account scope."""

from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import update

from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.tests.helpers import capture, observation, publish_prepared
from airweave.domains.search.canonical_visibility import visible_results
from airweave.domains.search.types.results import (
    SearchAccessControl,
    SearchResult,
    SearchSystemMetadata,
)
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


def hit(fence, locator):
    return SearchResult(
        entity_id="index-chunk",
        name="Synthetic",
        relevance_score=1,
        breadcrumbs=[],
        textual_representation="Private original text",
        airweave_system_metadata=SearchSystemMetadata(
            source_name="gmail",
            entity_type="message",
            sync_id=str(fence.sync_id),
            chunk_index=0,
            original_entity_id=locator,
        ),
        access=SearchAccessControl(),
        web_url="https://example.test",
        raw_source_fields={},
    )


async def test_search_rejects_stale_foreign_malformed_and_legacy_publications(database, source):
    service, fence = source
    await capture(database, service, fence, observation())
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="test", embedding_dimensions=3, sparse_embedder="test"
        )
        db.add(deployment)
        await db.flush()
        db.add(
            Collection(
                name="Test",
                readable_id="test",
                organization_id=fence.organization_id,
                vector_db_deployment_metadata_id=deployment.id,
            )
        )
        await db.flush()
        db.add(
            SourceConnection(
                name="Gmail",
                short_name="gmail",
                organization_id=fence.organization_id,
                readable_collection_id="test",
                sync_id=fence.sync_id,
                is_authenticated=True,
            )
        )
        await db.commit()
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    generation = uuid4()
    async with database() as db:
        assert await publish_prepared(store, db, work, generation, 1)
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=1,
        pipeline_version=1,
        generation=generation,
        part_index=0,
    )
    valid = hit(fence, locator.encode())
    registry = SimpleNamespace(
        get=lambda _: SimpleNamespace(
            source_class_ref=SimpleNamespace(canonical_record_types=("message",))
        )
    )
    wrong_revision = locator.model_copy(update={"revision": 2})
    inputs = [
        valid,
        hit(fence, wrong_revision.encode()),
        hit(fence, "native-id"),
        hit(fence, "canonical:invalid"),
    ]
    async with database() as db:
        assert await visible_results(db, fence.organization_id, "test", inputs, registry) == [valid]
        assert await visible_results(db, uuid4(), "test", inputs, registry) == []
        assert await visible_results(db, fence.organization_id, "other", inputs, registry) == []
    async with database() as db:
        await db.execute(update(SourceConnection).values(is_authenticated=False))
        await db.commit()
        assert await visible_results(db, fence.organization_id, "test", inputs, registry) == []
        await db.execute(update(SourceConnection).values(is_authenticated=True))
        await db.commit()
    await capture(database, service, fence, observation(payload={"id": "one", "summary": "Edited"}))
    async with database() as db:
        assert await visible_results(db, fence.organization_id, "test", inputs, registry) == []
