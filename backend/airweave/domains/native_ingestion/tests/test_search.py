"""Native publications use real SQL authority without a provider registry entry."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import update

from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.tests.helpers import publish_prepared
from airweave.domains.entities.canonical.tests.test_search_visibility import hit
from airweave.domains.native_ingestion.tests.test_ingestion import bind, ingest, snapshot
from airweave.domains.search.canonical_visibility import visible_results
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.search.types import SearchResults
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


async def test_native_search_requires_current_publication_and_source_access(database, source):
    """Index transport is synthetic; capture, scope checks and publication are real SQL."""
    _, fence = source
    await bind(database, fence)
    await ingest(database, fence, snapshot())
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="fake", embedding_dimensions=3, sparse_embedder="fake"
        )
        db.add(deployment)
        await db.flush()
        collection = Collection(
            name="Native synthetic",
            readable_id="native-search",
            organization_id=fence.organization_id,
            vector_db_deployment_metadata_id=deployment.id,
        )
        db.add(collection)
        await db.flush()
        await db.execute(
            update(SourceConnection)
            .where(SourceConnection.sync_id == fence.sync_id)
            .values(readable_collection_id=collection.readable_id, is_authenticated=True)
        )
        await db.commit()
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    generation = uuid4()
    async with database() as db:
        assert await publish_prepared(store, db, work, generation, 1, collection.id)
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=work.record.revision,
        pipeline_version=work.pipeline_version,
        generation=generation,
        part_index=0,
    )
    candidate = hit(fence, locator.encode())
    candidate.airweave_system_metadata.source_name = "almanac"
    registry = Mock()
    registry.get.side_effect = AssertionError("Native content has no provider lifecycle")
    executor = Mock()
    executor.prepare_query = AsyncMock(return_value=None)
    executor.execute = AsyncMock(return_value=SearchResults(results=[candidate]))
    service = OwnedSearchService(executor, registry)
    ctx = SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))
    request = OwnedSearchRequest(query="original", sync_ids=[fence.sync_id], mode="keyword")
    result = await service.search(database, ctx, request)
    assert len(result.items) == 1
    assert result.items[0].provider == "almanac"
    assert result.items[0].identity.record_type == "knowledge"
    async with database() as db:
        assert await visible_results(
            db, fence.organization_id, collection.readable_id, [candidate], registry
        ) == [candidate]
        legacy = candidate.model_copy(deep=True)
        legacy.airweave_system_metadata.original_entity_id = "unversioned-native-id"
        assert await visible_results(
            db, fence.organization_id, collection.readable_id, [legacy], registry
        ) == []

    async def revoke_during_retrieval(**kwargs):
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == fence.sync_id)
                .values(is_authenticated=False)
            )
            await db.commit()
        return SearchResults(results=[candidate])

    executor.execute.side_effect = revoke_during_retrieval
    with pytest.raises(HTTPException) as error:
        await service.search(database, ctx, request)
    assert error.value.status_code == 404
    registry.get.assert_not_called()
