"""Actual capture/blob/project/feed/publication/retrieval; fixed vectors, not quality."""

import hashlib
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select, update
from vespa.application import Vespa

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.api import deps
from airweave.api.v1.endpoints.records import router
from airweave.core.logging import logger
from airweave.db.session import get_db
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.embedders.types import DenseEmbedding, EmbeddingPurpose, SparseEmbedding
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.domains.entities.canonical.tests.vespa_helpers import deploy_schema
from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
from airweave.domains.search.executor import SearchPlanExecutor
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models import Entity
from airweave.models.collection import Collection
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.source_connection import SourceConnection
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata
from airweave.platform.destinations.vespa.destination import VespaDestination
from airweave.platform.sources.google_drive import GoogleDriveSource

pytestmark = pytest.mark.skipif(
    os.environ.get("OWNED_VESPA_TEST") != "1", reason="requires disposable real Vespa"
)


class FixedDense(FakeDenseEmbedder):
    """Only model inference is substituted; all processing stays production code."""

    async def embed_many(
        self, texts: list[str], *, purpose: EmbeddingPurpose = "document"
    ) -> list[DenseEmbedding]:
        return [DenseEmbedding(vector=[1.0] + [0.0] * 383) for _ in texts]


class FixedSparse(FakeSparseEmbedder):
    async def embed(self, text):
        return SparseEmbedding(indices=[1], values=[1.0])

    async def embed_many(self, texts):
        return [await self.embed(text) for text in texts]


async def test_captured_blob_to_published_search_and_withdrawal(
    database, source, tmp_path, monkeypatch
):
    """Search cannot expose another org, stale revision or withdrawn publication."""
    from airweave.core.config import settings

    monkeypatch.setattr(settings, "VESPA_URL", "http://localhost")
    monkeypatch.setattr(settings, "VESPA_PORT", 8081)
    async with httpx.AsyncClient(timeout=120) as http:
        await deploy_schema(http)
    service, fence = source
    log = logger.with_context(request_id="synthetic-complete-projection")
    storage = FilesystemBackend(tmp_path)
    content = b"Synthetic fundraising discussion. The team plans a fundraising meeting."
    digest = hashlib.sha256(content).hexdigest()
    blob = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="text/plain",
    )
    await storage.write_file(blob.key, content)
    original = observation(
        identity=RecordIdentity(record_type="file", native_id="fixture-file"),
        payload={"id": "fixture-file", "name": "Fundraising.txt", "mimeType": "text/plain"},
        blobs=(blob,),
    )
    await capture(database, service, fence, original)
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="fixed", embedding_dimensions=384, sparse_embedder="fixed"
        )
        db.add(deployment)
        await db.flush()
        collection = Collection(
            name="Synthetic",
            readable_id="projection-" + uuid4().hex,
            organization_id=fence.organization_id,
            vector_db_deployment_metadata_id=deployment.id,
        )
        db.add(collection)
        await db.flush()
        connection = SourceConnection(
            name="Synthetic",
            short_name="google_drive",
            organization_id=fence.organization_id,
            readable_collection_id=collection.readable_id,
            sync_id=fence.sync_id,
            is_authenticated=True,
        )
        db.add(connection)
        await db.commit()
    dense, sparse = FixedDense(dimensions=384), FixedSparse()
    store = CanonicalProjectionStore()
    projector = CanonicalProjector(
        store,
        lambda _organization: database(),
        ChunkEmbedProcessor(ConverterRegistry(), dense, sparse),
        storage,
    )
    destination = await VespaDestination.create(
        collection_id=collection.id, organization_id=fence.organization_id, logger=log
    )
    registry = SimpleNamespace(get=lambda _: SimpleNamespace(source_class_ref=GoogleDriveSource))
    blocked = AsyncMock(side_effect=AssertionError("No provider or model-agent call allowed"))
    engine = VespaVectorDB(
        app=Vespa(url="http://localhost", port=8081),
        logger=log,
        filter_translator=FilterTranslator(logger=log),
    )
    executor = SearchPlanExecutor(dense, sparse, engine, blocked, registry, blocked, blocked)
    owned = OwnedSearchService(executor, registry)
    ctx = SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id), logger=log)
    app = FastAPI()
    app.include_router(router, prefix="/sync")

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_tenant_db] = session
    app.dependency_overrides[deps.get_context] = lambda: ctx
    app.dependency_overrides[deps.get_owned_context] = lambda: ctx
    app.dependency_overrides[deps.get_tenant_session_factory] = lambda: database
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(owned_search=owned)
    request = {"query": "fundraising", "sync_ids": [str(fence.sync_id)], "mode": "keyword"}
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as api:
            before = await api.post("/sync/search", json=request)
            assert before.status_code == 200 and before.json()["items"] == []
            assert before.json()["sources"][0]["pending_records"] == 1
            batch = await projector.batch(
                fence.organization_id, fence.sync_id, "google_drive", destination, log
            )
            assert batch.published == 1 and batch.failed == 0
            async with database() as db:
                row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
                assert row.indexed_generation is not None and row.indexed_chunk_count > 0
                record_id = str(row.id)
                manifest = await db.scalar(select(ProjectionGeneration))
                assert manifest.documents
            for mode in ("keyword", "semantic", "hybrid"):
                result = await api.post("/sync/search", json={**request, "mode": mode})
                assert result.status_code == 200, result.text
                assert [hit["record_id"] for hit in result.json()["items"]] == [record_id]
                excerpt = " ".join(result.json()["items"][0]["excerpts"])
                # Lexical summaries may select the matching filename rather than
                # the body sentence. Semantic mode retains the full chunk fallback.
                assert "fundraising" in excerpt.casefold()
                assert "<hi>" not in excerpt and "<sep />" not in excerpt
                if mode == "semantic":
                    assert "fundraising meeting" in excerpt
                assert result.json()["sources"][0]["pending_records"] == 0
            changed = original.model_copy(
                update={"payload": {**original.payload, "name": "Updated fundraising.txt"}}
            )
            await capture(database, service, fence, changed)
            stale = await api.post("/sync/search", json=request)
            assert stale.status_code == 200 and stale.json()["items"] == []
            assert stale.json()["excluded_candidates"] > 0
            replacement = await projector.batch(
                fence.organization_id, fence.sync_id, "google_drive", destination, log
            )
            assert replacement.published == 1 and replacement.failed == 0
            current = await api.post("/sync/search", json=request)
            assert current.status_code == 200
            assert len(current.json()["items"]) == 1
            assert current.json()["items"][0]["revision"] == 2
            ctx.organization.id = uuid4()
            assert (await api.post("/sync/search", json=request)).status_code == 404
            ctx.organization.id = fence.organization_id
            async with database() as db:
                await db.execute(
                    update(SourceConnection)
                    .where(SourceConnection.id == connection.id)
                    .values(is_authenticated=False)
                )
                await db.commit()
            assert (await api.post("/sync/search", json=request)).status_code == 404
            async with database() as db:
                await db.execute(
                    update(SourceConnection)
                    .where(SourceConnection.id == connection.id)
                    .values(is_authenticated=True)
                )
                await db.commit()
            await capture(
                database,
                service,
                fence,
                changed.model_copy(update={"kind": "delete", "removal_reason": "provider_deleted"}),
            )
            # Old physical Vespa documents still exist: SQL authority must hide them now.
            withdrawn = await api.post("/sync/search", json=request)
            assert withdrawn.status_code == 200 and withdrawn.json()["items"] == []
            assert withdrawn.json()["excluded_candidates"] > 0
    finally:
        await destination.delete_by_sync_id(fence.sync_id)
        await destination.close_connection()
