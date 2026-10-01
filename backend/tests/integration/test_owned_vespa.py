"""Real engine smoke using only synthetic records in a disposable Vespa instance.

Explicit opt-in: OWNED_VESPA_TEST=1. This deploys the repository schema package
and is unsuitable for an existing application or customer index.
"""

import json
import math
import os
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest
from vespa.application import Vespa

from airweave.core.logging import logger
from airweave.domains.embedders.types import DenseEmbedding, SparseEmbedding
from airweave.domains.entities.canonical.tests.vespa_helpers import deploy_schema
from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
from airweave.domains.search.adapters.vector_db.vespa_config import ALL_VESPA_SCHEMAS
from airweave.domains.search.types.embeddings import QueryEmbeddings
from airweave.domains.search.types.filters import FilterCondition, FilterGroup
from airweave.domains.search.types.plan import RetrievalStrategy, SearchPlan, SearchQuery

pytestmark = pytest.mark.skipif(
    os.environ.get("OWNED_VESPA_TEST") != "1", reason="requires disposable real Vespa"
)


@pytest.mark.asyncio
async def test_real_schema_retrieval_collection_isolation_and_delete():
    """Exercise real query compilation/rank profiles; vectors are fixed fixtures."""
    async with httpx.AsyncClient(timeout=120) as http:
        await deploy_schema(http)
        collection, other = str(uuid4()), str(uuid4())
        sync = str(uuid4())
        vector = [1.0] + [0.0] * 383
        urls = []
        try:
            for number, scope in enumerate((collection, other)):
                identity = f"owned-smoke-{uuid4()}"
                url = f"http://localhost:8081/document/v1/airweave/base_entity/docid/{identity}"
                urls.append(url)
                fields = {
                    "entity_id": identity,
                    "created_at": int(datetime(2026, 9, 30, tzinfo=timezone.utc).timestamp()),
                    "name": "Synthetic fundraising discussion",
                    "textual_representation": "fundraising discussion fixture",
                    "payload": json.dumps({"web_url": "https://example.com/fixture"}),
                    "airweave_system_metadata_collection_id": scope,
                    "airweave_system_metadata_sync_id": sync,
                    "airweave_system_metadata_source_name": "gmail",
                    "airweave_system_metadata_entity_type": "GmailMessageEntity",
                    "airweave_system_metadata_original_entity_id": identity,
                    "dense_embedding": {"values": vector},
                    "sparse_embedding": {"cells": {"1": 1.0}},
                }
                fed = await http.post(url, json={"fields": fields})
                assert fed.status_code == 200, fed.text
                if number == 0:
                    expected_id = identity
            contextual = logger.with_context(request_id="synthetic-vespa-smoke")
            engine = VespaVectorDB(
                app=Vespa(url="http://localhost", port=8081),
                logger=contextual,
                filter_translator=FilterTranslator(logger=contextual),
            )
            embeddings = QueryEmbeddings(
                dense_embeddings=[DenseEmbedding(vector=vector)],
                sparse_embedding=SparseEmbedding(indices=[1], values=[1.0]),
            )
            for mode in RetrievalStrategy:
                compiled = await engine.compile_query(
                    SearchPlan(
                        query=SearchQuery(primary="fundraising"),
                        retrieval_strategy=mode,
                        limit=20,
                        offset=0,
                    ),
                    embeddings,
                    collection,
                )
                result = await engine.execute_query(compiled)
                assert not result.engine_partial
                assert [hit.entity_id for hit in result.results] == [expected_id]
                assert result.results[0].textual_representation == "fundraising discussion fixture"
            for operator, expected_ids in (
                ("greater_than_or_equal", []),
                ("less_than", [expected_id]),
            ):
                filtered = await engine.compile_query(
                    SearchPlan(
                        query=SearchQuery(primary="fundraising"),
                        retrieval_strategy=RetrievalStrategy.KEYWORD,
                        filter_groups=[
                            FilterGroup(
                                conditions=[
                                    FilterCondition(
                                        field="created_at",
                                        operator=operator,
                                        value="2026-09-30T00:00:00.500Z",
                                    )
                                ]
                            )
                        ],
                        limit=20,
                        offset=0,
                    ),
                    embeddings,
                    collection,
                )
                result = await engine.execute_query(filtered)
                assert not result.engine_partial
                assert [hit.entity_id for hit in result.results] == expected_ids
            deleted = await http.delete(urls[0])
            assert deleted.status_code == 200
            result = await engine.execute_query(compiled)
            assert result.results == []
        finally:
            for url in urls:
                response = await http.delete(url)
                assert response.status_code in (200, 404)


@pytest.mark.asyncio
async def test_all_schemas_retrieve_varied_vectors_with_collection_and_sync_scope():
    """Small all-schema recall regression; exact ID sets, not relevance-quality claims."""
    collection, foreign_collection, sync, foreign_sync = (str(uuid4()) for _ in range(4))
    filters = [
        FilterGroup(
            conditions=[
                FilterCondition(
                    field="airweave_system_metadata.sync_id",
                    operator="in",
                    value=[sync],
                )
            ]
        )
    ]
    contextual = logger.with_context(request_id="synthetic-all-schema-retrieval")
    engine = VespaVectorDB(
        app=Vespa(url="http://localhost", port=8081),
        logger=contextual,
        filter_translator=FilterTranslator(logger=contextual),
    )
    urls, expected = [], set()
    async with httpx.AsyncClient(timeout=120) as http:
        await deploy_schema(http)
        try:
            for index, schema in enumerate(ALL_VESPA_SCHEMAS):
                # Distinct finite, nonzero vectors exercise angular ANN across schemas.
                vector = [math.sin((index + 1) * (dimension + 1)) for dimension in range(384)]
                for scope, source in (
                    (collection, sync),
                    (foreign_collection, sync),
                    (collection, foreign_sync),
                ):
                    identity = f"all-schema-{uuid4()}"
                    url = f"http://localhost:8081/document/v1/airweave/{schema}/docid/{identity}"
                    urls.append(url)
                    response = await http.post(
                        url,
                        json={
                            "fields": {
                                "entity_id": identity,
                                "name": "Synthetic fundraising discussion",
                                "textual_representation": "fundraising all schema fixture",
                                "payload": "{}",
                                "airweave_system_metadata_collection_id": scope,
                                "airweave_system_metadata_sync_id": source,
                                "airweave_system_metadata_source_name": "gmail",
                                "airweave_system_metadata_entity_type": "SyntheticEntity",
                                "airweave_system_metadata_original_entity_id": identity,
                                "dense_embedding": {"values": vector},
                                "sparse_embedding": {"cells": {"1": 1.0}},
                            }
                        },
                    )
                    assert response.status_code == 200, response.text
                    if scope == collection and source == sync:
                        expected.add(identity)
            vectors = (
                [1.0] * 384,
                [-1.0] * 384,
                [math.cos(dimension + 1) for dimension in range(384)],
            )
            for mode in RetrievalStrategy:
                for vector in vectors if mode != RetrievalStrategy.KEYWORD else vectors[:1]:
                    compiled = await engine.compile_query(
                        SearchPlan(
                            query=SearchQuery(primary="fundraising"),
                            retrieval_strategy=mode,
                            filter_groups=filters,
                            limit=20,
                            offset=0,
                        ),
                        QueryEmbeddings(
                            dense_embeddings=[DenseEmbedding(vector=vector)],
                            sparse_embedding=SparseEmbedding(indices=[1], values=[1.0]),
                        ),
                        collection,
                    )
                    result = await engine.execute_query(compiled)
                    assert not result.engine_partial, (mode, result.engine_coverage_percent)
                    assert {item.entity_id for item in result.results} == expected, mode
                    assert len(result.results) == len(ALL_VESPA_SCHEMAS)
            browsed = await engine.filter_search(filters, collection, limit=20)
            assert {item.entity_id for item in browsed} == expected
            assert await engine.count(filters, collection) == len(expected)
        finally:
            # Delete and read back only this run's random synthetic document IDs.
            for url in urls:
                deleted = await http.delete(url)
                assert deleted.status_code in (200, 404), deleted.text
                assert (await http.get(url)).status_code == 404


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("OWNED_MINILM_TEST") != "1", reason="requires disposable pinned MiniLM service"
)
async def test_real_minilm_paraphrase_retrieval_and_exact_keyword():
    """Small synthetic retrieval baseline, not a general search-quality benchmark."""
    import asyncio

    from airweave.domains.embedders.dense.local import LocalDenseEmbedder
    from airweave.domains.embedders.sparse.fastembed import FastEmbedSparseEmbedder

    corpus = [
        (
            "funding",
            "Investors and venture capital",
            "We are raising a seed round to finance the startup. "
            "Venture investors will purchase equity in the company.",
        ),
        (
            "travel",
            "Summer holiday",
            "Book a seaside hotel for our family vacation. We will swim and relax on the beach.",
        ),
        (
            "incident",
            "Database incident ZXQ4829",
            "The database connection pool exhausted its limit. "
            "Restarting the server restored normal query latency.",
        ),
        (
            "food",
            "Dinner recipe",
            "Roast potatoes with olive oil and garlic. Serve the vegetables with fresh bread.",
        ),
    ]
    collection, foreign = str(uuid4()), str(uuid4())
    embedder = LocalDenseEmbedder(inference_url="http://localhost:8080", dimensions=384)
    sparse_embedder = FastEmbedSparseEmbedder(model="Qdrant/bm25")
    urls = []
    async with httpx.AsyncClient(timeout=120) as http:
        try:
            for _attempt in range(60):
                try:
                    health = await http.get("http://localhost:8080/.well-known/ready", timeout=5)
                    if health.status_code == 204:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(2)
            else:
                pytest.fail("Pinned MiniLM service did not become healthy")
            await deploy_schema(http)
            vectors = await embedder.embed_many([title + "\n" + text for _, title, text in corpus])
            sparse_vectors = await sparse_embedder.embed_many(
                [title + "\n" + text for _, title, text in corpus]
            )
            identities = {}
            for (key, title, text), vector, sparse in zip(
                corpus, vectors, sparse_vectors, strict=True
            ):
                assert len(vector.vector) == 384 and any(vector.vector)
                identity = f"quality-{uuid4()}"
                identities[key] = identity
                # A duplicate in another collection must not become a result.
                for scope, suffix in ((collection, ""), (foreign, "-foreign")):
                    document_id = identity + suffix
                    url = f"http://localhost:8081/document/v1/airweave/base_entity/docid/{document_id}"
                    urls.append(url)
                    response = await http.post(
                        url,
                        json={
                            "fields": {
                                "entity_id": document_id,
                                "name": title,
                                "textual_representation": text,
                                "payload": json.dumps({"web_url": "https://example.com/" + key}),
                                "airweave_system_metadata_collection_id": scope,
                                "airweave_system_metadata_sync_id": str(uuid4()),
                                "airweave_system_metadata_source_name": "gmail",
                                "airweave_system_metadata_entity_type": "GmailMessageEntity",
                                "airweave_system_metadata_original_entity_id": document_id,
                                "dense_embedding": {"values": vector.vector},
                                "sparse_embedding": {
                                    "cells": {
                                        str(index): value
                                        for index, value in zip(
                                            sparse.indices, sparse.values, strict=True
                                        )
                                    }
                                },
                            }
                        },
                    )
                    assert response.status_code == 200, response.text
            contextual = logger.with_context(request_id="synthetic-minilm-baseline")
            engine = VespaVectorDB(
                app=Vespa(url="http://localhost", port=8081),
                logger=contextual,
                filter_translator=FilterTranslator(logger=contextual),
            )
            for query, expected, mode in (
                (
                    "How are we obtaining money from equity investors?",
                    "funding",
                    RetrievalStrategy.SEMANTIC,
                ),
                ("How was the database outage resolved?", "incident", RetrievalStrategy.SEMANTIC),
                ("ZXQ4829", "incident", RetrievalStrategy.KEYWORD),
            ):
                embeddings = QueryEmbeddings(
                    dense_embeddings=[await embedder.embed(query)]
                    if mode == RetrievalStrategy.SEMANTIC
                    else None,
                    sparse_embedding=await sparse_embedder.embed(query)
                    if mode == RetrievalStrategy.KEYWORD
                    else None,
                )
                compiled = await engine.compile_query(
                    SearchPlan(
                        query=SearchQuery(primary=query), retrieval_strategy=mode, limit=4, offset=0
                    ),
                    embeddings,
                    collection,
                )
                results = await engine.execute_query(compiled)
                assert not results.engine_partial
                assert results.results, (query, "no matches")
                assert results.results[0].entity_id == identities[expected], (
                    query,
                    [hit.name for hit in results.results],
                )
                assert all(hit.entity_id in identities.values() for hit in results.results)
        finally:
            await embedder.close()
            for url in urls:
                response = await http.delete(url)
                assert response.status_code in (200, 404)
