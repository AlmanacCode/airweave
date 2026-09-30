"""Real engine smoke using only synthetic records in a disposable Vespa instance.

Explicit opt-in: OWNED_VESPA_TEST=1. This deploys the repository schema package
and is unsuitable for an existing application or customer index.
"""

import json
import os
from uuid import uuid4

import httpx
import pytest
from vespa.application import Vespa

from airweave.core.logging import logger
from airweave.domains.embedders.types import DenseEmbedding, SparseEmbedding
from airweave.domains.entities.canonical.tests.vespa_helpers import deploy_schema
from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
from airweave.domains.search.types.embeddings import QueryEmbeddings
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
            deleted = await http.delete(urls[0])
            assert deleted.status_code == 200
            result = await engine.execute_query(compiled)
            assert result.results == []
        finally:
            for url in urls:
                response = await http.delete(url)
                assert response.status_code in (200, 404)
