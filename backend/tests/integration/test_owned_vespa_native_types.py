"""Native type prefilters against a separately configured disposable Vespa.

Requires OWNED_VESPA_TEST=1 and explicit OWNED_VESPA_QUERY_URL /
OWNED_VESPA_CONFIG_URL. Never point these at the retained demo or customer index.
Fixed vectors qualify filtering/rank-profile execution, not embedding quality.
"""

import json
import os
from urllib.parse import urlsplit
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
from airweave.domains.search.types.filters import FilterCondition, FilterGroup
from airweave.domains.search.types.plan import RetrievalStrategy, SearchPlan, SearchQuery

pytestmark = pytest.mark.skipif(
    os.environ.get("OWNED_VESPA_TEST") != "1",
    reason="requires explicitly disposable real Vespa",
)


@pytest.mark.asyncio
async def test_native_types_filter_before_ranking_in_real_vespa():
    query_url = os.environ["OWNED_VESPA_QUERY_URL"].rstrip("/")
    config_url = os.environ["OWNED_VESPA_CONFIG_URL"].rstrip("/")
    address = urlsplit(query_url)
    collection, other, sync = (str(uuid4()) for _ in range(3))
    vector = [1.0] + [0.0] * 383
    urls, expected = [], {}
    async with httpx.AsyncClient(timeout=120) as http:
        await deploy_schema(http, config_url=config_url, query_url=query_url)
        try:
            for label, kind, scope in (
                ("person", "person", collection),
                ("task", "task", collection),
                ("provider", None, collection),
                ("legacy", None, collection),
                ("other-owner", "person", other),
            ):
                identity = f"native-type-{uuid4()}"
                expected[label] = identity
                url = f"{query_url}/document/v1/airweave/base_entity/docid/{identity}"
                urls.append(url)
                fields = {
                    "entity_id": identity,
                    "name": "Synthetic planning discussion",
                    "textual_representation": "planning discussion fixture",
                    "payload": json.dumps({"web_url": "https://example.test/fixture"}),
                    "airweave_system_metadata_collection_id": scope,
                    "airweave_system_metadata_sync_id": sync,
                    "airweave_system_metadata_source_name": "almanac",
                    "airweave_system_metadata_entity_type": "NativeFixture",
                    "airweave_system_metadata_original_entity_id": identity,
                    "dense_embedding": {"values": vector},
                    "sparse_embedding": {"cells": {"1": 1.0}},
                }
                if kind is not None:
                    fields["airweave_system_metadata_native_type"] = kind
                fed = await http.post(url, json={"fields": fields})
                assert fed.status_code == 200, fed.text
            contextual = logger.with_context(request_id="native-type-real-vespa")
            engine = VespaVectorDB(
                app=Vespa(url=f"{address.scheme}://{address.hostname}", port=address.port),
                logger=contextual,
                filter_translator=FilterTranslator(logger=contextual),
            )
            embeddings = QueryEmbeddings(
                dense_embeddings=[DenseEmbedding(vector=vector)],
                sparse_embedding=SparseEmbedding(indices=[1], values=[1.0]),
            )
            for mode in RetrievalStrategy:
                for kinds in (["person"], ["person", "task"]):
                    compiled = await engine.compile_query(
                        SearchPlan(
                            query=SearchQuery(primary="planning"),
                            retrieval_strategy=mode,
                            filter_groups=[
                                FilterGroup(
                                    conditions=[
                                        FilterCondition(
                                            field="airweave_system_metadata.native_type",
                                            operator="in",
                                            value=kinds,
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
                    result = await engine.execute_query(compiled)
                    assert not result.engine_partial
                    assert {hit.entity_id for hit in result.results} == {
                        expected[kind] for kind in kinds
                    }
        finally:
            for url in urls:
                deleted = await http.delete(url)
                assert deleted.status_code == 200, deleted.text
