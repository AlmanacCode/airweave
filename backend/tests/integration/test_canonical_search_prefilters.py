"""Opt-in real Vespa: canonical filtering happens before a bounded result window."""

import asyncio
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest
from vespa.application import Vespa

from airweave.core.logging import logger
from airweave.domains.embedders.types import DenseEmbedding, SparseEmbedding
from airweave.domains.entities.canonical.search_metadata import epoch_microseconds
from airweave.domains.entities.canonical.tests.vespa_helpers import deploy_schema
from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.search.types import FilterGroup, SearchPlan, SearchQuery
from airweave.domains.search.types.embeddings import QueryEmbeddings

pytestmark = pytest.mark.skipif(
    os.environ.get("OWNED_VESPA_TEST") != "1", reason="requires disposable real Vespa"
)


async def test_prefilter_excludes_over_two_hundred_wrong_dates_before_top_matches():
    async with httpx.AsyncClient(timeout=120) as http:
        await deploy_schema(http)
        collection, sync = str(uuid4()), uuid4()
        vector = [1.0] + [0.0] * 383
        instant = datetime(2026, 1, 1, microsecond=1, tzinfo=timezone.utc)
        urls = []
        expected = None
        try:
            for number in range(204):
                identity = f"canonical-prefilter-{uuid4()}"
                url = f"http://localhost:8081/document/v1/airweave/base_entity/docid/{identity}"
                urls.append(url)
                correct = number == 203
                known = number != 201
                fields = {
                    "entity_id": identity,
                    "name": "fundraising",
                    "textual_representation": "fundraising fixture",
                    "payload": json.dumps({}),
                    "airweave_system_metadata_collection_id": collection,
                    "airweave_system_metadata_sync_id": str(sync),
                    "airweave_system_metadata_original_entity_id": identity,
                    "airweave_system_metadata_source_name": "google_calendar",
                    "airweave_system_metadata_entity_type": "GoogleCalendarEventEntity",
                    "airweave_system_metadata_canonical_record_type": "message"
                    if number == 202
                    else "event",
                    "airweave_system_metadata_source_created_known": int(known),
                    "airweave_system_metadata_source_created_us": epoch_microseconds(instant)
                    if correct
                    else 0,
                    "dense_embedding": {"values": vector},
                    "sparse_embedding": {"cells": {"1": 1.0}},
                }
                response = await http.post(url, json={"fields": fields})
                assert response.status_code == 200, response.text
                if correct:
                    expected = identity
            context = logger.with_context(request_id="canonical-prefilter-test")
            engine = VespaVectorDB(
                app=Vespa(url="http://localhost", port=8081),
                logger=context,
                filter_translator=FilterTranslator(context),
            )
            request = OwnedSearchRequest(
                query="fundraising",
                sync_ids=(sync,),
                mode="keyword",
                record_types=("event",),
                created_after=instant,
            )
            compiled = await engine.compile_query(
                SearchPlan(
                    query=SearchQuery(primary="fundraising"),
                    retrieval_strategy="keyword",
                    limit=200,
                    offset=0,
                    filter_groups=[
                        FilterGroup(conditions=OwnedSearchService._prefilters(request, [sync]))
                    ],
                ),
                QueryEmbeddings(
                    dense_embeddings=[DenseEmbedding(vector=vector)],
                    sparse_embedding=SparseEmbedding(indices=[1], values=[1.0]),
                ),
                collection,
            )
            result = await engine.execute_query(compiled)
            assert not result.engine_partial
            assert [hit.entity_id for hit in result.results] == [expected]
        finally:
            await asyncio.gather(*(http.delete(url) for url in urls))
