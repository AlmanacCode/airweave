"""Actual actor OR/AND filters in the explicitly disposable, already deployed Vespa.

No schema deployment; only fresh synthetic documents are fed and deleted.
Fixed vectors prove compiled filtering across rank profiles, not embedding quality.
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
from airweave.domains.entities.canonical.actors import ActorAnyOf, ActorFilter, actor_match_token
from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.search.types.embeddings import QueryEmbeddings
from airweave.domains.search.types.filters import FilterGroup
from airweave.domains.search.types.plan import RetrievalStrategy, SearchPlan, SearchQuery

pytestmark = pytest.mark.skipif(
    os.environ.get("OWNED_VESPA_TEST") != "1", reason="requires explicitly disposable real Vespa"
)


@pytest.mark.asyncio
@pytest.mark.parametrize("match", ["raw", "endpoint"])
async def test_actor_any_of_and_single_role_filters_in_real_vespa(match):
    query_url = os.environ["OWNED_VESPA_QUERY_URL"].rstrip("/")
    address = urlsplit(query_url)
    assert address.hostname in ("127.0.0.1", "localhost") and address.port == 8086
    collection, other_collection, sync, other_sync = (uuid4() for _ in range(4))
    vector = [1.0] + [0.0] * 383
    required = ActorFilter(role="current_chat_member", handle="Required")
    first = "First" if match == "raw" else "+۱۴۱۵۵۵۵۲۶۷۱ ext. १२"
    second = "Second" if match == "raw" else "Case@EXAMPLE"
    wrong = "Third" if match == "raw" else "+14155552671 x13"
    case = "first" if match == "raw" else "case@example"
    alternatives = ("First", "Second") if match == "raw" else ("+14155552671 x12", "Case@EXAMPLE")
    any_of = ActorAnyOf(role="sender", handles=alternatives, match=match)
    urls, expected = [], set()
    async with httpx.AsyncClient(timeout=30) as http:
        try:
            for label, sender, role, member, selected_collection, selected_sync in (
                ("first", first, "sender", True, collection, sync),
                ("second", second, "sender", True, collection, sync),
                ("wrong-sender", wrong, "sender", True, collection, sync),
                ("wrong-role", first, "contact_handle", True, collection, sync),
                ("case", case, "sender", True, collection, sync),
                ("missing-and", first, "sender", False, collection, sync),
                ("other-source", first, "sender", True, collection, other_sync),
                ("other-owner", first, "sender", True, other_collection, sync),
            ):
                identity = f"actor-any-of-{label}-{uuid4()}"
                if label in ("first", "second"):
                    expected.add(identity)
                url = f"{query_url}/document/v1/airweave/base_entity/docid/{identity}"
                urls.append(url)
                fields = {
                    "entity_id": identity,
                    "name": "Synthetic planning discussion",
                    "textual_representation": "planning discussion fixture",
                    "payload": json.dumps({"web_url": "https://example.test/fixture"}),
                    "airweave_system_metadata_collection_id": str(selected_collection),
                    "airweave_system_metadata_sync_id": str(selected_sync),
                    "airweave_system_metadata_source_name": "imessage",
                    "airweave_system_metadata_entity_type": "ActorFixture",
                    "airweave_system_metadata_original_entity_id": identity,
                    "airweave_system_metadata_actor_tokens": [
                        actor_match_token(role, sender, match),
                        *([required.token] if member else []),
                    ],
                    "dense_embedding": {"values": vector},
                    "sparse_embedding": {"cells": {"1": 1.0}},
                }
                fed = await http.post(url, json={"fields": fields})
                assert fed.status_code == 200, fed.text
            contextual = logger.with_context(request_id="actor-any-of-real-vespa")
            engine = VespaVectorDB(
                app=Vespa(url=f"{address.scheme}://{address.hostname}", port=address.port),
                logger=contextual,
                filter_translator=FilterTranslator(logger=contextual),
            )
            embeddings = QueryEmbeddings(
                dense_embeddings=[DenseEmbedding(vector=vector)],
                sparse_embedding=SparseEmbedding(indices=[1], values=[1.0]),
            )
            request = OwnedSearchRequest(
                query="planning", sync_ids=(sync,), actor_filters=(required,), actor_any_of=any_of
            )
            for mode in RetrievalStrategy:
                compiled = await engine.compile_query(
                    SearchPlan(
                        query=SearchQuery(primary=request.query),
                        retrieval_strategy=mode,
                        filter_groups=[
                            FilterGroup(conditions=OwnedSearchService._prefilters(request, [sync]))
                        ],
                        limit=20,
                        offset=0,
                    ),
                    embeddings,
                    str(collection),
                )
                result = await engine.execute_query(compiled)
                assert not result.engine_partial
                assert {hit.entity_id for hit in result.results} == expected
        finally:
            for url in urls:
                deleted = await http.delete(url)
                assert deleted.status_code == 200, deleted.text
