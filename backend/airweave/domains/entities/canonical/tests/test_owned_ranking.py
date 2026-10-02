"""Real SQL disclosure and final publication fencing around a fake reranker."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event

from airweave.api import deps
from airweave.core.protocols.reranker import RerankerResult
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.domains.entities.canonical.tests.test_owned_search import (  # noqa: F401
    http_search,  # noqa: F811
    indexed,  # noqa: F811
)
from airweave.domains.entities.canonical.tests.test_search_visibility import hit
from airweave.domains.search.types import SearchResults


class QuarterCharacterTokenizer:
    """Fake count sufficient to expose full-chunk versus display-excerpt behavior."""

    def count_tokens(self, text):
        return (len(text) + 3) // 4


@pytest.mark.parametrize("edit_stage", [None, "before", "during", "before_revoke", "during_revoke"])
async def test_retained_match_disclosure_and_final_gate(
    database,
    source,
    indexed,  # noqa: F811
    http_search,  # noqa: F811
    edit_stage,
):
    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    capture_service, _ = source
    candidate = hit(fence, locator.encode()).model_copy(
        update={
            "textual_representation": "x" * 2500 + "MATCH_AFTER_DISPLAY_BOUNDARY",
            "raw_source_fields": {"secret": "RAW_MUST_NOT_TRANSFER"},
        }
    )
    vector.seed_results(SearchResults(results=[candidate]))
    service = client._transport.app.dependency_overrides[deps.get_container]().owned_search
    service._tokenizer = QuarterCharacterTokenizer()
    checked_out = 0

    def checkout(*args):
        nonlocal checked_out
        checked_out += 1

    def checkin(*args):
        nonlocal checked_out
        checked_out -= 1

    async with database() as db:
        engine = db.bind.sync_engine
    event.listen(engine, "checkout", checkout)
    event.listen(engine, "checkin", checkin)

    async def edit():
        changed = (
            observation(kind="delete", removal_reason="access_revoked")
            if edit_stage and edit_stage.endswith("revoke")
            else observation(payload={"changed": True})
        )
        await capture(database, capture_service, fence, changed)

    if edit_stage in {"before", "before_revoke"}:
        enrich = service._enrich

        async def enrich_then_edit(*args):
            result = await enrich(*args)
            await edit()
            return result

        service._enrich = enrich_then_edit

    async def rerank(query, documents, top_n):
        assert checked_out == 0
        assert "MATCH_AFTER_DISPLAY_BOUNDARY" in documents[0]
        assert "RAW_MUST_NOT_TRANSFER" not in documents[0]
        if edit_stage in {"during", "during_revoke"}:
            await edit()
        return [RerankerResult(index=0, relevance_score=0.9)]

    service._reranker = type("Port", (), {})()
    service._reranker.rerank = AsyncMock(side_effect=rerank)
    try:
        response = await client.post(
            "/sync/search",
            json={
                "query": "budget",
                "sync_ids": [str(fence.sync_id)],
                "mode": "keyword",
            },
        )
    finally:
        event.remove(engine, "checkout", checkout)
        event.remove(engine, "checkin", checkin)
    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body["items"]) == int(edit_stage is None)
    assert service._reranker.rerank.await_count == int(
        edit_stage not in {"before", "before_revoke"}
    )
    if edit_stage is None:
        assert body["ranking"]["method"] == "shared_rerank"
        assert "MATCH_AFTER_DISPLAY_BOUNDARY" not in body["items"][0]["excerpts"][0]
    else:
        assert body["excluded_candidates"] == 1
