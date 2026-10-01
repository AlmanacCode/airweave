"""Shared ranking orchestration with fake ports; SQL fencing is separately integrated."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from airweave.core.protocols.reranker import RerankerResult
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.search.types import SearchResults


class CharacterTokenizer:
    """Conservative fake: one character per token, including Unicode."""

    def count_tokens(self, text):
        return len(text)


def service(rerank=None):
    return OwnedSearchService(
        MagicMock(),
        MagicMock(),
        reranker=SimpleNamespace(rerank=rerank) if rerank else None,
        tokenizer=CharacterTokenizer(),
    )


async def test_one_shared_call_with_exact_budget_and_no_raw_payload():
    keys = [uuid4() for _ in range(205)]
    hits = {
        key: SimpleNamespace(title="Title " + "x" * 300, raw_source_fields={"secret": "NEVER"})
        for key in keys
    }
    matched = {key: "🎵" * 3000 for key in keys}
    rerank = AsyncMock(return_value=[RerankerResult(i, float(i)) for i in range(200)])
    ranked, meta = await service(rerank)._rank("query", keys, hits, matched)
    assert ranked == keys[199::-1]
    rerank.assert_awaited_once()
    query, documents = rerank.call_args.args
    assert query == "query" and len(documents) == 200
    assert all(len(doc) <= 2048 and "NEVER" not in doc for doc in documents)
    assert meta.candidates_considered == 205 and meta.candidates_reranked == 200
    assert meta.shortlist_truncated and meta.input_truncated_documents == 200
    assert meta.method == "shared_rerank" and meta.fallback_reason is None


@pytest.mark.parametrize(
    "output",
    [
        [RerankerResult(0, 0.5)],
        [RerankerResult(0, 0.5), RerankerResult(0, 0.3)],
        [RerankerResult(True, 0.5), RerankerResult(0, 0.3)],
        [RerankerResult(-1, 0.5), RerankerResult(0, 0.3)],
        [RerankerResult(0, float("nan")), RerankerResult(1, 0.3)],
        [RerankerResult(0, float("inf")), RerankerResult(1, 0.3)],
        [{"index": 0}, {"index": 1}],
        None,
    ],
)
async def test_invalid_output_has_explicit_deterministic_fallback(output):
    keys = [uuid4(), uuid4()]
    hits = {key: SimpleNamespace(title="Title") for key in keys}
    ranked, meta = await service(AsyncMock(return_value=output))._rank(
        "query",
        keys,
        hits,
        {key: "Text" for key in keys},
    )
    assert ranked == keys and meta.fallback_reason == "invalid_output"
    assert meta.method == "retrieval_rank" and meta.candidates_reranked == 0


@pytest.mark.parametrize(
    "error,reason", [(TimeoutError(), "timeout"), (RuntimeError("secret"), "provider_error")]
)
async def test_error_fallback_is_sanitized(error, reason):
    key = uuid4()
    ranked, meta = await service(AsyncMock(side_effect=error))._rank(
        "query",
        [key],
        {key: SimpleNamespace(title="Title")},
        {key: "Text"},
    )
    assert ranked == [key] and meta.fallback_reason == reason
    assert "secret" not in meta.model_dump_json()


async def test_absent_port_and_cancellation():
    key = uuid4()
    args = ("q", [key], {key: SimpleNamespace(title="Title")}, {key: "Text"})
    assert (await service()._rank(*args))[1].fallback_reason == "unconfigured"
    with pytest.raises(asyncio.CancelledError):
        await service(AsyncMock(side_effect=asyncio.CancelledError()))._rank(*args)


@pytest.mark.parametrize("withdraw", ["before", "during", None])
async def test_global_orchestration_rechecks_before_and_after_without_holding_session(withdraw):
    keys = [UUID(int=1), UUID(int=2)]
    syncs = [uuid4(), uuid4()]
    collections = [uuid4(), uuid4()]
    hits = {key: SimpleNamespace(title="Title") for key in keys}
    active = 0

    @asynccontextmanager
    async def sessions():
        nonlocal active
        active += 1
        try:
            yield object()
        finally:
            active -= 1

    async def rerank(query, documents, top_n):
        assert active == 0
        assert len(documents) == (1 if withdraw == "before" else 2)
        return [RerankerResult(index, float(index)) for index in range(top_n)]

    search = service(AsyncMock(side_effect=rerank))
    search._executor.prepare_query = AsyncMock(return_value=None)
    search._executor.execute = AsyncMock(return_value=SearchResults(results=[]))
    search._resolve_scopes = AsyncMock(
        return_value=(
            {},
            {(collections[0], "one"): [syncs[0]], (collections[1], "two"): [syncs[1]]},
        )
    )
    search._enrich = AsyncMock(
        side_effect=[
            ({keys[0]: hits[keys[0]]}, {keys[0]: (1, None)}, {keys[0]: "First collection"}, 0, 0),
            (
                {keys[1]: hits[keys[1]]},
                {keys[1]: (0.5, None)},
                {keys[1]: "Second collection"},
                0,
                0,
            ),
        ]
    )
    search._final_publications = AsyncMock(
        side_effect=[
            {keys[1]} if withdraw == "before" else set(keys),
            {keys[1]} if withdraw else set(keys),
        ]
    )
    search._coverage = AsyncMock(return_value=())
    # Observe final ordering without replacing the actual ranking/session flow.
    from unittest.mock import patch

    import airweave.domains.search.owned as module

    with patch.object(module, "OwnedSearchResponse", side_effect=lambda **fields: fields):
        result = await search.search(
            sessions,
            SimpleNamespace(),
            OwnedSearchRequest(query="q", sync_ids=tuple(syncs), limit=1),
        )
    assert result["items"] == (hits[keys[1]],)
    assert search._final_publications.await_count == 2
    assert search._resolve_scopes.await_count == 3
    assert result["excluded_candidates"] == int(withdraw is not None)


async def test_timeout_enforced_around_remote_await(monkeypatch):
    import airweave.domains.search.owned as module

    timeout = asyncio.timeout
    limits = []

    def short_timeout(seconds):
        limits.append(seconds)
        return timeout(0.001)

    async def slow(*args, **kwargs):
        await asyncio.sleep(1)

    monkeypatch.setattr(module.asyncio, "timeout", short_timeout)
    key = uuid4()
    ranked, metadata = await service(AsyncMock(side_effect=slow))._rank(
        "query",
        [key],
        {key: SimpleNamespace(title="Title")},
        {key: "Text"},
    )
    assert limits == [10] and ranked == [key] and metadata.fallback_reason == "timeout"


async def test_empty_candidates_need_no_ranking_fallback():
    ranked, metadata = await service()._rank("q", [], {}, {})
    assert ranked == [] and metadata.method == "retrieval_rank"
    assert metadata.fallback_reason is None and metadata.candidates_considered == 0
    assert metadata.token_count_basis == "local_tokenizer"
