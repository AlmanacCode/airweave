"""Cohere boundary: real SDK serialization over a mocked HTTP transport."""

import httpx
import pytest

from airweave.domains.embedders.dense.cohere import CohereDenseEmbedder
from airweave.domains.embedders.exceptions import (
    EmbedderAuthError,
    EmbedderDimensionError,
    EmbedderRateLimitError,
    EmbedderResponseError,
)


@pytest.fixture
def build(monkeypatch):
    original = httpx.AsyncClient

    def factory(handler):
        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs),
        )
        return CohereDenseEmbedder(api_key="fixture", model="embed-v5.0-pro", dimensions=256)

    return factory


def response(vectors):
    return httpx.Response(
        200,
        json={
            "id": "test",
            "texts": [],
            "embeddings": {"float": vectors},
            "meta": {"api_version": {"version": "2"}},
        },
    )


async def test_intent_batch_order_and_no_truncation(build):
    import json

    calls = []

    def handler(request):
        body = json.loads(request.content)
        calls.append(body)
        return response([[float(text.split(":")[0])] * 256 for text in body["texts"]])

    embedder = build(handler)
    try:
        texts = [f"{i}:नमस्ते hello" for i in range(97)]
        vectors = await embedder.embed_many(texts)
        await embedder.embed_many(["0:नमस्ते"], purpose="query")
        assert [v.vector[0] for v in vectors] == list(range(97))
        assert [len(call["texts"]) for call in calls] == [96, 1, 1]
        assert [call["input_type"] for call in calls] == [
            "search_document",
            "search_document",
            "search_query",
        ]
        assert all(call["truncate"] == "NONE" for call in calls)
        assert calls[0]["texts"][0] == texts[0]
        assert await embedder.embed_many([]) == []
        assert len(calls) == 3
    finally:
        await embedder.close()


@pytest.mark.parametrize("status,error", [(401, EmbedderAuthError), (429, EmbedderRateLimitError)])
async def test_safe_errors_and_no_hidden_retries(build, status, error):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={"message": "private provider detail"})

    embedder = build(handler)
    try:
        with pytest.raises(error) as exc:
            await embedder.embed("private document")
        assert "private" not in str(exc.value)
        assert len(calls) == 1
    finally:
        await embedder.close()


@pytest.mark.parametrize(
    "vectors,error", [([], EmbedderResponseError), ([[1.0]], EmbedderDimensionError)]
)
async def test_reject_corrupt_response(build, vectors, error):
    embedder = build(lambda request: response(vectors))
    try:
        with pytest.raises(error):
            await embedder.embed("content")
    finally:
        await embedder.close()
