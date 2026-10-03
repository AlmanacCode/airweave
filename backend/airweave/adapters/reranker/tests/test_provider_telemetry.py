"""Provider acknowledgments are observable without request content or billing authority."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from cohere.types import (
    ApiMeta,
    ApiMetaBilledUnits,
    EmbedByTypeResponse,
    EmbedByTypeResponseEmbeddings,
    RerankResponse,
)
from mistralai.models import FileChunk, OCRResponse, OCRUsageInfo

from airweave.adapters.reranker.cohere import CohereReranker
from airweave.adapters.reranker.exceptions import RerankerError
from airweave.core.logging import logger
from airweave.domains.embedders.dense.cohere import CohereDenseEmbedder
from airweave.domains.ocr.mistral.ocr_client import MistralOcrClient


@pytest.fixture(autouse=True)
def capture_adapter_logs(caplog):
    logger.logger.addHandler(caplog.handler)
    try:
        yield
    finally:
        logger.logger.removeHandler(caplog.handler)


def observations(caplog):
    return [r.custom_dimensions for r in caplog.records if r.message == "Provider call completed"]


@pytest.mark.parametrize("reported", [False, True])
async def test_embed_reported_units_or_null_before_validation(caplog, reported):
    caplog.set_level("INFO")
    embedder = CohereDenseEmbedder(api_key="private-key", model="embed-v5.0-pro", dimensions=256)
    embedder._client.embed = AsyncMock(
        return_value=EmbedByTypeResponse(
            id="fixture",
            embeddings=EmbedByTypeResponseEmbeddings(float_=[[1.0]]),
            meta=ApiMeta(billed_units=ApiMetaBilledUnits(input_tokens=7) if reported else None),
        )
    )
    try:
        from airweave.domains.embedders.exceptions import EmbedderDimensionError

        with pytest.raises(EmbedderDimensionError):
            await embedder.embed("private document content")
        (row,) = observations(caplog)
        assert row["outcome"] == "acknowledged"
        assert row["billed_units"]["input_tokens"] == 7 if reported else row["billed_units"] is None
        assert row["elapsed_ms"] >= 0
        assert "private" not in json.dumps(row)
    finally:
        await embedder.close()


@pytest.mark.parametrize("reported", [False, True])
async def test_rerank_reported_units_or_null(caplog, reported):
    caplog.set_level("INFO")
    reranker = CohereReranker(api_key="private-key")
    reranker._client.rerank = AsyncMock(
        return_value=RerankResponse(
            results=[],
            meta=ApiMeta(
                billed_units=ApiMetaBilledUnits(search_units=2) if reported else None,
                warnings=["private response warning"],
            ),
        )
    )
    assert await reranker.rerank("private query", ["private document"]) == []
    (row,) = observations(caplog)
    assert row["outcome"] == "acknowledged"
    assert row["billed_units"]["search_units"] == 2 if reported else row["billed_units"] is None
    assert "private" not in json.dumps(row)


@pytest.mark.parametrize("provider", ["embed", "rerank", "ocr"])
@pytest.mark.parametrize("error", [RuntimeError("private raw exception"), asyncio.CancelledError()])
async def test_errors_and_cancellation_have_unknown_consumption(caplog, provider, error):
    caplog.set_level("INFO")
    if provider == "embed":
        adapter = CohereDenseEmbedder(api_key="private-key", model="embed-v5.0-pro", dimensions=256)
        adapter._client.embed = AsyncMock(side_effect=error)
        call = adapter.embed("private input")
    elif provider == "rerank":
        adapter = CohereReranker(api_key="private-key")
        adapter._client.rerank = AsyncMock(side_effect=error)
        call = adapter.rerank("private query", ["private input"])
    else:
        adapter = MistralOcrClient()
        adapter._client = AsyncMock()
        adapter._client.ocr.process_async.side_effect = error
        call = adapter._ocr_request(FileChunk(file_id="private-file"))
    try:
        with pytest.raises(
            asyncio.CancelledError
            if isinstance(error, asyncio.CancelledError)
            else (RuntimeError, RerankerError)
        ):
            await call
        (row,) = observations(caplog)
        assert row["outcome"] == "failed"
        assert row.get("billed_units") is None
        assert row.get("pages_processed") is None
        assert row.get("doc_size_bytes") is None
        assert "private" not in json.dumps(row)
    finally:
        if provider == "embed":
            await adapter.close()


async def test_ocr_preserves_actual_reported_pages(caplog):
    caplog.set_level("INFO")
    client = MistralOcrClient()
    client._client = AsyncMock()
    response = OCRResponse(
        pages=[], model="fixture", usage_info=OCRUsageInfo(pages_processed=3, doc_size_bytes=None)
    )
    client._client.ocr.process_async.return_value = response
    assert await client._ocr_request(FileChunk(file_id="private-file")) is response
    (row,) = observations(caplog)
    assert row["outcome"] == "acknowledged"
    assert row["pages_processed"] == 3
    assert row["doc_size_bytes"] is None
    assert "private" not in json.dumps(row)
