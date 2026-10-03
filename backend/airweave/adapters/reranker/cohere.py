"""Cohere reranker implementation."""

from __future__ import annotations

import time

import cohere

from airweave.adapters.reranker.exceptions import RerankerError
from airweave.adapters.reranker.types import RerankerResult
from airweave.core.logging import logger
from airweave.core.protocols.reranker import RerankerProtocol

COHERE_RERANK_MODEL = "rerank-v4.0-pro"
COHERE_MAX_DOCUMENTS = 1000

# Our chunker (SemanticChunker) guarantees textual_representation is at most
# 8192 tokens (tiktoken cl100k_base). Setting max_tokens_per_doc to match means
# Cohere accepts every document without truncation.
COHERE_MAX_TOKENS_PER_DOC = 8192


class CohereReranker(RerankerProtocol):
    """Reranker using Cohere's rerank API."""

    def __init__(self, api_key: str) -> None:
        """Initialize with Cohere API key."""
        self._client = cohere.AsyncClientV2(api_key=api_key)

    async def rerank(
        self,
        query: str,
        documents: list[str],
        top_n: int | None = None,
    ) -> list[RerankerResult]:
        """Rerank documents using Cohere rerank API.

        Structured logs describe SDK acknowledgments, not validated rankings,
        durable invoices or full costs. SDK-internal retries are not individually
        visible, and calls with no response have unknown consumption.

        Raises:
            RerankerError: If the Cohere API call fails or document limit exceeded.
        """
        if len(documents) > COHERE_MAX_DOCUMENTS:
            raise RerankerError(
                f"Cohere rerank: {len(documents)} documents "
                f"exceeds the API limit of {COHERE_MAX_DOCUMENTS}"
            )

        started = time.monotonic()
        response = None
        try:
            response = await self._client.rerank(
                model=COHERE_RERANK_MODEL,
                query=query,
                documents=documents,
                top_n=top_n if top_n is not None else len(documents),
                max_tokens_per_doc=COHERE_MAX_TOKENS_PER_DOC,
            )
        except Exception as e:
            raise RerankerError(f"Cohere rerank failed: {e}", cause=e) from e

        finally:
            units = response.meta.billed_units if response and response.meta else None
            logger.info(
                "Provider call completed",
                extra={
                    "custom_dimensions": {
                        "provider": "cohere",
                        "model": COHERE_RERANK_MODEL,
                        "operation": "rerank",
                        "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
                        "outcome": "acknowledged" if response is not None else "failed",
                        "billed_units": units.model_dump(
                            mode="json",
                            include={
                                "images",
                                "input_tokens",
                                "image_tokens",
                                "output_tokens",
                                "search_units",
                                "classifications",
                            },
                        )
                        if units is not None
                        else None,
                    }
                },
            )

        return [
            RerankerResult(index=r.index, relevance_score=r.relevance_score)
            for r in response.results
        ]
