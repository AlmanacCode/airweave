"""Cohere text embeddings with explicit retrieval intent and bounded requests."""

import math
import time

import cohere
import httpx
from cohere.core.api_error import ApiError
from cohere.types import EmbedByTypeResponse

from airweave.core.logging import logger
from airweave.domains.embedders.exceptions import (
    EmbedderAuthError,
    EmbedderConfigError,
    EmbedderConnectionError,
    EmbedderDimensionError,
    EmbedderInputError,
    EmbedderProviderError,
    EmbedderRateLimitError,
    EmbedderResponseError,
    EmbedderTimeoutError,
)
from airweave.domains.embedders.types import DenseEmbedding, EmbeddingPurpose


class CohereDenseEmbedder:
    """Use the existing SDK; callers own retries and model migration policy."""

    def __init__(self, *, api_key: str, model: str, dimensions: int) -> None:
        """Fail configuration before creating network resources."""
        allowed = {
            "embed-v4.0": {256, 512, 1024, 1536},
            "embed-v5.0-pro": {256, 512, 768, 1024, 1536, 2048},
            "embed-v5.0-fast": {256, 512, 768, 1024, 1536, 2048},
        }
        if not api_key or model not in allowed or dimensions not in allowed[model]:
            raise EmbedderConfigError("Cohere requires an API key, supported model and dimension")
        self._model = model
        self._dimensions = dimensions
        self._http = httpx.AsyncClient(timeout=60)
        self._client = cohere.AsyncClientV2(api_key=api_key, httpx_client=self._http)

    @property
    def model_name(self) -> str:
        """Persist this identity alongside vector dimensions."""
        return self._model

    @property
    def dimensions(self) -> int:
        """Configured output width."""
        return self._dimensions

    async def embed(self, text: str) -> DenseEmbedding:
        """Embed a document; query callers must use the explicit batch purpose."""
        return (await self.embed_many([text]))[0]

    async def embed_many(
        self, texts: list[str], *, purpose: EmbeddingPurpose = "document"
    ) -> list[DenseEmbedding]:
        """Keep order, reject silent truncation, and avoid unbounded request fan-out."""
        if purpose not in ("document", "query") or any(not text.strip() for text in texts):
            raise EmbedderInputError("Cohere requires nonblank text and a valid embedding purpose")
        results = []
        for offset in range(0, len(texts), 96):
            batch = texts[offset : offset + 96]
            response = await self._request(batch, purpose)
            vectors = response.embeddings.float_
            if vectors is None or len(vectors) != len(batch):
                raise EmbedderResponseError("Cohere returned an unexpected vector count")
            for vector in vectors:
                if len(vector) != self._dimensions:
                    raise EmbedderDimensionError(expected=self._dimensions, actual=len(vector))
                if not all(math.isfinite(value) for value in vector):
                    raise EmbedderResponseError("Cohere returned a non-finite vector")
                results.append(DenseEmbedding(vector=vector))
        return results

    async def _request(self, batch: list[str], purpose: EmbeddingPurpose) -> EmbedByTypeResponse:
        """Translate provider errors without exposing request text or credentials.

        Logs describe SDK acknowledgments, not durable invoices or full costs.
        SDK-internal retries and calls with no response have unknown consumption.
        """
        started = time.monotonic()
        response = None
        try:
            response = await self._client.embed(
                model=self._model,
                texts=batch,
                input_type="search_query" if purpose == "query" else "search_document",
                output_dimension=self._dimensions,
                embedding_types=["float"],
                truncate="NONE",
                request_options={"max_retries": 0},
            )
            return response
        except ApiError as exc:
            if exc.status_code in (401, 403):
                raise EmbedderAuthError("Cohere authentication failed", provider="cohere") from exc
            if exc.status_code == 429:
                raise EmbedderRateLimitError("Cohere rate limited", provider="cohere") from exc
            raise EmbedderProviderError(
                "Cohere embedding request failed",
                provider="cohere",
                retryable=exc.status_code is not None and exc.status_code >= 500,
            ) from exc
        except httpx.TimeoutException as exc:
            raise EmbedderTimeoutError(provider="cohere") from exc
        except httpx.RequestError as exc:
            raise EmbedderConnectionError(provider="cohere") from exc

        finally:
            units = response.meta.billed_units if response and response.meta else None
            logger.info(
                "Provider call completed",
                extra={
                    "custom_dimensions": {
                        "provider": "cohere",
                        "model": self._model,
                        "operation": "embed",
                        "purpose": purpose,
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

    async def close(self) -> None:
        """Release the transport owned by this adapter."""
        await self._http.aclose()
