"""Embedding types for the search module.

QueryEmbeddings.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from airweave.domains.embedders.types import DenseEmbedding, SparseEmbedding
from airweave.domains.search.types.plan import RetrievalStrategy, SearchPlan


class QueryEmbeddings(BaseModel):
    """Query embeddings schema."""

    dense_embeddings: Optional[list[DenseEmbedding]] = Field(
        default=None, description="Dense embeddings for all query variations."
    )
    sparse_embedding: Optional[SparseEmbedding] = Field(
        default=None, description="Sparse embedding for the primary query only."
    )


class PreparedQueryEmbeddings(BaseModel):
    """Internal request-local work, valid only for its creating executor and query."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    primary: str
    variations: tuple[str, ...]
    strategy: RetrievalStrategy
    embeddings: QueryEmbeddings
    _owner: object | None = PrivateAttr(default=None)

    def require_match(self, plan: SearchPlan, owner: object) -> QueryEmbeddings:
        """Never reuse another model's embeddings or silently change a query."""
        if (
            self._owner is not owner
            or self.primary != plan.query.primary
            or self.variations != tuple(plan.query.variations)
            or self.strategy != plan.retrieval_strategy
        ):
            raise ValueError("Prepared query does not belong to this executor and search")
        return self.embeddings
