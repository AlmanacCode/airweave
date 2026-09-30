"""Bounded original-record retrieval; unsupported controls fail validation."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.search.types import RetrievalStrategy


class OwnedSearchRequest(BaseModel):
    """Source IDs are organization-scoped; the product separately owns account authorization."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    query: str = Field(min_length=1, max_length=4000)
    sync_ids: tuple[UUID, ...] = Field(min_length=1, max_length=20)
    mode: RetrievalStrategy = RetrievalStrategy.HYBRID
    record_types: tuple[str, ...] = Field(default=(), max_length=20)
    created_after: AwareDatetime | None = None
    created_before: AwareDatetime | None = None
    updated_after: AwareDatetime | None = None
    updated_before: AwareDatetime | None = None
    limit: int = Field(default=20, ge=1, le=200)

    @model_validator(mode="after")
    def validate_scope(self):
        """Reject ambiguous requests rather than silently correcting them."""
        if not self.query.strip() or len(set(self.sync_ids)) != len(self.sync_ids):
            raise ValueError("Query must be nonblank and source IDs unique")
        if any(not item.strip() for item in self.record_types):
            raise ValueError("Record types must be nonblank")
        for after, before in (
            (self.created_after, self.created_before),
            (self.updated_after, self.updated_before),
        ):
            if after is not None and before is not None and after >= before:
                raise ValueError("Time interval must have after before before")
        return self


class OwnedSearchHit(BaseModel):
    """Canonical identity plus excerpts; no native payload or generation IDs."""

    record_id: UUID
    revision: int
    sync_id: UUID
    source_connection_id: UUID
    provider: str
    identity: RecordIdentity
    title: str
    excerpts: tuple[str, ...]
    observed_at: AwareDatetime
    source_created_at: AwareDatetime | None
    source_updated_at: AwareDatetime | None
    completeness: Literal["complete", "partial", "metadata_only"]


class OwnedSearchCoverage(BaseModel):
    """Counts are source-store state, never total matches for this query."""

    sync_id: UUID
    active_records: int
    pending_records: int


class OwnedSearchResponse(BaseModel):
    """Top results from bounded live candidate windows, without traversal promises."""

    items: tuple[OwnedSearchHit, ...]
    sources: tuple[OwnedSearchCoverage, ...]
    consistency: Literal["live"] = "live"
    order: Literal["relevance"] = "relevance"
    coverage: Literal["bounded_candidates"] = "bounded_candidates"
    candidate_limit_per_collection: int = 200
    candidate_window_full: bool
    engine_partial: bool
    excluded_candidates: int
    postfilter_excluded: int
    retrieval_incomplete: bool
