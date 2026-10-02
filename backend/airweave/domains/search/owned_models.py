"""Bounded original-record retrieval; unsupported controls fail validation."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from airweave.domains.entities.canonical.content_models import MatchedPart
from airweave.domains.entities.canonical.coverage_models import CaptureCoverage
from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.native_ingestion.models import NativeVersion
from airweave.domains.search.retrieval_strategy import RetrievalStrategy


class OwnedSearchRequest(BaseModel):
    """Source IDs are organization-scoped; the product separately owns account authorization."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    query: str = Field(min_length=1, max_length=4000)
    sync_ids: tuple[UUID, ...] = Field(min_length=1, max_length=20)
    mode: RetrievalStrategy = RetrievalStrategy.HYBRID
    record_types: tuple[str, ...] = Field(default=(), max_length=20)
    native_types: tuple[
        Literal[
            "person",
            "organisation",
            "place",
            "event",
            "creative_work",
            "topic",
            "page",
            "task",
            "project",
            "session",
            "message",
        ],
        ...,
    ] = Field(default=(), max_length=11)
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


class OwnedSearchMatch(BaseModel):
    """Canonical identity plus excerpts; no native payload or generation IDs."""

    record_id: UUID
    revision: int
    sync_id: UUID
    source_connection_id: UUID
    provider: str
    identity: RecordIdentity
    native_version: NativeVersion | None = None
    title: str
    excerpts: tuple[str, ...]
    matched_part: MatchedPart | None = None
    email_thread_id: str | None = Field(default=None, max_length=512, pattern=r"^[A-Za-z0-9_-]+$")
    observed_at: AwareDatetime
    source_created_at: AwareDatetime | None
    source_updated_at: AwareDatetime | None
    completeness: Literal["complete", "partial", "metadata_only"]
    extraction: ExtractionCoverage | None = None


class OwnedSearchGroup(BaseModel):
    """Observed eligible shortlist members, never a complete conversation count."""

    kind: Literal["session", "email_thread"]
    native_id: str
    matched_records: int = Field(ge=1)
    additional_matches: tuple[OwnedSearchMatch, ...] = Field(default=(), max_length=3)


class OwnedSearchHit(OwnedSearchMatch):
    """An exact representative match, optionally with other conversation matches."""

    group: OwnedSearchGroup | None = None


class OwnedSearchCoverage(BaseModel):
    """Counts are source-store state, never total matches for this query."""

    sync_id: UUID
    active_records: int
    capture: CaptureCoverage | None = None
    pending_records: int
    partially_indexed_records: int = 0
    extraction_unavailable_records: int = 0
    extraction_unknown_records: int = 0


class OwnedRanking(BaseModel):
    """Ranking applies only to this request's bounded canonical-source candidates."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    method: Literal["shared_rerank", "retrieval_rank"] = "retrieval_rank"
    fallback_reason: (
        Literal["unconfigured", "timeout", "provider_error", "invalid_output"] | None
    ) = "unconfigured"
    candidates_considered: int = Field(default=0, ge=0)
    candidates_reranked: int = Field(default=0, ge=0)
    shortlisted_candidates: int = Field(default=0, ge=0, le=200)
    shortlist_limit: Literal[200] = 200
    document_token_limit: Literal[2048] = 2048
    token_count_basis: Literal["local_tokenizer"] = "local_tokenizer"
    timeout_seconds: Literal[10] = 10
    input_truncated_documents: int = Field(default=0, ge=0)
    shortlist_truncated: bool = False


class OwnedSearchResponse(BaseModel):
    """Top results from bounded live candidate windows, without traversal promises."""

    items: tuple[OwnedSearchHit, ...]
    ranking: OwnedRanking = Field(default_factory=OwnedRanking)
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


class OwnedCandidate(BaseModel):
    """Internal candidate for current product authorization before model disclosure."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    hit: OwnedSearchHit
    projection: ProjectionLocator
    retrieval_score: float = Field(gt=0, allow_inf_nan=False)
    text: str = Field(max_length=32000)
    text_truncated: bool

    @model_validator(mode="after")
    def exact_publication(self):
        """Capture identity must refer to the same exact published record."""
        if (
            self.hit.record_id != self.projection.record_id
            or self.hit.revision != self.projection.revision
        ):
            raise ValueError("Candidate identity differs from projection")
        return self


class OwnedCandidatesResponse(BaseModel):
    """Ungrouped retrieval shortlist; native authority still belongs to Almanac."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    candidates: tuple[OwnedCandidate, ...] = Field(max_length=200)
    sources: tuple[OwnedSearchCoverage, ...]
    candidate_window_full: bool
    engine_partial: bool
    excluded_candidates: int = Field(ge=0)
    postfilter_excluded: int = Field(ge=0)
    shortlist_truncated: bool
    authority: Literal["canonical_snapshot"] = "canonical_snapshot"
    order: Literal["retrieval_rank"] = "retrieval_rank"


class OwnedRankRequest(BaseModel):
    """Backend-approved candidates after current Almanac authority validation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    query: str = Field(min_length=1, max_length=4000)
    sync_ids: tuple[UUID, ...] = Field(min_length=1, max_length=20)
    candidates: tuple[OwnedCandidate, ...] = Field(max_length=200)

    @model_validator(mode="after")
    def distinct_scope(self):
        """One candidate per exact record and one declaration per source."""
        if not self.query.strip() or len(set(self.sync_ids)) != len(self.sync_ids):
            raise ValueError("Query must be nonblank and source IDs unique")
        ids = [candidate.hit.record_id for candidate in self.candidates]
        if len(set(ids)) != len(ids):
            raise ValueError("Ranking candidates must have distinct record IDs")
        return self


class OwnedRankResponse(BaseModel):
    """Order only: the caller retains candidate cards and owns product grouping."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_ids: tuple[UUID, ...] = Field(max_length=200)
    ranking: OwnedRanking
    excluded_candidates: int = Field(ge=0)
