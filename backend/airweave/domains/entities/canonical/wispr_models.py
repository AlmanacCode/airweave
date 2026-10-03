"""Typed retained Wispr meeting inventory and sequence-bound continuation."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StrictBool, model_validator

from airweave.domains.entities.canonical.coverage_models import CaptureCoverage


class MeetingFilters(BaseModel):
    """Half-open native meeting start interval, independent of source creation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    after: AwareDatetime | None = None
    before: AwareDatetime | None = None

    @model_validator(mode="after")
    def valid_interval(self):
        """An invalid interval cannot masquerade as an empty retained inventory."""
        if self.after is not None and self.before is not None and self.after >= self.before:
            raise ValueError("Meeting after must precede before")
        return self


class MeetingListQuery(BaseModel):
    """Bounded newest-first retained inventory request."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    filters: MeetingFilters = Field(default_factory=MeetingFilters)
    limit: int = Field(default=5, ge=1, le=100)
    cursor: str | None = None


class MeetingPreview(BaseModel):
    """Lean native identity and metadata; notes and transcript require exact read."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    sync_id: UUID
    revision: int = Field(ge=1)
    native_id: str = Field(min_length=1)
    title: str
    started_at: AwareDatetime
    modified_at: AwareDatetime | None
    has_transcript: StrictBool | None
    observed_at: AwareDatetime


class MeetingPage(BaseModel):
    """Retained-only coverage with explicit unknown filter membership."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    meetings: tuple[MeetingPreview, ...]
    start_cursor: str
    next_cursor: str | None
    has_more: bool
    consistency: Literal["sequence_fenced"] = "sequence_fenced"
    order: Literal["meeting_started_at_desc_id_desc"] = "meeting_started_at_desc_id_desc"
    date_basis: Literal["wispr_native_start"] = "wispr_native_start"
    coverage: Literal["stored_meetings_only"] = "stored_meetings_only"
    capture: CaptureCoverage | None
    metadata_missing: int = Field(ge=0)


class MeetingCursor(BaseModel):
    """Signed account/filter/capture-sequence keyset position."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_wispr_meetings"] = "canonical_wispr_meetings"
    version: Literal[1] = 1
    organization_id: UUID
    sync_id: UUID
    filters: MeetingFilters
    sequence: int = Field(ge=0)
    after_started_at: AwareDatetime | None = None
    after_id: UUID | None = None

    @model_validator(mode="after")
    def complete_position(self):
        """Initial cursors omit both keyset coordinates; later cursors require both."""
        if (self.after_started_at is None) != (self.after_id is None):
            raise ValueError("Meeting cursor position must be a complete pair")
        return self
