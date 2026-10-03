"""Typed exact-record queries; search ranking is a separate index concern."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

from airweave.domains.entities.canonical.coverage_models import CaptureCoverage
from airweave.domains.entities.canonical.models import IndexedRecordRead as IndexedRecordRead
from airweave.domains.entities.canonical.models import ObservedChange, SourceRecord
from airweave.domains.entities.canonical.models import RecordPage as RecordPage
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.native_ingestion.models import NativeVersion
from airweave.platform.sources.records.sheets_manifest import GridBounds, GridGap
from airweave.platform.sources.records.sheets_models import SpreadsheetCell
from airweave.platform.sources.records.workspace_manifest import WorkspaceManifestV1


class DocumentRead(BaseModel):
    """One authorized stored revision's native document and representation coverage."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    identity: RecordIdentity
    revision: int = Field(ge=1)
    observed_at: AwareDatetime
    completeness: Literal["complete", "partial", "metadata_only"]
    manifest: WorkspaceManifestV1
    document: dict[str, JsonValue]


class SpreadsheetRead(BaseModel):
    """Stored workbook metadata and bounded native cells; unknown ranges stay unknown."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    identity: RecordIdentity
    revision: int = Field(ge=1)
    observed_at: AwareDatetime
    completeness: Literal["complete", "partial", "metadata_only"]
    spreadsheet: dict[str, JsonValue]
    grid_status: Literal["complete", "partial"]
    captured: tuple[GridBounds, ...]
    missing: tuple[GridGap, ...]
    requested: GridBounds | None = None
    cells: tuple[SpreadsheetCell, ...] = ()
    comments: Literal["omitted"] = "omitted"
    embedded_media: Literal["not_retained"] = "not_retained"
    calculated_values: Literal["observed_at_capture"] = "observed_at_capture"


class RecordFilters(BaseModel):
    """Supported exact filters, preserved in every continuation token."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_type: str | None = None
    container_id: str | None = None
    parent_record_id: UUID | None = None
    state: Literal["active", "deleted", "all"] = "active"


class RecordListQuery(BaseModel):
    """Stable-ID ordered live traversal, explicitly not a frozen snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    filters: RecordFilters = Field(default_factory=RecordFilters)
    limit: int = Field(default=100, ge=1, le=500)
    cursor: str | None = None


class RecordChangePage(BaseModel):
    """A bounded journal window plus a cursor for future polling."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    changes: tuple[ObservedChange, ...]
    next_cursor: str
    has_more: bool
    high_watermark: int


class RecordCursor(BaseModel):
    """Versioned signed cursor payload; no provider secrets or record content."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_records"] = "canonical_records"
    version: Literal[1] = 1
    mode: Literal["list", "changes"]
    organization_id: UUID
    sync_id: UUID
    filters: RecordFilters | None = None
    after_id: UUID | None = None
    after_sequence: int = Field(default=0, ge=0)
    high_watermark: int | None = Field(default=None, ge=0)


class MailThreadPage(BaseModel):
    """Observed messages only; neither a complete thread nor a provider snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    thread_id: str
    messages: tuple[SourceRecord, ...]
    next_cursor: str | None
    has_more: bool
    consistency: Literal["live"] = "live"
    order: Literal["source_created_at_asc_nulls_last_id_asc"] = (
        "source_created_at_asc_nulls_last_id_asc"
    )
    coverage: Literal["stored_messages_only"] = "stored_messages_only"
    capture: CaptureCoverage | None = None


class MailThreadCursor(BaseModel):
    """Signed thread and account traversal position, independent of capture containers."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_mail_thread"] = "canonical_mail_thread"
    version: Literal[1] = 1
    organization_id: UUID
    sync_id: UUID
    thread_id: str
    after_created_at: datetime | None
    after_id: UUID


class HistoricalRecordRead(BaseModel):
    """Historical capture authorized by current access, not its original ACL."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record: SourceRecord
    current_revision: int = Field(ge=1)
    authority: Literal["current_source_record_access"] = "current_source_record_access"


class RecordBrowseFilters(BaseModel):
    """Exact retained inventory and half-open source clocks, bound to continuation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sync_ids: tuple[UUID, ...] = Field(min_length=1, max_length=20)
    record_types: tuple[Annotated[str, Field(min_length=1, max_length=128)], ...] = Field(
        default=(), max_length=20
    )
    native_types: tuple[Annotated[str, Field(min_length=1, max_length=128)], ...] = Field(
        default=(), max_length=20
    )
    basis: Literal["source_created", "source_updated"]
    order: Literal["asc", "desc"] = "desc"
    created_after: AwareDatetime | None = None
    created_before: AwareDatetime | None = None
    updated_after: AwareDatetime | None = None
    updated_before: AwareDatetime | None = None

    @field_validator("sync_ids", "record_types", "native_types")
    @classmethod
    def ordered_set(cls, values):
        """Canonicalize set order while retaining duplicates for explicit rejection."""
        return tuple(sorted(values))

    @model_validator(mode="after")
    def valid_scope(self):
        """Reject ambiguous inventory and empty intervals rather than widening them."""
        for values in (self.sync_ids, self.record_types, self.native_types):
            if len(values) != len(set(values)):
                raise ValueError("Browse inventory and types must be unique")
        if any(not value.strip() for value in (*self.record_types, *self.native_types)):
            raise ValueError("Browse types must be nonblank")
        for after, before in (
            (self.created_after, self.created_before),
            (self.updated_after, self.updated_before),
        ):
            if after is not None and before is not None and after >= before:
                raise ValueError("Browse after must precede before")
        return self


class RecordBrowseQuery(BaseModel):
    """SQL traversal, independent of indexed publication and relevance ranking."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    filters: RecordBrowseFilters
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None = Field(default=None, min_length=1, max_length=16384)


class RecordBrowseSource(BaseModel):
    """Current authenticated routing, revalidated after the metadata query."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sync_id: UUID
    source_connection_id: UUID
    provider: str


class RecordBrowseItem(RecordBrowseSource):
    """An original's bounded metadata; native versions still need author authorization."""

    record_id: UUID
    revision: int = Field(ge=1)
    identity: RecordIdentity
    parent: RecordIdentity | None = None
    native_version: NativeVersion | None = None
    native_type: str | None = None
    email_thread_id: str | None = Field(default=None, max_length=512, pattern=r"^[A-Za-z0-9_-]+$")
    title: str = Field(max_length=512)
    source_created_at: AwareDatetime | None
    source_updated_at: AwareDatetime | None
    observed_at: AwareDatetime
    completeness: Literal["complete", "partial", "metadata_only"]


class RecordBrowseCoverage(BaseModel):
    """Existing source capture facts, never preparation eligibility or query totals."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sync_id: UUID
    capture: CaptureCoverage | None


class RecordBrowsePage(BaseModel):
    """Live retained traversal; missing clocks excluded, never a provider snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    items: tuple[RecordBrowseItem, ...]
    next_cursor: str | None
    has_more: bool
    basis: Literal["source_created", "source_updated"]
    order: Literal["asc", "desc"]
    consistency: Literal["live"] = "live"
    coverage: Literal["retained_traversal"] = "retained_traversal"
    sources: tuple[RecordBrowseCoverage, ...]
    missing_clock: Literal["excluded"] = "excluded"


class RecordBrowseCursor(BaseModel):
    """Signed source inventory and exact chronological keyset, not a snapshot token."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_browse"] = "canonical_browse"
    version: Literal[1] = 1
    organization_id: UUID
    filters: RecordBrowseFilters
    sources: tuple[RecordBrowseSource, ...]
    limit: int = Field(ge=1, le=200)
    after_time: AwareDatetime
    after_id: UUID
