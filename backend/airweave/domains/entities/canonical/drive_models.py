"""Drive metadata from captured revisions, independent of preparation success."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StrictBool

from airweave.domains.entities.canonical.coverage_models import CaptureCoverage
from airweave.domains.entities.canonical.extraction_models import ExtractionCoverage

NativeID = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=256)]


class DriveFilters(BaseModel):
    """Literal saved metadata predicates, applied before LIMIT."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    folder: NativeID | None = None
    drive: NativeID | None = None
    name: str | None = Field(default=None, min_length=1, max_length=512)
    mime_type: str | None = Field(default=None, pattern=r"^[\w.+-]+/[\w.+-]+$")
    sort: Literal["name", "updated"] = "name"


class DriveListQuery(BaseModel):
    """One bounded page with a complete immutable filter set."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    filters: DriveFilters = Field(default_factory=DriveFilters)
    limit: int = Field(default=50, ge=1, le=100)
    cursor: str | None = Field(default=None, min_length=1, max_length=16384)


class DriveMetadata(BaseModel):
    """One exact current canonical file revision's native metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    sync_id: UUID
    revision: int = Field(ge=1)
    native_id: NativeID
    name: str
    mime_type: str
    description: str | None
    size_bytes: int | None = Field(ge=0)
    parents: tuple[str, ...] | None
    drive_id: str | None
    trashed: StrictBool
    source_created_at: AwareDatetime | None
    source_updated_at: AwareDatetime | None
    observed_at: AwareDatetime
    completeness: Literal["complete", "partial", "metadata_only"]
    extraction: ExtractionCoverage | None


class DriveMetadataPage(BaseModel):
    """Current retained inventory and explicit unknown metadata membership."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    files: tuple[DriveMetadata, ...]
    next_cursor: str | None
    has_more: bool
    capture: CaptureCoverage | None
    metadata_missing: int = Field(ge=0)
    consistency: Literal["sequence_fenced"] = "sequence_fenced"
    order: Literal["name_asc_id_asc", "updated_desc_nulls_last_id_asc"]
    coverage: Literal["stored_files_only"] = "stored_files_only"


class DriveMetadataRead(BaseModel):
    """Exact file revision plus capture scope evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    file: DriveMetadata
    capture: CaptureCoverage | None


class DriveCursor(BaseModel):
    """Signed source, complete query, sequence and deterministic position."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_drive_files"] = "canonical_drive_files"
    organization_id: UUID
    sync_id: UUID
    filters: DriveFilters
    sequence: int = Field(ge=0)
    after_name: str
    after_updated_at: AwareDatetime | None
    after_id: UUID
