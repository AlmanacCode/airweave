"""Body-free account progress, derived from current retained/publication facts."""

from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from airweave.domains.entities.canonical.coverage_models import CaptureCoverage


class PreparationStatus(BaseModel):
    """Candidates include unprepared policy decisions; this denominator can change."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    candidate_records: int = Field(ge=0)
    current_records: int = Field(ge=0)
    pending_records: int = Field(ge=0)
    failed_records: int = Field(ge=0)
    excluded_records: int = Field(ge=0)

    @model_validator(mode="after")
    def partition(self):
        """A failed publication remains pending, not a successfully extracted part."""
        if self.current_records + self.pending_records != self.candidate_records:
            raise ValueError("Preparation counts do not partition candidates")
        if self.failed_records > self.pending_records:
            raise ValueError("Failed records must remain pending")
        return self


class ExtractionStatus(BaseModel):
    """Content gaps among current candidate publications, independently of metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    partial_records: int = Field(ge=0)
    unavailable_records: int = Field(ge=0)
    unknown_records: int = Field(ge=0)


class SourceStatus(BaseModel):
    """SQL-only snapshot; unfinished capture does not mean a worker is running."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sync_id: UUID
    retained_records: int = Field(ge=0)
    last_observed_at: AwareDatetime | None
    capture: CaptureCoverage | None
    preparation: PreparationStatus
    extraction: ExtractionStatus

    @model_validator(mode="after")
    def partition(self):
        """Excluded records are saved but are not counted as searchable progress."""
        if self.retained_records != (
            self.preparation.candidate_records + self.preparation.excluded_records
        ):
            raise ValueError("Preparation must account for all retained records")
        if (
            sum(
                (
                    self.extraction.partial_records,
                    self.extraction.unavailable_records,
                    self.extraction.unknown_records,
                )
            )
            > self.preparation.current_records
        ):
            raise ValueError("Extraction gaps must belong to current publications")
        return self
