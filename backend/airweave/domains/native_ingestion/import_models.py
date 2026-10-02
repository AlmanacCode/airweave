"""Native import intent and durable identity, without client-controlled writer fences."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from airweave.core.shared_models import SyncJobStatus
from airweave.domains.entities.canonical.coverage_models import CompletionPolicy
from airweave.domains.entities.canonical.requests import WriterFence
from airweave.domains.native_ingestion.models import NativeModel


class StartNativeImport(NativeModel):
    """Publisher attestation; bounded roots do not authorize root absence deletion."""

    snapshot_id: str = Field(min_length=1, max_length=256)
    coverage: Literal["bounded", "complete"]
    transcript_coverage: Literal["bounded", "complete"] = Field(
        default="bounded",
        description="Complete message enumeration for each selected session; sessions only",
    )

    def completion_policy(self, record_type: str) -> CompletionPolicy:
        """Complete transcripts own only children of the selected session roots."""
        if self.coverage == "complete" or (
            record_type == "message" and self.transcript_coverage == "complete"
        ):
            return "exhaustive"
        return "discovery_only"


class NativeImportSummary(NativeModel):
    """Durable capture outcome; indexing completion is separately observed."""

    outcome: Literal["completed", "cancelled"]
    coverage: Literal["bounded", "complete"]
    finished_at: AwareDatetime
    sequence: int | None = Field(default=None, ge=0)
    completed_scopes: int = Field(ge=0)
    capture_complete: bool
    indexing: Literal["not_verified"] = "not_verified"

    @model_validator(mode="after")
    def consistent_outcome(self) -> "NativeImportSummary":
        """Malformed retained outcomes must not become contradictory public claims."""
        completed = self.outcome == "completed"
        if self.capture_complete != completed or (completed and self.sequence is None):
            raise ValueError("Native terminal summary contradicts its capture outcome")
        return self


class NativeImportReceipt(NativeModel):
    """Server-only job metadata; an identical request never reactivates a writer."""

    schema_version: Literal[1] = 1
    source_id: UUID
    request_key: str = Field(min_length=1, max_length=128)
    request: StartNativeImport
    fence: WriterFence
    cycle_id: UUID
    summary: NativeImportSummary | None = None

    @model_validator(mode="after")
    def consistent_coverage(self) -> "NativeImportReceipt":
        """A terminal result cannot expand the publisher's original scope claim."""
        if self.summary is not None and self.summary.coverage != self.request.coverage:
            raise ValueError("Native terminal coverage contradicts import intent")
        return self


class NativeImportState(NativeModel):
    """Public identity and execution state; running is not evidence of full capture."""

    source_id: UUID
    import_id: UUID
    request_key: str
    request: StartNativeImport
    status: SyncJobStatus
    cycle_id: UUID
    summary: NativeImportSummary | None = None
