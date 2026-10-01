"""Public capture coverage facts, independent of SQL and runtime settings."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

CompletionPolicy = Literal["exhaustive", "discovery_only", "discovery_with_validation"]


class FullCaptureCoverage(BaseModel):
    """Public coverage evidence excludes private provider continuation values."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    completed_at: AwareDatetime
    discovery: Literal["incomplete", "scope_enumeration_complete"]


class CaptureScopeSummary(BaseModel):
    """Counts describe eligible exact scopes, never original/indexed record totals."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    eligible: int = Field(ge=0, strict=True)
    completed_full: int = Field(ge=0, strict=True)
    completed_changes: int = Field(ge=0, strict=True)
    unfinished: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def partition(self):
        """Every eligible scope belongs to exactly one progress category."""
        if self.eligible != self.completed_full + self.completed_changes + self.unfinished:
            raise ValueError("Scope progress must partition eligible scopes")
        return self


class CaptureCoverage(BaseModel):
    """No cycle means unknown; completion only certifies the declared scope policies."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    phase: Literal["active", "complete"]
    mode: Literal["full", "changes", "mixed"] = "full"
    scope_summary: CaptureScopeSummary | None = None
    last_full_capture: FullCaptureCoverage | None = None
    provider_checkpoint_promoted_at: AwareDatetime | None = None
    policies: dict[str, CompletionPolicy]
    discovery: Literal["incomplete", "pending", "scope_enumeration_complete"]
