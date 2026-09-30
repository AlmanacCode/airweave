"""Immutable scope plans and successful evidence survive page-continuation replacement."""

import json
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

from airweave.domains.entities.canonical.cycle_models import CompletionPolicy, ProviderCheckpoint


class ScopePlan(BaseModel):
    """Effective provider parameters, not original content or a second page cursor."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    mode: Literal["full", "changes"] = "full"
    request_context: dict[str, JsonValue] = Field(default_factory=dict)
    starting_checkpoint: ProviderCheckpoint | None = None

    @field_validator("request_context")
    @classmethod
    def bounded(cls, value):
        """Bound immutable source parameters independently of provider progress."""
        if len(json.dumps(value, allow_nan=False).encode()) > 65536:
            raise ValueError("Scope request context exceeds64KiB")
        return value


class ScopeEvidence(BaseModel):
    """Only SQL completion can issue evidence for this exact owner and request."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    sweep_id: UUID
    completed_at: AwareDatetime
    parent_visibility_epoch: int | None
    request_context: dict[str, JsonValue]
    policy: CompletionPolicy
    checkpoint: ProviderCheckpoint | None = None

    def matches(self, plan: ScopePlan, epoch: int | None) -> bool:
        """Revival or changed native request parameters cannot reuse old authority."""
        return (
            self.parent_visibility_epoch == epoch and self.request_context == plan.request_context
        )


class ScopeExecution(BaseModel):
    """One current plan, with independent previously completed publication and baseline."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    plan: ScopePlan
    last_full: ScopeEvidence | None = None
    published: ScopeEvidence | None = None
