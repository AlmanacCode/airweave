"""Capture-owned checkpoint metadata, stamped only at the fenced commit boundary."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CanonicalCheckpoint(BaseModel):
    """Bind successful source state to its actual committed run and journal position."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    writer_attempt_id: UUID
    observed_change_sequence: int = Field(ge=0)
