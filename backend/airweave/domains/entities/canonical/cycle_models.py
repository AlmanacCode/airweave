"""Typed engine-owned cycle state inside the existing SyncCursor."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from airweave.domains.entities.canonical.requests import WriterFence

CYCLE_KEY = "canonical_cycle"


class CycleConfiguration(BaseModel):
    """Whole-scope topology; child containers are visible root native IDs."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    root_record_type: str = Field(min_length=1, max_length=200)
    child_record_types: tuple[str, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def distinct_types(self) -> "CycleConfiguration":
        """Keep scope discovery bounded and unambiguous."""
        if len(set(self.child_record_types)) != len(self.child_record_types):
            raise ValueError("Child record types must be unique")
        if any(not item or len(item) > 200 for item in self.child_record_types):
            raise ValueError("Invalid child record type")
        return self


class CycleVersion(BaseModel):
    """Exact checkpoint version observed by the caller."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    cycle_id: UUID
    revision: int = Field(ge=1)


class CaptureCycle(BaseModel):
    """Completion stays durable until an explicit next-cycle CAS."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    version: CycleVersion
    configuration: CycleConfiguration
    phase: Literal["active", "complete"] = "active"
    root_writer_attempt_id: UUID | None = None
    completed_job_id: UUID | None = None


class BeginCycle(BaseModel):
    """Resume an active cycle or CAS-advance its completed predecessor."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    configuration: CycleConfiguration
    expected: CycleVersion | None = None


class CompleteCycle(BaseModel):
    """Publish only an unchanged active cycle with all required scans complete."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    expected: CycleVersion


class RestartCycle(BaseModel):
    """Explicitly abandon the exact active cycle without certifying its partial capture."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    expected: CycleVersion
    configuration: CycleConfiguration


class CycleRoot(BaseModel):
    """Bounded operational scope discovery, without copying provider payloads."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    native_id: str
