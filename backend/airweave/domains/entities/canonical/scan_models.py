"""Declared-scope page commits; incomplete discovery never owns absence reconciliation."""

import json
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

from airweave.domains.entities.canonical.cycle_models import CompletionPolicy
from airweave.domains.entities.canonical.models import CaptureResult, SourceRecord
from airweave.domains.entities.canonical.requests import CaptureRecord, CompletedScope, WriterFence


class ScanVersion(BaseModel):
    """The exact durable state a caller observed before external I/O."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sweep_id: UUID
    revision: int = Field(ge=1)


class ScanContinuation(BaseModel):
    """Connector-validated opaque progress; never original content or an unbounded queue."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    value: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("value")
    @classmethod
    def bounded(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Bound serialized state before it enters a transaction."""
        if len(json.dumps(value, allow_nan=False, ensure_ascii=False).encode()) > 65536:
            raise ValueError("Scan continuation exceeds 64 KiB")
        return value


class BeginScan(BaseModel):
    """Resume a cycle, or explicitly CAS-restart its sweep after invalid continuation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    scope: CompletedScope
    cycle_id: UUID
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected: ScanVersion | None = None
    restart: bool = False
    continuation: ScanContinuation = Field(default_factory=ScanContinuation)


class ScanState(BaseModel):
    """Complete means the declared policy finished, not necessarily exhaustive discovery."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    scope: CompletedScope
    cycle_id: UUID
    version: ScanVersion
    phase: Literal["collecting", "reconciling", "complete"]
    fingerprint: str
    continuation: ScanContinuation
    started_at: AwareDatetime
    completed_at: AwareDatetime | None
    parent_visibility_epoch: int | None = None
    membership_attempt_id: UUID | None = None
    completion_policy: CompletionPolicy = "exhaustive"


class CommitScanPage(BaseModel):
    """Records and next state become durable together under one writer fence."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    scope: CompletedScope
    cycle_id: UUID
    expected: ScanVersion
    records: tuple[CaptureRecord, ...] = Field(max_length=500)
    continuation: ScanContinuation
    final: bool = False


class ReconcileScan(BaseModel):
    """One bounded removal batch, only after the final collection page committed."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    scope: CompletedScope
    cycle_id: UUID
    expected: ScanVersion
    observed_at: AwareDatetime
    removal_reason: Literal["absent", "scope_removed"] = "absent"
    limit: int = Field(default=250, ge=1, le=500)


class ScanResult(BaseModel):
    """Acknowledged SQL state plus changes committed by this step."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    state: ScanState
    capture: CaptureResult


class CommitOmission(BaseModel):
    """Fresh exact read after discovery; never broad absence evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    fence: WriterFence
    state: ScanState
    expected_record: SourceRecord
    observation: CaptureRecord
