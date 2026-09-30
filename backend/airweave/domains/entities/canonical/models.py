"""Canonical record results. Journal snapshots are historical, reads are current."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, JsonValue

from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity


class SourceRecord(BaseModel):
    """Durable source state, independent of search projection success."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    sync_id: UUID
    identity: RecordIdentity
    parent: RecordIdentity | None = None
    revision: int
    payload: dict[str, JsonValue]
    content_access: Literal["available", "unavailable"] = "available"
    payload_schema_version: int
    capture_hash: str
    content_hash: str | None
    completeness: str
    observed_at: datetime
    source_created_at: datetime | None
    source_updated_at: datetime | None
    deleted_at: datetime | None
    removal_reason: str | None
    blobs: tuple[BlobReference, ...]
    indexed_revision: int | None
    indexed_pipeline_version: int | None


class ObservedChange(BaseModel):
    """Snapshot at commit time; never a claim to every upstream transient event."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sequence: int
    kind: Literal["upsert", "delete"]
    record: SourceRecord


class CaptureResult(BaseModel):
    """Journal events created by this commit and its final per-sync position."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    changes: tuple[ObservedChange, ...]
    sequence: int
    unchanged: int


class ReconcileResult(BaseModel):
    """Repeat while more remains; only then may the source commit its checkpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    capture: CaptureResult
    has_more: bool


class ChangePage(BaseModel):
    """All committed changes after a position, capped at a fixed high watermark."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    changes: tuple[ObservedChange, ...]
    next_sequence: int
    high_watermark: int
    has_more: bool
