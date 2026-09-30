"""Typed exact-record queries; search ranking is a separate index concern."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.models import ObservedChange, SourceRecord


class RecordFilters(BaseModel):
    """Supported exact filters, preserved in every continuation token."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    record_type: str | None = None
    container_id: str | None = None
    state: Literal["active", "deleted", "all"] = "active"


class RecordListQuery(BaseModel):
    """Stable-ID ordered live traversal, explicitly not a frozen snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    filters: RecordFilters = Field(default_factory=RecordFilters)
    limit: int = Field(default=100, ge=1, le=500)
    cursor: str | None = None


class RecordPage(BaseModel):
    """Committed records with truthful traversal semantics."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    records: tuple[SourceRecord, ...]
    next_cursor: str | None
    has_more: bool
    consistency: Literal["live"] = "live"
    order: Literal["id_asc"] = "id_asc"


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
