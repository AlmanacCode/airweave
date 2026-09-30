"""Typed exact-record queries; search ranking is a separate index concern."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.coverage import CaptureCoverage
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
    capture: CaptureCoverage | None = None
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


class MailThreadPage(BaseModel):
    """Observed messages only; neither a complete thread nor a provider snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    thread_id: str
    messages: tuple[SourceRecord, ...]
    next_cursor: str | None
    has_more: bool
    consistency: Literal["live"] = "live"
    order: Literal["source_created_at_asc_nulls_last_id_asc"] = (
        "source_created_at_asc_nulls_last_id_asc"
    )
    coverage: Literal["stored_messages_only"] = "stored_messages_only"


class MailThreadCursor(BaseModel):
    """Signed thread and account traversal position, independent of capture containers."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_mail_thread"] = "canonical_mail_thread"
    version: Literal[1] = 1
    organization_id: UUID
    sync_id: UUID
    thread_id: str
    after_created_at: datetime | None
    after_id: UUID
