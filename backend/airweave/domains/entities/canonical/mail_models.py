"""Retained Gmail inventory and literal text traversal; no provider search syntax."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from airweave.domains.entities.canonical.coverage_models import CaptureCoverage
from airweave.domains.entities.canonical.mail_facts_v1 import MailAddress


class MailFilters(BaseModel):
    """Normalized facets bound to each traversal cursor."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    query: str = Field(default="", max_length=4096)
    from_addresses: tuple[str, ...] = Field(default=(), max_length=100)
    to_addresses: tuple[str, ...] = Field(default=(), max_length=100)
    after: AwareDatetime | None = None
    before: AwareDatetime | None = None
    folder: Literal["inbox", "sent", "trash", "spam", "drafts"] | None = None
    unread: bool | None = None

    @field_validator("query")
    @classmethod
    def literal_text(cls, value: str) -> str:
        """Casefold literal text without interpreting provider search operators."""
        return value.strip().casefold()

    @field_validator("from_addresses", "to_addresses")
    @classmethod
    def mailboxes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        """Use exact casefolded mailbox spelling; preserve dots and plus suffixes."""
        normalized = tuple(sorted({value.strip().casefold() for value in values}))
        if any(not value or "@" not in value for value in normalized):
            raise ValueError("Address filters require exact mailboxes")
        return normalized

    @model_validator(mode="after")
    def half_open_range(self):
        """Require a nonempty half-open interval when both bounds are provided."""
        if self.after is not None and self.before is not None and self.after >= self.before:
            raise ValueError("Mail after must precede before")
        return self


class MailMessageQuery(BaseModel):
    """Bounded metadata page, without ranking or provider syntax."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    filters: MailFilters = Field(default_factory=MailFilters)
    limit: int = Field(default=100, ge=1, le=100)
    cursor: str | None = None


class MailMatch(BaseModel):
    """Discovery preview with source identity and revision, without bodies."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    sync_id: UUID
    revision: int = Field(ge=1)
    native_id: str
    thread_id: str
    subject: str
    sender: tuple[MailAddress, ...]
    to: tuple[MailAddress, ...]
    sent_at: AwareDatetime
    labels: tuple[str, ...]
    snippet: str | None
    observed_at: AwareDatetime


class MailIndexing(BaseModel):
    """Facet-wide prepared text counts and source-wide unknown metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    metadata_missing: int = Field(ge=0)
    text_ready: int = Field(ge=0)
    text_partial: int = Field(ge=0)
    text_unavailable: int = Field(ge=0)


class MailMessagePage(BaseModel):
    """Retained-only inventory evidence, never a complete mailbox claim."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    messages: tuple[MailMatch, ...]
    next_cursor: str | None
    has_more: bool
    consistency: Literal["sequence_fenced"] = "sequence_fenced"
    order: Literal["source_created_at_asc_id_asc"] = "source_created_at_asc_id_asc"
    date_basis: Literal["gmail_internal_date"] = "gmail_internal_date"
    coverage: Literal["stored_messages_only"] = "stored_messages_only"
    capture: CaptureCoverage | None
    indexing: MailIndexing


class MailMessageCursor(BaseModel):
    """Signed canonical and prepared-text clock plus keyset position."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["retained_gmail_messages"] = "retained_gmail_messages"
    version: Literal[1] = 1
    organization_id: UUID
    sync_id: UUID
    filters: MailFilters
    sequence: int = Field(ge=0)
    text_sequence: int | None = Field(default=None, ge=0)
    pipeline_version: int = Field(ge=1)
    after_created_at: AwareDatetime
    after_id: UUID
