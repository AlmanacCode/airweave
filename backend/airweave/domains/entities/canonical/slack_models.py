"""Observed Slack thread messages and account/sequence-bound continuation."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue

from airweave.domains.entities.canonical.coverage_models import CaptureCoverage

SlackTimestamp = Annotated[str, Field(pattern=r"^[0-9]+\.[0-9]+$", max_length=64)]
SlackChannel = Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9]+$", max_length=128)]


class SlackThreadQuery(BaseModel):
    """Native channel plus root timestamp, never a channel-as-thread lookup."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    channel: SlackChannel
    thread_ts: SlackTimestamp
    limit: int = Field(default=15, ge=1, le=15)
    cursor: str | None = Field(default=None, max_length=16384)


class SlackFileReference(BaseModel):
    """An available captured child; bytes use the existing exact reader/download."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    revision: int = Field(ge=1)
    native_id: str


class SlackThreadMessage(BaseModel):
    """Native rich message fields with exact retained identity and revision."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: UUID
    revision: int = Field(ge=1)
    ts: SlackTimestamp
    thread_ts: SlackTimestamp | None
    text: str | None
    user: str | None
    bot_id: str | None
    subtype: str | None
    blocks: tuple[dict[str, JsonValue], ...] = ()
    attachments: tuple[dict[str, JsonValue], ...] = ()
    files: tuple[dict[str, JsonValue], ...] = ()
    captured_files: tuple[SlackFileReference, ...] = ()
    observed_at: AwareDatetime


class SlackThreadPage(BaseModel):
    """Exhaustion covers visible stored messages, never complete provider history."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sync_id: UUID
    channel: SlackChannel
    thread_ts: SlackTimestamp
    messages: tuple[SlackThreadMessage, ...]
    root_present: bool
    next_cursor: str | None
    has_more: bool
    consistency: Literal["sequence_fenced"] = "sequence_fenced"
    order: Literal["native_ts_numeric_asc_id_asc"] = "native_ts_numeric_asc_id_asc"
    coverage: Literal["stored_messages_only"] = "stored_messages_only"
    capture: CaptureCoverage | None
    metadata_missing: int = Field(ge=0)


class SlackThreadCursor(BaseModel):
    """Signed read scope and exact numeric position; original string is retained."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    purpose: Literal["canonical_slack_thread"] = "canonical_slack_thread"
    version: Literal[1] = 1
    organization_id: UUID
    sync_id: UUID
    channel: SlackChannel
    thread_ts: SlackTimestamp
    limit: int = Field(ge=1, le=15)
    sequence: int = Field(ge=0)
    after_ts: SlackTimestamp
    after_id: UUID
