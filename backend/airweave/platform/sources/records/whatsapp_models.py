"""Lossless shaped subsets of official Unipile v2 WhatsApp wire responses.

Schema evidence: unipile/unipile-node f839654cf7c8856635b9dae6032d00a91489560b.
Unified optional fields do not establish WhatsApp availability. Unknown native
fields are retained, and exclude_unset preserves absence versus explicit null.
"""

from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class WhatsAppNativeModel(BaseModel):
    """Retain the original JSON shape while typing fields used by acquisition."""

    model_config = ConfigDict(extra="allow", strict=True)

    def original(self) -> dict[str, JsonValue]:
        """Return only supplied fields, including unrecognized provider additions."""
        return self.model_dump(mode="json", exclude_unset=True)


class WhatsAppUser(WhatsAppNativeModel):
    """Native user ID; an LID must not be replaced by a phone JID."""

    id: str = Field(min_length=1)
    object: Literal["User", "UserProfile"]
    display_name: str | None = None
    public_identifier: str | None = None
    specifics: dict[str, JsonValue] | None = None


class WhatsAppAttachment(WhatsAppNativeModel):
    """Native attachment metadata, not proof that its bytes can be downloaded."""

    object: Literal["Attachment"]
    id: str = Field(min_length=1)
    type: str
    mimetype: str | None = None
    url: str | None = Field(default=None, repr=False)
    url_expires_at: str | None = None
    is_unavailable: bool | None = None
    file_size: int | None = Field(default=None, ge=0)
    filename: str | None = None
    voice_note: bool | None = None
    sticker: bool | None = None


class WhatsAppQuoted(WhatsAppNativeModel):
    """A quote is message context, not a separate owned message observation."""

    id: str = Field(min_length=1)
    text: str | None = None
    attachments: list[WhatsAppAttachment] = Field(default_factory=list)
    sender: WhatsAppUser | None = None


class WhatsAppMessage(WhatsAppNativeModel):
    """Exact native message identity and payload; timestamp is provider text."""

    object: Literal["Message"]
    provider: Literal["whatsapp"]
    id: str = Field(min_length=1)
    chat_id: str = Field(min_length=1)
    sender_id: str = Field(min_length=1)
    timestamp: str = Field(min_length=1)
    is_sender: bool
    text: str | None = None
    is_hidden: bool | None = None
    is_seen: bool | None = None
    is_delivered: bool | None = None
    is_deleted: bool | None = None
    is_edited: bool | None = None
    is_event: bool | None = None
    view_mode: Literal["once", "replayable"] | None = None
    attachments: list[WhatsAppAttachment] = Field(default_factory=list)
    sender: WhatsAppUser | None = None
    quoted: WhatsAppQuoted | None = None


class WhatsAppMessagePreview(WhatsAppNativeModel):
    """Chat summary returned by providers; not a complete message observation."""

    object: Literal["MessagePreview"]
    id: str | None = None
    text: str
    sender_display_name: str | None = None
    is_sender: bool | None = None


class WhatsAppParticipant(WhatsAppNativeModel):
    """Current group member observation, not membership history."""

    object: Literal["GroupParticipant"]
    is_self: bool
    is_admin: bool
    user: WhatsAppUser


class WhatsAppChat(WhatsAppNativeModel):
    """Supported native chat; universal schema is not support for communities."""

    object: Literal["Chat"]
    id: str = Field(min_length=1)
    provider: Literal["whatsapp"]
    name: str | None = None
    is_group: bool
    is_1to1: bool
    is_channel: bool
    user: WhatsAppUser | None = None
    participants: list[WhatsAppParticipant] = Field(default_factory=list)
    last_message: WhatsAppMessagePreview | WhatsAppMessage | None = None


class WhatsAppReaction(WhatsAppNativeModel):
    """Current reaction with its native sender identity."""

    object: Literal["Reaction"]
    value: str
    is_sender: bool
    sender: WhatsAppUser


class WhatsAppInitialSync(WhatsAppNativeModel):
    """Vendor bootstrap state; completion is not a full-phone archive guarantee."""

    status: Literal["pending", "running", "failed", "completed"]
    started_at: str | None = None


class WhatsAppAccount(WhatsAppNativeModel):
    """Self principal and provider must be checked against the bound account."""

    object: Literal["Account"]
    id: str = Field(min_length=1)
    provider: Literal["whatsapp"]
    user_id: str = Field(min_length=1)
    status: Literal["running", "errored", "disconnected", "degraded", "partial"]
    is_locked: bool
    initial_sync: WhatsAppInitialSync | None = None


Item = TypeVar("Item", bound=WhatsAppNativeModel)


class WhatsAppPage(WhatsAppNativeModel, Generic[Item]):
    """Method-list pagination interpreted only after a declared cursor or offset mode.

    Account lists separately declare has_more. Current chat/message schemas do
    not; an unexpected has_more here only vetoes a contradictory continuation.
    """

    data: list[Item]
    total_count: int | None = Field(default=None, ge=0)
    next_cursor: str | None = None
    # Not declared by current chat/message schemas. If supplied, it may veto a
    # contradictory end; it never selects a mode or replaces its documented end.
    has_more: bool | None = None

    def cursor_after(self, previous: str | None) -> str | None:
        """Declared cursor mode: omitted next_cursor is documented exhaustion."""
        if self.next_cursor == "" or (
            self.next_cursor is not None and self.next_cursor == previous
        ):
            raise ValueError("Unipile cursor made no progress")
        if self.has_more is True and self.next_cursor is None:
            raise ValueError("Unipile has_more contradicts exhausted cursor pagination")
        if self.has_more is False and self.next_cursor is not None:
            raise ValueError("Unipile has_more contradicts continuing cursor pagination")
        return self.next_cursor

    def offset_after(self, offset: int, limit: int) -> int | None:
        """Declared offset mode advances by requested limit, never by short-page size."""
        if offset < 0 or limit < 1:
            raise ValueError("Invalid Unipile offset pagination")
        if self.next_cursor is not None:
            raise ValueError("Unipile cursor contradicts declared offset pagination")
        if self.has_more is True and not self.data:
            raise ValueError("Unipile has_more contradicts exhausted offset pagination")
        return offset + limit if self.data else None
