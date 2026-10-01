"""Typed Outlook native metadata and bounded full-capture continuation."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OutlookAddress(BaseModel):
    """Native email address; sender and from retain separate meanings."""

    model_config = ConfigDict(extra="ignore", strict=True)
    name: str | None = None
    address: str | None = None


class OutlookRecipient(BaseModel):
    """Graph recipient envelope."""

    model_config = ConfigDict(extra="ignore", strict=True)
    emailAddress: OutlookAddress


class OutlookBody(BaseModel):
    """The full native body, distinct from bodyPreview."""

    model_config = ConfigDict(extra="ignore", strict=True)
    contentType: Literal["text", "html"]
    content: str


class OutlookMessage(BaseModel):
    """Validated native fields; capture retains the original JSON rather than a model dump."""

    model_config = ConfigDict(extra="ignore", strict=True, populate_by_name=True)
    id: str = Field(min_length=1)
    changeKey: str = Field(min_length=1)
    parentFolderId: str = Field(min_length=1)
    subject: str | None = None
    conversationId: str | None = None
    internetMessageId: str | None = None
    sender: OutlookRecipient | None = None
    from_: OutlookRecipient | None = Field(default=None, alias="from")
    replyTo: list[OutlookRecipient] = Field(default_factory=list)
    toRecipients: list[OutlookRecipient] = Field(default_factory=list)
    ccRecipients: list[OutlookRecipient] = Field(default_factory=list)
    bccRecipients: list[OutlookRecipient] = Field(default_factory=list)
    body: OutlookBody | None = None
    bodyPreview: str | None = None
    hasAttachments: bool = False
    isDraft: bool = False
    receivedDateTime: str | None = None
    sentDateTime: str | None = None
    createdDateTime: str | None = None
    lastModifiedDateTime: str | None = None
    webLink: str | None = None


class OutlookMessageID(BaseModel):
    """A list result is discovery, never an authoritative current message payload."""

    model_config = ConfigDict(extra="ignore", strict=True)
    id: str = Field(min_length=1)


class OutlookMessagePage(BaseModel):
    """Explicit array required: missing metadata cannot become an empty successful page."""

    model_config = ConfigDict(extra="ignore", strict=True)
    value: list[OutlookMessageID] = Field(max_length=500)
    next_link: str | None = Field(default=None, alias="@odata.nextLink", min_length=1)


class OutlookMailContinuation(BaseModel):
    """Only pending native IDs and one native next link; no mailbox-sized folder map."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    started: bool = False
    pending_ids: tuple[str, ...] = Field(default=(), max_length=500)
    next_link: str | None = None

    @model_validator(mode="after")
    def coherent_progress(self):
        """An initial cursor cannot masquerade as an acknowledged native page."""
        if len(set(self.pending_ids)) != len(self.pending_ids) or any(
            not x for x in self.pending_ids
        ):
            raise ValueError("Pending Outlook identities must be nonempty and unique")
        if not self.started and (self.pending_ids or self.next_link is not None):
            raise ValueError("Initial Outlook continuation cannot contain progress")
        return self
