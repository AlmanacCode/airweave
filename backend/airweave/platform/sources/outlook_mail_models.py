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


class OutlookFolder(OutlookMessageID):
    """Folder topology includes hidden folders and explicitly identifies search views."""

    # OData minimal metadata omits the declared base type, but requires derived types.
    odata_type: str = Field(default="#microsoft.graph.mailFolder", alias="@odata.type")


class OutlookFolderPage(BaseModel):
    """One native topology page; missing arrays never mean an empty mailbox."""

    model_config = ConfigDict(extra="ignore", strict=True)
    value: list[OutlookFolder] = Field(max_length=500)
    next_link: str | None = Field(default=None, alias="@odata.nextLink", min_length=1)


class OutlookDeltaPage(OutlookMessagePage):
    """Both ordinary and removed IDs require exact mailbox hydration."""

    delta_link: str | None = Field(default=None, alias="@odata.deltaLink", min_length=1)

    @model_validator(mode="after")
    def terminal_or_next(self):
        """Graph supplies exactly one continuation or terminal link."""
        if (self.next_link is None) == (self.delta_link is None):
            raise ValueError("Delta page requires exactly one next or delta link")
        return self


class OutlookMailCheckpoint(BaseModel):
    """One complete mailbox round; byte capacity is enforced by ProviderCheckpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[2] = 2
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    folder_links: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def nonempty_links(self):
        """An absent folder token cannot masquerade as a completed baseline."""
        if any(not folder or not link for folder, link in self.folder_links.items()):
            raise ValueError("Folder checkpoints require nonempty identities and links")
        return self


class OutlookMailContinuation(OutlookMailCheckpoint):
    """Working topology and one pending delta page share the existing bounded cursor."""

    mode: Literal["full", "changes"] = "full"
    phase: Literal["topology", "messages"] = "topology"
    # Empty identity represents only the mailbox topology root.
    folders_to_visit: tuple[str, ...] = ("",)
    discovered_folders: tuple[str, ...] = ()
    # Physical folders accumulated during topology, then consumed during message capture.
    remaining_folders: tuple[str, ...] = ()
    pending_ids: tuple[str, ...] = Field(default=(), max_length=500)
    next_link: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def coherent_progress(self):
        """Malformed progress must not silently skip discovery or pending originals."""
        for values in (self.discovered_folders, self.remaining_folders, self.pending_ids):
            if len(values) != len(set(values)) or any(not value for value in values):
                raise ValueError("Outlook progress identities must be nonempty and unique")
        if len(self.folders_to_visit) != len(set(self.folders_to_visit)):
            raise ValueError("Outlook topology queue must be unique")
        if self.phase == "topology":
            if not self.folders_to_visit or self.pending_ids:
                raise ValueError("Invalid topology phase progress")
            if not set(self.remaining_folders).issubset(self.discovered_folders):
                raise ValueError("Physical folders must have been discovered")
        elif self.folders_to_visit or self.discovered_folders:
            raise ValueError("Message phase cannot contain topology work")
        elif not self.remaining_folders and (self.pending_ids or self.next_link):
            raise ValueError("Completed message progress cannot contain pending work")
        return self
