"""Native Outlook calendar boundaries; originals are retained without model dumping."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from airweave.platform.configs.config import CalendarOccurrenceWindow
from airweave.platform.sources.outlook_mail_models import OutlookBody, OutlookRecipient


class OutlookCalendar(BaseModel):
    """Calendar identity and permission metadata, not a Google calendar representation."""

    model_config = ConfigDict(extra="ignore", strict=True)
    id: str = Field(min_length=1)
    name: str | None = None
    owner: dict[str, JsonValue] | None = None
    canEdit: bool | None = None
    canViewPrivateItems: bool | None = None
    isDefaultCalendar: bool | None = None


class OutlookDateTimeZone(BaseModel):
    """Graph wall time plus native zone, including Windows or custom zone names."""

    model_config = ConfigDict(extra="ignore", strict=True)
    dateTime: str = Field(min_length=1)
    timeZone: str = Field(min_length=1)


class OutlookCalendarEvent(BaseModel):
    """The native event kind distinguishes masters, singles and expanded instances."""

    model_config = ConfigDict(extra="ignore", strict=True)
    id: str = Field(min_length=1)
    changeKey: str = Field(min_length=1)
    type: Literal["singleInstance", "seriesMaster", "occurrence", "exception"]
    start: OutlookDateTimeZone
    end: OutlookDateTimeZone
    isAllDay: bool
    isCancelled: bool
    hasAttachments: bool
    subject: str | None = None
    body: OutlookBody | None = None
    bodyPreview: str | None = None
    organizer: OutlookRecipient | None = None
    attendees: list[dict[str, JsonValue]] = Field(default_factory=list)
    seriesMasterId: str | None = None
    originalStart: str | None = None
    originalStartTimeZone: str | None = None
    originalEndTimeZone: str | None = None
    recurrence: dict[str, JsonValue] | None = None
    cancelledOccurrences: list[str] | None = None
    iCalUId: str | None = None
    webLink: str | None = None
    createdDateTime: str | None = None
    lastModifiedDateTime: str | None = None


class OutlookCalendarID(BaseModel):
    """A collection item is discovery evidence, not the retained exact original."""

    model_config = ConfigDict(extra="ignore", strict=True)
    id: str = Field(min_length=1)
    removed: JsonValue = Field(default=None, alias="@removed")

    @model_validator(mode="after")
    def full_item(self):
        """These full-list endpoints have no delta tombstone contract."""
        if "removed" in self.model_fields_set:
            raise ValueError("Unexpected removed item in a full calendar listing")
        return self


class OutlookCalendarPage(BaseModel):
    """An absent array or unexpected delta removal is never an empty successful page."""

    model_config = ConfigDict(extra="ignore", strict=True)
    value: list[OutlookCalendarID] = Field(max_length=500)
    next_link: str | None = Field(default=None, alias="@odata.nextLink", min_length=1)


class OutlookCalendarScopeContext(BaseModel):
    """Immutable per-scope request authority, persisted by the shared page engine."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    calendar_id: str | None = Field(default=None, min_length=1)
    window: CalendarOccurrenceWindow


class OutlookCalendarContinuation(BaseModel):
    """Only one collection's opaque continuation, never the expanding original archive."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    context: OutlookCalendarScopeContext
    phase: Literal["calendars", "events", "calendarView", "done"]
    next_link: str | None = Field(default=None, min_length=1)
    recent_links: tuple[str, ...] = Field(default=(), max_length=128)
