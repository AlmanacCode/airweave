"""Original Calendar resources with per-calendar incremental synchronization."""

from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime, timezone
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
    RemovedScope,
    StartedScope,
)
from airweave.domains.entities.canonical.source import SourceObservation
from airweave.domains.sources.exceptions import (
    SourceEntityNotFoundError,
    SourceGoneError,
)
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.cursors.google_calendar import GoogleCalendarCursor

GetJSON = Callable[..., Awaitable[dict]]
BASE = "https://www.googleapis.com/calendar/v3"


class CalendarPage(BaseModel):
    """Validate pagination shape without stripping fields from original resources."""

    model_config = ConfigDict(extra="ignore")
    items: list[dict[str, JsonValue]] = Field(default_factory=list)
    nextPageToken: str | None = None
    nextSyncToken: str | None = None

    def completed_token(self) -> str:
        """An exhausted event listing must supply its durable continuation boundary."""
        if not self.nextSyncToken:
            raise ValueError("Calendar event enumeration ended without nextSyncToken")
        return self.nextSyncToken


def record(
    kind: str, payload: dict[str, JsonValue], calendar_id: str | None = None
) -> CaptureRecord:
    """Preserve sparse cancellation identity as well as full live provider JSON."""
    native_id = payload.get("id")
    if not isinstance(native_id, str) or not native_id:
        raise ValueError("Calendar returned a resource without an ID")
    deleted = payload.get("status") == "cancelled" or payload.get("deleted") is True
    return CaptureRecord(
        identity=RecordIdentity(record_type=kind, native_id=native_id, container_id=calendar_id),
        payload=payload,
        kind="delete" if deleted else "upsert",
        removal_reason="provider_deleted" if deleted else None,
        observed_at=datetime.now(timezone.utc),
    )


async def capture_calendar(
    get: GetJSON, calendar_id: str, token: str | None, tokens: dict[str, str]
) -> AsyncGenerator[SourceObservation, None]:
    """A 410 resets only this calendar, including sightings from earlier delta pages."""
    full = token is None
    if full:
        yield StartedScope(record_type="event", container_id=calendar_id)
    params: dict[str, str | int] = {
        "maxResults": 2500,
        "showDeleted": "true",
        "singleEvents": "false",
    }
    if token:
        params["syncToken"] = token
    seen_pages: set[str] = set()
    while True:
        try:
            page = CalendarPage.model_validate(
                await get(f"{BASE}/calendars/{quote(calendar_id, safe='')}/events", params=params)
            )
        except SourceGoneError:
            if full:
                raise
            full = True
            params.pop("syncToken", None)
            params.pop("pageToken", None)
            seen_pages.clear()
            yield StartedScope(record_type="event", container_id=calendar_id)
            continue
        for item in page.items:
            yield record("event", item, calendar_id)
        if not page.nextPageToken:
            tokens[calendar_id] = page.completed_token()
            if full:
                yield CompletedScope(record_type="event", container_id=calendar_id)
            return
        if page.nextPageToken in seen_pages:
            raise ValueError("Calendar returned a repeated page token")
        seen_pages.add(page.nextPageToken)
        params["pageToken"] = page.nextPageToken


async def generate_calendar_observations(
    get: GetJSON, cursor: SyncCursor | None
) -> AsyncGenerator[SourceObservation, None]:
    """Refresh calendar membership; capture each accessible calendar independently."""
    previous = GoogleCalendarCursor.model_validate(cursor.data if cursor else {})
    tokens: dict[str, str] = {}
    seen_calendars: set[str] = set()
    seen_pages: set[str] = set()
    params: dict[str, str | int] = {"maxResults": 250, "showHidden": "true"}
    yield StartedScope(record_type="calendar")
    while True:
        page = CalendarPage.model_validate(
            await get(f"{BASE}/users/me/calendarList", params=params)
        )
        for item in page.items:
            calendar_record = record("calendar", item)
            calendar_id = calendar_record.identity.native_id
            if calendar_id in seen_calendars:
                raise ValueError("Calendar list repeated an identity across pages")
            seen_calendars.add(calendar_id)
            yield calendar_record
            try:
                async for observation in capture_calendar(
                    get, calendar_id, previous.calendar_tokens.get(calendar_id), tokens
                ):
                    yield observation
            except SourceEntityNotFoundError:
                yield CaptureRecord(
                    identity=RecordIdentity(record_type="calendar", native_id=calendar_id),
                    payload={"id": calendar_id},
                    kind="delete",
                    removal_reason="access_revoked",
                    observed_at=datetime.now(timezone.utc),
                )
                yield RemovedScope(
                    record_type="event",
                    container_id=calendar_id,
                    removal_reason="access_revoked",
                    observed_at=datetime.now(timezone.utc),
                )
        if not page.nextPageToken:
            break
        if page.nextPageToken in seen_pages:
            raise ValueError("Calendar list returned a repeated page token")
        seen_pages.add(page.nextPageToken)
        params["pageToken"] = page.nextPageToken
    for missing in previous.calendar_tokens.keys() - seen_calendars:
        yield CaptureRecord(
            identity=RecordIdentity(record_type="calendar", native_id=missing),
            payload={"id": missing},
            kind="delete",
            removal_reason="scope_removed",
            observed_at=datetime.now(timezone.utc),
        )
        yield RemovedScope(
            record_type="event",
            container_id=missing,
            removal_reason="scope_removed",
            observed_at=datetime.now(timezone.utc),
        )
    yield CompletedScope(record_type="calendar")
    if cursor is not None:
        cursor.update(calendar_tokens=tokens)
