"""Original Calendar resources with per-calendar incremental synchronization."""

import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime, timezone
from urllib.parse import quote
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from airweave.domains.entities.canonical.calendar import is_cancelled_recurring_event
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
from airweave.platform.configs.config import CalendarOccurrenceWindow
from airweave.platform.cursors.google_calendar import CalendarWindowCoverage, GoogleCalendarCursor

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
    recurring_cancellation = kind == "event" and is_cancelled_recurring_event(payload)
    if (
        kind == "event"
        and payload.get("status") == "cancelled"
        and payload.get("recurringEventId")
        and not recurring_cancellation
    ):
        raise ValueError("Cancelled recurring event lacks its original occurrence identity")
    # Google requires clients to retain this exclusion for the lifetime of its series.
    # Keep its native ID/container so cancellation and reinstatement update the same row.
    deleted = (payload.get("status") == "cancelled" and not recurring_cancellation) or payload.get(
        "deleted"
    ) is True
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
    get: GetJSON, cursor: SyncCursor | None, window: CalendarOccurrenceWindow | None = None
) -> AsyncGenerator[SourceObservation, None]:
    """Refresh calendar membership; capture each accessible calendar independently."""
    previous = GoogleCalendarCursor.model_validate(cursor.data if cursor else {})
    tokens: dict[str, str] = {}
    coverage: dict[str, CalendarWindowCoverage] = {}
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
            async for observation in _capture_member(
                get, calendar_record, item, previous, window, tokens, coverage
            ):
                yield observation
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
            record_type="event_occurrence",
            container_id=missing,
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
        if window is not None:
            cursor.update(occurrence_coverage=coverage)


async def capture_occurrences(
    get: GetJSON, calendar_id: str, window: CalendarOccurrenceWindow
) -> AsyncGenerator[SourceObservation, None]:
    """Only a fully exhausted fixed horizon may reconcile this distinct scope."""
    yield StartedScope(record_type="event_occurrence", container_id=calendar_id)
    params = {
        "singleEvents": "true",
        "showDeleted": "true",
        "maxResults": 2500,
        "timeMin": window.start.isoformat(),
        "timeMax": window.end.isoformat(),
    }
    pages: set[str] = set()
    identities: set[str] = set()
    byte_count = 0
    while True:
        page = CalendarPage.model_validate(
            await get(f"{BASE}/calendars/{quote(calendar_id, safe='')}/events", params=params)
        )
        for item in page.items:
            byte_count += len(json.dumps(item).encode())
            observed = record("event_occurrence", item, calendar_id)
            if observed.identity.native_id in identities:
                raise ValueError("Expanded Calendar listing repeated an occurrence identity")
            identities.add(observed.identity.native_id)
            if len(identities) > 10000 or byte_count > 20 * 1024 * 1024:
                raise ValueError("Expanded Calendar capture exceeds bounded horizon budget")
            yield observed
        if not page.nextPageToken:
            yield CompletedScope(record_type="event_occurrence", container_id=calendar_id)
            return
        if page.nextPageToken in pages:
            raise ValueError("Expanded Calendar listing repeated a page token")
        pages.add(page.nextPageToken)
        if len(pages) >= 100:
            raise ValueError("Expanded Calendar capture exceeds page budget")
        params["pageToken"] = page.nextPageToken


async def _capture_member(
    get: GetJSON,
    calendar_record: CaptureRecord,
    item: dict[str, JsonValue],
    previous: GoogleCalendarCursor,
    window: CalendarOccurrenceWindow | None,
    tokens: dict[str, str],
    coverage: dict[str, CalendarWindowCoverage],
) -> AsyncGenerator[SourceObservation, None]:
    """Capture one member or record explicit access loss without claiming its window."""
    calendar_id = calendar_record.identity.native_id
    try:
        async for observation in capture_calendar(
            get, calendar_id, previous.calendar_tokens.get(calendar_id), tokens
        ):
            yield observation
        if window is not None:
            async for observation in capture_occurrences(get, calendar_id, window):
                yield observation
            calendar_timezone = item.get("timeZone")
            if not isinstance(calendar_timezone, str) or not calendar_timezone:
                raise ValueError("Calendar has no timezone for occurrence coverage")
            coverage[calendar_id] = CalendarWindowCoverage(
                start=window.start,
                end=window.end,
                timezone=calendar_timezone,
                completed_at=datetime.now(timezone.utc),
                scan_id=uuid4(),
            )
    except SourceEntityNotFoundError:
        yield CaptureRecord(
            identity=RecordIdentity(record_type="calendar", native_id=calendar_id),
            payload={"id": calendar_id},
            kind="delete",
            removal_reason="access_revoked",
            observed_at=datetime.now(timezone.utc),
        )
        yield RemovedScope(
            record_type="event_occurrence",
            container_id=calendar_id,
            removal_reason="access_revoked",
            observed_at=datetime.now(timezone.utc),
        )
        yield RemovedScope(
            record_type="event",
            container_id=calendar_id,
            removal_reason="access_revoked",
            observed_at=datetime.now(timezone.utc),
        )
