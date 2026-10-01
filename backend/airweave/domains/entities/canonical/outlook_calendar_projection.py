"""Offline projection of retained Graph calendar metadata and event bodies."""

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from airweave.domains.entities.canonical.blob_materializer import write_blob
from airweave.domains.entities.canonical.extraction_models import ExtractionPart
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_inputs import ProjectionInput, ProjectionInputs
from airweave.domains.sync_pipeline.pipeline.text_models import NativeTextBody
from airweave.platform.entities.outlook_calendar import (
    OutlookCalendarHtmlEventEntity,
    OutlookCalendarMetadataEntity,
    OutlookCalendarSearchEventEntity,
)
from airweave.platform.sources.outlook_calendar_models import OutlookCalendar, OutlookCalendarEvent
from airweave.platform.sources.outlook_mail_models import OutlookAddress, OutlookRecipient


class _Response(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    response: str | None = None


class _Attendee(OutlookRecipient):
    type: str | None = None
    status: _Response | None = None


class _Pattern(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    type: str
    interval: int | None = None
    month: int | None = None
    dayOfMonth: int | None = None
    daysOfWeek: list[str] = Field(default_factory=list)
    firstDayOfWeek: str | None = None
    index: str | None = None


class _Range(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    type: str
    startDate: str
    endDate: str | None = None
    recurrenceTimeZone: str | None = None
    numberOfOccurrences: int | None = None


class _Recurrence(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    pattern: _Pattern
    range: _Range


class _Location(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    displayName: str | None = None


class _Details(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    location: _Location | None = None
    locations: list[_Location] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    showAs: str | None = None


def _address(value: OutlookAddress) -> str | None:
    return (
        " <".join([value.name, value.address]) + ">"
        if value.name and value.address
        else (value.name or value.address)
    )


def _attendee(value: _Attendee) -> str:
    fields = [_address(value.emailAddress)]
    if value.type is not None:
        fields.append(f"type: {value.type}")
    if value.status is not None and value.status.response is not None:
        fields.append(f"response: {value.status.response}")
    return "; ".join(field for field in fields if field)


def _recurrence(value: _Recurrence) -> str:
    # Only declared native recurrence fields enter search, never extension JSON.
    sections = []
    for label, fields in (("Pattern", value.pattern), ("Range", value.range)):
        pairs = []
        for key, item in fields.model_dump(exclude_none=True).items():
            if item == []:
                continue
            rendered = ", ".join(item) if isinstance(item, list) else str(item)
            pairs.append(f"{key}: {rendered}")
        sections.append(f"{label}: " + "; ".join(pairs))
    return "\n".join(sections)


async def map_outlook_calendar(record: SourceRecord, directory: Path) -> ProjectionInputs:
    """Use canonical originals only; conversion and retained/indexed text stay shared."""
    if (
        record.deleted_at is not None
        or record.content_access != "available"
        or record.payload_schema_version != 1
        or record.blobs
    ):
        raise ValueError("Outlook calendar projection requires active schema-one JSON originals")
    if record.identity.record_type == "calendar":
        if record.parent is not None or record.identity.container_id is not None:
            raise ValueError("Outlook calendar must be a root identity")
        native = OutlookCalendar.model_validate(record.payload)
        if native.id != record.identity.native_id:
            raise ValueError("Outlook calendar native identity mismatch")
        entity = OutlookCalendarMetadataEntity(
            native_id=native.id,
            title=native.name or "Calendar",
            breadcrumbs=[],
            owner=_address(OutlookAddress.model_validate(native.owner)) if native.owner else None,
            can_edit=native.canEdit,
            can_view_private_items=native.canViewPrivateItems,
            is_default_calendar=native.isDefaultCalendar,
        )
        return ProjectionInputs(
            parts=(
                ProjectionInput(
                    part=ExtractionPart(part_index=0, key="/calendar", kind="record"),
                    entity=entity,
                ),
            )
        )
    parent = record.parent
    if (
        record.identity.record_type != "event"
        or parent is None
        or parent.record_type != "calendar"
        or parent.container_id is not None
        or record.identity.container_id != parent.native_id
        or record.completeness != "partial"
    ):
        raise ValueError("Outlook event requires its exact calendar parent and partial capture")
    event = OutlookCalendarEvent.model_validate(record.payload)
    if event.id != record.identity.native_id:
        raise ValueError("Outlook event native identity mismatch")
    details = _Details.model_validate(record.payload)
    locations = ([details.location] if details.location else []) + details.locations
    metadata = OutlookCalendarSearchEventEntity(
        native_id=event.id,
        title=event.subject or "Calendar event",
        calendar_id=parent.native_id,
        breadcrumbs=[],
        created_at=record.source_created_at,
        updated_at=record.source_updated_at,
        event_type=event.type,
        start=f"{event.start.dateTime} ({event.start.timeZone})",
        end=f"{event.end.dateTime} ({event.end.timeZone})",
        is_all_day=event.isAllDay,
        is_cancelled=event.isCancelled,
        organizer=_address(event.organizer.emailAddress) if event.organizer else None,
        attendees=[_attendee(_Attendee.model_validate(value)) for value in event.attendees],
        recurrence=_recurrence(_Recurrence.model_validate(event.recurrence))
        if event.recurrence is not None
        else None,
        series_master_id=event.seriesMasterId,
        original_start=event.originalStart,
        original_start_timezone=event.originalStartTimeZone,
        original_end_timezone=event.originalEndTimeZone,
        locations=list(
            dict.fromkeys(location.displayName for location in locations if location.displayName)
        ),
        categories=details.categories,
        show_as=details.showAs,
        web_link=event.webLink,
    )
    body = event.body
    if body is not None and body.contentType == "html":
        content = body.content.encode("utf-8")
        local = await write_blob(content, directory, suffix=".html")
        html_entity = OutlookCalendarHtmlEventEntity(
            **metadata.model_dump(round_trip=True),
            url=event.webLink or "https://outlook.office.com/calendar/",
            size=len(content),
            file_type="html",
            mime_type="text/html",
            local_path=str(local),
        )
        parts = [
            ProjectionInput(
                part=ExtractionPart(
                    part_index=0,
                    key="/body/content",
                    kind="body",
                    media_type="text/html",
                    extension=".html",
                ),
                entity=html_entity,
            )
        ]
    elif body is not None:
        parts = [
            ProjectionInput(
                part=ExtractionPart(
                    part_index=0, key="/body/content", kind="body", media_type="text/plain"
                ),
                entity=metadata,
                native_body=NativeTextBody(text=body.content),
            )
        ]
    else:
        parts = [
            ProjectionInput(
                part=ExtractionPart(part_index=0, key="/event", kind="record"), entity=metadata
            ),
            ProjectionInput(
                part=ExtractionPart(part_index=1, key="/body/content", kind="body"), entity=None
            ),
        ]
    for key in ("/attachment_inventory", "/cancelledOccurrences_inventory"):
        parts.append(
            ProjectionInput(
                part=ExtractionPart(part_index=len(parts), key=key, kind="record"), entity=None
            )
        )
    return ProjectionInputs(parts=tuple(parts))
