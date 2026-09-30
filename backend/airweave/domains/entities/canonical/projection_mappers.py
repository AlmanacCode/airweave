"""Typed search views of captured records. This module never contacts a provider."""

from __future__ import annotations

import mimetypes
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import JsonValue

from airweave.domains.entities.canonical.calendar import is_cancelled_recurring_event
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.entities._base import BaseEntity
from airweave.platform.entities.google_calendar import (
    GoogleCalendarEventEntity,
    GoogleCalendarListEntity,
)
from airweave.platform.entities.google_drive import GoogleDriveFileEntity, GoogleDriveFolderEntity
from airweave.platform.entities.slack import SlackChannelEntity, SlackMessageEntity
from airweave.platform.entities.wispr import WisprMeetingEntity


class ProjectionMappingError(ValueError):
    """Captured representation cannot be faithfully mapped; keep projection pending."""


def _string(value: JsonValue, *, default: str = "") -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise ProjectionMappingError("Expected a captured string field")
    return value


def _datetime(value: JsonValue) -> datetime | None:
    if value is None or value == "":
        return None
    parsed = datetime.fromisoformat(_string(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ProjectionMappingError("Captured timestamp lacks timezone")
    return parsed


async def _drive(
    record: SourceRecord,
    storage: StorageBackend,
    directory: Path,
) -> tuple[BaseEntity, ...]:
    from airweave.domains.entities.canonical.blob_materializer import read_blob, write_blob

    if record.identity.record_type != "file":
        raise ProjectionMappingError("Unsupported Drive record type")
    data = record.payload
    if data.get("mimeType") == "application/vnd.google-apps.folder":
        return (
            GoogleDriveFolderEntity(
                folder_id=record.identity.native_id,
                title=_string(data.get("name"), default="Untitled"),
                description=_string(data.get("description")),
                breadcrumbs=[],
            ),
        )
    if any(blob.role == "representation_manifest" for blob in record.blobs):
        from airweave.domains.entities.canonical.workspace_docs import read_document

        document = await read_document(record, storage)
        content = document.text().encode("utf-8")
        media_type = "text/plain"
        suffix = ".txt"
    else:
        # Historical Drive records retain their export-only contract.
        if len(record.blobs) != 1:
            raise ProjectionMappingError("Drive content requires exactly one captured blob")
        blob = record.blobs[0]
        content = await read_blob(record, blob, storage)
        media_type = blob.media_type or _string(data.get("mimeType"))
        suffix = mimetypes.guess_extension(media_type) or Path(_string(data.get("name"))).suffix
        if not suffix:
            raise ProjectionMappingError("Drive captured bytes have no known file format")
        suffix = suffix.lower()
    local_path = await write_blob(content, directory, suffix=suffix)
    metadata = {
        **data,
        "createdTime": data.get("createdTime") or record.observed_at.isoformat(),
        "modifiedTime": data.get("modifiedTime") or record.observed_at.isoformat(),
    }
    entity = GoogleDriveFileEntity.from_api(metadata, breadcrumbs=[])
    entity.local_path = str(local_path)
    entity.size = len(content)
    entity.mime_type = media_type
    entity.file_type = suffix.lstrip(".")
    entity.url = entity.web_url
    return (entity,)


def _calendar(record: SourceRecord) -> tuple[BaseEntity, ...]:
    metadata = {
        **record.payload,
        "created": record.payload.get("created") or record.observed_at.isoformat(),
        "updated": record.payload.get("updated") or record.observed_at.isoformat(),
    }
    if record.identity.record_type == "calendar":
        return (GoogleCalendarListEntity.from_api(metadata),)
    if record.identity.record_type != "event" or not record.identity.container_id:
        raise ProjectionMappingError("Calendar event requires its native calendar identity")
    return (
        GoogleCalendarEventEntity.from_api(
            metadata,
            calendar_key=record.identity.container_id,
            breadcrumbs=[],
        ),
    )


def _slack(record: SourceRecord) -> tuple[BaseEntity, ...]:
    payload = record.payload
    if record.identity.record_type == "channel":
        purpose = payload.get("purpose", {})
        topic = payload.get("topic", {})
        if not isinstance(purpose, dict) or not isinstance(topic, dict):
            raise ProjectionMappingError("Invalid Slack conversation description")
        return (
            SlackChannelEntity(
                channel_id=record.identity.native_id,
                title=_string(payload.get("name"), default=record.identity.native_id),
                purpose=_string(purpose.get("value")),
                topic=_string(topic.get("value")),
                breadcrumbs=[],
            ),
        )
    if record.identity.record_type != "message" or not record.identity.container_id:
        raise ProjectionMappingError("Slack message requires its conversation identity")
    timestamp = datetime.fromtimestamp(float(record.identity.native_id), timezone.utc)
    metadata = {**payload, "channel": {"id": record.identity.container_id}}
    entity = SlackMessageEntity.from_api(metadata, breadcrumbs=[])
    entity.created_at = entity.message_time = timestamp
    edited = payload.get("edited")
    if isinstance(edited, dict) and edited.get("ts"):
        entity.updated_at = datetime.fromtimestamp(float(edited["ts"]), timezone.utc)
    return (entity,)


def _wispr_text(responses: list[JsonValue], field: str) -> str:
    """Follow exactly the requested ranges; strip server continuation guidance."""
    expected = 0
    complete = False
    pieces: list[str] = []
    for page in responses:
        if not isinstance(page, dict):
            raise ProjectionMappingError("Invalid Wispr response range")
        arguments = page.get("requested_ranges")
        response = page.get("response")
        if not isinstance(arguments, dict) or not isinstance(response, dict):
            raise ProjectionMappingError("Missing Wispr native range response")
        window = arguments.get("view_" + field)
        if window is None:
            continue
        if not isinstance(window, dict) or window.get("start_char") != expected or complete:
            raise ProjectionMappingError("Wispr text ranges overlap or have a gap")
        text = _string(response.get(field))
        marker = re.search(
            rf"(?m)^\(\.\.\.truncated, \d+ chars remaining; continue with "
            rf"view_{field}\.start_char=(\d+)\.\.\.\)\s*$",
            text,
        )
        if marker:
            following = int(marker.group(1))
            if following <= expected:
                raise ProjectionMappingError("Wispr continuation does not advance")
            pieces.append(text[: marker.start()].rstrip())
            expected = following
        else:
            pieces.append(text)
            complete = True
    if not complete:
        raise ProjectionMappingError("Wispr text has an unfinished continuation")
    return "\n".join(pieces)


def _wispr(record: SourceRecord) -> tuple[BaseEntity, ...]:
    if record.identity.record_type == "meeting_listing":
        return ()
    if record.identity.record_type != "meeting":
        raise ProjectionMappingError("Unsupported Wispr record type")
    responses = record.payload.get("responses")
    if not isinstance(responses, list) or not responses or not isinstance(responses[0], dict):
        raise ProjectionMappingError("Wispr has no captured meeting content")
    first = responses[0].get("response")
    if not isinstance(first, dict):
        raise ProjectionMappingError("Wispr meeting response is invalid")
    return (
        WisprMeetingEntity(
            meeting_id=record.identity.native_id,
            title=_string(first.get("title"), default="Meeting"),
            notes=_wispr_text(responses, "content"),
            transcript=_wispr_text(responses, "transcript"),
            summary=_string(first.get("summary")),
            starts_at=_datetime(first.get("start")),
            modified_at=_datetime(first.get("modified_at")),
            share_link=_string(first.get("share_link")) or None,
            breadcrumbs=[],
        ),
    )


def excluded_from_search(record: SourceRecord, source_name: str) -> bool:
    """Retained calendar exclusions intentionally publish no searchable meeting."""
    return (
        source_name == "google_calendar"
        and (record.identity.record_type == "event_occurrence" or (
            record.identity.record_type == "event" and is_cancelled_recurring_event(record.payload)
        ))
    )


@asynccontextmanager
async def map_record(
    record: SourceRecord,
    source_name: str,
    storage: StorageBackend,
) -> AsyncIterator[tuple[BaseEntity, ...]]:
    """Keep verified local blob files alive only while strict projection consumes them."""
    if record.deleted_at is not None or record.content_access != "available":
        raise ProjectionMappingError("Unavailable records cannot be projected")
    if excluded_from_search(record, source_name):
        yield ()
        return
    with TemporaryDirectory(prefix="airweave-projection-") as temporary:
        directory = Path(temporary)
        if source_name == "gmail":
            from airweave.domains.entities.canonical.gmail_projection import map_gmail

            entities = await map_gmail(record, storage, directory)
        elif source_name == "github":
            from airweave.domains.entities.canonical.github_projection import map_github

            entities = await map_github(record, storage, directory)
        elif source_name == "linear":
            from airweave.domains.entities.canonical.linear_projection import map_linear

            entities = await map_linear(record, storage, directory)
        elif source_name == "notion":
            from airweave.domains.entities.canonical.notion_projection import map_notion

            entities = map_notion(record)
        elif source_name == "google_drive":
            entities = await _drive(record, storage, directory)
        elif source_name == "google_calendar":
            entities = _calendar(record)
        elif source_name == "slack":
            entities = _slack(record)
        elif source_name == "wispr":
            entities = _wispr(record)
        else:
            raise ProjectionMappingError("Source has no audited search projection mapper")
        yield entities
