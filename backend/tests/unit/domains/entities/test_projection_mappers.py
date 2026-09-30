"""Search projections preserve canonical data and cannot fetch missing source content."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import (
    ProjectionMappingError,
    map_record,
)
from airweave.domains.entities.canonical.requests import RecordIdentity


def record(record_type, payload, *, native_id="id", container_id=None):
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(
            record_type=record_type, native_id=native_id, container_id=container_id
        ),
        revision=1,
        payload=payload,
        payload_schema_version=1,
        capture_hash="hash",
        content_hash=None,
        completeness="complete",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


@pytest.mark.asyncio
async def test_calendar_keeps_master_recurrence_and_native_container():
    payload = {
        "id": "event",
        "summary": "Weekly",
        "start": {"date": "2026-10-01"},
        "end": {"date": "2026-10-02"},
        "recurrence": ["RRULE:FREQ=WEEKLY"],
        "unknown_native": "keep",
    }
    item = record("event", payload, native_id="event", container_id="calendar")
    async with map_record(item, "google_calendar", AsyncMock()) as entities:
        assert entities[0].calendar_key == "calendar"
        assert entities[0].recurrence == ["RRULE:FREQ=WEEKLY"]
        assert entities[0].start_date == "2026-10-01"
    assert item.payload == payload


@pytest.mark.asyncio
async def test_slack_history_shape_uses_container_without_provider_lookup():
    item = record(
        "message",
        {"ts": "123.456", "text": "hello", "edited": {"ts": "124.0"}},
        native_id="123.456",
        container_id="C1",
    )
    storage = AsyncMock()
    async with map_record(item, "slack", storage) as entities:
        assert entities[0].channel_id == "C1"
        assert entities[0].text == "hello"
        assert entities[0].updated_at.timestamp() == 124
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_drive_missing_blob_fails_without_remote_fallback():
    item = record("file", {"id": "file", "name": "file.pdf", "mimeType": "application/pdf"})
    with pytest.raises(ProjectionMappingError, match="blob"):
        async with map_record(item, "google_drive", AsyncMock()):
            pass


@pytest.mark.asyncio
async def test_wispr_ranges_do_not_duplicate_default_content():
    first = {
        "id": "meeting",
        "title": "Planning",
        "summary": "summary",
        "content": "notes",
        "transcript": (
            "abc\n(...truncated, 3 chars remaining; "
            "continue with view_transcript.start_char=3...)\n\nProvider guidance."
        ),
    }
    second = {"id": "meeting", "content": "notes", "transcript": "def"}
    payload = {
        "responses": [
            {
                "requested_ranges": {
                    "view_content": {"start_char": 0},
                    "view_transcript": {"start_char": 0},
                },
                "response": first,
            },
            {"requested_ranges": {"view_transcript": {"start_char": 3}}, "response": second},
        ]
    }
    async with map_record(record("meeting", payload), "wispr", AsyncMock()) as entities:
        assert entities[0].notes == "notes"
        assert entities[0].transcript == "abc\ndef"
        assert "guidance" not in entities[0].transcript


@pytest.mark.asyncio
async def test_wispr_unfinished_ranges_fail_instead_of_indexing_truncated_body():
    payload = {
        "responses": [
            {
                "requested_ranges": {
                    "view_content": {"start_char": 0},
                    "view_transcript": {"start_char": 0},
                },
                "response": {
                    "content": "",
                    "transcript": (
                        "(...truncated, 3 chars remaining; "
                        "continue with view_transcript.start_char=3...)"
                    ),
                },
            }
        ]
    }
    with pytest.raises(ProjectionMappingError, match="unfinished"):
        async with map_record(record("meeting", payload), "wispr", AsyncMock()):
            pass


@pytest.mark.asyncio
async def test_drive_uses_verified_owned_bytes_and_cleans_materialization():
    import hashlib
    from pathlib import Path

    from airweave.domains.entities.canonical.requests import BlobReference

    content = b"owned document body"
    item = record("file", {"id": "file", "name": "../../unsafe.txt", "mimeType": "text/plain"})
    digest = hashlib.sha256(content).hexdigest()
    blob = BlobReference(
        key=f"canonical/{item.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="text/plain",
    )
    item = item.model_copy(update={"blobs": (blob,)})
    storage = AsyncMock()
    storage.read_file.return_value = content
    async with map_record(item, "google_drive", storage) as entities:
        path = Path(entities[0].local_path)
        assert path.read_bytes() == content
        assert path.name == digest + ".txt"
        assert entities[0].mime_type == "text/plain"
    assert not path.exists()
    storage.read_file.assert_awaited_once_with(blob.key)


def test_added_typed_entities_are_registered():
    from airweave.domains.entities.registry import EntityDefinitionRegistry
    from airweave.platform.entities.google_drive import GoogleDriveFolderEntity
    from airweave.platform.entities.slack import SlackChannelEntity
    from airweave.platform.entities.wispr import WisprMeetingEntity

    registry = EntityDefinitionRegistry()
    registry.build()
    for entity in (WisprMeetingEntity, GoogleDriveFolderEntity, SlackChannelEntity):
        assert registry.get_short_name_by_class(entity)
