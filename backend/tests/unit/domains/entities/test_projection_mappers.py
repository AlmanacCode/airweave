"""Search projections preserve canonical data and cannot fetch missing source content."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
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
        entities = entities.entities
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
        entities = entities.entities
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
            "abc\n\n(...truncated, 3 chars remaining; "
            "continue with view_transcript.start_char=3...)"
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
    async with map_record(record("meeting", payload), "wispr", AsyncMock()) as mapped:
        assert [part.part.key for part in mapped.parts] == ["notes", "transcript"]
        assert mapped.parts[0].native_body.text == "notes"
        assert mapped.parts[1].native_body.text == "abcdef"
        assert mapped.entities[0].transcript == ""
        assert mapped.entities[1].notes == ""
        assert "guidance" not in mapped.parts[1].native_body.text


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
                        "abc\n\n(...truncated, 3 chars remaining; "
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
        entities = entities.entities
        path = Path(entities[0].local_path)
        assert path.read_bytes() == content
        assert path.name == digest + ".txt"
        assert entities[0].mime_type == "text/plain"
    assert not path.exists()
    storage.read_file.assert_awaited_once_with(blob.key, max_bytes=blob.size_bytes)


def test_added_typed_entities_are_registered():
    from airweave.domains.entities.registry import EntityDefinitionRegistry
    from airweave.platform.entities.google_drive import GoogleDriveFolderEntity
    from airweave.platform.entities.slack import SlackChannelEntity
    from airweave.platform.entities.wispr import WisprMeetingEntity

    registry = EntityDefinitionRegistry()
    registry.build()
    for entity in (WisprMeetingEntity, GoogleDriveFolderEntity, SlackChannelEntity):
        assert registry.get_short_name_by_class(entity)


async def test_owned_drive_spreadsheet_uses_real_converter():
    import hashlib
    from io import BytesIO
    from pathlib import Path

    import openpyxl

    from airweave.domains.converters.registry import ConverterRegistry
    from airweave.domains.entities.canonical.requests import BlobReference
    from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder

    workbook = openpyxl.Workbook()
    workbook.active.append(["Project", "Status"])
    workbook.active.append(["Owned capture", "Verified"])
    buffer = BytesIO()
    workbook.save(buffer)
    content = buffer.getvalue()
    digest = hashlib.sha256(content).hexdigest()
    mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    item = record(
        "file",
        {"id": "sheet", "name": "Planning", "mimeType": "application/vnd.google-apps.spreadsheet"},
    )
    blob = BlobReference(
        key=f"canonical/{item.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type=mime,
    )
    item = item.model_copy(update={"blobs": (blob,)})
    storage = AsyncMock()
    storage.read_file.return_value = content
    async with map_record(item, "google_drive", storage) as entities:
        entities = entities.entities
        path = entities[0].local_path
        assert Path(path).suffix == ".xlsx"
        tracker = AsyncMock()
        result = await TextualRepresentationBuilder(ConverterRegistry()).build_for_batch(
            list(entities),
            SimpleNamespace(source_short_name="google_drive", logger=MagicMock()),
            SimpleNamespace(entity_tracker=tracker),
        )
        assert len(result) == 1
        assert "Owned capture" in result[0].textual_representation
        assert "Verified" in result[0].textual_representation
        tracker.record_skipped.assert_not_awaited()
    assert not Path(path).exists()


@pytest.mark.asyncio
async def test_wispr_scratchpad_retains_all_text_ranges_without_meeting_fields():
    payload = {
        "responses": [
            {
                "requested_ranges": {"view_content": {"start_char": 0}},
                "response": {
                    "id": "note",
                    "title": "Ideas",
                    "content": (
                        "abc\n\n(...truncated, 3 chars remaining; "
                        "continue with view_content.start_char=3...)"
                    ),
                    "modified_at": "2026-09-30T00:00:00Z",
                },
            },
            {
                "requested_ranges": {"view_content": {"start_char": 3}},
                "response": {
                    "id": "note",
                    "title": "Ideas",
                    "content": "def",
                    "modified_at": "2026-09-30T00:00:00Z",
                },
            },
        ]
    }
    original = record("scratchpad_note", payload, native_id="note")
    before = original.model_dump()
    storage = AsyncMock()
    async with map_record(original, "wispr", storage) as entities:
        entities = entities.entities
        assert entities[0].note_id == "note"
        assert entities[0].content == "abcdef"
        assert entities[0].web_url == ""
    assert original.model_dump() == before
    assert storage.mock_calls == []


@pytest.mark.asyncio
async def test_wispr_projection_rejects_ambiguous_stored_continuation():
    marker = "(...truncated, 3 chars remaining; continue with view_content.start_char=3...)"
    payload = {
        "responses": [
            {
                "requested_ranges": {"view_content": {"start_char": 0}},
                "response": {"id": "note", "content": marker + "\n" + marker},
            }
        ]
    }
    with pytest.raises(ProjectionMappingError, match="ambiguous"):
        async with map_record(
            record("scratchpad_note", payload, native_id="note"), "wispr", AsyncMock()
        ):
            pass


@pytest.mark.asyncio
async def test_wispr_explicitly_absent_transcript_keeps_available_notes():
    native = {
        "id": "meeting",
        "content": "available notes",
        "has_transcript": False,
        "transcript": None,
    }
    payload = {
        "responses": [
            {
                "requested_ranges": {
                    "view_content": {"start_char": 0},
                    "view_transcript": {"start_char": 0},
                },
                "response": native,
            }
        ]
    }
    async with map_record(record("meeting", payload), "wispr", AsyncMock()) as result:
        assert result.entities[0].notes == "available notes"
        assert result.entities[0].transcript == ""
    assert native["transcript"] is None
    native["has_transcript"] = True
    with pytest.raises(ProjectionMappingError, match="unavailable or malformed"):
        async with map_record(record("meeting", payload), "wispr", AsyncMock()):
            pass


@pytest.mark.parametrize("unit", ["codepoints", "utf16"])
def test_wispr_ranges_preserve_unicode_and_source_whitespace(unit):
    from airweave.domains.entities.canonical.projection_mappers import _wispr_text

    first = "स्वीकृत 😀 \n"
    offset = len(first) if unit == "codepoints" else len(first.encode("utf-16-le")) // 2
    pages = [
        {
            "requested_ranges": {"view_content": {"start_char": 0}},
            "response": {
                "content": first
                + "\n\n(...truncated, 3 chars remaining; "
                + f"continue with view_content.start_char={offset}...)"
            },
        },
        {
            "requested_ranges": {"view_content": {"start_char": offset}},
            "response": {"content": "done \n\n"},
        },
    ]
    assert _wispr_text(pages, "content") == first + "done \n\n"


def test_wispr_wrapped_transcript_preserves_midword_and_terminal_whitespace():
    import copy

    from airweave.domains.entities.canonical.projection_mappers import (
        _WISPR_TRANSCRIPT_FOOTER,
        _WISPR_TRANSCRIPT_HEADER,
        _wispr_text,
    )

    pages = [
        {
            "requested_ranges": {"view_transcript": {"start_char": 0}},
            "response": {
                "transcript": _WISPR_TRANSCRIPT_HEADER
                + "appro\n\n(...truncated, 3 chars remaining; "
                "continue with view_transcript.start_char=5...)" + _WISPR_TRANSCRIPT_FOOTER
            },
        },
        {
            "requested_ranges": {"view_transcript": {"start_char": 5}},
            "response": {
                "transcript": _WISPR_TRANSCRIPT_HEADER + "ved \n" + _WISPR_TRANSCRIPT_FOOTER
            },
        },
    ]
    before = copy.deepcopy(pages)
    assert _wispr_text(pages, "transcript") == "approved \n"
    assert pages == before


@pytest.mark.parametrize("defect", ["separator", "footer", "length", "gap", "unit_change"])
def test_wispr_inconsistent_range_framing_fails(defect):
    from airweave.domains.entities.canonical.projection_mappers import _wispr_text

    pages = [
        {
            "requested_ranges": {"view_content": {"start_char": 0}},
            "response": {
                "content": "😀a\n\n(...truncated, 2 chars remaining; "
                "continue with view_content.start_char=2...)"
            },
        },
        {"requested_ranges": {"view_content": {"start_char": 2}}, "response": {"content": "bc"}},
    ]
    if defect == "separator":
        pages[0]["response"]["content"] = pages[0]["response"]["content"].replace("\n\n", "\n")
    elif defect == "footer":
        pages[0]["response"]["content"] += "\nUnknown guidance"
    elif defect == "length":
        pages[0]["response"]["content"] = pages[0]["response"]["content"].replace(
            "start_char=2", "start_char=7"
        )
    elif defect == "gap":
        pages[1]["requested_ranges"]["view_content"]["start_char"] = 3
    else:
        # First range requires codepoints, the next would require UTF16 units.
        pages[1]["response"]["content"] = (
            "😀a\n\n(...truncated, 1 chars remaining; continue with view_content.start_char=5...)"
        )
    with pytest.raises(ProjectionMappingError):
        _wispr_text(pages, "content")


def test_wispr_terminal_transcript_rejects_incomplete_wrapper():
    from airweave.domains.entities.canonical.projection_mappers import (
        _WISPR_TRANSCRIPT_HEADER,
        _wispr_text,
    )

    with pytest.raises(ProjectionMappingError, match="framing"):
        _wispr_text(
            [
                {
                    "requested_ranges": {"view_transcript": {"start_char": 0}},
                    "response": {"transcript": _WISPR_TRANSCRIPT_HEADER + "intact text"},
                }
            ],
            "transcript",
        )
