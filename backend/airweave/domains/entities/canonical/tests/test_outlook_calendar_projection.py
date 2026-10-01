"""Offline Graph event projection uses shared conversion and honest omitted-part coverage."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from airweave.domains.converters.html import HtmlConverter
from airweave.domains.entities.canonical.outlook_calendar_projection import map_outlook_calendar
from airweave.domains.entities.canonical.projection_models import ProjectionWork
from airweave.domains.entities.canonical.projector import StrictProjectionTracker, _select_inputs
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.test_outlook_projection import source
from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder


def event(body):
    record, _ = source(b"")
    parent = RecordIdentity(record_type="calendar", native_id="calendar-1")
    return record.model_copy(
        update={
            "identity": RecordIdentity(
                record_type="event", native_id="event-1", container_id="calendar-1"
            ),
            "parent": parent,
            "blobs": (),
            "payload": {
                "id": "event-1",
                "changeKey": "version-1",
                "type": "seriesMaster",
                "start": {
                    "dateTime": "2026-11-01T01:30:00.0000000",
                    "timeZone": "Pacific Standard Time",
                },
                "end": {
                    "dateTime": "2026-11-01T02:30:00.0000000",
                    "timeZone": "Pacific Standard Time",
                },
                "originalStartTimeZone": "tzone://Microsoft/Custom",
                "isAllDay": False,
                "isCancelled": True,
                "hasAttachments": False,
                "subject": "Réunion बैठक",
                "body": body,
                "bodyPreview": "LOSSY PREVIEW MUST NOT APPEAR",
                "organizer": {"emailAddress": {"name": "रोहन", "address": "r@example.com"}},
                "attendees": [
                    {
                        "emailAddress": {"name": "Éva", "address": "e@example.com"},
                        "type": "required",
                        "status": {"response": "accepted"},
                        "unknown": "DO NOT INDEX",
                    }
                ],
                "recurrence": {
                    "pattern": {
                        "type": "weekly",
                        "interval": 2,
                        "daysOfWeek": ["monday"],
                        "extension": "DO NOT INDEX",
                    },
                    "range": {
                        "type": "noEnd",
                        "startDate": "2026-10-01",
                        "recurrenceTimeZone": "Pacific Standard Time",
                    },
                },
                "location": {"displayName": "Office"},
                "categories": ["Planning"],
                "nativeUnknown": {"retained": "DO NOT INDEX"},
            },
        }
    )


async def build(record, tmp_path):
    mapped = await map_outlook_calendar(record, tmp_path)
    work = ProjectionWork(
        organization_id=uuid4(), record=record, pipeline_version=2, previous_generation=None
    )
    selected, coverage = _select_inputs(mapped, work, "outlook_calendar", uuid4(), lambda _: True)
    registry = MagicMock()
    registry.for_extension.return_value = HtmlConverter()
    batch = await TextualRepresentationBuilder(registry).build_with_text(
        selected,
        SimpleNamespace(source_short_name="outlook_calendar", logger=MagicMock()),
        SimpleNamespace(entity_tracker=StrictProjectionTracker()),
        native_bodies={
            item.entity.entity_id: item.native_body
            for item in mapped.parts
            if item.entity is not None and item.native_body is not None
        },
    )
    return mapped, coverage, batch


@pytest.mark.parametrize(
    "body_type,text",
    [
        ("text", "नमस्ते café\n<plain> & exact"),
        ("text", ""),
        ("html", "<p>नमस्ते <b>café</b> &amp; body</p>"),
    ],
)
async def test_body_provenance_unicode_schedule_and_partial_inventory(
    tmp_path, monkeypatch, body_type, text
):
    # Even an accidental future native HTTP request would fail this offline test.
    monkeypatch.setattr(
        "httpx.AsyncClient.request", AsyncMock(side_effect=AssertionError("offline"))
    )
    record = event({"contentType": body_type, "content": text})
    before = record.model_dump()
    mapped, coverage, batch = await build(record, tmp_path)
    built = batch.representations[0]
    assert built.kind == ("native_text" if body_type == "text" else "extracted_text")
    assert built.text == batch.entities[0].textual_representation
    body = built.text[built.content_start :]
    if body_type == "text":
        assert body == text
    else:
        assert "नमस्ते" in body and "café" in body and "&" in body
        assert "<p>" not in body and "<b>" not in body
        assert mapped.parts[0].native_body is None
    assert "2026-11-01T01:30:00.0000000 (Pacific Standard Time)" in built.text
    assert "01:30:00+00:00" not in built.text
    assert "tzone://Microsoft/Custom" in built.text
    assert "**Is Cancelled**: True" in built.text
    assert all(value in built.text for value in ("Éva", "accepted", "weekly", "monday", "Office"))
    assert "DO NOT INDEX" not in built.text and "LOSSY PREVIEW" not in built.text
    assert coverage.status == "partial"
    assert [(part.key, part.outcome) for part in coverage.parts[1:]] == [
        ("/attachment_inventory", "unavailable_original"),
        ("/cancelledOccurrences_inventory", "unavailable_original"),
    ]
    assert record.model_dump() == before


async def test_missing_body_is_not_preview_and_calendar_unknown_permissions_stay_unknown(tmp_path):
    record = event(None)
    _, coverage, batch = await build(record, tmp_path)
    assert batch.representations[0].kind == "generated_text"
    assert any(
        part.key == "/body/content" and part.outcome == "unavailable_original"
        for part in coverage.parts
    )
    calendar = record.model_copy(
        update={
            "identity": record.parent,
            "parent": None,
            "completeness": "complete",
            "payload": {
                "id": "calendar-1",
                "name": "Équipe",
                "owner": {"name": "Owner", "address": "o@x.test"},
                "unknown": "DO NOT INDEX",
            },
        }
    )
    mapped = await map_outlook_calendar(calendar, tmp_path)
    assert mapped.entities[0].owner == "Owner <o@x.test>"
    assert mapped.entities[0].can_edit is None
    assert len(mapped.parts) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"parent": None},
        {"parent": RecordIdentity(record_type="calendar", native_id="wrong")},
        {
            "parent": RecordIdentity(
                record_type="calendar", native_id="calendar-1", container_id="nested"
            )
        },
        {
            "identity": RecordIdentity(
                record_type="event", native_id="wrong", container_id="calendar-1"
            )
        },
        {"completeness": "complete"},
    ],
)
async def test_identity_parent_and_unsupported_completeness_fail_before_projection(
    tmp_path, changes
):
    with pytest.raises(ValueError):
        await map_outlook_calendar(event(None).model_copy(update=changes), tmp_path)


async def test_empty_html_does_not_publish_fabricated_native_body(tmp_path):
    # The shared converter cannot produce retained content for empty HTML.
    # Preserve its fail-closed behavior instead of declaring generated text to be the body.
    with pytest.raises(ValueError, match="Required projection input could not be converted"):
        await build(event({"contentType": "html", "content": ""}), tmp_path)


@pytest.mark.parametrize(
    "body_type,web_link",
    [
        ("text", "https://outlook.example/event/1"),
        ("html", "https://outlook.example/event/1"),
        ("text", None),
    ],
)
async def test_native_web_link_survives_vespa_payload_and_search_result(
    tmp_path, body_type, web_link
):
    from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
    from airweave.platform.destinations.vespa.transformer import EntityTransformer

    record = event({"contentType": body_type, "content": "Native body"})
    record = record.model_copy(update={"payload": {**record.payload, "webLink": web_link}})
    mapped = await map_outlook_calendar(record, tmp_path)
    fields = {"entity_id": "test-event", "name": "Event", "textual_representation": "body"}
    EntityTransformer(logger=MagicMock())._add_payload_field(fields, mapped.entities[0])
    engine = VespaVectorDB(app=MagicMock(), logger=MagicMock(), filter_translator=MagicMock())
    result = engine._convert_hits_to_results([{"fields": fields}]).results[0]
    assert result.web_url == (web_link or "")
    assert mapped.entities[0].web_url == web_link
