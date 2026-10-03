"""Calendar original descriptions reach prepared text and preview without metadata noise."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from airweave.domains.converters.fakes.registry import FakeConverterRegistry
from airweave.domains.entities.canonical.content_models import ContentProvenance
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.projection_models import (
    ProjectionBinding,
    ProjectionLocator,
    ProjectionWork,
)
from airweave.domains.entities.canonical.projector import (
    ProjectionConversionTracker,
    _select_inputs,
    _stamp_content,
)
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.test_slack_projection import message
from airweave.domains.entities.canonical.text_artifacts import prepare_text
from airweave.domains.search.owned import _matched_content
from airweave.domains.search.types.results import SearchResult
from airweave.domains.sync_pipeline.pipeline.text_builder import TextualRepresentationBuilder
from airweave.domains.sync_pipeline.pipeline.text_models import NativeTextBody
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.platform.destinations.vespa.transformer import EntityTransformer


def event(fields):
    return message([]).model_copy(
        update={
            "identity": RecordIdentity(record_type="event", native_id="E1", container_id="CAL1"),
            "parent": RecordIdentity(record_type="calendar", native_id="CAL1"),
            "completeness": "complete",
            "payload": {
                "id": "E1",
                "summary": "Meeting title",
                "location": "Office",
                "created": "2026-01-01T00:00:00Z",
                "updated": "2026-01-02T00:00:00Z",
                "start": {
                    "dateTime": "2026-07-20T08:00:00-07:00",
                    "timeZone": "America/Los_Angeles",
                },
                "end": {"dateTime": "2026-07-20T18:00:00-07:00", "timeZone": "America/Los_Angeles"},
                **fields,
            },
        }
    )


async def prepared_preview(record):
    """Use the production mapping, text/artifact and chunk/payload preview boundaries."""
    generation = uuid4()
    work = ProjectionWork(
        binding=ProjectionBinding(
            source_connection_id=uuid4(), source_name="google_calendar", collection_id=uuid4()
        ),
        organization_id=uuid4(),
        record=record,
        pipeline_version=5,
        previous_generation=None,
    )
    async with map_record(record, "google_calendar", AsyncMock()) as mapped:
        entities, coverage = _select_inputs(
            mapped, work, "google_calendar", generation, lambda _: True
        )
        batch = await TextualRepresentationBuilder(FakeConverterRegistry()).build_with_text(
            entities,
            SimpleNamespace(source_short_name="google_calendar", logger=MagicMock()),
            SimpleNamespace(entity_tracker=ProjectionConversionTracker()),
            native_bodies={
                item.entity.entity_id: item.native_body
                for item in mapped.parts
                if item.native_body is not None
            },
        )
        _stamp_content(batch, coverage)
        artifacts = prepare_text(batch.representations, generation)
        built = batch.representations[0]
        processor = ChunkEmbedProcessor(FakeConverterRegistry(), MagicMock(), MagicMock())
        (chunk,) = processor._multiply_entities(
            batch.entities,
            [[{"text": built.text, "start_index": 0, "end_index": len(built.text)}]],
            MagicMock(),
        )
        fields = {}
        EntityTransformer()._add_payload_field(fields, chunk)
        provenance = ContentProvenance.model_validate(
            json.loads(fields["payload"])["content_provenance"]
        )
        result = SearchResult(
            entity_id=chunk.entity_id,
            name=chunk.name,
            relevance_score=1,
            breadcrumbs=[],
            textual_representation=chunk.textual_representation,
            airweave_system_metadata={
                "source_name": "google_calendar",
                "entity_type": type(chunk).__name__,
                "chunk_index": 0,
                "original_entity_id": built.entity_id,
            },
            access={},
            web_url="",
            raw_source_fields={"content_provenance": provenance.model_dump()},
        )
        _, preview = _matched_content(result, ProjectionLocator.parse(built.entity_id), coverage)
        return built, artifacts[0], preview


@pytest.mark.parametrize(
    "description,expected",
    [
        ("I <3 café\nx < y\nनमस्ते", "I <3 café\nx < y\nनमस्ते"),
        (
            "<p>नमस्ते <b>café</b> &amp; <a href='https://example.com'>plans</a></p>",
            "नमस्ते **café** & [plans](https://example.com)",
        ),
        (
            "<style>.noise{display:none}</style><script>BAD()</script><p>Actual content</p>",
            "Actual content",
        ),
        ("", ""),
        (" \n", ""),
        ("<p><br></p>", ""),
    ],
)
async def test_description_prepared_content_and_preview_are_separate_from_dates(
    description, expected, monkeypatch
):
    monkeypatch.setattr(
        "httpx.AsyncClient.request", AsyncMock(side_effect=AssertionError("offline"))
    )
    record = event({"description": description})
    original = record.model_dump()
    built, (artifact, content), preview = await prepared_preview(record)
    assert built.kind == artifact.kind == "extracted_text"
    assert built.text[built.content_start :] == expected
    assert content.decode() == built.text
    metadata = built.text[: built.content_start]
    assert "**Description**:" not in metadata
    assert "2026-07-20" in metadata
    assert "2026-01-01" not in metadata
    assert "Meeting title" in metadata and "Office" in metadata
    assert preview == expected[:600].strip()
    assert "2026-01-01" not in preview and "2026-07-20" not in preview
    assert record.model_dump() == original


@pytest.mark.parametrize("fields", [{}, {"description": None}])
async def test_missing_description_has_no_original_body_or_fabricated_preview(fields):
    built, (artifact, _), preview = await prepared_preview(event(fields))
    assert built.kind == artifact.kind == "generated_text"
    assert built.content_start is None and preview is None


async def test_calendar_conversion_failure_is_not_silent_metadata_fallback(monkeypatch):
    def failed(_):
        raise ValueError("conversion failed")

    monkeypatch.setattr("airweave.domains.converters.html.html_to_text", failed)
    with pytest.raises(ValueError, match="conversion failed"):
        async with map_record(
            event({"description": "<p>body</p>"}), "google_calendar", AsyncMock()
        ):
            pass


def test_native_body_existing_default_and_kind_validation():
    assert NativeTextBody(text="exact").kind == "native_text"
    with pytest.raises(ValidationError):
        NativeTextBody(text="body", kind="generated_text")


async def test_missing_html_dependency_fails_batch_and_calendar_mapping(monkeypatch, tmp_path):
    import builtins

    from airweave.domains.converters.html import HtmlConverter
    from airweave.domains.sync_pipeline.exceptions import EntityProcessingError

    original_import = builtins.__import__

    def without_html(name, *args, **kwargs):
        if name == "html_to_markdown":
            raise ImportError("missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_html)
    with pytest.raises(EntityProcessingError, match="requires html-to-markdown"):
        await HtmlConverter().convert_batch([str(tmp_path / "body.html")])
    with pytest.raises(EntityProcessingError, match="requires html-to-markdown"):
        async with map_record(event({"description": "body"}), "google_calendar", AsyncMock()):
            pass


@pytest.mark.parametrize("description", [123, {}, []])
async def test_malformed_description_is_not_silently_coerced_or_dropped(description):
    with pytest.raises(ValidationError):
        async with map_record(event({"description": description}), "google_calendar", AsyncMock()):
            pass
