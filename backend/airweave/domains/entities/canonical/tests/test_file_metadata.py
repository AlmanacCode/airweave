"""Unextractable originals remain discoverable without claiming readable content."""

from unittest.mock import AsyncMock

import pytest

from airweave.domains.entities.canonical.extraction_models import (
    ExtractionCoverage,
    ExtractionOutcome,
)
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.tests.test_drive_projection import original
from airweave.platform.entities._base import FileEntity


@pytest.mark.asyncio
async def test_metadata_only_file_keeps_content_gap_and_never_fetches_bytes():
    record = original("video/mp4").model_copy(update={"completeness": "metadata_only"})
    record.payload.update(name="launch-demo.mp4", url_private="secret-never-index")
    before = record.model_dump(mode="json")
    storage = AsyncMock()
    async with map_record(record, "google_drive", storage) as mapped:
        content, metadata = mapped.parts
        assert content.entity is None and content.part.part_index == 0
        assert metadata.part.kind == "metadata" and metadata.part.part_index == 1
        assert metadata.entity.filename == "launch-demo.mp4"
        assert not isinstance(metadata.entity, FileEntity)
        assert "secret-never-index" not in metadata.entity.model_dump_json()
        coverage = ExtractionCoverage(
            parts=(
                ExtractionOutcome(
                    **content.part.model_dump(),
                    outcome="unavailable_original",
                    reason="original_not_captured",
                ),
                ExtractionOutcome(**metadata.part.model_dump(), outcome="indexed"),
            )
        )
        assert coverage.status == "unavailable"
    assert storage.mock_calls == []
    assert record.model_dump(mode="json") == before


@pytest.mark.asyncio
async def test_folder_does_not_gain_duplicate_metadata_part():
    record = original("application/vnd.google-apps.folder")
    async with map_record(record, "google_drive", AsyncMock()) as mapped:
        assert len(mapped.parts) == 1
        assert mapped.parts[0].part.kind == "record"
