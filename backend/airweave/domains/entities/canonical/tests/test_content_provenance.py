"""Known construction boundaries, exact chunk offsets and existing payload transport."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airweave.domains.converters.fakes.registry import FakeConverterRegistry
from airweave.domains.entities.canonical.content_models import ContentProvenance, MatchedPart
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.platform.chunkers.code import CodeChunker
from airweave.platform.chunkers.semantic import SemanticChunker
from airweave.platform.destinations.vespa.transformer import EntityTransformer
from airweave.platform.entities._base import AirweaveSystemMetadata
from airweave.platform.entities.slack import SlackMessageEntity


def provenance(text, start):
    return ContentProvenance(
        part=MatchedPart(part_index=0, key="body", kind="body", title="Example"),
        content_start=start,
        content_end=len(text),
    )


@pytest.mark.parametrize(
    "text,boundary,start,end,expected",
    [
        ("generated header\n# Metadata user content", 17, 0, 16, ""),
        ("generated header\n# Metadata user content", 17, 0, 40, "# Metadata user content"),
        ("generated header\n# Metadata user content", 17, 28, 40, "user content"),
        ("header" + "🙂" * 700, 6, 0, 706, "🙂" * 600),
    ],
)
def test_content_overlap_uses_offsets_not_delimiter_guessing(text, boundary, start, end, expected):
    end = min(end, len(text))
    assert (
        provenance(text, boundary).chunk_preview(text, text[start:end], start, end).preview
        == expected
    )


def test_malformed_offset_and_generated_text_cannot_claim_native_preview():
    full = "metadata\nreal content"
    with pytest.raises(ValueError, match="exact prepared-text offsets"):
        provenance(full, 9).chunk_preview(full, "real content", 0, 12)
    assert provenance(full, None).chunk_preview(full, full, 0, len(full)).preview is None


def test_processor_preserves_provenance_in_existing_vespa_payload():
    text = "generated header\n# Metadata user content"
    entity = SlackMessageEntity.from_api({"ts": "1.0", "text": text}, breadcrumbs=[])
    entity.entity_id = "part"
    entity.textual_representation = text
    entity.airweave_system_metadata = AirweaveSystemMetadata(
        content_provenance=provenance(text, 17)
    )
    processor = ChunkEmbedProcessor(FakeConverterRegistry(), MagicMock(), MagicMock())
    (chunk,) = processor._multiply_entities(
        [entity], [[{"text": text[17:], "start_index": 17, "end_index": len(text)}]], MagicMock()
    )
    fields = {}
    EntityTransformer()._add_payload_field(fields, chunk)
    stored = ContentProvenance.model_validate(json.loads(fields["payload"])["content_provenance"])
    assert stored.preview == "# Metadata user content"
    assert stored.part == entity.airweave_system_metadata.content_provenance.part
    assert entity.airweave_system_metadata.content_provenance.preview is None
    with pytest.raises(ValueError, match="source offsets"):
        processor._multiply_entities([entity], [[{"text": text}]], MagicMock())


@pytest.mark.parametrize("kind", ["semantic", "code"])
def test_fallback_offsets_are_relative_to_full_source(kind):
    cls = SemanticChunker if kind == "semantic" else CodeChunker
    chunker = object.__new__(cls)
    chunker._token_chunker = MagicMock()
    chunker._token_chunker.chunk_batch.return_value = [
        [
            SimpleNamespace(text="abc", start_index=0, end_index=3, token_count=3),
            SimpleNamespace(text="def", start_index=3, end_index=6, token_count=3),
        ]
    ]
    original = SimpleNamespace(
        text="abcdef", start_index=40, end_index=46, token_count=cls.MAX_TOKENS_PER_CHUNK + 1
    )
    (chunks,) = chunker._apply_safety_net_batched([[original]])
    assert [(x["start_index"], x["end_index"]) for x in chunks] == [(40, 43), (43, 46)]
