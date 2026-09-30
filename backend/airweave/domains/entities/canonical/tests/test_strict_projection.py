"""Failure injection at conversion, chunking, embedding and remote-feed boundaries."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from airweave.domains.converters.fakes.registry import FakeConverterRegistry
from airweave.domains.sync_pipeline.exceptions import EntityProcessingError
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.platform.destinations.vespa.destination import VespaDestination
from airweave.platform.entities._base import AirweaveSystemMetadata
from airweave.platform.entities.slack import SlackMessageEntity


@pytest.mark.parametrize("stage", ["conversion", "chunking", "embedding"])
async def test_strict_processor_rejects_dropped_required_content(stage):
    processor = ChunkEmbedProcessor(FakeConverterRegistry(), MagicMock(), MagicMock())
    entity = SlackMessageEntity.from_api({"ts": "1.0", "text": "Required content"}, breadcrumbs=[])
    entity.entity_id = "part"
    entity.airweave_system_metadata = AirweaveSystemMetadata()
    chunk = entity.model_copy(deep=True)
    chunk.entity_id = "part__chunk_0"
    chunk.airweave_system_metadata.original_entity_id = "part"
    entity.textual_representation = "Required content"
    processor._text_builder.build_for_batch = AsyncMock(
        return_value=[] if stage == "conversion" else [entity]
    )
    processor._chunk_entities = AsyncMock(return_value=[] if stage == "chunking" else [chunk])
    processor._embed_entities = AsyncMock(return_value=[] if stage == "embedding" else [chunk])
    with pytest.raises(EntityProcessingError, match="dropped required"):
        await processor.process(
            [entity],
            SimpleNamespace(logger=MagicMock()),
            SimpleNamespace(entity_tracker=AsyncMock()),
            strict=True,
        )


async def test_strict_destination_cannot_ack_dropped_transformations():
    destination = VespaDestination()
    destination._client = AsyncMock()
    destination._transformer = MagicMock()
    destination._transformer.transform_batch.return_value = {}
    with pytest.raises(RuntimeError, match="dropped required"):
        await destination.bulk_insert([MagicMock()], strict=True)
    destination._client.feed_documents.assert_not_called()


async def test_strict_delete_retries_partial_http_failure_and_accepts_absence():
    from airweave.platform.destinations.vespa.client import VespaClient

    client = VespaClient(MagicMock())
    transport = MagicMock()
    transport.delete = AsyncMock(
        side_effect=[
            SimpleNamespace(status_code=200),
            SimpleNamespace(status_code=503),
        ]
    )
    documents = [("base_entity", "first"), ("base_entity", "second")]
    with pytest.raises(RuntimeError, match="deletion incomplete"):
        await client._delete_by_doc_ids(documents, transport, strict=True)
    transport.delete = AsyncMock(
        side_effect=[
            SimpleNamespace(status_code=404),
            SimpleNamespace(status_code=200),
        ]
    )
    assert await client._delete_by_doc_ids(documents, transport, strict=True) == 2
