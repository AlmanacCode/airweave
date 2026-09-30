"""OCR-dependent bytes remain captured and pending instead of vanishing from search."""

import hashlib
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock

import fitz
from PIL import Image
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.core.logging import logger
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models import Entity


async def test_scanned_pdf_without_ocr_stays_pending_with_owned_bytes(database, source, tmp_path):
    """Use real PDF conversion and SQL; no mocked conversion or tracker behavior."""
    image = BytesIO()
    Image.new("RGB", (100, 100), color="blue").save(image, format="PNG")
    document = fitz.open()
    page = document.new_page()
    page.insert_image(page.rect, stream=image.getvalue())
    content = document.tobytes()
    document.close()
    service, fence = source
    digest = hashlib.sha256(content).hexdigest()
    blob = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="application/pdf",
    )
    storage = FilesystemBackend(tmp_path)
    await storage.write_file(blob.key, content)
    await capture(
        database,
        service,
        fence,
        observation(
            identity=RecordIdentity(record_type="file", native_id="scanned"),
            payload={"id": "scanned", "name": "scan.pdf", "mimeType": "application/pdf"},
            blobs=(blob,),
        ),
    )
    destination = MagicMock(feed_prepared=AsyncMock())
    store = CanonicalProjectionStore()
    projector = CanonicalProjector(
        store,
        database,
        ChunkEmbedProcessor(
            ConverterRegistry(ocr_provider=None), FakeDenseEmbedder(), FakeSparseEmbedder()
        ),
        storage,
    )
    result = await projector.batch(
        fence.organization_id, fence.sync_id, "google_drive", destination, logger
    )
    assert result.failed == 1 and result.published == 0
    destination.prepare_documents.assert_not_called()
    destination.feed_prepared.assert_not_awaited()
    async with database() as db:
        pending = await store.pending(db, fence.organization_id, fence.sync_id)
        assert len(pending) == 1
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        assert row.projection_error is not None and row.indexed_generation is None
        record = await service.store.read(db, fence.organization_id, fence.sync_id, row.id)
        assert record.completeness == "complete" and record.content_access == "available"
        assert record.blobs == (blob,) and record.payload["id"] == "scanned"
    assert await storage.read_file(blob.key, max_bytes=blob.size_bytes) == content
