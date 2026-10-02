"""Real SQL and conversion; synthetic originals, embeddings and remote feed."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import fitz
import pytest
from sqlalchemy import select, update

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.core.logging import logger
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.entities.canonical.extraction_models import (
    ExtractionCoverage,
    ExtractionOutcome,
)
from airweave.domains.entities.canonical.projection_models import ProjectionLocator
from airweave.domains.entities.canonical.projection_store import (
    CanonicalProjectionStore,
    current_extraction,
    publication_matches,
)
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.entities.canonical.tests.test_gmail_projection import part
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models import Entity, Sync
from airweave.models.projection_generation import ProjectionGeneration
from airweave.platform.destinations.vespa.transformer import EntityTransformer


def original(*attachments, completeness="complete"):
    return observation(
        identity=RecordIdentity(record_type="message", native_id="m1"),
        completeness=completeness,
        payload={
            "id": "m1",
            "threadId": "t1",
            "internalDate": "1000",
            "payload": {
                "mimeType": "multipart/mixed",
                "parts": [
                    part(b"Intact fundraising email body with useful context."),
                    *attachments,
                ],
            },
        },
    )


def destination(collection_id):
    result = MagicMock(collection_id=collection_id, feed_prepared=AsyncMock())
    result.prepare_documents = lambda chunks: {
        "base_entity": [
            EntityTransformer(collection_id=result.collection_id).transform(c) for c in chunks
        ]
    }
    return result


def projector(database, storage):
    return CanonicalProjector(
        CanonicalProjectionStore(),
        database,
        ChunkEmbedProcessor(ConverterRegistry(), FakeDenseEmbedder(), FakeSparseEmbedder()),
        storage,
    )


async def test_message_pdf_video_and_missing_part_publish_truthful_coverage(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text(
        (40, 40), "Retained PDF investment discussion and detailed fundraising plans. " * 4
    )
    data = pdf.tobytes()
    pdf.close()
    item = original(
        part(data, mime="application/pdf", filename="deck.pdf"),
        part(b"synthetic video bytes", mime="video/mp4", filename="meeting.mp4"),
        {
            "mimeType": "application/pdf",
            "filename": "large.pdf",
            "body": {"attachmentId": "missing", "size": 500000000},
        },
        completeness="partial",
    )
    item.payload["payload"]["parts"][0] = part(
        "Intact fundraising email body: नमस्ते — café".encode(),
        headers=[{"name": "Content-Type", "value": "text/plain; charset=gb2312"}],
    )
    await capture(database, service, fence, item)
    store = CanonicalProjectionStore()
    target = destination(binding.collection_id)
    result = await projector(database, FilesystemBackend(tmp_path)).batch(
        fence.organization_id, fence.sync_id, "gmail", target, logger
    )
    assert result.published == 1 and result.failed == 0
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        coverage = await current_extraction(
            db, fence.organization_id, fence.sync_id, row.id, row.record_revision
        )
        assert coverage.status == "partial"
        assert [p.outcome for p in coverage.parts] == [
            "indexed",
            "indexed",
            "unsupported",
            "unavailable_original",
        ]
        assert coverage.parts[1].key == "/payload/parts/1"
        recovery = coverage.parts[0].charset_recoveries[0]
        assert recovery.source_path == "/payload/parts/0"
        assert recovery.from_charset == "gb2312" and recovery.to_charset == "utf-8"
        assert (
            coverage.parts[1].kind == "file" and coverage.parts[1].media_type == "application/pdf"
        )
        assert row.completeness == "partial" and row.source_payload == item.payload
        assert not await store.pending(db, fence.organization_id, fence.sync_id)
        locator = ProjectionLocator(
            record_id=row.id,
            revision=row.record_revision,
            pipeline_version=row.indexed_pipeline_version,
            generation=row.indexed_generation,
            part_index=1,
        )
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(locator)))
            == row.id
        )
        assert (
            await db.scalar(
                select(Entity.id)
                .join(Sync)
                .where(publication_matches(locator.model_copy(update={"part_index": 2})))
            )
            is None
        )
        ctx = MagicMock()
        ctx.organization.id = fence.organization_id
        counts = await OwnedSearchService._coverage(
            db, ctx, OwnedSearchRequest(query="fundraising", sync_ids=(fence.sync_id,))
        )
        assert counts[0].partially_indexed_records == 1 and counts[0].pending_records == 0
    # New native revision withdraws every old part and its coverage before new extraction.
    await capture(
        database, service, fence, original(part(b"new video", mime="video/mp4", filename="new.mp4"))
    )
    async with database() as db:
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(locator)))
            is None
        )
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, row.id, row.record_revision
            )
            is None
        )
    assert (
        await projector(database, FilesystemBackend(tmp_path)).batch(
            fence.organization_id, fence.sync_id, "gmail", target, logger
        )
    ).published == 1
    async with database() as db:
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(locator)))
            is None
        )
        old = await db.get(ProjectionGeneration, locator.generation)
        assert old.retired_at is not None


async def test_supported_attachment_conversion_failure_and_feed_failure_stay_pending(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    await capture(
        database,
        service,
        fence,
        original(part(b"broken binary\x00", mime="application/pdf", filename="broken.pdf")),
    )
    target = destination(binding.collection_id)
    project = projector(database, FilesystemBackend(tmp_path))
    assert (
        await project.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    ).failed == 1
    target.feed_prepared.assert_not_awaited()
    await capture(
        database, service, fence, original(part(b"video", mime="video/mp4", filename="clip.mp4"))
    )
    target.feed_prepared.side_effect = ConnectionError("synthetic interruption")
    assert (
        await project.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    ).failed == 1
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        assert row.indexed_generation is None
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, row.id, row.record_revision
            )
            is None
        )
        assert (
            len(await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id))
            == 1
        )
    target.feed_prepared.side_effect = None
    assert (
        await project.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    ).published == 1


async def test_unsupported_only_publication_is_explicit_and_retries_on_pipeline_upgrade(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "google_drive")
    from airweave.domains.entities.canonical.tests.test_gmail_projection import blob, record

    captured = record({}, sync_id=fence.sync_id)
    ref = blob(captured, b"video bytes")
    storage = FilesystemBackend(tmp_path)
    await storage.write_file(ref.key, b"video bytes")
    await capture(
        database,
        service,
        fence,
        observation(
            identity=RecordIdentity(record_type="file", native_id="video"),
            payload={"id": "video", "name": "movie.mp4", "mimeType": "video/mp4"},
            blobs=(ref,),
        ),
    )
    target = destination(binding.collection_id)
    assert (
        await projector(database, storage).batch(
            fence.organization_id, fence.sync_id, "google_drive", target, logger
        )
    ).published == 1
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        assert row.indexed_chunk_count == 0 and row.completeness == "complete"
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, row.id, row.record_revision
            )
        ).status == "unavailable"
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=2)
        )
        await db.commit()
        assert (
            len(await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id))
            == 1
        )
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, row.id, row.record_revision
            )
            is None
        )


def test_coverage_rejects_missing_duplicate_and_contradictory_parts():
    one = ExtractionOutcome(part_index=0, key="/body", kind="body", outcome="indexed")
    for parts in [(one, one), (one.model_copy(update={"part_index": 1}),)]:
        with pytest.raises(ValueError):
            ExtractionCoverage(parts=parts)
    with pytest.raises(ValueError):
        ExtractionOutcome(part_index=0, key="a", kind="file", outcome="unsupported", reason=None)


async def test_coverage_manifest_is_exact_immutable_and_capture_fences_publication(
    database, source
):
    from airweave.domains.entities.canonical.projection_models import (
        ProjectionDocument,
        scope_projection_document_id,
    )

    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    await capture(database, service, fence, original())
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    generation, collection = uuid4(), binding.collection_id
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=work.record.revision,
        pipeline_version=work.pipeline_version,
        generation=generation,
        part_index=0,
    )
    documents = (
        ProjectionDocument(
            schema_name="base_entity",
            document_id=scope_projection_document_id(
                fence.sync_id, collection, f"Entity_{locator.encode()}__chunk_0"
            ),
        ),
    )
    indexed = ExtractionOutcome(part_index=0, key="/body", kind="body", outcome="indexed")
    async with database() as db:
        with pytest.raises(ValueError, match="exactly"):
            await store.prepare(
                db, work, generation, collection, documents, coverage=ExtractionCoverage(parts=())
            )
        assert await store.prepare(
            db,
            work,
            generation,
            collection,
            documents,
            coverage=ExtractionCoverage(parts=(indexed,)),
        )
        with pytest.raises(ValueError, match="immutable"):
            await store.prepare(
                db,
                work,
                generation,
                collection,
                documents,
                coverage=ExtractionCoverage(
                    parts=(indexed.model_copy(update={"key": "different"}),)
                ),
            )
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, work.record.id, work.record.revision
            )
            is None
        )
    await capture(
        database, service, fence, original(part(b"video", mime="video/mp4", filename="clip.mp4"))
    )
    async with database() as db:
        assert not await store.publish(db, work, generation, 1)
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, work.record.id, work.record.revision
            )
            is None
        )


async def test_inline_image_without_ocr_keeps_body_partial_but_converter_failure_is_fatal(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    item = original(part(b"retained image bytes", mime="image/png", filename="inline.png"))
    item.payload["payload"]["mimeType"] = "multipart/related"
    await capture(database, service, fence, item)
    storage = FilesystemBackend(tmp_path)
    target = destination(binding.collection_id)
    result = await projector(database, storage).batch(
        fence.organization_id, fence.sync_id, "gmail", target, logger
    )
    assert result.published == 1 and result.failed == 0
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        coverage = await current_extraction(
            db, fence.organization_id, fence.sync_id, row.id, row.record_revision
        )
        assert coverage.status == "partial"
        assert [p.outcome for p in coverage.parts] == ["indexed", "unsupported"]
        assert coverage.parts[1].media_type == "image/png"
        assert row.indexed_chunk_count > 0
        assert row.source_payload == item.payload
        # Enabling OCR changes the pipeline; an actual converter failure must not
        # masquerade as an unsupported-format success or preserve stale coverage.
        await db.execute(
            update(Sync).where(Sync.id == fence.sync_id).values(index_pipeline_version=2)
        )
        await db.commit()
    ocr = MagicMock(convert_batch=AsyncMock(side_effect=lambda paths: dict.fromkeys(paths)))
    configured = CanonicalProjector(
        CanonicalProjectionStore(),
        database,
        ChunkEmbedProcessor(ConverterRegistry(ocr), FakeDenseEmbedder(), FakeSparseEmbedder()),
        storage,
    )
    target.feed_prepared.reset_mock()
    failed = await configured.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    assert failed.failed == 1 and failed.published == 0
    ocr.convert_batch.assert_awaited_once()
    target.feed_prepared.assert_not_awaited()
    async with database() as db:
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, row.id, row.record_revision
            )
            is None
        )


async def test_drive_metadata_only_reports_missing_original_then_new_bytes_are_indexed(
    database, source, tmp_path
):
    from airweave.domains.entities.canonical.tests.test_gmail_projection import blob, record

    service, fence = source
    binding = await bind_projection(database, fence, "google_drive")
    identity = RecordIdentity(record_type="file", native_id="retained-file")
    payload = {"id": identity.native_id, "name": "Notes.txt", "mimeType": "text/plain"}
    metadata = observation(identity=identity, payload=payload, completeness="metadata_only")
    await capture(database, service, fence, metadata)
    storage = FilesystemBackend(tmp_path)
    target = destination(binding.collection_id)
    project = projector(database, storage)
    assert (
        await project.batch(fence.organization_id, fence.sync_id, "google_drive", target, logger)
    ).published == 1
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        coverage = await current_extraction(
            db, fence.organization_id, fence.sync_id, row.id, row.record_revision
        )
        assert coverage.status == "unavailable" and row.indexed_chunk_count == 0
        assert coverage.parts[0].outcome == "unavailable_original"
        assert coverage.parts[0].reason == "original_not_captured"
    content = b"Recovered original text contains the complete available document."
    ref = blob(record({}, sync_id=fence.sync_id), content).model_copy(
        update={"media_type": "text/plain"}
    )
    await storage.write_file(ref.key, content)
    await capture(
        database, service, fence, observation(identity=identity, payload=payload, blobs=(ref,))
    )
    assert (
        await project.batch(fence.organization_id, fence.sync_id, "google_drive", target, logger)
    ).published == 1
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        assert row.indexed_chunk_count > 0
        previous = ProjectionLocator(
            record_id=row.id,
            revision=row.record_revision,
            pipeline_version=row.indexed_pipeline_version,
            generation=row.indexed_generation,
            part_index=0,
        )
    # Losing retained body availability withdraws the prior text generation.
    await capture(database, service, fence, metadata)
    assert (
        await project.batch(fence.organization_id, fence.sync_id, "google_drive", target, logger)
    ).published == 1
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        assert row.indexed_chunk_count == 0
        assert (
            await db.scalar(select(Entity.id).join(Sync).where(publication_matches(previous)))
            is None
        )
        assert (
            await current_extraction(
                db, fence.organization_id, fence.sync_id, row.id, row.record_revision
            )
        ).status == "unavailable"
