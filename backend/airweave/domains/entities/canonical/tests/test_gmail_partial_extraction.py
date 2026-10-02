"""Real SQL authority and publication; synthetic originals/converter outages/embeddings."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import fitz
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.core.logging import logger
from airweave.domains.converters._base import ConversionResult
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.entities.canonical.mail_models import MailFilters
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.query import CanonicalQueryService, SourceNotFound
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture
from airweave.domains.entities.canonical.tests.test_extraction_coverage import destination
from airweave.domains.entities.canonical.tests.test_gmail_projection import part
from airweave.domains.entities.canonical.tests.test_http import query_app
from airweave.domains.entities.canonical.tests.test_mail_query import message, read
from airweave.domains.entities.canonical.text_query import CanonicalTextReader
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.sync_pipeline.exceptions import EntityProcessingError, SyncFailureError
from airweave.domains.sync_pipeline.pipeline.text_models import BuiltTextBatch
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models import Entity
from airweave.models.source_connection import SourceConnection


def attached(*parts):
    item = message("one")
    payload = item.payload["payload"]
    body = {"mimeType": payload["mimeType"], "body": payload.pop("body")}
    payload.update(mimeType="multipart/mixed", parts=[body, *parts])
    return item


def pipeline(database, tmp_path, registry):
    processor = ChunkEmbedProcessor(registry, FakeDenseEmbedder(), FakeSparseEmbedder())
    return CanonicalProjector(
        CanonicalProjectionStore(), database, processor, FilesystemBackend(tmp_path)
    )


def with_pdf_converter(converter):
    real = ConverterRegistry()
    registry = MagicMock(spec=ConverterRegistry)
    registry.for_extension.side_effect = lambda ext: (
        converter if ext == ".pdf" else real.for_extension(ext)
    )
    return registry


async def test_body_query_and_original_thread_read_work_before_attachment_finishes(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    item = attached(part(b"unavailable PDF", mime="application/pdf", filename="deck.pdf"))
    await capture(database, service, fence, item)
    entered, release = asyncio.Event(), asyncio.Event()

    async def unavailable(paths):
        entered.set()
        await release.wait()
        return dict.fromkeys(paths, ConversionResult(text=None))

    pdf = MagicMock(convert_batch=AsyncMock(side_effect=unavailable))
    project = pipeline(database, tmp_path, with_pdf_converter(pdf))
    target = destination(binding.collection_id)
    task = asyncio.create_task(
        project.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        page = await read(database, fence, filters=MailFilters(query="retained body boundary"))
        assert len(page.messages) == 1 and page.indexing.text_ready == 1
        async with database() as db:
            row = await db.get(Entity, page.messages[0].id)
            assert row.indexed_generation is None
        target.feed_prepared.assert_not_awaited()

        async def context():
            return SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id))

        async with AsyncClient(
            transport=ASGITransport(app=query_app(database, context)),
            base_url="http://test",
        ) as client:
            response = await client.get(f"/sync/{fence.sync_id}/mail/threads/old-and-new-thread")
        assert response.status_code == 200
        assert response.json()["messages"][0]["payload"] == item.payload
    finally:
        release.set()
        result = await task
    assert result.published == 1 and result.failed == 1


async def test_partial_retry_cannot_drop_a_good_part_and_recovers_without_recapture(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((40, 40), "Successful retained attachment investment discussion. " * 3)
    good = pdf.tobytes(deflate=True)
    pdf.close()
    # Invalid native bytes genuinely fail conversion; a valid short text layer
    # now remains useful partial text when OCR is unavailable.
    needs_ocr = b"\x00\xff damaged PDF bytes"
    item = attached(
        part(good, mime="application/pdf", filename="good.pdf"),
        part(needs_ocr, mime="application/pdf", filename="scan.pdf"),
        part(b"unsupported GIF", mime="image/gif", filename="animation.gif"),
    )
    await capture(database, service, fence, item)
    ocr = MagicMock(convert_batch=AsyncMock(side_effect=lambda paths: dict.fromkeys(paths)))
    project = pipeline(database, tmp_path, ConverterRegistry(ocr))
    target = destination(binding.collection_id)
    result = await project.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    assert result.published == 1 and result.failed == 1
    store = CanonicalProjectionStore()
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        generation, revision, payload = (
            row.indexed_generation,
            row.record_revision,
            row.source_payload,
        )
        assert row.projection_error == "conversion_failed"
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
        assert not await store.pending(db, fence.organization_id, fence.sync_id, skip_failed=True)
        assert not (await store.pending_sources(db, ("gmail",))).sources
        counts = await OwnedSearchService._coverage(
            db,
            SimpleNamespace(organization=SimpleNamespace(id=fence.organization_id)),
            OwnedSearchRequest(query="retained", sync_ids=(fence.sync_id,)),
        )
        assert counts[0].partially_indexed_records == 1 and counts[0].pending_records == 0
        reader = CanonicalTextReader(
            CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test-key"),
            FilesystemBackend(tmp_path),
        )
        descriptors = await reader.list(db, fence.organization_id, fence.sync_id, row.id, revision)
        assert len(descriptors.representations) == 2
        body = next(p for p in descriptors.representations if p.part_key == "/payload/body")
        text = await reader.read(
            db,
            fence.organization_id,
            fence.sync_id,
            row.id,
            revision,
            generation,
            body.id,
        )
        assert text.text == "Full retained body boundary canary"
    # A transient failure of the formerly successful PDF cannot downgrade current text/search.
    unavailable = MagicMock(
        convert_batch=AsyncMock(
            side_effect=lambda paths: dict.fromkeys(paths, ConversionResult(text=None))
        )
    )
    degraded = pipeline(database, tmp_path, with_pdf_converter(unavailable))
    result = await degraded.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    assert result.published == 0 and result.failed == 1
    async with database() as db:
        row = await db.get(Entity, work.record.id)
        assert row.indexed_generation == generation and row.source_payload == payload
    assert (
        await read(database, fence, filters=MailFilters(query="retained body boundary"))
    ).messages
    ocr.convert_batch.side_effect = lambda paths: dict.fromkeys(paths, "Recovered attachment text")
    result = await project.batch(fence.organization_id, fence.sync_id, "gmail", target, logger)
    assert result.published == 1 and result.failed == 0
    async with database() as db:
        from airweave.domains.entities.canonical.projection_store import current_extraction

        row = await db.get(Entity, work.record.id)
        assert row.record_revision == revision and row.source_payload == payload
        assert row.indexed_generation != generation and row.projection_error is None
        coverage = await current_extraction(
            db, fence.organization_id, fence.sync_id, row.id, revision
        )
        assert [p.outcome for p in coverage.parts] == [
            "indexed",
            "indexed",
            "indexed",
            "unsupported",
        ]
        assert coverage.status == "partial"  # Unsupported GIF is not a retryable converter failure.
        assert not await store.pending(db, fence.organization_id, fence.sync_id)


@pytest.mark.parametrize(
    "error", [SyncFailureError("Infrastructure failed"), RuntimeError("Bug"), None]
)
async def test_attachment_exceptions_are_not_published_as_recognized_content_failures(
    database, source, tmp_path, error
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    await capture(
        database,
        service,
        fence,
        attached(part(b"PDF", mime="application/pdf", filename="deck.pdf")),
    )
    converter = MagicMock(convert_batch=AsyncMock(side_effect=error, return_value={}))
    project = pipeline(database, tmp_path, with_pdf_converter(converter))
    async with database() as db:
        work = (await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id))[
            0
        ]
    target = destination(binding.collection_id)
    with pytest.raises(type(error) if error is not None else EntityProcessingError):
        await project.project_one(work, "gmail", target, logger)
    target.feed_prepared.assert_not_awaited()
    assert (
        await read(database, fence, filters=MailFilters(query="retained body boundary"))
    ).messages


async def test_unexplained_conversion_omission_does_not_become_successful_partial(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    await capture(
        database,
        service,
        fence,
        attached(part(b"PDF", mime="application/pdf", filename="deck.pdf")),
    )
    project = pipeline(database, tmp_path, ConverterRegistry())
    real_build = project._processor.build_text
    calls = 0

    async def omit(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            return BuiltTextBatch(entities=[], representations=())
        return await real_build(*args, **kwargs)

    project._processor.build_text = omit
    result = await project.batch(
        fence.organization_id, fence.sync_id, "gmail", destination(binding.collection_id), logger
    )
    assert result.published == 0 and result.failed == 1


async def test_source_withdrawal_during_partial_feed_denies_body_and_publication(
    database, source, tmp_path
):
    service, fence = source
    binding = await bind_projection(database, fence, "gmail")
    await capture(
        database,
        service,
        fence,
        attached(part(b"bad PDF\x00", mime="application/pdf", filename="deck.pdf")),
    )
    target = destination(binding.collection_id)

    async def withdraw(_):
        assert (
            await read(database, fence, filters=MailFilters(query="retained body boundary"))
        ).messages
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == fence.sync_id)
                .values(is_authenticated=False)
            )
            await db.commit()

    target.feed_prepared.side_effect = withdraw
    result = await pipeline(database, tmp_path, ConverterRegistry()).batch(
        fence.organization_id, fence.sync_id, "gmail", target, logger
    )
    assert result.published == 0 and result.failed == 0 and result.superseded == 1
    with pytest.raises(SourceNotFound):
        await read(database, fence, filters=MailFilters(query="retained body boundary"))
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        assert row.indexed_generation is None
