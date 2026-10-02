"""Real PDF conversion/SQL/blob reads; fixed chunking/embedding, no provider calls."""

import hashlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import fitz
import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select, update

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.api import deps
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.db.session import get_db
from airweave.domains.converters.registry import ConverterRegistry
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.entities.canonical.projection_gc import ProjectionGCStore
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.entities.canonical.text_models import TextArtifact
from airweave.domains.entities.canonical.text_query import CanonicalTextReader
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.sync import Sync
from airweave.platform.destinations.vespa.transformer import EntityTransformer


@pytest.fixture
async def publication(database, source, tmp_path):
    service, fence = source
    binding = await bind_projection(database, fence, "google_drive")
    storage = FilesystemBackend(tmp_path)
    pdf = fitz.open()
    for index in range(3):
        page = pdf.new_page()
        page.insert_textbox(
            fitz.Rect(40, 40, 560, 780),
            (
                f"Page content {index}. "
                + "Retained complete document words. " * 150
                + "\n# Content\nThis marker belongs to the source, not our metadata."
            ),
            fontsize=9,
        )
    raw = pdf.tobytes()
    pdf.close()
    digest = hashlib.sha256(raw).hexdigest()
    blob = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(raw),
        media_type="application/pdf",
    )
    await storage.write_file(blob.key, raw)
    parent = observation(
        identity=RecordIdentity(record_type="folder", native_id="parent"),
        payload={"id": "parent", "name": "Folder"},
    )
    original = observation(
        identity=RecordIdentity(record_type="file", native_id="pdf"),
        parent=parent.identity,
        payload={"id": "pdf", "name": "Complete.pdf", "mimeType": "application/pdf"},
        blobs=(blob,),
    )
    await capture(database, service, fence, parent, original)
    store = CanonicalProjectionStore()
    async with database() as db:
        work = next(
            item
            for item in await store.pending(db, fence.organization_id, fence.sync_id)
            if item.record.identity.record_type == "file"
        )
    registry = ConverterRegistry()
    converter = registry.for_extension(".pdf")
    converter.convert_batch = AsyncMock(wraps=converter.convert_batch)
    processor = ChunkEmbedProcessor(registry, FakeDenseEmbedder(), FakeSparseEmbedder())

    async def fixed_chunks(entities, context, runtime):
        return processor._multiply_entities(
            entities, [[{"text": entity.textual_representation}] for entity in entities], context
        )

    processor._chunk_entities = fixed_chunks
    destination = MagicMock(collection_id=binding.collection_id, feed_prepared=AsyncMock())
    destination.prepare_documents = lambda chunks: {
        "base_entity": [
            EntityTransformer(collection_id=destination.collection_id).transform(chunk)
            for chunk in chunks
        ]
    }
    records = CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test-key")
    reader = CanonicalTextReader(records, storage)
    projector = CanonicalProjector(store, database, processor, storage)
    return SimpleNamespace(
        service=service,
        fence=fence,
        storage=storage,
        work=work,
        reader=reader,
        projector=projector,
        destination=destination,
        converter=converter,
        original=original,
        parent=parent,
        records=records,
        blob=blob,
        raw=raw,
    )


async def test_pdf_full_content_read_is_single_conversion_and_published_with_index(
    database, publication
):
    p = publication

    async def feed(documents):
        async with database() as db:
            manifest = await db.scalar(
                select(ProjectionGeneration).where(
                    ProjectionGeneration.record_id == p.work.record.id
                )
            )
            assert manifest.text_representations
            listed = await p.reader.list(
                db, p.fence.organization_id, p.fence.sync_id, p.work.record.id, 1
            )
            assert listed.status == "unavailable"  # bytes exist but publication has not happened

    p.destination.feed_prepared.side_effect = feed
    assert await p.projector.project_one(p.work, "google_drive", p.destination, MagicMock())
    p.converter.convert_batch.assert_awaited_once()
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_context] = lambda: SimpleNamespace(
        organization=SimpleNamespace(id=p.fence.organization_id)
    )
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(
        storage_backend=p.storage
    )
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: p.records
    base = f"/sync/{p.fence.sync_id}/records/{p.work.record.id}/text-representations"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        listed = await client.get(base, params={"revision": 1})
        assert listed.status_code == 200
        descriptor = listed.json()["representations"][0]
        assert (
            descriptor["kind"] == "extracted_text" and descriptor["source_anchors"] == "unavailable"
        )
        params = {"revision": 1, "generation": descriptor["generation"], "limit": 100000}
        result = await client.get(f"{base}/{descriptor['id']}", params=params)
        assert result.status_code == 200
        text = result.json()["text"]
        assert len(text) > 1000 and "Page content 2" in text and "marker belongs" in text
        assert not text.startswith("# Metadata")
        assert result.json()["next_offset"] is None
        index = await client.get(f"{base}/{descriptor['id']}", params={**params, "view": "index"})
        assert index.json()["text"].startswith("# Metadata")
        assert index.json()["text"].endswith(text)
        await capture(
            database,
            p.service,
            p.fence,
            p.parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
        )
        denied = await client.get(f"{base}/{descriptor['id']}", params=params)
        assert denied.status_code == 404
    assert await p.storage.read_file(p.blob.key) == p.raw


async def test_blob_corruption_and_midread_revision_change_fail_closed(database, publication):
    from airweave.domains.entities.canonical.query import BlobUnavailable, StaleRecordRevision

    p = publication
    assert await p.projector.project_one(p.work, "google_drive", p.destination, MagicMock())
    async with database() as db:
        descriptor = (
            await p.reader.list(db, p.fence.organization_id, p.fence.sync_id, p.work.record.id, 1)
        ).representations[0]
        row = await db.get(ProjectionGeneration, descriptor.generation)
        artifact = TextArtifact.model_validate(row.text_representations[0])
    key = artifact.storage_key(p.fence.sync_id, descriptor.generation)
    original_bytes = await p.storage.read_file(key)
    await p.storage.write_file(key, b"corrupt")
    args = (
        p.fence.organization_id,
        p.fence.sync_id,
        p.work.record.id,
        1,
        descriptor.generation,
        descriptor.id,
    )
    async with database() as db:
        with pytest.raises(BlobUnavailable):
            await p.reader.read(db, *args)
    await p.storage.write_file(key, original_bytes)
    read_file = p.storage.read_file

    async def pipeline_changes(path, **kwargs):
        content = await read_file(path, **kwargs)
        async with database() as db:
            await db.execute(
                update(Sync).where(Sync.id == p.fence.sync_id).values(index_pipeline_version=2)
            )
            await db.commit()
        return content

    from airweave.domains.entities.canonical.text_query import TextUnavailable

    p.storage.read_file = pipeline_changes
    async with database() as db:
        with pytest.raises(TextUnavailable):
            await p.reader.read(db, *args)
    async with database() as db:
        await db.execute(
            update(Sync).where(Sync.id == p.fence.sync_id).values(index_pipeline_version=1)
        )
        await db.commit()

    async def changing_read(path, **kwargs):
        content = await read_file(path, **kwargs)
        await capture(
            database,
            p.service,
            p.fence,
            p.original.model_copy(
                update={"payload": {**p.original.payload, "name": "Changed.pdf"}}
            ),
        )
        return content

    p.storage.read_file = changing_read
    async with database() as db:
        with pytest.raises(StaleRecordRevision):
            await p.reader.read(db, *args)


async def test_abandoned_generation_text_is_in_bounded_repeatable_gc(database, publication):
    p = publication
    p.destination.feed_prepared.side_effect = RuntimeError("feed unavailable")
    with pytest.raises(RuntimeError):
        await p.projector.project_one(p.work, "google_drive", p.destination, MagicMock())
    async with database() as db:
        row = await db.scalar(
            select(ProjectionGeneration).where(ProjectionGeneration.record_id == p.work.record.id)
        )
        generation = row.id
    now = datetime.now(timezone.utc) + timedelta(hours=2)
    gc = ProjectionGCStore()
    async with database() as db:
        first = await gc.claim(db, generation, now=now, limit=1)
        assert len(first.documents) == 1 and not first.artifact_keys
        await gc.acknowledge(db, first, now=now)
    async with database() as db:
        second = await gc.claim(db, generation, now=now, limit=1)
        assert not second.documents and len(second.artifact_keys) == 1
        await gc.acknowledge(db, second, now=now, error="StorageUnavailable")
    async with database() as db:
        retry = await gc.claim(db, generation, now=now + timedelta(minutes=6), limit=1)
        assert retry.artifact_keys == second.artifact_keys
        for key in retry.artifact_keys:
            retained = await p.storage.read_file(key)
            await p.storage.delete_file(key)
        await gc.acknowledge(db, retry, now=now + timedelta(minutes=6))
    # A timed-out writer can finish after deletion; recurring cleanup retains exact keys.
    await p.storage.write_file(retry.artifact_keys[0], retained)
    async with database() as db:
        repeated = await gc.claim(db, generation, now=now + timedelta(hours=1), limit=100)
        assert repeated.artifact_keys == retry.artifact_keys
        for key in repeated.artifact_keys:
            await p.storage.delete_file(key)
        await gc.acknowledge(db, repeated, now=now + timedelta(hours=1))
    assert await p.storage.read_file(p.blob.key) == p.raw


async def test_source_withdrawal_during_retained_text_io_denies_text(database, publication):
    """Source revocation fences derived text even when publication and bytes remain."""
    from airweave.domains.entities.canonical.query import RecordNotFound
    from airweave.models.source_connection import SourceConnection

    p = publication
    assert await p.projector.project_one(p.work, "google_drive", p.destination, MagicMock())
    async with database() as db:
        descriptor = (
            await p.reader.list(db, p.fence.organization_id, p.fence.sync_id, p.work.record.id, 1)
        ).representations[0]
    read_file = p.storage.read_file

    async def withdraw_after_storage(path, **kwargs):
        content = await read_file(path, **kwargs)
        async with database() as db:
            await db.execute(
                update(SourceConnection)
                .where(SourceConnection.sync_id == p.fence.sync_id)
                .values(is_authenticated=False)
            )
            await db.commit()
        return content

    p.storage.read_file = withdraw_after_storage
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await p.reader.read(
                db,
                p.fence.organization_id,
                p.fence.sync_id,
                p.work.record.id,
                1,
                descriptor.generation,
                descriptor.id,
            )
    async with database() as db:
        with pytest.raises(RecordNotFound):
            await p.reader.list(db, p.fence.organization_id, p.fence.sync_id, p.work.record.id, 1)
