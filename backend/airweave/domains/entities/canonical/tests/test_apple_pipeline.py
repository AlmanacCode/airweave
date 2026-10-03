"""Actual device admission→conversion→Vespa→owned retrieval; fixed vectors, not quality."""

import base64
import copy
import gzip
import hashlib
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import select
from vespa.application import Vespa

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.core.logging import logger
from airweave.domains.converters.txt import TxtConverter
from airweave.domains.device_ingestion.models import (
    BindDevice,
    CommitDevicePage,
    DeviceObservation,
    DevicePrincipal,
    DeviceUploadIntent,
    EnsureDeviceSource,
    RevokeDevice,
)
from airweave.domains.device_ingestion.service import DeviceIngestion
from airweave.domains.device_ingestion.store import DeviceIngestionStore
from airweave.domains.embedders.fakes.embedder import FakeDenseEmbedder, FakeSparseEmbedder
from airweave.domains.embedders.types import DenseEmbedding, SparseEmbedding
from airweave.domains.entities.canonical.actors import ActorAnyOf, ActorFilter
from airweave.domains.entities.canonical.projection_models import ProjectionDocument
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.query import CanonicalQueryService, RecordNotFound
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.search.adapters.vector_db.filter_translator import FilterTranslator
from airweave.domains.search.adapters.vector_db.vespa_client import VespaVectorDB
from airweave.domains.search.executor import SearchPlanExecutor
from airweave.domains.search.owned import OwnedSearchService
from airweave.domains.search.owned_models import OwnedSearchRequest
from airweave.domains.sources.fakes.registry import FakeSourceRegistry
from airweave.domains.sync_pipeline.processors.chunk_embed import ChunkEmbedProcessor
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.projection_generation import ProjectionGeneration
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata
from airweave.platform.destinations.vespa.destination import VespaDestination

pytestmark = pytest.mark.skipif(
    not os.environ.get("APPLE_PIPELINE_VESPA_URL"), reason="requires disposable Apple Vespa"
)


class FixedDense(FakeDenseEmbedder):
    async def embed(self, text):
        return DenseEmbedding(vector=[1.0] + [0.0] * 383)

    async def embed_many(self, texts, *, purpose="document"):
        return [await self.embed(text) for text in texts]


class FixedSparse(FakeSparseEmbedder):
    async def embed(self, text):
        return SparseEmbedding(indices=[1], values=[1.0])

    async def embed_many(self, texts):
        return [await self.embed(text) for text in texts]


class TextConverters:
    """Real TXT conversion only; unrelated converter dependencies are not loaded."""

    def for_extension(self, extension):
        return TxtConverter() if extension == ".txt" else None

    def for_web(self):
        raise AssertionError("Apple projection cannot fetch a web page")


@pytest.mark.parametrize("source_name", ["imessage", "apple_notes"])
async def test_device_original_to_real_search_and_withdrawal(
    database, source, tmp_path, monkeypatch, source_name
):
    from urllib.parse import urlsplit

    from airweave.core.config import settings

    origin = os.environ["APPLE_PIPELINE_VESPA_URL"]
    parsed = urlsplit(origin)
    assert parsed.hostname in ("127.0.0.1", "localhost") and parsed.port == 8086
    monkeypatch.setattr(settings, "VESPA_URL", f"{parsed.scheme}://{parsed.hostname}")
    monkeypatch.setattr(settings, "VESPA_PORT", parsed.port)
    _, initial = source
    storage = FilesystemBackend(tmp_path)
    ingest = DeviceIngestion(DeviceIngestionStore(CanonicalRecordStore()), storage)
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="fixed", embedding_dimensions=384, sparse_embedder="fixed"
        )
        db.add(deployment)
        await db.flush()
        collection = Collection(
            name="Synthetic Apple pipeline",
            readable_id="apple-" + uuid4().hex,
            organization_id=initial.organization_id,
            vector_db_deployment_metadata_id=deployment.id,
        )
        db.add(collection)
        await db.commit()
        state = await ingest.ensure(
            db,
            initial.organization_id,
            EnsureDeviceSource(
                owner_id="synthetic-owner",
                account_id="synthetic-native-store",
                source_kind=source_name,
                collection=collection.readable_id,
            ),
        )
        bound = await ingest.bind(
            db,
            state.organization_id,
            state.source_id,
            BindDevice(
                owner_id="synthetic-owner",
                expected_generation=0,
                device_id=uuid4(),
                store_generation=uuid4(),
            ),
        )
        principal = DevicePrincipal(
            owner_id="synthetic-owner",
            device_id=bound.enrollment.device_id,
            generation=bound.enrollment.generation,
            store_generation=bound.enrollment.store_generation,
        )
        run = await ingest.start(
            db, state.organization_id, state.source_id, "pipeline-one", principal
        )
    sender = "+1 (415) 555-0100"
    original = {
        "schemaVersion": 1,
        "guid": "synthetic-message",
        "message": {
            "rowID": 9007199254740993,
            "fields": {
                "guid": {"text": {"_0": "synthetic-message"}},
                "text": {"text": {"_0": "Synthetic fundraising discussion مرحبا"}},
            },
        },
        "sender": {"rowID": 2, "fields": {"id": {"text": {"_0": sender}}}},
        "chats": [],
        "participants": [],
        "chatMemberships": [],
        "bodyFidelity": {"nativeTextOnly": {}},
        "attachments": [
            {
                "rowID": 3,
                "fields": {
                    "guid": {"text": {"_0": "synthetic-file"}},
                    "filename": {"text": {"_0": "/private/never-read/fundraising.txt"}},
                },
            }
        ],
    }
    if source_name == "apple_notes":
        from airweave.domains.entities.canonical.apple_preparation.notestore_pb2 import (
            NoteStoreProto,
        )

        body = NoteStoreProto()
        body.document.version = 1
        body.document.note.note_text = "Synthetic fundraising discussion مرحبا हिन्दी"
        original = {
            "schemaVersion": 1,
            "note": {
                "primaryKey": 9007199254740993,
                "fields": {
                    "Z_PK": {"integer": {"_0": 9007199254740993}},
                    "ZIDENTIFIER": {"text": {"_0": "synthetic-message"}},
                    "ZTITLE1": {"text": {"_0": "Synthetic note"}},
                    "ZISPASSWORDPROTECTED": {"integer": {"_0": 0}},
                    "ZMARKEDFORDELETION": {"integer": {"_0": 0}},
                },
            },
            "compressedBody": base64.b64encode(
                gzip.compress(body.SerializeToString(), mtime=0)
            ).decode(),
            "fidelity": {"compressedBodyUndecoded": {}},
            "attachments": [
                {
                    "primaryKey": 9007199254740994,
                    "fields": {
                        "Z_PK": {"integer": {"_0": 9007199254740994}},
                        "ZIDENTIFIER": {"text": {"_0": "synthetic-file"}},
                        "ZFILENAME": {"text": {"_0": "fundraising.txt"}},
                        "ZNOTE": {"integer": {"_0": 9007199254740993}},
                    },
                }
            ],
        }
    content = b"Synthetic fundraising attachment planning."
    handle, digest = uuid4(), hashlib.sha256(content).hexdigest()
    async with database() as db:
        await ingest.declare_upload(
            db,
            state.organization_id,
            state.source_id,
            "pipeline-one",
            handle,
            DeviceUploadIntent(
                **principal.model_dump(),
                native_id="synthetic-message",
                original=original,
                attachment_index=0,
                sha256=digest,
                size_bytes=len(content),
                media_type="text/plain",
            ),
        )
        await ingest.upload(
            db, state.organization_id, state.source_id, "pipeline-one", handle, principal, content
        )
        page = CommitDevicePage(
            **principal.model_dump(),
            page_id=uuid4(),
            expected=run.version,
            observations=(
                DeviceObservation(
                    native_id="synthetic-message", original=original, uploads=(handle,)
                ),
            ),
            final=True,
        )
        await ingest.page(
            db,
            state.organization_id,
            state.source_id,
            "pipeline-one",
            page.model_dump_json().encode(),
        )
        await ingest.complete(db, state.organization_id, state.source_id, "pipeline-one", principal)
    log = logger.with_context(request_id="synthetic-apple-pipeline")
    dense, sparse = FixedDense(384), FixedSparse()
    projector = CanonicalProjector(
        CanonicalProjectionStore(),
        database,
        ChunkEmbedProcessor(TextConverters(), dense, sparse),
        storage,
    )
    destination = await VespaDestination.create(
        collection_id=collection.id, organization_id=state.organization_id, logger=log
    )
    registry = FakeSourceRegistry()
    blocked = AsyncMock(side_effect=AssertionError("No live provider, ACL or model-agent call"))
    engine = VespaVectorDB(
        app=Vespa(url=f"{parsed.scheme}://{parsed.hostname}", port=parsed.port),
        logger=log,
        filter_translator=FilterTranslator(logger=log),
    )
    owned = OwnedSearchService(
        SearchPlanExecutor(dense, sparse, engine, blocked, registry, blocked, blocked), registry
    )
    ctx = SimpleNamespace(organization=SimpleNamespace(id=state.organization_id), logger=log)
    request = OwnedSearchRequest(query="fundraising", sync_ids=(state.sync_id,), mode="keyword")
    query = CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-key")
    try:
        assert not (await owned.search(database, ctx, request)).items
        batch = await projector.batch(
            state.organization_id, state.sync_id, source_name, destination, log
        )
        assert batch.published == 1 and batch.failed == 0, batch
        async with database() as db:
            entity = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
            assert entity.indexed_generation is not None and entity.indexed_chunk_count >= 2
            record_id = entity.id
            generation = await db.get(ProjectionGeneration, entity.indexed_generation)
            documents = tuple(
                ProjectionDocument.model_validate(item) for item in generation.documents
            )
            assert len(documents) >= 2
            retained = await query.read(db, state.organization_id, state.sync_id, record_id)
            assert retained.payload["original"] == original
            assert (
                await query.blob(
                    db,
                    state.organization_id,
                    state.sync_id,
                    record_id,
                    retained.revision,
                    digest,
                    storage,
                )
                == content
            )
        for mode in ("keyword", "semantic", "hybrid"):
            found = await owned.search(database, ctx, request.model_copy(update={"mode": mode}))
            assert [hit.record_id for hit in found.items] == [record_id]
            assert "fundraising" in " ".join(found.items[0].excerpts).lower()
        file_match = await owned.search(
            database, ctx, request.model_copy(update={"query": "planning"})
        )
        assert [hit.record_id for hit in file_match.items] == [record_id]
        assert file_match.items[0].matched_part.key == "attachment:synthetic-file"
        assert file_match.items[0].matched_part.kind == "file"
        if source_name == "imessage":
            matching = request.model_copy(
                update={"actor_filters": (ActorFilter(role="sender", handle=sender),)}
            )
            assert [
                hit.record_id for hit in (await owned.search(database, ctx, matching)).items
            ] == [record_id]
            endpoint_request = request.model_copy(
                update={
                    "actor_any_of": ActorAnyOf(
                        role="sender", handles=("+१४१५५५५०१००",), match="endpoint"
                    )
                }
            )
            assert [
                hit.record_id for hit in (await owned.search(database, ctx, endpoint_request)).items
            ] == [record_id]
            wrong_endpoint = endpoint_request.model_copy(
                update={
                    "actor_any_of": ActorAnyOf(
                        role="sender", handles=("+14155550100 x12",), match="endpoint"
                    )
                }
            )
            assert not (await owned.search(database, ctx, wrong_endpoint)).items
            # An actual old scope version cannot silently use incomplete endpoint terms.
            from airweave.models.sync import Sync

            async with database() as db:
                scope = await db.get(Sync, state.sync_id)
                scope.index_pipeline_version = 4
                await db.commit()
            with pytest.raises(HTTPException) as old_projection:
                await owned.search(database, ctx, endpoint_request)
            assert old_projection.value.status_code == 409
            assert old_projection.value.detail["code"] == "reindex_required"
            async with database() as db:
                scope = await db.get(Sync, state.sync_id)
                scope.index_pipeline_version = 5
                await db.commit()
            for actor in (
                ActorFilter(role="sender", handle="+14155550100"),
                ActorFilter(role="current_chat_member", handle=sender),
            ):
                assert not (
                    await owned.search(
                        database, ctx, request.model_copy(update={"actor_filters": (actor,)})
                    )
                ).items
        if source_name == "imessage":
            # Keep actual Vespa documents while SQL reports a newer canonical revision.
            # The old publication cannot qualify merely because its endpoint term matches.
            async with database() as db:
                current = await db.get(Entity, record_id)
                current.record_revision += 1
                await db.commit()
            assert not (await owned.search(database, ctx, endpoint_request)).items
            async with database() as db:
                current = await db.get(Entity, record_id)
                current.record_revision -= 1
                await db.commit()
        if source_name == "apple_notes":
            locked = copy.deepcopy(original)
            locked.pop("compressedBody")
            locked["attachments"] = []
            locked["fidelity"] = {"lockedBodyWithheld": {}}
            locked["note"]["fields"]["ZISPASSWORDPROTECTED"] = {"integer": {"_0": 1}}
            async with database() as db:
                withdrawal = await ingest.start(
                    db, state.organization_id, state.source_id, "note-locked", principal
                )
                locked_page = CommitDevicePage(
                    **principal.model_dump(),
                    page_id=uuid4(),
                    expected=withdrawal.version,
                    observations=(
                        DeviceObservation(
                            native_id="synthetic-message",
                            original=locked,
                            kind="delete",
                            removal_reason="access_revoked",
                        ),
                    ),
                    final=True,
                )
                await ingest.page(
                    db,
                    state.organization_id,
                    state.source_id,
                    "note-locked",
                    locked_page.model_dump_json().encode(),
                )
            # Old indexed chunks still exist: current record authority must hide them
            # before asynchronous projection cleanup, including the saved attachment.
            for mode in ("keyword", "semantic", "hybrid"):
                assert not (
                    await owned.search(database, ctx, request.model_copy(update={"mode": mode}))
                ).items
            async with database() as db:
                locked_record = await query.read(
                    db, state.organization_id, state.sync_id, record_id
                )
                assert locked_record.deleted_at is not None
                assert locked_record.removal_reason == "access_revoked"
                assert locked_record.content_access == "unavailable"
                assert locked_record.payload == {} and locked_record.blobs == ()
                with pytest.raises(RecordNotFound):
                    await query.blob(
                        db,
                        state.organization_id,
                        state.sync_id,
                        record_id,
                        retained.revision,
                        digest,
                        storage,
                    )
        async with database() as db:
            await ingest.revoke(
                db,
                state.organization_id,
                state.source_id,
                RevokeDevice(owner_id=principal.owner_id, expected_generation=principal.generation),
            )
        with pytest.raises(HTTPException) as error:
            await owned.search(database, ctx, request)
        assert error.value.status_code == 404
        if source_name == "imessage":
            with pytest.raises(HTTPException) as endpoint_revoked:
                await owned.search(database, ctx, endpoint_request)
            assert endpoint_revoked.value.status_code == 404
        # Physical documents remain: SQL source authority prevents their disclosure.
        async with httpx.AsyncClient(timeout=30) as http:
            for document in documents:
                response = await http.get(
                    f"{origin}/document/v1/airweave/{document.schema_name}/docid/{document.document_id}"
                )
                assert response.status_code == 200
        async with database() as db:
            withdrawn = await query.read(db, state.organization_id, state.sync_id, record_id)
            assert withdrawn.content_access == "unavailable"
            assert withdrawn.payload == {} and withdrawn.blobs == ()
            await db.rollback()
            with pytest.raises(RecordNotFound):
                await query.blob(
                    db,
                    state.organization_id,
                    state.sync_id,
                    record_id,
                    retained.revision,
                    digest,
                    storage,
                )
    finally:
        await destination.delete_by_sync_id(state.sync_id)
        await destination.close_connection()
