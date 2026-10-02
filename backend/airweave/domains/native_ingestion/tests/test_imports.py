"""Real PostgreSQL import request recovery, concurrent creation and rollback."""

import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.store import CanonicalRecordStore, WriterBusy
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_models import StartNativeImport
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.source_models import EnsureNativeSource
from airweave.domains.native_ingestion.source_service import NativeSources
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_cursor import SyncCursor
from airweave.models.sync_job import SyncJob
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


@pytest.fixture(params=["knowledge", "sessions"])
async def native(database, source, request):
    _, fence = source
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="fake", embedding_dimensions=3, sparse_embedder="fake"
        )
        db.add(deployment)
        await db.flush()
        db.add(
            Collection(
                organization_id=fence.organization_id,
                name="Native import",
                readable_id="native",
                vector_db_deployment_metadata_id=deployment.id,
            )
        )
        await db.commit()
    async with database() as db:
        return await NativeSources(NativeSourceStore()).ensure(
            db,
            fence.organization_id,
            EnsureNativeSource(owner_id="owner", dataset=request.param, collection="native"),
        )


async def start(database, native, key="request-one", request=None):
    store = NativeImportStore(NativeSourceStore(), CanonicalRecordStore())
    async with database() as db, UnitOfWork(db):
        return await store.start(
            db,
            native.organization_id,
            native.source_connection_id,
            key,
            request or StartNativeImport(snapshot_id="snapshot-one", coverage="bounded"),
        )


async def test_concurrent_start_recovers_one_writer_and_rejects_changed_key_body(database, native):
    first, duplicate = await asyncio.gather(start(database, native), start(database, native))
    assert first == duplicate
    async with database() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(SyncJob).where(SyncJob.sync_id == native.sync_id)
            )
            == 1
        )
        sync = await db.get(Sync, native.sync_id)
        assert sync.writer_epoch == 1
    with pytest.raises(NativeAdmissionError, match="conflicting intent"):
        await start(
            database, native, request=StartNativeImport(snapshot_id="other", coverage="complete")
        )
    with pytest.raises(WriterBusy):
        await start(database, native, key="other-key")
    async with database() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(SyncJob).where(SyncJob.sync_id == native.sync_id)
            )
            == 1
        )


async def test_terminal_retry_never_reactivates_and_new_request_replaces_partial_cycle(
    database, native
):
    first = await start(database, native)
    async with database() as db:
        job = await db.get(SyncJob, first.import_id)
        job.status = "cancelled"
        await db.commit()
    duplicate = await start(database, native)
    assert duplicate.status == "cancelled" and duplicate.cycle_id == first.cycle_id
    async with database() as db:
        assert (await db.get(Sync, native.sync_id)).writer_epoch == 1
    next_import = await start(database, native, key="new-request")
    assert next_import.cycle_id != first.cycle_id
    async with database() as db:
        assert (await db.get(Sync, native.sync_id)).writer_epoch == 2
    assert (await start(database, native)).status == "cancelled"
    async with database() as db:
        source = await db.get(SourceConnection, native.source_connection_id)
        source.is_authenticated = False
        await db.commit()
    assert (await start(database, native)).status == "cancelled"
    with pytest.raises(NativeAdmissionError, match="unavailable"):
        await start(database, native, key="after-withdrawal")
    async with database() as db:
        assert (await db.get(Sync, native.sync_id)).writer_epoch == 2


async def test_cycle_failure_rolls_back_job_and_fence(database, native, monkeypatch):
    import airweave.domains.native_ingestion.import_store as module

    async def fail(*args, **kwargs):
        raise RuntimeError("cycle publication failed")

    monkeypatch.setattr(module, "begin_cycle", fail)
    with pytest.raises(RuntimeError, match="cycle publication"):
        await start(database, native)
    async with database() as db:
        assert (await db.get(Sync, native.sync_id)).writer_epoch == 0
        assert (
            await db.scalar(
                select(func.count()).select_from(SyncJob).where(SyncJob.sync_id == native.sync_id)
            )
            == 0
        )
    from fastapi import HTTPException

    async with database() as db, UnitOfWork(db):
        with pytest.raises(HTTPException) as error:
            await NativeImportStore(NativeSourceStore(), CanonicalRecordStore()).read(
                db, uuid4(), native.source_connection_id, "request-one"
            )
        assert error.value.status_code == 404


async def test_transcript_coverage_is_sessions_only_and_part_of_retry_identity(database, native):
    request = StartNativeImport(
        snapshot_id="snapshot-one", coverage="bounded", transcript_coverage="complete"
    )
    if native.binding.dataset == "knowledge":
        with pytest.raises(NativeAdmissionError, match="requires a sessions source"):
            await start(database, native, request=request)
        async with database() as db:
            assert (await db.get(Sync, native.sync_id)).writer_epoch == 0
            assert (
                await db.scalar(
                    select(func.count())
                    .select_from(SyncJob)
                    .where(SyncJob.sync_id == native.sync_id)
                )
                == 0
            )
        return

    initial = await start(database, native)
    assert initial.request.transcript_coverage == "bounded"
    with pytest.raises(NativeAdmissionError, match="conflicting intent"):
        await start(database, native, request=request)
    assert await start(database, native) == initial


@pytest.mark.parametrize("native", ["sessions"], indirect=True)
async def test_legacy_import_without_transcript_coverage_resumes_without_changing_plan(
    database, native
):
    initial = await start(database, native)
    async with database() as db:
        job = await db.get(SyncJob, initial.import_id)
        request = dict(job.sync_metadata["request"])
        request.pop("transcript_coverage")
        job.sync_metadata = {**job.sync_metadata, "request": request}
        cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == native.sync_id))
        cycle = cursor.cursor_data["canonical_cycle"]
        plan = dict(cycle["source_plan"])
        plan.pop("transcript_coverage")
        fingerprint = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        legacy_cycle = {
            **cycle,
            "source_plan": plan,
            "configuration": {**cycle["configuration"], "fingerprint": fingerprint},
        }
        cursor.cursor_data = {**cursor.cursor_data, "canonical_cycle": legacy_cycle}
        await db.commit()

    assert await start(database, native) == initial
    async with database() as db:
        cursor = await db.scalar(select(SyncCursor).where(SyncCursor.sync_id == native.sync_id))
        assert cursor.cursor_data["canonical_cycle"] == legacy_cycle
        assert (await db.get(Sync, native.sync_id)).writer_epoch == 1
        job = await db.get(SyncJob, initial.import_id)
        assert "transcript_coverage" not in job.sync_metadata["request"]


async def test_legacy_cancellation_uses_actual_native_sync_identity(database, native, source):
    from fastapi import HTTPException

    from airweave.domains.owned_provisioning.guard import require_provider_sync

    async with database() as db:
        with pytest.raises(HTTPException) as error:
            await require_provider_sync(db, native.sync_id, native.organization_id)
        assert error.value.status_code == 409
        _, provider = source
        await require_provider_sync(db, provider.sync_id, provider.organization_id)
