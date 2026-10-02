"""Exact lost-response recovery uses the same real transaction as native page writes."""


# Imported pytest fixture names are resolved through function parameters.
# ruff: noqa: F811

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.cycle_store import cursor_row, cycle_state
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_models import BeginScan
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore, ScanConflict
from airweave.domains.entities.canonical.store import CanonicalRecordStore, StaleWriter
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.models import SessionVersion
from airweave.domains.native_ingestion.page_models import CommitNativePage
from airweave.domains.native_ingestion.page_store import NativePageStore
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.domains.native_ingestion.store import NativeIngestionStore
from airweave.domains.native_ingestion.tests.test_imports import native, start  # noqa: F401
from airweave.domains.native_ingestion.tests.test_ingestion import snapshot
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob


@pytest.fixture
async def scope(database, native):
    await start(database, native)
    canonical = CanonicalRecordStore()
    imports = NativeImportStore(NativeSourceStore(), canonical)
    async with database() as db, UnitOfWork(db):
        saved = await imports.active(
            db, native.organization_id, native.source_connection_id, "request-one"
        )
        cycle = cycle_state(await cursor_row(db, saved.fence))
        kind = "knowledge" if native.binding.dataset == "knowledge" else "session"
        state = await CanonicalScanStore(canonical).begin(
            db,
            BeginScan(
                fence=saved.fence,
                scope=CompletedScope(record_type=kind),
                cycle_id=saved.cycle_id,
                fingerprint=cycle.configuration.fingerprint,
            ),
        )
    return state


def page_request(native, state):
    item = snapshot(owner_id=native.binding.owner_id)
    if native.binding.dataset == "sessions":
        item = item.model_copy(
            update={
                "identity": RecordIdentity(record_type="session", native_id="one"),
                "version": SessionVersion(revision=1, content_revision=1),
            }
        )
    return CommitNativePage(
        page_id=uuid4(),
        scope=state.scope,
        expected=state.version,
        snapshots=(item,),
        cursor={"next": "two"},
    )


async def commit(database, native, request):
    canonical = CanonicalRecordStore()
    pages = NativePageStore(
        NativeImportStore(NativeSourceStore(), canonical), NativeIngestionStore(canonical)
    )
    async with database() as db, UnitOfWork(db):
        return await pages.commit(
            db, native.organization_id, native.source_connection_id, "request-one", request
        )


async def test_concurrent_page_retry_conflict_and_superseded_receipt(database, native, scope):
    request = page_request(native, scope)
    first, duplicate = await asyncio.gather(
        commit(database, native, request), commit(database, native, request)
    )
    assert first == duplicate and first.changed == 1 and first.sequence == 1
    async with database() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == native.sync_id)
            )
            == 1
        )
    with pytest.raises(ScanConflict, match="retry conflicts"):
        await commit(database, native, request.model_copy(update={"final": True}))
    last = request.model_copy(
        update={
            "page_id": uuid4(),
            "expected": first.version,
            "snapshots": (),
            "final": True,
        }
    )
    final = await commit(database, native, last)
    assert final.phase == "reconciling"
    assert await commit(database, native, last) == final
    with pytest.raises(ScanConflict, match="Page changed"):
        await commit(database, native, request)


async def test_cancelled_import_cannot_replay_a_page_receipt(database, native, scope):
    request = page_request(native, scope)
    await commit(database, native, request)
    imported = await start(database, native)
    async with database() as db:
        job = await db.get(SyncJob, imported.import_id)
        job.status = "cancelled"
        await db.commit()
    with pytest.raises(StaleWriter):
        await commit(database, native, request)


async def test_receipt_failure_rolls_back_capture_and_cursor(database, native, scope, monkeypatch):
    import airweave.domains.native_ingestion.page_store as module

    def fail(**kwargs):
        raise RuntimeError("receipt persistence failed")

    monkeypatch.setattr(module, "NativePageReceipt", fail)
    with pytest.raises(RuntimeError, match="receipt persistence"):
        await commit(database, native, page_request(native, scope))
    canonical = CanonicalRecordStore()
    imports = NativeImportStore(NativeSourceStore(), canonical)
    async with database() as db, UnitOfWork(db):
        saved = await imports.active(
            db, native.organization_id, native.source_connection_id, "request-one"
        )
        current = await CanonicalScanStore(canonical).read(db, saved.fence, scope.scope)
        assert current == scope
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == native.sync_id)
            )
            == 0
        )
