"""Explicit complete transcripts reconcile children within bounded observed roots."""

# Imported pytest fixture names are resolved through function parameters.
# ruff: noqa: F811

from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_models import StartNativeImport
from airweave.domains.native_ingestion.import_service import NativeImports
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.models import SessionVersion
from airweave.domains.native_ingestion.page_models import CommitNativePage
from airweave.domains.native_ingestion.scope_models import BeginNativeScope, ReconcileNativeScope
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.domains.native_ingestion.tests.test_imports import native  # noqa: F401
from airweave.domains.native_ingestion.tests.test_ingestion import ingest, snapshot
from airweave.models.entity import Entity

pytestmark = pytest.mark.parametrize("native", ["sessions"], indirect=True)


def imports():
    return NativeImports(NativeImportStore(NativeSourceStore(), CanonicalRecordStore()))


def session(native_id, revision=1):
    return snapshot(
        native_id,
        owner_id="owner",
        identity=RecordIdentity(record_type="session", native_id=native_id),
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=revision, content_revision=revision),
    )


def message(parent, native_id):
    return snapshot(
        native_id,
        owner_id="owner",
        identity=RecordIdentity(
            record_type="message", native_id=native_id, container_id=parent.identity.native_id
        ),
        parent=parent.identity,
        version=parent.version,
    )


def child_scope(parent):
    return CompletedScope(
        record_type="message", container_id=parent.identity.native_id, parent=parent.identity
    )


async def begin(database, native, key, scope):
    async with database() as db:
        return await imports().begin_scope(
            db,
            native.organization_id,
            native.source_connection_id,
            key,
            BeginNativeScope(scope=scope),
        )


async def page(database, native, key, state, items, *, final=True):
    async with database() as db:
        return await imports().page(
            db,
            native.organization_id,
            native.source_connection_id,
            key,
            CommitNativePage(
                page_id=uuid4(),
                scope=state.scope,
                expected=state.version,
                snapshots=items,
                final=final,
            ),
        )


async def reconcile(database, native, key, scope, version):
    async with database() as db:
        return await imports().reconcile_scope(
            db,
            native.organization_id,
            native.source_connection_id,
            key,
            ReconcileNativeScope(scope=scope, expected=version),
        )


async def capture_scope(database, native, key, scope, items):
    state = await begin(database, native, key, scope)
    ack = await page(database, native, key, state, items)
    return await reconcile(database, native, key, scope, ack.version)


async def start(database, native, key, **intent):
    async with database() as db:
        return await imports().start(
            db,
            native.organization_id,
            native.source_connection_id,
            key,
            StartNativeImport(snapshot_id=key, coverage="bounded", **intent),
        )


async def seed(database, native):
    first, second = session("first"), session("second")
    await start(database, native, "baseline")
    await capture_scope(
        database, native, "baseline", CompletedScope(record_type="session"), (first, second)
    )
    await capture_scope(
        database,
        native,
        "baseline",
        child_scope(first),
        (message(first, "kept"), message(first, "omitted")),
    )
    await capture_scope(
        database, native, "baseline", child_scope(second), (message(second, "unselected"),)
    )
    async with database() as db:
        await imports().finish(db, native.organization_id, native.source_connection_id, "baseline")
    return first, second


async def rows(database, native):
    async with database() as db:
        values = await db.scalars(select(Entity).where(Entity.sync_id == native.sync_id))
        return {item.native_id: item for item in values}


@pytest.mark.parametrize("complete", [False, True])
async def test_selected_transcript_policy_preserves_unselected_roots(database, native, complete):
    await seed(database, native)
    intent = {"transcript_coverage": "complete"} if complete else {}
    started = await start(database, native, "selected", **intent)
    assert await start(database, native, "selected", **intent) == started
    parent = session("first", revision=2)
    root = await capture_scope(
        database, native, "selected", CompletedScope(record_type="session"), (parent,)
    )
    assert root.coverage == "bounded"
    scope = await capture_scope(
        database, native, "selected", child_scope(parent), (message(parent, "kept"),)
    )
    assert scope.coverage == ("complete" if complete else "bounded")
    async with database() as db:
        read = await imports().read_scope(
            db,
            native.organization_id,
            native.source_connection_id,
            "selected",
            BeginNativeScope(scope=scope.scope),
        )
        assert read.coverage == scope.coverage
        result = await imports().finish(
            db, native.organization_id, native.source_connection_id, "selected"
        )
    assert result.summary.coverage == "bounded" and result.summary.capture_complete
    records = await rows(database, native)
    assert (records["omitted"].deleted_at is not None) == complete
    assert records["omitted"].removal_reason == ("absent" if complete else None)
    assert records["kept"].deleted_at is None
    for name in ("second", "unselected"):
        assert records[name].deleted_at is None and records[name].record_revision == 1


async def test_incomplete_failed_and_parent_changed_scan_cannot_reconcile_absence(database, native):
    old_parent, _ = await seed(database, native)
    await start(database, native, "selected", transcript_coverage="complete")
    parent = session("first", revision=2)
    await capture_scope(
        database, native, "selected", CompletedScope(record_type="session"), (parent,)
    )
    state = await begin(database, native, "selected", child_scope(parent))
    ack = await page(database, native, "selected", state, (message(parent, "kept"),), final=False)
    state = state.model_copy(update={"version": ack.version})
    with pytest.raises(NativeAdmissionError, match="version must match"):
        await page(database, native, "selected", state, (message(old_parent, "stale"),))
    with pytest.raises(ScanConflict, match="fully collected"):
        await reconcile(database, native, "selected", state.scope, ack.version)
    async with database() as db:
        saved = await imports().store.active(
            db, native.organization_id, native.source_connection_id, "selected"
        )
    await ingest(database, saved.fence, session("first", revision=3))
    with pytest.raises((CycleConflict, ScanConflict)):
        await page(database, native, "selected", state, ())
    records = await rows(database, native)
    assert records["omitted"].deleted_at is None and records["omitted"].record_revision == 1
    assert "stale" not in records
