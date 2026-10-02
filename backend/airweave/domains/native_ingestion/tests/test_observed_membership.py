"""Bounded native imports own only freshly observed parent/ancestor membership."""

import hashlib
import json

import pytest

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CompleteCycle,
    CycleConfiguration,
)
from airweave.domains.entities.canonical.cycle_store import (
    CycleConflict,
    begin_cycle,
    complete_cycle,
    cursor_row,
    cycle_state,
    next_scope_work,
)
from airweave.domains.entities.canonical.requests import CompletedScope, RecordIdentity
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.scan_store import CanonicalScanStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.native_ingestion.models import IngestNativePage, SessionVersion
from airweave.domains.native_ingestion.service import NativeIngestionService
from airweave.domains.native_ingestion.store import NativeIngestionStore
from airweave.domains.native_ingestion.tests.test_ingestion import (
    NOW,
    bind,
    ingest,
    retained,
    snapshot,
)


async def start_cycle(database, canonical, fence, *, expected=None):
    async with database() as db, UnitOfWork(db):
        await canonical._fenced_sync(db, fence)
        return await begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                expected=expected,
                configuration=CycleConfiguration(
                    fingerprint="a" * 64,
                    parents={"session": (None,), "message": ("session",)},
                    completion_policies={"session": "discovery_only", "message": "discovery_only"},
                    membership="observed",
                ),
            ),
        )


async def finish_scope(database, canonical, fence, cycle, scope, items=(), *, epoch=None):
    scans = CanonicalScanStore(canonical)
    async with database() as db, UnitOfWork(db):
        previous = await scans.read(db, fence, scope)
        state = await scans.begin(
            db,
            BeginScan(
                fence=fence,
                scope=scope,
                cycle_id=cycle.version.cycle_id,
                fingerprint=cycle.configuration.fingerprint,
                expected=previous.version if previous else None,
                expected_parent_epoch=epoch,
            ),
        )
    async with database() as db:
        result = await NativeIngestionService(NativeIngestionStore(canonical)).page(
            db,
            IngestNativePage(
                fence=fence,
                observed_at=NOW,
                snapshots=items,
                scope=scope,
                cycle_id=cycle.version.cycle_id,
                expected=state.version,
                final=True,
                continuation=ScanContinuation(),
            ),
        )
    async with database() as db, UnitOfWork(db):
        await scans.reconcile(
            db,
            ReconcileScan(
                fence=fence,
                scope=scope,
                cycle_id=cycle.version.cycle_id,
                expected=result.state.version,
                observed_at=NOW,
            ),
        )
    return result


async def finish_cycle(database, canonical, fence):
    async with database() as db, UnitOfWork(db):
        sync = await canonical._fenced_sync(db, fence)
        cycle = cycle_state(await cursor_row(db, fence))
        return await complete_cycle(db, sync, CompleteCycle(fence=fence, expected=cycle.version))


async def test_only_observed_unchanged_parent_requires_transcript_and_empty_import_keeps_all(
    database, source
):
    _, fence = source
    canonical = CanonicalRecordStore()
    await bind(database, fence, dataset="sessions")
    first, second = (
        snapshot(
            name,
            identity=RecordIdentity(record_type="session", native_id=name),
            version=SessionVersion(revision=1, content_revision=1),
        )
        for name in ("first", "second")
    )
    await ingest(database, fence, first, second)
    cycle = await start_cycle(database, canonical, fence)
    root = CompletedScope(record_type="session")
    result = await finish_scope(database, canonical, fence, cycle, root, (first,))
    assert result.capture.unchanged == 1 and result.capture.changes == ()
    with pytest.raises(CycleConflict, match="incomplete child"):
        await finish_cycle(database, canonical, fence)
    async with database() as db, UnitOfWork(db):
        work = await next_scope_work(db, fence, cycle.version.cycle_id)
        assert work.parent.identity == first.identity
    with pytest.raises(CycleConflict, match="membership"):
        await finish_scope(
            database,
            canonical,
            fence,
            cycle,
            CompletedScope(record_type="message", container_id="second", parent=second.identity),
            epoch=1,
        )
    await finish_scope(
        database,
        canonical,
        fence,
        cycle,
        CompletedScope(record_type="message", container_id="first", parent=first.identity),
        epoch=work.parent_visibility_epoch,
    )
    completed = await finish_cycle(database, canonical, fence)
    assert completed.last_full_capture.discovery == "incomplete"
    empty = await start_cycle(database, canonical, fence, expected=completed.version)
    await finish_scope(database, canonical, fence, empty, root)
    async with database() as db, UnitOfWork(db):
        assert await next_scope_work(db, fence, empty.version.cycle_id) is None
    completed_empty = await finish_cycle(database, canonical, fence)
    assert completed_empty.last_full_capture.discovery == "incomplete"
    rows = await retained(database, fence)
    assert len(rows) == 2 and all(row.deleted_at is None for row in rows)


def test_default_configuration_digest_stays_compatible():
    configuration = CycleConfiguration(fingerprint="a" * 64, parents={"session": (None,)})
    legacy = {
        "fingerprint": "a" * 64,
        "parents": {"session": [None]},
        "completion_policies": {"session": "exhaustive"},
    }
    assert (
        configuration.digest()
        == hashlib.sha256(json.dumps(legacy, sort_keys=True).encode()).hexdigest()
    )
    assert (
        configuration.model_copy(update={"membership": "observed"}).digest()
        != configuration.digest()
    )
