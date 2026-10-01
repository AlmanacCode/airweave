"""Native admission, capture and sweep continuation share one real transaction."""

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import BeginCycle, CycleConfiguration
from airweave.domains.entities.canonical.requests import (
    CompletedScope,
    RecordIdentity,
    RemovedScope,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.native_ingestion.errors import NativeAdmissionError
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
from airweave.models.entity import Entity


async def begin(database, canonical, fence, *, sessions=False):
    async with database() as db:
        return await canonical.begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                configuration=CycleConfiguration(
                    fingerprint="a" * 64,
                    parents={"session": (None,), "message": ("session",)}
                    if sessions
                    else {"knowledge": (None,)},
                ),
            ),
        )


async def scan(database, canonical, fence, cycle, scope, epoch=None):
    async with database() as db:
        return await canonical.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=scope,
                cycle_id=cycle.version.cycle_id,
                fingerprint="a" * 64,
                expected_parent_epoch=epoch,
            ),
        )


async def page(database, fence, state, *items, final=False):
    service = NativeIngestionService(NativeIngestionStore(CanonicalRecordStore()))
    async with database() as db:
        return await service.page(
            db,
            IngestNativePage(
                fence=fence,
                observed_at=NOW,
                snapshots=items,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                continuation=ScanContinuation(value={"next": "two"}),
                final=final,
            ),
        )


async def test_unchanged_new_sweep_survives_reconciliation_without_new_revision(database, source):
    canonical, fence = source
    await bind(database, fence)
    original = snapshot()
    await ingest(database, fence, original)
    cycle = await begin(database, canonical, fence)
    state = await scan(database, canonical, fence, cycle, CompletedScope(record_type="knowledge"))
    result = await page(database, fence, state, original, final=True)
    assert result.capture.unchanged == 1 and result.capture.changes == ()
    async with database() as db:
        reconciled = await canonical.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=result.state.version,
                observed_at=NOW,
            ),
        )
    row = (await retained(database, fence))[0]
    assert row.last_seen_run_id == state.version.sweep_id
    assert row.record_revision == 1 and row.deleted_at is None
    assert reconciled.capture.changes == ()


async def test_withdrawn_or_tombstoned_snapshot_cannot_be_seen_again(database, source):
    canonical, fence = source
    await bind(database, fence)
    item = snapshot()
    await ingest(database, fence, item)
    cycle = await begin(database, canonical, fence)
    state = await scan(database, canonical, fence, cycle, CompletedScope(record_type="knowledge"))
    async with database() as db:
        await canonical.remove_scope(
            db,
            fence,
            RemovedScope(record_type="knowledge", removal_reason="access_revoked", observed_at=NOW),
        )
    with pytest.raises(NativeAdmissionError, match="active original sightings"):
        await page(database, fence, state, item)
    with pytest.raises(NativeAdmissionError, match="not tombstones"):
        await page(database, fence, state, item.model_copy(update={"operation": "delete"}))
    async with database() as db:
        current = await canonical.read_scan(db, fence, state.scope)
    assert current == state


async def test_stale_unchanged_child_does_not_attest_membership_of_newer_parent(database, source):
    canonical, fence = source
    await bind(database, fence, dataset="sessions")
    parent = snapshot(
        identity=RecordIdentity(record_type="session", native_id="s"),
        version=SessionVersion(revision=1, content_revision=1),
    )
    child = snapshot(
        identity=RecordIdentity(record_type="message", native_id="m", container_id="s"),
        parent=parent.identity,
        version=parent.version,
    )
    await ingest(database, fence, parent, child)
    newer = parent.model_copy(update={"version": SessionVersion(revision=1, content_revision=2)})
    await ingest(database, fence, newer)
    cycle = await begin(database, canonical, fence, sessions=True)
    root = await scan(database, canonical, fence, cycle, CompletedScope(record_type="session"))
    root_result = await page(database, fence, root, newer, final=True)
    async with database() as db:
        await canonical.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=root.scope,
                cycle_id=root.cycle_id,
                expected=root_result.state.version,
                observed_at=NOW,
            ),
        )
    async with database() as db:
        epoch = await db.scalar(
            select(Entity.visibility_epoch).where(
                Entity.sync_id == fence.sync_id, Entity.native_id == "s"
            )
        )
    state = await scan(
        database,
        canonical,
        fence,
        cycle,
        CompletedScope(record_type="message", container_id="s", parent=parent.identity),
        epoch,
    )
    with pytest.raises(NativeAdmissionError, match="attested native session"):
        await page(database, fence, state, child)
    assert (
        await ingest(database, fence, child)
    ).unchanged == 1  # Ordinary retry is not a new sighting.
    updated = child.model_copy(update={"version": newer.version})
    result = await page(database, fence, state, updated)
    assert result.capture.changes[0].record.revision == 2


async def test_failure_after_capture_rolls_back_records_sightings_and_cursor(
    database, source, monkeypatch
):
    import airweave.domains.entities.canonical.scan_store as scan_module

    canonical, fence = source
    await bind(database, fence)
    original = snapshot()
    await ingest(database, fence, original)
    before = (await retained(database, fence))[0].last_seen_run_id
    cycle = await begin(database, canonical, fence)
    state = await scan(database, canonical, fence, cycle, CompletedScope(record_type="knowledge"))

    def fail(*args, **kwargs):
        raise RuntimeError("synthetic failure after page writes")

    monkeypatch.setattr(scan_module, "publish_scope", fail)
    with pytest.raises(RuntimeError, match="after page writes"):
        await page(database, fence, state, original, snapshot("new"), final=True)
    rows = await retained(database, fence)
    assert len(rows) == 1 and rows[0].last_seen_run_id == before
    async with database() as db:
        assert await canonical.read_scan(db, fence, state.scope) == state


async def test_empty_final_page_is_explicit_completion_not_implicit_absence(database, source):
    canonical, fence = source
    await bind(database, fence)
    await ingest(database, fence, snapshot())
    cycle = await begin(database, canonical, fence)
    state = await scan(database, canonical, fence, cycle, CompletedScope(record_type="knowledge"))
    partial = await page(database, fence, state)
    assert partial.state.phase == "collecting"
    assert (await retained(database, fence))[0].deleted_at is None
    final = await page(database, fence, partial.state, final=True)
    assert final.state.phase == "reconciling"
    assert (await retained(database, fence))[0].deleted_at is None
    async with database() as db:
        await canonical.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=final.state.version,
                observed_at=NOW,
            ),
        )
    row = (await retained(database, fence))[0]
    assert row.removal_reason == "absent" and row.source_payload["version"]["revision"] == 1
