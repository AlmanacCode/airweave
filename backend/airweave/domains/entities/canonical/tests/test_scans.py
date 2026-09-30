"""Actual PostgreSQL recovery and transaction tests, without provider traffic."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore, StaleWriter
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.entity_change import EntityChange

SCOPE = CompletedScope(record_type="event")
FINGERPRINT = "a" * 64


async def begin(database, service, fence, *, cycle=None, **kwargs):
    async with database() as db:
        return await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=SCOPE,
                cycle_id=cycle or uuid4(),
                fingerprint=FINGERPRINT,
                **kwargs,
            ),
        )


async def page(database, service, fence, state, *records, final=False):
    async with database() as db:
        return await service.commit_scan_page(
            db,
            CommitScanPage(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                records=records,
                continuation=ScanContinuation(value={"next": "page-2"}),
                final=final,
            ),
        )


async def reconcile(database, service, fence, state, limit=250):
    async with database() as db:
        return await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                observed_at=datetime.now(timezone.utc),
                limit=limit,
            ),
        )


async def test_resume_new_writer_preserves_prior_page_sightings(database, source):
    service, fence = source
    await capture(database, service, fence, observation("removed"))
    state = await begin(database, service, fence)
    first = await page(database, service, fence, state, observation("one"))
    async with database() as db:
        newer = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    resumed = await begin(database, service, newer, cycle=state.cycle_id)
    assert resumed == first.state
    with pytest.raises(StaleWriter):
        await page(database, service, fence, first.state, observation("stale"))
    final = await page(database, service, newer, resumed, observation("two"), final=True)
    complete = await reconcile(database, service, newer, final.state)
    assert complete.state.phase == "complete"
    assert [change.record.identity.native_id for change in complete.capture.changes] == ["removed"]
    async with database() as db:
        active = (
            await db.scalars(select(Entity.native_id).where(Entity.deleted_at.is_(None)))
        ).all()
    assert set(active) == {"one", "two"}


async def test_page_commit_without_ack_reload_and_stale_cas(database, source):
    service, fence = source
    state = await begin(database, service, fence)
    committed = await page(database, service, fence, state, observation())
    with pytest.raises(ScanConflict):
        await page(database, service, fence, state, observation("must-not-appear"))
    async with database() as db:
        loaded = await service.read_scan(db, fence, SCOPE)
    assert loaded == committed.state
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Entity)) == 1
        assert await db.scalar(select(func.count()).select_from(EntityChange)) == 1


async def test_page_failure_rolls_back_records_journal_and_progress(database, source):
    service, fence = source
    state = await begin(database, service, fence)

    class FailingStore(CanonicalRecordStore):
        async def _capture_locked(self, *args, **kwargs):
            await super()._capture_locked(*args, **kwargs)
            raise RuntimeError("synthetic post-write failure")

    failing = CanonicalCaptureService(FailingStore())
    with pytest.raises(RuntimeError, match="synthetic"):
        await page(database, failing, fence, state, observation(), final=True)
    async with database() as db:
        loaded = await service.read_scan(db, fence, SCOPE)
    assert loaded == state
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Entity)) == 0
        assert await db.scalar(select(func.count()).select_from(EntityChange)) == 0


async def test_partial_cannot_reconcile_and_expired_cursor_restarts_sweep(database, source):
    service, fence = source
    state = await begin(database, service, fence)
    partial = await page(database, service, fence, state, observation("prior-page"))
    with pytest.raises(ScanConflict, match="fully collected"):
        await reconcile(database, service, fence, partial.state)
    restarted = await begin(
        database, service, fence, cycle=state.cycle_id, expected=partial.state.version, restart=True
    )
    assert restarted.version.sweep_id != state.version.sweep_id
    assert restarted.continuation.value == {}
    with pytest.raises(ScanConflict):
        await page(database, service, fence, partial.state, observation("old-reply"))
    final = await page(database, service, fence, restarted, observation("now-present"), final=True)
    complete = await reconcile(database, service, fence, final.state)
    assert [change.record.identity.native_id for change in complete.capture.changes] == [
        "prior-page"
    ]


async def test_completed_scope_reused_in_cycle_but_next_cycle_refreshes(database, source):
    service, fence = source
    state = await begin(database, service, fence)
    with pytest.raises(ScanConflict, match="unfinished"):
        await begin(database, service, fence, expected=state.version)
    final = await page(database, service, fence, state, final=True)
    complete = (await reconcile(database, service, fence, final.state)).state
    assert await begin(database, service, fence, cycle=state.cycle_id) == complete
    next_cycle = await begin(database, service, fence, expected=complete.version)
    assert next_cycle.cycle_id != complete.cycle_id
    assert next_cycle.version.sweep_id != complete.version.sweep_id
    assert next_cycle.phase == "collecting"
    with pytest.raises(ScanConflict, match="unfinished"):
        await begin(
            database,
            service,
            fence,
            cycle=complete.cycle_id,
            expected=next_cycle.version,
            restart=True,
        )
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(CaptureScan)) == 1


async def test_reconcile_is_bounded_and_resumes_after_worker_loss(database, source):
    service, fence = source
    await capture(database, service, fence, *(observation(str(n)) for n in range(3)))
    state = await begin(database, service, fence)
    final = await page(database, service, fence, state, final=True)
    first = await reconcile(database, service, fence, final.state, limit=1)
    assert first.state.phase == "reconciling" and len(first.capture.changes) == 1
    resumed = await begin(database, service, fence, cycle=state.cycle_id)
    assert resumed == first.state
    last = await reconcile(database, service, fence, resumed)
    assert last.state.phase == "complete" and len(last.capture.changes) == 2
    with pytest.raises(ScanConflict):
        await page(database, service, fence, last.state, observation())


async def test_scope_identity_null_and_empty_are_distinct_and_pages_cannot_cross(database, source):
    service, fence = source
    state = await begin(database, service, fence)
    with pytest.raises(ScanConflict, match="outside"):
        await page(database, service, fence, state, observation(container_id=""))
    async with database() as db:
        other = await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=CompletedScope(record_type="event", container_id=""),
                cycle_id=state.cycle_id,
                fingerprint=FINGERPRINT,
            ),
        )
    assert state.version.sweep_id != other.version.sweep_id
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(CaptureScan)) == 2
        assert await db.scalar(select(func.count()).select_from(Entity)) == 0


def test_progress_is_bounded_and_unknown_fields_rejected():
    with pytest.raises(ValidationError, match="64 KiB"):
        ScanContinuation(value={"cursor": "x" * 65536})
    with pytest.raises(ValidationError):
        ScanContinuation(value={}, range_start="unsupported")


async def test_concurrent_same_page_has_one_commit_and_one_cas_conflict(database, source):
    import asyncio

    service, fence = source
    state = await begin(database, service, fence)
    outcomes = await asyncio.gather(
        page(database, service, fence, state, observation("one")),
        page(database, service, fence, state, observation("two")),
        return_exceptions=True,
    )
    assert sum(isinstance(result, ScanConflict) for result in outcomes) == 1
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Entity)) == 1
        assert await db.scalar(select(func.count()).select_from(EntityChange)) == 1
        assert (await db.scalar(select(CaptureScan))).revision == 2


@pytest.mark.parametrize("database", ["legacy"], indirect=True)
async def test_scan_migration_preserves_existing_legacy_records(database, source):
    service, fence = source
    state = await begin(database, service, fence)
    final = await page(database, service, fence, state, observation(), final=True)
    assert (await reconcile(database, service, fence, final.state)).state.phase == "complete"
    async with database() as db:
        legacy = await db.scalar(select(Entity).where(Entity.record_revision == 0))
        assert legacy is not None and legacy.entity_id == "legacy-native-key"


async def test_root_refresh_does_not_restart_completed_child_in_same_cycle(database, source):
    service, fence = source
    child = await begin(database, service, fence)
    final = await page(database, service, fence, child, final=True)
    complete = (await reconcile(database, service, fence, final.state)).state
    root_scope = CompletedScope(record_type="channel")
    async with database() as db:
        root = await service.begin_scan(
            db,
            BeginScan(
                fence=fence, scope=root_scope, cycle_id=child.cycle_id, fingerprint=FINGERPRINT
            ),
        )
    async with database() as db:
        refreshed = await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=root_scope,
                cycle_id=child.cycle_id,
                fingerprint=FINGERPRINT,
                expected=root.version,
                restart=True,
            ),
        )
    assert refreshed.version.sweep_id != root.version.sweep_id
    assert await begin(database, service, fence, cycle=child.cycle_id) == complete
