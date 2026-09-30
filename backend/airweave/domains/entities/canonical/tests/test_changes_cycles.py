"""Real SQL: provider progress never substitutes for committed capture or inventory evidence."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CompleteCycle,
    CycleConfiguration,
    ProviderCheckpoint,
    RestartCycle,
    TerminalCheckpoint,
)
from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.store import StaleWriter
from airweave.models.entity import Entity
from airweave.models.sync_cursor import SyncCursor

CONFIG = CycleConfiguration(fingerprint="a" * 64, parents={"message": (None,)})
SCOPE = CompletedScope(record_type="message")


def checkpoint(value):
    return ProviderCheckpoint(value={"history_id": value})


def record(native_id, **kwargs):
    return CaptureRecord(
        identity=RecordIdentity(record_type="message", native_id=native_id),
        payload={"id": native_id},
        observed_at=datetime.now(timezone.utc),
        **kwargs,
    )


async def begin(database, service, fence, **kwargs):
    async with database() as db:
        return await service.begin_cycle(
            db, BeginCycle(fence=fence, configuration=CONFIG, **kwargs)
        )


async def start(database, service, fence, cycle):
    async with database() as db:
        previous = await service.read_scan(db, fence, SCOPE)
        return await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=SCOPE,
                cycle_id=cycle.version.cycle_id,
                fingerprint=CONFIG.fingerprint,
                expected=previous.version if previous else None,
            ),
        )


async def page(database, service, fence, scan, *records, final=True, value=None):
    async with database() as db:
        result = await service.commit_scan_page(
            db,
            CommitScanPage(
                fence=fence,
                scope=SCOPE,
                cycle_id=scan.cycle_id,
                expected=scan.version,
                records=records,
                continuation=ScanContinuation(value={"terminal": "200"}),
                final=final,
                provider_checkpoint=checkpoint(
                    value or ("300" if scan.mode == "changes" else "200")
                )
                if final
                else None,
            ),
        )
        return result.state


async def complete(database, service, fence, cycle, scan, value="200"):
    async with database() as db:
        cycle = await service.read_cycle(db, fence)
        return await service.complete_cycle(
            db,
            CompleteCycle(
                fence=fence,
                expected=cycle.version,
                terminal_checkpoint=TerminalCheckpoint(
                    record_type="message", expected=scan.version, checkpoint=checkpoint(value)
                ),
            ),
        )


async def baseline(database, service, fence):
    cycle = await begin(database, service, fence, starting_checkpoint=checkpoint("100"))
    scan = await start(database, service, fence, cycle)
    scan = await page(database, service, fence, scan, record("A"), record("B"))
    assert scan.phase == "reconciling"
    async with database() as db:
        result = await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=SCOPE,
                cycle_id=scan.cycle_id,
                expected=scan.version,
                observed_at=datetime.now(timezone.utc),
            ),
        )
    return await complete(database, service, fence, cycle, result.state)


async def test_changes_commit_without_absence_and_promote_only_after_terminal(database, source):
    service, fence = source
    full = await baseline(database, service, fence)
    delta = await begin(database, service, fence, mode="changes", expected=full.version)
    assert delta.starting_checkpoint == checkpoint("200")
    scan = await start(database, service, fence, delta)
    scan = await page(database, service, fence, scan, record("A"), final=False)
    async with database() as db:
        state = await service.read_cycle(db, fence)
        assert state.promoted_checkpoint == full.promoted_checkpoint
    with pytest.raises(CycleConflict, match="incomplete"):
        await complete(database, service, fence, delta, scan, "300")
    with pytest.raises(ScanConflict, match="Changes"):
        async with database() as db:
            await service.scan_missing(db, fence, scan)
    with pytest.raises(ScanConflict, match="Changes"):
        async with database() as db:
            await service.reconcile_scan(
                db,
                ReconcileScan(
                    fence=fence,
                    scope=SCOPE,
                    cycle_id=scan.cycle_id,
                    expected=scan.version,
                    observed_at=datetime.now(timezone.utc),
                ),
            )
    scan = await page(database, service, fence, scan)
    assert scan.phase == "complete" and scan.completed_at is not None
    # A fresh session recovers after a crash between the final page and cycle publication.
    async with database() as db:
        scan = await service.read_scan(db, fence, SCOPE)
    finished = await complete(database, service, fence, delta, scan, "300")
    assert finished.last_full_capture == full.last_full_capture
    assert finished.promoted_checkpoint.checkpoint == checkpoint("300")
    assert await begin(database, service, fence, mode="changes") == finished  # lost ACK
    async with database() as db:
        rows = (await db.scalars(select(Entity))).all()
        assert {r.native_id for r in rows if r.deleted_at is None} == {"A", "B"}
        cursor = await db.scalar(select(SyncCursor))
        assert cursor.cursor_data["canonical_checkpoint"]["observed_change_sequence"] >= 2
        coverage = (await capture_coverage(db, fence.organization_id, (fence.sync_id,)))[
            fence.sync_id
        ]
    assert coverage.mode == "changes" and coverage.discovery == "scope_enumeration_complete"
    assert "history_id" not in coverage.model_dump_json()
    assert "configuration_digest" not in coverage.model_dump_json()


async def test_changes_rejects_no_baseline_wrong_start_and_changed_guarantee(database, source):
    service, fence = source
    with pytest.raises(CycleConflict, match="compatible"):
        await begin(database, service, fence, mode="changes")
    full = await baseline(database, service, fence)
    with pytest.raises(CycleConflict, match="last promoted"):
        await begin(
            database,
            service,
            fence,
            mode="changes",
            expected=full.version,
            starting_checkpoint=checkpoint("999"),
        )
    changed = CONFIG.model_copy(update={"completion_policies": {"message": "discovery_only"}})
    with pytest.raises(CycleConflict, match="compatible"):
        async with database() as db:
            await service.begin_cycle(
                db,
                BeginCycle(
                    fence=fence, configuration=changed, expected=full.version, mode="changes"
                ),
            )


async def test_terminal_version_and_lost_writer_cannot_publish(database, source):
    service, fence = source
    full = await baseline(database, service, fence)
    delta = await begin(database, service, fence, mode="changes", expected=full.version)
    first = await start(database, service, fence, delta)
    final = await page(database, service, fence, first, record("A"))
    with pytest.raises(CycleConflict, match="Terminal scan"):
        await complete(database, service, fence, delta, first, "300")
    async with database() as db:
        newer = await service.activate_writer(
            db,
            organization_id=fence.organization_id,
            sync_id=fence.sync_id,
            job_id=fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    with pytest.raises(StaleWriter):
        await complete(database, service, fence, delta, final, "300")
    async with database() as db:
        state = await service.read_cycle(db, newer)
        assert state.promoted_checkpoint == full.promoted_checkpoint
    finished = await complete(database, service, newer, delta, final, "300")
    assert finished.completed_job_id == newer.job_id


async def test_restart_preserves_successful_history_not_partial_delta(database, source):
    service, fence = source
    full = await baseline(database, service, fence)
    delta = await begin(database, service, fence, mode="changes", expected=full.version)
    scan = await start(database, service, fence, delta)
    with pytest.raises(ScanConflict, match="never absence"):
        await page(
            database, service, fence, scan, record("A", kind="delete", removal_reason="absent")
        )
    async with database() as db:
        delta = await service.read_cycle(db, fence)
        fresh = await service.restart_cycle(
            db,
            RestartCycle(
                fence=fence,
                expected=delta.version,
                configuration=CONFIG,
                starting_checkpoint=checkpoint("500"),
            ),
        )
    assert fresh.mode == "full" and fresh.starting_checkpoint == checkpoint("500")
    assert fresh.promoted_checkpoint == full.promoted_checkpoint
    assert fresh.last_full_capture == full.last_full_capture


async def test_exact_terminal_version_cannot_publish_different_checkpoint(database, source):
    service, fence = source
    full = await baseline(database, service, fence)
    delta = await begin(database, service, fence, mode="changes", expected=full.version)
    scan = await page(database, service, fence, await start(database, service, fence, delta))
    with pytest.raises(CycleConflict, match="does not match committed"):
        await complete(database, service, fence, delta, scan, "forged")
    async with database() as db:
        persisted = await service.read_scan(db, fence, SCOPE)
        state = await service.read_cycle(db, fence)
    assert persisted.provider_checkpoint == checkpoint("300")
    assert persisted.continuation.value == {"terminal": "200"}
    assert state.promoted_checkpoint == full.promoted_checkpoint


async def test_full_refresh_without_checkpoint_clears_old_delta_authority(database, source):
    service, fence = source
    old = await baseline(database, service, fence)
    fresh = await begin(database, service, fence, expected=old.version)
    scan = await start(database, service, fence, fresh)
    async with database() as db:
        result = await service.commit_scan_page(
            db,
            CommitScanPage(
                fence=fence,
                scope=SCOPE,
                cycle_id=scan.cycle_id,
                expected=scan.version,
                records=(record("B"),),
                continuation=ScanContinuation(),
                final=True,
            ),
        )
        scan = result.state
    async with database() as db:
        await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=SCOPE,
                cycle_id=scan.cycle_id,
                expected=scan.version,
                observed_at=datetime.now(timezone.utc),
            ),
        )
        current = await service.read_cycle(db, fence)
        done = await service.complete_cycle(
            db, CompleteCycle(fence=fence, expected=current.version)
        )
    assert done.promoted_checkpoint is None
    assert done.last_full_capture.cycle_id == fresh.version.cycle_id
    with pytest.raises(CycleConflict, match="compatible"):
        await begin(database, service, fence, mode="changes", expected=done.version)
    async with database() as db:
        assert (
            await db.scalar(select(Entity).where(Entity.native_id == "A"))
        ).deleted_at is not None


async def test_incomplete_full_evidence_stays_incomplete_during_changes(database, source):
    service, fence = source
    config = CONFIG.model_copy(update={"completion_policies": {"message": "discovery_only"}})
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
    scan = await start(database, service, fence, cycle)
    scan = await page(database, service, fence, scan, record("A"))
    async with database() as db:
        result = await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=SCOPE,
                cycle_id=scan.cycle_id,
                expected=scan.version,
                observed_at=datetime.now(timezone.utc),
            ),
        )
    done = await complete(database, service, fence, cycle, result.state)
    async with database() as db:
        await service.begin_cycle(
            db, BeginCycle(fence=fence, configuration=config, expected=done.version, mode="changes")
        )
        coverage = (await capture_coverage(db, fence.organization_id, (fence.sync_id,)))[
            fence.sync_id
        ]
    assert coverage.phase == "active" and coverage.mode == "changes"
    assert coverage.discovery == coverage.last_full_capture.discovery == "incomplete"
    assert coverage.provider_checkpoint_promoted_at is not None


async def test_changes_forest_is_explicitly_unsupported(database, source):
    service, fence = source
    config = CycleConfiguration(
        fingerprint="b" * 64, parents={"channel": (None,), "message": ("channel",)}
    )
    with pytest.raises(CycleConflict, match="one independent root"):
        async with database() as db:
            await service.begin_cycle(
                db, BeginCycle(fence=fence, configuration=config, mode="changes")
            )


@pytest.mark.parametrize("final, boundary", [(False, "early"), (True, None)])
async def test_invalid_checkpoint_page_rolls_back_records_and_progress(
    database, source, final, boundary
):
    service, fence = source
    full = await baseline(database, service, fence)
    delta = await begin(database, service, fence, mode="changes", expected=full.version)
    scan = await start(database, service, fence, delta)
    with pytest.raises(ScanConflict, match="[Tt]erminal"):
        async with database() as db:
            await service.commit_scan_page(
                db,
                CommitScanPage(
                    fence=fence,
                    scope=SCOPE,
                    cycle_id=scan.cycle_id,
                    expected=scan.version,
                    records=(record("uncommitted"),),
                    continuation=ScanContinuation(),
                    final=final,
                    provider_checkpoint=checkpoint(boundary) if boundary else None,
                ),
            )
    async with database() as db:
        assert await service.read_scan(db, fence, SCOPE) == scan
        assert await db.scalar(select(Entity.id).where(Entity.native_id == "uncommitted")) is None
        state = await service.read_cycle(db, fence)
    assert state.promoted_checkpoint == full.promoted_checkpoint
