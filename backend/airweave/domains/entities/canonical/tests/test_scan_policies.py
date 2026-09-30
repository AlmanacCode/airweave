"""Real SQL: incomplete discovery cannot acquire exhaustive deletion authority."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.coverage import capture_coverage
from airweave.domains.entities.canonical.cycle_models import (
    BeginCycle,
    CompleteCycle,
    CycleConfiguration,
)
from airweave.domains.entities.canonical.cycle_store import CycleConflict
from airweave.domains.entities.canonical.requests import CompletedScope
from airweave.domains.entities.canonical.scan_models import (
    BeginScan,
    CommitOmission,
    CommitScanPage,
    ReconcileScan,
    ScanContinuation,
)
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.tests.helpers import capture, observation


async def start(database, service, fence, policy):
    config = CycleConfiguration(
        fingerprint="c" * 64, parents={"event": (None,)}, completion_policies={"event": policy}
    )
    async with database() as db:
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
    async with database() as db:
        state = await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=CompletedScope(record_type="event"),
                cycle_id=cycle.version.cycle_id,
                fingerprint=config.fingerprint,
            ),
        )
    async with database() as db:
        result = await service.commit_scan_page(
            db,
            CommitScanPage(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                records=(),
                continuation=ScanContinuation(),
                final=True,
            ),
        )
    return result.state, config


async def finish(database, service, fence, state):
    async with database() as db:
        return await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=state.scope,
                cycle_id=state.cycle_id,
                expected=state.version,
                observed_at=datetime.now(timezone.utc),
            ),
        )


@pytest.mark.parametrize("policy", ["discovery_only", "discovery_with_validation"])
async def test_incomplete_discovery_preserves_known_record_and_reports_policy(
    database, source, policy
):
    service, fence = source
    original = (await capture(database, service, fence, observation())).changes[0].record
    state, config = await start(database, service, fence, policy)
    assert state.completion_policy == policy
    if policy == "discovery_with_validation":
        with pytest.raises(ScanConflict, match="still require"):
            await finish(database, service, fence, state)
        async with database() as db:
            request = CommitOmission(
                fence=fence,
                state=state,
                expected_record=original,
                observation=observation(payload={"summary": "Fresh"}),
            )
            result = await service.commit_omission(db, request)
        assert result.capture.changes[0].record.payload == {"summary": "Fresh"}
        with pytest.raises(ScanConflict):
            async with database() as db:
                await service.commit_omission(db, request)  # Lost acknowledgement.
        state = result.state
    complete = await finish(database, service, fence, state)
    assert complete.state.phase == "complete" and complete.capture.changes == ()
    async with database() as db:
        coverage = await capture_coverage(db, fence.organization_id, (fence.sync_id,))
        assert coverage[fence.sync_id].discovery == "incomplete"
        assert coverage[fence.sync_id].policies == {"event": policy}
        assert await capture_coverage(db, uuid4(), (fence.sync_id,)) == {}
    with pytest.raises(CycleConflict):
        async with database() as db:
            await service.begin_cycle(
                db,
                BeginCycle(
                    fence=fence,
                    configuration=config.model_copy(
                        update={"completion_policies": {"event": "exhaustive"}}
                    ),
                ),
            )

    async with database() as db:
        current = await service.read_cycle(db, fence)
    async with database() as db:
        await service.complete_cycle(db, CompleteCycle(fence=fence, expected=current.version))
    async with database() as db:
        completed_coverage = await capture_coverage(db, fence.organization_id, (fence.sync_id,))
    assert completed_coverage[fence.sync_id].phase == "complete"
    assert completed_coverage[fence.sync_id].discovery == "incomplete"


async def test_exact_validation_restores_tombstone_but_rejects_stale_or_wrong_identity(
    database, source
):
    service, fence = source
    deleted = observation(kind="delete", removal_reason="scope_removed")
    original = (await capture(database, service, fence, deleted)).changes[0].record
    state, _ = await start(database, service, fence, "discovery_with_validation")
    async with database() as db:
        missing = await service.scan_missing(db, fence, state)
    assert missing[0].id == original.id
    for change in [
        observation("other"),
        observation(container_id="other"),
        observation(kind="delete", removal_reason="absent"),
    ]:
        with pytest.raises(ScanConflict):
            async with database() as db:
                await service.commit_omission(
                    db,
                    CommitOmission(
                        fence=fence, state=state, expected_record=original, observation=change
                    ),
                )
    newer = (
        (await capture(database, service, fence, observation(payload={"v": 2}))).changes[0].record
    )
    with pytest.raises(ScanConflict):
        async with database() as db:
            await service.commit_omission(
                db,
                CommitOmission(
                    fence=fence, state=state, expected_record=original, observation=observation()
                ),
            )
    async with database() as db:
        result = await service.commit_omission(
            db,
            CommitOmission(
                fence=fence, state=state, expected_record=newer, observation=observation()
            ),
        )
    assert result.capture.changes[0].record.deleted_at is None
    await finish(database, service, fence, result.state)


async def test_restore_by_exact_validation_and_resume_after_lost_ack(database, source):
    service, fence = source
    original = (
        (
            await capture(
                database, service, fence, observation(kind="delete", removal_reason="scope_removed")
            )
        )
        .changes[0]
        .record
    )
    state, _ = await start(database, service, fence, "discovery_with_validation")
    async with database() as db:
        restored = await service.commit_omission(
            db,
            CommitOmission(
                fence=fence, state=state, expected_record=original, observation=observation()
            ),
        )
    assert restored.capture.changes[0].record.deleted_at is None
    assert restored.capture.changes[0].record.content_access == "available"
    async with database() as db:
        fresh_fence = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            fence.job_id,
            attempt_id=uuid4(),
            attempt_number=2,
        )
    async with database() as db:
        resumed = await service.read_scan(db, fresh_fence, state.scope)
        assert resumed.version == restored.state.version
    complete = await finish(database, service, fresh_fence, resumed)
    assert complete.state.phase == "complete"


async def test_exhaustive_default_cannot_use_omission_mutation(database, source):
    service, fence = source
    original = (await capture(database, service, fence, observation())).changes[0].record
    state, _ = await start(database, service, fence, "exhaustive")
    with pytest.raises(ScanConflict, match="does not accept"):
        async with database() as db:
            await service.commit_omission(
                db,
                CommitOmission(
                    fence=fence, state=state, expected_record=original, observation=observation()
                ),
            )
    finished = await finish(database, service, fence, state)
    assert finished.capture.changes[0].record.deleted_at is not None


async def test_driver_retries_failed_exact_read_without_absence_removal(database, source):
    from unittest.mock import AsyncMock, Mock

    from airweave.domains.entities.canonical.page_source import CapturePage
    from airweave.domains.storage.file_service import FileService
    from airweave.domains.sync_pipeline.canonical_scan import CanonicalScanDriver

    service, fence = source
    original = (await capture(database, service, fence, observation())).changes[0].record

    class Discovery:
        capture_cycle_configuration = CycleConfiguration(
            fingerprint="a" * 64,
            parents={"event": (None,)},
            completion_policies={"event": "discovery_with_validation"},
        )
        failed = False

        async def capture_page(self, scope, continuation, *, files, parent=None):
            return CapturePage(records=(), continuation=ScanContinuation(), final=True)

        async def refresh_known(self, record, *, files):
            if not self.failed:
                self.failed = True
                raise ConnectionError("Synthetic provider unavailable")
            assert record.id == original.id
            return observation(payload={"fresh": True})

    provider = Discovery()
    driver = CanonicalScanDriver(
        service, database, fence, provider, AsyncMock(), AsyncMock(), Mock(spec=FileService)
    )
    with pytest.raises(ConnectionError):
        await driver.run()
    async with database() as db:
        pending = await service.read_scan(db, fence, CompletedScope(record_type="event"))
        assert pending.phase == "reconciling"
        remaining = await service.scan_missing(db, fence, pending)
        assert remaining[0].deleted_at is None
    await driver.run()
    async with database() as db:
        completed = await service.read_scan(db, fence, pending.scope)
        assert completed.phase == "complete"
