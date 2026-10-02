"""Actual driver/pipeline and PostgreSQL, with an explicitly synthetic native-page source."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.cycle_models import CycleConfiguration, ProviderCheckpoint
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    CapturePlan,
    InvalidCaptureCheckpoint,
)
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.scan_models import ScanContinuation
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components
from airweave.domains.entities.canonical.tests.test_changes_cycles import record
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.entity import Entity


class NativeFixture:
    canonical_record_types = ("message",)
    canonical_container_parents = {}
    capture_cycle_configuration = CycleConfiguration(
        fingerprint="f" * 64, parents={"message": (None,)}, known_object_validation=("message",)
    )

    def __init__(self, *, stop=False, expired=0):
        self.stop, self.expired = stop, expired
        self.plans = []
        self.pages = []
        self.refreshes = []

    async def prepare_cycle(self, previous):
        self.plans.append(previous)
        return CapturePlan(
            starting_checkpoint=ProviderCheckpoint(value={"start": str(len(self.plans))})
        )

    def initial_continuation(self, cycle):
        return ScanContinuation(
            value={"start": next(iter(cycle.starting_checkpoint.value.values())), "page": 0}
        )

    async def capture_page(self, scope, continuation, *, files, parent=None):
        self.pages.append(continuation.value)
        if self.expired:
            self.expired -= 1
            raise InvalidCaptureCheckpoint("synthetic expired history")
        if continuation.value["page"] == 0:
            return CapturePage(
                records=(record("listed"),),
                continuation=ScanContinuation(value={**continuation.value, "page": 1}),
            )
        if self.stop:
            raise ConnectionError("synthetic interruption")
        return CapturePage(
            records=(),
            continuation=ScanContinuation(value={**continuation.value, "page": 2}),
            final=True,
            provider_checkpoint=ProviderCheckpoint(value={"terminal": "done"}),
        )

    async def refresh_known(self, old, *, files):
        self.refreshes.append(old.identity.native_id)
        return record(old.identity.native_id)

    def child_scope(self, parent, record_type):
        raise AssertionError("No child scope")

    async def confirm_absent(self, record):
        raise AssertionError("Exact hydration must replace speculative absence")


def pipeline(database, source, connector, *, attempt=1):
    service, fence = source
    ctx, _, runtime, bus = components(database, source)
    capture = CanonicalCapturePipeline(
        service,
        database,
        bus,
        connector.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id if attempt == 1 else uuid4(), number=attempt),
        connector.canonical_container_parents,
        page_source=connector,
        files=MagicMock(),
    )
    runtime.canonical_capture = capture
    return ctx, runtime, capture


async def test_actual_pipeline_resumes_plan_and_hydrates_known_omissions(database, source):
    service, fence = source
    async with database() as db:
        await service.capture(db, CaptureBatch(fence=fence, records=(record("omitted"),)))
    connector = NativeFixture(stop=True)
    ctx, runtime, capture = pipeline(database, source, connector)
    await capture.start(ctx)
    with pytest.raises(ConnectionError, match="interruption"):
        await capture.run_scans(ctx, runtime, AsyncMock())
    connector.stop = False
    ctx, runtime, capture = pipeline(database, source, connector, attempt=2)
    await capture.start(ctx)
    await capture.run_scans(ctx, runtime, AsyncMock())
    await capture.save_checkpoint(ctx, runtime)
    assert len(connector.plans) == 1
    assert [p["page"] for p in connector.pages] == [0, 1, 1]
    assert connector.refreshes == ["omitted"]
    async with database() as db:
        rows = (await db.scalars(select(Entity))).all()
        assert {r.native_id for r in rows if r.deleted_at is None} == {"listed", "omitted"}
        cycle = await service.read_cycle(db, capture._writer())
    assert cycle.phase == "complete"
    assert cycle.promoted_checkpoint.checkpoint.value == {"terminal": "done"}
    assert cycle.last_full_capture.discovery == "scope_enumeration_complete"


@pytest.mark.parametrize("expirations", [1, 2])
async def test_expired_checkpoint_restarts_whole_cycle_once(database, source, expirations):
    connector = NativeFixture(expired=expirations)
    ctx, runtime, capture = pipeline(database, source, connector)
    await capture.start(ctx)
    if expirations == 2:
        with pytest.raises(InvalidCaptureCheckpoint):
            await capture.run_scans(ctx, runtime, AsyncMock())
    else:
        await capture.run_scans(ctx, runtime, AsyncMock())
        await capture.save_checkpoint(ctx, runtime)
    assert len(connector.plans) == 2
    assert [p["start"] for p in connector.pages[:2]] == ["1", "2"]
    async with database() as db:
        current = await source[0].read_cycle(db, capture._writer())
    assert current.starting_checkpoint.value == {"start": "2"}
    assert current.phase == ("active" if expirations == 2 else "complete")
    if expirations == 2:
        assert current.promoted_checkpoint is None


async def test_driver_runs_changes_without_enumeration_or_omission_cleanup(database, source):
    from airweave.domains.entities.canonical.cycle_models import BeginCycle

    connector = NativeFixture()
    ctx, runtime, capture = pipeline(database, source, connector)
    await capture.start(ctx)
    await capture.run_scans(ctx, runtime, AsyncMock())
    await capture.save_checkpoint(ctx, runtime)
    service, fence = source
    async with database() as db:
        await service.capture(db, CaptureBatch(fence=fence, records=(record("not-in-delta"),)))
        previous = await service.read_cycle(db, fence)
        await service.begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                configuration=connector.capture_cycle_configuration,
                expected=previous.version,
                mode="changes",
            ),
        )
    connector.pages.clear()
    ctx, runtime, capture = pipeline(database, source, connector, attempt=2)
    await capture.start(ctx)
    await capture.run_scans(ctx, runtime, AsyncMock())
    await capture.save_checkpoint(ctx, runtime)
    assert connector.refreshes == []
    assert len(connector.plans) == 1
    async with database() as db:
        retained = await db.scalar(select(Entity).where(Entity.native_id == "not-in-delta"))
        current = await service.read_cycle(db, capture._writer())
    assert retained.deleted_at is None
    assert current.mode == "changes" and current.phase == "complete"
    assert current.last_full_capture == previous.last_full_capture


async def test_exhaustive_known_validation_is_enforced_by_sql(database, source):
    from datetime import datetime, timezone

    from airweave.domains.entities.canonical.cycle_models import BeginCycle
    from airweave.domains.entities.canonical.requests import CompletedScope
    from airweave.domains.entities.canonical.scan_models import (
        BeginScan,
        CommitOmission,
        CommitScanPage,
        ReconcileScan,
    )
    from airweave.domains.entities.canonical.scan_store import ScanConflict

    service, fence = source
    config = NativeFixture.capture_cycle_configuration
    scope = CompletedScope(record_type="message")
    async with database() as db:
        old = (
            (await service.capture(db, CaptureBatch(fence=fence, records=(record("omitted"),))))
            .changes[0]
            .record
        )
        cycle = await service.begin_cycle(db, BeginCycle(fence=fence, configuration=config))
        state = await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=scope,
                cycle_id=cycle.version.cycle_id,
                fingerprint=config.fingerprint,
            ),
        )
        state = (
            await service.commit_scan_page(
                db,
                CommitScanPage(
                    fence=fence,
                    scope=scope,
                    cycle_id=state.cycle_id,
                    expected=state.version,
                    records=(),
                    continuation=ScanContinuation(),
                    final=True,
                ),
            )
        ).state
    with pytest.raises(ScanConflict, match="still require"):
        async with database() as db:
            await service.reconcile_scan(
                db,
                ReconcileScan(
                    fence=fence,
                    scope=scope,
                    cycle_id=state.cycle_id,
                    expected=state.version,
                    observed_at=datetime.now(timezone.utc),
                ),
            )
    async with database() as db:
        refreshed = await service.commit_omission(
            db,
            CommitOmission(
                fence=fence, state=state, expected_record=old, observation=record("omitted")
            ),
        )
        result = await service.reconcile_scan(
            db,
            ReconcileScan(
                fence=fence,
                scope=scope,
                cycle_id=state.cycle_id,
                expected=refreshed.state.version,
                observed_at=datetime.now(timezone.utc),
            ),
        )
    assert result.state.phase == "complete" and result.capture.changes == ()


@pytest.mark.parametrize("force_flag", ["force_full_sync", "skip_load"])
async def test_explicit_full_sync_restarts_delta_but_completed_job_retry_is_idempotent(
    database, source, force_flag
):
    from airweave.domains.entities.canonical.cycle_models import BeginCycle

    connector = NativeFixture()
    ctx, runtime, capture = pipeline(database, source, connector)
    await capture.start(ctx)
    await capture.run_scans(ctx, runtime, AsyncMock())
    await capture.save_checkpoint(ctx, runtime)
    service, fence = source
    async with database() as db:
        before = await service.read_cycle(db, fence)
        delta = await service.begin_cycle(
            db,
            BeginCycle(
                fence=fence,
                configuration=connector.capture_cycle_configuration,
                expected=before.version,
                mode="changes",
            ),
        )
    ctx, runtime, capture = pipeline(database, source, connector, attempt=2)
    ctx.force_full_sync = force_flag == "force_full_sync"
    ctx.execution_config.cursor.skip_load = force_flag == "skip_load"
    await capture.start(ctx)
    await capture.run_scans(ctx, runtime, AsyncMock())
    await capture.save_checkpoint(ctx, runtime)
    async with database() as db:
        completed = await service.read_cycle(db, capture._writer())
    assert completed.mode == "full" and completed.version.cycle_id != delta.version.cycle_id
    assert completed.starting_checkpoint.value == {"start": "2"}
    pages = len(connector.pages)
    ctx, runtime, capture = pipeline(database, source, connector, attempt=3)
    ctx.force_full_sync = force_flag == "force_full_sync"
    ctx.execution_config.cursor.skip_load = force_flag == "skip_load"
    await capture.start(ctx)
    await capture.run_scans(ctx, runtime, AsyncMock())
    await capture.save_checkpoint(ctx, runtime)
    assert len(connector.pages) == pages and len(connector.plans) == 2
