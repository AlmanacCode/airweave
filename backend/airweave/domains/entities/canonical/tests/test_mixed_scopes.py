"""Real SQL and production page driver with a synthetic mixed-scope source."""

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.coverage import capture_coverage, mixed_scope_summary
from airweave.domains.entities.canonical.cycle_models import CycleConfiguration, ProviderCheckpoint
from airweave.domains.entities.canonical.page_source import (
    CapturePage,
    CapturePlan,
    InvalidScopeCheckpoint,
)
from airweave.domains.entities.canonical.requests import (
    CaptureRecord,
    CompletedScope,
    RecordIdentity,
)
from airweave.domains.entities.canonical.scan_models import ReconcileScan, ScanContinuation
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.scope_execution import ScopePlan
from airweave.domains.entities.canonical.tests.test_capture_pipeline import components
from airweave.domains.sources.exceptions import SourceServerError
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt
from airweave.models.capture_scan import CaptureScan
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob


class MixedSource:
    canonical_record_types = ("calendar", "event", "event_occurrence")
    canonical_container_parents = {"event": "calendar", "event_occurrence": "calendar"}
    capture_cycle_configuration = CycleConfiguration(
        fingerprint="a" * 64,
        parents={"calendar": (None,), "event": ("calendar",), "event_occurrence": ("calendar",)},
        scope_changes=("event",),
    )

    def __init__(self, *, fail_window=False, expire=False, window="fixed"):
        self.fail_window, self.expire, self.window = fail_window, expire, window
        self.calls = []
        self.prepared = 0

    async def prepare_cycle(self, previous):
        self.prepared += 1
        return CapturePlan(mode="mixed", source_plan={"window": self.window})

    async def prepare_scope(self, scope, cycle, previous, *, parent, force_full):
        context = (
            {"parameters": "masters"}
            if scope.record_type == "event"
            else {"window": cycle.source_plan["window"]}
        )
        published = previous.execution.published if previous and previous.execution else None
        changes = (
            scope.record_type == "event" and published and published.checkpoint and not force_full
        )
        return ScopePlan(
            mode="changes" if changes else "full",
            request_context=context,
            starting_checkpoint=published.checkpoint if changes else None,
        )

    def initial_scope_continuation(self, scope, cycle, plan):
        return ScanContinuation(
            value={
                "mode": plan.mode,
                "token": plan.starting_checkpoint.value if plan.starting_checkpoint else None,
                "window": cycle.source_plan["window"],
            }
        )

    def child_scope(self, parent, record_type):
        return CompletedScope(
            record_type=record_type, container_id=parent.identity.native_id, parent=parent.identity
        )

    async def confirm_absent(self, record):
        raise AssertionError("Synthetic membership is unchanged")

    async def capture_page(self, scope, continuation, *, files, parent=None):
        self.calls.append((scope.record_type, continuation.value))
        if scope.record_type == "event_occurrence" and self.fail_window:
            raise SourceServerError("Synthetic interruption", status_code=503)
        if scope.record_type == "event" and continuation.value["mode"] == "changes" and self.expire:
            self.expire = False
            raise InvalidScopeCheckpoint("Synthetic410")
        identities = (
            ("cal",)
            if scope.record_type == "calendar"
            else ("a",)
            if continuation.value["mode"] == "changes"
            else ("a", "b")
        )
        records = tuple(
            CaptureRecord(
                identity=RecordIdentity(
                    record_type=scope.record_type, native_id=native, container_id=scope.container_id
                ),
                payload={"id": native, "mode": continuation.value["mode"]},
                observed_at=datetime.now(timezone.utc),
            )
            for native in identities
        )
        return CapturePage(
            records=records,
            continuation=continuation,
            final=True,
            provider_checkpoint=ProviderCheckpoint(value={"sync_token": str(len(self.calls))})
            if scope.record_type == "event"
            else None,
        )


async def setup(database, source, connector, attempt=1):
    service, fence = source
    ctx, _, runtime, bus = components(database, source)
    pipeline = CanonicalCapturePipeline(
        service,
        database,
        bus,
        connector.canonical_record_types,
        CaptureAttempt(id=fence.attempt_id if attempt == 1 else uuid4(), number=attempt),
        connector.canonical_container_parents,
        page_source=connector,
        files=MagicMock(),
    )
    await pipeline.start(ctx)
    return pipeline, ctx, runtime


async def no_limits():
    pass


async def run(pipeline, ctx, runtime):
    await pipeline.run_scans(ctx, runtime, no_limits)
    await pipeline.cleanup_orphaned_entities(ctx, runtime)
    await pipeline.save_checkpoint(ctx, runtime)


async def next_job(database, source):
    service, fence = source
    async with database() as db:
        (await db.get(SyncJob, fence.job_id)).status = "completed"
        job = SyncJob(
            id=uuid4(),
            sync_id=fence.sync_id,
            organization_id=fence.organization_id,
            status="running",
        )
        db.add(job)
        await db.commit()
        return service, await service.activate_writer(
            db, fence.organization_id, fence.sync_id, job.id, attempt_id=uuid4(), attempt_number=1
        )


async def test_mixed_scope_resume_preserves_baseline_and_fixed_window(database, source):
    connector = MixedSource(fail_window=True)
    pipeline, ctx, runtime = await setup(database, source, connector)
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        event = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event"))
        old_full = event.execution_state["last_full"]
        assert event.phase == "complete" and old_full is not None
        coverage = (await capture_coverage(db, source[1].organization_id, (source[1].sync_id,)))[
            source[1].sync_id
        ]
        assert coverage.mode == "mixed" and coverage.scope_summary.unfinished == 1
    resumed = MixedSource(window="must-not-replace-active-window")
    pipeline, ctx, runtime = await setup(database, source, resumed, 2)
    await run(pipeline, ctx, runtime)
    assert resumed.prepared == 0
    assert [kind for kind, _ in resumed.calls] == ["calendar", "event_occurrence"]
    assert all(progress["window"] == "fixed" for _, progress in resumed.calls)
    source = await next_job(database, source)
    delta = MixedSource(window="next-window")
    pipeline, ctx, runtime = await setup(database, source, delta)
    await run(pipeline, ctx, runtime)
    assert [progress["mode"] for kind, progress in delta.calls if kind == "event"] == ["changes"]
    async with database() as db:
        event = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event"))
        assert event.execution_state["last_full"] == old_full
        assert (
            await db.scalar(
                select(Entity).where(
                    Entity.entity_definition_short_name == "event", Entity.native_id == "b"
                )
            )
        ).deleted_at is None
        coverage = (await capture_coverage(db, source[1].organization_id, (source[1].sync_id,)))[
            source[1].sync_id
        ]
        assert coverage.scope_summary.model_dump() == {
            "eligible": 3,
            "completed_full": 2,
            "completed_changes": 1,
            "unfinished": 0,
        }
        assert (
            coverage.last_full_capture is None and coverage.provider_checkpoint_promoted_at is None
        )
        assert coverage.discovery == "scope_enumeration_complete"


async def test_scope_expiry_restarts_only_event_inventory(database, source):
    first = MixedSource()
    pipeline, ctx, runtime = await setup(database, source, first)
    await run(pipeline, ctx, runtime)
    source = await next_job(database, source)
    connector = MixedSource(expire=True)
    pipeline, ctx, runtime = await setup(database, source, connector)
    await run(pipeline, ctx, runtime)
    assert [progress["mode"] for kind, progress in connector.calls if kind == "event"] == [
        "changes",
        "full",
    ]
    assert connector.prepared == 1
    assert sum(kind == "calendar" for kind, _ in connector.calls) == 1


async def test_changes_scope_rejects_absence_even_before_cycle_completion(database, source):
    pipeline, ctx, runtime = await setup(database, source, MixedSource())
    await run(pipeline, ctx, runtime)
    source = await next_job(database, source)
    pipeline, ctx, runtime = await setup(database, source, MixedSource(fail_window=True))
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        state = await source[0].read_scan(
            db, pipeline._writer(), CompletedScope(record_type="event", container_id="cal")
        )
        assert state.mode == "changes"
        with pytest.raises(ScanConflict, match="cannot reconcile"):
            await source[0].reconcile_scan(
                db,
                ReconcileScan(
                    fence=pipeline._writer(),
                    scope=state.scope,
                    cycle_id=state.cycle_id,
                    expected=state.version,
                    observed_at=datetime.now(timezone.utc),
                ),
            )


async def test_changed_capture_configuration_cannot_reuse_same_context_baseline(database, source):
    pipeline, ctx, runtime = await setup(database, source, MixedSource())
    await run(pipeline, ctx, runtime)
    source = await next_job(database, source)
    connector = MixedSource()
    connector.capture_cycle_configuration = connector.capture_cycle_configuration.model_copy(
        update={"fingerprint": "b" * 64}
    )
    pipeline, ctx, runtime = await setup(database, source, connector)
    # Synthetic adapter deliberately proposes an old token despite configuration change.
    with pytest.raises(ScanConflict, match="compatible full evidence"):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        row = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event"))
        assert row.fingerprint == "a" * 64


async def test_forced_mixed_cycle_resumes_intent_across_attempts_and_jobs(database, source):
    pipeline, ctx, runtime = await setup(database, source, MixedSource())
    await run(pipeline, ctx, runtime)
    source = await next_job(database, source)
    pipeline, ctx, runtime = await setup(database, source, MixedSource(fail_window=True))
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    forced = MixedSource(fail_window=True, window="forced-window")
    pipeline, ctx, runtime = await setup(database, source, forced, 2)
    ctx.force_full_sync = True
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    assert [p["mode"] for k, p in forced.calls if k == "event"] == ["full"]
    async with database() as db:
        initial = await source[0].read_cycle(db, pipeline._writer())
        assert initial.force_full_scopes
    repeated = MixedSource(fail_window=True, window="wrong")
    pipeline, ctx, runtime = await setup(database, source, repeated, 3)
    ctx.force_full_sync = True
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    assert repeated.prepared == 0 and not any(k == "event" for k, _ in repeated.calls)
    source = await next_job(database, source)
    resumed = MixedSource(window="also-wrong")
    pipeline, ctx, runtime = await setup(database, source, resumed)
    await run(pipeline, ctx, runtime)
    async with database() as db:
        completed = await source[0].read_cycle(db, pipeline._writer())
        assert completed.version.cycle_id == initial.version.cycle_id
        assert completed.source_plan == {"window": "forced-window"}
    assert resumed.prepared == 0


async def test_mixed_coverage_wire_from_sql(database, source, tmp_path, monkeypatch):
    import json

    from airweave.domains.entities.canonical.cycle_models import BeginCycle

    connector = MixedSource(fail_window=True)
    pipeline, ctx, runtime = await setup(database, source, connector)
    async with database() as db:
        await source[0].begin_cycle(
            db,
            BeginCycle(
                fence=pipeline._writer(),
                configuration=connector.capture_cycle_configuration,
                mode="mixed",
                source_plan={"window": "fixed"},
            ),
        )
        unknown = (await capture_coverage(db, source[1].organization_id, (source[1].sync_id,)))[
            source[1].sync_id
        ]
        assert unknown.scope_summary is None and unknown.discovery == "pending"
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        partial = (await capture_coverage(db, source[1].organization_id, (source[1].sync_id,)))[
            source[1].sync_id
        ]
        assert partial.scope_summary.unfinished == 1
    pipeline, ctx, runtime = await setup(database, source, MixedSource(), 2)
    await run(pipeline, ctx, runtime)
    async with database() as db:
        complete = (await capture_coverage(db, source[1].organization_id, (source[1].sync_id,)))[
            source[1].sync_id
        ]
        assert complete.scope_summary.unfinished == 0 and complete.phase == "complete"
        with monkeypatch.context() as patch:

            async def no_forest_query(*args):
                raise AssertionError("Lightweight coverage must not query the scope forest")

            patch.setattr(
                "airweave.domains.entities.canonical.coverage.mixed_scope_summary",
                no_forest_query,
            )
            lightweight = await capture_coverage(
                db,
                source[1].organization_id,
                (source[1].sync_id,),
                include_scope_summary=False,
            )
        assert source[1].sync_id not in lightweight
    assert complete.scope_summary.completed_changes == 0
    source = await next_job(database, source)
    delta = MixedSource(fail_window=True, window="next-window")
    pipeline, ctx, runtime = await setup(database, source, delta)
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    assert [value["mode"] for kind, value in delta.calls if kind == "event"] == ["changes"]
    async with database() as db:
        partial_delta = (
            await capture_coverage(db, source[1].organization_id, (source[1].sync_id,))
        )[source[1].sync_id]
        assert partial_delta.phase == "active" and partial_delta.discovery == "pending"
        assert partial_delta.scope_summary.model_dump() == {
            "eligible": 3,
            "completed_full": 1,
            "completed_changes": 1,
            "unfinished": 1,
        }
    resumed = MixedSource(window="must-preserve-next-window")
    pipeline, ctx, runtime = await setup(database, source, resumed, 2)
    await run(pipeline, ctx, runtime)
    assert [kind for kind, _ in resumed.calls] == ["calendar", "event_occurrence"]
    async with database() as db:
        complete_delta = (
            await capture_coverage(db, source[1].organization_id, (source[1].sync_id,))
        )[source[1].sync_id]
        assert complete_delta.phase == "complete"
        assert complete_delta.discovery == "scope_enumeration_complete"
        assert complete_delta.scope_summary.model_dump() == {
            "eligible": 3,
            "completed_full": 2,
            "completed_changes": 1,
            "unfinished": 0,
        }
        assert complete_delta.last_full_capture is None
        assert complete_delta.provider_checkpoint_promoted_at is None
        unchanged = await db.scalar(
            select(Entity).where(
                Entity.entity_definition_short_name == "event", Entity.native_id == "b"
            )
        )
        assert unchanged.deleted_at is None
    (tmp_path / "mixed-coverage.json").write_text(
        json.dumps(
            [
                item.model_dump(mode="json")
                for item in (unknown, partial, complete, partial_delta, complete_delta)
            ],
            indent=2,
        )
    )


async def test_parent_revival_invalidates_published_scope_history(database, source):
    from airweave.domains.entities.canonical.requests import CaptureBatch

    pipeline, ctx, runtime = await setup(database, source, MixedSource(fail_window=True))
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        old = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event"))
        epoch = old.execution_state["last_full"]["parent_visibility_epoch"]
        await source[0].capture(
            db,
            CaptureBatch(
                fence=pipeline._writer(),
                records=(
                    CaptureRecord(
                        identity=RecordIdentity(record_type="calendar", native_id="cal"),
                        payload={"id": "cal"},
                        kind="delete",
                        removal_reason="access_revoked",
                        observed_at=datetime.now(timezone.utc),
                    ),
                ),
            ),
        )
        await source[0].capture(
            db,
            CaptureBatch(
                fence=pipeline._writer(),
                records=(
                    CaptureRecord(
                        identity=RecordIdentity(record_type="calendar", native_id="cal"),
                        payload={"id": "cal"},
                        observed_at=datetime.now(timezone.utc),
                    ),
                ),
            ),
        )
    restored = MixedSource()
    pipeline, ctx, runtime = await setup(database, source, restored, 2)
    await run(pipeline, ctx, runtime)
    assert [p["mode"] for k, p in restored.calls if k == "event"] == ["full"]
    async with database() as db:
        row = await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event"))
        assert row.execution_state["last_full"]["parent_visibility_epoch"] > epoch


@pytest.mark.parametrize("race", ["owner", "writer"])
async def test_awaited_scope_plan_cannot_commit_after_owner_or_writer_change(
    database, source, race
):
    from airweave.domains.entities.canonical.requests import CaptureBatch
    from airweave.domains.entities.canonical.store import StaleWriter

    service, fence = source

    class RacingSource(MixedSource):
        async def prepare_scope(self, scope, cycle, previous, *, parent, force_full):
            plan = await super().prepare_scope(
                scope, cycle, previous, parent=parent, force_full=force_full
            )
            if scope.record_type == "event":
                async with database() as db:
                    if race == "writer":
                        await service.activate_writer(
                            db,
                            fence.organization_id,
                            fence.sync_id,
                            fence.job_id,
                            attempt_id=uuid4(),
                            attempt_number=2,
                        )
                    else:
                        await service.capture(
                            db,
                            CaptureBatch(
                                fence=fence,
                                records=(
                                    CaptureRecord(
                                        identity=parent.identity,
                                        payload={"id": "cal", "route": "changed"},
                                        observed_at=datetime.now(timezone.utc),
                                    ),
                                ),
                            ),
                        )
            return plan

    pipeline, ctx, runtime = await setup(database, source, RacingSource())
    with pytest.raises(StaleWriter if race == "writer" else ScanConflict):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        assert (
            await db.scalar(select(CaptureScan).where(CaptureScan.record_type == "event")) is None
        )
        assert (
            await db.scalar(select(Entity).where(Entity.entity_definition_short_name == "event"))
            is None
        )


async def test_nonmixed_replacement_clears_prior_scope_evidence(database, source):
    from airweave.domains.entities.canonical.cycle_models import BeginCycle
    from airweave.domains.entities.canonical.scan_models import BeginScan

    pipeline, ctx, runtime = await setup(database, source, MixedSource())
    await run(pipeline, ctx, runtime)
    async with database() as db:
        service = source[0]
        fence = pipeline._writer()
        previous = await service.read_cycle(db, fence)
        config = previous.configuration.model_copy(
            update={"fingerprint": "b" * 64, "scope_changes": ()}
        )
        current = await service.begin_cycle(
            db, BeginCycle(fence=fence, configuration=config, expected=previous.version)
        )
        scope = CompletedScope(record_type="calendar")
        old = await service.read_scan(db, fence, scope)
        assert old.execution is not None
        new = await service.begin_scan(
            db,
            BeginScan(
                fence=fence,
                scope=scope,
                cycle_id=current.version.cycle_id,
                fingerprint=config.fingerprint,
                expected=old.version,
            ),
        )
        assert new.execution is None


async def test_completed_coverage_survives_job_completion_but_not_new_writer(database, source):
    pipeline, ctx, runtime = await setup(database, source, MixedSource())
    await run(pipeline, ctx, runtime)
    service, fence = source
    async with database() as db:
        (await db.get(SyncJob, fence.job_id)).status = "completed"
        await db.commit()
        coverage = await capture_coverage(db, fence.organization_id, (fence.sync_id,))
        assert coverage[fence.sync_id].phase == "complete"
        assert coverage[fence.sync_id].scope_summary.unfinished == 0
    source = await next_job(database, source)
    async with database() as db:
        coverage = await capture_coverage(db, fence.organization_id, (fence.sync_id,))
        assert fence.sync_id not in coverage
    pipeline, ctx, runtime = await setup(database, source, MixedSource(fail_window=True))
    with pytest.raises(SourceServerError):
        await run(pipeline, ctx, runtime)
    async with database() as db:
        coverage = await capture_coverage(db, fence.organization_id, (fence.sync_id,))
        assert coverage[fence.sync_id].phase == "active"
        assert coverage[fence.sync_id].scope_summary.unfinished == 1


@pytest.mark.parametrize(
    "change", ["stale_root_inventory", "unknown_root_mode", "parent_epoch", "leaf_attempt"]
)
async def test_coverage_root_batch_preserves_scope_evidence(database, source, change):
    pipeline, ctx, runtime = await setup(database, source, MixedSource())
    await run(pipeline, ctx, runtime)
    service, fence = source
    async with database() as db:
        cycle = await service.read_cycle(db, fence)
        root = await db.scalar(
            select(CaptureScan).where(
                CaptureScan.sync_id == fence.sync_id, CaptureScan.record_type == "calendar"
            )
        )
        if change == "stale_root_inventory":
            root.membership_attempt_id = uuid4()
        elif change == "unknown_root_mode":
            root.execution_state = None
        elif change == "parent_epoch":
            parent = await db.scalar(
                select(Entity).where(
                    Entity.sync_id == fence.sync_id,
                    Entity.entity_definition_short_name == "calendar",
                )
            )
            parent.visibility_epoch += 1
        else:
            leaf = await db.scalar(
                select(CaptureScan).where(
                    CaptureScan.sync_id == fence.sync_id, CaptureScan.record_type == "event"
                )
            )
            leaf.membership_attempt_id = uuid4()
        await db.flush()

        summary = await mixed_scope_summary(db, fence.organization_id, fence.sync_id, cycle)
        if change == "stale_root_inventory":
            assert summary is None
        else:
            expected_unfinished = {"unknown_root_mode": 1, "parent_epoch": 2, "leaf_attempt": 0}
            assert summary.eligible == 3
            assert summary.unfinished == expected_unfinished[change]
            assert summary.completed_full == 3 - summary.unfinished
        assert await mixed_scope_summary(db, uuid4(), fence.sync_id, cycle) is None
