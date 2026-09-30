"""Single-crawl capture path against PostgreSQL, independent of index services."""

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select
from temporalio.testing import ActivityEnvironment

from airweave.adapters.event_bus.fake import FakeEventBus
from airweave.domains.entities.canonical.requests import CompletedScope, RemovedScope, StartedScope
from airweave.domains.entities.canonical.tests.helpers import observation
from airweave.domains.sync_pipeline.canonical_capture import CanonicalCapturePipeline
from airweave.domains.sync_pipeline.capture_attempt import CaptureAttempt, resolve_capture_attempt
from airweave.domains.sync_pipeline.config import SyncConfig
from airweave.domains.sync_pipeline.contexts.runtime import SyncRuntime
from airweave.domains.sync_pipeline.exceptions import SyncFailureError
from airweave.domains.sync_pipeline.orchestrator import SyncOrchestrator
from airweave.domains.sync_pipeline.pipeline.entity_tracker import EntityTracker
from airweave.domains.sync_pipeline.stream import AsyncSourceStream
from airweave.domains.sync_pipeline.worker_pool import AsyncWorkerPool
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.models import Entity
from airweave.models import SyncCursor as StoredCursor

pytestmark = pytest.mark.integration


def components(database, source):
    service, fence = source
    logger = logging.getLogger("canonical-capture-test")
    config = SyncConfig()
    config.behavior.skip_guardrails = True
    ctx = SimpleNamespace(
        organization_id=fence.organization_id,
        sync_id=fence.sync_id,
        sync_job_id=fence.job_id,
        sync=SimpleNamespace(id=fence.sync_id),
        sync_job=SimpleNamespace(id=fence.job_id),
        organization=SimpleNamespace(id=fence.organization_id),
        collection_id=uuid4(),
        source_connection_id=uuid4(),
        source_short_name="test",
        execution_config=config,
        logger=logger,
        batch_size=1,
        max_batch_latency_ms=0,
        should_batch=True,
    )
    bus = FakeEventBus()
    pipeline = CanonicalCapturePipeline(
        service,
        database,
        bus,
        ("event",),
        CaptureAttempt(id=fence.attempt_id, number=fence.attempt_number),
    )
    runtime = SyncRuntime(
        source=SimpleNamespace(source_name="synthetic", supports_access_control=False),
        entity_tracker=EntityTracker(fence.job_id, fence.sync_id, logger),
        cursor=SyncCursor(fence.sync_id),
        canonical_capture=pipeline,
    )
    return ctx, pipeline, runtime, bus


def orchestrator(ctx, pipeline, runtime, generator, bus):
    return SyncOrchestrator(
        entity_pipeline=pipeline,
        worker_pool=AsyncWorkerPool(logger=ctx.logger),
        stream=AsyncSourceStream(generator, logger=ctx.logger),
        sync_context=ctx,
        runtime=runtime,
        access_control_pipeline=AsyncMock(),
        event_bus=bus,
        usage_checker=AsyncMock(),
        usage_ledger=AsyncMock(),
        sync_cursor_service=AsyncMock(),
        state_machine=AsyncMock(),
        lifecycle_data=SimpleNamespace(),
        sync_state_machine=AsyncMock(),
    )


async def test_original_payload_commits_without_index_and_preserves_stream_order(database, source):
    ctx, pipeline, runtime, bus = components(database, source)

    async def generate():
        yield observation(payload={"native-only": {"nested": "retained"}, "version": 1})
        yield observation(payload={"native-only": {"nested": "retained"}, "version": 2})
        yield CompletedScope(record_type="event")
        runtime.cursor.update(history_id="committed")

    runner = orchestrator(ctx, pipeline, runtime, generate(), bus)
    await runner._start_sync()
    await runner._process_entities()
    await runner._cleanup_orphaned_entities_if_needed()
    await runner._save_cursor_data()
    async with database() as db:
        row = (await db.scalars(select(Entity))).one()
        assert row.source_payload == {"native-only": {"nested": "retained"}, "version": 2}
        assert row.record_revision == 2 and row.indexed_revision is None
        assert (await db.scalar(select(StoredCursor.cursor_data)))["history_id"] == "committed"
    assert runtime.destinations == []
    assert len(bus.events) == 2


async def test_failed_enumeration_keeps_partial_capture_but_no_cursor_or_orphan_delete(
    database, source
):
    ctx, pipeline, runtime, bus = components(database, source)
    await pipeline.start(ctx)
    await pipeline.process([observation("prior", "a")], ctx, runtime)

    async def generate():
        yield StartedScope(record_type="event", container_id="a")
        yield observation("partial", "a")
        runtime.cursor.update(history_id="must-not-save")
        raise ConnectionError("provider page failed")

    runner = orchestrator(ctx, pipeline, runtime, generate(), bus)
    await runner._start_sync()
    with pytest.raises(ConnectionError, match="provider page failed"):
        await runner._process_entities()
    async with database() as db:
        rows = list((await db.scalars(select(Entity))).all())
        assert "prior" in {row.native_id for row in rows}
        assert all(row.deleted_at is None for row in rows)
        assert await db.scalar(select(StoredCursor.id)) is None


async def test_same_attempt_rebaseline_and_confirmed_scope_loss(database, source):
    ctx, pipeline, runtime, _ = components(database, source)
    await pipeline.start(ctx)
    await pipeline.process(
        [
            observation("incremental-before-410", "a"),
            observation("unrelated", "b"),
            StartedScope(record_type="event", container_id="a"),
            observation("full-after-reset", "a"),
            CompletedScope(record_type="event", container_id="a"),
        ],
        ctx,
        runtime,
    )
    await pipeline.cleanup_orphaned_entities(ctx, runtime)
    async with database() as db:
        rows = {row.native_id: row for row in (await db.scalars(select(Entity))).all()}
        assert rows["incremental-before-410"].removal_reason == "absent"
        assert rows["full-after-reset"].deleted_at is None
        assert rows["unrelated"].deleted_at is None
    await pipeline.process(
        [
            RemovedScope(
                record_type="event",
                container_id="a",
                removal_reason="access_revoked",
                observed_at=datetime.now(timezone.utc),
            )
        ],
        ctx,
        runtime,
    )
    async with database() as db:
        rows = {row.native_id: row for row in (await db.scalars(select(Entity))).all()}
        assert rows["full-after-reset"].removal_reason == "access_revoked"
        assert rows["unrelated"].deleted_at is None
    with pytest.raises(SyncFailureError, match="after confirmed scope loss"):
        await pipeline.process([observation("late", "a")], ctx, runtime)


def test_attempt_identity_uses_temporal_metadata_or_explicit_local_identity():
    with pytest.raises(ValueError, match="explicit CaptureAttempt"):
        resolve_capture_attempt()
    explicit = CaptureAttempt(id=uuid4(), number=3)
    assert resolve_capture_attempt(explicit) == explicit
    environment = ActivityEnvironment()
    first = environment.run(resolve_capture_attempt)
    second = environment.run(resolve_capture_attempt)
    assert first == second and first.number == environment.info.attempt
