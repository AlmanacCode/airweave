"""Real PostgreSQL proofs; never connect without an explicit synthetic-test URL."""

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select, text, update

from airweave.domains.entities.canonical.requests import (
    CaptureBatch,
    CompletedScope,
    ReconcileScope,
)
from airweave.domains.entities.canonical.store import (
    SourceNotFound,
    StaleWriter,
    WriterBusy,
)
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.models import Entity, EntityChange, Sync, SyncCursor, SyncJob

pytestmark = pytest.mark.integration


async def test_identity_replay_metadata_tombstone_and_historical_snapshots(database, source):
    service, fence = source
    first = observation(container_id="cal-a")
    second = observation(container_id="cal-b")
    result = await capture(database, service, fence, first, second)
    assert [c.sequence for c in result.changes] == [1, 2]
    assert result.changes[0].record.id != result.changes[1].record.id
    replay = await capture(
        database,
        service,
        fence,
        first.model_copy(update={"observed_at": first.observed_at + timedelta(hours=1)}),
    )
    assert replay.unchanged == 1 and replay.sequence == 2 and not replay.changes
    changed = first.model_copy(update={"payload": {"id": "one", "labels": ["IMPORTANT"]}})
    changed_result = await capture(database, service, fence, changed)
    assert changed_result.changes[0].record.revision == 2
    assert changed_result.changes[0].record.content_hash == "same-body"
    cancelled = observation(
        "never-seen",
        "cal-a",
        payload={
            "id": "never-seen",
            "recurringEventId": "series",
            "originalStartTime": {"date": "2026-09-30"},
        },
        kind="delete",
        removal_reason="provider_deleted",
    )
    deleted = await capture(database, service, fence, cancelled)
    assert deleted.changes[0].record.payload["recurringEventId"] == "series"
    async with database() as db:
        page = await service.store.changes(db, fence.organization_id, fence.sync_id, limit=2)
        assert page.has_more and page.next_sequence == 2
        assert page.changes[0].record.payload["summary"] == "Synthetic event"
        tail = await service.store.changes(
            db,
            fence.organization_id,
            fence.sync_id,
            after=page.next_sequence,
            high_watermark=page.high_watermark,
        )
        assert [c.sequence for c in tail.changes] == [3, 4]
        assert not tail.has_more
        assert (
            await service.store.read(db, uuid4(), fence.sync_id, result.changes[0].record.id)
            is None
        )
        with pytest.raises(SourceNotFound):
            await service.store.changes(db, uuid4(), fence.sync_id)
        pending = await db.scalar(
            select(func.count())
            .select_from(Entity)
            .where(Entity.indexed_revision.is_distinct_from(Entity.record_revision))
        )
        assert pending == 3


async def test_rollback_has_no_record_journal_or_sequence_hole(database, source):
    service, fence = source
    async with database() as db:
        await service.store.capture(db, CaptureBatch(fence=fence, records=(observation(),)))
        await db.rollback()
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Entity)) == 0
        assert await db.scalar(select(func.count()).select_from(EntityChange)) == 0
        assert await db.scalar(select(Sync.observed_change_sequence)) == 0
    committed = await capture(database, service, fence, observation())
    assert committed.sequence == 1


async def test_parallel_commits_cannot_skip_a_change_watermark(database, source):
    service, fence = source
    first_ready, release_first, second_started = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def first_writer():
        async with database() as db:
            result = await service.store.capture(
                db, CaptureBatch(fence=fence, records=(observation("first"),))
            )
            first_ready.set()
            await release_first.wait()
            await db.commit()
            return result

    async def second_writer():
        await first_ready.wait()
        second_started.set()
        return await capture(database, service, fence, observation("second"))

    first_task = asyncio.create_task(first_writer())
    second_task = asyncio.create_task(second_writer())
    await asyncio.wait_for(second_started.wait(), 5)
    async with database() as db:
        page = await service.store.changes(db, fence.organization_id, fence.sync_id)
        assert page.high_watermark == 0 and not page.changes
    assert not second_task.done()
    release_first.set()
    a, b = await asyncio.wait_for(asyncio.gather(first_task, second_task), 5)
    assert (a.sequence, b.sequence) == (1, 2)
    async with database() as db:
        page = await service.store.changes(db, fence.organization_id, fence.sync_id)
        assert [c.sequence for c in page.changes] == [1, 2]


async def test_writer_fences_checkpoint_and_job_deletion_preserves_records(database, source):
    service, fence = source
    captured = await capture(database, service, fence, observation())
    next_job = uuid4()
    async with database() as db:
        db.add(
            SyncJob(
                id=next_job,
                organization_id=fence.organization_id,
                sync_id=fence.sync_id,
                status="running",
            )
        )
        await db.commit()
    async with database() as db:
        with pytest.raises(WriterBusy):
            await service.activate_writer(
                db,
                fence.organization_id,
                fence.sync_id,
                next_job,
                attempt_id=uuid4(),
                attempt_number=1,
            )
    async with database() as db:
        await db.execute(
            update(SyncJob).where(SyncJob.id == fence.job_id).values(status="completed")
        )
        await db.commit()
    async with database() as db:
        next_fence = await service.activate_writer(
            db, fence.organization_id, fence.sync_id, next_job, attempt_id=uuid4(), attempt_number=1
        )
        assert next_fence.epoch == fence.epoch + 1
    async with database() as db:
        with pytest.raises(StaleWriter):
            await service.save_checkpoint(db, fence, {"history_id": "old"})
    async with database() as db:
        await service.save_checkpoint(db, next_fence, {"history_id": "new"})
    async with database() as db:
        await db.execute(delete(SyncJob).where(SyncJob.id == fence.job_id))
        await db.commit()
        entity = await db.get(Entity, captured.changes[0].record.id)
        assert entity is not None and entity.sync_job_id is None
        assert await db.scalar(select(func.count()).select_from(EntityChange)) == 1
        assert (await db.scalar(select(SyncCursor.cursor_data)))["history_id"] == "new"


async def test_reconcile_only_complete_exact_container_and_keep_marks_seen(database, source):
    service, fence = source
    keep, removed, outside = (
        observation("keep", "a"),
        observation("gone", "a"),
        observation("other", "b"),
    )
    await capture(database, service, fence, keep, removed, outside)
    async with database() as db:
        await db.execute(
            update(SyncJob).where(SyncJob.id == fence.job_id).values(status="completed")
        )
        new_job = SyncJob(
            organization_id=fence.organization_id, sync_id=fence.sync_id, status="running"
        )
        db.add(new_job)
        await db.commit()
        new_fence = await service.activate_writer(
            db,
            fence.organization_id,
            fence.sync_id,
            new_job.id,
            attempt_id=uuid4(),
            attempt_number=1,
        )
    kept = await capture(database, service, new_fence, keep)
    assert kept.unchanged == 1
    async with database() as db:
        result = await service.reconcile_scope(
            db,
            ReconcileScope(
                fence=new_fence,
                scope=CompletedScope(record_type="event", container_id="a"),
                observed_at=datetime.now(timezone.utc),
            ),
        )
        assert [c.record.identity.native_id for c in result.capture.changes] == ["gone"]
        assert result.capture.changes[0].record.removal_reason == "absent"
        assert not result.has_more
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.deleted_at.is_(None))
            )
            == 2
        )


async def test_retry_attempt_fences_prior_activity_and_seen_scope(database, source):
    service, old = source
    await capture(database, service, old, observation("from-first-attempt", "a"))
    attempt_id = uuid4()
    async with database() as db:
        newer = await service.activate_writer(
            db,
            old.organization_id,
            old.sync_id,
            old.job_id,
            attempt_id=attempt_id,
            attempt_number=2,
        )
        repeated = await service.activate_writer(
            db,
            old.organization_id,
            old.sync_id,
            old.job_id,
            attempt_id=attempt_id,
            attempt_number=2,
        )
        assert repeated == newer and newer.epoch > old.epoch
    with pytest.raises(StaleWriter):
        await capture(database, service, old, observation("late-old"))
    async with database() as db:
        with pytest.raises(StaleWriter):
            await service.activate_writer(
                db,
                old.organization_id,
                old.sync_id,
                old.job_id,
                attempt_id=old.attempt_id,
                attempt_number=1,
            )
    # Seen in an interrupted attempt is not seen in the retry's complete scan.
    async with database() as db:
        result = await service.reconcile_scope(
            db,
            ReconcileScope(
                fence=newer,
                scope=CompletedScope(record_type="event", container_id="a"),
                observed_at=datetime.now(timezone.utc),
            ),
        )
        assert len(result.capture.changes) == 1
        assert result.capture.changes[0].record.deleted_at is not None


async def test_cancellation_and_capture_have_a_database_barrier(database, source):
    service, fence = source
    async with database() as writer:
        await service.store.capture(writer, CaptureBatch(fence=fence, records=(observation(),)))
        cancellation_started = asyncio.Event()

        async def cancel():
            async with database() as db:
                cancellation_started.set()
                await db.execute(
                    update(SyncJob).where(SyncJob.id == fence.job_id).values(status="cancelled")
                )
                await db.commit()

        task = asyncio.create_task(cancel())
        await cancellation_started.wait()
        assert not task.done()
        await writer.commit()
        await asyncio.wait_for(task, 5)
    # Cancellation has committed; old capture and checkpoint now both fail.
    with pytest.raises(StaleWriter):
        await capture(database, service, fence, observation("after-cancel"))
    async with database() as db:
        with pytest.raises(StaleWriter):
            await service.save_checkpoint(db, fence, {"history_id": "late"})
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Entity)) == 1
        assert await db.scalar(select(func.count()).select_from(SyncCursor)) == 0


async def test_service_rolls_back_partial_batch_on_real_database_failure(database, source):
    service, fence = source
    async with database() as db:
        await db.execute(
            text("""
            CREATE FUNCTION reject_second_change() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.sequence = 2 THEN RAISE EXCEPTION 'synthetic journal failure'; END IF;
              RETURN NEW;
            END $$
        """)
        )
        await db.execute(
            text("""
            CREATE TRIGGER reject_second_change BEFORE INSERT ON entity_change
            FOR EACH ROW EXECUTE FUNCTION reject_second_change()
        """)
        )
        await db.commit()
    from sqlalchemy.exc import DBAPIError

    with pytest.raises(DBAPIError, match="synthetic journal failure"):
        await capture(database, service, fence, observation("first"), observation("second"))
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Entity)) == 0
        assert await db.scalar(select(func.count()).select_from(EntityChange)) == 0
        assert await db.scalar(select(Sync.observed_change_sequence)) == 0


@pytest.mark.parametrize("database", ["legacy"], indirect=True)
async def test_upgrade_preserves_existing_metadata_without_claiming_capture(database):
    async with database() as db:
        entity = (await db.scalars(select(Entity))).one()
        assert entity.entity_id == "legacy-native-key" and entity.hash == "old-hash"
        assert entity.record_revision == 0 and entity.source_payload is None
        assert entity.native_id is None and entity.indexed_revision is None
        assert await db.scalar(select(func.count()).select_from(EntityChange)) == 0
        await db.execute(delete(SyncJob).where(SyncJob.id == entity.sync_job_id))
        await db.commit()
        await db.refresh(entity)
        assert entity.sync_job_id is None
