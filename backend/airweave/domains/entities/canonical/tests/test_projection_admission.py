"""Authenticated binding and current-input admission before external projection work."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.tests.helpers import (
    bind_projection,
    capture,
    observation,
    publish_prepared,
)
from airweave.models import Entity, Organization, Sync
from airweave.models.collection import Collection
from airweave.models.source_connection import SourceConnection


async def test_pending_requires_exact_authenticated_tenant_binding(database, source):
    service, fence = source
    await capture(database, service, fence, observation())
    store = CanonicalProjectionStore()
    async with database() as db:
        assert not await store.pending(db, fence.organization_id, fence.sync_id)
    binding = await bind_projection(database, fence)
    async with database() as db:
        assert len(await store.pending(db, fence.organization_id, fence.sync_id)) == 1
        assert not await store.pending(db, uuid4(), fence.sync_id)
        await db.execute(update(SourceConnection).values(is_authenticated=False))
        await db.commit()
    async with database() as db:
        assert not await store.pending(db, fence.organization_id, fence.sync_id)
        foreign = Organization(name="Other synthetic tenant")
        db.add(foreign)
        await db.flush()
        await db.execute(update(SourceConnection).values(is_authenticated=True))
        await db.execute(
            update(Collection)
            .where(Collection.id == binding.collection_id)
            .values(organization_id=foreign.id)
        )
        await db.commit()
    async with database() as db:
        assert not await store.pending(db, fence.organization_id, fence.sync_id)


@pytest.mark.parametrize("change", ["auth", "revision", "pipeline", "source", "collection"])
async def test_stale_work_is_denied_before_mapper_storage_or_processor(
    database, source, monkeypatch, change
):
    service, fence = source
    binding = await bind_projection(database, fence)
    await capture(database, service, fence, observation())
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    if change == "revision":
        await capture(database, service, fence, observation(payload={"summary": "new"}))
    else:
        async with database() as db:
            if change == "auth":
                await db.execute(update(SourceConnection).values(is_authenticated=False))
            elif change == "pipeline":
                await db.execute(update(Sync).values(index_pipeline_version=2))
            elif change == "source":
                await db.execute(update(SourceConnection).values(short_name="slack"))
            else:
                await db.execute(update(SourceConnection).values(readable_collection_id=None))
            await db.commit()
    mapper = MagicMock(side_effect=AssertionError("mapper must not read stale input"))
    monkeypatch.setattr("airweave.domains.entities.canonical.projection_mappers.map_record", mapper)
    processor, storage = MagicMock(), AsyncMock()
    destination = MagicMock(collection_id=binding.collection_id, feed_prepared=AsyncMock())
    project = CanonicalProjector(store, database, processor, storage)
    assert not (await project.project_one(work, "gmail", destination, MagicMock())).published
    mapper.assert_not_called()
    assert not processor.mock_calls and not storage.mock_calls
    destination.feed_prepared.assert_not_awaited()


async def test_deauth_after_preparation_rejects_publication_and_failure_write(database, source):
    service, fence = source
    binding = await bind_projection(database, fence)
    await capture(
        database, service, fence, observation(kind="delete", removal_reason="provider_deleted")
    )
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
        assert await store.admit(db, work)
    generation = uuid4()
    async with database() as db:
        assert await store.prepare(db, work, generation, binding.collection_id, ())
    async with database() as db:
        # Same Sync fence used by owned provisioning pause/withdraw.
        await db.scalar(select(Sync).where(Sync.id == fence.sync_id).with_for_update())
        await db.execute(update(SourceConnection).values(is_authenticated=False))
        await db.commit()
    async with database() as db:
        assert not await store.publish(db, work, generation, 0)
        await store.fail(db, work, "late failure")
        row = await db.get(Entity, work.record.id)
        assert row.indexed_generation is None and row.projection_error is None


async def test_wrong_destination_rejected_before_processing_and_manifest(database, source):
    service, fence = source
    await bind_projection(database, fence)
    await capture(database, service, fence, observation())
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    processor = MagicMock()
    project = CanonicalProjector(store, database, processor, AsyncMock())
    assert not (
        await project.project_one(work, "gmail", MagicMock(collection_id=uuid4()), MagicMock())
    ).published
    assert not processor.mock_calls
    async with database() as db:
        with pytest.raises(ValueError, match="destination"):
            await publish_prepared(store, db, work, uuid4(), 1, collection_id=uuid4())


async def test_publication_waits_for_owned_pause_fence_and_observes_revocation(database, source):
    import asyncio

    service, fence = source
    binding = await bind_projection(database, fence)
    await capture(
        database, service, fence, observation(kind="delete", removal_reason="provider_deleted")
    )
    store = CanonicalProjectionStore()
    generation = uuid4()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
        assert await store.prepare(db, work, generation, binding.collection_id, ())

    async def publish():
        async with database() as db:
            return await store.publish(db, work, generation, 0)

    async with database() as paused:
        await paused.scalar(select(Sync).where(Sync.id == fence.sync_id).with_for_update())
        publishing = asyncio.create_task(publish())
        try:
            # Publication cannot pass the same Sync row held by owned pause.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(publishing), timeout=0.1)
            await paused.execute(update(SourceConnection).values(is_authenticated=False))
            await paused.commit()
            assert not await asyncio.wait_for(publishing, timeout=5)
        finally:
            if not publishing.done():
                publishing.cancel()
                await asyncio.gather(publishing, return_exceptions=True)


async def test_stale_activity_route_does_not_poison_new_binding_work(database, source):
    service, fence = source
    binding = await bind_projection(database, fence, "slack")
    await capture(database, service, fence, observation())
    project = CanonicalProjector(CanonicalProjectionStore(), database, MagicMock(), AsyncMock())
    result = await project.batch(
        fence.organization_id,
        fence.sync_id,
        "gmail",
        MagicMock(collection_id=binding.collection_id),
        MagicMock(),
    )
    assert result.superseded == 1 and result.failed == 0
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        assert row.projection_error is None and row.indexed_generation is None
