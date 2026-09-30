"""Real PostgreSQL retirement fences and repeatable cleanup after late remote writes."""

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import delete, select

from airweave.domains.entities.canonical.projection_gc import ProjectionGCStore
from airweave.domains.entities.canonical.projection_models import (
    ProjectionDocument,
    ProjectionLocator,
    scope_projection_document_id,
)
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.models import Entity, Sync
from airweave.models.projection_generation import ProjectionGeneration


async def prepare(database, source, count=3):
    service, fence = source
    await capture(database, service, fence, observation())
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    generation = uuid4()
    locator = ProjectionLocator(
        record_id=work.record.id,
        revision=work.record.revision,
        pipeline_version=work.pipeline_version,
        generation=generation,
        part_index=0,
    ).encode()
    collection_id = uuid4()
    docs = tuple(
        ProjectionDocument(
            schema_name="base_entity",
            document_id=scope_projection_document_id(
                work.record.sync_id, collection_id, f"Entity_{locator}__chunk_{n}"
            ),
        )
        for n in range(count)
    )
    async with database() as db:
        assert await store.prepare(db, work, generation, collection_id, docs)
    return store, work, generation, docs


async def test_retirement_prevents_late_publish_and_repeats_after_late_feed(database, source):
    store, work, generation, docs = await prepare(database, source)
    gc = ProjectionGCStore()
    now = datetime.now(timezone.utc) + timedelta(hours=2)
    async with database() as db:
        page = await gc.claim(db, generation, now=now, limit=2)
    assert page.documents == docs[:2]
    async with database() as db:
        assert not await store.publish(db, work, generation, 3)
    async with database() as db:
        await gc.acknowledge(db, page, now=now, error="HTTPError")
    async with database() as db:
        row = await db.get(ProjectionGeneration, generation)
        assert row.delete_cursor == 0 and row.gc_error == "HTTPError"
    now += timedelta(minutes=6)
    async with database() as db:
        retry = await gc.claim(db, generation, now=now, limit=2)
    assert retry.documents == page.documents
    async with database() as db:
        await gc.acknowledge(db, retry, now=now)
    async with database() as db:
        tail = await gc.claim(db, generation, now=now, limit=2)
    assert tail.documents == docs[2:]
    async with database() as db:
        await gc.acknowledge(db, tail, now=now)
    # A timed-out feed can recreate documents after this full delete pass.
    # The retained ledger schedules all exact IDs again; no query-index discovery required.
    async with database() as db:
        repeated = await gc.claim(db, generation, now=now + timedelta(hours=1), limit=100)
    assert repeated.documents == docs
    async with database() as db:
        await gc.acknowledge(db, tail, now=now)  # stale callback cannot ack newer attempt
        row = await db.get(ProjectionGeneration, generation)
        assert row.gc_attempt == repeated.attempt and row.gc_passes == 1


async def test_current_generation_protected_then_atomically_retired_on_replacement(
    database, source
):
    store, work, generation, docs = await prepare(database, source)
    gc = ProjectionGCStore()
    async with database() as db:
        assert await store.publish(db, work, generation, len(docs))
    async with database() as db:
        assert (
            await gc.claim(db, generation, now=datetime.now(timezone.utc) + timedelta(days=2))
            is None
        )
    service, fence = source
    await capture(database, service, fence, observation(payload={"changed": True}))
    async with database() as db:
        newer = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    new_generation = uuid4()
    new_docs = tuple(
        doc.model_copy(
            update={
                "document_id": doc.document_id.replace(
                    str(generation), str(new_generation)
                ).replace(":1:1:", ":2:1:")
            }
        )
        for doc in docs
    )
    async with database() as db:
        assert await store.prepare(
            db, newer, new_generation, UUID(docs[0].document_id.split("_")[1]), new_docs
        )
    async with database() as db:
        assert await store.publish(db, newer, new_generation, len(docs))
    async with database() as db:
        old = await db.get(ProjectionGeneration, generation)
        assert old.retired_at is not None
        assert (await db.get(Entity, work.record.id)).indexed_generation == new_generation


async def test_source_deletion_does_not_erase_cleanup_obligations(database, source):
    store, work, generation, docs = await prepare(database, source)
    async with database() as db:
        await db.execute(delete(Sync).where(Sync.id == work.record.sync_id))
        await db.commit()
    gc = ProjectionGCStore()
    async with database() as db:
        page = await gc.claim(db, generation, now=datetime.now(timezone.utc) + timedelta(hours=2))
    assert page.documents == docs
    async with database() as db:
        assert not await store.publish(db, work, generation, len(docs))


async def test_gc_waits_on_publication_lock_and_cannot_retire_current(database, source):
    import asyncio

    store, work, generation, docs = await prepare(database, source)
    gc = ProjectionGCStore()
    entered = asyncio.Event()
    async with database() as publishing:
        # Hold the real shared lock and publish the pointer in the same transaction.
        await publishing.scalar(
            select(Sync.id).where(Sync.id == work.record.sync_id).with_for_update()
        )
        row = await publishing.get(Entity, work.record.id)
        row.indexed_generation = generation
        await publishing.flush()

        async def collect():
            entered.set()
            async with database() as db:
                return await gc.claim(
                    db, generation, now=datetime.now(timezone.utc) + timedelta(hours=2)
                )

        attempt = asyncio.create_task(collect())
        await entered.wait()
        await asyncio.sleep(0.05)
        assert not attempt.done()
        await publishing.commit()
        assert await attempt is None
    async with database() as db:
        assert (await db.get(ProjectionGeneration, generation)).retired_at is None


async def test_deleted_current_generation_retired_without_another_projection(database, source):
    store, work, generation, docs = await prepare(database, source)
    async with database() as db:
        assert await store.publish(db, work, generation, len(docs))
    service, fence = source
    await capture(
        database,
        service,
        fence,
        observation(kind="delete", removal_reason="provider_deleted", payload={"id": "one"}),
    )
    async with database() as db:
        page = await ProjectionGCStore().claim(
            db, generation, now=datetime.now(timezone.utc) + timedelta(days=2)
        )
        assert page.documents == docs
        record = await db.get(Entity, work.record.id)
        assert record.indexed_generation is None and record.indexed_revision is None
        assert record.deleted_at is not None
        assert (await db.get(ProjectionGeneration, generation)).retired_at is not None


async def test_hidden_parent_retirement_restoration_requires_fresh_generation(database, source):
    from airweave.domains.entities.canonical.requests import RecordIdentity

    store, work, generation, docs = await prepare(database, source)
    async with database() as db:
        assert await store.publish(db, work, generation, len(docs))
    service, fence = source
    parent = RecordIdentity(record_type="calendar", native_id="calendar-one")
    # Add a real captured parent reference, then simulate the crash window between
    # parent revocation and bounded child reconciliation: child remains active.
    await capture(database, service, fence, observation(identity=parent))
    await capture(database, service, fence, observation(parent=parent))
    await capture(
        database,
        service,
        fence,
        observation(
            identity=parent,
            kind="delete",
            removal_reason="access_revoked",
            payload={"id": "calendar-one"},
        ),
    )
    async with database() as db:
        page = await ProjectionGCStore().claim(
            db, generation, now=datetime.now(timezone.utc) + timedelta(days=2)
        )
        assert page.documents == docs
        assert (await db.get(Entity, work.record.id)).deleted_at is None
    await capture(database, service, fence, observation(identity=parent))
    async with database() as db:
        assert all(
            item.record.id != work.record.id
            for item in await store.pending(db, fence.organization_id, fence.sync_id)
        )
    # Restoration of the parent alone cannot reauthorize retained descendant bytes.
    await capture(database, service, fence, observation(parent=parent))
    async with database() as db:
        restored = next(
            item
            for item in await store.pending(db, fence.organization_id, fence.sync_id)
            if item.record.id == work.record.id
        )
        assert restored.previous_generation is None
        assert not await store.publish(db, restored, generation, len(docs))


async def test_manifest_rejects_foreign_scope_and_unanchored_locator(database, source):
    import pytest

    store, work, generation, docs = await prepare(database, source)
    collection_id = UUID(docs[0].document_id.split("_")[1])
    for document_id in (
        docs[0].document_id.replace(str(work.record.sync_id), str(uuid4())),
        docs[0].document_id.replace(str(collection_id), str(uuid4())),
        "arbitrary_" + docs[0].document_id,
        docs[0].document_id + "_suffix",
    ):
        async with database() as db:
            with pytest.raises(ValueError, match="Projection manifest"):
                await store.prepare(
                    db,
                    work,
                    generation,
                    collection_id,
                    (docs[0].model_copy(update={"document_id": document_id}),),
                )


async def test_empty_tombstone_publication_does_not_become_pending_each_gc_pass(database, source):
    service, fence = source
    await capture(
        database,
        service,
        fence,
        observation(kind="delete", removal_reason="provider_deleted", payload={"id": "one"}),
    )
    store = CanonicalProjectionStore()
    generation = uuid4()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
    async with database() as db:
        assert await store.prepare(db, work, generation, uuid4(), ())
    async with database() as db:
        assert await store.publish(db, work, generation, 0)
    async with database() as db:
        assert (
            await ProjectionGCStore().claim(
                db, generation, now=datetime.now(timezone.utc) + timedelta(days=2)
            )
            is None
        )
        assert not await store.pending(db, fence.organization_id, fence.sync_id)
