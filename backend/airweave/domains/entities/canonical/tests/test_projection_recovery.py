"""Recovery discovers exact pending scope and never retries failed originals implicitly."""

from unittest.mock import AsyncMock
from uuid import uuid4

from sqlalchemy import select

from airweave.domains.entities.canonical.projection_models import ProjectionBatchResult
from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.projector import CanonicalProjector
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.models import Collection, Entity, Organization, SourceConnection, Sync, SyncJob
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


async def test_source_pages_skip_failed_indexed_unsupported_and_foreign_rows(database, source):
    service, seed = source
    foreign = uuid4()
    async with database() as db:
        db.add(Organization(id=foreign, name="Foreign"))
        metadata = VectorDbDeploymentMetadata(
            dense_embedder="test", sparse_embedder="test", embedding_dimensions=3
        )
        db.add(metadata)
        await db.flush()
        for owner, readable in ((seed.organization_id, "owned"), (foreign, "foreign")):
            db.add(
                Collection(
                    id=uuid4(),
                    name=readable,
                    readable_id=readable,
                    organization_id=owner,
                    vector_db_deployment_metadata_id=metadata.id,
                )
            )
        await db.commit()
    expected = set()
    for case in (
        "fresh",
        "provider",
        "unauthenticated",
        "missing-collection",
        "foreign-collection",
        "tombstone",
        "failed",
        "indexed",
        "unsupported",
        "foreign-source",
        "foreign-record",
        "withdrawn",
    ):
        sync_id, job_id = uuid4(), uuid4()
        async with database() as db:
            db.add(Sync(id=sync_id, organization_id=seed.organization_id, name=case))
            await db.flush()
            db.add(
                SyncJob(
                    id=job_id,
                    sync_id=sync_id,
                    organization_id=seed.organization_id,
                    status="running",
                )
            )
            db.add(
                SourceConnection(
                    id=uuid4(),
                    sync_id=sync_id,
                    organization_id=foreign if case == "foreign-source" else seed.organization_id,
                    name=case,
                    short_name=(
                        "legacy"
                        if case == "unsupported"
                        else "slack"
                        if case == "provider"
                        else "almanac"
                    ),
                    is_authenticated=case != "unauthenticated",
                    readable_collection_id=(
                        None
                        if case == "missing-collection"
                        else "foreign"
                        if case == "foreign-collection"
                        else "owned"
                    ),
                )
            )
            await db.commit()
        async with database() as db:
            fence = await service.activate_writer(
                db,
                seed.organization_id,
                sync_id,
                job_id,
                attempt_id=uuid4(),
                attempt_number=1,
            )
        await capture(database, service, fence, observation())
        if case == "tombstone":
            await capture(
                database,
                service,
                fence,
                observation().model_copy(update={"kind": "delete", "removal_reason": "deleted"}),
            )
        async with database() as db:
            record = await db.scalar(select(Entity).where(Entity.sync_id == sync_id))
            if case == "failed":
                record.projection_error = "ConversionFailed"
            elif case == "indexed":
                record.indexed_revision = record.record_revision
                record.indexed_pipeline_version = 1
                record.indexed_generation = uuid4()
            elif case == "foreign-record":
                record.organization_id = foreign
            elif case == "withdrawn":
                record.removal_reason = "access_revoked"
            await db.commit()
        if case in ("fresh", "provider", "tombstone"):
            expected.add(sync_id)
    store = CanonicalProjectionStore()
    async with database() as db:
        first = await store.pending_sources(db, ("almanac", "slack"), limit=2)
        assert len(first.sources) == 2
        assert first.next_cursor == first.sources[-1].sync_id
        second = await store.pending_sources(
            db, ("almanac", "slack"), after_id=first.next_cursor, limit=2
        )
        assert len(second.sources) == 1 and second.next_cursor is None
        assert {item.sync_id for item in (*first.sources, *second.sources)} == expected
        assert all(
            item.organization_id == seed.organization_id
            for item in (*first.sources, *second.sources)
        )
        assert not (await store.pending_sources(db, ())).sources


async def test_recovery_execution_skips_failed_until_original_changes(database, source):
    service, fence = source
    await bind_projection(database, fence, "almanac")
    await capture(database, service, fence, observation())
    store = CanonicalProjectionStore()
    async with database() as db:
        work = (await store.pending(db, fence.organization_id, fence.sync_id))[0]
        await store.fail(db, work, "ConversionFailed")
    async with database() as db:
        assert len(await store.pending(db, fence.organization_id, fence.sync_id)) == 1
        assert not await store.pending(db, fence.organization_id, fence.sync_id, skip_failed=True)
    # Isolate only the expensive external projection; exercise the actual batch selector.
    projector = CanonicalProjector(store, database, None, None)
    projector.project_one = AsyncMock(return_value=True)
    logger = AsyncMock()
    assert (
        await projector.batch(
            fence.organization_id, fence.sync_id, "almanac", None, logger, skip_failed=True
        )
        == ProjectionBatchResult()
    )
    projector.project_one.assert_not_awaited()
    await capture(database, service, fence, observation(payload={"summary": "Changed original"}))
    result = await projector.batch(
        fence.organization_id, fence.sync_id, "almanac", None, logger, skip_failed=True
    )
    assert result.published == 1
    projector.project_one.assert_awaited_once()
