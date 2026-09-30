"""Real PostgreSQL proof of record-forest visibility, independent of provider type depth."""

import pytest
from sqlalchemy import select, text

from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalStoreError, content_is_available
from airweave.domains.entities.canonical.tests.helpers import capture, observation
from airweave.models.entity import Entity


def node(name, parent=None):
    return observation(identity=RecordIdentity(record_type="block", native_id=name), parent=parent)


async def available(database, ids):
    async with database() as db:
        return dict(
            (
                await db.execute(
                    select(Entity.id, content_is_available()).where(Entity.id.in_(ids))
                )
            ).all()
        )


async def test_nested_loss_revival_requires_fresh_attestation_without_changing_bytes(
    database, source
):
    service, fence = source
    root = node("root")
    child = node("child", root.identity)
    leaf = node("leaf", child.identity)
    result = await capture(database, service, fence, root, child, leaf)
    ids = [change.record.id for change in result.changes]
    assert all((await available(database, ids)).values())
    await capture(
        database,
        service,
        fence,
        root.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    assert not any((await available(database, ids)).values())
    await capture(database, service, fence, root)
    assert list((await available(database, ids)).values()).count(True) == 1
    refreshed = await capture(database, service, fence, child)
    assert refreshed.changes[0].record.payload == child.payload
    assert refreshed.changes[0].record.revision == 2
    assert (await available(database, ids))[ids[2]] is False
    await capture(database, service, fence, leaf)
    assert all((await available(database, ids)).values())
    async with database() as db:
        epochs = list((await db.scalars(select(Entity.visibility_epoch))).all())
    assert epochs == [2, 2, 2]
    unchanged = await capture(database, service, fence, child)
    assert unchanged.unchanged == 1
    await capture(
        database, service, fence, root.model_copy(update={"payload": {"new": "metadata"}})
    )
    assert all((await available(database, ids)).values())
    async with database() as db:
        assert (await db.get(Entity, ids[0])).visibility_epoch == 2


async def test_record_cycle_rejected_and_transaction_rolls_back(database, source):
    service, fence = source
    root = node("root")
    child = node("child", root.identity)
    result = await capture(database, service, fence, root, child)
    with pytest.raises(CanonicalStoreError, match="cycle"):
        await capture(database, service, fence, root.model_copy(update={"parent": child.identity}))
    assert all((await available(database, [item.record.id for item in result.changes])).values())
    with pytest.raises(CanonicalStoreError, match="cycle"):
        await capture(
            database,
            service,
            fence,
            root.model_copy(
                update={
                    "parent": child.identity,
                    "kind": "delete",
                    "removal_reason": "provider_deleted",
                }
            ),
        )
    with pytest.raises(CanonicalStoreError, match="parent itself"):
        await capture(database, service, fence, root.model_copy(update={"parent": root.identity}))


async def test_missing_parent_rejected_and_corrupt_record_cycle_fails_closed(database, source):
    service, fence = source
    with pytest.raises(CanonicalStoreError, match="must exist"):
        await capture(
            database,
            service,
            fence,
            node("orphan", RecordIdentity(record_type="block", native_id="missing")),
        )
    root = node("root")
    child = node("child", root.identity)
    result = await capture(database, service, fence, root, child)
    async with database() as db:
        row = await db.get(Entity, result.changes[0].record.id)
        row.parent_record_type = "block"
        row.parent_native_id = "child"
        row.parent_visibility_epoch = 1
        await db.commit()
    async with database() as db:
        await db.execute(text("SET LOCAL statement_timeout='2s'"))
        assert not any(
            (await db.execute(select(content_is_available()).select_from(Entity))).scalars().all()
        )


async def test_deep_same_kind_forest_has_no_twenty_level_cutoff(database, source):
    service, fence = source
    records = [node("0")]
    for index in range(1, 35):
        records.append(node(str(index), records[-1].identity))
    result = await capture(database, service, fence, *records)
    assert all((await available(database, [item.record.id for item in result.changes])).values())


@pytest.mark.parametrize("database", ["forest_upgrade"], indirect=True)
async def test_upgrade_attests_only_existing_available_flat_links(database):
    async with database() as db:
        rows = list((await db.scalars(select(Entity).where(Entity.record_revision > 0))).all())
        states = dict(
            (
                await db.execute(
                    select(Entity.native_id, content_is_available()).where(
                        Entity.record_revision > 0
                    )
                )
            ).all()
        )
    assert states == {
        "active-root": True,
        "active-child": True,
        "withdrawn-root": False,
        "withdrawn-child": False,
        "hidden-child": False,
        "orphan": False,
    }
    assert next(row for row in rows if row.native_id == "active-child").parent_visibility_epoch == 1
    assert all(
        row.source_payload == {"native": row.native_id} and row.record_revision == 1 for row in rows
    )


async def test_cleanup_propagates_ancestor_reason_even_before_intermediate_cleanup(
    database, source
):
    service, fence = source
    root = node("root")
    child = node("child", root.identity)
    leaf = node("leaf", child.identity)
    await capture(database, service, fence, root, child, leaf)
    await capture(
        database,
        service,
        fence,
        root.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    async with database() as db:
        removed = await service.reconcile_parents(db, fence)
    assert len(removed.capture.changes) == 2
    assert {change.record.removal_reason for change in removed.capture.changes} == {
        "access_revoked"
    }


async def test_reparent_invalidates_descendants_and_failed_batch_rolls_back(database, source):
    service, fence = source
    first, second = node("first"), node("second")
    child = node("child", first.identity)
    leaf = node("leaf", child.identity)
    result = await capture(database, service, fence, first, second, child, leaf)
    leaf_id = result.changes[-1].record.id
    moved = child.model_copy(update={"parent": second.identity, "allow_reparent": True})
    with pytest.raises(CanonicalStoreError, match="different parent"):
        await capture(database, service, fence, moved.model_copy(update={"allow_reparent": False}))
    assert (await available(database, [leaf_id]))[leaf_id] is True
    with pytest.raises(CanonicalStoreError):
        await capture(database, service, fence, moved, node("bad", node("missing").identity))
    assert (await available(database, [leaf_id]))[leaf_id] is True
    await capture(database, service, fence, moved)
    replay = await capture(
        database, service, fence, moved.model_copy(update={"allow_reparent": False})
    )
    assert replay.unchanged == 1 and replay.changes == ()
    assert (await available(database, [leaf_id]))[leaf_id] is False
    await capture(database, service, fence, leaf)
    assert (await available(database, [leaf_id]))[leaf_id] is True


@pytest.mark.parametrize("shape,size", [("deep", 96), ("wide", 1024)])
async def test_real_recursive_query_plan(shape, size, database, source):
    import json

    from sqlalchemy.dialects import postgresql

    service, fence = source
    records = [node("root")]
    for index in range(size):
        parent = records[-1] if shape == "deep" else records[0]
        records.append(node(str(index), parent.identity))
    for offset in range(0, len(records), 500):
        await capture(database, service, fence, *records[offset : offset + 500])
    query = select(Entity.id).where(
        Entity.organization_id == fence.organization_id,
        Entity.sync_id == fence.sync_id,
        content_is_available(),
    )
    compiled = query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    async with database() as db:
        await db.execute(text("ANALYZE entity"))
        plan = await db.scalar(text("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + str(compiled)))
    assert plan[0]["Plan"]["Actual Rows"] == size + 1
    assert "Recursive Union" in json.dumps(plan)
    print(
        json.dumps(
            {"forest_shape": shape, "records": size + 1, "execution_ms": plan[0]["Execution Time"]}
        )
    )


async def test_nested_withdrawal_gates_original_blob_history_search_and_publication(
    database, source
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from airweave.domains.entities.canonical.projection_models import ProjectionLocator
    from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
    from airweave.domains.entities.canonical.query import CanonicalQueryService, RecordNotFound
    from airweave.domains.entities.canonical.query_models import RecordFilters
    from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
    from airweave.domains.entities.canonical.requests import BlobReference
    from airweave.domains.entities.canonical.tests.helpers import publish_prepared
    from airweave.domains.entities.canonical.tests.test_search_visibility import hit
    from airweave.domains.search.canonical_visibility import visible_results
    from airweave.models.collection import Collection
    from airweave.models.source_connection import SourceConnection
    from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata

    service, fence = source
    root = node("root")
    child = node("child", root.identity)
    leaf = node("leaf", child.identity).model_copy(
        update={"blobs": (BlobReference(key="not-read", sha256="a" * 64, size_bytes=1),)}
    )
    captured = await capture(database, service, fence, root, child, leaf)
    leaf_id = captured.changes[-1].record.id
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="test", embedding_dimensions=3, sparse_embedder="test"
        )
        db.add(deployment)
        await db.flush()
        db.add(
            Collection(
                name="Test",
                readable_id="test",
                organization_id=fence.organization_id,
                vector_db_deployment_metadata_id=deployment.id,
            )
        )
        await db.flush()
        db.add(
            SourceConnection(
                name="Test",
                short_name="gmail",
                organization_id=fence.organization_id,
                readable_collection_id="test",
                sync_id=fence.sync_id,
                is_authenticated=True,
            )
        )
        await db.commit()
    projection = CanonicalProjectionStore()
    async with database() as db:
        work = next(
            item
            for item in await projection.pending(db, fence.organization_id, fence.sync_id)
            if item.record.id == leaf_id
        )
        generation = uuid4()
        assert await publish_prepared(projection, db, work, generation, 1)
    locator = ProjectionLocator(
        record_id=leaf_id, revision=1, pipeline_version=1, generation=generation, part_index=0
    )
    candidate = hit(fence, locator.encode())
    registry = SimpleNamespace(
        get=lambda _: SimpleNamespace(
            source_class_ref=SimpleNamespace(canonical_record_types=("block",))
        )
    )
    async with database() as db:
        assert await visible_results(db, fence.organization_id, "test", [candidate], registry) == [
            candidate
        ]
    await capture(
        database,
        service,
        fence,
        root.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
    )
    query = CanonicalQueryService(service.store, CanonicalQueryStore(), "test-signing-key")
    storage = AsyncMock()
    async with database() as db:
        exact = await query.read(db, fence.organization_id, fence.sync_id, leaf_id)
        assert exact.content_access == "unavailable" and exact.payload == {} and not exact.blobs
        assert not await query.queries.list_records(
            db, fence.organization_id, fence.sync_id, RecordFilters(), after_id=None, limit=100
        )
        history = await service.store.changes(db, fence.organization_id, fence.sync_id)
        assert all(item.record.content_access == "unavailable" for item in history.changes)
        with pytest.raises(RecordNotFound):
            await query.blob(
                db, fence.organization_id, fence.sync_id, leaf_id, 1, "a" * 64, storage
            )
        assert not storage.mock_calls
        assert not await visible_results(db, fence.organization_id, "test", [candidate], registry)
        assert not await projection.publish(db, work, generation, 1)
