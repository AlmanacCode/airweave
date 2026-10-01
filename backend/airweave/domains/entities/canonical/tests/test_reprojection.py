"""Real SQL operator scope, dry-run and retry/version fencing."""

from uuid import uuid4

import pytest
from sqlalchemy import select, update

from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore
from airweave.domains.entities.canonical.reprojection import plan_reprojection
from airweave.domains.entities.canonical.tests.test_owned_search import (
    indexed as search_indexed,  # noqa: F401, F811
)
from airweave.models.sync import Sync


async def test_dry_run_then_apply_and_identical_resume(database, search_indexed):  # noqa: F811
    fence, _, _ = search_indexed
    kwargs = {"expected_version": 2, "target_version": 3}
    async with database() as db:
        plan = await plan_reprojection(db, fence.organization_id, fence.sync_id, **kwargs)
        assert not plan.applied and plan.target_pending_records == 1
        await db.commit()
    async with database() as db:
        assert await db.scalar(select(Sync.index_pipeline_version)) == 2
        applied = await plan_reprojection(
            db, fence.organization_id, fence.sync_id, apply=True, **kwargs
        )
        await db.commit()
    async with database() as db:
        resumed = await plan_reprojection(
            db, fence.organization_id, fence.sync_id, apply=True, **kwargs
        )
        assert resumed.current_version == 3 and resumed.workflow_id == applied.workflow_id
        await db.commit()
    async with database() as db:
        pending = await CanonicalProjectionStore().pending(db, fence.organization_id, fence.sync_id)
        assert len(pending) == 1 and pending[0].pipeline_version == 3


async def test_wrong_scope_and_changed_version_fail(database, search_indexed):  # noqa: F811
    fence, _, _ = search_indexed
    async with database() as db:
        with pytest.raises(ValueError, match="unavailable"):
            await plan_reprojection(
                db, uuid4(), fence.sync_id, expected_version=2, target_version=3
            )
        await db.rollback()
        await db.execute(update(Sync).values(index_pipeline_version=4))
        await db.commit()
    async with database() as db:
        with pytest.raises(ValueError, match="changed"):
            await plan_reprojection(
                db,
                fence.organization_id,
                fence.sync_id,
                expected_version=2,
                target_version=3,
                apply=True,
            )


async def test_launch_failure_keeps_queue_and_identical_retry_starts_once(
    database,
    search_indexed,  # noqa: F811
    monkeypatch,
):
    from argparse import Namespace
    from unittest.mock import AsyncMock

    from airweave.domains.entities.canonical.projection_store import publication_matches
    from airweave.models.entity import Entity
    from scripts import reproject_canonical

    fence, locator, _ = search_indexed
    args = Namespace(
        organization=fence.organization_id, sync=fence.sync_id, expect=2, target=3, apply=True
    )
    monkeypatch.setattr(reproject_canonical, "AsyncSessionLocal", database)
    monkeypatch.setattr(
        reproject_canonical, "get_client", AsyncMock(side_effect=RuntimeError("offline"))
    )
    with pytest.raises(RuntimeError, match="offline"):
        await reproject_canonical.run(args)
    async with database() as db:
        assert await db.scalar(select(Sync.index_pipeline_version)) == 3
        visible = await db.scalar(
            select(Entity.id)
            .join(Sync, Sync.id == Entity.sync_id)
            .where(publication_matches(locator))
        )
        assert visible is None
    client = AsyncMock()
    monkeypatch.setattr(reproject_canonical, "get_client", AsyncMock(return_value=client))
    await reproject_canonical.run(args)
    assert client.start_workflow.await_count == 1
    assert client.start_workflow.call_args.kwargs["id"] == (
        f"canonical-projection:{fence.organization_id}:{fence.sync_id}"
    )
    async with database() as db:
        assert await db.scalar(select(Sync.index_pipeline_version)) == 3
