"""Production admission policy yields four SDK slots while forced syncs are busy."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

from temporalio import activity, workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from airweave.domains.temporal.workflows.run_source_connection import (
        RunSourceConnectionWorkflow,
    )


@workflow.defn
class AdmissionWorkflow:
    """Exercise the production policy without invoking provider work."""

    @workflow.run
    async def run(self, args: dict) -> None:
        """Admission returns a skipped terminal operation after the active job completes."""
        await RunSourceConnectionWorkflow()._ensure_sync_job(
            {"id": args["sync"]}, None, args["context"], args.get("force", True)
        )


@workflow.defn
class CompletionWorkflow:
    """A queued activity represents work that must finish before admission can succeed."""

    @workflow.run
    async def run(self) -> None:
        """Run only once a SDK activity slot has been released."""
        await workflow.execute_activity(
            "finish_active_job", start_to_close_timeout=timedelta(seconds=5)
        )


async def test_four_busy_admissions_release_slots_sql_and_keep_operation_identity():
    """SDK backoff holds neither the SQL context nor an activity execution slot."""
    from airweave.adapters.event_bus.fake import FakeEventBus
    from airweave.crud.crud_sync_job import SyncJobBusy
    from airweave.domains.collections.fakes.repository import FakeCollectionRepository
    from airweave.domains.connections.fakes.repository import FakeConnectionRepository
    from airweave.domains.source_connections.fakes.repository import FakeSourceConnectionRepository
    from airweave.domains.syncs.fakes.repository import FakeSyncRepository
    from airweave.domains.syncs.jobs.fakes.repository import FakeSyncJobRepository
    from airweave.domains.temporal.activities.create_sync_job import CreateSyncJobActivity
    from airweave.domains.temporal.activities.tests.conftest import make_ctx_dict

    sync_id = uuid4()
    syncs = FakeSyncRepository()
    syncs.seed_model(sync_id, MagicMock(status="active"))
    jobs = FakeSyncJobRepository()
    entered = 0
    open_sql = 0
    all_busy = asyncio.Event()
    release = asyncio.Event()
    finished = False
    identities: dict[str, list[UUID]] = {}

    @asynccontextmanager
    async def sql_context(_organization):
        nonlocal open_sql
        open_sql += 1
        try:
            yield AsyncMock()
        finally:
            open_sql -= 1

    async def admission(db, obj_in, ctx):
        nonlocal entered
        identities.setdefault(activity.info().workflow_run_id, []).append(obj_in.id)
        if not finished:
            entered += 1
            if entered == 4:
                all_busy.set()
            await release.wait()
            raise SyncJobBusy()
        return MagicMock(status="completed")

    jobs.create = admission
    actual = CreateSyncJobActivity(
        FakeEventBus(),
        syncs,
        jobs,
        FakeSourceConnectionRepository(),
        FakeConnectionRepository(),
        FakeCollectionRepository(),
    )

    @activity.defn(name="finish_active_job")
    async def finish() -> None:
        nonlocal finished
        assert open_sql == 0
        finished = True

    task_queue = str(uuid4())
    with patch(
        "airweave.domains.temporal.activities.create_sync_job.get_tenant_db_context", sql_context
    ):
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AdmissionWorkflow, CompletionWorkflow],
                activities=[actual.run, finish],
                max_concurrent_activities=4,
            ):
                handles = [
                    await env.client.start_workflow(
                        AdmissionWorkflow.run,
                        {"sync": str(sync_id), "context": make_ctx_dict()},
                        id=str(uuid4()),
                        task_queue=task_queue,
                    )
                    for _ in range(4)
                ]
                try:
                    await asyncio.wait_for(all_busy.wait(), timeout=20)
                    completion = await env.client.start_workflow(
                        CompletionWorkflow.run, id=str(uuid4()), task_queue=task_queue
                    )
                finally:
                    release.set()
                await completion.result()
                for handle in handles:
                    await handle.result()
    assert finished and open_sql == 0
    assert len(identities) == 4
    assert all(len(ids) >= 2 and len(set(ids)) == 1 for ids in identities.values())
    assert len({ids[0] for ids in identities.values()}) == 4


async def test_ordinary_sdk_timeout_after_commit_retries_same_pending_job():
    """A lost activity reply has a reachable bounded recovery, not an abandoned pending job."""
    from airweave.adapters.event_bus.fake import FakeEventBus
    from airweave.domains.collections.fakes.repository import FakeCollectionRepository
    from airweave.domains.connections.fakes.repository import FakeConnectionRepository
    from airweave.domains.source_connections.fakes.repository import FakeSourceConnectionRepository
    from airweave.domains.syncs.fakes.repository import FakeSyncRepository
    from airweave.domains.syncs.jobs.fakes.repository import FakeSyncJobRepository
    from airweave.domains.temporal.activities.create_sync_job import CreateSyncJobActivity
    from airweave.domains.temporal.activities.tests.conftest import make_ctx_dict

    sync_id = uuid4()
    syncs = FakeSyncRepository()
    syncs.seed_model(sync_id, MagicMock(status="active"))
    jobs = FakeSyncJobRepository()
    actual = CreateSyncJobActivity(
        FakeEventBus(),
        syncs,
        jobs,
        FakeSourceConnectionRepository(),
        FakeConnectionRepository(),
        FakeCollectionRepository(),
    )
    committed = asyncio.Event()
    blocked = asyncio.Event()
    calls = 0

    @asynccontextmanager
    async def sql_context(_organization):
        yield AsyncMock()

    @activity.defn(name="create_sync_job_activity")
    async def lost_reply(sync: str, context: dict, force: bool):
        nonlocal calls
        calls += 1
        result = await actual.run(sync, context, force)
        if calls == 1:
            committed.set()
            await blocked.wait()
        return result

    task_queue = str(uuid4())
    with patch(
        "airweave.domains.temporal.activities.create_sync_job.get_tenant_db_context", sql_context
    ):
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AdmissionWorkflow],
                activities=[lost_reply],
            ):
                handle = await env.client.start_workflow(
                    AdmissionWorkflow.run,
                    {"sync": str(sync_id), "context": make_ctx_dict(), "force": False},
                    id=str(uuid4()),
                    task_queue=task_queue,
                )
                await asyncio.wait_for(committed.wait(), timeout=20)
                await asyncio.wait_for(handle.result(), timeout=50)
    assert calls == 2
    assert len(jobs._created) == 1
    creates = [call[2].id for call in jobs._calls if call[0] == "create"]
    assert len(creates) == 2 and creates[0] == creates[1]
