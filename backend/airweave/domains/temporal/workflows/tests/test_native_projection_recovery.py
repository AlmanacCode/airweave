"""Real Temporal child deduplication, page progress and bounded failure recovery."""

import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from airweave.domains.temporal.workflows.cleanup_stuck_sync_jobs import CleanupStuckSyncJobsWorkflow
from airweave.domains.temporal.workflows.project_canonical_records import (
    ProjectCanonicalRecordsWorkflow,
)
from airweave.domains.temporal.workflows.recover_native_projection import (
    RecoverNativeProjectionWorkflow,
)


@workflow.defn(name="CleanupStuckSyncJobsWorkflow")
class PreviousCleanupWorkflow:
    """Previous command sequence, used to generate real pre-recovery history."""

    @workflow.run
    async def run(self) -> None:
        retry = RetryPolicy(
            maximum_attempts=3,
            initial_interval=timedelta(seconds=10),
            maximum_interval=timedelta(seconds=60),
        )
        await workflow.execute_activity(
            "cleanup_stuck_sync_jobs_activity",
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry,
        )
        if workflow.patched("canonical-generation-gc-v1"):
            await workflow.execute_activity(
                "cleanup_projection_generations_activity",
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=retry,
            )


@pytest.mark.parametrize("failed", [False, True])
async def test_previous_maintenance_history_replays_without_recovery_commands(failed):
    @activity.defn(name="cleanup_stuck_sync_jobs_activity")
    async def cleanup():
        if failed:
            raise ApplicationError("Previous cleanup failed", non_retryable=True)

    @activity.defn(name="cleanup_projection_generations_activity")
    async def generations():
        pass

    async with await WorkflowEnvironment.start_time_skipping() as env:
        queue = "native-replay-" + uuid4().hex
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[PreviousCleanupWorkflow],
            activities=[cleanup, generations],
        ):
            handle = await env.client.start_workflow(
                PreviousCleanupWorkflow.run, id=uuid4().hex, task_queue=queue
            )
            if failed:
                with pytest.raises(WorkflowFailureError):
                    await handle.result()
            else:
                await handle.result()
            history = await handle.fetch_history()
        await Replayer(workflows=[CleanupStuckSyncJobsWorkflow]).replay_workflow(history)


async def test_recovery_walks_later_page_despite_active_source_and_cleanup_failure():
    organization = str(uuid4())
    sources = [str(UUID(int=i)) for i in range(1, 22)]
    calls, cursors, cleanup_calls = [], [], []
    started, release = asyncio.Event(), asyncio.Event()

    @activity.defn(name="project_canonical_records_activity")
    async def project(org: str, sync: str, after: str | None = None, skip_failed: bool = False):
        assert org == organization and skip_failed
        calls.append(sync)
        if sync == sources[0]:
            started.set()
            await release.wait()
        return {"after_id": None, "has_more": False, "failed": 0, "published": 1}

    @activity.defn(name="discover_native_projection_activity")
    async def discover(after: str | None = None):
        cursors.append(after)
        page = sources[:20] if after is None else sources[20:]
        return {
            "sources": [{"organization_id": organization, "sync_id": sync} for sync in page],
            "next_cursor": sources[19] if after is None else None,
        }

    @activity.defn(name="cleanup_stuck_sync_jobs_activity")
    async def cleanup():
        raise ApplicationError("Unrelated provider cleanup failed", non_retryable=True)

    @activity.defn(name="cleanup_projection_generations_activity")
    async def generations():
        cleanup_calls.append("generations")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        queue = "native-recovery-" + uuid4().hex
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[
                CleanupStuckSyncJobsWorkflow,
                RecoverNativeProjectionWorkflow,
                ProjectCanonicalRecordsWorkflow,
            ],
            activities=[project, discover, cleanup, generations],
        ):
            active = await env.client.start_workflow(
                ProjectCanonicalRecordsWorkflow.run,
                args=[organization, sources[0], None, 0, 0, True],
                id=f"canonical-projection:{organization}:{sources[0]}",
                task_queue=queue,
            )
            await asyncio.wait_for(started.wait(), timeout=20)
            try:
                with pytest.raises(WorkflowFailureError):
                    await env.client.execute_workflow(
                        CleanupStuckSyncJobsWorkflow.run, id=uuid4().hex, task_queue=queue
                    )
                await env.client.get_workflow_handle("native-projection-recovery").result()
                release.set()
                await active.result()
                for sync in sources[1:]:
                    await env.client.get_workflow_handle(
                        f"canonical-projection:{organization}:{sync}"
                    ).result()
            finally:
                release.set()
    assert cleanup_calls == ["generations"]
    assert cursors == [None, sources[19]]
    assert sorted(calls) == sorted(sources)


async def test_automatic_recovery_reports_failure_without_restarting_failed_sweeps():
    calls = []

    @activity.defn(name="project_canonical_records_activity")
    async def project(org: str, sync: str, after: str | None = None, skip_failed: bool = False):
        calls.append(skip_failed)
        return {"after_id": None, "has_more": False, "failed": 1, "published": 0}

    async with await WorkflowEnvironment.start_time_skipping() as env:
        queue = "native-failure-" + uuid4().hex
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[ProjectCanonicalRecordsWorkflow],
            activities=[project],
        ):
            with pytest.raises(WorkflowFailureError):
                await env.client.execute_workflow(
                    ProjectCanonicalRecordsWorkflow.run,
                    args=[str(uuid4()), str(uuid4()), None, 0, 0, True],
                    id=uuid4().hex,
                    task_queue=queue,
                )
    assert calls == [True]


async def test_maintenance_projects_during_capture_and_finalizer_reuses_active_projection():
    """Capture need not finish for search publication, and both triggers share ownership."""
    from airweave.domains.temporal.workflows.run_source_connection import (
        RunSourceConnectionWorkflow,
    )

    from .conftest import (
        ORG_ID,
        SYNC_ID,
        ActivityRecorder,
        make_collection_dict,
        make_connection_dict,
        make_ctx_dict,
        make_sync_dict,
        make_sync_job_dict,
        mock_transition_sync_job,
    )

    capturing, projected = asyncio.Event(), asyncio.Event()
    finish_capture, finish_projection = asyncio.Event(), asyncio.Event()
    calls = []

    @activity.defn(name="run_sync_activity")
    async def capture(
        sync: dict,
        job: dict,
        collection: dict,
        connection: dict,
        context: dict,
        token: str | None,
        force: bool,
    ):
        capturing.set()
        await finish_capture.wait()

    @activity.defn(name="discover_native_projection_activity")
    async def discover(after: str | None = None):
        assert capturing.is_set() and not finish_capture.is_set()
        return {"sources": [{"organization_id": ORG_ID, "sync_id": SYNC_ID}], "next_cursor": None}

    @activity.defn(name="project_canonical_records_activity")
    async def project(org: str, sync: str, after: str | None = None, skip_failed: bool = False):
        assert skip_failed
        calls.append(sync)
        projected.set()
        await finish_projection.wait()
        return {"after_id": None, "has_more": False, "failed": 0, "published": 1}

    recorder = ActivityRecorder()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        queue = "concurrent-projection-" + uuid4().hex
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[
                RunSourceConnectionWorkflow,
                RecoverNativeProjectionWorkflow,
                ProjectCanonicalRecordsWorkflow,
            ],
            activities=[capture, discover, project, mock_transition_sync_job(recorder)],
        ):
            handle = await env.client.start_workflow(
                RunSourceConnectionWorkflow.run,
                args=[
                    make_sync_dict(),
                    make_sync_job_dict(),
                    make_collection_dict(),
                    make_connection_dict(),
                    make_ctx_dict(),
                ],
                id=uuid4().hex,
                task_queue=queue,
            )
            try:
                await asyncio.wait_for(capturing.wait(), 20)
                await env.client.execute_workflow(
                    RecoverNativeProjectionWorkflow.run, id=uuid4().hex, task_queue=queue
                )
                await asyncio.wait_for(projected.wait(), 20)
                assert not finish_capture.is_set()
                finish_capture.set()
                await handle.result()
                assert recorder.called("transition_completed")
                assert calls == [SYNC_ID]
            finally:
                finish_capture.set()
                finish_projection.set()
            await env.client.get_workflow_handle(
                f"canonical-projection:{ORG_ID}:{SYNC_ID}"
            ).result()


@workflow.defn(name="RecoverNativeProjectionWorkflow")
class PreviousRecoveryWorkflow:
    """The historical native-only child ID must remain replayable."""

    @workflow.run
    async def run(self, after_sync_id: str | None = None) -> None:
        page = await workflow.execute_activity(
            "discover_native_projection_activity",
            after_sync_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        for source in page["sources"]:
            await workflow.start_child_workflow(
                ProjectCanonicalRecordsWorkflow.run,
                args=[source["organization_id"], source["sync_id"], None, 0, 0, True],
                id=f"native-projection:{source['organization_id']}:{source['sync_id']}",
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )


async def test_previous_source_projection_identity_replays():
    organization, sync = str(uuid4()), str(uuid4())

    @activity.defn(name="discover_native_projection_activity")
    async def discover(after: str | None = None):
        return {
            "sources": [{"organization_id": organization, "sync_id": sync}],
            "next_cursor": None,
        }

    @activity.defn(name="project_canonical_records_activity")
    async def project(org: str, sync: str, after: str | None = None, skip_failed: bool = False):
        return {"after_id": None, "has_more": False, "failed": 0, "published": 1}

    async with await WorkflowEnvironment.start_time_skipping() as env:
        queue = "previous-projection-" + uuid4().hex
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[PreviousRecoveryWorkflow, ProjectCanonicalRecordsWorkflow],
            activities=[discover, project],
        ):
            handle = await env.client.start_workflow(
                PreviousRecoveryWorkflow.run, id=uuid4().hex, task_queue=queue
            )
            await handle.result()
            history = await handle.fetch_history()
        await Replayer(workflows=[RecoverNativeProjectionWorkflow]).replay_workflow(history)
