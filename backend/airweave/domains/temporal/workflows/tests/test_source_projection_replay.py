"""Replay real pre-shared-ID source histories against the current workflow."""

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from airweave.domains.temporal.activities import run_sync_activity
from airweave.domains.temporal.workflows.project_canonical_records import (
    ProjectCanonicalRecordsWorkflow,
)
from airweave.domains.temporal.workflows.run_source_connection import RunSourceConnectionWorkflow

from .conftest import (
    ActivityRecorder,
    make_collection_dict,
    make_connection_dict,
    make_ctx_dict,
    make_sync_dict,
    make_sync_job_dict,
    mock_run_sync,
    mock_transition_sync_job,
)


@workflow.defn(name="RunSourceConnectionWorkflow")
class PreviousSourceWorkflow(RunSourceConnectionWorkflow):
    """Keep the former finalizer's ID, arguments and execution policies verbatim."""

    @workflow.run
    async def run(
        self,
        sync_dict: dict[str, Any],
        sync_job_dict: dict[str, Any] | None,
        collection_dict: dict[str, Any],
        connection_dict: dict[str, Any],
        ctx_dict: dict[str, Any],
        access_token: str | None = None,
        force_full_sync: bool = False,
    ) -> None:
        await super().run(
            sync_dict,
            sync_job_dict,
            collection_dict,
            connection_dict,
            ctx_dict,
            access_token,
            force_full_sync,
        )

    async def _execute_sync(
        self,
        sync_dict,
        sync_job_dict,
        collection_dict,
        connection_dict,
        ctx_dict,
        access_token,
        force_full_sync,
    ) -> None:
        heartbeat = (
            timedelta(hours=1)
            if ctx_dict.get("local_development", False)
            else timedelta(minutes=15)
        )
        try:
            await workflow.execute_activity(
                run_sync_activity,
                args=[
                    sync_dict,
                    sync_job_dict,
                    collection_dict,
                    connection_dict,
                    ctx_dict,
                    access_token,
                    force_full_sync,
                ],
                start_to_close_timeout=timedelta(days=7),
                heartbeat_timeout=heartbeat,
                cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        finally:
            await asyncio.shield(
                workflow.start_child_workflow(
                    ProjectCanonicalRecordsWorkflow.run,
                    args=[str(ctx_dict["organization"]["id"]), str(sync_dict["id"])],
                    id=f"projection:{sync_dict['id']}:{workflow.info().run_id}",
                    parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                )
            )


@pytest.mark.parametrize("local,fails", [(False, False), (True, True)])
async def test_old_source_projection_finalizer_history_replays(local, fails):
    recorder = ActivityRecorder()

    @activity.defn(name="project_canonical_records_activity")
    async def project(organization: str, sync: str, after: str | None = None):
        return {"after_id": None, "has_more": False, "failed": 0, "published": 0}

    ctx = make_ctx_dict()
    ctx["local_development"] = local
    sync = make_sync_dict()
    error = ApplicationError("Synthetic capture failure", non_retryable=True) if fails else None
    async with await WorkflowEnvironment.start_time_skipping() as env:
        queue = "source-projection-replay-" + uuid4().hex
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[PreviousSourceWorkflow, ProjectCanonicalRecordsWorkflow],
            activities=[
                mock_run_sync(recorder, error),
                mock_transition_sync_job(recorder),
                project,
            ],
        ):
            handle = await env.client.start_workflow(
                PreviousSourceWorkflow.run,
                args=[
                    sync,
                    make_sync_job_dict(),
                    make_collection_dict(),
                    make_connection_dict(),
                    ctx,
                    "synthetic-token",
                    True,
                ],
                id=uuid4().hex,
                task_queue=queue,
            )
            if fails:
                with pytest.raises(WorkflowFailureError):
                    await handle.result()
            else:
                await handle.result()
            history = await handle.fetch_history()
            children = [
                event.start_child_workflow_execution_initiated_event_attributes
                for event in history.events
                if event.HasField("start_child_workflow_execution_initiated_event_attributes")
            ]
            assert len(children) == 1
            assert children[0].workflow_id.startswith(f"projection:{sync['id']}:")
            assert len(children[0].input.payloads) == 2
            await env.client.get_workflow_handle(children[0].workflow_id).result()
        await Replayer(workflows=[RunSourceConnectionWorkflow]).replay_workflow(history)
    assert recorder.called("transition_failed" if fails else "transition_completed")
