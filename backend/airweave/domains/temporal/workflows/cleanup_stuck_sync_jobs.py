"""Cleanup stuck sync jobs workflow — periodic detection and cancellation of stuck jobs."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

with workflow.unsafe.imports_passed_through():
    from airweave.domains.temporal.activities import cleanup_stuck_sync_jobs_activity
    from airweave.domains.temporal.activities.cleanup_projection_generations import (
        CleanupProjectionGenerationsActivity,
    )
    from airweave.domains.temporal.workflows.recover_native_projection import (
        RecoverNativeProjectionWorkflow,
    )

_CLEANUP_TIMEOUT = timedelta(minutes=5)
_CLEANUP_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=10),
    maximum_interval=timedelta(seconds=60),
)


@workflow.defn
class CleanupStuckSyncJobsWorkflow:
    """Workflow for cleaning up stuck sync jobs.

    Scheduled to run periodically (every 150 seconds) to:
    - Cancel jobs stuck in CANCELLING/PENDING for > 3 minutes
    - Cancel jobs in RUNNING for > 15 minutes with no entity updates
    """

    @workflow.run
    async def run(self) -> None:
        """Preserve cleanup failures while independently dispatching native recovery."""
        try:
            await workflow.execute_activity(
                cleanup_stuck_sync_jobs_activity,  # type: ignore[arg-type]
                start_to_close_timeout=_CLEANUP_TIMEOUT,
                retry_policy=_CLEANUP_RETRY,
            )
            if workflow.patched("canonical-generation-gc-v1"):
                await workflow.execute_activity(
                    CleanupProjectionGenerationsActivity.run,
                    start_to_close_timeout=_CLEANUP_TIMEOUT,
                    retry_policy=_CLEANUP_RETRY,
                )
        finally:
            if workflow.patched("native-projection-recovery-v1"):
                try:
                    await workflow.start_child_workflow(
                        RecoverNativeProjectionWorkflow.run,
                        id="native-projection-recovery",
                        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
                        parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                    )
                except WorkflowAlreadyStartedError:
                    pass
