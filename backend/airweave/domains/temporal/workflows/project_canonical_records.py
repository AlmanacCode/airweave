"""Independent retryable projection on the existing Temporal worker/task queue."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from airweave.domains.temporal.activities.project_canonical_records import (
        ProjectCanonicalRecordsActivity,
    )


@workflow.defn
class ProjectCanonicalRecordsWorkflow:
    """Scan pending rows; publication failures never change capture-job status."""

    @workflow.run
    async def run(
        self,
        organization_id: str,
        sync_id: str,
        after_id: str | None = None,
        failures: int = 0,
        sweep: int = 0,
        skip_failed: bool = False,
    ) -> None:
        """Drain bounded pages; retry failed sweeps without recapturing providers."""
        for _ in range(100):
            result = await workflow.execute_activity(
                ProjectCanonicalRecordsActivity.run,
                args=(
                    [organization_id, sync_id, after_id, True]
                    if skip_failed
                    else [organization_id, sync_id, after_id]
                ),
                start_to_close_timeout=timedelta(minutes=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            failures += result["failed"]
            after_id = result["after_id"]
            if not result["has_more"]:
                if not failures:
                    return
                if skip_failed or sweep >= 2:
                    raise ApplicationError(
                        "Canonical projection still has failed records; retained as pending",
                        non_retryable=True,
                    )
                await workflow.sleep(timedelta(seconds=30 * (2**sweep)))
                workflow.continue_as_new(args=[organization_id, sync_id, None, 0, sweep + 1])
            # Bound history even for very large sources.
        args = [organization_id, sync_id, after_id, failures, sweep]
        if skip_failed:
            args.append(True)
        workflow.continue_as_new(args=args)
