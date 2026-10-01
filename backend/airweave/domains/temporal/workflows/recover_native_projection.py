"""Recover native publications independently of unrelated cleanup outcomes."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

with workflow.unsafe.imports_passed_through():
    from airweave.domains.entities.canonical.projection_models import ProjectionSourcePage
    from airweave.domains.temporal.activities.discover_native_projection import (
        DiscoverNativeProjectionActivity,
    )
    from airweave.domains.temporal.workflows.project_canonical_records import (
        ProjectCanonicalRecordsWorkflow,
    )


@workflow.defn
class RecoverNativeProjectionWorkflow:
    """Page through durable pending records; never recapture or retry known failures."""

    @workflow.run
    async def run(self, after_sync_id: str | None = None) -> None:
        """Advance every source page independently of its child projection outcome."""
        # Keyset progress lives in workflow history, not another checkpoint table.
        # A bad/active source cannot keep every later source off the first page.
        for _ in range(100):
            page = ProjectionSourcePage.model_validate(
                await workflow.execute_activity(
                    DiscoverNativeProjectionActivity.run,
                    after_sync_id,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            )
            for source in page.sources:
                try:
                    await workflow.start_child_workflow(
                        ProjectCanonicalRecordsWorkflow.run,
                        args=[str(source.organization_id), str(source.sync_id), None, 0, 0, True],
                        id=f"native-projection:{source.organization_id}:{source.sync_id}",
                        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
                        parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                    )
                except WorkflowAlreadyStartedError:
                    pass  # An existing run owns this source; keep walking other pages.
            if page.next_cursor is None:
                return
            after_sync_id = str(page.next_cursor)
        workflow.continue_as_new(args=[after_sync_id])
