"""Real Temporal test-server proof of independent bounded projection retries."""

from uuid import uuid4

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from airweave.domains.temporal.workflows.project_canonical_records import (
    ProjectCanonicalRecordsWorkflow,
)


async def test_projection_retries_failed_sweep_without_provider_capture():
    calls = []

    @activity.defn(name="project_canonical_records_activity")
    async def project(organization: str, sync: str, after: str | None = None) -> dict:
        calls.append((organization, sync, after))
        return {
            "after_id": None,
            "has_more": False,
            "failed": int(len(calls) == 1),
            "published": int(len(calls) > 1),
            "superseded": 0,
        }

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="canonical-projection-test",
            workflows=[ProjectCanonicalRecordsWorkflow],
            activities=[project],
        ):
            await env.client.execute_workflow(
                ProjectCanonicalRecordsWorkflow.run,
                args=[str(uuid4()), str(uuid4())],
                id=str(uuid4()),
                task_queue="canonical-projection-test",
            )
    assert len(calls) == 2
    assert calls[0] == calls[1]
