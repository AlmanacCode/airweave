"""Actual SDK admission with the configured pollers and finite activity slots."""

import asyncio
from datetime import timedelta
from uuid import uuid4

from temporalio import activity, workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from airweave.domains.temporal.worker.config import WorkerConfig


@workflow.defn
class CapacityWorkflow:
    """Five activities exercise a four-slot worker without provider or SQL I/O."""

    @workflow.run
    async def run(self) -> None:
        """Schedule all work together; SDK admission owns concurrency."""
        await asyncio.gather(
            *(
                workflow.execute_activity(
                    "capacity_probe", start_to_close_timeout=timedelta(seconds=30)
                )
                for _ in range(5)
            )
        )


async def test_actual_sdk_keeps_fifth_activity_queued_until_a_slot_releases():
    """Pollers may exceed slots; they cannot admit an extra running activity."""
    config = WorkerConfig(
        task_queue=str(uuid4()), metrics_port=8080, graceful_shutdown_timeout_seconds=30
    )
    started = 0
    four = asyncio.Event()
    fifth = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="capacity_probe")
    async def probe() -> None:
        nonlocal started
        started += 1
        if started == 4:
            four.set()
        if started == 5:
            fifth.set()
        await release.wait()

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=config.task_queue,
            workflows=[CapacityWorkflow],
            activities=[probe],
            max_concurrent_activities=config.max_concurrent_activities,
            max_concurrent_workflow_tasks=config.max_concurrent_workflow_tasks,
            max_concurrent_activity_task_polls=config.max_concurrent_activity_polls,
            max_concurrent_workflow_task_polls=config.max_concurrent_workflow_polls,
        ):
            handle = await env.client.start_workflow(
                CapacityWorkflow.run, id=str(uuid4()), task_queue=config.task_queue
            )
            try:
                await asyncio.wait_for(four.wait(), timeout=20)
                await asyncio.sleep(0.05)
                assert started == 4 and not fifth.is_set()
            finally:
                release.set()
            await handle.result()
            assert started == 5
