"""Dry-run by default; scoped re-projection without provider capture or writes."""

import argparse
import asyncio
from uuid import UUID

from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from airweave.core.config import settings
from airweave.db.session import AsyncSessionLocal
from airweave.domains.entities.canonical.reprojection import plan_reprojection
from airweave.domains.temporal.client import get_client
from airweave.domains.temporal.workflows.project_canonical_records import (
    ProjectCanonicalRecordsWorkflow,
)


async def run(args: argparse.Namespace) -> None:
    """Commit durable pending state, then start/resume the existing Temporal consumer."""
    async with AsyncSessionLocal() as db:
        plan = await plan_reprojection(
            db,
            args.organization,
            args.sync,
            expected_version=args.expect,
            target_version=args.target,
            apply=args.apply,
        )
        if args.apply:
            await db.commit()
        else:
            await db.rollback()
    print(plan.model_dump_json())
    if not args.apply:
        return
    # Non-atomic by design: a launch failure leaves pending rows durable. Repeat
    # the exact expect/target command; don't choose another version to retry.
    client = await get_client()
    try:
        await client.start_workflow(
            ProjectCanonicalRecordsWorkflow.run,
            args=[str(args.organization), str(args.sync)],
            id=plan.workflow_id,
            task_queue=settings.TEMPORAL_TASK_QUEUE,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
    except WorkflowAlreadyStartedError as exc:
        raise SystemExit(
            "Projection is already running. The requested version remains durable; "
            "retry this exact command after it finishes to include previously failed records."
        ) from exc
    print("Projection workflow accepted; this does not mean indexing is complete.")


def main() -> None:
    """Parse explicit scope and version pair; dry-run unless --apply is supplied."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--organization", type=UUID, required=True)
    parser.add_argument("--sync", type=UUID, required=True)
    parser.add_argument("--expect", type=int, required=True)
    parser.add_argument("--target", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
