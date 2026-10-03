"""Cleanup stuck sync jobs activity — detects and cancels jobs stuck in transitional states."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from temporalio import activity

from airweave import schemas
from airweave.core.context import BaseContext
from airweave.core.datetime_utils import utc_now_naive
from airweave.core.logging import LoggerConfigurator
from airweave.core.redis_client import redis_client
from airweave.core.shared_models import SyncJobStatus
from airweave.db.session import get_db_context, get_tenant_db_context
from airweave.domains.entities.protocols import EntityRepositoryProtocol
from airweave.domains.organizations.protocols import OrganizationRepositoryProtocol
from airweave.domains.syncs.jobs.protocols import (
    SyncJobStateMachineProtocol,
)
from airweave.domains.temporal.protocols import TemporalWorkflowServiceProtocol
from airweave.models.source_connection import SourceConnection
from airweave.models.sync_job import SyncJob

_CANCELLING_PENDING_CUTOFF = timedelta(minutes=3)
_RUNNING_CUTOFF = timedelta(minutes=15)
_POST_CANCEL_SLEEP_S = 2
_STUCK_CANCEL_REASON = "Cancelled by cleanup job (stuck in transitional state)"
_STUCK_RUNNING_REASON = "Timed out by cleanup job (no activity for 15+ minutes)"
_REDIS_SNAPSHOT_KEY_PREFIX = "sync_progress_snapshot"


@dataclass
class CleanupStuckSyncJobsActivity:
    """Clean up sync jobs stuck in transitional states.

    Dependencies:
        temporal_workflow_service: Cancel stuck workflows via Temporal
        state_machine: Validated sync job state transitions
        sync_job_repo: Query stuck jobs
        entity_repo: Fallback entity timestamp checks
        org_repo: Fetch organization for context building

    Detects and cancels:
    - CANCELLING/PENDING jobs stuck for > 3 minutes
    - RUNNING jobs stuck for > 15 minutes with no entity updates
    """

    temporal_workflow_service: TemporalWorkflowServiceProtocol
    state_machine: SyncJobStateMachineProtocol
    entity_repo: EntityRepositoryProtocol
    org_repo: OrganizationRepositoryProtocol

    @activity.defn(name="cleanup_stuck_sync_jobs_activity")
    async def run(self) -> None:
        """Run the cleanup activity."""
        logger = LoggerConfigurator.configure_logger(
            "airweave.temporal.cleanup",
            dimensions={"activity": "cleanup_stuck_sync_jobs"},
        )

        logger.info("Starting cleanup of stuck sync jobs...")

        now = utc_now_naive()
        cancelling_pending_cutoff = now - _CANCELLING_PENDING_CUTOFF
        running_cutoff = now - _RUNNING_CUTOFF

        try:
            all_stuck_jobs = await self._find_stuck_jobs(
                cancelling_pending_cutoff, running_cutoff, logger
            )

            if not all_stuck_jobs:
                logger.info("No stuck jobs found. Cleanup complete.")
                return

            logger.info(f"Processing {len(all_stuck_jobs)} stuck sync jobs...")

            cancelled_count = 0
            failed_count = 0
            for job in all_stuck_jobs:
                if await self._cancel_stuck_job(job, logger):
                    cancelled_count += 1
                else:
                    failed_count += 1

            logger.info(
                f"Cleanup complete. Processed {len(all_stuck_jobs)} stuck jobs: "
                f"{cancelled_count} cancelled, {failed_count} failed"
            )

        except Exception as e:
            logger.error(f"Error during cleanup activity: {e}", exc_info=True)
            raise

    async def _find_stuck_jobs(
        self,
        cancelling_pending_cutoff: datetime,
        running_cutoff: datetime,
        logger: Any,
    ) -> list[SyncJob]:
        """Discover only IDs globally; load/check each candidate under its immutable scope."""
        found = []
        after = None
        while True:
            async with get_db_context() as db:
                candidates = (
                    await db.execute(
                        text(
                            "SELECT organization_id,job_id FROM owned_stale_jobs("
                            ":pending,:running,CAST(:after AS uuid),100)"
                        ),
                        {
                            "pending": cancelling_pending_cutoff,
                            "running": running_cutoff,
                            "after": after,
                        },
                    )
                ).all()
            if not candidates:
                return found
            for organization, job_id in candidates:
                async with get_tenant_db_context(organization) as db:
                    job = await db.get(SyncJob, job_id)
                    if job is None:
                        continue
                    native = await db.scalar(
                        select(SourceConnection.id)
                        .where(
                            SourceConnection.organization_id == organization,
                            SourceConnection.sync_id == job.sync_id,
                            SourceConnection.short_name == "almanac",
                        )
                        .limit(1)
                    )
                    if native is not None:
                        continue
                    if (
                        job.status in (SyncJobStatus.CANCELLING.value, SyncJobStatus.PENDING.value)
                        and job.modified_at < cancelling_pending_cutoff
                    ):
                        found.append(job)
                    elif (
                        job.status == SyncJobStatus.RUNNING.value
                        and job.started_at is not None
                        and job.started_at < running_cutoff
                        and await self._is_running_job_stuck(job, running_cutoff, db, logger)
                    ):
                        found.append(job)
            after = candidates[-1].job_id
            if len(candidates) < 100:
                return found

    async def _is_running_job_stuck(
        self, job: SyncJob, running_cutoff: datetime, db: Any, logger: Any
    ) -> bool:
        """Check if a running job is stuck (no recent activity).

        Uses the caller's DB session for entity timestamp fallback since this
        runs inside the discovery session scope.
        """
        if job.sync_config:
            handlers = job.sync_config.get("handlers", {})
            is_arf_only = not handlers.get("enable_postgres_handler", True)
            if is_arf_only:
                logger.debug(f"Skipping ARF-only job {job.id} from stuck detection")
                return False

        job_id_str = str(job.id)
        snapshot_key = f"{_REDIS_SNAPSHOT_KEY_PREFIX}:{job_id_str}"

        try:
            snapshot_json = await redis_client.client.get(snapshot_key)

            if not snapshot_json:
                logger.debug(f"No snapshot for job {job_id_str} - skipping")
                return False

            snapshot = json.loads(snapshot_json)
            last_update_str = snapshot.get("last_update_timestamp")

            if not last_update_str:
                latest_entity_time = await self.entity_repo.get_latest_entity_time_for_job(
                    db=db, sync_job_id=UUID(str(job.id))
                )
                return latest_entity_time is None or latest_entity_time < running_cutoff

            last_update = datetime.fromisoformat(last_update_str)
            if last_update.tzinfo is not None:
                last_update = last_update.replace(tzinfo=None)

            if last_update < running_cutoff:
                total_ops = sum(
                    snapshot.get(key, 0)
                    for key in ("inserted", "updated", "deleted", "kept", "skipped")
                )
                logger.info(
                    f"Job {job_id_str} last activity at {last_update} "
                    f"({total_ops} total ops) - marking as stuck"
                )
                return True

            logger.debug(f"Job {job_id_str} active at {last_update} - healthy")
            return False

        except Exception as e:
            logger.warning(f"Error checking job {job_id_str}: {e}, falling back to DB check")
            latest_entity_time = await self.entity_repo.get_latest_entity_time_for_job(
                db=db, sync_job_id=UUID(str(job.id))
            )
            return latest_entity_time is None or latest_entity_time < running_cutoff

    async def _cancel_stuck_job(self, job: SyncJob, logger: Any) -> bool:
        """Cancel a single stuck job via Temporal and update database.

        Opens its own DB session for the org lookup so no session is held
        across the Temporal gRPC call, asyncio.sleep, or state machine
        transition (which opens its own session internally).

        RUNNING jobs transition to FAILED (RUNNING -> CANCELLED is invalid).
        CANCELLING/PENDING jobs transition to CANCELLED (valid transitions).
        """
        job_id = str(job.id)
        sync_id = str(job.sync_id)
        org_id = str(job.organization_id)
        current_status = SyncJobStatus(job.status)

        logger.info(
            f"Attempting to cancel stuck job {job_id} "
            f"(status: {job.status}, sync: {sync_id}, org: {org_id})"
        )

        try:
            async with get_tenant_db_context(job.organization_id) as db:
                organization = await self.org_repo.get(
                    db=db,
                    id=job.organization_id,
                    skip_access_validation=True,
                )
        except Exception as e:
            logger.error(f"Failed to fetch organization {org_id} for job {job_id}: {e}")
            return False

        ctx = BaseContext(
            organization=schemas.Organization.model_validate(organization),
            logger=logger,
        )

        try:
            cancel_success = await self.temporal_workflow_service.cancel_sync_job_workflow(
                job_id, ctx
            )

            if cancel_success:
                logger.info(f"Successfully requested Temporal cancellation for job {job_id}")
                await asyncio.sleep(_POST_CANCEL_SLEEP_S)

            if current_status == SyncJobStatus.RUNNING:
                await self.state_machine.transition(
                    sync_job_id=UUID(job_id),
                    target=SyncJobStatus.FAILED,
                    ctx=ctx,
                    error=_STUCK_RUNNING_REASON,
                )
            else:
                await self.state_machine.transition(
                    sync_job_id=UUID(job_id),
                    target=SyncJobStatus.CANCELLED,
                    ctx=ctx,
                    error=_STUCK_CANCEL_REASON,
                )

            logger.info(f"Successfully cleaned up stuck job {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to cancel stuck job {job_id}: {e}", exc_info=True)
            return False
