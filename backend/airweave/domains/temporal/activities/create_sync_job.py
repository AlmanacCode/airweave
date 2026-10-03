"""Create sync job activity — creates a new sync job record for scheduled runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.exceptions import ApplicationError

from airweave import schemas
from airweave.core.context import BaseContext
from airweave.core.events.sync import SyncLifecycleEvent
from airweave.core.exceptions import NotFoundException
from airweave.core.protocols import EventBus
from airweave.core.shared_models import SyncJobStatus, SyncStatus
from airweave.crud.crud_sync_job import SyncJobBusy
from airweave.db.session import get_tenant_db_context
from airweave.domains.collections.protocols import CollectionRepositoryProtocol
from airweave.domains.connections.protocols import ConnectionRepositoryProtocol
from airweave.domains.source_connections.protocols import SourceConnectionRepositoryProtocol
from airweave.domains.syncs.jobs.protocols import SyncJobRepositoryProtocol
from airweave.domains.syncs.protocols import SyncRepositoryProtocol
from airweave.domains.temporal.activities.context import build_activity_context
from airweave.domains.temporal.activity_results import CreateSyncJobResult
from airweave.models.sync_job import SyncJob


@dataclass
class CreateSyncJobActivity:
    """Create a new sync job record.

    Dependencies:
        event_bus: Publish PENDING event when job is created.
        sync_repo: Verify sync still exists.
        sync_job_repo: Create jobs and check for running jobs.
        sc_repo: Look up source connection for lifecycle events.
        conn_repo: Look up connection for lifecycle events.
        collection_repo: Look up collection for lifecycle events.

    Returns a CreateSyncJobResult with the job dict, or orphaned/skipped flags.
    """

    event_bus: EventBus
    sync_repo: SyncRepositoryProtocol
    sync_job_repo: SyncJobRepositoryProtocol
    sc_repo: SourceConnectionRepositoryProtocol
    conn_repo: ConnectionRepositoryProtocol
    collection_repo: CollectionRepositoryProtocol

    @activity.defn(name="create_sync_job_activity")
    async def run(
        self,
        sync_id: str,
        ctx_dict: Dict[str, Any],
        force_full_sync: bool = False,
    ) -> CreateSyncJobResult:
        """Create a new sync job for the given sync.

        Args:
            sync_id: The sync ID to create a job for
            ctx_dict: The API context as dict
            force_full_sync: If True (daily cleanup), defer busy admission through Temporal retry

        Returns:
            CreateSyncJobResult with sync_job_dict, or orphaned/skipped flags.
        """
        try:
            return await self._admit(sync_id, ctx_dict, force_full_sync)
        except ApplicationError:
            raise
        except Exception as exc:
            raise ApplicationError(
                "Sync job admission failed", type="SyncJobAdmissionFailed", non_retryable=True
            ) from exc

    async def _admit(
        self, sync_id: str, ctx_dict: Dict[str, Any], force_full_sync: bool
    ) -> CreateSyncJobResult:
        """One short SQL attempt; Temporal owns waiting between busy attempts."""
        ctx = await build_activity_context(ctx_dict, sync_id=sync_id)
        organization = ctx.organization

        ctx.logger.info(f"Creating sync job for sync {sync_id} (force_full_sync={force_full_sync})")

        async with get_tenant_db_context(ctx.organization.id) as db:
            try:
                sync = await self.sync_repo.get_without_connections(
                    db=db,
                    id=UUID(sync_id),
                    ctx=ctx,
                )
            except NotFoundException as e:
                ctx.logger.info(
                    f"🧹 Could not verify sync {sync_id} exists: {e}. "
                    f"Marking as orphaned to trigger cleanup."
                )
                return CreateSyncJobResult(
                    orphaned=True, sync_id=sync_id, reason=f"Sync lookup error: {e}"
                )

            if sync is None:
                ctx.logger.info(f"Sync {sync_id} not found, marking as orphaned.")
                return CreateSyncJobResult(orphaned=True, sync_id=sync_id, reason="Sync not found")

            if SyncStatus(sync.status) != SyncStatus.ACTIVE:
                ctx.logger.info(f"Sync {sync_id} is {sync.status}, skipping job creation.")
                return CreateSyncJobResult(
                    skipped=True,
                    sync_id=sync_id,
                    reason=f"Sync status is {sync.status}",
                )

            info = activity.info()
            operation_id = uuid5(
                NAMESPACE_URL,
                f"sync-job:{organization.id}:{sync_id}:{info.workflow_namespace}:"
                f"{info.workflow_run_id}:{info.activity_id}",
            )
            sync_job_in = schemas.SyncJobCreate(sync_id=UUID(sync_id), id=operation_id)
            try:
                sync_job = await self.sync_job_repo.create(db=db, obj_in=sync_job_in, ctx=ctx)
            except SyncJobBusy as exc:
                if force_full_sync:
                    raise ApplicationError("Sync is busy", type="SyncJobBusy") from exc
                return CreateSyncJobResult(skipped=True, sync_id=sync_id, reason=exc.detail)
            if sync_job.status in (
                SyncJobStatus.FAILED,
                SyncJobStatus.CANCELLED,
                SyncJobStatus.CANCELLING,
            ):
                raise ApplicationError(
                    "Operation job failed or was cancelled",
                    type="SyncJobAdmissionFailed",
                    non_retryable=True,
                )
            if sync_job.status != SyncJobStatus.PENDING:
                return CreateSyncJobResult(
                    skipped=True, sync_id=sync_id, reason="Operation job is no longer pending"
                )
            sync_job_id = sync_job.id

            ctx.logger.info(f"Created sync job {sync_job_id} for sync {sync_id}")

            await self._publish_pending_event(db, sync_id, organization, sync_job, ctx)

            sync_job_schema = schemas.SyncJob.model_validate(sync_job)
            return CreateSyncJobResult(
                sync_job_dict=sync_job_schema.model_dump(mode="json"),
                sync_id=sync_id,
            )

    async def _publish_pending_event(
        self,
        db: AsyncSession,
        sync_id: str,
        organization: schemas.Organization,
        sync_job: SyncJob,
        ctx: BaseContext,
    ) -> None:
        """Publish PENDING lifecycle event."""
        try:
            source_conn = await self.sc_repo.get_by_sync_id(
                db=db,
                sync_id=UUID(sync_id),
                ctx=ctx,
            )
            if source_conn and source_conn.connection_id:
                connection = await self.conn_repo.get(
                    db=db,
                    id=source_conn.connection_id,
                    ctx=ctx,
                )
                collection = await self.collection_repo.get_by_readable_id(
                    db=db,
                    readable_id=str(source_conn.readable_collection_id),
                    ctx=ctx,
                )
                if connection and collection:
                    await self.event_bus.publish(
                        SyncLifecycleEvent.pending(
                            organization_id=organization.id,
                            source_connection_id=UUID(str(source_conn.id)),
                            sync_job_id=UUID(str(sync_job.id)),
                            sync_id=UUID(sync_id),
                            collection_id=UUID(str(collection.id)),
                            source_type=connection.short_name,
                            collection_name=collection.name,
                            collection_readable_id=str(collection.readable_id),
                        )
                    )
        except Exception as event_err:
            ctx.logger.warning(f"Failed to publish pending event: {event_err}")
