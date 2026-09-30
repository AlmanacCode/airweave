"""Reconcile committed account intent using existing native validation and Temporal."""

import asyncio
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.exceptions import WorkflowAlreadyStartedError

from airweave import schemas
from airweave.api.context import ApiContext
from airweave.core.datetime_utils import utc_now_naive
from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.owned_provisioning.models import EnsureSource, ProvisionedSource
from airweave.domains.owned_provisioning.store import ProvisioningStore, locked_intent
from airweave.domains.sources.protocols import SourceLifecycleServiceProtocol
from airweave.domains.syncs.jobs.protocols import SyncJobRepositoryProtocol
from airweave.domains.syncs.protocols import SyncRepositoryProtocol
from airweave.domains.temporal.protocols import (
    TemporalScheduleServiceProtocol,
    TemporalWorkflowServiceProtocol,
)
from airweave.models.collection import Collection
from airweave.models.connection import Connection
from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob
from airweave.schemas.sync_job import SyncJobCreate

EXECUTION_TIMEOUT_SECONDS = 20


class OwnedProvisioningService:
    """Retries repair one desired relationship; there is no independent job queue."""

    def __init__(
        self,
        store: ProvisioningStore,
        lifecycle: SourceLifecycleServiceProtocol,
        jobs: SyncJobRepositoryProtocol,
        syncs: SyncRepositoryProtocol,
        schedules: TemporalScheduleServiceProtocol,
        workflows: TemporalWorkflowServiceProtocol,
    ):
        """Use existing source verification, job admission and Temporal services."""
        self.store, self.lifecycle = store, lifecycle
        self.jobs, self.syncs = jobs, syncs
        self.schedules, self.workflows = schedules, workflows

    async def ensure(
        self, db: AsyncSession, ctx: ApiContext, account: UUID, request: EnsureSource
    ) -> ProvisionedSource:
        """Persist before verification/execution; failures are retried with the same request."""
        await self.store.ensure(db, ctx, account, request)
        return await self.reconcile(db, ctx, account, request.generation)

    async def get(self, db: AsyncSession, ctx: ApiContext, account: UUID) -> ProvisionedSource:
        """Read only current committed relationship; never infer completion from IDs."""
        row = await db.scalar(
            select(OwnedProvisioning)
            .where(
                OwnedProvisioning.organization_id == ctx.organization.id,
                OwnedProvisioning.client_namespace == "almanac",
                OwnedProvisioning.account_id == account,
            )
            .execution_options(populate_existing=True)
        )
        if row is None:
            raise HTTPException(status_code=404, detail="Owned account source not found")
        spec = EnsureSource.model_validate(row.request_payload)
        return ProvisionedSource(
            account_id=row.account_id,
            organization_id=row.organization_id,
            generation=row.generation,
            observed_generation=row.observed_generation,
            state=(
                "pending"
                if row.observed_generation != row.generation
                else "ready"
                if row.desired_state == "active"
                else "disconnected"
            ),
            source_connection_id=row.source_connection_id,
            sync_id=row.sync_id,
            expected_identity=spec.source.expected_identity if spec.source else None,
        )

    async def reconcile(
        self, db: AsyncSession, ctx: ApiContext, account: UUID, generation: int
    ) -> ProvisionedSource:
        """Provider requests outside locks; late success must pass generation CAS."""
        async with UnitOfWork(db):
            row = await locked_intent(db, ctx.organization.id, account)
            self._current(row, generation)
            done = row.observed_generation == generation
            source_id, sync_id = row.source_connection_id, row.sync_id
            active = row.desired_state == "active"
            needs_verification = active and row.verified_at is None
        if done:
            return await self.get(db, ctx, account)
        if needs_verification:
            # create performs the connector's native principal validation, not
            # merely connected-account metadata validation in Composio.
            verified_source = await self.lifecycle.create(db, source_id, ctx)
            await verified_source.http_client.aclose()
            await db.commit()
            async with UnitOfWork(db) as uow:
                row = await locked_intent(db, ctx.organization.id, account)
                self._current(row, generation)
                sync = await db.scalar(
                    select(Sync)
                    .where(Sync.id == sync_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                source = await db.get(SourceConnection, source_id)
                sync.provisioning_ready_generation = generation
                sync.status = "active"
                source.is_authenticated = True
                row.verified_at = utc_now_naive()
                if row.initial_job_id is None:
                    job = await self.jobs.create(db, SyncJobCreate(sync_id=sync.id), ctx, uow=uow)
                    await db.flush()
                    row.initial_job_id = job.id
        try:
            async with asyncio.timeout(EXECUTION_TIMEOUT_SECONDS):
                await self._execution(db, ctx, account, generation)
        except TimeoutError:
            raise HTTPException(
                status_code=503, detail="Owned capture setup is still pending"
            ) from None
        return await self.get(db, ctx, account)

    @staticmethod
    def _current(row: OwnedProvisioning, generation: int) -> None:
        if row.generation != generation:
            raise HTTPException(status_code=409, detail="Account generation was superseded")

    async def _execution(
        self, db: AsyncSession, ctx: ApiContext, account: UUID, generation: int
    ) -> None:
        # Only bounded Temporal RPCs occur under this relationship lock. This
        # serializes older schedule creation against newer disconnect cleanup.
        async with UnitOfWork(db) as uow:
            row = await locked_intent(db, ctx.organization.id, account)
            self._current(row, generation)
            if row.observed_generation == generation:
                return
            for job_id in row.cancellation_job_ids:
                result = await self.workflows.cancel_sync_job_workflow(job_id, ctx)
                if not result["success"]:
                    raise HTTPException(
                        status_code=503, detail="Owned capture cancellation is pending"
                    )
            row.cancellation_job_ids = []
            if row.desired_state == "disconnected":
                if row.sync_id is not None:
                    await self.schedules.delete_all_schedules_for_sync(
                        row.sync_id, db, ctx, uow=uow, strict=True
                    )
            else:
                await self._start(db, ctx, row, uow)
            row.observed_generation = generation

    async def _start(
        self, db: AsyncSession, ctx: ApiContext, row: OwnedProvisioning, uow: UnitOfWork
    ) -> None:
        spec = EnsureSource.model_validate(row.request_payload).source
        source = await db.get(SourceConnection, row.source_connection_id)
        # A prior RPC may have succeeded before its DB link committed. Remove
        # deterministic handles even when temporal_schedule_id is absent.
        await self.schedules.delete_all_schedules_for_sync(
            row.sync_id, db, ctx, uow=uow, strict=True
        )
        await self.schedules.create_or_update_schedule(
            row.sync_id,
            spec.cron,
            db,
            ctx,
            uow,
            collection_readable_id=source.readable_collection_id,
            connection_id=source.connection_id,
        )
        job = await db.get(SyncJob, row.initial_job_id)
        if job.status not in ("pending", "running"):
            return  # A lost reply may be retried after the initial run already finished.
        sync = await self.syncs.get(db, row.sync_id, ctx)
        collection = await db.scalar(
            select(Collection).where(
                Collection.organization_id == ctx.organization.id,
                Collection.readable_id == source.readable_collection_id,
            )
        )
        connection = await db.get(Connection, source.connection_id)
        try:
            await self.workflows.run_source_connection_workflow(
                sync=sync,
                sync_job=schemas.SyncJob.model_validate(job, from_attributes=True),
                collection=schemas.CollectionRecord.model_validate(
                    collection, from_attributes=True
                ),
                connection=schemas.Connection.model_validate(connection, from_attributes=True),
                ctx=ctx,
            )
        except WorkflowAlreadyStartedError:
            pass  # Same durable job ID is the exact same execution, not a new attempt.
