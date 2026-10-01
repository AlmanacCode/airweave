"""Commit desired generation and source configuration before remote execution."""

import hashlib
import json
from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.core.datetime_utils import utc_now_naive
from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.owned_provisioning.models import EnsureSource, native_principal
from airweave.domains.source_connections.protocols import SourceConnectionCreateServiceProtocol
from airweave.domains.sources.protocols import SourceValidationServiceProtocol
from airweave.models.connection import Connection
from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob
from airweave.platform.configs.config import StripeConfig
from airweave.schemas.source_connection import (
    AuthProviderAuthentication,
    ScheduleConfig,
    SourceConnectionCreate,
)


async def locked_intent(db: AsyncSession, organization: UUID, account: UUID) -> OwnedProvisioning:
    """Every generation/execution mutation serializes on this one relationship."""
    row = await db.scalar(
        select(OwnedProvisioning)
        .where(
            OwnedProvisioning.organization_id == organization,
            OwnedProvisioning.client_namespace == "almanac",
            OwnedProvisioning.account_id == account,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Owned account source not found")
    return row


class ProvisioningStore:
    """Source domain creation participates in our transaction; it cannot commit early."""

    def __init__(
        self,
        create: SourceConnectionCreateServiceProtocol,
        validation: SourceValidationServiceProtocol,
    ):
        """Reuse transactional source creation and source-specific validation."""
        self.create = create
        self.validation = validation

    async def ensure(
        self, db: AsyncSession, ctx: ApiContext, account: UUID, request: EnsureSource
    ) -> None:
        """Lost replies replay one source; lower generations cannot overwrite it."""
        payload = request.model_dump(mode="json")
        fingerprint = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        async with UnitOfWork(db) as uow:
            now = utc_now_naive()
            await db.execute(
                insert(OwnedProvisioning)
                .values(
                    id=uuid4(),
                    organization_id=ctx.organization.id,
                    client_namespace="almanac",
                    account_id=account,
                    generation=request.generation,
                    request_hash=fingerprint,
                    request_payload=payload,
                    desired_state=request.state,
                    observed_generation=0,
                    cancellation_job_ids=[],
                    created_at=now,
                    modified_at=now,
                )
                .on_conflict_do_nothing(constraint="uq_owned_provisioning_account")
            )
            row = await locked_intent(db, ctx.organization.id, account)
            if request.generation < row.generation:
                raise HTTPException(status_code=409, detail="Account generation was superseded")
            if request.generation == row.generation and row.request_hash != fingerprint:
                raise HTTPException(
                    status_code=409, detail="Generation already names another request"
                )
            if request.generation == row.generation and (
                row.sync_id is not None or row.desired_state != "active"
            ):
                return
            if row.desired_state == "disconnected" and request.state != "disconnected":
                raise HTTPException(
                    status_code=409, detail="Connect a new account after disconnect"
                )
            if request.state == "active" and row.source_connection_id is not None:
                source = await db.scalar(
                    select(SourceConnection)
                    .where(
                        SourceConnection.id == row.source_connection_id,
                        SourceConnection.organization_id == ctx.organization.id,
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if (
                    source.short_name,
                    native_principal(source.short_name, source.config_fields),
                    source.readable_collection_id,
                ) != (
                    request.source.provider,
                    (request.source.expected_identity, request.source.expected_user_identity),
                    request.source.collection,
                ):
                    raise HTTPException(
                        status_code=409, detail="Reconnect changes original account identity"
                    )
            if request.source is not None:
                await self._configure(db, uow, ctx, row, request)
            if row.sync_id is not None:
                sync = await db.scalar(
                    select(Sync)
                    .where(Sync.id == row.sync_id, Sync.organization_id == ctx.organization.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                sync.provisioning_generation = request.generation
                sync.status = "paused"
                sync.writer_epoch += 1
                jobs = (
                    await db.scalars(
                        select(SyncJob)
                        .where(
                            SyncJob.sync_id == sync.id,
                            SyncJob.status.in_(("pending", "running", "cancelling")),
                        )
                        .with_for_update()
                    )
                ).all()
                row.cancellation_job_ids = list(
                    dict.fromkeys([*row.cancellation_job_ids, *(str(job.id) for job in jobs)])
                )
                for job in jobs:
                    job.status = "cancelled"
                source = await db.get(SourceConnection, row.source_connection_id)
                source.is_authenticated = False
            row.generation = request.generation
            row.request_payload = payload
            row.request_hash = fingerprint
            row.desired_state = request.state
            row.initial_job_id = None
            row.verified_at = None
            await db.flush()

    async def _configure(
        self,
        db: AsyncSession,
        uow: UnitOfWork,
        ctx: ApiContext,
        row: OwnedProvisioning,
        request: EnsureSource,
    ) -> None:
        spec = request.source
        auth = await db.scalar(
            select(Connection).where(
                Connection.organization_id == ctx.organization.id,
                Connection.readable_id == spec.auth_provider,
                Connection.short_name == "composio",
            )
        )
        if auth is None:
            raise HTTPException(
                status_code=400,
                detail="Owned capture requires this organization's Composio connection",
            )
        config = self.validation.validate_config(spec.provider, spec.source_config(), ctx)
        if row.source_connection_id is None:
            created = await self.create.create_deferred(
                db,
                ctx=ctx,
                uow=uow,
                obj_in=SourceConnectionCreate(
                    name="Almanac " + spec.provider,
                    short_name=spec.provider,
                    readable_collection_id=spec.collection,
                    config=config,
                    schedule=ScheduleConfig(cron=spec.cron),
                    sync_immediately=False,
                    authentication=AuthProviderAuthentication(
                        provider_readable_id=spec.auth_provider, provider_config=spec.auth_config()
                    ),
                ),
            )
            if created.sync_id is None:
                raise ValueError("Owned source creation must create its durable sync")
            row.source_connection_id = created.id
            row.sync_id = created.sync_id
        else:
            source = await db.scalar(
                select(SourceConnection)
                .where(
                    SourceConnection.id == row.source_connection_id,
                    SourceConnection.organization_id == ctx.organization.id,
                )
                .with_for_update()
            )
            if spec.provider == "stripe" and (
                StripeConfig.model_validate(source.config_fields or {}).original_capture
                != StripeConfig.model_validate(config).original_capture
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Reconnect changes Stripe account mode, context or API version; "
                    "connect a new account",
                )
            if spec.provider == "github":
                from airweave.platform.configs.config import GitHubConfig

                if GitHubConfig.model_validate(source.config_fields) != GitHubConfig.model_validate(
                    config
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="Reconnect changes GitHub repository selection or capture options; "
                        "connect a new account",
                    )
            if spec.provider == "linear":
                from airweave.platform.configs.config import LinearConfig

                previous_teams = set(LinearConfig.model_validate(source.config_fields).team_ids)
                current_teams = set(LinearConfig.model_validate(config).team_ids)
                if previous_teams != current_teams:
                    raise HTTPException(
                        status_code=409,
                        detail="Reconnect changes selected Linear teams; connect a new account",
                    )
            source.config_fields = config
            source.readable_auth_provider_id = spec.auth_provider
            source.auth_provider_config = spec.auth_config()
            sync = await db.get(Sync, row.sync_id)
            sync.cron_schedule = spec.cron
