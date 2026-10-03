"""Commit desired generation and source configuration before remote execution."""

import hashlib
import json
from uuid import UUID, uuid4

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.core.datetime_utils import utc_now_naive
from airweave.core.shared_models import IntegrationType
from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.source_lifecycle import stop_source_writer
from airweave.domains.owned_provisioning.models import EnsureSource, ManagedSource, source_assurance
from airweave.domains.owned_provisioning.settings import OwnedComposioSettings
from airweave.domains.source_connections.protocols import SourceConnectionCreateServiceProtocol
from airweave.domains.sources.protocols import SourceValidationServiceProtocol
from airweave.models.connection import Connection
from airweave.models.organization import Organization
from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_connection import SyncConnection
from airweave.platform.configs.config import StripeConfig
from airweave.schemas.source_connection import (
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


async def owned_creation_spec(
    db: AsyncSession,
    ctx: ApiContext,
    intent_id: UUID,
) -> ManagedSource:
    """Require a persisted tenant intent before admitting secret-free source creation."""
    result = (
        await db.execute(
            select(OwnedProvisioning, Organization.owned_owner_user_id)
            .join(Organization, Organization.id == OwnedProvisioning.organization_id)
            .where(
                OwnedProvisioning.id == intent_id,
                OwnedProvisioning.organization_id == ctx.organization.id,
                OwnedProvisioning.client_namespace == "almanac",
            )
        )
    ).one_or_none()
    if not ctx.is_api_key_auth or result is None:
        raise HTTPException(403, "Owned source creation requires its tenant intent")
    row, owner = result
    if (
        row.source_connection_id is not None
        or row.sync_id is not None
        or row.desired_state != "active"
    ):
        raise HTTPException(403, "Owned source creation requires its tenant intent")
    return _owned_spec(row, owner)


def _owned_spec(row: OwnedProvisioning, owner: str | None) -> ManagedSource:
    try:
        request = EnsureSource.model_validate(row.request_payload)
    except ValidationError:
        raise HTTPException(409, "Owned source configuration requires reprovisioning") from None
    spec = request.source
    if (
        owner is None
        or spec is None
        or spec.user_id != owner
        or request.state != "active"
        or request.generation != row.generation
    ):
        raise HTTPException(409, "Owned source owner or generation mismatch")
    return spec


async def owned_source_spec(
    db: AsyncSession,
    source: SourceConnection,
    organization: UUID,
) -> ManagedSource | None:
    """Verify ownership, generation and source linkage in one tenant-scoped read."""
    if source.organization_id != organization:
        raise HTTPException(409, "Owned source organization mismatch")
    linked = (
        select(SyncConnection.id)
        .join(Connection, Connection.id == SyncConnection.connection_id)
        .where(
            SyncConnection.sync_id == OwnedProvisioning.sync_id,
            SyncConnection.connection_id == source.connection_id,
            Connection.organization_id == organization,
            Connection.short_name == source.short_name,
            Connection.integration_type == IntegrationType.SOURCE,
            Connection.integration_credential_id.is_(None),
        )
        .exists()
    )
    result = (
        await db.execute(
            select(
                OwnedProvisioning,
                Organization.owned_owner_user_id,
                Sync.provisioning_generation,
                linked,
            )
            .join(Organization, Organization.id == OwnedProvisioning.organization_id)
            .outerjoin(
                Sync,
                (Sync.id == OwnedProvisioning.sync_id) & (Sync.organization_id == organization),
            )
            .where(
                OwnedProvisioning.organization_id == organization,
                OwnedProvisioning.source_connection_id == source.id,
                OwnedProvisioning.client_namespace == "almanac",
            )
        )
    ).one_or_none()
    if result is None:
        return None
    row, owner, generation, connection_linked = result
    spec = _owned_spec(row, owner)
    if (
        row.sync_id != source.sync_id
        or row.desired_state != "active"
        or generation != row.generation
        or not connection_linked
        or source.short_name != spec.provider
        or source.readable_collection_id != spec.collection
        or source.readable_auth_provider_id is not None
        or source.auth_provider_config != spec.auth_config()
        or source_assurance(source.short_name, source.config_fields, source.auth_provider_config)
        != spec.account_assurance
    ):
        raise HTTPException(409, "Owned source binding mismatch")
    return spec


class ProvisioningStore:
    """Source domain creation participates in our transaction; it cannot commit early."""

    def __init__(
        self,
        create: SourceConnectionCreateServiceProtocol,
        validation: SourceValidationServiceProtocol,
        shared_composio: OwnedComposioSettings | None = None,
    ):
        """Reuse transactional source creation and source-specific validation."""
        self.create = create
        self.validation = validation
        self.shared_composio = shared_composio

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
            retain_reads = request.state == "paused"
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
                    source_assurance(
                        source.short_name, source.config_fields, source.auth_provider_config
                    ),
                    source.readable_collection_id,
                ) != (
                    request.source.provider,
                    request.source.account_assurance,
                    request.source.collection,
                ):
                    raise HTTPException(
                        status_code=409, detail="Reconnect changes original account identity"
                    )
                previous = EnsureSource.model_validate(row.request_payload)
                # Preserve only the already-readable scope. Credentials/cron may
                # change; provider identity, broker route and saved scope may not.
                retain_reads = (
                    row.desired_state == "active"
                    and row.observed_generation > 0
                    and source.is_authenticated
                    and previous.source is not None
                    and previous.source.account_assurance == request.source.account_assurance
                    and previous.source.config == request.source.config
                    and previous.source.project_key == request.source.project_key
                    and previous.source.auth_config_id == request.source.auth_config_id
                    and previous.source.user_id == request.source.user_id
                )
            # Publish the new intent inside this same transaction before owned creation.
            row.generation = request.generation
            row.request_payload = payload
            row.desired_state = request.state
            await db.flush()
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
                source = await db.get(SourceConnection, row.source_connection_id)
                jobs = await stop_source_writer(
                    db, sync, source, retain_read_authority=retain_reads
                )
                row.cancellation_job_ids = list(
                    dict.fromkeys([*row.cancellation_job_ids, *(str(job) for job in jobs)])
                )
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
        if self.shared_composio is None:
            raise HTTPException(503, "Owned Composio deployment configuration is unavailable")
        self.shared_composio.verify(spec)
        owner = await db.scalar(
            select(Organization.owned_owner_user_id).where(
                Organization.id == ctx.organization.id,
            )
        )
        if owner is None or spec.user_id != owner:
            raise HTTPException(409, "Owned source owner mismatch")
        config = self.validation.validate_config(spec.provider, spec.source_config(), ctx)
        if row.source_connection_id is None:
            created = await self.create.create_owned_deferred(
                db,
                ctx=ctx,
                uow=uow,
                intent_id=row.id,
                obj_in=SourceConnectionCreate(
                    name="Almanac " + spec.provider,
                    short_name=spec.provider,
                    readable_collection_id=spec.collection,
                    config=config,
                    schedule=ScheduleConfig(cron=spec.cron),
                    sync_immediately=False,
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
            source.readable_auth_provider_id = None
            source.auth_provider_config = spec.auth_config()
            sync = await db.get(Sync, row.sync_id)
            sync.cron_schedule = spec.cron
