"""Real tenant binding checks before shared Composio credentials are selected."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

from airweave.core.exceptions import NotFoundException
from airweave.core.shared_models import AuthMethod, IntegrationType
from airweave.domains.connections.repository import ConnectionRepository
from airweave.domains.owned_provisioning.models import ManagedSource
from airweave.domains.owned_provisioning.settings import OwnedComposioSettings
from airweave.domains.owned_provisioning.store import owned_creation_spec
from airweave.domains.owned_provisioning.tests.test_provisioning import setup as provisioning_setup
from airweave.domains.source_connections.tests.test_create import _ctx
from airweave.domains.sources.tests.test_lifecycle import _make_service
from airweave.models import Collection, Connection, Organization, SourceConnection
from airweave.models.integration_credential import IntegrationCredential
from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.sync import Sync
from airweave.platform.configs.config import GmailConfig
from airweave.schemas.source_connection import AuthProviderAuthentication, SourceConnectionCreate


@pytest.fixture
async def setup(database):
    """Reuse the real scoped provisioning composition without shadowed fixture imports."""
    return await provisioning_setup.__wrapped__(database)


async def test_two_tenants_share_project_without_tenant_credentials(database, setup):
    ctx, service, request, account, _, _, _ = setup
    creator = service.store.create
    service.store.validation = creator._source_validation
    creator._source_registry.get.return_value.config_ref = GmailConfig
    config = GmailConfig.model_validate(request.source.source_config()).model_dump(mode="json")
    creator._source_validation.seed_config_result("gmail", config)
    async with database() as db:
        first = await service.ensure(db, ctx, account, request)
        first_source = await db.get(SourceConnection, first.source_connection_id)
        assert await creator._sc_repo.get_owned_source(db, first_source, ctx) == request.source
        metadata = (await db.scalar(select(Collection))).vector_db_deployment_metadata_id
    second_ctx = _ctx()
    second_ctx.auth_method = AuthMethod.API_KEY
    second_spec = request.source.model_copy(
        update={
            "collection": "second-owned",
            "user_id": "second-owner",
            "expected_identity": "second@example.test",
            "connected_account_id": "ca_second",
        }
    )
    async with database() as db:
        db.add(
            Organization(
                id=second_ctx.organization.id,
                name="Second tenant",
                owned_owner_user_id="second-owner",
            )
        )
        await db.flush()
        db.add(
            Collection(
                name="Second",
                readable_id="second-owned",
                organization_id=second_ctx.organization.id,
                vector_db_deployment_metadata_id=metadata,
            )
        )
        await db.commit()
    creator._source_validation.seed_config_result(
        "gmail", GmailConfig.model_validate(second_spec.source_config()).model_dump(mode="json")
    )
    async with database() as db:
        second = await service.ensure(
            db, second_ctx, uuid4(), request.model_copy(update={"source": second_spec})
        )
        second_source = await db.get(SourceConnection, second.source_connection_id)
        assert await creator._sc_repo.get_owned_source(db, second_source, second_ctx) == second_spec
        assert await db.scalar(select(func.count()).select_from(IntegrationCredential)) == 0
        assert await db.scalar(select(func.count()).select_from(Connection)) == 2
        assert (
            first_source.readable_auth_provider_id
            is second_source.readable_auth_provider_id
            is None
        )
        assert (
            first_source.auth_provider_config["project_key"]
            == second_source.auth_provider_config["project_key"]
            == "project"
        )
        with pytest.raises(NotFoundException):
            await creator._sc_repo.get(db, second_source.id, ctx)
        with pytest.raises(HTTPException, match="organization mismatch"):
            await creator._sc_repo.get_owned_source(db, second_source, ctx)
        row = await db.scalar(
            select(OwnedProvisioning).where(
                OwnedProvisioning.source_connection_id == second_source.id
            )
        )
        with pytest.raises(HTTPException, match="tenant intent"):
            await owned_creation_spec(db, ctx, row.id)
        first_source = await db.get(SourceConnection, first.source_connection_id)
        # Transplant an existing foreign tenant source connection into the first source.
        first_source.connection_id = second_source.connection_id
        await db.flush()
        with pytest.raises(HTTPException, match="binding mismatch"):
            await creator._sc_repo.get_owned_source(db, first_source, ctx)


@pytest.mark.parametrize(
    "damage",
    ["project", "user", "auth_config", "scope", "extra", "generation", "legacy", "sync"],
)
async def test_corrupt_binding_rejects_before_shared_key_or_provider(database, setup, damage):
    ctx, service, request, account, _, _, _ = setup
    creator = service.store.create
    service.store.validation = creator._source_validation
    creator._source_registry.get.return_value.config_ref = GmailConfig
    creator._source_validation.seed_config_result(
        "gmail", GmailConfig.model_validate(request.source.source_config()).model_dump(mode="json")
    )
    async with database() as db:
        first = await service.ensure(db, ctx, account, request)
        source = await db.get(SourceConnection, first.source_connection_id)
        row = await db.scalar(
            select(OwnedProvisioning).where(OwnedProvisioning.source_connection_id == source.id)
        )
        assert await creator._sc_repo.get_owned_source(db, source, ctx) == request.source
        if damage in {"project", "user", "auth_config"}:
            key = {"project": "project_key", "user": "user_id", "auth_config": "auth_config_id"}[
                damage
            ]
            source.auth_provider_config = {**source.auth_provider_config, key: "other"}
        elif damage in {"scope", "extra"}:
            source.config_fields = {
                **source.config_fields,
                "included_labels" if damage == "scope" else "unexpected_scope": ["INBOX"],
            }
        elif damage == "generation":
            (await db.get(Sync, source.sync_id)).provisioning_generation += 1
        elif damage == "legacy":
            db.add(
                Connection(
                    name="Legacy selector",
                    readable_id="composio",
                    short_name="composio",
                    organization_id=ctx.organization.id,
                    integration_type=IntegrationType.AUTH_PROVIDER,
                )
            )
            await db.flush()
            source.readable_auth_provider_id = "composio"
        else:
            source.sync_id = None
        await db.flush()
        lifecycle = _make_service(sc_repo=creator._sc_repo, conn_repo=ConnectionRepository())
        lifecycle._source_registry = creator._source_registry
        lifecycle._shared_composio = service.store.shared_composio
        provider = AsyncMock()
        lifecycle._auth_provider_registry = provider
        expected = (
            "scope or configuration mismatch"
            if damage in {"scope", "extra"}
            else "binding mismatch"
        )
        with pytest.raises(HTTPException, match=expected):
            await lifecycle._load_source_connection_data(db, source.id, ctx)
        assert not provider.mock_calls
        assert row.source_connection_id == source.id


@pytest.mark.parametrize("change", ["missing", "project", "auth_config"])
async def test_shared_config_failure_has_no_generic_credential_fallback(database, setup, change):
    ctx, service, request, account, _, _, _ = setup
    creator = service.store.create
    service.store.validation = creator._source_validation
    creator._source_registry.get.return_value.config_ref = GmailConfig
    creator._source_validation.seed_config_result(
        "gmail", GmailConfig.model_validate(request.source.source_config()).model_dump(mode="json")
    )
    async with database() as db:
        first = await service.ensure(db, ctx, account, request)
        lifecycle = _make_service(sc_repo=creator._sc_repo, conn_repo=ConnectionRepository())
        lifecycle._source_registry = creator._source_registry
        shared = service.store.shared_composio
        lifecycle._shared_composio = shared
        healthy = await lifecycle._load_source_connection_data(db, first.source_connection_id, ctx)
        assert healthy.owned_source == request.source
        lifecycle._shared_composio = (
            None
            if change == "missing"
            else shared.model_copy(
                update={
                    "project_key" if change == "project" else "auth_config_ids": "other"
                    if change == "project"
                    else {"gmail": "other"}
                }
            )
        )
        with pytest.raises(
            Exception, match="configuration is unavailable|project or auth config mismatch"
        ):
            await lifecycle._load_source_connection_data(db, first.source_connection_id, ctx)
        with pytest.raises(Exception, match="not found"):
            await creator.create(
                db,
                ctx=ctx,
                obj_in=SourceConnectionCreate(
                    short_name="gmail",
                    readable_collection_id="owned",
                    sync_immediately=False,
                    authentication=AuthProviderAuthentication(
                        provider_readable_id="project", provider_config=request.source.auth_config()
                    ),
                ),
            )
        assert await db.scalar(select(func.count()).select_from(IntegrationCredential)) == 0


@pytest.mark.parametrize("provider", ["gmail", "google_calendar", "google_drive", "slack", "wispr"])
async def test_owner_mismatch_denied_for_every_launch_provider(database, setup, provider):
    ctx, service, request, account, lifecycle, _, _ = setup
    # Owner admission runs before provider-specific source configuration or side effects.
    bad = request.source.model_copy(
        update={
            "provider": provider,
            "user_id": "another-owner",
            "auth_config_id": service.store.shared_composio.auth_config_ids[provider],
        }
    )
    async with database() as db:
        with pytest.raises(HTTPException, match="owner mismatch"):
            await service.store.ensure(db, ctx, account, request.model_copy(update={"source": bad}))
    lifecycle.create.assert_not_awaited()


def test_shared_config_requires_key_and_legacy_request_is_rejected():
    with pytest.raises(ValidationError):
        OwnedComposioSettings(project_key="project", api_key=" ", auth_config_ids={"gmail": "auth"})
    with pytest.raises(ValidationError):
        ManagedSource(
            provider="gmail",
            expected_identity="owner@example.test",
            collection="owned",
            auth_provider="composio",
            connected_account_id="account",
            auth_config_id="auth",
            user_id="owner",
            cron="0 * * * *",
        )


@pytest.mark.parametrize("field", ["node_selection", "node_selections"])
def test_owned_request_rejects_legacy_node_selection(field):
    with pytest.raises(ValidationError, match="persisted scope"):
        ManagedSource(
            provider="gmail",
            project_key="project",
            expected_identity="owner@example.test",
            collection="owned",
            connected_account_id="account",
            auth_config_id="auth",
            user_id="owner",
            cron="0 * * * *",
            config={field: []},
        )


async def test_database_refuses_owner_rebinding(database, setup):
    ctx, _, _, _, _, _, _ = setup
    async with database() as db:
        owner = await db.get(Organization, ctx.organization.id)
        owner.owned_owner_user_id = "different"
        with pytest.raises(DBAPIError, match="owner binding is immutable"):
            await db.flush()
