"""Real SQL lifecycle and lost-reply recovery; no private provider or Temporal calls."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from airweave import schemas
from airweave.core.shared_models import AuthMethod, IntegrationType
from airweave.domains.collections.repository import CollectionRepository
from airweave.domains.connections.repository import ConnectionRepository
from airweave.domains.owned_provisioning.models import EnsureSource, ManagedSource
from airweave.domains.owned_provisioning.service import OwnedProvisioningService
from airweave.domains.owned_provisioning.store import ProvisioningStore
from airweave.domains.source_connections.repository import SourceConnectionRepository
from airweave.domains.source_connections.tests.test_create import _ctx, _entry, _service
from airweave.domains.syncs.jobs.repository import SyncJobRepository
from airweave.domains.syncs.repository import SyncRepository
from airweave.domains.syncs.service import SyncService
from airweave.models import Collection, Connection, Organization, SourceConnection, Sync, SyncJob
from airweave.models.owned_provisioning import OwnedProvisioning
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata

pytestmark = pytest.mark.integration


@pytest.fixture
async def setup(database):
    ctx = _ctx()
    ctx.auth_method = AuthMethod.API_KEY
    metadata_id = uuid4()
    async with database() as db:
        db.add(Organization(id=ctx.organization.id, name="Synthetic provisioning"))
        db.add(
            VectorDbDeploymentMetadata(
                id=metadata_id,
                dense_embedder="test",
                sparse_embedder="test",
                embedding_dimensions=3,
            )
        )
        await db.flush()
        db.add(
            Collection(
                name="Owned",
                readable_id="owned",
                organization_id=ctx.organization.id,
                vector_db_deployment_metadata_id=metadata_id,
            )
        )
        db.add(
            Connection(
                name="Composio",
                readable_id="composio",
                short_name="composio",
                organization_id=ctx.organization.id,
                integration_type=IntegrationType.AUTH_PROVIDER,
            )
        )
        await db.commit()

    lifecycle = SimpleNamespace(
        create=AsyncMock(
            return_value=SimpleNamespace(http_client=SimpleNamespace(aclose=AsyncMock()))
        )
    )
    schedules = SimpleNamespace(
        create_or_update_schedule=AsyncMock(), delete_all_schedules_for_sync=AsyncMock()
    )
    workflows = SimpleNamespace(
        run_source_connection_workflow=AsyncMock(),
        cancel_sync_job_workflow=AsyncMock(return_value={"success": True, "workflow_found": True}),
    )
    entry = _entry()
    entry.short_name = "gmail"
    creator = _service(entry)
    creator._sc_repo = SourceConnectionRepository(creator._source_registry)
    creator._collection_repo = CollectionRepository(creator._source_registry, creator._sc_repo)
    creator._connection_repo = ConnectionRepository()
    creator._source_validation.seed_config_result(
        "gmail", {"expected_mailbox": "owner@example.test"}
    )
    syncs = SyncRepository()
    jobs = SyncJobRepository()
    creator._sync_service = SyncService(
        syncs, jobs, Mock(), Mock(), Mock(), workflows, schedules, Mock()
    )
    creator._sync_service.resolve_destination_ids = AsyncMock(return_value=[])
    creator._response_builder.build_response = AsyncMock(
        side_effect=lambda db, source, ctx: SimpleNamespace(id=source.id, sync_id=source.sync_id)
    )
    service = OwnedProvisioningService(
        ProvisioningStore(
            creator, SimpleNamespace(validate_config=Mock(side_effect=lambda p, c, ctx: c))
        ),
        lifecycle,
        jobs,
        syncs,
        schedules,
        workflows,
    )
    request = EnsureSource(
        generation=1,
        state="active",
        source=ManagedSource(
            provider="gmail",
            expected_identity="owner@example.test",
            collection="owned",
            auth_provider="composio",
            connected_account_id="ca_first",
            auth_config_id="ac_test",
            user_id="owner",
            cron="0 * * * *",
        ),
    )
    return ctx, service, request, uuid4(), lifecycle, schedules, workflows


async def test_lost_reply_and_execution_failure_reuse_source_and_job(database, setup):
    ctx, service, request, account, lifecycle, schedules, workflows = setup
    schedules.create_or_update_schedule.side_effect = RuntimeError("Temporal unavailable")
    async with database() as db:
        with pytest.raises(RuntimeError, match="Temporal unavailable"):
            await service.ensure(db, ctx, account, request)
    async with database() as db:
        pending = await service.get(db, ctx, account)
        assert pending.state == "pending"
        row = await db.scalar(
            select(OwnedProvisioning).where(OwnedProvisioning.account_id == account)
        )
        first_job = row.initial_job_id
    schedules.create_or_update_schedule.side_effect = None
    async with database() as db:
        ready = await service.ensure(db, ctx, account, request)
    async with database() as db:
        replay = await service.ensure(db, ctx, account, request)
        assert ready == replay and ready.state == "ready"
        assert ready.sync_id == pending.sync_id
        assert (
            await db.scalar(
                select(func.count())
                .select_from(SourceConnection)
                .where(SourceConnection.organization_id == ctx.organization.id)
            )
            == 1
        )
        assert (
            await db.scalar(
                select(func.count()).select_from(SyncJob).where(SyncJob.sync_id == ready.sync_id)
            )
            == 1
        )
        assert (await db.get(SyncJob, first_job)).provisioning_generation == 1
        with pytest.raises(HTTPException) as conflict:
            await service.ensure(
                db,
                ctx,
                account,
                request.model_copy(
                    update={
                        "source": request.source.model_copy(
                            update={"connected_account_id": "ca_other"}
                        )
                    }
                ),
            )
        assert conflict.value.status_code == 409
    lifecycle.create.assert_awaited_once()
    workflows.run_source_connection_workflow.assert_awaited_once()
    # Reconciliation always repairs orphan deterministic schedules, even with no DB link.
    assert schedules.delete_all_schedules_for_sync.await_count == 2


async def test_disconnect_wins_over_inflight_verification_and_blocks_admission(database, setup):
    ctx, service, request, account, lifecycle, schedules, workflows = setup
    started, release = asyncio.Event(), asyncio.Event()
    resource = SimpleNamespace(http_client=SimpleNamespace(aclose=AsyncMock()))

    async def verify(*args):
        started.set()
        await release.wait()
        return resource

    lifecycle.create.side_effect = verify

    async def connect():
        async with database() as db:
            return await service.ensure(db, ctx, account, request)

    task = asyncio.create_task(connect())
    await started.wait()
    async with database() as db:
        pending = await service.get(db, ctx, account)
        with pytest.raises(HTTPException) as blocked:
            await service.jobs.create(db, schemas.SyncJobCreate(sync_id=pending.sync_id), ctx)
        assert blocked.value.status_code == 409
    async with database() as db:
        disconnected = await service.ensure(
            db, ctx, account, EnsureSource(generation=2, state="disconnected")
        )
        assert disconnected.state == "disconnected"
    release.set()
    with pytest.raises(HTTPException) as stale:
        await task
    assert stale.value.status_code == 409
    resource.http_client.aclose.assert_awaited_once()
    workflows.run_source_connection_workflow.assert_not_awaited()
    async with database() as db:
        with pytest.raises(HTTPException):
            await service.ensure(db, ctx, account, request)
        assert (await db.get(Sync, pending.sync_id)).provisioning_ready_generation == 0


async def test_rotation_timeout_retains_cleanup_and_original_source(database, setup, monkeypatch):
    ctx, service, request, account, lifecycle, schedules, workflows = setup
    async with database() as db:
        first = await service.ensure(db, ctx, account, request)
        original_job = await db.scalar(select(SyncJob.id).where(SyncJob.sync_id == first.sync_id))
    rotated = request.model_copy(
        update={
            "generation": 2,
            "source": request.source.model_copy(update={"connected_account_id": "ca_new"}),
        }
    )

    async def stalled(*args):
        await asyncio.Event().wait()

    workflows.cancel_sync_job_workflow.side_effect = stalled
    monkeypatch.setattr(
        "airweave.domains.owned_provisioning.service.EXECUTION_TIMEOUT_SECONDS", 0.02
    )
    async with database() as db:
        with pytest.raises(HTTPException) as timeout:
            await service.ensure(db, ctx, account, rotated)
        assert timeout.value.status_code == 503
    async with database() as db:
        row = await db.scalar(
            select(OwnedProvisioning).where(OwnedProvisioning.account_id == account)
        )
        assert row.cancellation_job_ids == [str(original_job)]
        assert row.generation == 2 and row.observed_generation == 1
        assert (
            row.sync_id == first.sync_id and row.source_connection_id == first.source_connection_id
        )
    workflows.cancel_sync_job_workflow.side_effect = None
    monkeypatch.setattr("airweave.domains.owned_provisioning.service.EXECUTION_TIMEOUT_SECONDS", 20)
    async with database() as db:
        second = await service.ensure(db, ctx, account, rotated)
        assert second.state == "ready" and second.sync_id == first.sync_id
        assert (
            await db.scalar(
                select(func.count()).select_from(SyncJob).where(SyncJob.sync_id == first.sync_id)
            )
            == 2
        )
        other = _ctx()
        with pytest.raises(HTTPException) as denied:
            await service.get(db, other, account)
        assert denied.value.status_code == 404


async def test_http_boundary_and_legacy_edit_cannot_bypass_generation(database, setup):
    import httpx
    from fastapi import FastAPI

    from airweave.api import deps
    from airweave.api.v1.endpoints.owned_provisioning import router
    from airweave.db.session import get_db
    from airweave.domains.source_connections.tests.test_delete import (
        _build_service as delete_service,
    )
    from airweave.domains.source_connections.tests.test_update import (
        _build_service as update_service,
    )
    from airweave.schemas.source_connection import SourceConnectionUpdate

    ctx, service, request, account, lifecycle, schedules, workflows = setup
    app = FastAPI()
    app.include_router(router, prefix="/owned-sources")

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_context] = lambda: ctx
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(
        owned_provisioning=service
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        ctx.auth_method = AuthMethod.SYSTEM
        denied = await http.put(f"/owned-sources/{account}", json=request.model_dump(mode="json"))
        assert denied.status_code == 403
        lifecycle.create.assert_not_awaited()
        ctx.auth_method = AuthMethod.API_KEY
        connected = await http.put(
            f"/owned-sources/{account}", json=request.model_dump(mode="json")
        )
        assert connected.status_code == 200, connected.text
        source_id = connected.json()["source_connection_id"]
    from uuid import UUID

    source_id = UUID(source_id)
    repo = SourceConnectionRepository(service.store.create._source_registry)
    async with database() as db:
        with pytest.raises(HTTPException) as guarded:
            await update_service(sc_repo=repo).update(
                db,
                id=source_id,
                obj_in=SourceConnectionUpdate(
                    config={"expected_mailbox": "someone-else@example.test"}
                ),
                ctx=ctx,
            )
        assert guarded.value.status_code == 409
    async with database() as db:
        with pytest.raises(HTTPException) as guarded:
            await delete_service(sc_repo=repo).delete(db, id=source_id, ctx=ctx)
        assert guarded.value.status_code == 409


async def test_creation_failure_rolls_back_relationship_source_and_sync(database, setup):
    ctx, service, request, account, lifecycle, schedules, workflows = setup
    response = service.store.create._response_builder.build_response
    original = response.side_effect
    response.side_effect = RuntimeError("creation interrupted before commit")
    async with database() as db:
        with pytest.raises(RuntimeError, match="creation interrupted"):
            await service.ensure(db, ctx, account, request)
    async with database() as db:
        for model in (OwnedProvisioning, SourceConnection, Sync, SyncJob):
            assert (
                await db.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.organization_id == ctx.organization.id)
                )
                == 0
            )
    lifecycle.create.assert_not_awaited()
    schedules.create_or_update_schedule.assert_not_awaited()
    response.side_effect = original
    async with database() as db:
        ready = await service.ensure(db, ctx, account, request)
        assert ready.state == "ready"


async def test_pause_resume_preserves_identity_source_and_cleanup(database, setup):
    ctx, service, request, account, lifecycle, schedules, workflows = setup
    async with database() as db:
        first = await service.ensure(db, ctx, account, request)
    async with database() as db:
        paused = await service.ensure(db, ctx, account, EnsureSource(generation=2, state="paused"))
        assert paused.state == "paused"
        assert paused.expected_identity == request.source.expected_identity
        assert (paused.source_connection_id, paused.sync_id) == (
            first.source_connection_id,
            first.sync_id,
        )
        sync = await db.get(Sync, first.sync_id)
        assert sync.status == "paused" and sync.provisioning_generation == 2
        with pytest.raises(HTTPException):
            await service.jobs.create(db, schemas.SyncJobCreate(sync_id=first.sync_id), ctx)
    workflows.cancel_sync_job_workflow.assert_awaited_once()
    async with database() as db:
        wrong_source = request.source.model_copy(update={"expected_identity": "other@example.test"})
        wrong = request.model_copy(update={"generation": 3, "source": wrong_source})
        with pytest.raises(HTTPException, match="Reconnect changes original account identity"):
            await service.ensure(db, ctx, account, wrong)
    async with database() as db:
        resumed = await service.ensure(
            db, ctx, account, request.model_copy(update={"generation": 3})
        )
        assert resumed.state == "ready"
        assert (resumed.source_connection_id, resumed.sync_id) == (
            first.source_connection_id,
            first.sync_id,
        )
        assert await db.scalar(select(func.count()).select_from(SourceConnection)) == 1
    assert lifecycle.create.await_count == 2
    async with database() as db:
        stopped = await service.ensure(
            db, ctx, account, EnsureSource(generation=4, state="disconnected")
        )
        assert stopped.expected_identity == request.source.expected_identity
    async with database() as db:
        with pytest.raises(HTTPException, match="Connect a new account after disconnect"):
            await service.ensure(db, ctx, account, request.model_copy(update={"generation": 5}))


async def test_initial_pause_can_activate_but_initial_disconnect_is_terminal(database, setup):
    ctx, service, request, account, lifecycle, schedules, workflows = setup
    async with database() as db:
        paused = await service.ensure(db, ctx, account, EnsureSource(generation=1, state="paused"))
        assert paused.state == "paused" and paused.expected_identity is None
        assert paused.source_connection_id is None and paused.sync_id is None
    async with database() as db:
        active = await service.ensure(
            db, ctx, account, request.model_copy(update={"generation": 2})
        )
        assert active.state == "ready"
    other = uuid4()
    async with database() as db:
        await service.ensure(db, ctx, other, EnsureSource(generation=1, state="disconnected"))
    async with database() as db:
        with pytest.raises(HTTPException, match="Connect a new account after disconnect"):
            await service.ensure(db, ctx, other, request.model_copy(update={"generation": 2}))
