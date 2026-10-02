"""Real API-key authentication plus issued-key canonical reads on disposable SQL."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import Depends, FastAPI, Request
from sqlalchemy import func, select

from airweave.adapters.cache.fake import FakeContextCache
from airweave.adapters.rate_limiter.fake import FakeRateLimiter
from airweave.api import deps
from airweave.api.context_resolver import ContextResolver
from airweave.api.v1.endpoints import admin, api_keys, owned_tenants, records
from airweave.core import credentials
from airweave.core.config import AuthMode, settings
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import observation
from airweave.domains.organizations.repository import ApiKeyRepository, OrganizationRepository
from airweave.domains.owned_tenants.service import OwnedTenantService, utc_now
from airweave.domains.owned_tenants.store import OwnedTenantStore
from airweave.domains.users.repository import UserRepository
from airweave.models import APIKey, Entity, Organization, SourceConnection, Sync, SyncJob
from airweave.models.feature_flag import FeatureFlag
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata

pytestmark = pytest.mark.integration


@pytest.fixture
async def api(database, monkeypatch):
    now = utc_now()
    control_org, other_org, allowed_id, wrong_id, other_id = [uuid4() for _ in range(5)]
    async with database() as db:
        db.add(Organization(id=control_org, name="Synthetic control"))
        db.add(Organization(id=other_org, name="Synthetic other control"))
        db.add(
            VectorDbDeploymentMetadata(
                dense_embedder="test", sparse_embedder="test", embedding_dimensions=3
            )
        )
        await db.flush()
        db.add(FeatureFlag(organization_id=control_org, flag="api_key_admin_sync", enabled=True))
        for org, key_id, key in (
            (control_org, allowed_id, "test-control"),
            (control_org, wrong_id, "test-wrong-control"),
            (other_org, other_id, "test-other-org"),
        ):
            db.add(
                APIKey(
                    id=key_id,
                    organization_id=org,
                    encrypted_key=credentials.encrypt({"key": key}),
                    expiration_date=now + timedelta(days=1),
                )
            )
        await db.commit()
    monkeypatch.setattr(settings, "AUTH_MODE", AuthMode.API_KEY)
    monkeypatch.setattr(settings, "OWNED_TENANT_CONTROL_ORGANIZATION_ID", control_org)
    # Wrong-org key is deliberately allowlisted to prove the independent org fence.
    monkeypatch.setattr(settings, "OWNED_TENANT_CONTROL_API_KEY_IDS", (allowed_id, other_id))
    resolver = ContextResolver(
        cache=FakeContextCache(),
        rate_limiter=FakeRateLimiter(),
        user_repo=UserRepository(),
        api_key_repo=ApiKeyRepository(),
        org_repo=OrganizationRepository(),
    )

    async def session():
        async with database() as db:
            yield db

    async def authenticated(request: Request, db=Depends(deps.get_db)):
        return await resolver.resolve(
            request,
            db,
            None,
            request.headers.get("X-API-Key"),
            request.headers.get("X-Organization-ID"),
        )

    app = FastAPI()
    app.include_router(owned_tenants.router, prefix="/owned-tenants")
    app.include_router(owned_tenants.router, prefix="/backend/v1/owned-tenants")
    app.include_router(records.router, prefix="/sync")
    app.include_router(api_keys.router, prefix="/api-keys")
    app.include_router(admin.router, prefix="/admin")
    app.add_exception_handler(CanonicalStoreError, records.record_error_response)
    app.dependency_overrides[deps.get_db] = session
    app.dependency_overrides[deps.get_context] = authenticated
    app.dependency_overrides[deps.get_owned_search_context] = authenticated
    app.dependency_overrides[deps.get_search_session_factory] = lambda: database
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-cursor-key"
    )
    app.dependency_overrides[owned_tenants.enrollment_service] = lambda: OwnedTenantService(
        OwnedTenantStore(), clock=lambda: now
    )
    container = SimpleNamespace(storage_backend=AsyncMock(), owned_search=AsyncMock())
    app.dependency_overrides[deps.get_container] = lambda: container
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client, container


async def test_enrollment_requires_both_configured_control_org_and_key(database, api, monkeypatch):
    client, _ = api
    for key in ("test-wrong-control", "test-other-org", "counterfeit"):
        response = await client.post(
            "/owned-tenants/ensure", headers={"X-API-Key": key}, json={"owner_user_id": "user_a"}
        )
        assert response.status_code == 403
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Organization)) == 2
    monkeypatch.setattr(settings, "OWNED_TENANT_CONTROL_API_KEY_IDS", ())
    response = await client.post(
        "/owned-tenants/ensure",
        headers={"X-API-Key": "test-control"},
        json={"owner_user_id": "user_a"},
    )
    assert response.status_code == 503


async def test_existing_only_does_not_create_missing_deleted_owner(database, api):
    client, _ = api
    response = await client.post(
        "/owned-tenants/ensure",
        headers={"X-API-Key": "test-control"},
        json={"owner_user_id": "user_never_enrolled", "existing_only": True},
    )
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "not_enrolled"
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Organization)) == 2
    created = await client.post(
        "/owned-tenants/ensure",
        headers={"X-API-Key": "test-control"},
        json={"owner_user_id": "user_a"},
    )
    existing = await client.post(
        "/owned-tenants/ensure",
        headers={"X-API-Key": "test-control"},
        json={"owner_user_id": "user_a", "existing_only": True},
    )
    assert existing.json() == created.json()


async def seed_original(database, tenant):
    from uuid import UUID

    org, sync_id, job_id = UUID(tenant["organization_id"]), uuid4(), uuid4()
    async with database() as db:
        db.add(Sync(id=sync_id, organization_id=org, name="Synthetic owned source"))
        await db.flush()
        db.add(SyncJob(id=job_id, organization_id=org, sync_id=sync_id, status="running"))
        db.add(
            SourceConnection(
                organization_id=org,
                name="Synthetic source",
                short_name="gmail",
                sync_id=sync_id,
                readable_collection_id=tenant["collection"],
                is_authenticated=True,
            )
        )
        await db.commit()
    capture = CanonicalCaptureService(CanonicalRecordStore())
    async with database() as db:
        fence = await capture.activate_writer(
            db, org, sync_id, job_id, attempt_id=uuid4(), attempt_number=1
        )
    async with database() as db:
        await capture.capture(
            db, CaptureBatch(fence=fence, records=(observation("synthetic-original"),))
        )
    async with database() as db:
        record_id = await db.scalar(select(Entity.id).where(Entity.sync_id == sync_id))
    return sync_id, record_id


async def test_returned_plaintext_key_reads_only_its_owner_and_cannot_enroll(database, api):
    client, container = api
    tenants = []
    for owner in ("user_a", "user_b"):
        response = await client.post(
            "/owned-tenants/ensure",
            headers={"X-API-Key": "test-control"},
            json={"owner_user_id": owner},
        )
        assert response.status_code == 200, response.text
        assert response.headers["Cache-Control"] == "no-store"
        assert len(response.json()["api_key"]) >= 32
        tenants.append(response.json())
    a, b = tenants
    sync_id, record_id = await seed_original(database, a)
    read_path = f"/sync/{sync_id}/records/{record_id}"
    own = await client.get(read_path, headers={"X-API-Key": a["api_key"]})
    assert own.status_code == 200, own.text
    assert own.json()["identity"]["native_id"] == "synthetic-original"
    for key in (b["api_key"], "test-control"):
        denied = await client.get(read_path, headers={"X-API-Key": key})
        assert denied.status_code == (403 if key == "test-control" else 404), denied.text
        assert "synthetic-original" not in denied.text
        # An org header never upgrades a scoped key to cross-tenant authority.
        headers = {"X-API-Key": key, "X-Organization-ID": a["organization_id"]}
        for path, method, body in (
            (read_path, "GET", None),
            (read_path + "/blobs/" + "a" * 64 + "?revision=1", "GET", None),
            ("/sync/search/candidates", "POST", {"query": "test", "sync_ids": [str(sync_id)]}),
        ):
            denied = await client.request(method, path, headers=headers, json=body)
            assert denied.status_code == 403, denied.text
    denied = await client.post(
        "/owned-tenants/ensure",
        headers={"X-API-Key": a["api_key"]},
        json={"owner_user_id": "user_c"},
    )
    assert denied.status_code == 403
    denied = await client.get(read_path, headers={"X-API-Key": "counterfeit"})
    assert denied.status_code == 403
    container.owned_search.candidates.assert_not_awaited()
    container.storage_backend.read.assert_not_awaited()


async def test_control_key_is_enrollment_only_even_with_admin_feature(database, api, monkeypatch):
    client, _ = api
    headers = {"X-API-Key": "test-control"}
    enrolled = await client.post(
        "/backend/v1/owned-tenants/ensure/",
        headers=headers,
        json={"owner_user_id": "user_mounted"},
    )
    assert enrolled.status_code == 200
    async with database() as db:
        assert await db.scalar(select(FeatureFlag.enabled)) is True
    for method, path, body in (
        ("GET", "/api-keys/" + enrolled.json()["api_key_id"], None),
        ("GET", "/admin/feature-flags", None),
        (
            "POST",
            "/admin/collections/" + enrolled.json()["collection"] + "/search",
            {"query": "test"},
        ),
        ("POST", "/sync/search/candidates", {"query": "test", "sync_ids": [str(uuid4())]}),
    ):
        response = await client.request(method, path, headers=headers, json=body)
        assert response.status_code == 403, response.text
        assert "restricted to enrollment" in response.text
    monkeypatch.setattr(
        settings,
        "OWNED_TENANT_CONTROL_API_KEY_IDS",
        settings.OWNED_TENANT_CONTROL_API_KEY_IDS[1:],
    )
    removed = await client.post(
        "/owned-tenants/ensure", headers=headers, json={"owner_user_id": "user_mounted"}
    )
    assert removed.status_code == 403
    removed = await client.get("/admin/feature-flags", headers=headers)
    assert removed.status_code == 403
    removed = await client.post(
        "/sync/search/candidates",
        headers=headers,
        json={"query": "test", "sync_ids": [str(uuid4())]},
    )
    assert removed.status_code == 403
