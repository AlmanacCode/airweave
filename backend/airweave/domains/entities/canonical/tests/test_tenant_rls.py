"""Real retained schema RLS under separate actual tenant/control login roles."""

import os
import secrets
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.db.session import _require_runtime_role
from airweave.db.tenant_session import tenant_session_factory
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.tests.conftest import migrate
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation
from airweave.models import Entity, Organization, Sync, SyncJob
from airweave.models.entity_change import EntityChange
from airweave.models.projection_generation import ProjectionGeneration


@pytest.fixture
async def tenant_runtime(database):
    """Apply actual policy migration only inside this test's new canonical schema."""
    base = database.kw["bind"]
    async with base.begin() as db:
        schema = await db.scalar(text("SELECT current_schema()"))
        await db.run_sync(migrate, "0017_owned_tenant_rls.py")
        await db.run_sync(migrate, "0018_owned_worker_discovery.py")
        await db.run_sync(migrate, "0020_owned_source_limits.py")
    roles = []
    engines = []
    for group in ("airweave_tenant", "airweave_control"):
        role, password = "owned_rls_" + uuid4().hex, secrets.token_hex(24)
        async with base.begin() as db:
            await db.execute(
                text(
                    f'CREATE ROLE "{role}" LOGIN INHERIT NOSUPERUSER NOBYPASSRLS '
                    f"NOCREATEDB NOCREATEROLE PASSWORD '{password}'"
                )
            )
            await db.execute(text(f'GRANT "{group}" TO "{role}"'))
        roles.append(role)
        engines.append(
            create_async_engine(
                make_url(os.environ["CANONICAL_TEST_DATABASE_URL"]).set(
                    username=role, password=password
                ),
                pool_size=1,
                max_overflow=0,
                connect_args={"server_settings": {"search_path": schema}},
            )
        )
        _require_runtime_role(engines[-1], group)
    try:
        yield SimpleNamespace(tenant=engines[0], control=engines[1], schema=schema)
    finally:
        for engine in engines:
            await engine.dispose()
        async with base.begin() as db:
            for role in roles:
                await db.execute(text(f'DROP ROLE "{role}"'))


async def test_actual_canonical_capture_missing_predicates_and_reference_fences(
    database, source, tenant_runtime
):
    """RLS protects originals/journal/manifests even when SQL omits tenant predicates."""
    capture, first = source
    await bind_projection(database, first)
    b, sync_b, job_b = uuid4(), uuid4(), uuid4()
    async with database() as db:
        db.add(Organization(id=b, name="Other synthetic tenant"))
        await db.flush()
        db.add(Sync(id=sync_b, organization_id=b, name="Other source"))
        await db.flush()
        db.add(SyncJob(id=job_b, organization_id=b, sync_id=sync_b, status="running"))
        await db.commit()
        second = await capture.activate_writer(
            db, b, sync_b, job_b, attempt_id=uuid4(), attempt_number=1
        )
    await bind_projection(database, second)
    for fence in (first, second):
        async with tenant_session_factory(tenant_runtime.tenant, fence.organization_id)() as db:
            await capture.capture(
                db, CaptureBatch(fence=fence, records=(observation("synthetic"),))
            )
    async with tenant_session_factory(tenant_runtime.tenant, first.organization_id)() as db:
        assert {row.organization_id for row in (await db.scalars(select(Entity))).all()} == {
            first.organization_id
        }
        assert {row.organization_id for row in (await db.scalars(select(EntityChange))).all()} == {
            first.organization_id
        }
        assert {row.organization_id for row in (await db.scalars(select(SyncJob))).all()} == {
            first.organization_id
        }
        assert await db.get(Sync, sync_b) is None
        assert await db.get(Organization, b) is None
        with pytest.raises(DBAPIError, match="foreign key"):
            db.add(SyncJob(organization_id=first.organization_id, sync_id=sync_b, status="running"))
            await db.commit()
        await db.rollback()
        with pytest.raises(DBAPIError, match="row-level security"):
            await db.execute(
                ProjectionGeneration.__table__.insert().values(
                    id=uuid4(),
                    organization_id=b,
                    sync_id=sync_b,
                    collection_id=uuid4(),
                    record_id=uuid4(),
                    revision=1,
                    pipeline_version=1,
                    next_gc_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
                    created_at=datetime(2026, 10, 2),
                    modified_at=datetime(2026, 10, 2),
                )
            )
        await db.rollback()
    async with async_sessionmaker(tenant_runtime.tenant)() as db:
        assert not (await db.scalars(select(Entity))).all()
        assert not (await db.scalars(select(EntityChange))).all()
        assert not (await db.scalars(select(SyncJob))).all()
    async with async_sessionmaker(tenant_runtime.control)() as db:
        assert len((await db.scalars(select(Organization))).all()) == 2
        from airweave.domains.entities.canonical.projection_gc import ProjectionGCStore
        from airweave.domains.entities.canonical.projection_store import CanonicalProjectionStore

        page = await CanonicalProjectionStore().pending_sources(db, ("gmail",))
        assert {item.organization_id for item in page.sources} == {first.organization_id, b}
        assert not await ProjectionGCStore().due(db, now=datetime.now(timezone.utc))
        assert not (
            await db.execute(
                text("SELECT * FROM owned_stale_jobs(:pending,:running,NULL,100)"),
                {"pending": datetime(2020, 1, 1), "running": datetime(2020, 1, 1)},
            )
        ).all()
        await db.rollback()
        with pytest.raises(DBAPIError, match="permission denied"):
            await db.scalars(select(Entity))
        await db.rollback()
        with pytest.raises(DBAPIError, match="permission denied"):
            await db.execute(text("SET LOCAL ROLE airweave_discovery"))


async def test_migration_refuses_preexisting_public_policy(database):
    """A permissive policy must never OR-bypass the owned tenant fence."""
    async with database.kw["bind"].begin() as db:
        await db.execute(text("ALTER TABLE entity ENABLE ROW LEVEL SECURITY"))
        await db.execute(text("CREATE POLICY legacy_public ON entity TO PUBLIC USING (true)"))
        with pytest.raises(RuntimeError, match="preexisting policies"):
            await db.run_sync(migrate, "0017_owned_tenant_rls.py")


async def test_runtime_pool_refuses_migration_owner(database):
    """Configuration cannot silently opt the migration owner into tenant operations."""
    engine = create_async_engine(database.kw["bind"].url, pool_size=1, max_overflow=0)
    _require_runtime_role(engine, "airweave_tenant")
    try:
        with pytest.raises(RuntimeError, match="forbidden authority"):
            async with engine.connect():
                pytest.fail("Migration-owner connection must be rejected before checkout")
    finally:
        await engine.dispose()


async def test_real_http_auth_scope_concurrent_pool_reuse_and_revocation(
    database, source, tenant_runtime, monkeypatch
):
    """Persisted keys choose fresh scoped sessions; source revocation remains immediate."""
    import asyncio
    from datetime import timedelta

    from cryptography.fernet import Fernet
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient
    from sqlalchemy import delete, update

    from airweave.adapters.cache.fake import FakeContextCache
    from airweave.adapters.rate_limiter.fake import FakeRateLimiter
    from airweave.api import deps
    from airweave.api.v1.endpoints import records
    from airweave.core import container as container_mod
    from airweave.core import credentials
    from airweave.core.config import AuthMode, settings
    from airweave.domains.entities.canonical.query import CanonicalQueryService
    from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
    from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
    from airweave.models import APIKey, SourceConnection

    monkeypatch.setattr(settings, "AUTH_MODE", AuthMode.API_KEY)
    monkeypatch.setattr(settings, "OWNED_TENANT_CONTROL_ORGANIZATION_ID", uuid4())
    monkeypatch.setattr(settings, "OWNED_TENANT_CONTROL_API_KEY_IDS", (uuid4(),))
    monkeypatch.setattr(settings, "ENCRYPTION_KEY", Fernet.generate_key().decode())
    monkeypatch.setattr(
        container_mod,
        "container",
        SimpleNamespace(context_cache=FakeContextCache(), rate_limiter=FakeRateLimiter()),
    )
    capture, fence = source
    await bind_projection(database, fence)
    other = uuid4()
    async with database() as db:
        db.add(Organization(id=other, name="Foreign HTTP synthetic tenant"))
        await db.flush()
        for org, key in ((fence.organization_id, "owned-http-a"), (other, "owned-http-b")):
            db.add(
                APIKey(
                    organization_id=org,
                    encrypted_key=credentials.encrypt({"key": key}),
                    expiration_date=datetime.now(timezone.utc).replace(tzinfo=None)
                    + timedelta(hours=1),
                )
            )
        await db.commit()
    async with tenant_session_factory(tenant_runtime.tenant, fence.organization_id)() as db:
        await capture.capture(
            db, CaptureBatch(fence=fence, records=(observation("http-original"),))
        )
        record = await db.scalar(select(Entity.id))
    app = FastAPI()
    app.include_router(records.router, prefix="/sync")
    # Auth mode is normally chosen before module import. Match the configured
    # API-key mode's inactive Auth0 dependency; persisted SQL key resolution stays real.
    app.dependency_overrides[deps.auth0.get_user] = lambda: None
    app.add_exception_handler(CanonicalStoreError, records.record_error_response)
    app.dependency_overrides[deps.get_control_session_factory] = lambda: async_sessionmaker(
        tenant_runtime.control, expire_on_commit=False
    )
    app.dependency_overrides[deps.get_owned_tenant_engine] = lambda: tenant_runtime.tenant
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-http-cursor"
    )
    path = f"/sync/{fence.sync_id}/records/{record}"
    async with AsyncClient(transport=ASGITransport(app), base_url="http://synthetic") as client:
        replies = await asyncio.gather(
            *(
                client.get(path, headers={"X-API-Key": key})
                for key in ("owned-http-a", "owned-http-b", "owned-http-a", "owned-http-b")
            )
        )
        assert [r.status_code for r in replies] == [200, 404, 200, 404], [r.text for r in replies]
        assert replies[0].json()["identity"]["native_id"] == "http-original"
        assert "http-original" not in replies[1].text
        forged = await client.get(
            path,
            headers={"X-API-Key": "owned-http-b", "X-Organization-ID": str(fence.organization_id)},
        )
        assert forged.status_code == 403
        async with tenant_session_factory(tenant_runtime.tenant, fence.organization_id)() as db:
            await db.execute(update(SourceConnection).values(is_authenticated=False))
            await db.commit()
        revoked = await client.get(path, headers={"X-API-Key": "owned-http-a"})
        assert revoked.status_code == 200
        assert revoked.json()["content_access"] == "unavailable"
        assert revoked.json()["payload"] == {}
        assert revoked.json()["blobs"] == []
        async with tenant_session_factory(tenant_runtime.tenant, fence.organization_id)() as db:
            await db.execute(delete(APIKey))
            await db.commit()
        revoked_key = await client.get(path, headers={"X-API-Key": "owned-http-a"})
        assert revoked_key.status_code == 403


async def test_shared_worker_loads_discovered_ids_only_through_tenant_sessions(
    database, source, tenant_runtime, monkeypatch
):
    """Real fixed discovery plus the actual worker crosses only ID boundaries."""
    from unittest.mock import MagicMock

    from sqlalchemy import update

    from airweave.domains.temporal.activities import cleanup_stuck_sync_jobs as worker

    _, first = source
    await bind_projection(database, first)
    other, sync, job = uuid4(), uuid4(), uuid4()
    async with database() as db:
        db.add(Organization(id=other, name="Second synthetic worker tenant"))
        await db.flush()
        db.add(Sync(id=sync, organization_id=other, name="Other shared-worker source"))
        await db.flush()
        db.add(SyncJob(id=job, organization_id=other, sync_id=sync, status="pending"))
        await db.commit()
    await bind_projection(database, SimpleNamespace(organization_id=other, sync_id=sync))
    for organization in (first.organization_id, other):
        async with tenant_session_factory(tenant_runtime.tenant, organization)() as db:
            await db.execute(
                update(SyncJob).values(status="pending", modified_at=datetime(2020, 1, 1))
            )
            await db.commit()
    monkeypatch.setattr(
        worker,
        "get_db_context",
        lambda: async_sessionmaker(tenant_runtime.control, expire_on_commit=False)(),
    )
    monkeypatch.setattr(
        worker,
        "get_tenant_db_context",
        lambda organization: tenant_session_factory(tenant_runtime.tenant, organization)(),
    )
    activity = worker.CleanupStuckSyncJobsActivity(
        temporal_workflow_service=MagicMock(),
        state_machine=MagicMock(),
        entity_repo=MagicMock(),
        org_repo=MagicMock(),
    )
    candidates = await activity._find_stuck_jobs(
        datetime(2021, 1, 1), datetime(2021, 1, 1), MagicMock()
    )
    assert {(candidate.organization_id, candidate.id) for candidate in candidates} == {
        (first.organization_id, first.job_id),
        (other, job),
    }
