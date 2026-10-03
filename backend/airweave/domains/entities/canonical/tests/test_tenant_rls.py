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
        with pytest.raises(DBAPIError, match="permission denied"):
            await db.scalars(select(Entity))


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
