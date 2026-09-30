"""Isolated real PostgreSQL fixtures shared by canonical store/query tests."""

import importlib.util
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.domains.entities.canonical.service import CanonicalCaptureService
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.models import Organization, Sync, SyncJob
from alembic.migration import MigrationContext
from alembic.operations import Operations


def migrate(connection, filename):
    """Run the actual migration with this connection's isolated search_path."""
    path = Path(__file__).resolve().parents[5] / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location("canonical_test_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with Operations.context(MigrationContext.configure(connection)):
        module.upgrade()


def seed_legacy(connection):
    """Insert an existing metadata row using the pre-migration schema."""
    from sqlalchemy import MetaData, Table

    metadata = MetaData()
    tables = {
        name: Table(name, metadata, autoload_with=connection)
        for name in ("organization", "sync", "sync_job", "entity")
    }
    org_id, sync_id, job_id, entity_id = [uuid4() for _ in range(4)]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    times = {"created_at": now, "modified_at": now}
    connection.execute(
        tables["organization"].insert().values(id=org_id, name="Legacy synthetic", **times)
    )
    connection.execute(
        tables["sync"]
        .insert()
        .values(
            id=sync_id,
            organization_id=org_id,
            name="Legacy sync",
            status="ACTIVE",
            sync_type="full",
            **times,
        )
    )
    connection.execute(
        tables["sync_job"]
        .insert()
        .values(
            id=job_id,
            organization_id=org_id,
            sync_id=sync_id,
            status="completed",
            entities_inserted=1,
            entities_updated=0,
            entities_deleted=0,
            entities_kept=0,
            entities_skipped=0,
            scheduled=False,
            **times,
        )
    )
    connection.execute(
        tables["entity"]
        .insert()
        .values(
            id=entity_id,
            organization_id=org_id,
            sync_id=sync_id,
            sync_job_id=job_id,
            entity_id="legacy-native-key",
            entity_definition_short_name="event",
            hash="old-hash",
            **times,
        )
    )


@pytest.fixture
async def database(request):
    """Every test gets its own schema; no global application DSN is consulted."""
    url = os.environ.get("CANONICAL_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set CANONICAL_TEST_DATABASE_URL to a disposable PostgreSQL database")
    schema = "canonical_test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with engine.begin() as connection:
            await connection.run_sync(migrate, "0000_baseline.py")
            if getattr(request, "param", None) == "legacy":
                await connection.run_sync(seed_legacy)
            await connection.run_sync(migrate, "0001_canonical_records.py")
            await connection.run_sync(migrate, "0002_projection_publication.py")
            await connection.run_sync(migrate, "0003_mail_thread_index.py")
            await connection.run_sync(migrate, "0004_projection_generation.py")
            await connection.run_sync(migrate, "0005_capture_scan.py")
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
async def source(database):
    organization_id, sync_id, job_id = uuid4(), uuid4(), uuid4()
    async with database() as db:
        db.add(Organization(id=organization_id, name="Synthetic canonical tests"))
        await db.flush()
        db.add(Sync(id=sync_id, organization_id=organization_id, name="Synthetic Gmail"))
        await db.flush()
        db.add(
            SyncJob(id=job_id, organization_id=organization_id, sync_id=sync_id, status="running")
        )
        await db.commit()
    service = CanonicalCaptureService(CanonicalRecordStore())
    async with database() as db:
        fence = await service.activate_writer(
            db, organization_id, sync_id, job_id, attempt_id=uuid4(), attempt_number=1
        )
    return service, fence
