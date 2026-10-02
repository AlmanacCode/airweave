"""A fresh migrated database per test; no shared corpus schema changes."""

import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.domains.entities.canonical.tests.conftest import migrate


@pytest.fixture
async def database():
    url = os.environ.get("CANONICAL_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set CANONICAL_TEST_DATABASE_URL to disposable PostgreSQL")
    name = "owned_tenant_test_" + uuid4().hex
    admin = create_async_engine(url, isolation_level="AUTOCOMMIT")
    async with admin.connect() as db:
        await db.execute(text(f'CREATE DATABASE "{name}"'))
    engine = create_async_engine(make_url(url).set(database=name))
    try:
        async with engine.begin() as db:
            versions = Path(__file__).resolve().parents[4] / "alembic" / "versions"
            for migration in sorted(versions.glob("[0-9][0-9][0-9][0-9]_*.py")):
                await db.run_sync(migrate, migration.name)
        yield async_sessionmaker(engine, autoflush=False, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin.connect() as db:
            await db.execute(text(f'DROP DATABASE "{name}"'))
        await admin.dispose()
