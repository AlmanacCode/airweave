"""Real persisted-key authentication through the original-record HTTP boundary."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.adapters.rate_limiter.null import NullRateLimiter
from airweave.api import deps
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.core import container as container_mod
from airweave.core import credentials
from airweave.core.config import AuthMode, settings
from airweave.core.datetime_utils import utc_now_naive
from airweave.db.session import get_db
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation
from airweave.models import Organization
from airweave.models.api_key import APIKey


@pytest.fixture
async def runtime_database(database):
    """Keep migration privileges out of the HTTP runtime in this disposable schema."""
    import os
    import re

    role = "canonical_runtime_" + uuid4().hex
    async with database() as db:
        schema = await db.scalar(text("SELECT current_schema()"))
        assert re.fullmatch(r"canonical_test_[a-f0-9]{32}", schema)
        await db.execute(
            text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE')
        )
        await db.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
        await db.execute(
            text(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES "
                f'IN SCHEMA "{schema}" TO "{role}"'
            )
        )
        await db.execute(
            text(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "{schema}" TO "{role}"')
        )
        await db.commit()
    engine = create_async_engine(
        os.environ["CANONICAL_TEST_DATABASE_URL"],
        connect_args={"server_settings": {"search_path": schema, "role": role}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as db:
            actual = (
                await db.execute(
                    text(
                        "SELECT rolname, rolsuper, rolbypassrls, rolcreatedb, rolcreaterole "
                        "FROM pg_roles WHERE rolname=current_user"
                    )
                )
            ).one()
            assert tuple(actual) == (role, False, False, False, False)
            with pytest.raises(DBAPIError, match="permission denied"):
                await db.execute(text("CREATE TABLE forbidden_runtime_ddl (id integer)"))
            await db.rollback()
        yield sessions
    finally:
        await engine.dispose()
        async with database() as db:
            await db.execute(text(f'REVOKE ALL ON ALL TABLES IN SCHEMA "{schema}" FROM "{role}"'))
            await db.execute(
                text(f'REVOKE ALL ON ALL SEQUENCES IN SCHEMA "{schema}" FROM "{role}"')
            )
            await db.execute(text(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{role}"'))
            await db.execute(text(f'DROP ROLE "{role}"'))
            await db.commit()


async def test_persisted_keys_scope_expiry_and_revocation_over_http(
    database, runtime_database, source, monkeypatch
):
    """Only Redis cache/rate limiting are substituted; auth and SQL stay real."""
    monkeypatch.setattr(settings, "AUTH_MODE", AuthMode.API_KEY)
    monkeypatch.setattr(settings, "ENCRYPTION_KEY", Fernet.generate_key().decode())
    cache = SimpleNamespace(
        get_organization=AsyncMock(return_value=None), set_organization=AsyncMock()
    )
    monkeypatch.setattr(
        container_mod,
        "container",
        SimpleNamespace(context_cache=cache, rate_limiter=NullRateLimiter()),
    )
    capture, fence = source
    await bind_projection(database, fence)
    other_org = uuid4()
    owner_key, foreign_key = uuid4().hex, uuid4().hex
    async with database() as db:
        db.add(Organization(id=other_org, name="Other synthetic owner"))
        await db.flush()
        keys = [
            APIKey(
                organization_id=org,
                encrypted_key=credentials.encrypt({"key": key}),
                expiration_date=utc_now_naive() + timedelta(days=1),
                created_by_email="synthetic@example.com",
                modified_by_email="synthetic@example.com",
            )
            for org, key in ((fence.organization_id, owner_key), (other_org, foreign_key))
        ]
        db.add_all(keys)
        await db.commit()
        # Capture also runs under the same restricted runtime privileges.
    async with runtime_database() as db:
        result = await capture.capture(db, CaptureBatch(fence=fence, records=(observation("one"),)))
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def session():
        async with runtime_database() as db:
            yield db

    app.dependency_overrides[deps.get_control_session_factory] = lambda: runtime_database
    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_tenant_db] = session
    base = f"/sync/{fence.sync_id}/records"
    paths = (base, f"{base}/{result.changes[0].record.id}", base + "/changes")
    async with AsyncClient(transport=ASGITransport(app), base_url="http://test") as client:
        for path in paths:
            assert (await client.get(path)).status_code == 401
            assert (await client.get(path, headers={"X-API-Key": "invalid"})).status_code == 403
            valid = await client.get(path, headers={"X-API-Key": owner_key})
            assert valid.status_code == 200, valid.text
            assert (await client.get(path, headers={"X-API-Key": foreign_key})).status_code == 404
            assert (
                await client.get(
                    path,
                    headers={
                        "X-API-Key": foreign_key,
                        "X-Organization-ID": str(fence.organization_id),
                    },
                )
            ).status_code == 403
        async with database() as db:
            await db.execute(
                update(APIKey)
                .where(APIKey.id == keys[0].id)
                .values(
                    expiration_date=utc_now_naive() - timedelta(seconds=1),
                )
            )
            await db.commit()
        assert (await client.get(base, headers={"X-API-Key": owner_key})).status_code == 403
        async with database() as db:
            await db.execute(delete(APIKey).where(APIKey.id == keys[1].id))
            await db.commit()
        assert (await client.get(base, headers={"X-API-Key": foreign_key})).status_code == 403
