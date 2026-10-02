"""Real persisted-key authentication through the original-record HTTP boundary."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from cryptography.fernet import Fernet
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, update

from airweave.adapters.rate_limiter.null import NullRateLimiter
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.core import container as container_mod
from airweave.core import credentials
from airweave.core.config import AuthMode, settings
from airweave.core.datetime_utils import utc_now_naive
from airweave.db.session import get_db
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import observation
from airweave.models import Organization
from airweave.models.api_key import APIKey


async def test_persisted_keys_scope_expiry_and_revocation_over_http(database, source, monkeypatch):
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
        result = await capture.capture(db, CaptureBatch(fence=fence, records=(observation("one"),)))
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[get_db] = session
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
