"""Configured provider limits use actual tenant RLS and disposable Redis."""

import asyncio
import os
import shutil
import socket
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import redis.asyncio as redis
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from airweave.db.tenant_session import tenant_session_factory
from airweave.domains.entities.canonical.tests.test_tenant_rls import tenant_runtime  # noqa: F401
from airweave.domains.sources.rate_limiting.config_provider import DatabaseRateLimitConfigProvider
from airweave.domains.sources.rate_limiting.service import SourceRateLimiter
from airweave.models import Organization
from airweave.models.source_rate_limit import SourceRateLimit
from airweave.platform.http_client.airweave_client import AirweaveHttpClient


@pytest.fixture
async def local_redis(tmp_path):
    """One nonpersistent process, no shared keys or existing Redis connections."""
    executable = shutil.which("redis-server")
    if executable is None:
        pytest.skip("Source limit integration requires redis-server")
    with socket.socket() as socket_:
        socket_.bind(("127.0.0.1", 0))
        port = socket_.getsockname()[1]
    process = await asyncio.create_subprocess_exec(
        executable, "--bind", "127.0.0.1", "--port", str(port),
        "--save", "", "--appendonly", "no", "--dir", str(tmp_path),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        env={**os.environ, "LC_ALL": "C"},
    )
    client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
    try:
        for _ in range(100):
            try:
                await client.ping()
                break
            except redis.ConnectionError:
                await asyncio.sleep(0.02)
        else:
            raise RuntimeError("Disposable Redis did not start")
        yield client
    finally:
        try:
            await client.close()
        finally:
            if process.returncode is None:
                process.terminate()
            await process.wait()


async def test_own_limit_rls_absence_and_outage_block_provider(
    database, tenant_runtime, local_redis, monkeypatch  # noqa: F811
):
    own, foreign = uuid4(), uuid4()
    async with database() as db:
        db.add_all([Organization(id=own, name="Own"), Organization(id=foreign, name="Foreign")])
        await db.flush()
        db.add_all([
            SourceRateLimit(
                organization_id=own, source_short_name="gmail", limit=1, window_seconds=60
            ),
            SourceRateLimit(
                organization_id=foreign, source_short_name="gmail", limit=99, window_seconds=60
            ),
        ])
        await db.commit()
    async with tenant_session_factory(tenant_runtime.tenant, own)() as db:
        rows = (await db.scalars(select(SourceRateLimit))).all()
        assert [(row.organization_id, row.limit) for row in rows] == [(own, 1)]
    async with async_sessionmaker(tenant_runtime.tenant)() as db:
        assert not (await db.scalars(select(SourceRateLimit))).all()
    async with async_sessionmaker(tenant_runtime.control)() as db:
        with pytest.raises(DBAPIError, match="permission denied"):
            await db.scalars(select(SourceRateLimit))

    @asynccontextmanager
    async def scoped(organization_id):
        async with tenant_session_factory(tenant_runtime.tenant, organization_id)() as db:
            yield db

    monkeypatch.setattr(
        "airweave.domains.sources.rate_limiting.config_provider.get_tenant_db_context", scoped
    )
    provider = DatabaseRateLimitConfigProvider(local_redis)
    assert (await provider.get_config(own, "gmail")).limit == 1
    assert await provider.get_config(own, "slack") is None
    assert (await provider.get_config(foreign, "gmail")).limit == 99
    registry = SimpleNamespace(get=lambda _: SimpleNamespace(rate_limit_level="org"))
    limiter = SourceRateLimiter(local_redis, registry, provider)
    requests = []

    async def transport(request):
        requests.append(request)
        return httpx.Response(200, json={"synthetic": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AirweaveHttpClient(raw, own, "gmail", rate_limiter=limiter)
        url = "https://gmail.googleapis.com/gmail/v1/users/me/profile"
        assert (await client.get(url)).status_code == 200
        with pytest.raises(httpx.HTTPStatusError) as exceeded:
            await client.get("https://gmail.googleapis.com/gmail/v1/users/me/profile")
        assert exceeded.value.response.status_code == 429
        assert len(requests) == 1
        await local_redis.delete(f"source_rate_limit_config:{own}:gmail")

        @asynccontextmanager
        async def unavailable(_):
            raise RuntimeError("synthetic database unavailable")
            yield  # pragma: no cover

        monkeypatch.setattr(
            "airweave.domains.sources.rate_limiting.config_provider.get_tenant_db_context",
            unavailable
        )
        with pytest.raises(RuntimeError, match="database unavailable"):
            await client.get("https://gmail.googleapis.com/gmail/v1/users/me/profile")
        assert len(requests) == 1
        assert await local_redis.get(f"source_rate_limit_config:{own}:gmail") is None
