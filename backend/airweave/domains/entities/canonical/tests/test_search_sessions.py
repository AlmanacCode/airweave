"""Real PostgreSQL: search owns short reads, even when remote work stops or fails."""

# Imported pytest fixtures are requested by parameter name.
# ruff: noqa: F811

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet
from fastapi import Request
from fastapi_auth0 import Auth0User
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from airweave.adapters.cache.fake import FakeContextCache
from airweave.adapters.rate_limiter.fake import FakeRateLimiter
from airweave.api import deps
from airweave.core import credentials
from airweave.core.config import AuthMode, settings
from airweave.core.shared_models import AuthMethod
from airweave.db import session as db_session
from airweave.domains.entities.canonical.tests.test_owned_search import (  # noqa: F401
    http_search,
    indexed,
)
from airweave.domains.entities.canonical.tests.test_search_visibility import hit
from airweave.domains.search.types import SearchResults
from airweave.models.api_key import APIKey
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.user import User
from airweave.models.user_organization import UserOrganization


@pytest.fixture
async def single_connection(database):
    async with database() as db:
        schema = await db.scalar(text("SELECT current_schema()"))
        url = db.bind.url
    engine = create_async_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.25,
        connect_args={"server_settings": {"search_path": schema}},
    )
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def connection_is_free(sessions):
    # A second checkout of this one-connection pool would fail if search held it.
    async with sessions() as probe:
        assert await probe.scalar(text("SELECT 1")) == 1


@pytest.mark.parametrize("stage", ["embedding", "vespa"])
async def test_waiting_search_releases_connection_and_cancels_cleanly(
    indexed, http_search, single_connection, stage, monkeypatch
):
    fence, _, _ = indexed
    client, vector, _, executor, _ = http_search
    client._transport.app.dependency_overrides[deps.get_tenant_session_factory] = (
        lambda: single_connection
    )
    entered, release = asyncio.Event(), asyncio.Event()
    target, method = (
        (executor._sparse_embedder, "embed") if stage == "embedding" else (vector, "execute_query")
    )
    original = getattr(target, method)

    async def paused(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(target, method, paused)
    task = asyncio.create_task(
        client.post(
            "/sync/search",
            json={
                "query": "budget",
                "sync_ids": [str(fence.sync_id)],
                "mode": "keyword",
            },
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        await connection_is_free(single_connection)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
    await connection_is_free(single_connection)


async def test_revoked_source_during_vespa_cannot_return_cached_hit(
    indexed, http_search, single_connection, monkeypatch
):
    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    client._transport.app.dependency_overrides[deps.get_tenant_session_factory] = (
        lambda: single_connection
    )
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    original = vector.execute_query

    async def revoke_then_return(query):
        # This mutation also proves the network phase has released the only connection.
        async with single_connection() as db:
            await db.execute(update(SourceConnection).values(is_authenticated=False))
            await db.commit()
        return await original(query)

    monkeypatch.setattr(vector, "execute_query", revoke_then_return)
    response = await client.post(
        "/sync/search",
        json={
            "query": "budget",
            "sync_ids": [str(fence.sync_id)],
            "mode": "keyword",
        },
    )
    assert response.status_code == 404
    assert "Private original text" not in response.text
    await connection_is_free(single_connection)


@pytest.mark.parametrize("outcome", ["failure", "cancellation"])
async def test_read_phase_releases_acquired_connection(
    indexed, http_search, single_connection, outcome
):
    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    app = client._transport.app
    app.dependency_overrides[deps.get_tenant_session_factory] = lambda: single_connection
    service = app.dependency_overrides[deps.get_container]().owned_search
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    entered = asyncio.Event()

    async def interrupt_after_sql(db, *args):
        await db.scalar(select(Entity.id))
        entered.set()
        if outcome == "failure":
            raise RuntimeError("synthetic enrichment failure")
        await asyncio.Event().wait()

    service._enrich = interrupt_after_sql
    task = asyncio.create_task(
        client.post(
            "/sync/search",
            json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
        )
    )
    waiting = asyncio.create_task(entered.wait())
    try:
        await asyncio.wait({task, waiting}, timeout=5, return_when=asyncio.FIRST_COMPLETED)
        if not entered.is_set():
            if task.done():
                response = await task  # Surface an early exception instead of hiding it as timeout.
                pytest.fail(f"Search returned HTTP {response.status_code} before enrichment")
            pytest.fail("Search did not reach enrichment within five seconds")
        if outcome == "cancellation":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(RuntimeError, match="synthetic enrichment failure"):
                await task
    finally:
        # Failed setup must not leave a request borrowing from the fixture's pool.
        waiting.cancel()
        task.cancel()
        await asyncio.gather(waiting, task, return_exceptions=True)
    await connection_is_free(single_connection)


@pytest.mark.parametrize("mode", [AuthMode.API_KEY, AuthMode.AUTH0])
async def test_search_auth_closes_own_transaction_without_committing_auth0_activity(
    source, single_connection, mode, monkeypatch
):
    _, fence = source
    monkeypatch.setattr(settings, "AUTH_MODE", mode)
    monkeypatch.setattr(settings, "ENCRYPTION_KEY", Fernet.generate_key().decode())
    # No external analytics in this real SQL authentication proof.
    monkeypatch.setattr("airweave.api.context_resolver.analytics.identify_user", Mock())
    old_active = datetime(2020, 1, 1)
    async with single_connection() as db:
        user = User(email="search@example.com", auth0_id="auth0|search", last_active_at=old_active)
        db.add(user)
        await db.flush()
        user_id = user.id
        db.add(
            UserOrganization(
                user_id=user.id,
                organization_id=fence.organization_id,
                role="owner",
                is_primary=True,
            )
        )
        db.add(
            APIKey(
                organization_id=fence.organization_id,
                encrypted_key=credentials.encrypt({"key": "synthetic-owned-search-key"}),
                expiration_date=datetime.now(timezone.utc).replace(tzinfo=None)
                + timedelta(hours=1),
            )
        )
        await db.commit()
    monkeypatch.setattr(db_session, "AsyncSessionLocal", single_connection)
    assert deps.get_control_session_factory() is single_connection
    auth0_user = Auth0User.model_construct(id="auth0|search", email="search@example.com")
    ctx = await deps.get_owned_context(
        request=Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/sync/search",
                "headers": [],
                "client": ("127.0.0.1", 1),
                "scheme": "http",
            }
        ),
        sessions=deps.get_control_session_factory(),
        x_api_key="synthetic-owned-search-key" if mode == AuthMode.API_KEY else None,
        x_organization_id=str(fence.organization_id),
        auth0_user=auth0_user if mode == AuthMode.AUTH0 else None,
        cache=FakeContextCache(),
        rate_limiter=FakeRateLimiter(),
    )
    assert ctx.organization.id == fence.organization_id
    assert ctx.auth_method == (AuthMethod.API_KEY if mode == AuthMode.API_KEY else AuthMethod.AUTH0)
    async with single_connection() as db:
        assert await db.scalar(select(User.last_active_at).where(User.id == user_id)) == old_active
    if mode == AuthMode.AUTH0:
        assert ctx.user.last_active_at > old_active
    await connection_is_free(single_connection)


@pytest.mark.parametrize("change", ["collection", "pipeline"])
async def test_scope_identity_change_during_retrieval_rejects_response(
    indexed, http_search, single_connection, monkeypatch, change
):
    fence, locator, _ = indexed
    client, vector, _, _, _ = http_search
    client._transport.app.dependency_overrides[deps.get_tenant_session_factory] = (
        lambda: single_connection
    )
    vector.seed_results(SearchResults(results=[hit(fence, locator.encode())]))
    original = vector.execute_query

    async def change_then_return(query):
        async with single_connection() as db:
            if change == "collection":
                await db.execute(update(Collection).values(id=uuid4()))
            else:
                await db.execute(update(Sync).values(index_pipeline_version=1))
            await db.commit()
        return await original(query)

    monkeypatch.setattr(vector, "execute_query", change_then_return)
    response = await client.post(
        "/sync/search",
        json={"query": "budget", "sync_ids": [str(fence.sync_id)], "mode": "keyword"},
    )
    assert response.status_code == 404
    assert "Private original text" not in response.text
    await connection_is_free(single_connection)
