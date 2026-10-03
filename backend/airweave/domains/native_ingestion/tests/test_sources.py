"""Actual SQL races, rollback and HTTP backend-authority gate for native sources."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from sqlalchemy import func, select, update

from airweave.api import deps
from airweave.api.v1.endpoints.native_sources import router
from airweave.domains.native_ingestion.source_models import (
    EnsureNativeSource,
    native_source_id,
    native_sync_id,
)
from airweave.domains.native_ingestion.source_service import NativeSources
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.models import Collection, Organization, SourceConnection, Sync
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


@pytest.fixture
async def native_setup(database):
    org, foreign = uuid4(), uuid4()
    async with database() as db:
        db.add_all([Organization(id=org, name="Native"), Organization(id=foreign, name="Other")])
        metadata = VectorDbDeploymentMetadata(
            dense_embedder="test", sparse_embedder="test", embedding_dimensions=3
        )
        db.add(metadata)
        await db.flush()
        for name, owner in (("one", org), ("two", org), ("foreign", foreign)):
            db.add(
                Collection(
                    name=name,
                    readable_id=name,
                    organization_id=owner,
                    vector_db_deployment_metadata_id=metadata.id,
                )
            )
        await db.commit()
    return org, foreign, NativeSources(NativeSourceStore())


async def ensure(database, service, org, request):
    async with database() as db:
        return await service.ensure(db, org, request)


async def test_concurrent_same_identity_and_immutable_collection(database, native_setup):
    org, _, service = native_setup
    request = EnsureNativeSource(owner_id="owner", dataset="knowledge", collection="one")
    results = await asyncio.gather(*(ensure(database, service, org, request) for _ in range(4)))
    assert all(result == results[0] for result in results)
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Sync)) == 1
        assert await db.scalar(select(Sync.index_pipeline_version)) == 4
        await db.execute(update(Sync).values(index_pipeline_version=2))
        await db.commit()
        assert await db.scalar(select(func.count()).select_from(SourceConnection)) == 1
    await ensure(database, service, org, request)
    async with database() as db:
        assert await db.scalar(select(Sync.index_pipeline_version)) == 2  # Never upgrade existing.
    with pytest.raises(HTTPException) as conflict:
        await ensure(database, service, org, request.model_copy(update={"collection": "two"}))
    assert conflict.value.status_code == 409
    async with database() as db:
        assert await service.get(db, org, results[0].source_connection_id) == results[0]


async def test_creation_rollback_and_partial_state_fail_closed(database, native_setup, monkeypatch):
    org, _, service = native_setup
    request = EnsureNativeSource(owner_id="owner", dataset="sessions", collection="one")
    original = service.store.require

    async def fail(*args, **kwargs):
        raise RuntimeError("synthetic failure after source insert")

    monkeypatch.setattr(service.store, "require", fail)
    with pytest.raises(RuntimeError, match="after source insert"):
        await ensure(database, service, org, request)
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(Sync)) == 0
        assert await db.scalar(select(func.count()).select_from(SourceConnection)) == 0
    monkeypatch.setattr(service.store, "require", original)
    async with database() as db:
        db.add(
            Sync(
                id=native_sync_id(native_source_id(org, request.binding())),
                organization_id=org,
                name="Unexplained partial",
            )
        )
        await db.commit()
    with pytest.raises(HTTPException) as conflict:
        await ensure(database, service, org, request)
    assert conflict.value.status_code == 409
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(SourceConnection)) == 0


async def test_owner_namespace_and_collection_organization_isolation(database, native_setup):
    org, foreign, service = native_setup
    request = EnsureNativeSource(owner_id="same-owner", dataset="knowledge", collection="one")
    ours = await ensure(database, service, org, request)
    theirs = await ensure(
        database, service, foreign, request.model_copy(update={"collection": "foreign"})
    )
    assert ours.source_connection_id != theirs.source_connection_id
    async with database() as db:
        with pytest.raises(HTTPException) as missing:
            await service.get(db, foreign, ours.source_connection_id)
    assert missing.value.status_code == 404
    with pytest.raises(HTTPException) as missing:
        await ensure(database, service, foreign, request)
    assert missing.value.status_code == 404


async def test_http_session_cannot_ensure_or_read_but_backend_key_can(database, native_setup):
    org, _, service = native_setup
    ctx = SimpleNamespace(organization=SimpleNamespace(id=org), is_api_key_auth=False)
    app = FastAPI()
    app.include_router(router, prefix="/native/sources")

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_tenant_db] = session
    app.dependency_overrides[deps.get_owned_context] = lambda: ctx
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(native_sources=service)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        body = {"owner_id": "owner", "dataset": "knowledge", "collection": "one"}
        assert (await client.put("/native/sources", json=body)).status_code == 403
        assert (await client.get(f"/native/sources/{uuid4()}")).status_code == 403
        ctx.is_api_key_auth = True
        created = await client.put("/native/sources", json=body)
        assert created.status_code == 200, created.text
        loaded = await client.get(f"/native/sources/{created.json()['source_connection_id']}")
        assert loaded.status_code == 200 and loaded.json() == created.json()
        assert (await client.put("/native/sources", json=body)).json() == created.json()
