"""Native HTTP contract with actual PostgreSQL transactions and safe diagnostics."""

from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from sqlalchemy import func, select

from airweave.api import deps, middleware
from airweave.api.v1.endpoints.native_imports import router
from airweave.db.session import get_db
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.native_ingestion.import_service import NativeImports
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.source_models import EnsureNativeSource
from airweave.domains.native_ingestion.source_service import NativeSources
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.models import Collection, Organization, SyncJob
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


@pytest.fixture
async def native_api(database, monkeypatch):
    org = uuid4()
    async with database() as db:
        db.add(Organization(id=org, name="Native API"))
        metadata = VectorDbDeploymentMetadata(
            dense_embedder="test", sparse_embedder="test", embedding_dimensions=3
        )
        db.add(metadata)
        await db.flush()
        db.add(
            Collection(
                name="API",
                readable_id="native-api",
                organization_id=org,
                vector_db_deployment_metadata_id=metadata.id,
            )
        )
        await db.commit()
    sources = NativeSourceStore()
    async with database() as db:
        source = await NativeSources(sources).ensure(
            db,
            org,
            EnsureNativeSource(owner_id="owner", dataset="knowledge", collection="native-api"),
        )
    service = NativeImports(NativeImportStore(sources, CanonicalRecordStore()))
    ctx = SimpleNamespace(is_api_key_auth=True, organization=SimpleNamespace(id=org))
    app = FastAPI()
    app.include_router(router, prefix="/native/sources")
    app.exception_handler(RequestValidationError)(middleware.validation_exception_handler)
    app.middleware("http")(middleware.log_requests)
    logs = []
    monkeypatch.setattr(
        middleware,
        "logger",
        SimpleNamespace(info=logs.append, error=logs.append, warning=logs.append),
    )

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_context] = lambda: ctx
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(native_imports=service)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        yield client, ctx, source, logs


async def test_import_http_retry_conflict_and_key_validation(database, native_api):
    client, _, source, logs = native_api
    path = f"/native/sources/{source.source_connection_id}/imports/private-request-key"
    body = {"snapshot_id": "private-snapshot-value", "coverage": "bounded"}
    first = await client.put(path, json=body)
    assert first.status_code == 200, first.text
    assert (await client.put(path, json=body)).json() == first.json()
    assert (await client.get(path)).json() == first.json()
    assert "fence" not in first.json()
    conflict = await client.put(path, json={**body, "coverage": "complete"})
    assert conflict.status_code == 409
    assert (await client.put(path + "-other", json=body)).status_code == 409
    assert (await client.get(path + "-missing")).status_code == 404
    assert (await client.put(path + "x" * 129, json=body)).status_code == 422
    assert (
        await client.put(path, json={**body, "coverage": "private-invalid-value"})
    ).status_code == 422
    assert "private-request-key" not in str(logs)
    assert "private-snapshot-value" not in str(logs)
    assert "private-invalid-value" not in str(logs)
    assert "{request_key}" in str(logs)
    async with database() as db:
        assert await db.scalar(select(func.count()).select_from(SyncJob)) == 1


async def test_import_http_rejects_session_and_foreign_organization(native_api):
    client, ctx, source, _ = native_api
    path = f"/native/sources/{source.source_connection_id}/imports/request-key"
    body = {"snapshot_id": "snapshot", "coverage": "bounded"}
    ctx.is_api_key_auth = False
    assert (await client.put(path, json=body)).status_code == 403
    assert (await client.get(path)).status_code == 403
    ctx.is_api_key_auth = True
    ctx.organization.id = uuid4()
    assert (await client.put(path, json=body)).status_code == 404
    assert (await client.get(path)).status_code == 404


async def test_native_analytics_and_exception_logs_keep_only_safe_route(monkeypatch):
    from starlette.requests import Request
    from starlette.responses import Response

    request = Request(
        {
            "type": "http",
            "method": "PUT",
            "scheme": "http",
            "headers": [],
            "path": "/native/sources/source/imports/private-request-key",
            "query_string": b"payload=private-query",
            "path_params": {"source_id": "source", "request_key": "private-request-key"},
            "route": SimpleNamespace(path="/native/sources/{source_id}/imports/{request_key}"),
        }
    )
    captured = []
    monkeypatch.setattr(middleware.analytics, "track_event", lambda *a, **k: captured.append(a))
    monkeypatch.setattr(middleware, "logger", SimpleNamespace(error=captured.append))
    await middleware._track_api_call_async(None, Response(status_code=200), 12.5, request)

    async def failed(_):
        raise ValueError("private-payload-content")

    response = await middleware.exception_logging_middleware(request, failed)
    assert response.status_code == 500
    assert "private-" not in str(captured)
    assert "{request_key}" in str(captured)
    assert "private-" not in response.body.decode()
