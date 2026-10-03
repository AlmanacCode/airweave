"""Actual retained-history routes with injected synthetic authentication and PostgreSQL."""

from dataclasses import dataclass
from hashlib import sha256

import httpx
from fastapi import FastAPI

from airweave import schemas
from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.api import deps
from airweave.api.context import ApiContext, AuthMethod
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.db.session import get_db
from airweave.domains.entities.canonical.requests import BlobReference, CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation
from airweave.domains.entities.canonical.tests.test_query import query_service
from airweave.models.organization import Organization


@dataclass
class FixtureContainer:
    storage_backend: FilesystemBackend


async def test_historical_routes_preserve_current_download_contract(database, source, tmp_path):
    capture, fence = source
    await bind_projection(database, fence)
    storage = FilesystemBackend(tmp_path)
    content = b"synthetic historical attachment"
    digest = sha256(content).hexdigest()
    ref = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
    )
    await storage.write_file(ref.key, content)
    async with database() as db:
        first = await capture.capture(
            db, CaptureBatch(fence=fence, records=(observation(blobs=(ref,)),))
        )
        actor = ApiContext(
            organization=schemas.Organization.model_validate(
                await db.get(Organization, fence.organization_id)
            ),
            auth_method=AuthMethod.API_KEY,
        )
    record_id = first.changes[0].record.id
    async with database() as db:
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(observation(payload={"summary": "new body"}, content_hash="new"),),
            ),
        )
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def database_dependency():
        async with database() as db:
            yield db

    app.dependency_overrides[get_db] = database_dependency
    app.dependency_overrides[deps.get_context] = lambda: actor
    app.dependency_overrides[deps.get_canonical_query_service] = query_service
    app.dependency_overrides[deps.get_container] = lambda: FixtureContainer(storage)
    base = f"/api/v1/sync/{fence.sync_id}/records/{record_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://fixture"
    ) as client:
        read = await client.get(base + "/revisions/1")
        assert read.status_code == 200, read.text
        assert read.headers["cache-control"] == "private, no-store"
        assert read.json()["authority"] == "current_source_record_access"
        assert read.json()["current_revision"] == 2
        assert read.json()["record"]["revision"] == 1
        download = await client.get(base + f"/revisions/1/blobs/{digest}")
        assert download.status_code == 200
        assert download.content == content
        assert download.headers["cache-control"] == "private, no-store"
        assert (await client.get(base + f"/blobs/{digest}?revision=1")).status_code == 409
        assert (await client.get(base + "/revisions/999")).status_code == 404
        assert (await client.get(base + "/revisions/1/blobs/" + "0" * 64)).status_code == 404
