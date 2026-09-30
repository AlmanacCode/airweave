"""HTTP contract verification using the real PostgreSQL-backed query service."""

from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from airweave.api import deps
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.db.session import get_db
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import observation


async def test_http_read_list_changes_and_cross_tenant_denial(database, source):
    capture, fence = source
    async with database() as db:
        result = await capture.capture(
            db, CaptureBatch(fence=fence, records=(observation("one"), observation("two")))
        )
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)
    owner = fence.organization_id

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=owner))

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_context] = context
    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-cursor-secret"
    )
    base = f"/sync/{fence.sync_id}/records"
    record_path = f"{base}/{result.changes[0].record.id}"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(record_path)
        assert response.status_code == 200
        assert response.json()["identity"]["native_id"] == "one"
        listed = await client.get(base, params={"limit": 1})
        assert listed.status_code == 200
        assert listed.json()["has_more"]
        continued = await client.get(
            base, params={"cursor": listed.json()["next_cursor"], "limit": 1}
        )
        assert continued.status_code == 200 and not continued.json()["has_more"]
        changes = await client.get(base + "/changes")
        assert changes.status_code == 200
        assert len(changes.json()["changes"]) == 2
        invalid = await client.get(base, params={"cursor": "not-a-valid-cursor"})
        assert invalid.status_code == 400
        assert invalid.json()["error"]["code"] == "invalid_cursor"
        oversized = await client.get(base, params={"limit": 501})
        assert oversized.status_code == 422
        owner = uuid4()
        assert (await client.get(record_path)).status_code == 404
        assert (await client.get(base)).status_code == 404
        assert (await client.get(base + "/changes")).status_code == 404
