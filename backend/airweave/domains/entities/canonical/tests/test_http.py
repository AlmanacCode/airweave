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
from airweave.domains.entities.canonical.requests import BlobReference, CaptureBatch, RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import observation


def query_app(database, context):
    """Use the real query service with synthetic HTTP authentication and signing."""
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_context] = context
    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-cursor-secret"
    )
    return app


async def test_http_read_list_changes_and_cross_tenant_denial(database, source):
    capture, fence = source
    async with database() as db:
        result = await capture.capture(
            db, CaptureBatch(fence=fence, records=(observation("one"), observation("two")))
        )
    owner = fence.organization_id

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=owner))

    app = query_app(database, context)
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


async def test_http_change_continuation_redacts_withdrawn_snapshots_and_rejects_foreign_cursors(
    database, source
):
    capture, fence = source
    parent_identity = RecordIdentity(record_type="calendar", native_id="cal")
    parent = observation(identity=parent_identity)
    child = observation(
        "event",
        "cal",
        parent=parent_identity,
        blobs=(BlobReference(key="immutable", sha256="a" * 64, size_bytes=7),),
    )
    async with database() as db:
        initial = await capture.capture(db, CaptureBatch(fence=fence, records=(parent, child)))
    original_child = initial.changes[1].record
    owner = fence.organization_id

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=owner))

    app = query_app(database, context)
    base = f"/sync/{fence.sync_id}/records"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        first_response = await client.get(base + "/changes", params={"limit": 1})
        assert first_response.status_code == 200
        first = first_response.json()
        assert first["has_more"] and first["high_watermark"] == 2
        assert [change["sequence"] for change in first["changes"]] == [1]

        # Commit withdrawal between pages, before descendant reconciliation.
        tombstone = parent.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"})
        async with database() as db:
            withdrawn = await capture.capture(db, CaptureBatch(fence=fence, records=(tombstone,)))
        assert withdrawn.sequence == 3

        params = {"cursor": first["next_cursor"], "limit": 1}
        continued_response = await client.get(base + "/changes", params=params)
        assert continued_response.status_code == 200
        continued = continued_response.json()
        assert not continued["has_more"] and continued["high_watermark"] == 2
        assert [change["sequence"] for change in continued["changes"]] == [2]
        historical = continued["changes"][0]
        assert historical["kind"] == "upsert"
        assert historical["record"]["id"] == str(original_child.id)
        assert historical["record"]["revision"] == original_child.revision
        assert historical["record"]["deleted_at"] is None
        assert historical["record"]["content_access"] == "unavailable"
        assert historical["record"]["payload"] == {} and historical["record"]["blobs"] == []
        replay = await client.get(base + "/changes", params=params)
        assert replay.status_code == 200 and replay.json() == continued

        polled_response = await client.get(
            base + "/changes", params={"cursor": continued["next_cursor"], "limit": 1}
        )
        assert polled_response.status_code == 200
        polled = polled_response.json()
        assert not polled["has_more"] and polled["high_watermark"] == 3
        assert [change["sequence"] for change in polled["changes"]] == [3]
        deletion = polled["changes"][0]
        assert deletion["kind"] == "delete"
        assert deletion["record"]["removal_reason"] == "access_revoked"
        assert deletion["record"]["content_access"] == "unavailable"
        assert deletion["record"]["payload"] == {} and deletion["record"]["blobs"] == []
        empty = await client.get(base + "/changes", params={"cursor": polled["next_cursor"]})
        assert empty.status_code == 200
        assert empty.json()["changes"] == [] and empty.json()["high_watermark"] == 3

        foreign_source = await client.get(f"/sync/{uuid4()}/records/changes", params=params)
        foreign_operation = await client.get(base, params=params)
        owner = uuid4()
        foreign_owner = await client.get(base + "/changes", params=params)
        for rejected in (foreign_source, foreign_operation, foreign_owner):
            assert rejected.status_code == 400
            assert rejected.json()["error"]["code"] == "invalid_cursor"
