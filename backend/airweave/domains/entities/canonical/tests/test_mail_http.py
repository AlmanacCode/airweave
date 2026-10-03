"""Real PostgreSQL mail traversal and HTTP blob authorization over synthetic bytes."""

import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from airweave.api import deps
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.db.session import get_db
from airweave.domains.entities.canonical.query import CanonicalQueryService, InvalidRecordCursor
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import BlobReference, CaptureBatch, RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import bind_projection, observation
from airweave.domains.storage.exceptions import StorageConnectionError, StorageNotFoundError
from airweave.models import Sync, SyncJob


def message(native_id, created=None, **kwargs):
    return observation(
        native_id,
        identity=RecordIdentity(record_type="message", native_id=native_id),
        payload={"id": native_id, "threadId": "same-thread"},
        source_created_at=created,
        **kwargs,
    )


def service():
    return CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "test-secret")


async def test_thread_chronology_ties_unknown_dates_and_scope(database, source):
    capture, fence = source
    await bind_projection(database, fence)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    other_sync, other_job = uuid4(), uuid4()
    async with database() as db:
        db.add(Sync(id=other_sync, organization_id=fence.organization_id, name="Other Gmail"))
        await db.flush()
        db.add(
            SyncJob(
                id=other_job,
                organization_id=fence.organization_id,
                sync_id=other_sync,
                status="running",
            )
        )
        await db.commit()
        other_fence = await capture.activate_writer(
            db, fence.organization_id, other_sync, other_job, attempt_id=uuid4(), attempt_number=1
        )
        await capture.capture(db, CaptureBatch(fence=other_fence, records=(message("other", now),)))
        await capture.capture(
            db,
            CaptureBatch(
                fence=fence,
                records=(
                    message("unknown"),
                    message("unknown2"),
                    message("b", now, completeness="partial"),
                    message("a", now),
                ),
            ),
        )
    await bind_projection(database, other_fence)
    query = service()
    rows, cursor = [], None
    async with database() as db:
        while True:
            page = await query.mail_thread(
                db, fence.organization_id, fence.sync_id, "same-thread", limit=1, cursor=cursor
            )
            rows.extend(page.messages)
            assert page.coverage == "stored_messages_only"
            if not page.has_more:
                break
            cursor = page.next_cursor
        assert {r.identity.native_id for r in rows[-2:]} == {"unknown", "unknown2"}
        assert rows[-2].id < rows[-1].id
        assert rows[0].id < rows[1].id
        assert {r.identity.native_id for r in rows} == {"a", "b", "unknown", "unknown2"}
        assert all(r.identity.container_id is None for r in rows)
        assert any(r.completeness == "partial" for r in rows)
        for org, sync, thread in [
            (uuid4(), fence.sync_id, "same-thread"),
            (fence.organization_id, other_sync, "same-thread"),
            (fence.organization_id, fence.sync_id, "different"),
        ]:
            with pytest.raises(InvalidRecordCursor):
                await query.mail_thread(db, org, sync, thread, cursor=cursor)
        other = await query.mail_thread(db, fence.organization_id, other_sync, "same-thread")
        assert [r.identity.native_id for r in other.messages] == ["other"]
        index = await db.scalar(
            text(
                "SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() "
                "AND indexname='ix_entity_mail_thread'"
            )
        )
        assert "threadId" in index and "source_created_at" in index


async def test_http_blob_scope_revision_missing_corrupt_and_revoke_during_io(database, source):
    capture, fence = source
    await bind_projection(database, fence)
    content = b"synthetic MIME body"
    digest = hashlib.sha256(content).hexdigest()
    ref = BlobReference(
        key=f"canonical/{fence.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="text/plain",
    )
    item = message("one", blobs=(ref,), completeness="partial")
    async with database() as db:
        result = await capture.capture(db, CaptureBatch(fence=fence, records=(item,)))
    record_id = result.changes[0].record.id
    storage = AsyncMock()
    storage.read_file.return_value = content
    owner = fence.organization_id
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=owner))

    async def session():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_owned_context] = context
    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_tenant_db] = session
    app.dependency_overrides[deps.get_canonical_query_service] = service
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(storage_backend=storage)
    path = f"/sync/{fence.sync_id}/records/{record_id}/blobs/{digest}"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(path, params={"revision": 1})
        assert response.status_code == 200 and response.content == content
        assert response.headers["cache-control"] == "private, no-store"
        thread = await client.get(f"/sync/{fence.sync_id}/mail/threads/same-thread")
        assert (
            thread.status_code == 200 and thread.json()["messages"][0]["completeness"] == "partial"
        )
        assert (await client.get(path, params={"revision": 2})).status_code == 409
        assert (
            await client.get(path.replace(digest, "f" * 64), params={"revision": 1})
        ).status_code == 404
        assert (
            await client.get(path.replace(str(fence.sync_id), str(uuid4())), params={"revision": 1})
        ).status_code == 404
        owner = uuid4()
        assert (await client.get(path, params={"revision": 1})).status_code == 404
        assert (
            await client.get(f"/sync/{fence.sync_id}/mail/threads/same-thread")
        ).status_code == 404
        owner = fence.organization_id
        for failure in (StorageNotFoundError("missing"), StorageConnectionError("backend down")):
            storage.read_file.side_effect = failure
            unavailable = await client.get(path, params={"revision": 1})
            assert unavailable.status_code == 503
            assert unavailable.json()["error"]["code"] == "blob_unavailable"
        storage.read_file.side_effect = ValueError("programming error")
        with pytest.raises(ValueError, match="programming error"):
            await client.get(path, params={"revision": 1})
        storage.read_file.side_effect = None
        storage.read_file.return_value = b"corrupt"
        assert (await client.get(path, params={"revision": 1})).status_code == 503

        async def revoke(_, *, max_bytes):
            async with database() as db:
                await capture.capture(
                    db,
                    CaptureBatch(
                        fence=fence,
                        records=(
                            item.model_copy(
                                update={"kind": "delete", "removal_reason": "access_revoked"}
                            ),
                        ),
                    ),
                )
            return content

        storage.read_file.side_effect = revoke
        assert (await client.get(path, params={"revision": 1})).status_code == 404
        hidden = await client.get(f"/sync/{fence.sync_id}/mail/threads/same-thread")
        assert hidden.json()["messages"] == []
