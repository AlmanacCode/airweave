"""Real retained HTTP routes + SQL; synthetic auth context is not enrollment proof."""

from hashlib import sha256
from types import SimpleNamespace
from uuid import uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import select

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.api import deps
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.db.session import get_db
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import CaptureBatch
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import bind_projection
from airweave.domains.entities.canonical.tests.test_whatsapp_recovery import (
    MEDIA,
    connector,
    message,
    responder,
    run,
)
from airweave.domains.storage.file_service import FileService
from airweave.models.entity import Entity


async def test_whatsapp_retained_http_read_download_and_tenant_denial(database, source, tmp_path):
    service, fence = source
    await bind_projection(database, fence, source_name="whatsapp")
    storage = FilesystemBackend(tmp_path)
    files = FileService(uuid4(), storage, sync_id=fence.sync_id)
    calls = []
    native_response = responder(calls)

    def respond(request):
        if request.url.path.endswith("/messages/m1"):
            calls.append((request.url.path, None))
            return httpx.Response(200, json=message("m1", changed=True))
        return native_response(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as upstream:
        await run(database, service, fence, upstream, files)
    # Provider client is now closed; HTTP consumer only sees persisted originals.
    acquired_calls = len(calls)
    async with database() as db:
        record = await db.scalar(
            select(Entity).where(
                Entity.entity_definition_short_name == "whatsapp_message", Entity.native_id == "m1"
            )
        )
        record_id = record.id
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)
    owner = fence.organization_id

    async def session():
        async with database() as db:
            yield db

    async def context():
        return SimpleNamespace(organization=SimpleNamespace(id=owner))

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_context] = context
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(storage_backend=storage)
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing-secret"
    )
    path = f"/sync/{fence.sync_id}/records/{record_id}"
    blob_path = f"{path}/blobs/{sha256(MEDIA).hexdigest()}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as consumer:
        response = await consumer.get(path)
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["identity"]["native_id"] == "m1"
        assert payload["identity"]["container_id"] == "group@lid"
        assert payload["revision"] == 1
        assert payload["payload"]["text"] == "Original مرحباً"
        assert payload["payload"]["native_unknown"] == {"retained": [None, 3]}
        download = await consumer.get(blob_path, params={"revision": payload["revision"]})
        assert download.status_code == 200, download.text
        assert download.content == MEDIA
        assert download.headers["cache-control"] == "private, no-store"
        assert download.headers["x-content-type-options"] == "nosniff"
        assert len(calls) == acquired_calls
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as upstream:
            queries = CanonicalQueryService(
                CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing-secret"
            )
            async with database() as db:
                chat = await db.scalar(
                    select(Entity).where(Entity.entity_definition_short_name == "whatsapp_chat")
                )
                parent = await queries.read(db, fence.organization_id, fence.sync_id, chat.id)
                original = await db.scalar(
                    select(Entity).where(
                        Entity.entity_definition_short_name == "whatsapp_message",
                        Entity.native_id == "m1",
                    )
                )
                assert original.record_revision == 1
            capture = connector(upstream)
            await capture.validate()  # Current caller-attested run, once before exact acquisition.
            edited = await capture.acquire_message_refresh(
                event_account_id="acc_bound",
                event_chat_id="group@lid",
                event_message_id="m1",
                parent=parent,
                files=files,
            )
            async with database() as db:
                await service.capture(db, CaptureBatch(fence=fence, records=(edited,)))
        acquired_calls = len(calls)
        # Edited original and media remain readable after the exact-fetch client closes.
        response = await consumer.get(path)
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["revision"] == 2
        assert payload["payload"] == message("m1", changed=True)
        download = await consumer.get(blob_path, params={"revision": payload["revision"]})
        assert download.status_code == 200 and download.content == MEDIA
        owner = uuid4()
        for url, params in [(path, {}), (blob_path, {"revision": payload["revision"]})]:
            denied = await consumer.get(url, params=params)
            assert denied.status_code == 404
            assert "Original" not in denied.text and "canonical/" not in denied.text
    assert len(calls) == acquired_calls
