"""Stored Docs HTTP reads use real SQL visibility and verified synthetic blob bytes."""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from airweave.api import deps
from airweave.api.v1.endpoints.records import record_error_response, router
from airweave.db.session import get_db
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.helpers import bind_projection, capture, observation
from airweave.domains.storage.exceptions import StorageNotFoundError


def document_capture(sync_id, variant):
    contents = {}

    def blob(value, role=None):
        content = json.dumps(value).encode()
        digest = hashlib.sha256(content).hexdigest()
        key = f"canonical/{sync_id}/blobs/sha256/{digest}"
        contents[key] = content
        return BlobReference(
            key=key,
            sha256=digest,
            size_bytes=len(content),
            media_type="application/json",
            role=role,
        )

    document = {
        "documentId": "wrong-file" if variant == "wrong_file" else "doc",
        "title": "Retained document",
        "futureField": {"preserve": [1, True]},
        "tabs": [{"tabProperties": {"tabId": "tab"}, "documentTab": {"body": {"content": []}}}],
    }
    native = blob(document)
    manifest = blob(
        {
            "file_id": "doc",
            "drive_version": "42",
            "export": {"status": "unavailable", "reason": "unsupported"},
            "native": {
                "status": "unavailable"
                if variant == "native_unavailable"
                else ("partial" if variant == "native_partial" else "complete"),
                "document_blob": None if variant == "native_unavailable" else native.sha256,
                "embedded_media": "not_retained",
                "missing": [{"reason": "capture_budget"}]
                if variant in {"native_partial", "native_unavailable"}
                else [],
            },
        },
        "representation_manifest",
    )
    root = observation("folder", identity=RecordIdentity(record_type="folder", native_id="folder"))
    record = observation(
        "doc",
        identity=RecordIdentity(record_type="file", native_id="doc"),
        parent=root.identity,
        payload={"id": "doc", "version": "42", "mimeType": "application/vnd.google-apps.document"},
        blobs=(native,) if variant == "missing_manifest" else (native, manifest),
        completeness="partial",
    )
    return root, record, contents, document


def document_storage(database, service, fence, root, document_record, contents, variant):
    mutated = False

    async def read_bytes(key, *, max_bytes):
        nonlocal mutated
        if variant == "missing_bytes":
            raise StorageNotFoundError("private-storage-key")
        if variant == "corrupt_bytes":
            return b"corrupt private content"
        if not mutated:
            mutated = True
            update = None
            if variant == "changed_during_read":
                update = document_record.model_copy(
                    update={"payload": {**document_record.payload, "version": "43"}}
                )
            elif variant == "deleted_during_read":
                update = document_record.model_copy(
                    update={"kind": "delete", "removal_reason": "provider_deleted"}
                )
            elif variant == "ancestor_revoked_during_read":
                update = root.model_copy(
                    update={"kind": "delete", "removal_reason": "access_revoked"}
                )
            if update is not None:
                await capture(database, service, fence, update)
        assert len(contents[key]) <= max_bytes
        return contents[key]

    storage = AsyncMock()
    storage.read_file.side_effect = read_bytes
    return storage


@pytest.mark.parametrize(
    "variant,status,code",
    [
        ("available", 200, None),
        ("missing_manifest", 409, "document_unavailable"),
        ("wrong_file", 409, "document_unavailable"),
        ("native_unavailable", 409, "document_unavailable"),
        ("native_partial", 409, "document_incomplete"),
        ("missing_bytes", 503, "blob_unavailable"),
        ("corrupt_bytes", 503, "blob_unavailable"),
        ("changed_during_read", 409, "stale_record_revision"),
        ("deleted_during_read", 404, "record_not_found"),
        ("ancestor_revoked_during_read", 404, "record_not_found"),
        ("revoked_before_read", 404, "record_not_found"),
        ("other_tenant", 404, "record_not_found"),
        ("stale_revision", 409, "stale_record_revision"),
        ("missing_revision", 422, None),
    ],
)
async def test_document_read_authorization_integrity_and_native_contract(
    database, source, variant, status, code
):
    service, fence = source
    await bind_projection(database, fence)
    root, document_record, contents, document = document_capture(fence.sync_id, variant)
    saved = await capture(database, service, fence, root, document_record)
    record_id = saved.changes[-1].record.id
    if variant == "revoked_before_read":
        await capture(
            database,
            service,
            fence,
            root.model_copy(update={"kind": "delete", "removal_reason": "access_revoked"}),
        )
    storage = document_storage(database, service, fence, root, document_record, contents, variant)
    app = FastAPI()
    app.include_router(router, prefix="/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def session():
        async with database() as db:
            yield db

    owner = uuid4() if variant == "other_tenant" else fence.organization_id
    app.dependency_overrides[deps.get_owned_context] = lambda: SimpleNamespace(
        organization=SimpleNamespace(id=owner)
    )
    app.dependency_overrides[get_db] = session
    app.dependency_overrides[deps.get_tenant_db] = session
    app.dependency_overrides[deps.get_container] = lambda: SimpleNamespace(storage_backend=storage)
    app.dependency_overrides[deps.get_canonical_query_service] = lambda: CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "fixture"
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            f"/sync/{fence.sync_id}/records/{record_id}/document",
            params={}
            if variant == "missing_revision"
            else {"revision": 2 if variant == "stale_revision" else 1},
        )
    assert response.status_code == status, response.text
    if status == 200:
        result = response.json()
        assert result["document"] == document
        assert result["id"] == str(record_id)
        assert result["identity"]["native_id"] == "doc"
        assert result["revision"] == 1 and result["observed_at"]
        assert result["completeness"] == "partial"
        assert result["manifest"]["native"]["status"] == "complete"
        assert response.headers["cache-control"] == "private, no-store"
        assert "key" not in result and "blobs" not in result
    elif code:
        assert response.json()["error"]["code"] == code
        assert "private" not in response.text
        assert "documentId" not in response.text
    assert "canonical/" not in response.text
    if variant in {"other_tenant", "revoked_before_read", "stale_revision", "missing_revision"}:
        storage.read_file.assert_not_awaited()
