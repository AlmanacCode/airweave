"""Actual synthetic Swift original bytes traverse HTTP admission, storage and retained reads."""

import asyncio
import base64
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from airweave import schemas
from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.api import deps
from airweave.api.backend_actor import backend_owned_actor
from airweave.api.context import ApiContext
from airweave.api.v1.endpoints.device_ingestion import router as device_router
from airweave.api.v1.endpoints.records import record_error_response
from airweave.api.v1.endpoints.records import router as record_router
from airweave.core.shared_models import AuthMethod
from airweave.domains.device_ingestion.models import CommitDevicePage
from airweave.domains.device_ingestion.service import DeviceIngestion
from airweave.domains.device_ingestion.store import DeviceIngestionStore
from airweave.domains.device_ingestion.tests.test_admission import bound  # noqa: F401
from airweave.domains.entities.canonical.store import CanonicalRecordStore, CanonicalStoreError
from airweave.domains.entities.canonical.tests.test_query import query_service
from airweave.models.entity import Entity
from airweave.models.organization import Organization


@dataclass
class FixtureContainer:
    device_ingestion: DeviceIngestion
    storage_backend: FilesystemBackend


@pytest.mark.skipif(
    not os.environ.get("APPLE_NATIVE_FIXTURE"), reason="requires synthetic Swift executable"
)
async def test_messages_original_runtime_http_retained_download(database, bound, tmp_path):  # noqa: F811, C901
    binary = Path(os.environ["APPLE_NATIVE_FIXTURE"])
    assert binary.is_file()
    state, publisher, run, _, _ = bound
    storage = FilesystemBackend(tmp_path / "originals")
    ingestion = DeviceIngestion(DeviceIngestionStore(CanonicalRecordStore()), storage)
    async with database() as db:
        await ingestion.page(
            db,
            state.organization_id,
            state.source_id,
            "run-one",
            CommitDevicePage(
                **publisher.model_dump(),
                page_id=uuid4(),
                expected=run.version,
                observations=(),
                final=True,
            )
            .model_dump_json()
            .encode(),
        )
        await ingestion.complete(db, state.organization_id, state.source_id, "run-one", publisher)
        organization = await db.get(Organization, state.organization_id)
        actor = ApiContext(
            organization=schemas.Organization.model_validate(organization),
            auth_method=AuthMethod.API_KEY,
        )
    app = FastAPI()
    app.include_router(device_router, prefix="/api/v1/device/sources")
    app.include_router(record_router, prefix="/api/v1/sync")
    app.add_exception_handler(CanonicalStoreError, record_error_response)

    async def database_dependency():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_tenant_db] = database_dependency
    app.dependency_overrides[backend_owned_actor] = lambda: actor
    app.dependency_overrides[deps.get_owned_context] = lambda: actor
    app.dependency_overrides[deps.get_container] = lambda: FixtureContainer(ingestion, storage)
    app.dependency_overrides[deps.get_canonical_query_service] = query_service
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--stdio-original",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=12 * 1024 * 1024,
    )
    assert process.stdin and process.stdout

    async def command(value):
        process.stdin.write(json.dumps(value).encode() + b"\n")
        await process.stdin.drain()

    expected = bytearray([42]) * (3 * 1024 * 1024)
    expected[0], expected[-1] = 0, 255
    expected = bytes(expected)
    digest = hashlib.sha256(expected).hexdigest()
    configuration = {
        "owner_id": "owner",
        "account_id": str(uuid4()),
        "binding_id": str(state.source_id),
        "device_id": str(publisher.device_id),
        "generation": publisher.generation,
        "store_generation": str(publisher.store_generation),
        "source_kind": "imessage",
        "resume_existing_grant": True,
        "api_origin": "http://127.0.0.1:8123",
    }
    trace = []
    intent_original = None
    try:
        await command({"schema_version": 1, "type": "configure", "configuration": configuration})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://fixture"
        ) as client:
            async with asyncio.timeout(20):
                while True:
                    line = await process.stdout.readline()
                    assert line, trace
                    frame = json.loads(line)
                    trace.append((frame["type"], frame.get("state"), frame.get("operation")))
                    if frame["type"] == "status":
                        assert frame["state"] != "failed", trace
                        if frame["state"] == "complete":
                            break
                        continue
                    if frame["type"] != "request":
                        continue
                    operation, key = frame["operation"], frame["request_key"]
                    raw = base64.b64decode(frame["body_base64"], validate=True)
                    path = f"/api/v1/device/sources/{state.source_id}/runs/{key}"
                    headers = {"Content-Type": "application/json", "X-Request-Key": key}
                    if operation == "begin":
                        response = await client.put(path, content=raw, headers=headers)
                    elif operation == "upload-intent":
                        intent = json.loads(raw)
                        assert intent["sha256"] == digest and intent["size_bytes"] == len(expected)
                        assert intent["attachment_index"] == 0
                        intent_original = intent["original"]
                        assert (
                            intent_original["message"]["fields"]["date"]["integer"]["_0"]
                            == 9223372036854775807
                        )
                        assert (
                            intent_original["attachments"][0]["fields"]["filename"]["text"]["_0"]
                            == "original.bin"
                        )
                        response = await client.put(
                            path + f"/uploads/{frame['handle']}/intent",
                            content=raw,
                            headers=headers,
                        )
                    elif operation == "upload-content":
                        assert raw == expected and frame["content"] == {
                            "sha256": digest,
                            "size_bytes": len(expected),
                        }
                        response = await client.put(
                            path + f"/uploads/{frame['handle']}/content",
                            content=raw,
                            params=publisher.model_dump(mode="json"),
                            headers={
                                "Content-Type": "application/octet-stream",
                                "X-Request-Key": key,
                            },
                        )
                    elif operation == "page":
                        page = json.loads(raw)
                        assert page["observations"][0]["original"] == intent_original
                        assert len(page["observations"][0]["uploads"]) == 1
                        response = await client.put(path + "/pages", content=raw, headers=headers)
                    elif operation == "complete":
                        response = await client.post(
                            path + "/complete", content=raw, headers=headers
                        )
                    else:
                        raise AssertionError((operation, trace))
                    assert response.status_code == 200, (trace, response.text)
                    await command(
                        {
                            "schema_version": 1,
                            "type": "response",
                            "id": frame["id"],
                            "success": True,
                            "body_base64": base64.b64encode(response.content).decode(),
                        }
                    )
            async with database() as db:
                entity = await db.scalar(
                    select(Entity).where(
                        Entity.sync_id == state.sync_id, Entity.native_id == "fixture-guid"
                    )
                )
                record_id = entity.id
            path = f"/api/v1/sync/{state.sync_id}/records/{record_id}"
            read = await client.get(path)
            assert read.status_code == 200, read.text
            record = read.json()
            assert record["payload"]["original"] == intent_original
            assert (
                record["payload"]["original"]["message"]["fields"]["date"]["integer"]["_0"]
                == 9223372036854775807
            )
            assert len(record["blobs"]) == 1
            assert record["blobs"][0]["source_path"] == "/original/attachments/0"
            assert record["blobs"][0]["sha256"] == digest
            blob_path = path + f"/blobs/{digest}"
            downloaded = await client.get(blob_path, params={"revision": record["revision"]})
            assert downloaded.status_code == 200 and downloaded.content == expected
            assert hashlib.sha256(downloaded.content).hexdigest() == digest
            assert downloaded.headers["cache-control"] == "private, no-store"
            stale = await client.get(blob_path, params={"revision": record["revision"] + 1})
            assert stale.status_code == 409
            revoked = await client.post(
                f"/api/v1/device/sources/{state.source_id}/revoke",
                json={"owner_id": "owner", "expected_generation": publisher.generation},
            )
            assert revoked.status_code == 200
            withdrawn = await client.get(path)
            assert withdrawn.status_code == 200
            assert withdrawn.json()["content_access"] == "unavailable"
            assert withdrawn.json()["payload"] == {} and withdrawn.json()["blobs"] == []
            assert (
                await client.get(blob_path, params={"revision": record["revision"]})
            ).status_code == 404
    finally:
        process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), 3)
        except TimeoutError:
            process.kill()
            await process.wait()
