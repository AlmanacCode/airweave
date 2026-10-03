"""Synthetic Swift stdio → actual FastAPI ingestion routes → disposable PostgreSQL."""

import asyncio
import base64
import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from airweave import schemas
from airweave.api import deps
from airweave.api.backend_actor import backend_owned_actor
from airweave.api.context import ApiContext
from airweave.api.v1.endpoints.device_ingestion import router
from airweave.core.shared_models import AuthMethod
from airweave.domains.device_ingestion.service import DeviceIngestion
from airweave.domains.device_ingestion.tests.test_admission import (  # noqa: F401
    bound,
    service,
    setup_kind,
)
from airweave.domains.entities.canonical.query_models import RecordListQuery
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.entities.canonical.tests.test_query import query_service
from airweave.models.entity import Entity
from airweave.models.organization import Organization
from airweave.models.sync_job import SyncJob


@dataclass
class FixtureContainer:
    device_ingestion: DeviceIngestion


@pytest.mark.skipif(
    not os.environ.get("APPLE_NATIVE_FIXTURE"), reason="requires compiled synthetic Swift fixture"
)
async def test_native_notes_lost_ack_lock_recovery_over_http(database, bound):  # noqa: F811, C901
    binary = Path(os.environ["APPLE_NATIVE_FIXTURE"])
    assert binary.is_file()
    state, publisher, _ = await setup_kind(database, bound[0], "apple_notes")
    # setup_kind creates a writer; finish its empty run before the native-owned lifecycle.
    from airweave.domains.device_ingestion.models import CommitDevicePage

    async with database() as db:
        current = await service().get_run(
            db, state.organization_id, state.source_id, "run-one", "owner"
        )
        await service().page(
            db,
            state.organization_id,
            state.source_id,
            "run-one",
            CommitDevicePage(
                **publisher.model_dump(),
                page_id=uuid4(),
                expected=current.version,
                observations=(),
                final=True,
            )
            .model_dump_json()
            .encode(),
        )
        await service().complete(db, state.organization_id, state.source_id, "run-one", publisher)
        organization = await db.get(Organization, state.organization_id)
        actor = ApiContext(
            organization=schemas.Organization.model_validate(organization),
            auth_method=AuthMethod.API_KEY,
        )
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/device/sources")

    async def database_dependency():
        async with database() as db:
            yield db

    app.dependency_overrides[deps.get_tenant_db] = database_dependency
    app.dependency_overrides[backend_owned_actor] = lambda: actor
    app.dependency_overrides[deps.get_container] = lambda: FixtureContainer(service())
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--stdio-notes",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=12 * 1024 * 1024,
    )
    assert process.stdin and process.stdout

    async def command(value):
        process.stdin.write(json.dumps(value).encode() + b"\n")
        await process.stdin.drain()

    configuration = {
        "owner_id": "owner",
        "account_id": str(uuid4()),
        "binding_id": str(state.source_id),
        "device_id": str(publisher.device_id),
        "generation": publisher.generation,
        "store_generation": str(publisher.store_generation),
        "source_kind": "apple_notes",
        "resume_existing_grant": True,
        "api_origin": "http://127.0.0.1:8123",
    }
    trace = []
    old_run = None
    old_key = None
    old_begin = None
    first_page = True
    replacement_seen = False
    withdrawal_seen = False
    try:
        await command({"schema_version": 1, "type": "configure", "configuration": configuration})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://fixture"
        ) as client:
            async with asyncio.timeout(20):
                while True:
                    line = await process.stdout.readline()
                    assert line, "native fixture exited before completion"
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
                    if operation == "begin":
                        intent = json.loads(raw)
                        if old_run and key != old_key:
                            assert intent["replaces_run_id"].lower() == old_run.lower()
                            assert intent["acquisition_mode"] == "delta"
                            replacement_seen = True
                        response = await client.put(
                            path,
                            content=raw,
                            headers={"Content-Type": "application/json", "X-Request-Key": key},
                        )
                        assert response.status_code == 200, response.text
                        if old_run is None:
                            old_run = response.json()["run_id"]
                            old_key, old_begin = key, raw
                        elif key == old_key:
                            assert raw == old_begin
                            assert response.json()["run_id"] == old_run
                    elif operation == "page":
                        items = json.loads(raw)["observations"]
                        if replacement_seen:
                            assert not any(
                                i["native_id"] == "note-1" and i["kind"] == "upsert" for i in items
                            )
                            if not withdrawal_seen:
                                locked = next(i for i in items if i["native_id"] == "note-1")
                                assert (
                                    locked["kind"] == "delete"
                                    and locked["removal_reason"] == "access_revoked"
                                )
                                assert not any(i["native_id"] == "note-101" for i in items)
                                withdrawal_seen = True
                        response = await client.put(
                            path + "/pages",
                            content=raw,
                            headers={"Content-Type": "application/json", "X-Request-Key": key},
                        )
                        assert response.status_code == 200, response.text
                        if first_page:
                            first_page = False
                            async with database() as db:
                                saved = await db.scalar(
                                    select(Entity).where(
                                        Entity.sync_id == state.sync_id,
                                        Entity.native_id == "note-1",
                                    )
                                )
                                assert saved.deleted_at is None
                                assert saved.source_payload["original"]["compressedBody"]
                            await command({"type": "fixture-lock-restart"})
                            continue  # HTTP committed; intentionally lose the native page ACK.
                    elif operation == "complete":
                        response = await client.post(
                            path + "/complete",
                            content=raw,
                            headers={"Content-Type": "application/json", "X-Request-Key": key},
                        )
                        assert response.status_code == 200, response.text
                    else:
                        raise AssertionError(f"Unexpected fixture operation {operation}")
                    trace.append(
                        (
                            operation,
                            response.status_code,
                            response.json().get("version"),
                            response.json().get("acknowledgement"),
                        )
                    )
                    await command(
                        {
                            "schema_version": 1,
                            "type": "response",
                            "id": frame["id"],
                            "success": True,
                            "body_base64": base64.b64encode(response.content).decode(),
                        }
                    )
        assert replacement_seen and withdrawal_seen
        async with database() as db:
            saved = await db.scalar(
                select(Entity).where(Entity.sync_id == state.sync_id, Entity.native_id == "note-1")
            )
            assert saved.deleted_at is not None and saved.removal_reason == "access_revoked"
            assert saved.blob_references == []
            assert "compressedBody" not in saved.source_payload["original"]
            retained = await CanonicalRecordStore().read(
                db, state.organization_id, state.sync_id, saved.id
            )
            assert (
                retained.content_access == "unavailable"
                and retained.payload == {}
                and retained.blobs == ()
            )
            assert (await db.get(SyncJob, UUID(old_run))).status == "cancelled"
            visible = await query_service().list_records(
                db, state.organization_id, state.sync_id, RecordListQuery(limit=100)
            )
            assert "note-1" not in {r.identity.native_id for r in visible.records}
            assert "note-2" in {r.identity.native_id for r in visible.records}
    finally:
        process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), 3)
        except TimeoutError:
            process.kill()
            await process.wait()
