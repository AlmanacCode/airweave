"""HTTP scope/page/reconcile flow using actual retained records and scan transactions."""

# Imported fixtures are resolved by pytest parameter name.
# ruff: noqa: F811

from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.native_ingestion.models import SessionVersion
from airweave.domains.native_ingestion.tests.test_import_api import native_api  # noqa: F401
from airweave.domains.native_ingestion.tests.test_ingestion import snapshot
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob


@pytest.mark.parametrize("coverage", ["bounded", "complete"])
async def test_http_scope_pages_retry_read_and_reconcile(database, native_api, coverage):
    client, _, source, logs = native_api
    base = f"/native/sources/{source.source_connection_id}/imports/scope-key"
    started = await client.put(base, json={"snapshot_id": "snapshot", "coverage": coverage})
    assert started.status_code == 200
    scope = {"record_type": "knowledge"}
    assert (await client.post(base + "/scopes/read", json={"scope": scope})).status_code == 409
    opened = await client.put(base + "/scopes", json={"scope": scope})
    assert opened.status_code == 200, opened.text
    assert (await client.put(base + "/scopes", json={"scope": scope})).json() == opened.json()
    assert (
        await client.post(
            base + "/scopes/reconcile",
            json={
                "scope": scope,
                "expected": opened.json()["version"],
            },
        )
    ).status_code == 409
    original = snapshot(owner_id="owner", original={"body": "private-native-content नमस्ते"})
    body = {
        "page_id": str(uuid4()),
        "scope": scope,
        "expected": opened.json()["version"],
        "snapshots": [original.model_dump(mode="json")],
        "cursor": {"position": 1},
        "final": True,
    }
    committed = await client.put(base + "/pages", json=body)
    assert committed.status_code == 200, committed.text
    assert (await client.put(base + "/pages", json=body)).json() == committed.json()
    read = await client.post(base + "/scopes/read", json={"scope": scope})
    assert read.json()["last_page"] == committed.json()
    assert read.json()["cursor"] == {"position": 1}
    assert "digest" not in read.text and "fence" not in read.text
    reconciled = await client.post(
        base + "/scopes/reconcile",
        json={
            "scope": scope,
            "expected": committed.json()["version"],
        },
    )
    assert reconciled.status_code == 200, reconciled.text
    assert reconciled.json()["phase"] == "complete"
    assert reconciled.json()["coverage"] == coverage
    assert (await client.put(base + "/pages", json=body)).status_code == 409
    assert (
        await client.post(base + "/scopes/read", json={"scope": scope})
    ).json() == reconciled.json()
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == source.sync_id))
        assert row.record_revision == 1
        assert row.source_payload["original"] == original.original
    assert "scope-key" not in str(logs) and "private-native-content" not in str(logs)


async def test_scope_and_page_reject_session_foreign_scope_and_client_fence(native_api):
    client, ctx, source, _ = native_api
    base = f"/native/sources/{source.source_connection_id}/imports/scope-key"
    await client.put(base, json={"snapshot_id": "snapshot", "coverage": "bounded"})
    ctx.is_api_key_auth = False
    assert (
        await client.put(base + "/scopes", json={"scope": {"record_type": "knowledge"}})
    ).status_code == 403
    ctx.is_api_key_auth = True
    assert (
        await client.put(base + "/scopes", json={"scope": {"record_type": "email"}})
    ).status_code == 409
    scope = await client.put(base + "/scopes", json={"scope": {"record_type": "knowledge"}})
    body = {
        "scope": {"record_type": "knowledge"},
        "page_id": str(uuid4()),
        "expected": scope.json()["version"],
        "snapshots": [],
        "final": True,
        "fence": {"job_id": str(uuid4())},
    }
    assert (await client.put(base + "/pages", json=body)).status_code == 422
    body.pop("fence")
    assert (await client.put(base + "/pages", json=body)).status_code == 200
    ctx.organization.id = uuid4()
    assert (
        await client.post(base + "/scopes/read", json={"scope": {"record_type": "knowledge"}})
    ).status_code == 404


@pytest.mark.parametrize("native_api", ["sessions"], indirect=True)
async def test_sessions_http_parent_and_child_scopes_reused_by_new_import(database, native_api):
    client, _, source, _ = native_api
    parent = snapshot(
        owner_id="owner",
        identity=RecordIdentity(record_type="session", native_id="s"),
        version=SessionVersion(revision=1, content_revision=1),
    )
    child = snapshot(
        owner_id="owner",
        identity=RecordIdentity(record_type="message", native_id="m", container_id="s"),
        parent=parent.identity,
        version=parent.version,
    )
    root_scope = {"record_type": "session"}
    child_scope = {
        "record_type": "message",
        "container_id": "s",
        "parent": parent.identity.model_dump(mode="json"),
    }

    async def upload(base, scope, item):
        opened = await client.put(base + "/scopes", json={"scope": scope})
        assert opened.status_code == 200, opened.text
        captured = await client.put(
            base + "/pages",
            json={
                "page_id": str(uuid4()),
                "scope": scope,
                "expected": opened.json()["version"],
                "snapshots": [item.model_dump(mode="json")],
                "final": True,
            },
        )
        assert captured.status_code == 200, captured.text
        reconciled = await client.post(
            base + "/scopes/reconcile",
            json={
                "scope": scope,
                "expected": captured.json()["version"],
            },
        )
        assert reconciled.status_code == 200, reconciled.text
        return captured.json()

    base = f"/native/sources/{source.source_connection_id}/imports/first"
    started = await client.put(base, json={"snapshot_id": "one", "coverage": "bounded"})
    assert (await client.put(base + "/scopes", json={"scope": child_scope})).status_code == 409
    await upload(base, root_scope, parent)
    old_child = await upload(base, child_scope, child)
    # Simulate external cancellation; dedicated cancellation HTTP is not implemented yet.
    async with database() as db:
        job = await db.get(SyncJob, started.json()["import_id"])
        job.status = "cancelled"
        await db.commit()
    later = base.replace("/first", "/second")
    assert (
        await client.put(later, json={"snapshot_id": "two", "coverage": "bounded"})
    ).status_code == 200
    await upload(later, root_scope, parent)
    new_child = await upload(later, child_scope, child)
    assert new_child["version"]["sweep_id"] != old_child["version"]["sweep_id"]
    assert new_child["changed"] == 0 and new_child["unchanged"] == 1
