"""Durable native terminal outcomes and fencing through actual HTTP/SQL."""

# ruff: noqa: F811
from uuid import uuid4

from sqlalchemy import select

from airweave.domains.native_ingestion.tests.test_import_api import native_api  # noqa: F401
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob


async def test_completion_retry_terminal_scope_read_and_new_import(database, native_api):
    client, ctx, source, _ = native_api
    base = f"/native/sources/{source.source_connection_id}/imports/first"
    body = {"snapshot_id": "snapshot", "coverage": "bounded"}
    await client.put(base, json=body)
    assert (await client.post(base + "/complete")).status_code == 409
    scope = {"record_type": "knowledge"}
    opened = (await client.put(base + "/scopes", json={"scope": scope})).json()
    page = {
        "scope": scope,
        "page_id": str(uuid4()),
        "expected": opened["version"],
        "snapshots": [],
        "final": True,
    }
    ack = (await client.put(base + "/pages", json=page)).json()
    await client.post(base + "/scopes/reconcile", json={"scope": scope, "expected": ack["version"]})
    done = await client.post(base + "/complete")
    assert done.status_code == 200, done.text
    summary = done.json()["summary"]
    assert summary["capture_complete"] and summary["indexing"] == "not_verified"
    assert summary["coverage"] == "bounded" and summary["completed_scopes"] == 1
    assert (await client.post(base + "/complete")).json() == done.json()
    assert (await client.post(base + "/scopes/read", json={"scope": scope})).status_code == 200
    assert (await client.put(base + "/pages", json=page)).status_code == 409
    newer = base.replace("/first", "/next")
    await client.put(newer, json=body)
    await client.put(newer + "/scopes", json={"scope": scope})
    assert (await client.post(base + "/scopes/read", json={"scope": scope})).status_code == 409
    assert (await client.post(base + "/complete")).json() == done.json()
    assert (await client.get(base)).json() == done.json()
    assert (await client.post(base + "/cancel")).json() == done.json()
    async with database() as db:
        sync = await db.get(Sync, source.sync_id)
        assert (await db.get(SyncJob, sync.writer_job_id)).status == "running"
    ctx.organization.id = uuid4()
    assert (await client.post(base + "/complete")).status_code == 404
    assert (await client.post(newer + "/cancel")).status_code == 404


async def test_cancel_retries_do_not_cancel_new_writer(native_api, database):
    client, _, source, _ = native_api
    base = f"/native/sources/{source.source_connection_id}/imports/cancelled"
    body = {"snapshot_id": "snapshot", "coverage": "complete"}
    await client.put(base, json=body)
    scope = {"record_type": "knowledge"}
    opened = (await client.put(base + "/scopes", json={"scope": scope})).json()
    cancelled = await client.post(base + "/cancel")
    assert cancelled.status_code == 200
    assert not cancelled.json()["summary"]["capture_complete"]
    assert (await client.post(base + "/complete")).status_code == 409
    assert (
        await client.put(
            base + "/pages",
            json={
                "scope": scope,
                "page_id": str(uuid4()),
                "expected": opened["version"],
                "snapshots": [],
                "final": True,
            },
        )
    ).status_code == 409
    newer = base.replace("/cancelled", "/next")
    started = await client.put(newer, json=body)
    assert started.status_code == 200
    assert (await client.post(base + "/cancel")).json() == cancelled.json()
    async with database() as db:
        job = await db.scalar(select(SyncJob).where(SyncJob.id == started.json()["import_id"]))
        assert job.status == "running"
        assert (await db.get(Sync, source.sync_id)).writer_job_id == job.id


async def test_terminal_summary_failure_rolls_back_cycle_completion(
    native_api, database, monkeypatch
):
    import airweave.domains.native_ingestion.import_store as module

    client, _, source, _ = native_api
    base = f"/native/sources/{source.source_connection_id}/imports/rollback"
    await client.put(base, json={"snapshot_id": "snapshot", "coverage": "bounded"})
    scope = {"record_type": "knowledge"}
    opened = (await client.put(base + "/scopes", json={"scope": scope})).json()
    page = (
        await client.put(
            base + "/pages",
            json={
                "scope": scope,
                "page_id": str(uuid4()),
                "expected": opened["version"],
                "snapshots": [],
                "final": True,
            },
        )
    ).json()
    await client.post(
        base + "/scopes/reconcile", json={"scope": scope, "expected": page["version"]}
    )
    original = module.NativeImportSummary

    def fail(**kwargs):
        raise RuntimeError("synthetic summary persistence failure")

    monkeypatch.setattr(module, "NativeImportSummary", fail)
    import pytest

    with pytest.raises(RuntimeError, match="summary persistence"):
        await client.post(base + "/complete")
    assert (await client.get(base)).json()["status"] == "running"
    monkeypatch.setattr(module, "NativeImportSummary", original)
    # Successful retry also proves the cursor was not left complete by the failed transaction.
    assert (await client.post(base + "/complete")).status_code == 200
