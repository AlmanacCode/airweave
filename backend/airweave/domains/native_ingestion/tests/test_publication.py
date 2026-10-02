"""Publisher restart, bounded metadata and authority through actual HTTP and PostgreSQL."""

# ruff: noqa: F811
from uuid import uuid4

import pytest
from sqlalchemy import event, select

from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.native_ingestion.models import SessionVersion
from airweave.domains.native_ingestion.tests.test_access_api import capture, row_id
from airweave.domains.native_ingestion.tests.test_import_api import native_api  # noqa: F401
from airweave.domains.native_ingestion.tests.test_ingestion import snapshot
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob


async def test_terminal_ack_restart_recovers_source_position_without_local_checkpoint(
    database, native_api
):
    client, _, source, _ = native_api
    source_path = f"/native/sources/{source.source_connection_id}"
    publication = source_path + "/publication?owner_id=owner"
    assert (await client.get(publication)).json()["current"] is None
    cursor = {"version": 1, "before": {"after_id": None}, "after": {"after_id": "one"}}
    body = {"snapshot_id": "snapshot", "coverage": "bounded", "publisher_cursor": cursor}
    base = source_path + "/imports/restart"
    started = await client.put(base, json=body)
    assert started.status_code == 200, started.text
    assert (await client.get(publication)).json()["current"] == started.json()
    await capture(client, base, snapshot(owner_id="owner"))
    # The completion transaction commits remotely. Simulate loss of its response
    # and local process state by recovering solely from a fresh current read.
    completed = await client.post(base + "/complete")
    assert completed.status_code == 200, completed.text
    recovered = (await client.get(publication)).json()["current"]
    assert recovered == completed.json()
    assert recovered["request"]["publisher_cursor"] == cursor
    assert recovered["status"] == "completed" and recovered["summary"]["capture_complete"]
    assert "fence" not in recovered
    assert (await client.put(base, json=body)).json() == recovered
    changed = {**body, "publisher_cursor": {**cursor, "after": {"after_id": "two"}}}
    assert (await client.put(base, json=changed)).status_code == 409
    async with database() as db:
        assert (await db.get(Sync, source.sync_id)).writer_epoch == 1
    next_body = {**body, "publisher_cursor": {"before": cursor["after"], "after": None}}
    next_base = source_path + "/imports/next"
    next_started = await client.put(next_base, json=next_body)
    assert (await client.get(publication)).json()["current"] == next_started.json()
    cancelled = await client.post(next_base + "/cancel")
    assert (await client.get(publication)).json()["current"] == cancelled.json()
    assert not cancelled.json()["summary"]["capture_complete"]


async def test_cursor_bounds_legacy_null_and_unknown_receipts_fail_closed(database, native_api):
    client, _, source, _ = native_api
    source_path = f"/native/sources/{source.source_connection_id}"
    base = source_path + "/imports/receipt"
    body = {"snapshot_id": "snapshot", "coverage": "bounded"}
    assert (
        await client.put(base, json={**body, "publisher_cursor": {"x": "x" * 32768}})
    ).status_code == 422
    assert (
        await client.put(base, json={**body, "publisher_cursor": ["not-an-object"]})
    ).status_code == 422
    started = await client.put(base, json=body)
    async with database() as db:
        job = await db.get(SyncJob, started.json()["import_id"])
        retained = job.sync_metadata
        legacy_request = dict(retained["request"])
        legacy_request.pop("publisher_cursor")
        job.sync_metadata = {**retained, "request": legacy_request}
        await db.commit()
    publication = source_path + "/publication?owner_id=owner"
    assert (await client.get(publication)).json()["current"]["request"]["publisher_cursor"] is None
    for malformed in (
        {"foreign_worker_state": True},
        {**retained, "schema_version": 99},
        {**retained, "source_id": str(uuid4())},
        {**retained, "request_key": "another-key"},
        {**retained, "fence": {**retained["fence"], "epoch": 999}},
    ):
        async with database() as db:
            job = await db.get(SyncJob, started.json()["import_id"])
            job.sync_metadata = malformed
            await db.commit()
        response = await client.get(publication)
        assert response.status_code == 409, response.text
        assert response.json()["detail"]["code"] == "native_admission_failed"
    async with database() as db:
        sync = await db.get(Sync, source.sync_id)
        sync.writer_job_id = uuid4()
        await db.commit()
    assert (await client.get(publication)).status_code == 409


async def test_inventory_traverses_over_200_without_original_or_projection_reads(
    database, native_api
):
    client, _, source, _ = native_api
    source_path = f"/native/sources/{source.source_connection_id}"
    base = source_path + "/imports/inventory"
    await client.put(base, json={"snapshot_id": "batch", "coverage": "bounded"})
    scope = {"record_type": "knowledge"}
    opened = (await client.put(base + "/scopes", json={"scope": scope})).json()
    originals = [
        snapshot(
            f"object-{index}",
            owner_id="owner",
            revision=index + 1,
            original={"body": "PRIVATE BODY" * 1000},
        )
        for index in range(205)
    ]
    result = await client.put(
        base + "/pages",
        json={
            "page_id": str(uuid4()),
            "scope": scope,
            "expected": opened["version"],
            "snapshots": [item.model_dump(mode="json") for item in originals],
            "final": True,
        },
    )
    assert result.status_code == 200, result.text
    statements = []

    def observed(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    engine = database.kw["bind"].sync_engine
    event.listen(engine, "before_cursor_execute", observed)
    records = []
    after = None
    try:
        while True:
            params = {"owner_id": "owner", "limit": 100}
            if after:
                params["after"] = after
            response = await client.get(source_path + "/records", params=params)
            assert response.status_code == 200, response.text
            assert "PRIVATE BODY" not in response.text
            page = response.json()
            assert page["consistency"] == "live" and page["order"] == "id_asc"
            assert len(page["records"]) <= 100
            records.extend(page["records"])
            after = page["next_after"]
            assert page["has_more"] == (after is not None)
            if not after:
                break
    finally:
        event.remove(engine, "before_cursor_execute", observed)
    assert len(records) == 205 and len({item["record_id"] for item in records}) == 205
    assert [item["record_id"] for item in records] == sorted(item["record_id"] for item in records)
    assert {item["version"]["revision"] for item in records} == set(range(1, 206))
    assert all(
        set(item)
        == {
            "record_id",
            "identity",
            "revision",
            "parent_visibility_epoch",
            "available",
            "removal_reason",
            "version",
        }
        for item in records
    )
    inventory_sql = [item for item in statements if "jsonb_build_object" in item]
    assert len(inventory_sql) == 3
    assert all(
        "original" not in item
        and "projection_generation" not in item
        and "text_body" not in item
        and "blob" not in item
        for item in inventory_sql
    )


@pytest.mark.parametrize("native_api", ["sessions"], indirect=True)
async def test_inventory_reports_parent_epoch_and_withdrawal_without_restoring_children(
    database, native_api
):
    client, _, source, _ = native_api
    source_path = f"/native/sources/{source.source_connection_id}"
    base = source_path + "/imports/parent"
    await client.put(base, json={"snapshot_id": "batch", "coverage": "bounded"})
    parent = snapshot(
        owner_id="owner",
        identity=RecordIdentity(record_type="session", native_id="s"),
        version=SessionVersion(revision=2, content_revision=7),
    )
    child = snapshot(
        owner_id="owner",
        identity=RecordIdentity(record_type="message", native_id="m", container_id="s"),
        parent=parent.identity,
        version=parent.version,
    )
    await capture(client, base, parent)
    await capture(client, base, child)
    parent_url = base + f"/records/{await row_id(database, source, 'session')}/access"
    withdrawn = await client.post(
        parent_url, json={"action": "withdraw", "expected_revision": 1, "reason": "scope_removed"}
    )
    assert withdrawn.status_code == 200
    page = (await client.get(source_path + "/records", params={"owner_id": "owner"})).json()
    assert len(page["records"]) == 2 and not any(item["available"] for item in page["records"])
    restored = await client.post(
        parent_url,
        json={
            "action": "renew",
            "expected_revision": 2,
            "snapshot": parent.model_dump(mode="json"),
        },
    )
    assert restored.status_code == 200, restored.text
    page = (await client.get(source_path + "/records", params={"owner_id": "owner"})).json()
    child_state = next(
        item for item in page["records"] if item["identity"]["record_type"] == "message"
    )
    assert child_state["parent_visibility_epoch"] == 2 and not child_state["available"]
    assert child_state["version"] == parent.version.model_dump(mode="json")


async def test_new_reads_deny_wrong_owner_org_session_and_source_withdrawal_but_not_pause(
    database, native_api
):
    client, ctx, source, _ = native_api
    source_path = f"/native/sources/{source.source_connection_id}"
    paths = [source_path + "/publication", source_path + "/records"]
    for path in paths:
        assert (await client.get(path, params={"owner_id": "other-owner"})).status_code == 404
        ctx.is_api_key_auth = False
        assert (await client.get(path, params={"owner_id": "owner"})).status_code == 403
        ctx.is_api_key_auth = True
        ctx.organization.id = uuid4()
        assert (await client.get(path, params={"owner_id": "owner"})).status_code == 404
        ctx.organization.id = source.organization_id
    async with database() as db:
        (await db.get(Sync, source.sync_id)).status = "paused"
        await db.commit()
    for path in paths:
        assert (await client.get(path, params={"owner_id": "owner"})).status_code == 200
    async with database() as db:
        (await db.get(SourceConnection, source.source_connection_id)).is_authenticated = False
        await db.commit()
    for path in paths:
        assert (await client.get(path, params={"owner_id": "owner"})).status_code == 404


@pytest.mark.parametrize("corruption", ["owner", "version", "identity"])
async def test_inventory_malformed_metadata_never_becomes_empty_success(
    database, native_api, corruption
):
    client, _, source, _ = native_api
    source_path = f"/native/sources/{source.source_connection_id}"
    base = source_path + "/imports/malformed"
    await client.put(base, json={"snapshot_id": "batch", "coverage": "bounded"})
    await capture(client, base, snapshot(owner_id="owner"))
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == source.sync_id))
        metadata = dict(row.source_payload)
        if corruption == "owner":
            metadata["owner_id"] = "different-owner"
        elif corruption == "version":
            metadata["version"] = {"kind": "record", "revision": "not-an-integer"}
        else:
            metadata["identity"] = {"record_type": "knowledge", "native_id": "another-original"}
        row.source_payload = metadata
        await db.commit()
    response = await client.get(source_path + "/records", params={"owner_id": "owner"})
    assert response.status_code == 409, response.text
