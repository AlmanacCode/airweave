"""Explicit access lifecycle using HTTP and isolated PostgreSQL, not provider mutations."""

# Imported fixture is resolved through pytest parameters.
# ruff: noqa: F811
from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.entities.canonical.requests import RecordIdentity
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.domains.native_ingestion.errors import NativeAdmissionError
from airweave.domains.native_ingestion.import_store import NativeImportStore
from airweave.domains.native_ingestion.models import IngestNativeBatch, SessionVersion
from airweave.domains.native_ingestion.service import NativeIngestionService
from airweave.domains.native_ingestion.source_store import NativeSourceStore
from airweave.domains.native_ingestion.store import NativeIngestionStore
from airweave.domains.native_ingestion.tests.test_import_api import native_api  # noqa: F401
from airweave.domains.native_ingestion.tests.test_ingestion import NOW, snapshot
from airweave.models.entity import Entity
from airweave.models.source_connection import SourceConnection


async def capture(client, base, original):
    scope = {"record_type": original.identity.record_type}
    if original.parent:
        scope.update(
            container_id=original.identity.container_id,
            parent=original.parent.model_dump(mode="json"),
        )
    opened = await client.put(base + "/scopes", json={"scope": scope})
    assert opened.status_code == 200, opened.text
    result = await client.put(
        base + "/pages",
        json={
            "page_id": str(uuid4()),
            "scope": scope,
            "expected": opened.json()["version"],
            "snapshots": [original.model_dump(mode="json")],
            "final": True,
        },
    )
    assert result.status_code == 200, result.text
    completed = await client.post(
        base + "/scopes/reconcile",
        json={
            "scope": scope,
            "expected": result.json()["version"],
        },
    )
    assert completed.status_code == 200, completed.text


async def row_id(database, source, kind):
    async with database() as db:
        return await db.scalar(
            select(Entity.id).where(
                Entity.sync_id == source.sync_id,
                Entity.entity_definition_short_name == kind,
            )
        )


async def test_access_cas_preserves_original_and_rejects_stale_restore(database, native_api):
    client, ctx, source, logs = native_api
    base = f"/native/sources/{source.source_connection_id}/imports/access"
    await client.put(base, json={"snapshot_id": "one", "coverage": "bounded"})
    original = snapshot(owner_id="owner", revision=3)
    await capture(client, base, original)
    record_id = await row_id(database, source, "knowledge")
    url = base + f"/records/{record_id}/access"
    initial = await client.get(url)
    assert initial.json()["available"] and initial.json()["revision"] == 1
    assert "original" not in initial.text and "payload" not in initial.text
    withdraw = {"action": "withdraw", "expected_revision": 1, "reason": "access_revoked"}
    revoked = await client.post(url, json=withdraw)
    assert revoked.status_code == 200, revoked.text
    assert revoked.json()["revision"] == 2 and not revoked.json()["available"]
    # Lost response recovery reads state; old CAS never replays across newer observations.
    assert (await client.post(url, json=withdraw)).status_code == 409
    assert (await client.get(url)).json() == revoked.json()
    async with database() as db:
        retained = await db.get(Entity, record_id)
        assert retained.source_payload == original.model_dump(mode="json")
    renew = {
        "action": "renew",
        "expected_revision": 2,
        "snapshot": original.model_dump(mode="json"),
    }
    for changes in (
        {"owner_id": "wrong"},
        {"original": {"body": "same-version-conflict"}},
        {"version": {"kind": "record", "revision": 2}},
    ):
        rejected = await client.post(url, json=renew | {"snapshot": renew["snapshot"] | changes})
        assert rejected.status_code == 409, rejected.text
        assert (await client.get(url)).json() == revoked.json()
    restored = await client.post(url, json=renew)
    assert restored.status_code == 200, restored.text
    assert restored.json()["available"] and restored.json()["revision"] == 3
    revoked_again = await client.post(url, json=withdraw | {"expected_revision": 3})
    assert revoked_again.json()["revision"] == 4
    assert (await client.post(url, json=renew)).status_code == 409
    assert not (await client.get(url)).json()["available"]
    ctx.is_api_key_auth = False
    assert (await client.get(url)).status_code == 403
    ctx.is_api_key_auth = True
    ctx.organization.id = uuid4()
    assert (await client.get(url)).status_code == 404
    assert (await client.post(url, json=renew)).status_code == 404
    assert "same-version-conflict" not in str(logs)


@pytest.mark.parametrize("native_api", ["sessions"], indirect=True)
async def test_parent_restore_requires_fresh_child_attestation_and_active_import(
    database, native_api
):
    client, _, source, _ = native_api
    base = f"/native/sources/{source.source_connection_id}/imports/access"
    await client.put(base, json={"snapshot_id": "one", "coverage": "bounded"})
    parent = snapshot(
        owner_id="owner",
        identity=RecordIdentity(record_type="session", native_id="s"),
        version=SessionVersion(created_at="2026-10-01T00:00:00Z", revision=1, content_revision=1),
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
    child_url = base + f"/records/{await row_id(database, source, 'message')}/access"
    withdrawn = await client.post(
        parent_url,
        json={
            "action": "withdraw",
            "reason": "scope_removed",
            "expected_revision": 1,
        },
    )
    assert withdrawn.status_code == 200, withdrawn.text
    assert not (await client.get(child_url)).json()["available"]
    child_renew = {
        "expected_parent_epoch": 1,
        "action": "renew",
        "expected_revision": 1,
        "snapshot": child.model_dump(mode="json"),
    }
    assert (await client.post(child_url, json=child_renew)).status_code == 409
    restored = await client.post(
        parent_url,
        json={
            "action": "renew",
            "expected_revision": 2,
            "snapshot": parent.model_dump(mode="json"),
        },
    )
    assert restored.status_code == 200, restored.text
    assert not (await client.get(child_url)).json()["available"]
    # The request prepared before revocation must not cross the parent epoch change.
    assert (await client.post(child_url, json=child_renew)).status_code == 409
    parent_new = parent.model_copy(
        update={"version": SessionVersion(created_at="2026-10-01T00:00:00Z", revision=2, content_revision=1)}
    )
    parent_update = await client.post(
        parent_url,
        json={
            "action": "renew",
            "expected_revision": 3,
            "snapshot": parent_new.model_dump(mode="json"),
        },
    )
    assert parent_update.status_code == 200, parent_update.text
    child_new = child.model_copy(update={"version": parent_new.version})
    canonical = CanonicalRecordStore()
    imports = NativeImportStore(NativeSourceStore(), canonical)
    async with database() as db, UnitOfWork(db):
        saved = await imports.active(
            db, source.organization_id, source.source_connection_id, "access"
        )
    async with database() as db:
        with pytest.raises(NativeAdmissionError, match="renewal"):
            await NativeIngestionService(NativeIngestionStore(canonical)).ingest(
                db,
                IngestNativeBatch(fence=saved.fence, observed_at=NOW, snapshots=(child_new,)),
            )
    current = (await client.get(child_url)).json()
    assert current["revision"] == 1 and current["parent_visibility_epoch"] == 2
    child_renew = child_renew | {
        "expected_parent_epoch": current["parent_visibility_epoch"],
        "snapshot": child_new.model_dump(mode="json"),
    }
    refreshed = await client.post(child_url, json=child_renew)
    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["available"] and refreshed.json()["revision"] == 2
    await client.post(base + "/cancel")
    assert (await client.get(child_url)).status_code == 200
    assert (
        await client.post(child_url, json=child_renew | {"expected_revision": 2})
    ).status_code == 409
    async with database() as db:
        bound = await db.get(SourceConnection, source.source_connection_id)
        bound.is_authenticated = False
        await db.commit()
    assert not (await client.get(child_url)).json()["available"]
