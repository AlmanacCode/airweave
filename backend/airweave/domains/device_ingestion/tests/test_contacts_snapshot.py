"""Disposable SQL proves full visible Contacts snapshots, retries and writer replacement."""

from uuid import uuid4

import pytest
from sqlalchemy import func, select

from airweave.domains.device_ingestion.models import (
    CommitDevicePage,
    DeviceBeginRequest,
    DeviceObservation,
    RevokeDevice,
)
from airweave.domains.device_ingestion.store import DeviceAdmissionError
from airweave.domains.device_ingestion.tests.test_admission import bound, service, setup_kind
from airweave.domains.entities.canonical.store import StaleWriter
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob


def contact(native_id):
    return {
        "schemaVersion": 1,
        "contact": {
            "nativeID": native_id,
            "namePrefix": "",
            "givenName": "Synthetic",
            "middleName": "",
            "familyName": native_id,
            "nameSuffix": "",
            "nickname": "",
            "organizationName": "",
            "phones": [],
            "emails": [],
        },
    }


async def send(database, state, publisher, key, version, ids, *, final=False):
    request = CommitDevicePage(
        **publisher.model_dump(),
        page_id=uuid4(),
        expected=version,
        observations=tuple(DeviceObservation(native_id=i, original=contact(i)) for i in ids),
        cursor={"contacts": {"enumeration_finished": True}} if final else {},
        final=final,
    )
    raw = request.model_dump_json().encode()
    async with database() as db:
        ack = await service().page(db, state.organization_id, state.source_id, key, raw)
    return request, raw, ack


async def finish(database, state, publisher, key):
    async with database() as db:
        return await service().complete(db, state.organization_id, state.source_id, key, publisher)


async def start(database, state, publisher, key, **intent):
    async with database() as db:
        return await service().start(
            db,
            state.organization_id,
            state.source_id,
            key,
            DeviceBeginRequest(**publisher.model_dump(), **intent),
        )


async def counts(database, state):
    async with database() as db:
        total = await db.scalar(
            select(func.count()).select_from(Entity).where(Entity.sync_id == state.sync_id)
        )
        removed = await db.scalar(
            select(func.count())
            .select_from(Entity)
            .where(Entity.sync_id == state.sync_id, Entity.deleted_at.is_not(None))
        )
        return total, removed


async def test_snapshot_removals_wait_for_final_page_and_reconcile_across_calls(database, bound):
    state, publisher, initial = await setup_kind(database, bound[0], "apple_contacts")
    await send(
        database,
        state,
        publisher,
        "run-one",
        initial.version,
        [f"stale-{i}" for i in range(260)],
        final=True,
    )
    done = await finish(database, state, publisher, "run-one")
    snapshot = await start(
        database, state, publisher, "reset", acquisition_mode="contacts_snapshot"
    )
    _, raw, ack = await send(database, state, publisher, "reset", snapshot.version, ["survivor"])
    async with database() as db:
        assert await service().page(db, state.organization_id, state.source_id, "reset", raw) == ack
        old = await service().get_run(
            db, state.organization_id, state.source_id, "run-one", "owner"
        )
        assert old == done
    assert await counts(database, state) == (261, 0)
    with pytest.raises(DeviceAdmissionError, match="final collected"):
        await finish(database, state, publisher, "reset")
    with pytest.raises(DeviceAdmissionError):
        await finish(
            database,
            state,
            publisher.model_copy(update={"generation": publisher.generation + 1}),
            "reset",
        )
    assert await counts(database, state) == (261, 0)
    _, _, final = await send(
        database, state, publisher, "reset", ack.acknowledgement.version, [], final=True
    )
    first = await finish(database, state, publisher, "reset")
    assert first.status == "running" and first.phase == "reconciling"
    assert first.final_page_version == final.acknowledgement.version
    assert first.acquisition_mode == "contacts_snapshot"
    assert await counts(database, state) == (261, 250)
    async with database() as db:
        job = await db.get(SyncJob, first.run_id)
        assert job.sync_metadata["completed_scan"] is None
    completed = await finish(database, state, publisher, "reset")
    assert completed.status == "completed" and completed.phase == "complete"
    assert completed.final_page_version == final.acknowledgement.version
    assert completed.version.revision == completed.final_page_version.revision + 2
    assert await counts(database, state) == (261, 260)
    assert await finish(database, state, publisher, "reset") == completed
    async with database() as db:
        reasons = (
            await db.scalars(
                select(Entity.removal_reason).where(
                    Entity.sync_id == state.sync_id, Entity.deleted_at.is_not(None)
                )
            )
        ).all()
        assert set(reasons) == {"scope_removed"}
        assert (
            await service().get_run(db, state.organization_id, state.source_id, "run-one", "owner")
            == done
        )


@pytest.mark.parametrize("old_mode", ["delta", "contacts_snapshot"])
async def test_explicit_snapshot_replacement_does_not_union_partial_seen_cards(
    database, bound, old_mode
):
    state, publisher, initial = await setup_kind(database, bound[0], "apple_contacts")
    if old_mode == "contacts_snapshot":
        await send(database, state, publisher, "run-one", initial.version, [], final=True)
        await finish(database, state, publisher, "run-one")
        initial = await start(database, state, publisher, "old-snapshot", acquisition_mode=old_mode)
        old_key = "old-snapshot"
    else:
        old_key = "run-one"
    _, old_raw, old_ack = await send(
        database, state, publisher, old_key, initial.version, ["deleted-between-enumerations"]
    )
    replacement = await start(
        database,
        state,
        publisher,
        "replacement",
        acquisition_mode="contacts_snapshot",
        replaces_run_id=initial.run_id,
    )
    assert replacement.version.sweep_id != initial.version.sweep_id
    assert await counts(database, state) == (1, 0)
    assert (
        await start(
            database,
            state,
            publisher,
            "replacement",
            acquisition_mode="contacts_snapshot",
            replaces_run_id=initial.run_id,
        )
        == replacement
    )
    with pytest.raises(DeviceAdmissionError, match="conflicting"):
        await start(
            database,
            state,
            publisher,
            "replacement",
            acquisition_mode="contacts_snapshot",
            replaces_run_id=uuid4(),
        )
    async with database() as db:
        old_job = await db.get(SyncJob, initial.run_id)
        assert old_job.status == "cancelled"
        with pytest.raises(StaleWriter):
            await service().page(db, state.organization_id, state.source_id, old_key, old_raw)
    await send(
        database, state, publisher, "replacement", replacement.version, ["visible-card"], final=True
    )
    done = await finish(database, state, publisher, "replacement")
    assert done.status == "completed" and await counts(database, state) == (2, 1)


async def test_snapshot_modes_and_replacements_cannot_cross_source_or_generation(database, bound):
    state, publisher, initial, _, _ = bound
    with pytest.raises(DeviceAdmissionError, match="Only Contacts"):
        await start(database, state, publisher, "invalid", acquisition_mode="contacts_snapshot")
    contacts, contact_publisher, current = await setup_kind(database, state, "apple_contacts")
    for bad in (
        contact_publisher.model_copy(update={"owner_id": "foreign"}),
        contact_publisher.model_copy(update={"generation": contact_publisher.generation + 1}),
        contact_publisher.model_copy(update={"device_id": uuid4()}),
    ):
        with pytest.raises(DeviceAdmissionError):
            await start(
                database,
                contacts,
                bad,
                "invalid-replacement",
                acquisition_mode="contacts_snapshot",
                replaces_run_id=current.run_id,
            )
    with pytest.raises(DeviceAdmissionError, match="current writer"):
        await start(
            database,
            contacts,
            contact_publisher,
            "invalid-replacement",
            acquisition_mode="contacts_snapshot",
            replaces_run_id=initial.run_id,
        )
    await send(database, contacts, contact_publisher, "run-one", current.version, [], final=True)
    done = await finish(database, contacts, contact_publisher, "run-one")
    async with database() as db:
        await service().revoke(
            db,
            contacts.organization_id,
            contacts.source_id,
            RevokeDevice(owner_id="owner", expected_generation=contact_publisher.generation),
        )
    with pytest.raises(DeviceAdmissionError):
        async with database() as db:
            await service().get_run(
                db, contacts.organization_id, contacts.source_id, "run-one", "owner"
            )
    assert done.status == "completed"
