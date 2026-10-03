"""Real SQL Notes delta replacement fences replay without exhaustive removals."""

from uuid import uuid4

import pytest
from sqlalchemy import select

from airweave.domains.device_ingestion.models import CommitDevicePage, DeviceObservation
from airweave.domains.device_ingestion.store import DeviceAdmissionError
from airweave.domains.device_ingestion.tests.test_admission import (
    bound,  # noqa: F401
    note,
    service,
    setup_kind,
)
from airweave.domains.device_ingestion.tests.test_contacts_snapshot import finish, start
from airweave.domains.entities.canonical.store import StaleWriter
from airweave.models.entity import Entity
from airweave.models.sync_job import SyncJob


async def page(database, state, publisher, key, version, observations, *, final=False):
    request = CommitDevicePage(
        **publisher.model_dump(),
        page_id=uuid4(),
        expected=version,
        observations=tuple(observations),
        final=final,
    )
    raw = request.model_dump_json().encode()
    async with database() as db:
        ack = await service().page(db, state.organization_id, state.source_id, key, raw)
    return raw, ack


def observed(native_id):
    original = note()
    original["note"]["fields"]["ZIDENTIFIER"] = {"text": {"_0": native_id}}
    return DeviceObservation(native_id=native_id, original=original)


async def test_notes_replacement_lost_receipts_fence_and_preserve_unseen(database, bound):  # noqa: F811
    state, principal, old = await setup_kind(database, bound[0], "apple_notes")
    raw, _ = await page(
        database,
        state,
        principal,
        "run-one",
        old.version,
        [observed("locked-later"), observed("unaffected")],
    )
    # Simulate lost page receipt: local exact pending bytes still name the old writer.
    replacement = await start(database, state, principal, "recover", replaces_run_id=old.run_id)
    assert replacement.acquisition_mode == "delta"
    assert replacement.version.sweep_id != old.version.sweep_id
    # Lost replacement receipt retries exactly, never rotates the writer again.
    assert (
        await start(database, state, principal, "recover", replaces_run_id=old.run_id)
        == replacement
    )
    async with database() as db:
        assert (await db.get(SyncJob, old.run_id)).status == "cancelled"
        with pytest.raises(StaleWriter):
            await service().page(db, state.organization_id, state.source_id, "run-one", raw)
    locked = note(locked=True)
    locked["note"]["fields"]["ZIDENTIFIER"] = {"text": {"_0": "locked-later"}}
    await page(
        database,
        state,
        principal,
        "recover",
        replacement.version,
        [
            DeviceObservation(
                native_id="locked-later",
                original=locked,
                kind="delete",
                removal_reason="access_revoked",
            )
        ],
        final=True,
    )
    completed = await finish(database, state, principal, "recover")
    assert completed.status == "completed"
    async with database() as db:
        records = {
            r.native_id: r
            for r in await db.scalars(select(Entity).where(Entity.sync_id == state.sync_id))
        }
        assert records["locked-later"].deleted_at is not None
        assert records["unaffected"].deleted_at is None


async def test_replacement_source_and_principal_boundaries(database, bound):  # noqa: F811
    messages, publisher, current, _, _ = bound
    with pytest.raises(DeviceAdmissionError, match="Replacement requires"):
        await start(database, messages, publisher, "reject", replaces_run_id=current.run_id)
    contacts, publisher, current = await setup_kind(database, messages, "apple_contacts")
    with pytest.raises(DeviceAdmissionError, match="Replacement requires"):
        await start(database, contacts, publisher, "reject", replaces_run_id=current.run_id)
    notes, publisher, current = await setup_kind(database, messages, "apple_notes")
    for changed in [
        {"owner_id": "other"},
        {"device_id": uuid4()},
        {"store_generation": uuid4()},
        {"generation": publisher.generation + 1},
    ]:
        with pytest.raises(DeviceAdmissionError):
            await start(
                database,
                notes,
                publisher.model_copy(update=changed),
                "reject",
                replaces_run_id=current.run_id,
            )
    with pytest.raises(DeviceAdmissionError, match="current writer"):
        await start(database, notes, publisher, "reject", replaces_run_id=uuid4())


async def test_completed_notes_run_uses_new_delta_and_fences_old_receipt(database, bound):  # noqa: F811
    state, principal, old = await setup_kind(database, bound[0], "apple_notes")
    raw, ack = await page(
        database,
        state,
        principal,
        "run-one",
        old.version,
        [observed("locked-later"), observed("unaffected")],
        final=True,
    )
    # Server completion can precede local receipt persistence; no pending bytes are rewritten.
    completed = await finish(database, state, principal, "run-one")
    assert completed.status == "completed"
    with pytest.raises(DeviceAdmissionError, match="interrupted active run"):
        await start(database, state, principal, "recover", replaces_run_id=old.run_id)
    fresh = await start(database, state, principal, "ordinary-next")
    assert fresh.acquisition_mode == "delta"
    async with database() as db:
        assert (await db.get(SyncJob, old.run_id)).status == "completed"
        # Even exact previously acknowledged bytes cannot bypass a newer writer fence.
        with pytest.raises(StaleWriter):
            await service().page(db, state.organization_id, state.source_id, "run-one", raw)
    locked = note(locked=True)
    locked["note"]["fields"]["ZIDENTIFIER"] = {"text": {"_0": "locked-later"}}
    await page(
        database,
        state,
        principal,
        "ordinary-next",
        fresh.version,
        [
            DeviceObservation(
                native_id="locked-later",
                original=locked,
                kind="delete",
                removal_reason="access_revoked",
            )
        ],
        final=True,
    )
    assert (await finish(database, state, principal, "ordinary-next")).status == "completed"
    async with database() as db:
        records = {
            r.native_id: r
            for r in await db.scalars(select(Entity).where(Entity.sync_id == state.sync_id))
        }
        assert records["locked-later"].deleted_at is not None
        assert records["unaffected"].deleted_at is None
