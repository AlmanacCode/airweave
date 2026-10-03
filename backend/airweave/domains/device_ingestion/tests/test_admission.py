"""Real SQL proves device authority, exact bytes, shared receipts and transaction rollback."""

import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from airweave.domains.device_ingestion.models import (
    BindDevice,
    CommitDevicePage,
    DeviceObservation,
    DevicePrincipal,
    EnsureDeviceSource,
    RevokeDevice,
    device_run_id,
)
from airweave.domains.device_ingestion.service import DeviceIngestion
from airweave.domains.device_ingestion.store import (
    DeviceAdmissionError,
    DeviceIngestionStore,
    DeviceSourceNotFound,
)
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.read_authority import source_is_readable
from airweave.domains.entities.canonical.scan_store import ScanConflict
from airweave.domains.entities.canonical.store import CanonicalRecordStore, WriterBusy
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.sync import Sync
from airweave.models.sync_job import SyncJob
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


def service():
    return DeviceIngestion(DeviceIngestionStore(CanonicalRecordStore()))


def message(native_id="message-one"):
    return {
        "schemaVersion": 1,
        "guid": native_id,
        "message": {
            "rowID": 9007199254740993,
            "fields": {
                "guid": {"text": {"_0": native_id}},
                "text": {"text": {"_0": "Urdu اردو Hindi हिन्दी"}},
                "date": {"integer": {"_0": 9007199254740993}},
            },
        },
        "chats": [],
        "participants": [],
        "attachments": [],
        "chatMemberships": [],
        "bodyFidelity": {"nativeTextOnly": {}},
    }


@pytest.fixture
async def bound(database, source):
    _, fence = source
    async with database() as db:
        deployment = VectorDbDeploymentMetadata(
            dense_embedder="fake", embedding_dimensions=3, sparse_embedder="fake"
        )
        db.add(deployment)
        await db.flush()
        db.add(
            Collection(
                organization_id=fence.organization_id,
                name="Synthetic device",
                readable_id="device",
                vector_db_deployment_metadata_id=deployment.id,
            )
        )
        await db.commit()
    request = EnsureDeviceSource(
        owner_id="owner", account_id="device-store-one", source_kind="imessage", collection="device"
    )
    async with database() as db:
        state = await service().ensure(db, fence.organization_id, request)
    binding = BindDevice(
        owner_id="owner", expected_generation=0, device_id=uuid4(), store_generation=uuid4()
    )
    async with database() as db:
        state = await service().bind(db, state.organization_id, state.source_id, binding)
    publisher = DevicePrincipal(
        owner_id="owner",
        device_id=binding.device_id,
        generation=state.enrollment.generation,
        store_generation=binding.store_generation,
    )
    async with database() as db:
        run = await service().start(
            db, state.organization_id, state.source_id, "run-one", publisher
        )
    return state, publisher, run, request, binding


def page(publisher, run, *, final=False):
    return CommitDevicePage(
        **publisher.model_dump(),
        page_id=uuid4(),
        expected=run.version,
        observations=(DeviceObservation(native_id="message-one", original=message()),),
        cursor={"lastRowID": 9007199254740993},
        final=final,
    )


async def commit(database, state, request_bytes):
    async with database() as db:
        return await service().page(
            db, state.organization_id, state.source_id, "run-one", request_bytes
        )


async def test_exact_byte_ack_and_conflicting_retry_use_canonical_receipt(database, bound):
    state, publisher, run, _, _ = bound
    request = page(publisher, run)
    raw = request.model_dump_json().encode()
    first, retry = await asyncio.gather(commit(database, state, raw), commit(database, state, raw))
    assert first == retry
    assert first.sha256 == hashlib.sha256(raw).hexdigest()
    assert first.authority.binding_id == state.source_id
    assert first.authority.generation == publisher.generation
    assert first.authority.local_store_generation == publisher.store_generation
    assert first.acknowledgement.changed == 1 and first.acknowledgement.sequence == 1
    # Same parsed content but different exact-byte identity is a conflicting retry.
    different_bytes = json.dumps(json.loads(raw), indent=2, ensure_ascii=False).encode()
    with pytest.raises(ScanConflict, match="retry conflicts"):
        await commit(database, state, different_bytes)
    async with database() as db:
        record = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        assert record.source_payload["original"] == message()
        assert record.source_payload["original"]["message"]["rowID"] == 9007199254740993
        assert record.record_revision == 1
        recovered = await service().get_run(
            db, state.organization_id, state.source_id, "run-one", "owner"
        )
        assert recovered.cursor == {"lastRowID": 9007199254740993}
        assert recovered.last_page == first.acknowledgement


async def test_owner_org_device_store_generation_isolation(database, bound):
    state, publisher, run, _, _ = bound
    for replacement in [
        {"owner_id": "foreign"},
        {"device_id": uuid4()},
        {"generation": publisher.generation + 1},
        {"store_generation": uuid4()},
    ]:
        request = page(publisher, run).model_copy(update=replacement)
        with pytest.raises(DeviceAdmissionError):
            await commit(database, state, request.model_dump_json().encode())
    with pytest.raises(DeviceSourceNotFound):
        async with database() as db:
            await service().get(db, uuid4(), state.source_id, "owner")
    async with database() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == state.sync_id)
            )
            == 0
        )


async def test_binding_retry_and_primary_replacement_fence_old_publisher(database, bound):
    state, publisher, run, request, binding = bound
    async with database() as db:
        assert await service().bind(db, state.organization_id, state.source_id, binding) == state
    async with database() as db:
        assert await service().ensure(db, state.organization_id, request) == state
    replacement = BindDevice(
        owner_id="owner",
        expected_generation=publisher.generation,
        device_id=uuid4(),
        store_generation=uuid4(),
    )
    async with database() as db:
        changed = await service().bind(db, state.organization_id, state.source_id, replacement)
    assert changed.enrollment.generation == publisher.generation + 1
    with pytest.raises(DeviceAdmissionError, match="superseded"):
        await commit(database, state, page(publisher, run).model_dump_json().encode())
    async with database() as db:
        assert (
            await service().bind(db, state.organization_id, state.source_id, replacement) == changed
        )


async def test_revoke_serializes_against_locked_page_and_hides_retained_source(database, bound):
    state, publisher, run, _, _ = bound
    raw = page(publisher, run).model_dump_json().encode()
    locked = database()
    await locked.__aenter__()
    try:
        writer = DeviceIngestionStore(CanonicalRecordStore())
        await writer.require(locked, state.organization_id, state.source_id, "owner")
        revoke = RevokeDevice(owner_id="owner", expected_generation=publisher.generation)

        async def revoke_remote():
            async with database() as db:
                return await service().revoke(db, state.organization_id, state.source_id, revoke)

        task = asyncio.create_task(revoke_remote())
        await asyncio.sleep(0.03)
        assert not task.done()  # Both operations serialize through the same actual Sync lock.
        request = CommitDevicePage.model_validate_json(raw)
        ack = await writer.page(
            locked,
            state.organization_id,
            state.source_id,
            "run-one",
            request,
            hashlib.sha256(raw).hexdigest(),
        )
        await locked.commit()
        revoked = await task
        assert not revoked.retained_read_authority and not revoked.enrollment.active
        with pytest.raises(DeviceAdmissionError):
            await commit(database, state, raw)
        async with database() as db:
            assert not await db.scalar(
                select(source_is_readable(state.organization_id, state.sync_id))
            )
            assert (
                await service().revoke(db, state.organization_id, state.source_id, revoke)
                == revoked
            )
            record = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
            assert record.record_revision == 1 and ack.acknowledgement.changed == 1
    finally:
        await locked.__aexit__(None, None, None)


async def test_receipt_failure_rolls_back_capture_and_progress(database, bound, monkeypatch):
    import airweave.domains.entities.canonical.page_receipts as receipts

    state, publisher, run, _, _ = bound

    def fail(**kwargs):
        raise RuntimeError("synthetic receipt failure")

    monkeypatch.setattr(receipts, "PageReceipt", fail)
    with pytest.raises(RuntimeError, match="synthetic receipt"):
        await commit(database, state, page(publisher, run).model_dump_json().encode())
    async with database() as db:
        current = await service().get_run(
            db, state.organization_id, state.source_id, "run-one", "owner"
        )
        assert current.version == run.version and current.cursor == {} and current.last_page is None
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == state.sync_id)
            )
            == 0
        )


async def test_bounded_completion_and_new_cycle_do_not_infer_absence(database, bound):
    state, publisher, run, _, _ = bound
    async with database() as db:
        with pytest.raises(DeviceAdmissionError, match="no final"):
            await service().complete(
                db, state.organization_id, state.source_id, "run-one", publisher
            )
    with pytest.raises(WriterBusy):
        async with database() as db:
            await service().start(db, state.organization_id, state.source_id, "other", publisher)
    await commit(database, state, page(publisher, run, final=True).model_dump_json().encode())
    async with database() as db:
        done = await service().complete(
            db, state.organization_id, state.source_id, "run-one", publisher
        )
        assert (
            done.status == "completed"
            and done.phase == "complete"
            and done.indexing == "not_verified"
        )
    async with database() as db:
        assert (
            await service().complete(
                db, state.organization_id, state.source_id, "run-one", publisher
            )
            == done
        )
        newer = await service().start(
            db, state.organization_id, state.source_id, "run-two", publisher
        )
    async with database() as db:
        old = await service().get_run(
            db, state.organization_id, state.source_id, "run-one", "owner"
        )
        assert old.version == done.version and old.phase == "complete"
        assert old.final_page_version == done.final_page_version
        assert newer.version != run.version
        record = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        assert record.deleted_at is None and record.record_revision == 1


async def setup_kind(database, existing, kind):
    async with database() as db:
        state = await service().ensure(
            db,
            existing.organization_id,
            EnsureDeviceSource(
                owner_id="owner",
                account_id="device-store-one",
                source_kind=kind,
                collection="device",
            ),
        )
    binding = BindDevice(
        owner_id="owner", expected_generation=0, device_id=uuid4(), store_generation=uuid4()
    )
    async with database() as db:
        state = await service().bind(db, state.organization_id, state.source_id, binding)
        publisher = DevicePrincipal(
            owner_id="owner",
            device_id=binding.device_id,
            generation=state.enrollment.generation,
            store_generation=binding.store_generation,
        )
        run = await service().start(
            db, state.organization_id, state.source_id, "run-one", publisher
        )
    return state, publisher, run


def note(*, locked=False, marked=False):
    return {
        "schemaVersion": 1,
        "note": {
            "primaryKey": 7,
            "fields": {
                "Z_PK": {"integer": {"_0": 7}},
                "ZIDENTIFIER": {"text": {"_0": "note-one"}},
                "ZISPASSWORDPROTECTED": {"integer": {"_0": int(locked)}},
                "ZMARKEDFORDELETION": {"integer": {"_0": int(marked)}},
                "ZTITLE1": {"text": {"_0": "Synthetic note"}},
            },
        },
        "attachments": [],
        "fidelity": {"lockedBodyWithheld" if locked else "bodyUnavailable": {}},
    }


def contact_payload():
    return {
        "schemaVersion": 1,
        "contact": {
            "nativeID": "contact-one",
            "namePrefix": "",
            "givenName": "سمیر",
            "middleName": "",
            "familyName": "शर्मा",
            "nameSuffix": "",
            "nickname": "",
            "organizationName": "",
            "phones": [{"nativeLabelID": "phone-one", "rawValue": "+1 (555) 0100"}],
            "emails": [],
        },
    }


@pytest.mark.parametrize("kind", ["apple_notes", "apple_contacts"])
async def test_typed_source_envelopes_retain_original_omissions(database, bound, kind):
    state, publisher, run = await setup_kind(database, bound[0], kind)
    original = note() if kind == "apple_notes" else contact_payload()
    native_id = "note-one" if kind == "apple_notes" else "contact-one"
    request = CommitDevicePage(
        **publisher.model_dump(),
        page_id=uuid4(),
        expected=run.version,
        observations=(DeviceObservation(native_id=native_id, original=original),),
    )
    ack = await commit(database, state, request.model_dump_json().encode())
    assert ack.acknowledgement.changed == 1
    async with database() as db:
        entity = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        assert entity.source_payload["original"] == original


@pytest.mark.parametrize(
    "locked,marked,reason", [(True, False, "access_revoked"), (False, True, "scope_removed")]
)
async def test_note_lifecycle_requires_conservative_explicit_withdrawal(
    database, bound, locked, marked, reason
):
    state, publisher, run = await setup_kind(database, bound[0], "apple_notes")
    original = note(locked=locked, marked=marked)
    request = CommitDevicePage(
        **publisher.model_dump(),
        page_id=uuid4(),
        expected=run.version,
        observations=(DeviceObservation(native_id="note-one", original=original),),
    )
    with pytest.raises(DeviceAdmissionError, match="withdrawal"):
        await commit(database, state, request.model_dump_json().encode())
    withdrawn = request.model_copy(
        update={
            "observations": (
                DeviceObservation(
                    native_id="note-one", original=original, kind="delete", removal_reason=reason
                ),
            )
        }
    )
    ack = await commit(database, state, withdrawn.model_dump_json().encode())
    assert ack.acknowledgement.changed == 1
    async with database() as db:
        entity = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        assert entity.deleted_at is not None and entity.removal_reason == reason


async def test_record_withdrawal_survives_source_reauthorization(database, bound):
    state, publisher, run, _, _ = bound
    first = await commit(database, state, page(publisher, run).model_dump_json().encode())
    deleted = CommitDevicePage(
        **publisher.model_dump(),
        page_id=uuid4(),
        expected=first.acknowledgement.version,
        observations=(
            DeviceObservation(
                native_id="message-one", kind="delete", removal_reason="access_revoked"
            ),
        ),
    )
    await commit(database, state, deleted.model_dump_json().encode())
    async with database() as db:
        revoked = await service().revoke(
            db,
            state.organization_id,
            state.source_id,
            RevokeDevice(owner_id="owner", expected_generation=publisher.generation),
        )
        rebound = await service().bind(
            db,
            state.organization_id,
            state.source_id,
            BindDevice(
                owner_id="owner",
                expected_generation=revoked.enrollment.generation,
                device_id=publisher.device_id,
                store_generation=uuid4(),
            ),
        )
        assert rebound.retained_read_authority
        entity = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        assert entity.deleted_at is not None and entity.removal_reason == "access_revoked"


async def test_unknown_identity_and_blob_admission_fail_without_capture(database, bound):
    from pydantic import ValidationError

    state, publisher, run, _, _ = bound
    request = page(publisher, run)
    mismatched = request.model_copy(
        update={"observations": (DeviceObservation(native_id="wrong-id", original=message()),)}
    )
    with pytest.raises(DeviceAdmissionError, match="identity disagrees"):
        await commit(database, state, mismatched.model_dump_json().encode())
    malformed = request.model_copy(
        update={
            "observations": (
                DeviceObservation(native_id="message-one", original={"body": "unqualified"}),
            )
        }
    )
    with pytest.raises(DeviceAdmissionError, match="supported source schema"):
        await commit(database, state, malformed.model_dump_json().encode())
    supplied = json.loads(request.model_dump_json())
    supplied["observations"][0]["blobs"] = [{"storage_key": "foreign"}]
    with pytest.raises(ValidationError):
        await commit(database, state, json.dumps(supplied).encode())
    with pytest.raises(ValidationError):
        CommitDevicePage(
            **publisher.model_dump(),
            page_id=uuid4(),
            expected=run.version,
            observations=(DeviceObservation(native_id="message-one", original=message()),) * 2,
        )
    async with database() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == state.sync_id)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_attachment_upload_commit_and_verified_read(database, bound, tmp_path, monkeypatch):
    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.device_ingestion.models import DeviceUploadIntent
    from airweave.domains.entities.canonical.query import CanonicalQueryService

    state, publisher, run, _, _ = bound
    storage = FilesystemBackend(tmp_path)
    svc = DeviceIngestion(DeviceIngestionStore(CanonicalRecordStore()), storage)
    original = message()
    original["attachments"] = [
        {"rowID": 11, "fields": {"guid": {"text": {"_0": "attachment-one"}}}}
    ]
    content = b"synthetic attachment original\x00\xff"
    handle = uuid4()
    intent = DeviceUploadIntent(
        **publisher.model_dump(),
        native_id="message-one",
        original=original,
        attachment_index=0,
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        media_type="application/octet-stream",
    )
    async with database() as db:
        pending = await svc.declare_upload(
            db, state.organization_id, state.source_id, "run-one", handle, intent
        )
        assert not pending.uploaded
        assert (
            await svc.declare_upload(
                db, state.organization_id, state.source_id, "run-one", handle, intent
            )
            == pending
        )
        with pytest.raises(DeviceAdmissionError, match="conflicting"):
            await svc.declare_upload(
                db,
                state.organization_id,
                state.source_id,
                "run-one",
                handle,
                intent.model_copy(update={"sha256": "0" * 64}),
            )
    async with database() as db:
        with pytest.raises(DeviceAdmissionError, match="hash or size"):
            await svc.upload(
                db, state.organization_id, state.source_id, "run-one", handle, publisher, b"wrong"
            )
        uploaded = await svc.upload(
            db, state.organization_id, state.source_id, "run-one", handle, publisher, content
        )
        assert uploaded.uploaded
    async with database() as db:
        with pytest.raises(DeviceAdmissionError):
            await svc.upload(
                db,
                state.organization_id,
                state.source_id,
                "run-one",
                handle,
                publisher.model_copy(update={"device_id": uuid4()}),
                content,
            )
    bad = page(publisher, run).model_copy(
        update={
            "observations": (
                DeviceObservation(native_id="message-one", original=message(), uploads=(handle,)),
            )
        }
    )
    async with database() as db:
        with pytest.raises(DeviceAdmissionError, match="observation"):
            await svc.page(
                db,
                state.organization_id,
                state.source_id,
                "run-one",
                bad.model_dump_json().encode(),
            )
    request = page(publisher, run).model_copy(
        update={
            "observations": (
                DeviceObservation(native_id="message-one", original=original, uploads=(handle,)),
            )
        }
    )

    async def fail_receipt(*args, **kwargs):
        raise RuntimeError("synthetic crash after blob-bearing capture")

    with monkeypatch.context() as patch:
        patch.setattr(svc.store.receipts, "persist", fail_receipt)
        async with database() as db:
            with pytest.raises(RuntimeError, match="synthetic crash"):
                await svc.page(
                    db,
                    state.organization_id,
                    state.source_id,
                    "run-one",
                    request.model_dump_json().encode(),
                )
            assert (
                await db.scalar(
                    select(func.count()).select_from(Entity).where(Entity.sync_id == state.sync_id)
                )
                == 0
            )
            await db.rollback()
    async with database() as db:
        await svc.page(
            db,
            state.organization_id,
            state.source_id,
            "run-one",
            request.model_dump_json().encode(),
        )
        record = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        record_id = record.id
        await db.rollback()
        current = await CanonicalQueryService(
            CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing-key"
        ).read(db, state.organization_id, state.sync_id, record_id)
        assert current.blobs[0].source_path == "/original/attachments/0"
        from airweave.domains.entities.canonical.projection_mappers import map_record
        from airweave.platform.entities.apple import AppleAttachmentEntity

        async with map_record(current, "imessage", storage) as mapped:
            attachment = mapped.parts[-1]
            assert isinstance(attachment.entity, AppleAttachmentEntity)
            from pathlib import Path

            projected_file = Path(attachment.entity.local_path)
            assert projected_file.read_bytes() == content
            assert attachment.part.key == "attachment:attachment-one"
            assert attachment.entity.url == ""
        assert not projected_file.exists()
        assert current.payload["original"] == original

        assert (
            await CanonicalQueryService(
                CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing-key"
            ).blob(
                db,
                state.organization_id,
                state.sync_id,
                record_id,
                current.revision,
                intent.sha256,
                storage,
            )
            == content
        )
        await db.rollback()
    await storage.write_file(current.blobs[0].key, b"corrupt")
    async with database() as db:
        from airweave.domains.entities.canonical.query import BlobUnavailable

        with pytest.raises(BlobUnavailable):
            await CanonicalQueryService(
                CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing-key"
            ).blob(
                db,
                state.organization_id,
                state.sync_id,
                record_id,
                current.revision,
                intent.sha256,
                storage,
            )

    await storage.write_file(current.blobs[0].key, content)
    reading, release = asyncio.Event(), asyncio.Event()

    class PausedRead(FilesystemBackend):
        async def read_file(self, path, *, max_bytes=None):
            result = await super().read_file(path, max_bytes=max_bytes)
            reading.set()
            await release.wait()
            return result

    async def download():
        async with database() as db:
            return await CanonicalQueryService(
                CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing-key"
            ).blob(
                db,
                state.organization_id,
                state.sync_id,
                record_id,
                current.revision,
                intent.sha256,
                PausedRead(tmp_path),
            )

    task = asyncio.create_task(download())
    await asyncio.wait_for(reading.wait(), 5)
    async with database() as db:
        await svc.revoke(
            db,
            state.organization_id,
            state.source_id,
            RevokeDevice(owner_id=publisher.owner_id, expected_generation=publisher.generation),
        )
    release.set()
    from airweave.domains.entities.canonical.query import RecordNotFound

    with pytest.raises(RecordNotFound):
        await task


@pytest.mark.asyncio
async def test_revoke_during_attachment_write_hides_orphan(database, bound, tmp_path):
    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.device_ingestion.models import DeviceUploadIntent

    state, publisher, _, _, _ = bound
    writing, finish = asyncio.Event(), asyncio.Event()

    class PausedStorage(FilesystemBackend):
        async def write_file(self, path, content):
            await super().write_file(path, content)
            writing.set()
            await finish.wait()

    storage = PausedStorage(tmp_path)
    svc = DeviceIngestion(DeviceIngestionStore(CanonicalRecordStore()), storage)
    original = message()
    original["attachments"] = [{"rowID": 11, "fields": {}}]
    content = b"synthetic revoke race"
    handle = uuid4()
    intent = DeviceUploadIntent(
        **publisher.model_dump(),
        native_id="message-one",
        original=original,
        attachment_index=0,
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
    )
    async with database() as db:
        await svc.declare_upload(
            db, state.organization_id, state.source_id, "run-one", handle, intent
        )

    async def upload():
        async with database() as db:
            return await svc.upload(
                db, state.organization_id, state.source_id, "run-one", handle, publisher, content
            )

    task = asyncio.create_task(upload())
    await asyncio.wait_for(writing.wait(), 5)
    async with database() as db:
        await asyncio.wait_for(
            svc.revoke(
                db,
                state.organization_id,
                state.source_id,
                RevokeDevice(owner_id=publisher.owner_id, expected_generation=publisher.generation),
            ),
            5,
        )
    finish.set()
    with pytest.raises(DeviceAdmissionError):
        await task
    async with database() as db:
        job = await db.get(SyncJob, device_run_id(state.source_id, "run-one"))
        assert job.sync_metadata["uploads"][0]["blob"] is None
        assert (
            await db.scalar(
                select(func.count()).select_from(Entity).where(Entity.sync_id == state.sync_id)
            )
            == 0
        )
    assert list(
        tmp_path.rglob(intent.sha256)
    )  # Actual bytes exist but have no committed reference.


def test_native_contract_import_does_not_load_provider_registry():
    """A real isolated interpreter checks import isolation; no modules are mocked."""
    import subprocess
    import sys
    from pathlib import Path

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from airweave.domains.entities.canonical.query import CanonicalQueryService
import airweave.platform.sources as sources
assert 'airweave.platform.sources.bitbucket' not in sys.modules
assert 'AirtableSource' in dir(sources)
from airweave.platform.sources import AirtableSource
assert AirtableSource is sources.AirtableSource
assert 'airweave.platform.sources.bitbucket' not in sys.modules
assert len(sources._SOURCE_MODULES) == 66
assert sources.__all__[-1] == 'ALL_SOURCES'
""",
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parents[4],
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_device_pipeline_version_only_initializes_new_source(database, bound):
    state, _, _, request, _ = bound
    async with database() as db:
        sync = await db.get(Sync, state.sync_id)
        assert sync.index_pipeline_version == 5
        sync.index_pipeline_version = 3
        await db.commit()
        await service().ensure(db, state.organization_id, request)
        assert (await db.get(Sync, state.sync_id)).index_pipeline_version == 3


async def test_missing_original_recapture_preserves_history(database, bound, tmp_path):
    """Body edits remain partial; old original bytes belong to explicit history."""
    import copy

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.device_ingestion.models import DeviceUploadIntent

    state, publisher, run, _, _ = bound
    storage = FilesystemBackend(tmp_path)
    svc = DeviceIngestion(DeviceIngestionStore(CanonicalRecordStore()), storage)
    original = message()
    original["attachments"] = [
        {"rowID": 11, "fields": {"guid": {"text": {"_0": "attachment-one"}}}}
    ]
    content = b"retained synthetic original"

    async def upload(native):
        handle = uuid4()
        intent = DeviceUploadIntent(
            **publisher.model_dump(),
            native_id="message-one",
            original=native,
            attachment_index=0,
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )
        async with database() as db:
            await svc.declare_upload(
                db, state.organization_id, state.source_id, "run-one", handle, intent
            )
            await svc.upload(
                db, state.organization_id, state.source_id, "run-one", handle, publisher, content
            )
        return handle

    pending_bytes = []

    async def commit(native, version, handles=(), kind="upsert"):
        request = page(publisher, run).model_copy(
            update={
                "expected": version,
                "observations": (
                    DeviceObservation(
                        native_id="message-one",
                        original=native,
                        uploads=handles,
                        kind=kind,
                        removal_reason="scope_removed" if kind == "delete" else None,
                    ),
                ),
            }
        )
        raw = request.model_dump_json().encode()
        pending_bytes.append(raw)
        async with database() as db:
            return await svc.page(
                db,
                state.organization_id,
                state.source_id,
                "run-one",
                raw,
            )

    handle = await upload(original)
    ack = await commit(original, run.version, (handle,))
    async with database() as db:
        saved = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        original_blob = saved.blob_references[0]
    edited = copy.deepcopy(original)
    edited["message"]["fields"]["text"] = {"text": {"_0": "body-only edit"}}
    revised = await commit(edited, ack.acknowledgement.version)
    retry_bytes = pending_bytes[-1]
    async with database() as db:
        retry = await svc.page(db, state.organization_id, state.source_id, "run-one", retry_bytes)
        assert retry == revised
        current = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        assert current.record_revision == 2
        assert current.source_payload["original"] == edited
        assert current.completeness == "partial"
        assert current.blob_references == []
        record_id = current.id
        current_run = await svc.get_run(
            db, state.organization_id, state.source_id, "run-one", publisher.owner_id
        )
        assert current_run.version == revised.acknowledgement.version
    from airweave.domains.entities.canonical.query import (
        CanonicalQueryService,
        RecordNotFound,
        StaleRecordRevision,
    )

    query = CanonicalQueryService(
        CanonicalRecordStore(), CanonicalQueryStore(), "synthetic-signing-key"
    )
    digest = hashlib.sha256(content).hexdigest()
    async with database() as db:
        historical = await query.read_revision(
            db, state.organization_id, state.sync_id, record_id, 1
        )
        assert historical.current_revision == 2
        assert historical.record.payload["original"] == original
        assert historical.record.blobs[0].model_dump(mode="json") == original_blob
        assert (
            await query.historical_blob(
                db, state.organization_id, state.sync_id, record_id, 1, digest, storage
            )
            == content
        )
        with pytest.raises(StaleRecordRevision):
            await query.blob(
                db, state.organization_id, state.sync_id, record_id, 1, digest, storage
            )
    deleted = await commit(edited, revised.acknowledgement.version, kind="delete")
    assert deleted.acknowledgement.version != revised.acknowledgement.version
    async with database() as db:
        current = await db.scalar(select(Entity).where(Entity.sync_id == state.sync_id))
        assert current.deleted_at is not None
        with pytest.raises(RecordNotFound):
            await query.historical_blob(
                db, state.organization_id, state.sync_id, record_id, 1, digest, storage
            )
