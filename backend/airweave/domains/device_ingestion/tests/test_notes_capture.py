"""Swift synthetic acquisition bytes admitted by real SQL; no native source grants."""

import copy
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from airweave.domains.device_ingestion.models import (
    BindDevice,
    CommitDevicePage,
    DeviceObservation,
    DevicePrincipal,
    EnsureDeviceSource,
)
from airweave.domains.device_ingestion.tests.test_admission import service
from airweave.domains.entities.canonical.query import CanonicalQueryService
from airweave.domains.entities.canonical.query_store import CanonicalQueryStore
from airweave.domains.entities.canonical.store import CanonicalRecordStore
from airweave.models.collection import Collection
from airweave.models.entity import Entity
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata


def fixture(name):
    return json.loads((Path(__file__).parent / "fixtures" / name).read_bytes())


async def test_swift_notes_pages_preserve_originals_and_explicit_withdrawals(database, source):
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
                name="Synthetic Notes",
                readable_id="notes",
                vector_db_deployment_metadata_id=deployment.id,
            )
        )
        await db.commit()
        state = await service().ensure(
            db,
            fence.organization_id,
            EnsureDeviceSource(
                owner_id="owner",
                account_id="notes-store",
                source_kind="apple_notes",
                collection="notes",
            ),
        )
        state = await service().bind(
            db,
            state.organization_id,
            state.source_id,
            BindDevice(
                owner_id="owner", expected_generation=0, device_id=uuid4(), store_generation=uuid4()
            ),
        )
        principal = DevicePrincipal(
            owner_id="owner",
            device_id=state.enrollment.device_id,
            generation=state.enrollment.generation,
            store_generation=state.enrollment.store_generation,
        )
        run = await service().start(
            db, state.organization_id, state.source_id, "notes-capture", principal
        )
        initial, marked = fixture("notes-initial-page.json"), fixture("notes-marked-page.json")
        assert initial["observations"][1]["kind"] == "delete"
        assert initial["observations"][1]["removal_reason"] == "access_revoked"
        assert marked["observations"][0]["kind"] == "delete"
        assert marked["observations"][0]["removal_reason"] == "scope_removed"
        # Prior synthetic observations make the later native flag withdrawals observable.
        seeds = []
        for item in (initial["observations"][1], marked["observations"][0]):
            original = copy.deepcopy(item["original"])
            original["note"]["fields"]["ZISPASSWORDPROTECTED"] = {"integer": {"_0": 0}}
            original["note"]["fields"]["ZMARKEDFORDELETION"] = {"integer": {"_0": 0}}
            original["fidelity"] = {"bodyUnavailable": {}}
            seeds.append(DeviceObservation(native_id=item["native_id"], original=original))
        seed = CommitDevicePage(
            **principal.model_dump(),
            page_id=uuid4(),
            expected=run.version,
            observations=tuple(seeds),
            final=False,
        )
        ack = await service().page(
            db,
            state.organization_id,
            state.source_id,
            "notes-capture",
            seed.model_dump_json().encode(),
        )
        for page in (initial, marked):
            # Route authority differs per disposable test. Native originals remain untouched.
            page.update(principal.model_dump(mode="json"))
            page["expected"] = ack.acknowledgement.version.model_dump(mode="json")
            raw = json.dumps(
                page, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode()
            ack = await service().page(
                db, state.organization_id, state.source_id, "notes-capture", raw
            )
            assert ack.sha256 == hashlib.sha256(raw).hexdigest()
            assert ack == await service().page(
                db, state.organization_id, state.source_id, "notes-capture", raw
            )
        rows = (await db.scalars(select(Entity).where(Entity.sync_id == state.sync_id))).all()
        records = {row.native_id: row for row in rows}
        normal = records["note-a"]
        assert normal.source_payload["original"] == initial["observations"][0]["original"]
        assert normal.source_payload["original"]["note"]["primaryKey"] == 9007199254740993
        assert normal.source_payload["original"]["compressedBody"] == "/wA="
        assert normal.source_payload["original"]["attachments"][0]["primaryKey"] == 9007199254740996
        assert records["note-locked"].removal_reason == "access_revoked"
        assert records["note-marked"].removal_reason == "scope_removed"
        reader = CanonicalQueryService(CanonicalRecordStore(), CanonicalQueryStore(), "key")
        for identity in ("note-locked", "note-marked"):
            retained = await reader.read(
                db, state.organization_id, state.sync_id, records[identity].id
            )
            assert (
                retained.content_access == "unavailable"
                and retained.payload == {}
                and retained.blobs == ()
            )
