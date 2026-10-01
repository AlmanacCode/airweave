"""Retained Slack file projection: no provider calls or live authorization claims."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.blob_materializer import BlobIntegrityError
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.platform.entities.slack import SlackAttachmentEntity


def message(files):
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(record_type="message", native_id="1.000001", container_id="C1"),
        revision=1,
        payload={"ts": "1.000001", "text": "Intact message", "files": files},
        payload_schema_version=1,
        capture_hash="capture",
        content_hash=None,
        completeness="partial",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )


def blob(record, data, index):
    digest = hashlib.sha256(data).hexdigest()
    return BlobReference(
        key=f"canonical/{record.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(data),
        source_path=f"/files/{index}",
    )


@pytest.mark.asyncio
async def test_body_retained_pdf_duplicate_bytes_and_unavailable_file():
    record = message(
        [
            {"id": "F1", "name": "one.pdf", "mimetype": "application/pdf"},
            {"id": "F2", "name": "two.pdf", "mimetype": "application/pdf"},
            {"id": "F3", "name": "clip.mp4", "mimetype": "video/mp4"},
        ]
    )
    content = b"%PDF-1.4 retained fixture, converter not exercised"
    record = record.model_copy(
        update={"blobs": (blob(record, content, 0), blob(record, content, 1))}
    )
    before = record.model_dump()
    storage = AsyncMock()
    storage.read_file.return_value = content
    async with map_record(record, "slack", storage) as mapped:
        assert [part.part.key for part in mapped.parts] == ["body", "file:F1", "file:F2", "file:F3"]
        assert mapped.parts[0].entity.text == "Intact message"
        assert mapped.parts[3].entity is None
        first, second = mapped.parts[1].entity, mapped.parts[2].entity
        assert isinstance(first, SlackAttachmentEntity)
        assert first.entity_id != second.entity_id
        assert first.local_path == second.local_path
        assert Path(first.local_path).read_bytes() == content
        assert mapped.parts[1].part.media_type == "application/pdf"
        assert mapped.parts[1].part.extension == ".pdf"
    assert record.model_dump() == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "files", [[{"id": "F1"}, {"id": "F1"}], [{"name": "missing-id"}], "bad-list"]
)
async def test_ambiguous_or_malformed_files_never_become_complete(files):
    storage = AsyncMock()
    with pytest.raises(ValueError, match="Slack"):
        async with map_record(message(files), "slack", storage):
            pass
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_wrong_blob_owner_stale_path_and_corrupt_bytes_fail_before_conversion():
    record = message([{"id": "F1", "name": "safe.pdf"}])
    valid = blob(record, b"expected", 0)
    storage = AsyncMock()
    for bad in (
        valid.model_copy(update={"key": f"canonical/{uuid4()}/blobs/sha256/{valid.sha256}"}),
        valid.model_copy(update={"source_path": "/files/9"}),
    ):
        with pytest.raises(ValueError):
            async with map_record(record.model_copy(update={"blobs": (bad,)}), "slack", storage):
                pass
    storage.read_file.assert_not_called()
    storage.read_file.return_value = b"wrong"
    with pytest.raises(BlobIntegrityError):
        async with map_record(record.model_copy(update={"blobs": (valid,)}), "slack", storage):
            pass
    with pytest.raises(ValueError, match="lacks a retained file"):
        async with map_record(
            record.model_copy(update={"completeness": "complete"}), "slack", storage
        ):
            pass


async def test_slack_pdf_publication_records_partial_extraction(database, source, tmp_path):
    """Real PDF conversion/SQL publication, with synthetic embedding and feed only."""
    import fitz
    from sqlalchemy import select

    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.core.logging import logger
    from airweave.domains.entities.canonical.projection_store import current_extraction
    from airweave.domains.entities.canonical.tests.helpers import capture, observation
    from airweave.domains.entities.canonical.tests.test_extraction_coverage import (
        destination,
        projector,
    )
    from airweave.domains.storage.file_service import FileService
    from airweave.models import Entity

    service, fence = source
    storage = FilesystemBackend(tmp_path)
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text(
        (40, 40), "Retained Slack PDF with useful searchable investment discussion. " * 4
    )
    content = pdf.tobytes()
    pdf.close()
    files = FileService(uuid4(), storage, sync_id=fence.sync_id)
    retained = await files.store_canonical_blob(content, media_type="application/pdf")
    item = observation(
        identity=RecordIdentity(record_type="message", native_id="1.000001", container_id="C1"),
        payload={
            "ts": "1.000001",
            "text": "Intact Slack discussion.",
            "files": [
                {"id": "F1", "name": "discussion.pdf", "mimetype": "application/pdf"},
                {"id": "F2", "name": "missing.pdf", "mimetype": "application/pdf"},
            ],
        },
        completeness="partial",
        blobs=(retained.model_copy(update={"source_path": "/files/0"}),),
    )
    await capture(database, service, fence, item)
    result = await projector(database, storage).batch(
        fence.organization_id, fence.sync_id, "slack", destination(), logger
    )
    assert result.published == 1 and result.failed == 0
    async with database() as db:
        row = await db.scalar(select(Entity).where(Entity.sync_id == fence.sync_id))
        coverage = await current_extraction(
            db, fence.organization_id, fence.sync_id, row.id, row.record_revision
        )
        assert coverage.status == "partial"
        assert [part.outcome for part in coverage.parts] == [
            "indexed",
            "indexed",
            "unavailable_original",
        ]
        assert coverage.parts[1].key == "file:F1"
        assert row.completeness == "partial"
        old_revision = row.record_revision

    # Switch one retained inline original to explicit child ownership. The parent
    # revision invalidates its previous PDF publication before children publish.
    from airweave.domains.entities.canonical.requests import parent_container_key
    from airweave.domains.entities.canonical.slack_files import SlackFileManifest, SlackFileOutcome

    parent = item.identity
    children = []
    for index, native in enumerate(item.payload["files"]):
        manifest = SlackFileManifest(
            files=(
                SlackFileOutcome(
                    index=0,
                    native_id=native["id"],
                    outcome="captured" if index == 0 else "unavailable",
                    reason=None if index == 0 else "access_denied",
                ),
            )
        )
        evidence = await files.store_canonical_blob(
            manifest.model_dump_json().encode(), media_type="application/json"
        )
        refs = (evidence.model_copy(update={"role": "representation_manifest"}),)
        if index == 0:
            refs += (retained.model_copy(update={"source_path": ""}),)
        children.append(
            observation(
                identity=RecordIdentity(
                    record_type="file",
                    native_id=native["id"],
                    container_id=parent_container_key(parent),
                ),
                parent=parent,
                payload=native,
                completeness="complete" if index == 0 else "partial",
                blobs=refs,
            )
        )
    await capture(
        database,
        service,
        fence,
        item.model_copy(
            update={
                "payload_schema_version": 2,
                "blobs": (),
                "completeness": "complete",
                "descendant_visibility_fields": ("files",),
            }
        ),
        *children,
    )
    async with database() as db:
        parent_row = await db.scalar(select(Entity).where(Entity.native_id == parent.native_id))
        assert parent_row.record_revision > old_revision
        assert parent_row.indexed_revision != parent_row.record_revision
    result = await projector(database, storage).batch(
        fence.organization_id, fence.sync_id, "slack", destination(), logger
    )
    assert result.published == 3 and result.failed == 0
    async with database() as db:
        rows = (await db.scalars(select(Entity).where(Entity.sync_id == fence.sync_id))).all()
        coverage_by_id = {
            row.native_id: await current_extraction(
                db, fence.organization_id, fence.sync_id, row.id, row.record_revision
            )
            for row in rows
        }
        assert [part.key for part in coverage_by_id[parent.native_id].parts] == ["body"]
        assert [part.outcome for part in coverage_by_id["F1"].parts] == ["indexed"]
        assert [part.outcome for part in coverage_by_id["F2"].parts] == ["unavailable_original"]


@pytest.mark.asyncio
async def test_manifest_enrichment_requires_exact_native_identity_and_blob_evidence():
    """Slack Connect stubs retain their native JSON; acquisition evidence supplies format."""
    import json

    record = message([{"id": "F1", "file_access": "check_file_info"}, {"id": "F2"}])
    content = b"retained PDF fixture"
    original = blob(record, content, 0)
    payload = {
        "version": 1,
        "files": [
            {
                "index": 0,
                "native_id": "F1",
                "outcome": "captured",
                "file": {"id": "F1", "name": "verified.pdf", "mimetype": "application/pdf"},
            },
            {"index": 1, "native_id": "F2", "outcome": "unavailable", "reason": "access_denied"},
        ],
    }

    async def run(value, *, include_original=True):
        data = json.dumps(value).encode()
        evidence = blob(record, data, 9).model_copy(
            update={"source_path": None, "role": "representation_manifest"}
        )
        source = record.model_copy(
            update={"blobs": ((original,) if include_original else ()) + (evidence,)}
        )
        storage = AsyncMock()
        storage.read_file.side_effect = lambda key, **_: data if key == evidence.key else content
        async with map_record(source, "slack", storage) as mapped:
            assert mapped.parts[1].entity.filename == "verified.pdf"
            assert mapped.parts[1].part.media_type == "application/pdf"
            assert mapped.parts[2].entity is None
            assert source.payload == record.payload

    await run(payload)
    with pytest.raises(ValueError, match="contradicts"):
        await run(payload, include_original=False)
    payload["files"][0].update(outcome="unavailable", reason="access_denied")
    with pytest.raises(ValueError, match="contradicts"):
        await run(payload)
    payload["files"][0].update(outcome="captured", reason=None)
    payload["files"][1]["native_id"] = "F3"
    with pytest.raises(ValueError, match="native message identities"):
        await run(payload)


@pytest.mark.asyncio
async def test_child_owned_message_never_indexes_inline_attachments():
    source = message([{"id": "F1", "name": "one.pdf"}]).model_copy(
        update={"payload_schema_version": 2, "completeness": "complete"}
    )
    storage = AsyncMock()
    async with map_record(source, "slack", storage) as mapped:
        assert [part.part.key for part in mapped.parts] == ["body"]
    storage.read_file.assert_not_called()
    with pytest.raises(ValueError, match="cannot retain inline"):
        async with map_record(
            source.model_copy(update={"blobs": (blob(source, b"x", 0),)}), "slack", storage
        ):
            pass


@pytest.mark.asyncio
async def test_child_file_retained_and_unavailable_have_one_extraction_owner():
    import json

    from airweave.domains.entities.canonical.requests import parent_container_key

    parent = message([]).identity
    source = message([]).model_copy(
        update={
            "identity": RecordIdentity(
                record_type="file", native_id="F1", container_id=parent_container_key(parent)
            ),
            "parent": parent,
            "payload": {"id": "F1", "name": "one.pdf", "mimetype": "application/pdf"},
        }
    )
    storage = AsyncMock()
    for captured in (True, False):
        content = b"fixture PDF bytes"
        original = blob(source, content, 0).model_copy(update={"source_path": ""})
        data = json.dumps(
            {
                "version": 1,
                "files": [
                    {
                        "index": 0,
                        "native_id": "F1",
                        "outcome": "captured" if captured else "unavailable",
                        "reason": None if captured else "access_denied",
                    }
                ],
            }
        ).encode()
        manifest = blob(source, data, 0).model_copy(
            update={"source_path": None, "role": "representation_manifest"}
        )
        current = source.model_copy(
            update={"blobs": (manifest, original) if captured else (manifest,)}
        )
        contents = {manifest.key: data, original.key: content}
        storage.read_file.side_effect = lambda key, contents=contents, **_: contents[key]
        async with map_record(current, "slack", storage) as mapped:
            assert len(mapped.parts) == 1 and mapped.parts[0].part.key == "file:F1"
            assert (mapped.parts[0].entity is not None) == captured
        with pytest.raises(ValueError, match="exact message parent"):
            async with map_record(current.model_copy(update={"parent": None}), "slack", storage):
                pass
        if captured:
            with pytest.raises(ValueError, match="root original"):
                async with map_record(
                    current.model_copy(
                        update={
                            "blobs": (
                                manifest,
                                original.model_copy(update={"source_path": "/files/0"}),
                            )
                        }
                    ),
                    "slack",
                    storage,
                ):
                    pass
