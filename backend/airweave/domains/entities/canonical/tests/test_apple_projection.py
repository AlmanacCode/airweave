"""Retained Apple projection preserves originals and exposes unsupported coverage."""

import base64
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.apple_projection import map_apple
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.requests import RecordIdentity


def record(kind, identity, payload):
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(record_type=kind, native_id=identity),
        revision=1,
        payload={
            "authority": "device",
            "source_kind": {
                "imessage_message": "imessage",
                "apple_note": "apple_notes",
                "apple_contact": "apple_contacts",
            }[kind],
            "account_id": "synthetic-store",
            "original": payload,
        },
        payload_schema_version=1,
        capture_hash="test",
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


def message():
    return record(
        "imessage_message",
        "M1",
        {
            "schemaVersion": 1,
            "guid": "M1",
            "message": {
                "rowID": 9007199254740993,
                "fields": {
                    "guid": {"text": {"_0": "M1"}},
                    "text": {"text": {"_0": "Hello — مرحبا — हिन्दी — 👩🏽‍💻"}},
                    "attributedBody": {
                        "blob": {
                            "_0": base64.b64encode(
                                (
                                    Path(__file__).parents[1]
                                    / "apple_preparation/tests/fixtures/attributed-body.bin"
                                ).read_bytes()
                            ).decode()
                        }
                    },
                },
            },
            "chats": [],
            "participants": [],
            "chatMemberships": [],
            "attachments": [
                {
                    "rowID": 2,
                    "fields": {
                        "guid": {"text": {"_0": "A1"}},
                        "filename": {"text": {"_0": "/private/never-read.pdf"}},
                    },
                }
            ],
            "bodyFidelity": {"attributedBodyUndecoded": {}},
        },
    )


def note():
    original = (
        Path(__file__).parents[1]
        / "apple_preparation/tests/fixtures/simple_note_protobuf_gzipped.bin"
    ).read_bytes()
    return record(
        "apple_note",
        "N1",
        {
            "schemaVersion": 1,
            "note": {
                "primaryKey": 1,
                "fields": {
                    "ZIDENTIFIER": {"text": {"_0": "N1"}},
                    "Z_PK": {"integer": {"_0": 1}},
                    "ZTITLE1": {"text": {"_0": "My note"}},
                    "ZISPASSWORDPROTECTED": {"integer": {"_0": 0}},
                    "ZMARKEDFORDELETION": {"integer": {"_0": 0}},
                },
            },
            "attachments": [],
            "compressedBody": base64.b64encode(original).decode(),
            "fidelity": {"compressedBodyUndecoded": {}},
        },
    )


@pytest.mark.asyncio
async def test_message_body_and_explicit_uncaptured_parts():
    source = message()
    before = source.model_dump()
    storage = AsyncMock()
    async with map_record(source, "imessage", storage) as result:
        assert result.parts[0].native_body.text == "Hello — مرحبا — हिन्दी — 👩🏽‍💻"
    assert not storage.mock_calls
    assert [part.part.key for part in result.parts] == [
        "body",
        "rich-message-content",
        "attachment:A1",
    ]
    assert result.parts[0].native_body.text == "Hello — مرحبا — हिन्दी — 👩🏽‍💻"
    assert result.parts[0].entity.web_url == ""
    assert result.parts[1].omission == "unsupported_format"
    assert result.parts[2].entity is None
    assert source.model_dump() == before


@pytest.mark.asyncio
async def test_note_native_body_preparation_and_rich_gap():
    source = note()
    before = source.model_dump()
    result = await map_apple(source, "apple_notes")
    assert result.parts[0].native_body.text == "Title"
    assert result.parts[0].entity.title == "My note"
    assert result.parts[1].part.key == "rich-note-content"
    assert result.parts[1].omission == "unsupported_format"
    assert source.model_dump() == before


@pytest.mark.asyncio
async def test_withdrawn_identity_mismatch_and_note_lock_fail_closed():
    source = message()
    with pytest.raises(ValueError, match="Unavailable"):
        await map_apple(source.model_copy(update={"content_access": "revoked"}), "imessage")
    wrong = source.model_copy(
        update={"identity": RecordIdentity(record_type="imessage_message", native_id="OTHER")}
    )
    with pytest.raises(ValueError, match="identity"):
        await map_apple(wrong, "imessage")
    locked = note()
    locked.payload["original"]["note"]["fields"]["ZISPASSWORDPROTECTED"] = {"integer": {"_0": 1}}
    locked.payload["original"].pop("compressedBody")
    locked.payload["original"]["fidelity"] = {"lockedBodyWithheld": {}}
    with pytest.raises(ValueError, match="Locked"):
        await map_apple(locked, "apple_notes")


@pytest.mark.asyncio
async def test_contacts_native_payload_stays_individual():
    import json

    payload = json.loads(
        (Path(__file__).parents[1] / "apple_payload_tests/fixtures/contact-swift.json").read_text()
    )
    source = record("apple_contact", payload["contact"]["nativeID"], payload)
    result = await map_apple(source, "apple_contacts")
    text = result.parts[0].native_body.text
    assert "Given name: سمیر" in text and "Family name: शर्मा" in text
    assert "+1 (555) 0100" in text and "sam+work@example.com" in text
    assert result.parts[0].native_body.preparation.processor == "apple_contacts"
    assert result.parts[0].entity.native_id == payload["contact"]["nativeID"]
    assert source.payload["original"] == payload


@pytest.mark.asyncio
async def test_committed_attachment_bytes_materialize_and_tempfile_lifetime(tmp_path):
    import hashlib
    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.entities.canonical.blob_materializer import BlobIntegrityError, read_blob
    from airweave.domains.entities.canonical.requests import BlobReference
    from airweave.platform.entities.apple import AppleAttachmentEntity

    source = message()
    content = b"synthetic retained pdf original"
    digest = hashlib.sha256(content).hexdigest()
    ref = BlobReference(
        key=f"canonical/{source.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="application/pdf",
        source_path="/original/attachments/0",
    )
    source = source.model_copy(update={"blobs": (ref,)})
    original = source.model_dump()
    storage = FilesystemBackend(tmp_path / "storage")
    await storage.write_file(ref.key, content)
    async with map_record(source, "imessage", storage) as result:
        attachment = result.parts[-1]
        assert attachment.part.key == "attachment:A1"
        assert attachment.part.extension == ".pdf"
        assert isinstance(attachment.entity, AppleAttachmentEntity)
        path = Path(attachment.entity.local_path)
        assert path.read_bytes() == content
        assert path.name == digest + ".pdf"
        assert path != Path("/private/never-read.pdf")
        assert attachment.entity.filename == "never-read.pdf"
        assert attachment.entity.url == ""
        assert await read_blob(source, ref, storage) == content
    assert not path.exists()
    assert source.model_dump() == original
    await storage.write_file(ref.key, b"corrupt")
    with pytest.raises(BlobIntegrityError):
        async with map_record(source, "imessage", storage):
            pass


@pytest.mark.asyncio
async def test_attachment_membership_ambiguity_and_exact_envelope(tmp_path):
    import hashlib
    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.entities.canonical.requests import BlobReference

    source = message()
    digest = hashlib.sha256(b"synthetic").hexdigest()
    ref = BlobReference(
        key=f"canonical/{source.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=9,
        source_path="/original/attachments/0",
    )
    storage = FilesystemBackend(tmp_path)
    for refs in ((ref, ref), (ref.model_copy(update={"source_path": "/original/attachments/99"}),)):
        with pytest.raises(ValueError):
            async with map_record(source.model_copy(update={"blobs": refs}), "imessage", storage):
                pass
    with pytest.raises(ValueError):
        await map_apple(
            source.model_copy(update={"payload": source.payload["original"]}), "imessage"
        )
    with pytest.raises(ValueError, match="source kind"):
        await map_apple(
            source.model_copy(update={"payload": {**source.payload, "source_kind": "apple_notes"}}),
            "imessage",
        )


@pytest.mark.asyncio
async def test_note_attachment_uses_original_pointer_and_verified_bytes(tmp_path):
    import hashlib
    from airweave.adapters.storage.filesystem import FilesystemBackend
    from airweave.domains.entities.canonical.requests import BlobReference
    from airweave.domains.storage.exceptions import StorageNotFoundError
    from airweave.platform.entities import ENTITIES_BY_SOURCE
    from airweave.platform.entities.apple import AppleAttachmentEntity

    source = note()
    source.payload["original"]["attachments"] = [
        {
            "primaryKey": 9,
            "fields": {
                "Z_PK": {"integer": {"_0": 9}},
                "ZIDENTIFIER": {"text": {"_0": "note-file"}},
                "ZFILENAME": {"text": {"_0": "../../never-read.txt"}},
            },
        }
    ]
    content = "Retained attachment text مرحبا\n".encode()
    digest = hashlib.sha256(content).hexdigest()
    ref = BlobReference(
        key=f"canonical/{source.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type="text/plain",
        source_path="/original/attachments/0",
    )
    source = source.model_copy(update={"blobs": (ref,)})
    storage = FilesystemBackend(tmp_path)
    with pytest.raises(StorageNotFoundError):
        async with map_record(source, "apple_notes", storage):
            pass
    await storage.write_file(ref.key, content)
    async with map_record(source, "apple_notes", storage) as mapped:
        file = mapped.parts[-1]
        assert file.part.key == "attachment:note-file"
        assert file.part.extension == ".txt"
        assert file.entity.filename == "never-read.txt"
        assert Path(file.entity.local_path).read_bytes() == content
    assert AppleAttachmentEntity in ENTITIES_BY_SOURCE["apple_notes"]
    assert AppleAttachmentEntity in ENTITIES_BY_SOURCE["imessage"]
    assert AppleAttachmentEntity not in ENTITIES_BY_SOURCE["apple_contacts"]
