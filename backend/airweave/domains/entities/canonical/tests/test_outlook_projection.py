"""Retained MIME replay: no snippet substitution, bytes loss, or invented attachment identity."""

import hashlib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.outlook_projection import map_outlook
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity


def source(content: bytes):
    sync_id = uuid4()
    digest = hashlib.sha256(content).hexdigest()
    record = SourceRecord(
        id=uuid4(),
        sync_id=sync_id,
        identity=RecordIdentity(record_type="message", native_id="immutable-1"),
        revision=1,
        payload={
            "id": "immutable-1",
            "changeKey": "change-1",
            "parentFolderId": "folder-1",
            "subject": "Meeting",
            "conversationId": "conversation-1",
            "bodyPreview": "lossy-preview",
            "sender": {"emailAddress": {"name": "रोहन", "address": "assistant@example.com"}},
            "from": {"emailAddress": {"address": "author@example.com"}},
            "toRecipients": [
                {"emailAddress": {"name": "Doe, Jane", "address": "jane@example.com"}}
            ],
            "bccRecipients": [{"emailAddress": {"address": "bcc@example.com"}}],
            "replyTo": [{"emailAddress": {"address": "reply@example.com"}}],
            "receivedDateTime": "2026-10-01T01:02:03Z",
            "nativeUnknown": {"retained": True},
        },
        payload_schema_version=1,
        capture_hash="capture",
        content_hash=None,
        completeness="partial",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(
            BlobReference(
                key=f"canonical/{sync_id}/blobs/sha256/{digest}",
                sha256=digest,
                size_bytes=len(content),
                media_type="message/rfc822",
            ),
        ),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )
    storage = AsyncMock()
    storage.read_file.return_value = content
    return record, storage


@pytest.mark.asyncio
async def test_mixed_related_alternative_preserves_unicode_metadata_and_attachments(tmp_path):
    message = EmailMessage()
    message.set_content("duplicate plain body")
    message.add_alternative("<p>नमस्ते café, पूरा पाठ</p>", subtype="html", charset="utf-8")
    message.get_payload()[1].add_related(
        b"image-original", maintype="image", subtype="png", cid="<image>", filename="../../same.png"
    )
    message.add_attachment(
        b"pdf-original", maintype="application", subtype="pdf", filename="../../same.png"
    )
    record, storage = source(message.as_bytes())
    before = record.model_dump()
    result = await map_outlook(record, storage, tmp_path)
    body, image, attachment = result.entities
    assert "नमस्ते café, पूरा पाठ" in Path(body.local_path).read_text()
    assert "duplicate plain body" not in Path(body.local_path).read_text()
    assert "lossy-preview" not in Path(body.local_path).read_text()
    assert body.sender == '"रोहन" <assistant@example.com>'
    assert body.from_address == "author@example.com"
    assert body.to_recipients == ['"Doe, Jane" <jane@example.com>']
    assert body.bcc_recipients == ["bcc@example.com"] and body.reply_to == ["reply@example.com"]
    assert body.conversation_id == "conversation-1" and body.folder_id == "folder-1"
    assert body.folder_name is None
    assert image.is_inline and image.content_id == "<image>"
    assert attachment.file_type == "pdf" and image.file_type == "png"
    assert image.composite_id != attachment.composite_id
    assert [part.part.key for part in result.parts[1:-1]] == [
        "/mime/parts/0/parts/1/parts/1",
        "/mime/parts/1",
    ]
    assert {Path(entity.local_path).read_bytes() for entity in result.entities[1:]} == {
        b"image-original",
        b"pdf-original",
    }
    assert all(Path(entity.local_path).parent == tmp_path for entity in result.entities)
    assert result.parts[-1].part.key == "/attachment_inventory" and result.parts[-1].entity is None
    assert record.model_dump() == before
    storage.read_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_latin_charset_and_explicit_related_root_are_respected(tmp_path):
    message = EmailMessage()
    message.set_content("café <full text>", charset="iso-8859-1")
    record, storage = source(message.as_bytes())
    record.payload["subject"] = "  "
    result = await map_outlook(record, storage, tmp_path)
    assert result.entities[0].subject == "Email message"
    assert "café &lt;full text&gt;" in Path(result.entities[0].local_path).read_text()
    message = EmailMessage()
    message.make_related()
    message.set_param("start", "<body>")
    image = EmailMessage()
    image.set_content(b"image", maintype="image", subtype="png")
    image["Content-ID"] = "<image>"
    body = EmailMessage()
    body.set_content("selected body")
    body["Content-ID"] = "<body>"
    message.attach(image)
    message.attach(body)
    record, storage = source(message.as_bytes())
    result = await map_outlook(record, storage, tmp_path)
    assert "selected body" in Path(result.entities[0].local_path).read_text()
    assert result.entities[1].content_id == "<image>"


@pytest.mark.asyncio
async def test_missing_and_unsupported_originals_remain_explicit(tmp_path):
    record, storage = source(b"Subject: empty\r\n\r\n")
    omitted = record.model_copy(update={"blobs": (), "completeness": "metadata_only"})
    result = await map_outlook(omitted, storage, tmp_path)
    assert not result.entities and result.parts[0].entity is None
    storage.read_file.assert_not_called()
    with pytest.raises(ValueError, match="metadata-only"):
        await map_outlook(record.model_copy(update={"blobs": ()}), storage, tmp_path)
    message = EmailMessage()
    message.set_content("retained body")
    nested = EmailMessage()
    nested.set_content("nested mail is not silently indexed")
    message.add_attachment(nested)
    record, storage = source(message.as_bytes())
    result = await map_outlook(record, storage, tmp_path)
    assert len(result.entities) == 1
    assert result.parts[1].omission == "unsupported_format"
    assert result.parts[1].part.media_type == "message/rfc822"
    encrypted = b"MIME-Version: 1.0\r\nContent-Type: application/pkcs7-mime\r\n\r\nencrypted"
    record, storage = source(encrypted)
    result = await map_outlook(record, storage, tmp_path)
    assert not result.entities and result.parts[0].omission == "unsupported_format"


@pytest.mark.asyncio
async def test_identity_and_blob_integrity_fail_before_materialization(tmp_path):
    record, storage = source(b"Subject: retained\r\n\r\nbody")
    bad = record.model_copy(
        update={"identity": RecordIdentity(record_type="message", native_id="different")}
    )
    with pytest.raises(ValueError, match="identity"):
        await map_outlook(bad, storage, tmp_path)
    storage.read_file.assert_not_called()
    bad_ref = record.blobs[0].model_copy(update={"key": "canonical/other/blobs/sha256/hash"})
    with pytest.raises(ValueError, match="source scope"):
        await map_outlook(record.model_copy(update={"blobs": (bad_ref,)}), storage, tmp_path)
    storage.read_file.assert_not_called()
    storage.read_file.return_value = b"bad bytes"
    with pytest.raises(ValueError, match="digest"):
        await map_outlook(record, storage, tmp_path)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_malformed_transfers_charset_and_structure_are_never_repaired_silently(tmp_path):
    invalid_messages = [
        b"Content-Transfer-Encoding: base64\r\n\r\naGVsbG8=@@",
        b"Content-Transfer-Encoding: quoted-printable\r\n\r\nhello=ZZ",
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n\xff",
        b"Content-Type: multipart/mixed; boundary=missing\r\n\r\nno boundary",
    ]
    for content in invalid_messages:
        record, storage = source(content)
        with pytest.raises(ValueError):
            await map_outlook(record, storage, tmp_path)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_pathological_nesting_is_explicit_failure_not_partial_text(tmp_path):
    message = EmailMessage()
    message.set_content("body")
    for _ in range(34):
        parent = EmailMessage()
        parent.make_mixed()
        parent.attach(message)
        message = parent
    record, storage = source(message.as_bytes())
    with pytest.raises(ValueError, match="depth or part limit"):
        await map_outlook(record, storage, tmp_path)
    assert not list(tmp_path.iterdir())
