"""Replay source MIME content without provider requests or lossy snippet fallbacks."""

import base64
import hashlib
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.blob_materializer import read_blob
from airweave.domains.entities.canonical.gmail_projection import map_gmail
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity


def part(content=b"hello", mime="text/plain", **fields):
    return {
        "mimeType": mime,
        "body": {"data": base64.urlsafe_b64encode(content).decode(), "size": len(content)},
        **fields,
    }


def record(payload, **changes):
    values = {
        "id": uuid4(),
        "sync_id": uuid4(),
        "identity": RecordIdentity(record_type="message", native_id="m1"),
        "revision": 1,
        "payload": {
            "id": "m1",
            "threadId": "t1",
            "internalDate": "1000",
            "snippet": "must not substitute this snippet",
            "payload": payload,
        },
        "payload_schema_version": 1,
        "capture_hash": "capture",
        "content_hash": None,
        "completeness": "complete",
        "observed_at": datetime.now(timezone.utc),
        "source_created_at": None,
        "source_updated_at": None,
        "deleted_at": None,
        "removal_reason": None,
        "blobs": (),
        "indexed_revision": None,
        "indexed_pipeline_version": None,
    }
    values.update(changes)
    return SourceRecord(**values)


def blob(source, data, source_path="/payload/body"):
    digest = hashlib.sha256(data).hexdigest()
    return BlobReference(
        key=f"canonical/{source.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(data),
        source_path=source_path,
    )


@pytest.mark.asyncio
async def test_plain_charset_body_and_quoted_address_are_preserved(tmp_path):
    source = record(
        part(
            "café <hello>".encode("latin-1"),
            headers=[
                {"name": "Content-Type", "value": "text/plain; charset=iso-8859-1"},
                {"name": "To", "value": '"Doe, Jane" <jane@example.com>, bob@example.com'},
                {"name": "Subject", "value": "Subject"},
            ],
        )
    )
    before = source.model_dump()
    entities = await map_gmail(source, AsyncMock(), tmp_path)
    entities = entities.entities
    assert len(entities) == 1
    assert entities[0].to == ['"Doe, Jane" <jane@example.com>', "bob@example.com"]
    assert "café &lt;hello&gt;" in next(tmp_path.glob("*.html")).read_text()
    assert source.model_dump() == before


@pytest.mark.asyncio
async def test_alternative_prefers_full_html_not_plain_duplicate_or_snippet(tmp_path):
    source = record(
        {
            "mimeType": "multipart/alternative",
            "parts": [
                part(b"unselected plain"),
                part(b"<p>Full HTML body</p>", "text/html"),
            ],
        }
    )
    entities = await map_gmail(source, AsyncMock(), tmp_path)
    entities = entities.entities
    text = next(tmp_path.glob("*.html")).read_text()
    assert text == "<p>Full HTML body</p>"
    assert entities[0].local_path is not None


@pytest.mark.asyncio
async def test_external_blob_and_duplicate_filename_attachments_get_distinct_identity(tmp_path):
    source = record(
        {
            "mimeType": "multipart/mixed",
            "parts": [
                {"mimeType": "text/html", "body": {"attachmentId": "body", "size": 11}},
                part(b"one", "text/plain", filename="../../same.txt", partId="1"),
                part(b"two", "text/plain", filename="../../same.txt", partId="2"),
            ],
        }
    )
    data = b"<b>body</b>"
    source = source.model_copy(update={"blobs": (blob(source, data, "/payload/parts/0/body"),)})
    # Provider's declared size must match actual content.
    source.payload["payload"]["parts"][0]["body"]["size"] = len(data)
    storage = AsyncMock()
    storage.read_file.return_value = data
    entities = await map_gmail(source, storage, tmp_path)
    entities = entities.entities
    assert len(entities) == 3
    assert entities[1].attachment_key != entities[2].attachment_key
    from pathlib import Path

    assert all(Path(e.local_path).parent == tmp_path for e in entities)
    assert {Path(e.local_path).read_bytes() for e in entities[1:]} == {b"one", b"two"}


@pytest.mark.asyncio
async def test_missing_blob_ref_fails_instead_of_indexing_snippet(tmp_path):
    source = record({"mimeType": "text/html", "body": {"attachmentId": "body", "size": 3}})
    with pytest.raises(ValueError, match="exactly one"):
        await map_gmail(source, AsyncMock(), tmp_path)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_cross_source_blob_is_rejected_before_storage_access():
    source = record(part())
    reference = blob(source, b"abc").model_copy(update={"key": "canonical/other/blobs/sha256/hash"})
    storage = AsyncMock()
    with pytest.raises(ValueError, match="source scope"):
        await read_blob(source, reference, storage)
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_corrupt_blob_bytes_fail_before_projection(tmp_path):
    source = record({"mimeType": "text/plain", "body": {"attachmentId": "body", "size": 3}})
    source = source.model_copy(update={"blobs": (blob(source, b"abc"),)})
    storage = AsyncMock()
    storage.read_file.return_value = b"bad"
    with pytest.raises(ValueError, match="digest"):
        await map_gmail(source, storage, tmp_path)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_missing_attachment_keeps_available_body_searchable_and_original_partial(tmp_path):
    source = record(
        {
            "mimeType": "multipart/mixed",
            "parts": [
                part(b"Available message text"),
                {
                    "mimeType": "application/pdf",
                    "filename": "uncaptured.pdf",
                    "body": {"attachmentId": "large", "size": 500000000},
                },
            ],
        },
        completeness="partial",
    )
    before = source.model_dump()
    storage = AsyncMock()
    entities = await map_gmail(source, storage, tmp_path)
    entities = entities.entities
    assert len(entities) == 1
    assert "Available message text" in next(tmp_path.glob("*.html")).read_text()
    assert source.model_dump() == before
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_partial_capture_still_requires_actual_message_body(tmp_path):
    source = record(
        {"mimeType": "text/plain", "body": {"attachmentId": "body", "size": 3}},
        completeness="partial",
    )
    with pytest.raises(ValueError, match="exactly one"):
        await map_gmail(source, AsyncMock(), tmp_path)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_related_html_branch_does_not_get_replaced_by_plain_alternative(tmp_path):
    source = record(
        {
            "mimeType": "multipart/alternative",
            "parts": [
                part(b"plain duplicate"),
                {
                    "mimeType": "multipart/related",
                    "parts": [
                        part(b"<p>Complete rich content</p>", "text/html"),
                        part(b"image bytes", "image/png", filename="image.png"),
                    ],
                },
            ],
        }
    )
    entities = await map_gmail(source, AsyncMock(), tmp_path)
    entities = entities.entities
    assert len(entities) == 2
    from pathlib import Path

    assert Path(entities[0].local_path).read_text().strip() == "<p>Complete rich content</p>"
    assert entities[1].file_type == "png"
