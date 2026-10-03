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
async def test_mislabeled_utf8_body_is_recovered_without_changing_original(tmp_path):
    text = "<p>नमस्ते — café</p>"
    source = record(
        part(
            text.encode(),
            "text/html",
            headers=[{"name": "Content-Type", "value": "text/html; charset=gb2312"}],
        )
    )
    before = source.model_dump()
    result = await map_gmail(source, AsyncMock(), tmp_path)
    assert next(tmp_path.glob("*.html")).read_text() == text
    assert result.parts[0].part.charset_recoveries[0].source_path == "/payload"
    assert result.parts[0].part.charset_recoveries[0].from_charset == "gb2312"
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


@pytest.mark.parametrize("html_body", [b"", b" \n\t "])
@pytest.mark.asyncio
async def test_alternative_falls_back_to_plain_when_html_is_empty(tmp_path, html_body):
    source = record(
        {
            "mimeType": "multipart/alternative",
            "parts": [part(b"usable plain body", "text/plain"), part(html_body, "text/html")],
        }
    )

    await map_gmail(source, AsyncMock(), tmp_path)

    assert next(tmp_path.glob("*.html")).read_text() == "<pre>usable plain body</pre>"


@pytest.mark.asyncio
async def test_related_empty_html_alternative_falls_back_without_materializing_siblings(tmp_path):
    source = record(
        {
            "mimeType": "multipart/alternative",
            "parts": [
                part(b"usable plain body", "text/plain"),
                {
                    "mimeType": "multipart/related",
                    "parts": [
                        part(b" \n ", "text/html"),
                        part(b"image bytes", "image/png", filename="image.png"),
                    ],
                },
            ],
        }
    )

    entities = await map_gmail(source, AsyncMock(), tmp_path)

    assert len(entities.entities) == 1
    assert next(tmp_path.glob("*.html")).read_text() == "<pre>usable plain body</pre>"


@pytest.mark.asyncio
async def test_unreadable_html_alternative_does_not_fall_back_to_plain(tmp_path):
    source = record(
        {
            "mimeType": "multipart/alternative",
            "parts": [
                part(b"usable plain body", "text/plain"),
                {"mimeType": "text/html", "body": {"attachmentId": "missing", "size": 0}},
            ],
        }
    )

    with pytest.raises(ValueError, match="exactly one"):
        await map_gmail(source, AsyncMock(), tmp_path)

    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.asyncio
async def test_last_readable_html_skips_earlier_missing_blob_and_reads_selected_once(
    tmp_path, nested
):
    payload = {
        "mimeType": "multipart/alternative",
        "parts": [
            {"mimeType": "text/html", "body": {"attachmentId": "missing", "size": 0}},
            {"mimeType": "text/html", "body": {"attachmentId": "selected", "size": 0}},
        ],
    }
    body_path = "/payload/parts/1/body"
    if nested:
        payload = {
            "mimeType": "multipart/alternative",
            "parts": [
                part(b"plain", "text/plain"),
                {"mimeType": "multipart/related", "parts": [payload]},
            ],
        }
        body_path = "/payload/parts/1/parts/0/parts/1/body"
    source = record(payload)
    content = b"<p>Selected external HTML</p>"
    source = source.model_copy(update={"blobs": (blob(source, content, body_path),)})
    storage = AsyncMock()
    storage.read_file.return_value = content

    await map_gmail(source, storage, tmp_path)

    assert next(tmp_path.glob("*.html")).read_text() == content.decode()
    storage.read_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_external_blob_and_duplicate_filename_attachments_get_distinct_identity(tmp_path):
    source = record(
        {
            "mimeType": "multipart/mixed",
            "parts": [
                {"mimeType": "text/html", "body": {"attachmentId": "body", "size": 10}},
                part(b"one", "text/plain", filename="../../same.txt", partId="1"),
                part(b"two", "text/plain", filename="../../same.txt", partId="2"),
            ],
        }
    )
    data = b"<b>body</b>"
    source = source.model_copy(update={"blobs": (blob(source, data, "/payload/parts/0/body"),)})
    # Canonical SHA and actual size are authoritative even when native size differs.
    assert source.payload["payload"]["parts"][0]["body"]["size"] != len(data)
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


@pytest.mark.asyncio
async def test_inline_native_size_discrepancy_preserves_data_and_metadata(tmp_path):
    source = record(part(b"<p>Retained full body</p>", "text/html"))
    source.payload["payload"]["body"]["size"] -= 1
    before = source.model_dump()
    mapped = await map_gmail(source, AsyncMock(), tmp_path)
    from pathlib import Path

    assert Path(mapped.entities[0].local_path).read_bytes() == b"<p>Retained full body</p>"
    assert source.model_dump() == before
    source.payload["payload"]["body"]["data"] = "invalid!!"
    with pytest.raises(ValueError):
        await map_gmail(source, AsyncMock(), tmp_path)


@pytest.mark.asyncio
async def test_inert_mime_text_preserves_structure_charset_and_original(tmp_path):
    from airweave.domains.converters.registry import ConverterRegistry

    text = (
        "BEGIN:VCALENDAR\r\nSUMMARY:नमस्ते — café\r\n"
        "DTSTART;TZID=America/Los_Angeles:20261003T090000\r\n"
        "RRULE:FREQ=WEEKLY;COUNT=2\r\nURL:https://example.invalid/inert\r\nEND:VCALENDAR\r\n"
    )
    source = record(
        {
            "mimeType": "multipart/mixed",
            "parts": [
                part(b"message body"),
                part(
                    text.encode(),
                    "application/ics",
                    filename="invite.bin",
                    headers=[
                        {"name": "Content-Type", "value": "application/ics; charset=gb2312"},
                    ],
                ),
                part(
                    "Diagnostic-Code: smtp; café\r\n".encode("latin-1"),
                    "message/delivery-status",
                    headers=[
                        {
                            "name": "Content-Type",
                            "value": "message/delivery-status; charset=iso-8859-1",
                        },
                    ],
                ),
                part(b"Subject: unchanged\r\n\tfolded\r\n", "text/rfc822-headers"),
            ],
        }
    )
    before = source.model_dump()
    mapped = await map_gmail(source, AsyncMock(), tmp_path)
    registry = ConverterRegistry()
    extracted = []
    for item in mapped.parts[1:]:
        path = item.entity.local_path
        converter = registry.for_extension(item.part.extension)
        extracted.append((await converter.convert_batch([path]))[path].text)
    assert extracted == [
        text,
        "Diagnostic-Code: smtp; café\r\n",
        "Subject: unchanged\r\n\tfolded\r\n",
    ]
    assert mapped.parts[1].part.charset_recoveries[0].from_charset == "gb2312"
    assert registry.for_extension(".bin") is None
    assert source.model_dump() == before


@pytest.mark.asyncio
async def test_invalid_inert_attachment_does_not_discard_parent_body(tmp_path):
    from airweave.domains.converters.strict_text import StrictTextConverter

    source = record(
        {
            "mimeType": "multipart/mixed",
            "parts": [
                part(b"valid parent"),
                part(b"\xff\xfe", "text/calendar"),
            ],
        }
    )
    mapped = await map_gmail(source, AsyncMock(), tmp_path)
    assert "valid parent" in next(tmp_path.glob("*.html")).read_text()
    assert mapped.parts[1].omission == "conversion_failed"
    assert mapped.parts[1].entity is None
    invalid = tmp_path / "invalid.ics"
    invalid.write_bytes(b"\xff")
    assert (await StrictTextConverter().convert_batch([str(invalid)]))[str(invalid)].text is None
