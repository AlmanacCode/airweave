"""Native Docs representation preserves structure and yields one deliberate search view."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.domains.entities.canonical.workspace_docs import DocumentReadError, read_document


def paragraph(text):
    return {"paragraph": {"elements": [{"textRun": {"content": text}}]}}


def document():
    return {
        "documentId": "doc",
        "title": "Native title",
        "futureDocumentField": {"keep": True},
        "tabs": [
            {
                "tabProperties": {"tabId": "first", "title": "First"},
                "documentTab": {
                    "body": {
                        "content": [
                            paragraph("Body"),
                            {
                                "table": {
                                    "tableRows": [
                                        {
                                            "tableCells": [{"content": [paragraph("Cell")]}],
                                        }
                                    ]
                                }
                            },
                        ]
                    },
                    "headers": {"h": {"content": [paragraph("Header")]}},
                    "footers": {"f": {"content": [paragraph("Footer")]}},
                    "footnotes": {"n": {"content": [paragraph("Footnote")]}},
                },
                "childTabs": [
                    {
                        "tabProperties": {"tabId": "child", "title": "Nested"},
                        "documentTab": {"body": {"content": [paragraph("Child content")]}},
                        "unknownTabProperty": 12,
                    }
                ],
            }
        ],
    }


def capture(native=None, *, schema_version=1):
    native = document() if native is None else native
    sync = uuid4()
    contents = {}

    def blob(value, media_type="application/json", role=None):
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        digest = hashlib.sha256(data).hexdigest()
        key = f"canonical/{sync}/blobs/sha256/{digest}"
        contents[key] = data
        return BlobReference(
            key=key, sha256=digest, size_bytes=len(data), media_type=media_type, role=role
        )

    native_blob = blob(native)
    export = blob(b"NOT THE SEARCH BODY", "application/octet-stream")
    manifest = blob(
        {
            "schema_version": schema_version,
            "file_id": "doc",
            "drive_version": "42",
            "export": {"status": "retained", "blob": export.sha256, "reason": None},
            "native": {
                "kind": "docs",
                "status": "complete",
                "document_blob": native_blob.sha256,
                "include_tabs_content": True,
                "suggestions_view": "DEFAULT_FOR_CURRENT_ACCESS",
                "comments_view": "COMMENTS_VIEW_MODE_OMITTED",
                "embedded_media": "not_retained",
                "media": [],
                "missing": [],
            },
        },
        role="representation_manifest",
    )
    record = SourceRecord(
        id=uuid4(),
        sync_id=sync,
        identity=RecordIdentity(record_type="file", native_id="doc"),
        revision=1,
        payload={
            "id": "doc",
            "name": "Document",
            "version": "42",
            "mimeType": "application/vnd.google-apps.document",
        },
        payload_schema_version=1,
        capture_hash="hash",
        content_hash=manifest.sha256,
        completeness="complete",
        observed_at=datetime.now(timezone.utc),
        source_created_at=None,
        source_updated_at=None,
        deleted_at=None,
        removal_reason=None,
        blobs=(export, native_blob, manifest),
        indexed_revision=None,
        indexed_pipeline_version=None,
    )
    storage = AsyncMock()

    async def read(key, *, max_bytes):
        assert len(contents[key]) <= max_bytes
        return contents[key]

    storage.read_file.side_effect = read
    return record, storage, contents


@pytest.mark.asyncio
async def test_recursive_tabs_preserve_original_structure_without_edit_revision():
    record, storage, _ = capture()
    result = await read_document(record, storage)
    assert result.document == document()
    assert "revisionId" not in result.document
    assert result.tab("child") == document()["tabs"][0]["childTabs"][0]
    with pytest.raises(DocumentReadError, match="not present"):
        result.tab("missing")
    text = result.text()
    for expected in ("Body", "Cell", "Header", "Footer", "Footnote", "Child content"):
        assert text.count(expected) == 1


@pytest.mark.asyncio
async def test_projection_reads_native_once_and_never_indexes_export():
    record, storage, _ = capture()
    async with map_record(record, "google_drive", storage) as entities:
        assert len(entities.entities) == 2
        assert entities.parts[-1].part.kind == "metadata"
        entity = entities.parts[0].entity
        assert entity.file_id == "doc"
        assert entity.mime_type == "text/plain"
        text = Path(entity.local_path).read_text()
        assert "Child content" in text and "NOT THE SEARCH BODY" not in text
    assert all(call.args[0] != record.blobs[0].key for call in storage.read_file.call_args_list)


@pytest.mark.asyncio
async def test_unknown_text_shape_keeps_original_read_but_fails_projection():
    native = document()
    native["tabs"][0]["documentTab"]["body"]["content"].append(
        {
            "paragraph": {"elements": [{"futureText": {"content": "Do not silently lose"}}]},
        }
    )
    record, storage, _ = capture(native)
    assert (await read_document(record, storage)).document == native
    with pytest.raises(DocumentReadError, match="Unsupported"):
        async with map_record(record, "google_drive", storage):
            pass


@pytest.mark.asyncio
async def test_unavailable_record_does_not_read_storage():
    record, storage, _ = capture()
    with pytest.raises(DocumentReadError, match="unavailable"):
        await read_document(record.model_copy(update={"content_access": "unavailable"}), storage)
    storage.read_file.assert_not_called()


@pytest.mark.asyncio
async def test_native_bytes_are_verified_not_trusted_from_manifest():
    record, storage, contents = capture()
    contents[record.blobs[1].key] = b"corrupt"
    with pytest.raises(ValueError, match="size and digest"):
        await read_document(record, storage)


@pytest.mark.asyncio
async def test_manifest_cannot_resolve_a_blob_outside_its_record():
    record, storage, _ = capture()
    record = record.model_copy(update={"blobs": (record.blobs[0], record.blobs[2])})
    with pytest.raises(ValueError, match="account"):
        await read_document(record, storage)
    assert storage.read_file.await_count == 1


@pytest.mark.asyncio
async def test_native_document_identity_must_match_drive_original():
    native = document()
    native["documentId"] = "other-file"
    record, storage, _ = capture(native)
    with pytest.raises(ValueError, match="another file"):
        await read_document(record, storage)


@pytest.mark.asyncio
async def test_duplicate_tab_ids_are_not_resolved_by_first_match():
    native = document()
    native["tabs"][0]["childTabs"][0]["tabProperties"]["tabId"] = "first"
    record, storage, _ = capture(native)
    with pytest.raises(ValueError, match="duplicated"):
        await read_document(record, storage)


@pytest.mark.asyncio
async def test_drive_version_mismatch_does_not_read_native_response():
    record, storage, _ = capture()
    record = record.model_copy(update={"payload": {**record.payload, "version": "43"}})
    with pytest.raises(ValueError, match="version"):
        await read_document(record, storage)
    assert storage.read_file.await_count == 1


@pytest.mark.asyncio
async def test_smart_chip_display_text_is_used_without_following_link():
    native = document()
    native["tabs"][0]["documentTab"]["body"]["content"] = [
        {
            "paragraph": {
                "elements": [
                    {
                        "person": {
                            "personProperties": {"name": "Alice", "email": "private@example.com"}
                        }
                    },
                    {"textRun": {"content": " and "}},
                    {"person": {"personProperties": {"email": "bob@example.com"}}},
                    {"textRun": {"content": " read "}},
                    {
                        "richLink": {
                            "richLinkProperties": {
                                "title": "Roadmap",
                                "uri": "https://untrusted.example/never-fetch",
                            }
                        }
                    },
                ]
            }
        }
    ]
    record, storage, _ = capture(native)
    result = await read_document(record, storage)
    assert "Alice and bob@example.com read Roadmap" in result.text()
    assert "untrusted.example" not in result.text()
    assert "private@example.com" not in result.text()
    assert storage.read_file.await_count == 2


@pytest.mark.asyncio
async def test_equation_is_retained_but_not_invented_as_search_text():
    native = document()
    native["tabs"][0]["documentTab"]["body"]["content"] = [
        {"paragraph": {"elements": [{"equation": {}}]}},
    ]
    record, storage, _ = capture(native)
    result = await read_document(record, storage)
    assert result.document == native
    with pytest.raises(DocumentReadError, match="equation text"):
        result.text()


@pytest.mark.asyncio
async def test_unsupported_manifest_never_falls_back_to_export():
    record, storage, _ = capture(schema_version=2)
    with pytest.raises(ValueError):
        async with map_record(record, "google_drive", storage):
            pass
    assert storage.read_file.await_count == 1
    assert storage.read_file.call_args.args[0] == record.blobs[2].key


@pytest.mark.asyncio
async def test_incomplete_native_fields_are_not_published_as_complete_text():
    record, storage, _ = capture()
    result = await read_document(record, storage)
    native = result.manifest.native.model_copy(update={"status": "partial"})
    partial = result.model_copy(
        update={"manifest": result.manifest.model_copy(update={"native": native})}
    )
    assert partial.tab("child")["tabProperties"]["tabId"] == "child"
    with pytest.raises(DocumentReadError, match="incomplete"):
        partial.text()
