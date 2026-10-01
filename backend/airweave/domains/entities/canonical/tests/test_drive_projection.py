"""Drive projection preserves retained representations and explicit content gaps."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.entities.canonical.projection_mappers import map_record
from airweave.domains.entities.canonical.projection_models import ProjectionBinding, ProjectionWork
from airweave.domains.entities.canonical.projector import _select_inputs
from airweave.domains.entities.canonical.requests import BlobReference, RecordIdentity
from airweave.platform.sources.records.workspace_manifest import (
    DOCS_MIME,
    MANIFEST_MIME,
    DocsGap,
    DocsState,
    ExportState,
    WorkspaceManifestV1,
    canonical_json,
)


def original(mime=DOCS_MIME):
    return SourceRecord(
        id=uuid4(),
        sync_id=uuid4(),
        identity=RecordIdentity(record_type="file", native_id="doc"),
        revision=1,
        payload={"id": "doc", "version": "7", "name": "document", "mimeType": mime},
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


def retain(source, content, media_type, *, role=None):
    digest = hashlib.sha256(content).hexdigest()
    return BlobReference(
        key=f"canonical/{source.sync_id}/blobs/sha256/{digest}",
        sha256=digest,
        size_bytes=len(content),
        media_type=media_type,
        role=role,
    )


def docs_record(*, native_available):
    source = original()
    if native_available:
        content = canonical_json(
            {
                "documentId": "doc",
                "tabs": [
                    {
                        "tabProperties": {"tabId": "tab"},
                        "documentTab": {
                            "body": {
                                "content": [
                                    {
                                        "paragraph": {
                                            "elements": [
                                                {"textRun": {"content": "retained authored text"}},
                                                {
                                                    "inlineObjectElement": {
                                                        "inlineObjectId": "image"
                                                    }
                                                },
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                    }
                ],
            }
        )
        ref = retain(source, content, "application/json")
        native = DocsState(
            status="complete",
            document_blob=ref.sha256,
            embedded_media="not_retained",
            missing=(
                DocsGap(
                    source_path="/tabs/0/documentTab/inlineObjects/image/embeddedObject",
                    reason="unsupported_media",
                ),
            ),
        )
        export = ExportState(status="unavailable", reason="export_size_limit")
    else:
        content = b"retained DOCX bytes for the downstream converter"
        ref = retain(
            source,
            content,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        native = DocsState(
            status="unavailable",
            embedded_media="not_retained",
            missing=(DocsGap(reason="read_size_limit"),),
        )
        export = ExportState(status="retained", blob=ref.sha256)
    manifest = WorkspaceManifestV1(file_id="doc", drive_version="7", export=export, native=native)
    raw = canonical_json(manifest.model_dump(mode="json"))
    marked = retain(source, raw, MANIFEST_MIME, role="representation_manifest")
    source = source.model_copy(update={"blobs": (ref, marked)})
    contents = {ref.key: content, marked.key: raw}
    storage = AsyncMock()
    storage.read_file.side_effect = lambda key, **kwargs: contents[key]
    return source, storage, content


def coverage(result, source):
    work = ProjectionWork(
        binding=ProjectionBinding(
            source_connection_id=uuid4(), source_name="google_drive", collection_id=uuid4()
        ),
        organization_id=uuid4(),
        record=source,
        pipeline_version=2,
        previous_generation=None,
    )
    return _select_inputs(result, work, "google_drive", uuid4(), lambda _: True)[1]


@pytest.mark.asyncio
async def test_docs_text_keeps_missing_media_in_extraction_parts():
    source, storage, _ = docs_record(native_available=True)
    before = source.model_dump()
    async with map_record(source, "google_drive", storage) as result:
        assert len(result.parts) == 2
        assert "retained authored text" in Path(result.entities[0].local_path).read_text()
        missing = result.parts[1]
        assert missing.entity is None and missing.omission is None
        assert missing.part.key == "/tabs/0/documentTab/inlineObjects/image/embeddedObject"
        assert missing.part.part_index == 1
        evidence = coverage(result, source)
        assert evidence.status == "partial"
        assert [part.outcome for part in evidence.parts] == ["indexed", "unavailable_original"]
    assert source.model_dump() == before
    assert storage.read_file.await_count == 2


@pytest.mark.asyncio
async def test_docs_uses_retained_export_when_native_capture_unavailable():
    source, storage, content = docs_record(native_available=False)
    before = source.model_dump()
    async with map_record(source, "google_drive", storage) as result:
        entity = result.entities[0]
        assert Path(entity.local_path).suffix == ".docx"
        assert Path(entity.local_path).read_bytes() == content
        assert result.parts[1].entity is None and result.parts[1].part.key == "/native"
        assert coverage(result, source).status == "partial"
    assert source.model_dump() == before and source.completeness == "partial"
    assert storage.read_file.await_count == 2


@pytest.mark.asyncio
async def test_unknown_extensionless_original_is_unsupported_not_conversion_failure():
    source = original("application/x-almanac-unknown-binary")
    content = b"retained opaque original"
    ref = retain(source, content, source.payload["mimeType"])
    source = source.model_copy(update={"blobs": (ref,), "completeness": "complete"})
    storage = AsyncMock()
    storage.read_file.return_value = content
    before = source.model_dump()
    async with map_record(source, "google_drive", storage) as result:
        assert len(result.parts) == 1 and not result.entities
        assert result.parts[0].omission == "unsupported_format"
        assert coverage(result, source).status == "unavailable"
    assert source.model_dump() == before
    storage.read_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_docs_without_native_or_export_publishes_only_unavailable_coverage():
    source = original()
    manifest = WorkspaceManifestV1(
        file_id="doc",
        drive_version="7",
        export=ExportState(status="unavailable", reason="export_size_limit"),
        native=DocsState(
            status="unavailable",
            embedded_media="not_retained",
            missing=(DocsGap(reason="read_size_limit"),),
        ),
    )
    raw = canonical_json(manifest.model_dump(mode="json"))
    marked = retain(source, raw, MANIFEST_MIME, role="representation_manifest")
    source = source.model_copy(update={"blobs": (marked,)})
    before = source.model_dump()
    storage = AsyncMock()
    storage.read_file.return_value = raw
    async with map_record(source, "google_drive", storage) as result:
        assert not result.entities
        assert len(result.parts) == 1 and result.parts[0].part.part_index == 0
        evidence = coverage(result, source)
        assert evidence.status == "unavailable"
        assert evidence.parts[0].outcome == "unavailable_original"
    assert source.model_dump() == before and source.completeness == "partial"
    storage.read_file.assert_awaited_once()
