"""Real bounded HTTP acquisition and immutable filesystem storage; no provider calls."""

import json
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceRateLimitError,
    SourceServerError,
)
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.file_service import FileService
from airweave.platform.sources.records import google_docs_content
from airweave.platform.sources.records.google_drive import file_record
from airweave.platform.sources.records.google_drive_content import capture_file_content
from airweave.platform.sources.records.workspace_manifest import parse_manifest

DOCUMENT = {
    "documentId": "doc",
    "title": "All tabs",
    "tabs": [
        {
            "tabProperties": {"tabId": "one"},
            "documentTab": {"body": {"content": []}},
            "childTabs": [{"tabProperties": {"tabId": "two"}, "documentTab": {"unknown": "kept"}}],
        }
    ],
}


@pytest.fixture
def files(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir",
        lambda _: str(tmp_path / "temp"),
    )
    return FileService(uuid4(), FilesystemBackend(tmp_path / "storage"), sync_id=uuid4())


async def acquire(files, handler, *, version="7"):
    record = file_record(
        {"id": "doc", "mimeType": "application/vnd.google-apps.document", "version": "7"}
    )
    get = AsyncMock(return_value={"version": version})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await capture_file_content(
            record,
            files=files,
            get=get,
            client=client,
            auth=StaticTokenProvider("test"),
            logger=Mock(),
        )
    return result, get


def response(request):
    if request.url.host == "docs.googleapis.com":
        assert request.url.params["includeTabsContent"] == "true"
        assert request.url.params["commentsViewMode"] == "COMMENTS_VIEW_MODE_OMITTED"
        assert request.url.params["suggestionsViewMode"] == "DEFAULT_FOR_CURRENT_ACCESS"
        assert "fields" not in request.url.params
        return httpx.Response(200, json=DOCUMENT)
    return httpx.Response(200, content=b"synthetic-docx")


async def manifest_for(record, files):
    marked = next(blob for blob in record.blobs if blob.role)
    content = await files.storage.read_file(marked.key)
    return parse_manifest(content, file_id="doc", drive_version="7", blobs=record.blobs)


async def test_all_tabs_native_and_export_are_published_together_and_deterministic(files):
    first, get = await acquire(files, response)
    second, _ = await acquire(files, response)
    assert first.blobs == second.blobs and first.content_hash == second.content_hash
    assert len(first.blobs) == 3 and first.completeness == "complete"
    assert first.payload == {
        "id": "doc",
        "mimeType": "application/vnd.google-apps.document",
        "version": "7",
    }
    manifest = await manifest_for(first, files)
    native = next(blob for blob in first.blobs if blob.sha256 == manifest.native.document_blob)
    assert json.loads(await files.storage.read_file(native.key)) == DOCUMENT
    assert get.await_count == 1


async def test_changed_drive_version_rejects_staged_representations(files):
    with pytest.raises(ValueError, match="changed during"):
        await acquire(files, response, version="8")
    # Uncommitted immutable bytes can remain; no source blob GC is claimed.
    assert len(await files.storage.list_files()) == 3


async def test_native_success_survives_explicit_export_size_limit(files):
    def limited(request):
        if request.url.host == "docs.googleapis.com":
            return response(request)
        return httpx.Response(
            403, json={"error": {"errors": [{"reason": "exportSizeLimitExceeded"}]}}
        )

    record, _ = await acquire(files, limited)
    manifest = await manifest_for(record, files)
    assert record.completeness == "complete"
    assert manifest.export.reason == "export_size_limit" and manifest.native.status == "complete"


async def test_embedded_objects_are_explicitly_partial_not_downloaded(files):
    def media(request):
        if request.url.host == "docs.googleapis.com":
            return httpx.Response(
                200,
                json={
                    **DOCUMENT,
                    "inlineObjects": {
                        "image": {
                            "embeddedObject": {
                                "imageProperties": {"contentUri": "https://foreign.invalid/image"}
                            }
                        }
                    },
                },
            )
        return response(request)

    record, _ = await acquire(files, media)
    manifest = await manifest_for(record, files)
    assert record.completeness == "partial"
    assert manifest.native.status == "complete" and manifest.native.embedded_media == "not_retained"
    assert manifest.native.missing[0].source_path == "/inlineObjects/image/embeddedObject"


@pytest.mark.parametrize(
    "status,error", [(401, SourceAuthError), (429, SourceRateLimitError), (500, SourceServerError)]
)
async def test_oversized_error_is_never_successful_native_gap(files, monkeypatch, status, error):
    monkeypatch.setattr(google_docs_content, "MAX_DOCS_BYTES", 20)

    def failure(request):
        if request.url.host == "docs.googleapis.com":
            return httpx.Response(status, content=b"x" * 100, headers={"Retry-After": "300"})
        return response(request)

    with pytest.raises(error):
        await acquire(files, failure)


async def test_successful_oversize_records_explicit_native_gap(files, monkeypatch):
    monkeypatch.setattr(google_docs_content, "MAX_DOCS_BYTES", 20)
    record, _ = await acquire(files, response)
    manifest = await manifest_for(record, files)
    assert record.completeness == "partial"
    assert (
        manifest.native.document_blob is None
        and manifest.native.missing[0].reason == "read_size_limit"
    )


async def test_foreign_document_and_storage_failure_do_not_publish(files):
    def foreign(request):
        if request.url.host == "docs.googleapis.com":
            return httpx.Response(200, json={**DOCUMENT, "documentId": "foreign"})
        return response(request)

    with pytest.raises(ValueError, match="another file"):
        await acquire(files, foreign)
    files.store_canonical_blob = AsyncMock(side_effect=ConnectionError("storage unavailable"))
    with pytest.raises(ConnectionError):
        await acquire(files, response)


async def test_identical_part_bytes_share_one_workspace_descriptor_without_losing_uses(files):
    from airweave.platform.sources.records.workspace_manifest import canonical_json

    def identical(request):
        return httpx.Response(200, content=canonical_json(DOCUMENT))

    record, _ = await acquire(files, identical)
    manifest = await manifest_for(record, files)
    assert len(record.blobs) == 2
    assert manifest.export.blob == manifest.native.document_blob
    part = next(blob for blob in record.blobs if blob.role is None)
    assert part.media_type == "application/octet-stream"
