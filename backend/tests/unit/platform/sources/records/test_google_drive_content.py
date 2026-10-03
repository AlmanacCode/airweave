"""Body completeness requires durable bytes matching the observed file version."""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from airweave.domains.entities.canonical.requests import BlobReference
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.platform.sources.records.google_drive import file_record
from airweave.platform.sources.records.google_drive_content import capture_file_content


@pytest.fixture
def inputs():
    blob = BlobReference(
        key="canonical/test/blobs/sha256/" + "a" * 64,
        sha256="a" * 64,
        size_bytes=3,
        media_type="text/plain",
    )

    async def store(content, *, media_type):
        digest = hashlib.sha256(content).hexdigest()
        return BlobReference(
            key="canonical/test/blobs/sha256/" + digest,
            sha256=digest,
            size_bytes=len(content),
            media_type=media_type,
        )

    return {
        "files": SimpleNamespace(
            capture_canonical_url=AsyncMock(return_value=blob),
            store_canonical_blob=AsyncMock(side_effect=store),
        ),
        "get": AsyncMock(return_value={"version": "7"}),
        "client": object(),
        "auth": object(),
        "logger": Mock(),
    }


async def test_complete_body_keeps_original_metadata_and_blob(inputs):
    record = file_record(
        {"id": "file/id", "mimeType": "text/plain", "version": "7", "name": "notes"}
    )
    result = await capture_file_content(record, **inputs)
    assert result.completeness == "complete" and result.blobs[0].size_bytes == 3
    assert result.blobs[0].filename == "notes"
    assert result.payload == record.payload and result.content_hash == "a" * 64
    assert "file%2Fid" in inputs["files"].capture_canonical_url.call_args.kwargs["url"]


async def test_changed_version_cannot_claim_complete(inputs):
    inputs["get"].return_value = {"version": "8"}
    record = file_record({"id": "file", "mimeType": "text/plain", "version": "7"})
    with pytest.raises(ValueError, match="changed during content capture"):
        await capture_file_content(record, **inputs)


async def test_oversized_body_remains_explicitly_metadata_only(inputs):
    inputs["files"].capture_canonical_url.side_effect = FileSkippedException(
        reason="too large", filename="file"
    )
    record = file_record({"id": "file", "mimeType": "text/plain", "version": "7"})
    result = await capture_file_content(record, **inputs)
    assert result.completeness == "metadata_only" and len(result.blobs) == 1
    manifest = json.loads(inputs["files"].store_canonical_blob.call_args.args[0])
    assert manifest["export"] == {
        "status": "unavailable",
        "blob": None,
        "reason": "read_size_limit",
    }
    assert result.payload == record.payload
    inputs["get"].assert_awaited_once()


async def test_storage_failure_is_not_silently_a_partial_success(inputs):
    inputs["files"].capture_canonical_url.side_effect = ConnectionError("storage unavailable")
    record = file_record({"id": "file", "mimeType": "text/plain", "version": "7"})
    with pytest.raises(ConnectionError):
        await capture_file_content(record, **inputs)


async def test_native_spreadsheet_keeps_existing_export_only_format(inputs):
    record = file_record(
        {
            "id": "doc",
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "version": "7",
            "name": "बजट",
        }
    )
    result = await capture_file_content(record, **inputs)
    assert result.blobs[0].filename == "बजट.xlsx"
    call = inputs["files"].capture_canonical_url.call_args.kwargs
    assert "/export?" in call["url"]
    assert call["media_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@pytest.mark.parametrize(
    "reason", ["exportSizeLimitExceeded", "insufficientPermissions", "unknown"]
)
async def test_provider_export_limit_is_narrow_and_preserves_native_payload(inputs, reason):
    response = httpx.Response(
        403,
        json={"error": {"errors": [{"reason": reason}]}},
        request=httpx.Request("GET", "https://www.googleapis.com/drive/v3/files/test/export"),
    )
    failure = httpx.HTTPStatusError(
        "provider rejection", request=response.request, response=response
    )
    inputs["files"].capture_canonical_url.side_effect = failure
    record = file_record(
        {"id": "doc", "mimeType": "application/vnd.google-apps.spreadsheet", "version": "7"}
    )
    if reason != "exportSizeLimitExceeded":
        with pytest.raises(httpx.HTTPStatusError):
            await capture_file_content(record, **inputs)
        return
    result = await capture_file_content(record, **inputs)
    assert result.completeness == "metadata_only" and len(result.blobs) == 1
    manifest = json.loads(inputs["files"].store_canonical_blob.call_args.args[0])
    assert manifest["export"]["reason"] == "export_size_limit"
    assert result.payload == record.payload
    inputs["get"].assert_awaited_once()


def test_unnamed_blob_preserves_historical_descriptor_and_named_blob_roundtrips():
    old = {
        "key": "blob",
        "sha256": "a" * 64,
        "size_bytes": 3,
        "media_type": None,
        "source_path": None,
    }
    blob = BlobReference.model_validate(old)
    assert blob.model_dump(mode="json") == old
    named = blob.model_copy(update={"filename": "../बजट.pdf"})
    assert BlobReference.model_validate_json(named.model_dump_json()).filename == "../बजट.pdf"
    assert named.key == blob.key and named.sha256 == blob.sha256


@pytest.mark.parametrize(
    "mime,capabilities,reason",
    [
        (
            "application/vnd.google-apps.presentation",
            {"canDownload": False},
            "download_not_permitted",
        ),
        ("application/vnd.google-apps.unknown", {}, "unsupported"),
    ],
)
async def test_declared_omission_is_retained_and_version_checked(
    inputs, mime, capabilities, reason
):
    record = file_record(
        {"id": "file", "version": "7", "mimeType": mime, "capabilities": capabilities}
    )
    result = await capture_file_content(record, **inputs)
    manifest = json.loads(inputs["files"].store_canonical_blob.call_args.args[0])
    assert manifest["export"]["reason"] == reason
    assert result.payload == record.payload and result.completeness == "metadata_only"
    inputs["files"].capture_canonical_url.assert_not_awaited()
    inputs["get"].return_value = {"version": "8"}
    with pytest.raises(ValueError, match="changed during content capture"):
        await capture_file_content(record, **inputs)


async def test_changed_version_rejects_export_omission(inputs):
    inputs["files"].capture_canonical_url.side_effect = FileSkippedException(
        reason="large", filename="file"
    )
    inputs["get"].return_value = {"version": "8"}
    record = file_record(
        {"id": "file", "version": "7", "mimeType": "application/vnd.google-apps.presentation"}
    )
    with pytest.raises(ValueError, match="changed during content capture"):
        await capture_file_content(record, **inputs)


@pytest.mark.parametrize("status", [401, 429, 500])
async def test_auth_rate_and_provider_failures_never_become_export_gaps(inputs, status):
    response = httpx.Response(
        status,
        json={"error": {"errors": [{"reason": "exportSizeLimitExceeded"}]}},
        request=httpx.Request("GET", "https://provider.test/export"),
    )
    inputs["files"].capture_canonical_url.side_effect = httpx.HTTPStatusError(
        "failure", request=response.request, response=response
    )
    record = file_record(
        {"id": "file", "version": "7", "mimeType": "application/vnd.google-apps.presentation"}
    )
    with pytest.raises(httpx.HTTPStatusError):
        await capture_file_content(record, **inputs)
    inputs["files"].store_canonical_blob.assert_not_awaited()
    inputs["get"].assert_not_awaited()
