"""Immutable Drive bodies captured before a record may claim content completeness."""

from urllib.parse import quote, urlencode

import httpx
from airweave.core.logging import ContextualLogger
from airweave.domains.entities.canonical.requests import BlobReference, CaptureRecord
from airweave.domains.sources.token_providers.protocol import SourceAuthProvider
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.platform.entities.google_drive import GOOGLE_EXPORT_FORMATS
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.sources.records.export_manifest import (
    ExportManifestV3,
    parse_export_manifest,
)
from airweave.platform.sources.records.google_docs_content import capture_document_parts
from airweave.platform.sources.records.google_drive import BASE, GetJSON
from airweave.platform.sources.records.google_sheets_content import capture_spreadsheet_parts
from airweave.platform.sources.records.sheets_manifest import SHEETS_MIME
from airweave.platform.sources.records.workspace_manifest import (
    DOCS_MIME,
    MANIFEST_MIME,
    ExportState,
    canonical_json,
)


async def capture_file_content(
    record: CaptureRecord,
    *,
    files: FileService,
    get: GetJSON,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
    logger: ContextualLogger,
    capture_native_sheets: bool = False,
) -> CaptureRecord:
    """Reuse downloads/storage; never pair bytes with metadata from another file version."""
    if record.kind == "delete" or record.completeness == "complete":
        return record
    payload = record.payload
    mime = payload.get("mimeType")
    version = payload.get("version")
    if not isinstance(mime, str) or not isinstance(version, str):
        return record
    capabilities = payload.get("capabilities")
    url = f"{BASE}/files/{quote(record.identity.native_id, safe='')}"
    blob, export_state = await _capture_representation(
        url, mime, capabilities, files=files, client=client, auth=auth, logger=logger
    )
    blob = _label_representation(blob, payload.get("name"), mime)
    has_native_parts = False
    if export_state.reason in {"unsupported", "download_not_permitted"}:
        record = await _capture_export_coverage(record, export_state, files=files)
        has_native_parts = True
    elif mime == DOCS_MIME:
        record = await capture_document_parts(
            record,
            export=export_state,
            export_blob=blob,
            files=files,
            client=client,
            auth=auth,
        )
        has_native_parts = True
    elif mime == SHEETS_MIME and capture_native_sheets:
        record = await capture_spreadsheet_parts(
            record, export=export_state, export_blob=blob, files=files, client=client, auth=auth
        )
        has_native_parts = True
    elif blob is None:
        record = await _capture_export_coverage(record, export_state, files=files)
        has_native_parts = True
    latest = await get(url, params={"fields": "version", "supportsAllDrives": "true"})
    if latest.get("version") != version:
        raise ValueError(
            "Drive file changed during content capture; retry before advancing checkpoint"
        )
    if has_native_parts:
        return record
    return record.model_copy(
        update={"blobs": (blob,), "content_hash": blob.sha256, "completeness": "complete"}
    )


async def _capture_representation(
    url: str,
    mime: str,
    capabilities: object,
    *,
    files: FileService,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
    logger: ContextualLogger,
) -> tuple[BlobReference | None, ExportState]:
    """Distinguish declared acquisition limitations from provider or storage failures."""
    target = _download_target(url, mime)
    if isinstance(capabilities, dict) and capabilities.get("canDownload") is False:
        blob, export_state = (
            None,
            ExportState(status="unavailable", reason="download_not_permitted"),
        )
    elif target is None:
        blob, export_state = None, ExportState(status="unavailable", reason="unsupported")
    else:
        download_url, media_type = target
        blob, export_state = await _download_representation(
            download_url, media_type, mime, files=files, client=client, auth=auth, logger=logger
        )
    return blob, export_state


async def _capture_export_coverage(
    record: CaptureRecord, export: ExportState, *, files: FileService
) -> CaptureRecord:
    """Retain omission evidence without mutating the original provider observation."""
    manifest = ExportManifestV3(
        file_id=record.identity.native_id,
        drive_version=record.payload["version"],
        media_type=record.payload["mimeType"],
        export=export,
    )
    content = canonical_json(manifest.model_dump(mode="json"))
    blob = (await files.store_canonical_blob(content, media_type=MANIFEST_MIME)).model_copy(
        update={"role": "representation_manifest"}
    )
    parse_export_manifest(
        content,
        file_id=manifest.file_id,
        drive_version=manifest.drive_version,
        media_type=manifest.media_type,
        blobs=(blob,),
    )
    return CaptureRecord.model_validate(
        record.model_dump()
        | {"blobs": (blob,), "content_hash": blob.sha256, "completeness": "metadata_only"}
    )


def _label_representation(
    blob: BlobReference | None, name: object, mime: str
) -> BlobReference | None:
    """Native document titles need the selected export extension; binary names are exact."""
    if blob is None or not isinstance(name, str) or not name:
        return blob
    export = GOOGLE_EXPORT_FORMATS.get(mime)
    filename = name + export[1] if export and not name.lower().endswith(export[1]) else name
    return blob.model_copy(update={"filename": filename})


def _download_target(url: str, mime: str) -> tuple[str, str] | None:
    if not mime.startswith("application/vnd.google-apps."):
        return url + "?alt=media&supportsAllDrives=true", mime
    export = GOOGLE_EXPORT_FORMATS.get(mime)
    if export is None:
        return None
    return url + "/export?" + urlencode({"mimeType": export[0]}), export[0]


async def _download_representation(
    url: str,
    media_type: str,
    mime: str,
    *,
    files: FileService,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
    logger: ContextualLogger,
) -> tuple[BlobReference | None, ExportState]:
    """Export-size limits are supported gaps; unrelated provider failures propagate."""
    try:
        blob = await files.capture_canonical_url(
            url=url, client=client, auth=auth, logger=logger, media_type=media_type
        )
    except httpx.HTTPStatusError as error:
        if not _export_limit_reached(error, mime):
            raise
        return None, ExportState(status="unavailable", reason="export_size_limit")
    except FileSkippedException:
        return None, ExportState(status="unavailable", reason="read_size_limit")
    return blob, ExportState(status="retained", blob=blob.sha256)


def _export_limit_reached(error: httpx.HTTPStatusError, mime: str) -> bool:
    """Only the explicit native-export size failure permits retaining metadata."""
    if error.response.status_code != 403 or not mime.startswith("application/vnd.google-apps."):
        return False
    try:
        payload = error.response.json()
    except ValueError:
        return False
    if not isinstance(payload, dict) or not isinstance(payload.get("error"), dict):
        return False
    errors = payload["error"].get("errors")
    return (
        isinstance(errors, list)
        and bool(errors)
        and all(
            isinstance(item, dict) and item.get("reason") == "exportSizeLimitExceeded"
            for item in errors
        )
    )
