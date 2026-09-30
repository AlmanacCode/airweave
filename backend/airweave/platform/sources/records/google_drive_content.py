"""Immutable Drive bodies captured before a record may claim content completeness."""

from urllib.parse import quote, urlencode

import httpx

from airweave.core.logging import ContextualLogger
from airweave.domains.entities.canonical.requests import CaptureRecord
from airweave.domains.sources.token_providers.protocol import SourceAuthProvider
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.platform.entities.google_drive import GOOGLE_EXPORT_FORMATS
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.sources.records.google_drive import BASE, GetJSON


async def capture_file_content(
    record: CaptureRecord,
    *,
    files: FileService,
    get: GetJSON,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
    logger: ContextualLogger,
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
    if isinstance(capabilities, dict) and capabilities.get("canDownload") is False:
        return record
    url = f"{BASE}/files/{quote(record.identity.native_id, safe='')}"
    if mime.startswith("application/vnd.google-apps."):
        export = GOOGLE_EXPORT_FORMATS.get(mime)
        if export is None:
            return record
        media_type = export[0]
        download_url = url + "/export?" + urlencode({"mimeType": media_type})
    else:
        media_type = mime
        download_url = url + "?alt=media&supportsAllDrives=true"
    try:
        blob = await files.capture_canonical_url(
            url=download_url, client=client, auth=auth, logger=logger, media_type=media_type
        )
    except httpx.HTTPStatusError as error:
        if not _export_limit_reached(error, mime):
            raise
        logger.info("Drive export exceeds provider size limit; retained metadata only")
        return record
    except FileSkippedException:
        # Declared/streamed size limits are a visible metadata-only record, not absent data.
        return record
    latest = await get(url, params={"fields": "version", "supportsAllDrives": "true"})
    if latest.get("version") != version:
        raise ValueError(
            "Drive file changed during content capture; retry before advancing checkpoint"
        )
    return record.model_copy(
        update={"blobs": (blob,), "content_hash": blob.sha256, "completeness": "complete"}
    )


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
