"""Immutable Drive bodies captured before a record may claim content completeness."""

from urllib.parse import quote, urlencode

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
