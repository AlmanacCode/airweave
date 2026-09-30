"""Bounded native Docs acquisition under the owning Drive file transaction."""

import json
from urllib.parse import quote

import httpx
from pydantic import JsonValue, TypeAdapter

from airweave.domains.entities.canonical.requests import BlobReference, CaptureRecord
from airweave.domains.sources.token_providers.protocol import (
    SourceAuthProvider,
    authorization_headers,
)
from airweave.domains.storage.exceptions import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.http_client.bounded_response import bounded_response_bytes
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.platform.sources.records.workspace_manifest import (
    MANIFEST_MIME,
    DocsGap,
    DocsState,
    ExportState,
    WorkspaceManifestV1,
    canonical_json,
    document_media_gaps,
    parse_manifest,
    validate_document,
)

MAX_DOCS_BYTES = 32 * 1024 * 1024
_DOCUMENT = TypeAdapter(dict[str, JsonValue])


async def read_native_document(
    file_id: str, *, client: AirweaveHttpClient, auth: SourceAuthProvider
) -> dict[str, JsonValue]:
    """No field mask: preserve native tab structure, including unknown returned fields."""
    url = "https://docs.googleapis.com/v1/documents/" + quote(file_id, safe="")
    parameters = {
        "includeTabsContent": "true",
        "suggestionsViewMode": "DEFAULT_FOR_CURRENT_ACCESS",
        "commentsViewMode": "COMMENTS_VIEW_MODE_OMITTED",
    }
    for attempt in range(2):
        headers = await authorization_headers(auth, refresh=attempt == 1)
        headers["Accept-Encoding"] = "identity"
        async with client.stream(
            "GET", url, headers=headers, params=parameters, timeout=60.0
        ) as response:
            if response.status_code == 401 and auth.supports_refresh and attempt == 0:
                continue
            # Classify errors without reading an unbounded/private error body. A large
            # 401/429/5xx can never become a successful missing-representation gap.
            if not response.is_success:
                raise_for_status(
                    httpx.Response(
                        response.status_code,
                        headers=response.headers,
                        content=b"",
                        request=response.request,
                    ),
                    source_short_name="google_drive",
                    token_provider_kind=auth.provider_kind,
                )
            if response.headers.get("content-encoding", "identity").lower() != "identity":
                raise ValueError("Docs capture requires identity response encoding")
            body = await bounded_response_bytes(response, MAX_DOCS_BYTES, label="Docs JSON")
            document = _DOCUMENT.validate_python(json.loads(body))
            validate_document(document, file_id=file_id)
            return document
    raise ValueError("Docs authentication refresh did not complete")


async def capture_document_parts(
    record: CaptureRecord,
    *,
    export: ExportState,
    export_blob: BlobReference | None,
    files: FileService,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
) -> CaptureRecord:
    """Stage every part then manifest; the caller still must check final Drive version."""
    parts = [export_blob] if export_blob is not None else []
    try:
        document = await read_native_document(record.identity.native_id, client=client, auth=auth)
    except FileSkippedException:
        native = DocsState(
            status="unavailable",
            embedded_media="not_retained",
            missing=(DocsGap(reason="read_size_limit"),),
        )
    else:
        blob = await files.store_canonical_blob(
            canonical_json(document), media_type="application/json"
        )
        parts.append(blob)
        gaps = document_media_gaps(document)
        native = DocsState(
            status="complete",
            document_blob=blob.sha256,
            embedded_media="not_retained" if gaps else "retained",
            missing=gaps,
        )
    version = record.payload["version"]
    manifest = WorkspaceManifestV1(
        file_id=record.identity.native_id, drive_version=version, export=export, native=native
    )
    content = canonical_json(manifest.model_dump(mode="json"))
    manifest_blob = (
        await files.store_canonical_blob(content, media_type=MANIFEST_MIME)
    ).model_copy(update={"role": "representation_manifest"})
    # Workspace-local identity is bytes, never descriptor order. Other providers keep MIME paths.
    unique: dict[str, BlobReference] = {}
    for part in parts:
        previous = unique.get(part.sha256)
        if previous is not None and (previous.key, previous.size_bytes) != (
            part.key,
            part.size_bytes,
        ):
            raise ValueError("Workspace identical digest has conflicting storage descriptors")
        if previous is not None and previous.media_type != part.media_type:
            part = part.model_copy(update={"media_type": "application/octet-stream"})
        unique[part.sha256] = part
    blobs = tuple(unique[key] for key in sorted(unique)) + (manifest_blob,)
    parse_manifest(content, file_id=record.identity.native_id, drive_version=version, blobs=blobs)
    complete = native.status == "complete" and not native.missing
    return CaptureRecord.model_validate(
        record.model_dump()
        | {
            "blobs": blobs,
            "content_hash": manifest_blob.sha256,
            "completeness": "complete" if complete else "partial",
        }
    )
