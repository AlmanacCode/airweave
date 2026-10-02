"""Bounded native grid acquisition; Drive owns inventory, identity and publication."""

import asyncio
import json
from time import monotonic
from urllib.parse import quote

import httpx
from pydantic import BaseModel, Field, JsonValue, TypeAdapter

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
from airweave.platform.sources.records.sheets_manifest import (
    GridBounds,
    GridGap,
    GridPart,
    SheetsState,
    WorkspaceManifestV2,
    parse_sheet_manifest,
)
from airweave.platform.sources.records.sheets_models import (
    SheetProperties,
    a1_range,
    metadata_shape,
    validate_grid,
    validate_partition,
)
from airweave.platform.sources.records.workspace_manifest import (
    MANIFEST_MIME,
    ExportState,
    canonical_json,
)

MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_NATIVE_BYTES = 32 * 1024 * 1024
MAX_GRID_REQUESTS = 32
MAX_CAPTURE_SECONDS = 120
_JSON = TypeAdapter(dict[str, JsonValue])


async def read_native_sheet(
    file_id: str,
    *,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
    range_name: str | None = None,
    timeout: float = 60,
) -> dict[str, JsonValue]:
    """GET retains read-only scope compatibility; never interpret HTTP errors as gaps."""
    url = "https://sheets.googleapis.com/v4/spreadsheets/" + quote(file_id, safe="")
    params = {"commentsViewMode": "COMMENTS_VIEW_MODE_OMITTED"}
    if range_name is not None:
        params.update(ranges=range_name, includeGridData="true")
    for attempt in range(2):
        headers = await authorization_headers(auth, refresh=attempt == 1)
        headers["Accept-Encoding"] = "identity"
        async with client.stream(
            "GET", url, headers=headers, params=params, timeout=timeout
        ) as response:
            if response.status_code == 401 and auth.supports_refresh and attempt == 0:
                continue
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
                raise ValueError("Sheets capture requires identity response encoding")
            body = await bounded_response_bytes(response, MAX_RESPONSE_BYTES, label="Sheets JSON")
            data = _JSON.validate_python(json.loads(body))
            metadata_shape(data, file_id)
            return data
    raise ValueError("Sheets authentication refresh did not complete")


def remaining_ranges(properties: SheetProperties, row: int, column: int) -> tuple[GridBounds, ...]:
    """Compact the unrequested suffix into at most two rectangles, not millions of tiles."""
    grid = properties.gridProperties
    if grid is None:
        raise ValueError("GRID sheet has no dimensions")
    common = {"sheet_id": properties.sheetId, "end_column": grid.columnCount}
    result = []
    end_row = min(row + 100, grid.rowCount)
    if column < grid.columnCount and row < grid.rowCount:
        result.append(GridBounds(**common, start_row=row, end_row=end_row, start_column=column))
    if end_row < grid.rowCount:
        result.append(
            GridBounds(**common, start_row=end_row, end_row=grid.rowCount, start_column=0)
        )
    return tuple(result)


def grid_tiles(properties: SheetProperties):
    """Lazily plan fixed rectangles; large empty allocated grids are never materialized."""
    grid = properties.gridProperties
    if grid is None:
        raise ValueError("GRID sheet has no dimensions")
    for row in range(0, grid.rowCount, 100):
        for column in range(0, grid.columnCount, 100):
            yield GridBounds(
                sheet_id=properties.sheetId,
                start_row=row,
                end_row=min(row + 100, grid.rowCount),
                start_column=column,
                end_column=min(column + 100, grid.columnCount),
            )


class GridAcquisition(BaseModel):
    """One file's transient budget, retained descriptors and exact unknown ranges."""

    started: float
    used_bytes: int
    requests: int = 0
    blobs: list[BlobReference]
    parts: list[GridPart] = Field(default_factory=list)
    gaps: list[GridGap] = Field(default_factory=list)

    async def capture_sheet(
        self,
        properties: SheetProperties,
        *,
        file_id: str,
        files: FileService,
        client: AirweaveHttpClient,
        auth: SourceAuthProvider,
    ) -> None:
        """Stop the native scan explicitly at the budget; never imply blank unknown ranges."""
        if properties.sheetType != "GRID":
            self.gaps.append(GridGap(sheet_id=properties.sheetId, reason="unsupported_sheet_type"))
            return
        for bounds in grid_tiles(properties):
            remaining = MAX_CAPTURE_SECONDS - (monotonic() - self.started)
            if (
                self.requests >= MAX_GRID_REQUESTS
                or self.used_bytes >= MAX_NATIVE_BYTES
                or remaining <= 0
            ):
                self.gaps.extend(
                    GridGap(sheet_id=properties.sheetId, bounds=b, reason="capture_budget")
                    for b in remaining_ranges(properties, bounds.start_row, bounds.start_column)
                )
                return
            self.requests += 1
            try:
                async with asyncio.timeout(remaining):
                    content = await read_grid_bytes(
                        file_id,
                        properties,
                        bounds,
                        client=client,
                        auth=auth,
                        timeout=min(60, remaining),
                    )
            except TimeoutError:
                self.gaps.extend(
                    GridGap(sheet_id=properties.sheetId, bounds=b, reason="capture_budget")
                    for b in remaining_ranges(properties, bounds.start_row, bounds.start_column)
                )
                return
            except FileSkippedException:
                self.gaps.append(
                    GridGap(sheet_id=properties.sheetId, bounds=bounds, reason="read_size_limit")
                )
                continue
            if self.used_bytes + len(content) > MAX_NATIVE_BYTES:
                self.gaps.append(
                    GridGap(sheet_id=properties.sheetId, bounds=bounds, reason="capture_budget")
                )
                self.used_bytes = MAX_NATIVE_BYTES
                continue
            blob = await files.store_canonical_blob(content, media_type="application/json")
            self.used_bytes += len(content)
            self.blobs.append(blob)
            self.parts.append(GridPart(bounds=bounds, blob=blob.sha256))


async def read_grid_bytes(
    file_id: str,
    properties: SheetProperties,
    bounds: GridBounds,
    *,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
    timeout: float,
) -> bytes:
    """Check stable native sheet identity as well as the outer Drive version fence."""
    data = await read_native_sheet(
        file_id,
        client=client,
        auth=auth,
        range_name=a1_range(properties.title, bounds),
        timeout=timeout,
    )
    response = metadata_shape(data, file_id)
    if [sheet.properties.sheetId for sheet in response.sheets] != [properties.sheetId]:
        raise ValueError("Requested grid returned different sheets")
    if response.sheets[0].properties != properties:
        raise ValueError("Sheet identity or dimensions changed during acquisition")
    validate_grid(data, file_id, bounds)
    return canonical_json(data)


async def capture_spreadsheet_parts(
    record: CaptureRecord,
    *,
    export: ExportState,
    export_blob: BlobReference | None,
    files: FileService,
    client: AirweaveHttpClient,
    auth: SourceAuthProvider,
) -> CaptureRecord:
    """Keep original responses and exact requested bounds; caller fences Drive version."""
    started = monotonic()
    metadata = await read_native_sheet(record.identity.native_id, client=client, auth=auth)
    shape = metadata_shape(metadata, record.identity.native_id)
    raw = canonical_json(metadata)
    metadata_blob = await files.store_canonical_blob(raw, media_type="application/json")
    blobs = [metadata_blob] + ([export_blob] if export_blob else [])
    acquisition = GridAcquisition(started=started, used_bytes=len(raw), blobs=blobs)
    for sheet in shape.sheets:
        await acquisition.capture_sheet(
            sheet.properties,
            file_id=record.identity.native_id,
            files=files,
            client=client,
            auth=auth,
        )
    parts, gaps, blobs = acquisition.parts, acquisition.gaps, acquisition.blobs
    manifest = WorkspaceManifestV2(
        file_id=record.identity.native_id,
        drive_version=record.payload["version"],
        export=export,
        native=SheetsState(
            status="partial" if gaps else "complete",
            metadata_blob=metadata_blob.sha256,
            parts=tuple(parts),
            missing=tuple(gaps),
        ),
    )
    validate_partition(manifest, metadata)
    content = canonical_json(manifest.model_dump(mode="json"))
    marked = (await files.store_canonical_blob(content, media_type=MANIFEST_MIME)).model_copy(
        update={"role": "representation_manifest"}
    )
    unique: dict[str, BlobReference] = {}
    for blob in blobs:
        previous = unique.get(blob.sha256)
        if previous is not None and (previous.key, previous.size_bytes) != (
            blob.key,
            blob.size_bytes,
        ):
            raise ValueError("Identical spreadsheet bytes have conflicting descriptors")
        if previous is not None and previous.media_type != blob.media_type:
            blob = blob.model_copy(update={"media_type": "application/octet-stream"})
        unique[blob.sha256] = blob
    retained = tuple(unique[key] for key in sorted(unique)) + (marked,)
    parse_sheet_manifest(
        content,
        file_id=record.identity.native_id,
        drive_version=record.payload["version"],
        blobs=retained,
    )
    return CaptureRecord.model_validate(
        record.model_dump()
        | {
            "blobs": retained,
            "content_hash": marked.sha256,
            "completeness": "partial" if gaps else "complete",
        }
    )
