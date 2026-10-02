"""Validated native spreadsheet reads from committed Drive-owned representations."""

import json

from pydantic import BaseModel, ConfigDict, JsonValue

from airweave.domains.entities.canonical.blob_materializer import read_blob
from airweave.domains.entities.canonical.models import SourceRecord
from airweave.domains.storage.protocols import StorageBackend
from airweave.platform.sources.records.sheets_manifest import (
    SHEETS_MIME,
    GridBounds,
    GridPart,
    WorkspaceManifestV2,
    parse_sheet_manifest,
)
from airweave.platform.sources.records.sheets_models import (
    SheetGrid,
    SpreadsheetCell,
    a1_range,
    column_name,
    metadata_shape,
    validate_grid,
    validate_partition,
)


class CapturedGrid(BaseModel):
    """One requested native response paired with its admitted bounds."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    part: GridPart
    data: dict[str, JsonValue]


class CapturedSpreadsheet(BaseModel):
    """One exact stored file revision with validated structured coverage."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    manifest: WorkspaceManifestV2
    spreadsheet: dict[str, JsonValue]
    grids: tuple[CapturedGrid, ...]

    def cells(self, bounds: GridBounds) -> tuple[SpreadsheetCell, ...]:
        """Return only retained cells in the selected rectangle, with coverage separately."""
        sheets = metadata_shape(self.spreadsheet, self.manifest.file_id).sheets
        selected = next((s for s in sheets if s.properties.sheetId == bounds.sheet_id), None)
        if selected is None or selected.properties.gridProperties is None:
            raise ValueError("Requested sheet is absent or non-grid")
        dims = selected.properties.gridProperties
        if bounds.end_row > dims.rowCount or bounds.end_column > dims.columnCount:
            raise ValueError("Requested range exceeds sheet dimensions")
        result = []
        for grid in self.grids:
            if grid.part.bounds.sheet_id == bounds.sheet_id:
                result.extend(_selected_cells(grid.data, bounds))
        return tuple(result)

    def text(self) -> str:
        """One searchable representation; native formula expressions remain in originals."""
        if self.manifest.native.status != "complete" or len(self.grids) != len(
            self.manifest.native.parts
        ):
            raise ValueError("Partial native grids cannot replace the full export projection")
        lines = []
        for sheet in metadata_shape(self.spreadsheet, self.manifest.file_id).sheets:
            title = sheet.properties.title
            lines.append(title)
            for grid in self.grids:
                if grid.part.bounds.sheet_id == sheet.properties.sheetId:
                    lines.append(a1_range(title, grid.part.bounds))
                    lines.extend(_grid_text(grid.data))
        return "\n".join(lines)


def _grid_text(data: dict[str, JsonValue]):
    for block in SheetGrid.model_validate(data["sheets"][0]).data:
        for offset, row in enumerate(block.rowData):
            values = []
            for cell in row.values:
                value = cell.get("formattedValue", "")
                note = cell.get("note", "")
                if not isinstance(value, str) or not isinstance(note, str):
                    raise ValueError("Malformed native spreadsheet display text")
                values.append(value + (" [Note: " + note + "]" if note else ""))
            address = f"{column_name(block.startColumn)}{block.startRow + offset + 1}"
            yield address + ": " + "\t".join(values)


def _selected_cells(grid: dict[str, JsonValue], bounds: GridBounds):
    for block in SheetGrid.model_validate(grid["sheets"][0]).data:
        for offset, row in enumerate(block.rowData):
            row_index = block.startRow + offset
            if not bounds.start_row <= row_index < bounds.end_row:
                continue
            for offset, cell in enumerate(row.values):
                column = block.startColumn + offset
                if bounds.start_column <= column < bounds.end_column:
                    yield SpreadsheetCell(row=row_index, column=column, value=cell)


def intersects(first: GridBounds, second: GridBounds) -> bool:
    """Select retained rectangles without expanding sparse grids into dense cell arrays."""
    return (
        first.sheet_id == second.sheet_id
        and max(first.start_row, second.start_row) < min(first.end_row, second.end_row)
        and max(first.start_column, second.start_column) < min(first.end_column, second.end_column)
    )


async def read_spreadsheet(
    record: SourceRecord,
    storage: StorageBackend,
    *,
    bounds: GridBounds | None = None,
    for_projection: bool = False,
) -> CapturedSpreadsheet:
    """Resolve only admitted blobs and validate all native range identities."""
    if record.content_access != "available" or record.deleted_at is not None:
        raise ValueError("Captured spreadsheet is unavailable")
    if record.identity.record_type != "file" or record.payload.get("mimeType") != SHEETS_MIME:
        raise ValueError("Captured file is not a spreadsheet")
    manifests = [blob for blob in record.blobs if blob.role == "representation_manifest"]
    if len(manifests) != 1 or not isinstance(record.payload.get("version"), str):
        raise ValueError("Native spreadsheet has not been captured")
    manifest = parse_sheet_manifest(
        await read_blob(record, manifests[0], storage),
        file_id=record.identity.native_id,
        drive_version=record.payload["version"],
        blobs=record.blobs,
    )
    by_digest = {blob.sha256: blob for blob in record.blobs}
    metadata = json.loads(
        await read_blob(record, by_digest[manifest.native.metadata_blob], storage)
    )
    validate_partition(manifest, metadata)
    grids = []
    for part in manifest.native.parts:
        if not (
            (for_projection and manifest.native.status == "complete")
            or (bounds is not None and intersects(part.bounds, bounds))
        ):
            continue
        grid = json.loads(await read_blob(record, by_digest[part.blob], storage))
        validate_grid(grid, manifest.file_id, part.bounds)
        grids.append(CapturedGrid(part=part, data=grid))
    return CapturedSpreadsheet(manifest=manifest, spreadsheet=metadata, grids=tuple(grids))
