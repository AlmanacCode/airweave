"""Typed native Sheets shape and exact retained-grid admission."""

from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationError

from airweave.platform.sources.records.sheets_manifest import GridBounds, WorkspaceManifestV2


class GridDimensions(BaseModel):
    """Allocated grid dimensions from native sheet metadata."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    rowCount: int = Field(ge=0)
    columnCount: int = Field(ge=0)


class SheetProperties(BaseModel):
    """Identity and dimensions needed for version-consistent range acquisition."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    sheetId: int = Field(ge=0)
    title: str = Field(min_length=1)
    sheetType: str = "GRID"
    gridProperties: GridDimensions | None = None


class SheetMetadata(BaseModel):
    """Native sheet metadata boundary; unmodeled fields remain in retained JSON."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    properties: SheetProperties


class SpreadsheetMetadata(BaseModel):
    """Workbook identity and sheet inventory used to admit grid coverage."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    spreadsheetId: str
    sheets: tuple[SheetMetadata, ...]


def metadata_shape(data: dict[str, JsonValue], file_id: str) -> SpreadsheetMetadata:
    """Validate native identity without stripping fields from retained JSON."""
    try:
        metadata = SpreadsheetMetadata.model_validate(data)
    except ValidationError:
        raise ValueError("Malformed native spreadsheet metadata") from None
    if metadata.spreadsheetId != file_id:
        raise ValueError("Spreadsheet response belongs to another file")
    ids = [s.properties.sheetId for s in metadata.sheets]
    if len(ids) != len(set(ids)):
        raise ValueError("Spreadsheet contains duplicate sheet IDs")
    return metadata


class NativeRow(BaseModel):
    """Native sparse cells, preserving all returned fields."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    values: tuple[dict[str, JsonValue], ...] = ()


class NativeGrid(BaseModel):
    """Native sparse block with explicit zero-based offsets."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    startRow: int = Field(default=0, ge=0)
    startColumn: int = Field(default=0, ge=0)
    rowData: tuple[NativeRow, ...] = ()


class SheetGrid(BaseModel):
    """Grid blocks supplied for one requested native sheet."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    data: tuple[NativeGrid, ...] = ()


def validate_grid(data: dict[str, JsonValue], file_id: str, bounds: GridBounds) -> None:
    """Provider sparsity is valid only inside the explicit requested rectangle."""
    shape = metadata_shape(data, file_id)
    if [sheet.properties.sheetId for sheet in shape.sheets] != [bounds.sheet_id]:
        raise ValueError("Grid response must identify its exact requested sheet")
    sheets = data["sheets"]
    try:
        sheet = SheetGrid.model_validate(sheets[0])
    except ValidationError:
        raise ValueError("Malformed native spreadsheet grid") from None
    seen: set[tuple[int, int]] = set()
    for block in sheet.data:
        if not block.rowData:
            continue
        if not (
            bounds.start_row <= block.startRow < bounds.end_row
            and bounds.start_column <= block.startColumn < bounds.end_column
        ):
            raise ValueError("Grid response starts outside the requested bounds")
        if block.startRow + len(block.rowData) > bounds.end_row:
            raise ValueError("Grid response exceeds requested rows")
        if any(block.startColumn + len(row.values) > bounds.end_column for row in block.rowData):
            raise ValueError("Grid response exceeds requested columns")
        coordinates = {
            (row_index, column_index)
            for row_index, row in enumerate(block.rowData, start=block.startRow)
            for column_index in range(block.startColumn, block.startColumn + len(row.values))
        }
        if seen.intersection(coordinates):
            raise ValueError("Grid response contains overlapping cells")
        seen.update(coordinates)


def validate_partition(manifest: WorkspaceManifestV2, metadata: dict[str, JsonValue]) -> None:
    """No overlap, missing range, foreign sheet or optimistic complete declaration."""
    shape = metadata_shape(metadata, manifest.file_id)
    identities = {sheet.properties.sheetId for sheet in shape.sheets}
    if any(p.bounds.sheet_id not in identities for p in manifest.native.parts):
        raise ValueError("Retained grid references an absent sheet")
    if any(g.sheet_id not in identities for g in manifest.native.missing):
        raise ValueError("Grid gap references an absent sheet")
    for sheet in shape.sheets:
        props = sheet.properties
        parts = [p.bounds for p in manifest.native.parts if p.bounds.sheet_id == props.sheetId]
        gaps = [g for g in manifest.native.missing if g.sheet_id == props.sheetId]
        if props.sheetType != "GRID":
            if parts or len(gaps) != 1 or gaps[0].reason != "unsupported_sheet_type":
                raise ValueError("Unsupported sheet must have explicit unavailable coverage")
            continue
        dims = props.gridProperties
        if dims is None or any(g.bounds is None for g in gaps):
            raise ValueError("GRID coverage requires dimensions and explicit ranges")
        bounds = parts + [g.bounds for g in gaps]
        _validate_rectangles(bounds, dims)


def _validate_rectangles(bounds: list[GridBounds], dims: GridDimensions) -> None:
    area = 0
    for index, current in enumerate(bounds):
        if current.end_row > dims.rowCount or current.end_column > dims.columnCount:
            raise ValueError("Grid coverage exceeds sheet dimensions")
        area += (current.end_row - current.start_row) * (current.end_column - current.start_column)
        for previous in bounds[:index]:
            if max(previous.start_row, current.start_row) < min(
                previous.end_row, current.end_row
            ) and max(previous.start_column, current.start_column) < min(
                previous.end_column, current.end_column
            ):
                raise ValueError("Grid coverage overlaps")
    if area != dims.rowCount * dims.columnCount:
        raise ValueError("Grid coverage does not account for every configured cell")


class SpreadsheetCell(BaseModel):
    """One native cell with absolute zero-based coordinates."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    row: int = Field(ge=0)
    column: int = Field(ge=0)
    value: dict[str, JsonValue]


def column_name(index: int) -> str:
    """Convert a zero-based column to its native A1 name without a sheet-title parser."""
    result = ""
    number = index + 1
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def a1_range(title: str, bounds: GridBounds) -> str:
    """Quote the native title while stable sheet IDs remain the admission authority."""
    escaped = title.replace("'", "''")
    return (
        f"'{escaped}'!{column_name(bounds.start_column)}{bounds.start_row + 1}:"
        f"{column_name(bounds.end_column - 1)}{bounds.end_row}"
    )
