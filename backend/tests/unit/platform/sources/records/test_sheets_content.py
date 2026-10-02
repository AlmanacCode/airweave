"""Native structured capture preserves content and truthful bounded coverage."""

from copy import deepcopy
from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest

from airweave.adapters.storage.filesystem import FilesystemBackend
from airweave.domains.sources.token_providers.static import StaticTokenProvider
from airweave.domains.storage.file_service import FileService
from airweave.platform.sources.records.google_drive import file_record
from airweave.platform.sources.records.google_drive_content import capture_file_content
from airweave.platform.sources.records.sheets_manifest import SHEETS_MIME, GridBounds
from airweave.platform.sources.records.sheets_models import validate_grid


def metadata(rows=2, columns=2):
    return {
        "spreadsheetId": "book",
        "properties": {"locale": "en_US", "timeZone": "UTC"},
        "sheets": [
            {
                "properties": {
                    "sheetId": 0,
                    "title": "A'B",
                    "hidden": True,
                    "gridProperties": {"rowCount": rows, "columnCount": columns},
                },
                "merges": [
                    {
                        "sheetId": 0,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 2,
                    }
                ],
            }
        ],
    }


def grid():
    value = metadata()
    value["sheets"][0]["data"] = [
        {
            "rowData": [
                {
                    "values": [
                        {
                            "userEnteredValue": {"formulaValue": "=1+1"},
                            "effectiveValue": {"numberValue": 2},
                            "formattedValue": "2",
                            "note": "native note",
                            "effectiveFormat": {"numberFormat": {"type": "NUMBER"}},
                        }
                    ]
                }
            ]
        }
    ]
    return value


async def acquire(tmp_path, monkeypatch, *, partial=False, wrong=False):
    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir",
        lambda _: str(tmp_path / "temp"),
    )
    if partial:
        monkeypatch.setattr(
            "airweave.platform.sources.records.google_sheets_content.MAX_GRID_REQUESTS", 0
        )
    storage = FilesystemBackend(tmp_path / "store")
    files = FileService(uuid4(), storage, sync_id=uuid4())
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path.endswith("/export"):
            return httpx.Response(200, content=b"full-xlsx")
        assert request.method == "GET"
        if "ranges" in request.url.params:
            assert request.url.params["ranges"] == "'A''B'!A1:B2"
            result = grid()
            if wrong:
                result["sheets"][0]["properties"]["sheetId"] = 9
            return httpx.Response(200, json=result)
        return httpx.Response(200, json=metadata())

    async def latest(*args, **kwargs):
        return {"version": "7"}

    original = file_record({"id": "book", "version": "7", "mimeType": SHEETS_MIME})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        captured = await capture_file_content(
            original,
            files=files,
            get=latest,
            client=client,
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            capture_native_sheets=True,
        )
    return captured, storage, calls


@pytest.mark.asyncio
async def test_native_formula_values_and_sparse_grid_retained(tmp_path, monkeypatch):
    captured, storage, calls = await acquire(tmp_path, monkeypatch)
    assert captured.completeness == "complete"
    assert len(captured.blobs) == 4
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_budget_preserves_export_and_explicit_unknown_ranges(tmp_path, monkeypatch):
    captured, storage, calls = await acquire(tmp_path, monkeypatch, partial=True)
    assert captured.completeness == "partial"
    assert len(captured.blobs) == 3
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_wrong_native_sheet_rejects_capture(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="different sheets"):
        await acquire(tmp_path, monkeypatch, wrong=True)


def test_sparse_grid_cannot_escape_requested_bounds():
    bounds = GridBounds(sheet_id=0, start_row=0, end_row=2, start_column=0, end_column=2)
    validate_grid(grid(), "book", bounds)
    bad = deepcopy(grid())
    bad["sheets"][0]["data"][0]["startRow"] = 2
    with pytest.raises(ValueError, match="outside"):
        validate_grid(bad, "book", bounds)


@pytest.mark.asyncio
async def test_disabled_native_capture_keeps_export_only_behavior(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir",
        lambda _: str(tmp_path / "temp"),
    )
    files = FileService(uuid4(), FilesystemBackend(tmp_path / "store"), sync_id=uuid4())

    async def latest(*args, **kwargs):
        return {"version": "7"}

    def response(request):
        assert request.url.host == "www.googleapis.com"
        assert request.url.path.endswith("/export")
        return httpx.Response(200, content=b"legacy-xlsx")

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        captured = await capture_file_content(
            file_record({"id": "book", "version": "7", "mimeType": SHEETS_MIME}),
            files=files,
            get=latest,
            client=client,
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
        )
    assert captured.completeness == "complete"
    assert len(captured.blobs) == 1 and captured.blobs[0].role is None


def test_overlapping_native_cells_are_rejected():
    value = grid()
    value["sheets"][0]["data"].append(deepcopy(value["sheets"][0]["data"][0]))
    with pytest.raises(ValueError, match="overlapping cells"):
        validate_grid(
            value,
            "book",
            GridBounds(sheet_id=0, start_row=0, end_row=2, start_column=0, end_column=2),
        )


def test_native_search_groups_sheet_titles_and_preserves_sparse_row_addresses():
    from airweave.domains.entities.canonical.workspace_sheets import (
        CapturedGrid,
        CapturedSpreadsheet,
    )
    from airweave.platform.sources.records.sheets_manifest import (
        GridPart,
        SheetsState,
        WorkspaceManifestV2,
    )
    from airweave.platform.sources.records.workspace_manifest import ExportState

    parts = tuple(
        GridPart(
            bounds=GridBounds(
                sheet_id=sheet, start_row=0, end_row=100, start_column=0, end_column=1
            ),
            blob=str(sheet + 1) * 64,
        )
        for sheet in (0, 1)
    )
    sheets, grids = [], []
    for index, title in enumerate(("First sheet", "Second sheet")):
        properties = {
            "sheetId": index,
            "title": title,
            "gridProperties": {"rowCount": 100, "columnCount": 1},
        }
        sheets.append({"properties": properties})
        grids.append(
            CapturedGrid(
                part=parts[index],
                data={
                    "spreadsheetId": "book",
                    "sheets": [
                        {
                            "properties": properties,
                            "data": [
                                {
                                    "startRow": 48,
                                    "rowData": [
                                        {"values": [{"formattedValue": f"unique-value-{index}"}]}
                                    ],
                                }
                            ],
                        }
                    ],
                },
            )
        )
    captured = CapturedSpreadsheet(
        manifest=WorkspaceManifestV2(
            file_id="book",
            drive_version="7",
            export=ExportState(status="unavailable", reason="export_size_limit"),
            native=SheetsState(status="complete", metadata_blob="a" * 64, parts=parts),
        ),
        spreadsheet={"spreadsheetId": "book", "sheets": sheets},
        grids=tuple(grids),
    )
    text = captured.text()
    assert (
        text.index("First sheet")
        < text.index("unique-value-0")
        < text.index("Second sheet")
        < text.index("unique-value-1")
    )
    assert "A49: unique-value-0" in text
    assert "'First sheet'!A1:A100" in text


@pytest.mark.asyncio
async def test_distinct_blank_ranges_share_bytes_without_losing_coverage(tmp_path, monkeypatch):
    from airweave.platform.sources.records.sheets_manifest import parse_sheet_manifest
    from airweave.platform.sources.records.sheets_models import validate_partition

    monkeypatch.setattr(
        "airweave.domains.storage.file_service.paths.temp_sync_dir", lambda _: str(tmp_path / "tmp")
    )
    files = FileService(uuid4(), FilesystemBackend(tmp_path / "store"), sync_id=uuid4())
    empty = metadata(rows=200, columns=1)
    requested = []

    async def latest(*args, **kwargs):
        return {"version": "7"}

    def response(request):
        if request.url.path.endswith("/export"):
            return httpx.Response(200, content=b"empty-workbook-export")
        if "ranges" in request.url.params:
            requested.append(request.url.params["ranges"])
        return httpx.Response(200, json=empty)

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        captured = await capture_file_content(
            file_record({"id": "book", "version": "7", "mimeType": SHEETS_MIME}),
            files=files,
            get=latest,
            client=client,
            auth=StaticTokenProvider("synthetic"),
            logger=MagicMock(),
            capture_native_sheets=True,
        )
    assert requested == ["'A''B'!A1:A100", "'A''B'!A101:A200"]
    assert len(captured.blobs) == 3
    marked = next(blob for blob in captured.blobs if blob.role == "representation_manifest")
    manifest = parse_sheet_manifest(
        await files.storage.read_file(marked.key),
        file_id="book",
        drive_version="7",
        blobs=captured.blobs,
    )
    assert len(manifest.native.parts) == 2
    assert {part.blob for part in manifest.native.parts} == {manifest.native.metadata_blob}
    assert manifest.native.parts[0].bounds != manifest.native.parts[1].bounds
    validate_partition(manifest, empty)
