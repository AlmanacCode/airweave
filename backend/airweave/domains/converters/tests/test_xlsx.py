"""Tests for XlsxConverter — empty sheets error and valid data extraction."""

import os
import tempfile

import pytest

from airweave.domains.converters.xlsx import XlsxConverter


@pytest.fixture
def converter():
    return XlsxConverter()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def _create_xlsx(path, sheets_data):
    """Create a test XLSX file.

    Args:
        path: File path to create
        sheets_data: dict of sheet_name -> list of rows (each row is a list of cell values)
    """
    from openpyxl import Workbook

    wb = Workbook()
    first = True
    for sheet_name, rows in sheets_data.items():
        if first:
            ws = wb.active
            ws.title = sheet_name
            first = False
        else:
            ws = wb.create_sheet(title=sheet_name)
        for row in rows:
            ws.append(row)
    wb.save(path)


class TestXlsxConverter:

    @pytest.mark.asyncio
    async def test_valid_xlsx_returns_markdown(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "data.xlsx")
        _create_xlsx(
            file_path,
            {"Sheet1": [["Name", "Age"], ["Alice", 30], ["Bob", 25]]},
        )

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is not None
        assert "Alice" in result[file_path].text
        assert "Bob" in result[file_path].text
        assert "| Name | Age |" in result[file_path].text

    @pytest.mark.asyncio
    async def test_default_empty_workbook_still_extracts(self, converter, temp_dir):
        """XLSX with a default empty sheet still extracts a sheet header."""
        file_path = os.path.join(temp_dir, "empty.xlsx")
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = "Empty"
        wb.save(file_path)

        result = await converter.convert_batch([file_path])

        assert file_path in result
        # openpyxl reports at least A1 even on an empty sheet
        assert result[file_path].text is not None

    @pytest.mark.asyncio
    async def test_no_sheets_raises_error(self, converter, temp_dir):
        """XLSX with no sheet names → EntityProcessingError → None."""
        file_path = os.path.join(temp_dir, "no_sheets.xlsx")
        from openpyxl import Workbook

        wb = Workbook()
        wb.save(file_path)

        # Corrupt the file to have no sheets by removing them after save
        # We test via the error path more directly
        from unittest.mock import MagicMock, patch

        mock_wb = MagicMock()
        mock_wb.sheetnames = []

        with patch("openpyxl.load_workbook", return_value=mock_wb):
            result = await converter.convert_batch([file_path])

        assert file_path in result
        assert result[file_path].text is None

    @pytest.mark.asyncio
    async def test_multi_sheet_xlsx(self, converter, temp_dir):
        file_path = os.path.join(temp_dir, "multi.xlsx")
        _create_xlsx(
            file_path,
            {
                "Users": [["Name"], ["Charlie"]],
                "Products": [["Item", "Price"], ["Widget", "9.99"]],
            },
        )

        result = await converter.convert_batch([file_path])

        assert file_path in result
        assert "Sheet: Users" in result[file_path].text
        assert "Sheet: Products" in result[file_path].text
        assert "Charlie" in result[file_path].text
        assert "Widget" in result[file_path].text

    @pytest.mark.asyncio
    async def test_nonexistent_file_returns_none(self, converter):
        result = await converter.convert_batch(["/nonexistent/file.xlsx"])
        assert result["/nonexistent/file.xlsx"].text is None


@pytest.mark.asyncio
async def test_multilingual_formulas_and_later_wider_rows(tmp_path):
    """Every actual column survives; formula source and Unicode remain unchanged."""
    path = str(tmp_path / "languages.xlsx")
    _create_xlsx(path, {"言語": [["Heading"], ["नमस्ते", "اردو", "e\u0301", "=1+2"]]})
    before = (tmp_path / "languages.xlsx").read_bytes()
    result = (await XlsxConverter().convert_batch([path]))[path]
    assert result.failure_reason is None
    assert "| Heading |  |  |  |" in result.text
    assert "| नमस्ते | اردو | e\u0301 | =1+2 |" in result.text
    assert (tmp_path / "languages.xlsx").read_bytes() == before


@pytest.mark.asyncio
async def test_false_large_dimensions_do_not_expand_rectangle(tmp_path):
    """A bad producer dimension alone must not reject a small ordinary workbook."""
    from zipfile import ZipFile

    path = tmp_path / "dimension.xlsx"
    _create_xlsx(str(path), {"Data": [["Header"], ["Actual value"]]})
    with ZipFile(path) as archive:
        members = {item.filename: archive.read(item.filename) for item in archive.infolist()}
    members["xl/worksheets/sheet1.xml"] = members["xl/worksheets/sheet1.xml"].replace(
        b'ref="A1:A2"', b'ref="A1:XFD1048576"'
    )
    with ZipFile(path, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    result = (await XlsxConverter().convert_batch([str(path)]))[str(path)]
    assert result.failure_reason is None and "Actual value" in result.text
    assert len(result.text) < 100


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limit",
    [
        {"maximum_package_members": 1},
        {"maximum_member_bytes": 10},
        {"maximum_expanded_bytes": 100},
        {"maximum_rows": 1},
        {"maximum_columns": 1},
        # One narrow header followed by a wider row expands the final rectangle.
        {"maximum_cells": 3},
        {"maximum_output_bytes": 40},
    ],
)
async def test_each_content_bound_fails_without_partial_text_or_original_mutation(tmp_path, limit):
    from airweave.domains.converters.xlsx_limits import XlsxLimits

    path = tmp_path / "bounded.xlsx"
    _create_xlsx(str(path), {"Data": [["Heading"], ["नमस्ते", "اردو"]]})
    before = path.read_bytes()
    result = (await XlsxConverter(XlsxLimits(**limit)).convert_batch([str(path)]))[str(path)]
    assert result.text is None and result.failure_reason == "preparation_limit"
    assert result.gap is None and path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("coordinate", ["A1048576", "XFD1"])
async def test_actual_far_sparse_cells_hit_limits_without_rectangular_expansion(
    tmp_path, coordinate
):
    from openpyxl import Workbook

    path = tmp_path / "sparse.xlsx"
    workbook = Workbook()
    workbook.active["A1"] = "Header"
    workbook.active[coordinate] = "Retained far cell"
    workbook.save(path)
    workbook.close()
    result = (await XlsxConverter().convert_batch([str(path)]))[str(path)]
    assert result.text is None and result.failure_reason == "preparation_limit"


@pytest.mark.asyncio
async def test_corrupt_package_is_failure_and_next_file_still_converts(tmp_path):
    corrupt, valid = tmp_path / "bad.xlsx", tmp_path / "good.xlsx"
    corrupt.write_bytes(b"not a zip workbook")
    _create_xlsx(str(valid), {"Data": [["Retained good workbook"]]})
    results = await XlsxConverter().convert_batch([str(corrupt), str(valid)])
    assert results[str(corrupt)].text is None
    assert results[str(corrupt)].failure_reason is None
    assert "Retained good workbook" in results[str(valid)].text
    assert corrupt.read_bytes() == b"not a zip workbook"
