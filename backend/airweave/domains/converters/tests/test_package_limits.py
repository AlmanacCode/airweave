"""Shared package/output boundaries reject whole conversions before OCR."""

from unittest.mock import AsyncMock, patch
from zipfile import ZipFile

import pytest

from airweave.domains.converters.docx import DocxConverter
from airweave.domains.converters.package_limits import (
    PackageTextLimits,
    PreparationLimit,
    bounded_join,
)
from airweave.domains.converters.pptx import PptxConverter


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["docx", "pptx"])
@pytest.mark.parametrize(
    "override",
    [
        {"maximum_package_members": 1},
        {"maximum_member_bytes": 3},
        {"maximum_expanded_bytes": 7},
    ],
)
async def test_archive_rejection_precedes_parser_and_ocr(tmp_path, kind, override):
    path = tmp_path / f"bounded.{kind}"
    with ZipFile(path, "w") as package:
        package.writestr("one", b"1234")
        package.writestr("two", b"5678")
    before = path.read_bytes()
    ocr = AsyncMock()
    ocr.convert_batch.side_effect = AssertionError("Limit rejection must not call OCR")
    converter = (DocxConverter if kind == "docx" else PptxConverter)(
        ocr_provider=ocr, limits=PackageTextLimits(**override)
    )
    with patch("docx.Document" if kind == "docx" else "pptx.Presentation") as parser:
        result = (await converter.convert_batch([str(path)]))[str(path)]
        parser.assert_not_called()
    assert result.text is None and result.gap is None
    assert result.failure_reason == "preparation_limit"
    assert path.read_bytes() == before
    ocr.convert_batch.assert_not_awaited()


def test_bounded_join_counts_unicode_bytes_and_all_delimiters():
    parts = ["اردو", "हिन्दी"]
    expected = "\n\n".join(parts)
    size = len(expected.encode("utf-8"))
    assert (
        bounded_join(iter(parts), "\n\n", PackageTextLimits(maximum_output_bytes=size)) == expected
    )
    with pytest.raises(PreparationLimit):
        bounded_join(iter(parts), "\n\n", PackageTextLimits(maximum_output_bytes=size - 1))
