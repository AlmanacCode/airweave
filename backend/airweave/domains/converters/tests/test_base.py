"""Tests for HybridDocumentConverter._try_read_as_text binary detection."""

import os
import tempfile
from unittest.mock import AsyncMock

import pytest

from airweave.domains.converters._base import HybridDocumentConverter
from airweave.domains.converters.pdf import PdfConverter
from airweave.domains.sync_pipeline.exceptions import SyncFailureError


async def test_infrastructure_failure_does_not_become_unavailable_conversion(tmp_path):
    """A missing parser dependency cannot be relabeled a failed content part."""
    path = tmp_path / "input.pdf"
    path.write_bytes(b"synthetic PDF bytes")
    fallback = AsyncMock()
    converter = PdfConverter(ocr_provider=fallback)
    converter._extract_local = AsyncMock(side_effect=SyncFailureError("Parser unavailable"))
    with pytest.raises(SyncFailureError, match="Parser unavailable"):
        await converter.convert_batch([str(path)])
    fallback.convert_batch.assert_not_awaited()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestTryReadAsText:
    """Tests for _try_read_as_text static method (binary detection)."""

    def test_plain_text_file_returns_content(self, temp_dir):
        path = os.path.join(temp_dir, "readme.docx")
        with open(path, "w", encoding="utf-8") as f:
            f.write("This is actually a plain text file with enough characters to pass threshold.")

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is not None
        assert "plain text file" in result

    def test_binary_file_returns_none(self, temp_dir):
        """File with >5% control chars → None."""
        path = os.path.join(temp_dir, "binary.docx")
        with open(path, "wb") as f:
            # Lots of control characters
            f.write(bytes(range(0, 32)) * 50)

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_empty_file_returns_none(self, temp_dir):
        path = os.path.join(temp_dir, "empty.docx")
        with open(path, "wb") as f:
            f.write(b"")

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_short_file_returns_none(self, temp_dir):
        """File with <10 chars stripped → None."""
        path = os.path.join(temp_dir, "short.docx")
        with open(path, "w", encoding="utf-8") as f:
            f.write("  hi  ")

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_non_utf8_returns_none(self, temp_dir):
        """Non-UTF-8 file → None."""
        path = os.path.join(temp_dir, "latin.docx")
        with open(path, "wb") as f:
            f.write(b"\xff\xfe" + "Héllo wörld".encode("utf-16-le"))

        result = HybridDocumentConverter._try_read_as_text(path)
        assert result is None

    def test_nonexistent_file_returns_none(self):
        result = HybridDocumentConverter._try_read_as_text("/nonexistent/file.docx")
        assert result is None


@pytest.mark.parametrize("ocr_text", ["Recovered visual text", None])
async def test_non_ocr_gap_survives_existing_no_text_fallback(tmp_path, ocr_text):
    from docx import Document
    from docx.opc.constants import RELATIONSHIP_TYPE as RT

    from airweave.domains.converters.docx import DocxConverter

    document = Document()
    document.part.relate_to("https://never-fetch.invalid/image", RT.IMAGE, is_external=True)
    path = tmp_path / "no-text.docx"
    document.save(path)
    fallback = AsyncMock()
    fallback.convert_batch.return_value = {str(path): ocr_text}
    result = (await DocxConverter(fallback).convert_batch([str(path)]))[str(path)]
    fallback.convert_batch.assert_awaited_once_with([str(path)])
    assert result.text == ocr_text and result.gap == "embedded_content_unprocessed"
    no_ocr = (await DocxConverter().convert_batch([str(path)]))[str(path)]
    assert no_ocr.text is None and no_ocr.gap == "embedded_content_unprocessed"


async def test_returned_limit_failure_cannot_fall_through_text_disguise_or_ocr(tmp_path):
    from airweave.domains.converters._base import ConversionResult
    from airweave.domains.converters.docx import DocxConverter

    path = tmp_path / "text.docx"
    path.write_text("Readable disguised text must not bypass the explicit preparation limit.")
    ocr = AsyncMock()
    converter = DocxConverter(ocr)
    converter._extract_local = AsyncMock(
        return_value=ConversionResult(text=None, failure_reason="preparation_limit")
    )
    result = (await converter.convert_batch([str(path)]))[str(path)]
    assert result.text is None and result.failure_reason == "preparation_limit"
    ocr.convert_batch.assert_not_awaited()


async def test_office_no_text_ocr_fallback_cannot_exceed_selected_output_bound(tmp_path):
    from docx import Document
    from docx.opc.constants import RELATIONSHIP_TYPE as RT

    from airweave.domains.converters.docx import DocxConverter
    from airweave.domains.converters.package_limits import PackageTextLimits

    document = Document()
    document.part.relate_to("https://never-fetch.invalid/image", RT.IMAGE, is_external=True)
    path = tmp_path / "image.docx"
    document.save(path)
    ocr = AsyncMock()
    ocr.convert_batch.return_value = {str(path): "اردو"}
    result = (
        await DocxConverter(ocr, PackageTextLimits(maximum_output_bytes=7)).convert_batch(
            [str(path)]
        )
    )[str(path)]
    assert result.text is None and result.failure_reason == "preparation_limit"
    ocr.convert_batch.assert_awaited_once_with([str(path)])
