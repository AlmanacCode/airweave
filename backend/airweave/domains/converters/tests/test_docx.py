"""Tests for DocxConverter — _try_extract success and failure paths."""

from unittest.mock import AsyncMock, patch

import pytest

from airweave.domains.converters.docx import DocxConverter


@pytest.fixture
def converter():
    return DocxConverter(ocr_provider=None)


class TestDocxConverter:
    @pytest.mark.asyncio
    async def test_try_extract_success(self, converter):
        """When extract_docx_text returns content, _try_extract returns it."""
        with patch(
            "airweave.domains.converters.docx.extract_docx_text",
            new_callable=AsyncMock,
            return_value="# Document\n\nHello world",
        ):
            result = await converter._try_extract("/fake/doc.docx")

        assert result == "# Document\n\nHello world"

    @pytest.mark.asyncio
    async def test_try_extract_returns_none(self, converter):
        """When extract_docx_text returns None, _try_extract returns None (needs OCR)."""
        with patch(
            "airweave.domains.converters.docx.extract_docx_text",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await converter._try_extract("/fake/doc.docx")

        assert result is None

    @pytest.mark.asyncio
    async def test_try_extract_empty_string(self, converter):
        """Empty string from extractor → None (falsy)."""
        with patch(
            "airweave.domains.converters.docx.extract_docx_text",
            new_callable=AsyncMock,
            return_value="",
        ):
            result = await converter._try_extract("/fake/doc.docx")

        assert result is None or result == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("media", ["none", "inline", "table", "header", "floating", "external"])
async def test_docx_relationship_gap_preserves_multilingual_text_and_original(tmp_path, media):
    from io import BytesIO

    from docx import Document
    from docx.opc.constants import RELATIONSHIP_TYPE as RT
    from docx.oxml.ns import qn
    from PIL import Image

    document = Document()
    text = (
        "नमस्ते — اردو — e\u0301 — 中文. Retained paragraph with enough useful text for extraction."
    )
    document.add_paragraph(text)
    table = document.add_table(rows=1, cols=1)
    table.cell(0, 0).text = "Multilingual table 日本語"
    image = BytesIO()
    Image.new("RGB", (10, 10), "blue").save(image, format="PNG")
    if media in {"inline", "floating"}:
        picture = document.add_paragraph().add_run().add_picture(image)
        if media == "floating":
            # Controlled relationship fixture; no claim of rendering this floating layout.
            picture._inline.tag = qn("wp:anchor")
    elif media == "table":
        table.cell(0, 0).paragraphs[0].add_run().add_picture(image)
    elif media == "header":
        document.sections[0].header.paragraphs[0].add_run().add_picture(image)
    elif media == "external":
        document.part.relate_to("https://never-fetch.invalid/image", RT.IMAGE, is_external=True)
    path = tmp_path / "mixed.docx"
    document.save(path)
    before = path.read_bytes()
    ocr = AsyncMock()
    ocr.convert_batch.side_effect = AssertionError("Usable mixed text must not call OCR")
    result = (await DocxConverter(ocr).convert_batch([str(path)]))[str(path)]
    assert text in result.text and "日本語" in result.text
    assert result.gap == (None if media == "none" else "embedded_content_unprocessed")
    ocr.convert_batch.assert_not_awaited()
    assert path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("text", ["नमस्ते", "اردو"])
async def test_short_unicode_docx_survives_without_ocr(tmp_path, text):
    from docx import Document

    document = Document()
    document.add_paragraph(text)
    path = tmp_path / "short.docx"
    document.save(path)
    ocr = AsyncMock()
    result = (await DocxConverter(ocr).convert_batch([str(path)]))[str(path)]
    assert result.text == text and result.failure_reason is None
    ocr.convert_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_docx_public_block_order_and_output_limit_preserve_original(tmp_path):
    from docx import Document

    from airweave.domains.converters.package_limits import PackageTextLimits

    document = Document()
    document.add_paragraph("पहले")
    document.add_table(rows=1, cols=1).cell(0, 0).text = "درمیان"
    document.add_paragraph("बाद")
    path = tmp_path / "ordered.docx"
    document.save(path)
    before = path.read_bytes()
    ocr = AsyncMock()
    ocr.convert_batch.side_effect = AssertionError("Output limit must not call OCR")
    text = (await DocxConverter(ocr).convert_batch([str(path)]))[str(path)].text
    assert text.index("पहले") < text.index("درمیان") < text.index("बाद")
    bounded = DocxConverter(ocr, PackageTextLimits(maximum_output_bytes=len(text.encode()) - 1))
    result = (await bounded.convert_batch([str(path)]))[str(path)]
    assert result.text is None and result.failure_reason == "preparation_limit"
    assert path.read_bytes() == before
    ocr.convert_batch.assert_not_awaited()
