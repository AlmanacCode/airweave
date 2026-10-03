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
