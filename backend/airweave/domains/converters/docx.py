"""DOCX converter with hybrid text extraction + OCR fallback."""

from __future__ import annotations

from typing import Optional

from airweave.domains.converters._base import ConversionResult, HybridDocumentConverter
from airweave.domains.converters.text_extractors.docx import extract_docx, extract_docx_text


class DocxConverter(HybridDocumentConverter):
    """Converts DOCX files to markdown using text extraction with OCR fallback."""

    async def _try_extract(self, path: str) -> Optional[str]:
        return await extract_docx_text(path)

    async def _extract_local(self, path: str) -> ConversionResult:
        return await extract_docx(path)
