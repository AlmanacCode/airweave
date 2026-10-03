"""DOCX converter with hybrid text extraction + OCR fallback."""

from __future__ import annotations

from typing import Optional

from airweave.domains.converters._base import ConversionResult, HybridDocumentConverter
from airweave.domains.converters.package_limits import PackageTextLimits
from airweave.domains.converters.text_extractors.docx import extract_docx, extract_docx_text
from airweave.domains.ocr.protocols import OcrProvider


class DocxConverter(HybridDocumentConverter):
    """Converts DOCX files to markdown using text extraction with OCR fallback."""

    def __init__(
        self, ocr_provider: OcrProvider | None = None, limits: PackageTextLimits | None = None
    ) -> None:
        """Compose actual immutable package/text limits alongside existing OCR fallback."""
        self.limits = limits or PackageTextLimits()
        super().__init__(ocr_provider, output_limits=self.limits)

    async def _try_extract(self, path: str) -> Optional[str]:
        return await extract_docx_text(path, self.limits)

    async def _extract_local(self, path: str) -> ConversionResult:
        return await extract_docx(path, self.limits)
